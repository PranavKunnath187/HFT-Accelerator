#!/usr/bin/env python3
"""
tcp_gen.py — TCP byte-stream generator for ITCH-style framing

Paper-style framing (Option A):
    [ LEN ][ TYPE ][ PAYLOAD ]
where:
    LEN  = 1 byte, counts bytes(TYPE + PAYLOAD)
    TYPE = 1 byte
    PAYLOAD_LEN = LEN - 1

This tool connects to the board’s TCP server and writes a stream of framed messages.
It can intentionally fragment writes to stress-test stream correctness (partial LEN/TYPE/PAYLOAD).
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import socket
import struct
import sys
import time
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple


# -----------------------------
# Config / types
# -----------------------------

@dataclass(frozen=True)
class GenConfig:
    host: str
    port: int
    duration_s: float
    rate_mps: float               # messages per second (0 => as fast as possible)
    seed: int
    tcp_nodelay: bool
    recv_replies: bool
    reply_timeout_s: float
    log_csv: Optional[str]

    # Message behavior
    msg_type: int                 # 0..255 (1 byte)
    payload_len: int              # bytes (0..254, because LEN is 1 byte and includes TYPE)
    burst: bool
    burst_msgs: int
    burst_idle_ms: int

    # Stream stress / fragmentation
    fragment: bool
    fragment_max_chunk: int       # max bytes per send() call when fragmenting
    coalesce: bool                # if True, send multiple messages in a single send()


@dataclass
class MsgStats:
    sent: int = 0
    send_bytes: int = 0
    send_fail: int = 0
    replies: int = 0
    recv_bytes: int = 0

    # Timing
    t0: float = 0.0
    t_last_report: float = 0.0


# -----------------------------
# Framing helpers (paper-style)
# -----------------------------

def build_message(msg_type: int, payload: bytes) -> bytes:
    """
    Build one framed message: [LEN][TYPE][PAYLOAD]
    LEN is 1 byte and includes TYPE + PAYLOAD (Option A).
    """
    if not (0 <= msg_type <= 0xFF):
        raise ValueError("msg_type must fit in 1 byte")
    if len(payload) > 254:
        # LEN is 1 byte, and must include TYPE (1) + payload
        raise ValueError("payload too large for 1-byte LEN framing")
    length = 1 + len(payload)  # TYPE + PAYLOAD
    return struct.pack("BB", length, msg_type) + payload


def make_payload(cfg: GenConfig, seq: int, rng: random.Random) -> bytes:
    """
    TODO: Replace this with a real ITCH message body generator.

    For now, emit a deterministic payload of cfg.payload_len bytes, embedding seq in the first 4 bytes.
    This is good enough for bring-up + parser boundary testing.
    """
    n = cfg.payload_len
    if n == 0:
        return b""

    # Example pattern: first 4 bytes = seq (big-endian), rest = pseudo-random
    out = bytearray(n)
    if n >= 4:
        out[0:4] = struct.pack(">I", seq & 0xFFFFFFFF)
        for i in range(4, n):
            out[i] = rng.randrange(0, 256)
    else:
        # n < 4: just fill with seq bytes
        tmp = struct.pack(">I", seq & 0xFFFFFFFF)
        out[:] = tmp[0:n]

    return bytes(out)


# -----------------------------
# Socket send utilities
# -----------------------------

def send_all(sock: socket.socket, data: bytes) -> None:
    """Reliable send-all for TCP sockets."""
    view = memoryview(data)
    total = 0
    while total < len(data):
        n = sock.send(view[total:])
        if n <= 0:
            raise RuntimeError("socket.send returned <= 0")
        total += n


def send_fragmented(sock: socket.socket, data: bytes, rng: random.Random, max_chunk: int) -> None:
    """
    Intentionally split a single message across multiple send() calls.
    This simulates real TCP chunking and stresses the receiver/parser.
    """
    if max_chunk <= 0:
        raise ValueError("max_chunk must be > 0")
    i = 0
    while i < len(data):
        # Choose a random chunk length up to max_chunk
        chunk_len = rng.randint(1, min(max_chunk, len(data) - i))
        send_all(sock, data[i:i + chunk_len])
        i += chunk_len


def maybe_set_nodelay(sock: socket.socket, enabled: bool) -> None:
    if enabled:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


# -----------------------------
# Generator loop
# -----------------------------

def message_stream(cfg: GenConfig) -> Iterator[bytes]:
    """
    Generate framed messages indefinitely. The caller controls duration/rate.
    """
    rng = random.Random(cfg.seed)
    seq = 0

    while True:
        payload = make_payload(cfg, seq=seq, rng=rng)
        msg = build_message(cfg.msg_type, payload)
        yield msg
        seq += 1


def rate_controller_next_deadline(rate_mps: float) -> Iterator[float]:
    """
    Yields successive send deadlines for a target message rate.
    If rate_mps <= 0, yields immediately (no throttling).
    """
    if rate_mps <= 0:
        while True:
            yield 0.0
    else:
        period = 1.0 / rate_mps
        t = time.perf_counter()
        while True:
            t += period
            yield t


def maybe_sleep_until(deadline: float) -> None:
    if deadline <= 0:
        return
    now = time.perf_counter()
    dt = deadline - now
    if dt > 0:
        # sleep most of it; leave a little slack
        time.sleep(dt)


def run(cfg: GenConfig) -> int:
    stats = MsgStats()
    stats.t0 = time.perf_counter()
    stats.t_last_report = stats.t0

    # CSV logging (optional)
    csv_file = None
    csv_writer = None
    if cfg.log_csv:
        os.makedirs(os.path.dirname(cfg.log_csv) or ".", exist_ok=True)
        csv_file = open(cfg.log_csv, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["seq", "send_time_s", "bytes_sent", "note"])

    # Connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(5.0)
        sock.connect((cfg.host, cfg.port))
        maybe_set_nodelay(sock, cfg.tcp_nodelay)
        sock.settimeout(cfg.reply_timeout_s if cfg.recv_replies else None)

        gen = message_stream(cfg)
        deadlines = rate_controller_next_deadline(cfg.rate_mps)
        rng = random.Random(cfg.seed ^ 0xA5A5A5A5)

        end_time = stats.t0 + cfg.duration_s if cfg.duration_s > 0 else float("inf")

        seq = 0
        while time.perf_counter() < end_time:
            # Optional burst mode: send burst_msgs quickly, then idle
            if cfg.burst:
                for _ in range(cfg.burst_msgs):
                    if time.perf_counter() >= end_time:
                        break
                    _send_one(cfg, sock, next(gen), rng, stats, csv_writer, seq)
                    seq += 1
                time.sleep(cfg.burst_idle_ms / 1000.0)
                continue

            # Normal mode (rate-controlled)
            deadline = next(deadlines)
            maybe_sleep_until(deadline)
            _send_one(cfg, sock, next(gen), rng, stats, csv_writer, seq)
            seq += 1

            # (Optional) receive replies / status (placeholder)
            if cfg.recv_replies:
                # TODO: define reply format in interfaces.md and parse accordingly.
                # For now, just try to read some bytes and count them.
                try:
                    data = sock.recv(4096)
                    if data:
                        stats.replies += 1
                        stats.recv_bytes += len(data)
                    else:
                        # peer closed
                        break
                except socket.timeout:
                    pass

            _maybe_report(stats)

        _final_report(stats)
        return 0

    except Exception as e:
        print(f"[tcp_gen] ERROR: {e}", file=sys.stderr)
        return 1

    finally:
        try:
            sock.close()
        except Exception:
            pass
        if csv_file:
            csv_file.close()


def _send_one(
    cfg: GenConfig,
    sock: socket.socket,
    msg: bytes,
    rng: random.Random,
    stats: MsgStats,
    csv_writer: Optional[csv.writer],
    seq: int,
) -> None:
    """
    Send one message, possibly fragmented or coalesced.
    Coalescing is handled by sending batches elsewhere; here we only handle fragmentation.
    """
    t_send = time.perf_counter()

    try:
        if cfg.fragment:
            send_fragmented(sock, msg, rng=rng, max_chunk=cfg.fragment_max_chunk)
        else:
            send_all(sock, msg)

        stats.sent += 1
        stats.send_bytes += len(msg)
        if csv_writer:
            csv_writer.writerow([seq, f"{t_send:.9f}", len(msg), "ok"])

    except Exception:
        stats.send_fail += 1
        if csv_writer:
            csv_writer.writerow([seq, f"{t_send:.9f}", 0, "send_fail"])
        # Re-raise to stop the run; you can change this policy if desired.
        raise


def _maybe_report(stats: MsgStats, every_s: float = 1.0) -> None:
    now = time.perf_counter()
    if now - stats.t_last_report >= every_s:
        elapsed = now - stats.t0
        mps = stats.sent / elapsed if elapsed > 0 else 0.0
        mbps = (stats.send_bytes * 8) / elapsed / 1e6 if elapsed > 0 else 0.0
        print(f"[tcp_gen] sent={stats.sent}  mps={mps:.1f}  Mbps={mbps:.2f}  fail={stats.send_fail}")
        stats.t_last_report = now


def _final_report(stats: MsgStats) -> None:
    now = time.perf_counter()
    elapsed = now - stats.t0
    mps = stats.sent / elapsed if elapsed > 0 else 0.0
    mbps = (stats.send_bytes * 8) / elapsed / 1e6 if elapsed > 0 else 0.0
    print("---- tcp_gen summary ----")
    print(f"elapsed_s:   {elapsed:.3f}")
    print(f"messages:    {stats.sent}")
    print(f"send_bytes:  {stats.send_bytes}")
    print(f"mps:         {mps:.2f}")
    print(f"throughput:  {mbps:.3f} Mbps")
    print(f"send_fail:   {stats.send_fail}")
    if stats.replies or stats.recv_bytes:
        print(f"replies:     {stats.replies}")
        print(f"recv_bytes:  {stats.recv_bytes}")


# -----------------------------
# CLI
# -----------------------------

def parse_args(argv: list[str]) -> GenConfig:
    p = argparse.ArgumentParser(description="TCP ITCH-style stream generator (LEN+TYPE+PAYLOAD).")
    p.add_argument("--host", required=True, help="Board IP/hostname")
    p.add_argument("--port", type=int, required=True, help="Board TCP port")
    p.add_argument("--duration", type=float, default=10.0, help="Seconds to run (0 => forever)")
    p.add_argument("--rate", type=float, default=0.0, help="Messages/sec (0 => as fast as possible)")
    p.add_argument("--seed", type=int, default=1, help="RNG seed for deterministic payloads")
    p.add_argument("--nodelay", action="store_true", help="Enable TCP_NODELAY")

    p.add_argument("--msg-type", type=lambda x: int(x, 0), default=0x44, help="1-byte TYPE (e.g., 0x44)")
    p.add_argument("--payload-len", type=int, default=16, help="Payload length in bytes (LEN=1+payload_len, max 254)")

    p.add_argument("--burst", action="store_true", help="Enable burst mode")
    p.add_argument("--burst-msgs", type=int, default=100, help="Messages per burst")
    p.add_argument("--burst-idle-ms", type=int, default=100, help="Idle time between bursts in ms")

    p.add_argument("--fragment", action="store_true", help="Fragment messages across multiple send() calls")
    p.add_argument("--fragment-max-chunk", type=int, default=8, help="Max chunk size when fragmenting (bytes)")

    p.add_argument("--recv-replies", action="store_true", help="Attempt to recv() reply bytes (placeholder)")
    p.add_argument("--reply-timeout", type=float, default=0.01, help="recv timeout when --recv-replies is set (s)")

    p.add_argument("--log-csv", default=None, help="Write per-message log CSV (optional)")

    a = p.parse_args(argv)

    return GenConfig(
        host=a.host,
        port=a.port,
        duration_s=a.duration,
        rate_mps=a.rate,
        seed=a.seed,
        tcp_nodelay=a.nodelay,
        recv_replies=a.recv_replies,
        reply_timeout_s=a.reply_timeout,

        msg_type=a.msg_type & 0xFF,
        payload_len=a.payload_len,
        burst=a.burst,
        burst_msgs=a.burst_msgs,
        burst_idle_ms=a.burst_idle_ms,

        fragment=a.fragment,
        fragment_max_chunk=a.fragment_max_chunk,
        coalesce=False,  # reserved if you later add batch-send
        log_csv=a.log_csv,
    )


def main() -> int:
    cfg = parse_args(sys.argv[1:])
    if cfg.payload_len < 0 or cfg.payload_len > 254:
        print("[tcp_gen] payload_len must be 0..254", file=sys.stderr)
        return 2
    return run(cfg)


if __name__ == "__main__":
    raise SystemExit(main())

