// rtl/defines.svh
`ifndef DEFINES_SVH
`define DEFINES_SVH

// Paper framing: LEN=1 byte includes (TYPE + PAYLOAD)
`define LEN_W      8
`define TYPE_W     8
`define BYTE_W     8

// Choose depth large enough to absorb bursts (tune later)
`define RING_DEPTH 2048

`endif
