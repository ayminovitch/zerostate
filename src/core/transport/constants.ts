// 2 I/O threads: one TX, one RX. Beyond this, thread-scheduler overhead
// outweighs gains unless sockets exceed 4 per process.
export const ZMQ_IO_THREADS = 2;
export const ZMQ_MAX_SOCKETS = 256;

// Low HWM is intentional. A stale delta queued for 50 ms is worse than a
// dropped one — receivers recover via anti-entropy, not a deep queue drain.
export const HWM_SEND = 1_000;
export const HWM_RECV = 2_000;

export const LINGER_MS            = 0;
export const HEARTBEAT_IVL_MS     = 1_000;
export const HEARTBEAT_TIMEOUT_MS = 3_000;
export const RECONNECT_IVL_MS     = 100;

// Wire frame layout — every multipart message, every socket type:
//   [0] identity  16 bytes   NodeId (UUID raw bytes, no dashes)
//   [1] type       1 byte    MessageType discriminant
//   [2] seq        8 bytes   uint64 big-endian
//   [3] payload    N bytes   msgpack body
//
// Routing decisions happen on frames 0-1 only; frame 3 is never touched by
// relay nodes. Keeping identity in its own frame lets ROUTER sockets route
// without deserialising the envelope.
export const FRAME_COUNT     = 4;
export const F_IDENTITY      = 0;
export const F_TYPE          = 1;
export const F_SEQ           = 2;
export const F_PAYLOAD       = 3;
export const ID_LEN          = 16;
export const TYPE_LEN        = 1;
export const SEQ_LEN         = 8;

export const enum MessageType {
  DELTA     = 0x01,
  HEARTBEAT = 0x02,
  SYNC_REQ  = 0x03,
  SYNC_RES  = 0x04,
  JOIN      = 0x05,
  LEAVE     = 0x06,
  ACK       = 0x07,
}

export const BASE_PUB_PORT    = 5550;
export const BASE_ROUTER_PORT = 6550;
