import type { NodeId } from "../transport/index.js";

// What travels over the wire inside a DeltaPayload mutation tuple.
// storeSeq is local to each node's LwwSet — it is NOT transmitted.
export interface WireEntry {
  value:  Uint8Array | null;  // null = tombstone (deletion)
  ts:     number;             // skew-adjusted wall-clock ms
  seq:    bigint;             // sender's Lamport clock at mutation time
  nodeId: NodeId;             // sender identity for deterministic tie-breaking
}

// What LwwSet stores internally after a successful merge.
export interface StoreEntry extends WireEntry {
  // Local monotonic counter incremented on every winning merge.
  // Used exclusively for delta(since) queries — never sent over the wire.
  storeSeq: bigint;
}

export interface CrdtStats {
  namespaces: number;
  totalKeys:  number;
  tombstones: number;
  // Highest storeSeq across all namespaces — a proxy for total mutation count.
  maxStoreSeq: bigint;
}

// GC window must exceed deadMs (default 6s) so tombstones outlive any
// peer that could have missed the deletion and reconnects to re-add it.
export const TOMBSTONE_GC_MS  = 30_000;
export const GC_INTERVAL_MS   = 60_000;
// Cap SYNC_RES payload. Prevents one anti-entropy response from filling the HWM.
export const MAX_DELTA_ENTRIES = 10_000;
