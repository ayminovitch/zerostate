import { cmpNodeId } from "../transport/index.js";
import type { WireEntry, StoreEntry } from "./types.js";

// Keys are hex-encoded so Map lookup is O(1) string comparison rather than
// byte-by-byte Uint8Array iteration. Reconstructed to Buffer for delta output.
function toKey(raw: Uint8Array): string {
  return Buffer.from(raw).toString("hex");
}

export class LwwSet {
  private readonly data = new Map<string, StoreEntry>();
  private _storeSeq   = 0n;
  private _tombstones = 0;

  // Returns true if the incoming entry displaced the existing one.
  merge(rawKey: Uint8Array, incoming: WireEntry): boolean {
    const k        = toKey(rawKey);
    const existing = this.data.get(k);

    if (existing !== undefined && !this.wins(incoming, existing)) return false;

    // Maintain tombstone counter without a full scan.
    if (existing !== undefined) {
      if (existing.value === null && incoming.value !== null) this._tombstones--;
      if (existing.value !== null && incoming.value === null) this._tombstones++;
    } else if (incoming.value === null) {
      this._tombstones++;
    }

    this._storeSeq += 1n;
    this.data.set(k, { ...incoming, storeSeq: this._storeSeq });
    return true;
  }

  get(rawKey: Uint8Array): StoreEntry | undefined {
    return this.data.get(toKey(rawKey));
  }

  has(rawKey: Uint8Array): boolean {
    return this.data.has(toKey(rawKey));
  }

  // All entries with storeSeq > since, up to limit.
  // Returns [entries, hasMore] — hasMore drives SYNC_RES pagination.
  delta(since: bigint, limit: number): [Array<[Uint8Array, StoreEntry]>, boolean] {
    const out: Array<[Uint8Array, StoreEntry]> = [];

    for (const [hexK, entry] of this.data) {
      if (entry.storeSeq <= since) continue;
      if (out.length >= limit) return [out, true];
      out.push([Buffer.from(hexK, "hex"), entry]);
    }

    return [out, false];
  }

  // Remove tombstones older than olderThanMs. Called by the GC timer.
  // Returns the number of entries pruned.
  gcTombstones(olderThanMs: number): number {
    const threshold = Date.now() - olderThanMs;
    let pruned = 0;

    for (const [k, entry] of this.data) {
      if (entry.value === null && entry.ts < threshold) {
        this.data.delete(k);
        this._tombstones--;
        pruned++;
      }
    }

    return pruned;
  }

  get size():           number { return this.data.size; }
  get tombstoneCount(): number { return this._tombstones; }
  get storeSeq():       bigint { return this._storeSeq; }

  // Three-level total order: wall-clock → Lamport seq → NodeId.
  // All three layers are needed: NTP can collapse ms precision, Lamport
  // seq can collide across causally-unrelated branches, NodeId is always unique.
  private wins(a: WireEntry, b: WireEntry): boolean {
    if (a.ts  !== b.ts)  return a.ts  > b.ts;
    if (a.seq !== b.seq) return a.seq > b.seq;
    return cmpNodeId(a.nodeId, b.nodeId) > 0;
  }
}
