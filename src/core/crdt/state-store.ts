import type { Node } from "../node/index.js";
import type { Envelope } from "../transport/types.js";
import type { DeltaPayload, SyncReqPayload, SyncResPayload } from "../transport/types.js";
import { decodePayload } from "../transport/index.js";
import { MessageType } from "../transport/constants.js";
import { LwwSet } from "./lww-set.js";
import type { WireEntry, CrdtStats } from "./types.js";
import { TOMBSTONE_GC_MS, GC_INTERVAL_MS, MAX_DELTA_ENTRIES } from "./types.js";
import { LifecycleError } from "../../errors.js";

export class StateStore {
  private readonly node:       Node;
  private readonly namespaces: Map<string, LwwSet> = new Map();
  private gcTimer:             NodeJS.Timeout | null = null;
  private live = false;

  constructor(node: Node) {
    this.node = node;
  }

  start(): void {
    if (this.live) throw new LifecycleError("StateStore already started");
    this.live = true;

    this.node.on("delta",   (env)           => this.onDelta(env));
    this.node.on("syncReq", (env, respond)  => void this.onSyncReq(env, respond));

    this.gcTimer = setInterval(() => this.gc(), GC_INTERVAL_MS);
    this.gcTimer.unref();
  }

  stop(): void {
    if (this.gcTimer) { clearInterval(this.gcTimer); this.gcTimer = null; }
    this.live = false;
  }

  // Apply a local mutation and broadcast it as a DELTA.
  // ts defaults to Date.now() — callers can pass a pre-computed timestamp
  // to batch multiple mutations with the same logical instant.
  async mutate(
    ns:    string,
    key:   Uint8Array,
    value: Uint8Array | null,
    ts?:   number,
  ): Promise<void> {
    const set  = this.ns(ns);
    const wall = ts ?? Date.now();

    const entry: WireEntry = {
      value,
      ts:     wall,
      seq:    this.node.logicalClock,
      nodeId: this.node.id,
    };

    set.merge(key, entry);
    this.node.setKeyCount(this.keyCount());

    await this.node.publishDelta({
      ns,
      mutations: [[key, value, wall]],
    });
  }

  // Delete a key by writing a tombstone.
  async delete(ns: string, key: Uint8Array): Promise<void> {
    return this.mutate(ns, key, null);
  }

  get(ns: string, key: Uint8Array): Uint8Array | null | undefined {
    const entry = this.namespaces.get(ns)?.get(key);
    if (!entry) return undefined;
    // Tombstone is a valid resolved state; return null, not undefined.
    return entry.value;
  }

  has(ns: string, key: Uint8Array): boolean {
    const entry = this.namespaces.get(ns)?.get(key);
    return entry !== undefined && entry.value !== null;
  }

  // Request a full anti-entropy sync from a specific peer for a namespace.
  async requestSync(ns: string, peerRouterAddr: string): Promise<void> {
    const since = this.namespaces.get(ns)?.storeSeq ?? 0n;
    this.node.dialRouter(peerRouterAddr);
    const payload: SyncReqPayload = { ns, since };
    await this.node.rpcSend(MessageType.SYNC_REQ, payload);
  }

  get stats(): CrdtStats {
    let keys = 0, tombstones = 0, maxSeq = 0n;
    for (const set of this.namespaces.values()) {
      keys       += set.size;
      tombstones += set.tombstoneCount;
      if (set.storeSeq > maxSeq) maxSeq = set.storeSeq;
    }
    return { namespaces: this.namespaces.size, totalKeys: keys, tombstones, maxStoreSeq: maxSeq };
  }

  // ── Private ──────────────────────────────────────────────────────────────────

  private onDelta(env: Envelope): void {
    const body = decodePayload<DeltaPayload>(env.payload);
    const set  = this.ns(body.ns);

    for (const [key, value, ts] of body.mutations) {
      const entry: WireEntry = {
        value,
        ts,
        seq:    env.seq,
        nodeId: env.senderId,
      };
      set.merge(key, entry);
    }

    this.node.setKeyCount(this.keyCount());
  }

  private async onSyncReq(
    env:     Envelope,
    respond: (body: unknown) => Promise<void>,
  ): Promise<void> {
    const req = decodePayload<SyncReqPayload>(env.payload);
    const set = this.namespaces.get(req.ns);

    if (!set) {
      const empty: SyncResPayload = { ns: req.ns, entries: [], isComplete: true };
      await respond(empty);
      return;
    }

    const [entries, hasMore] = set.delta(req.since, MAX_DELTA_ENTRIES);

    const body: SyncResPayload = {
      ns:         req.ns,
      entries:    entries.map(([k, e]) => [k, e.value, e.ts, e.storeSeq]),
      isComplete: !hasMore,
    };

    await respond(body);

    // If the delta was paginated, the peer must re-request with an updated
    // `since` cursor. We do not proactively send the next page — that would
    // require per-peer state tracking and could saturate the HWM.
  }

  private onSyncRes(env: Envelope): void {
    const body = decodePayload<SyncResPayload>(env.payload);
    const set  = this.ns(body.ns);

    for (const [key, value, ts, _peerStoreSeq] of body.entries) {
      const entry: WireEntry = {
        value,
        ts,
        seq:    env.seq,
        nodeId: env.senderId,
      };
      set.merge(key, entry);
    }

    this.node.setKeyCount(this.keyCount());
  }

  private gc(): void {
    for (const set of this.namespaces.values()) {
      set.gcTombstones(TOMBSTONE_GC_MS);
    }
  }

  private ns(name: string): LwwSet {
    let set = this.namespaces.get(name);
    if (!set) { set = new LwwSet(); this.namespaces.set(name, set); }
    return set;
  }

  private keyCount(): number {
    let n = 0;
    for (const set of this.namespaces.values()) n += set.size;
    return n;
  }

  // Expose rpcSend for callers who need to manually trigger sync
  // (e.g., on peer:up after a period of disconnect).
  private dialAndSync(ns: string, peerAddr: string): void {
    void this.requestSync(ns, peerAddr);
  }
}
