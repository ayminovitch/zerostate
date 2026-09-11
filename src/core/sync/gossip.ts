import { EventEmitter } from "node:events";
import type { Node }        from "../node/index.js";
import type { StateStore }  from "../crdt/state-store.js";
import type { PeerEntry }   from "../node/types.js";
import { PeerState }        from "../node/types.js";
import { nodeIdToHex }      from "../transport/index.js";
import { MessageType }      from "../transport/constants.js";
import type { SyncReqPayload } from "../transport/types.js";
import { LifecycleError }   from "../../errors.js";
import type { GossipCfg, GossipRoundStats } from "./types.js";
import { DEFAULT_GOSSIP_CFG } from "./types.js";

export interface GossipEvents {
  "round": [stats: GossipRoundStats];
  "error": [err: Error];
}

export class GossipEngine extends EventEmitter<GossipEvents> {
  private readonly node:  Node;
  private readonly store: StateStore;
  private readonly cfg:   Required<GossipCfg>;

  private timer:          ReturnType<typeof setTimeout> | null = null;
  private live            = false;
  private roundCount      = 0;
  private stableRounds    = 0;
  private currentInterval: number;
  private lastMaxSeq:      bigint = 0n;

  // Prevents concurrent sync requests to the same peer within one process.
  // Without this, a slow peer can accumulate queued SYNC_REQs faster than
  // it can respond, filling its ROUTER receive HWM.
  private readonly inFlight = new Set<string>();

  constructor(node: Node, store: StateStore, cfg?: Partial<GossipCfg>) {
    super();
    this.node  = node;
    this.store = store;
    this.cfg   = { ...DEFAULT_GOSSIP_CFG, ...cfg };
    this.currentInterval = this.cfg.intervalMs;
  }

  start(): void {
    if (this.live) throw new LifecycleError("GossipEngine already started");
    this.live = true;

    // Trigger an immediate sync round when a peer transitions CONNECTING → ALIVE.
    // This is the hot path: a reconnected or newly-discovered peer needs state
    // immediately, not after waiting for the next scheduled round.
    this.node.on("peer:up", (entry) => void this.syncWithPeer(entry));

    this.scheduleTick(this.cfg.intervalMs);
  }

  stop(): void {
    this.live = false;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
  }

  get currentIntervalMs(): number { return this.currentInterval; }

  // ── Private ──────────────────────────────────────────────────────────────────

  private scheduleTick(ms: number): void {
    this.timer = setTimeout(() => void this.round(), ms);
    this.timer.unref();
  }

  private async round(): Promise<void> {
    if (!this.live) return;

    this.roundCount++;
    const peers = this.pickPeers();

    // Run per-peer syncs concurrently — each is independently bounded by inFlight.
    // allSettled ensures one failing peer doesn't abort the others.
    const namespaces = Array.from(this.store.digest().keys());
    await Promise.allSettled(peers.map((p) => this.syncWithPeer(p, namespaces)));

    const diverged = this.adaptInterval();

    this.emit("round", {
      round:          this.roundCount,
      peersContacted: peers.length,
      intervalMs:     this.currentInterval,
      stableRounds:   this.stableRounds,
      diverged,
    });

    if (this.live) this.scheduleTick(this.currentInterval);
  }

  private async syncWithPeer(peer: PeerEntry, namespaces?: string[]): Promise<void> {
    if (peer.state !== PeerState.ALIVE) return;

    const peerId = nodeIdToHex(peer.id);
    if (this.inFlight.has(peerId)) return;
    this.inFlight.add(peerId);

    try {
      const digest = this.store.digest();
      const ns     = namespaces ?? Array.from(digest.keys());

      if (ns.length === 0) return;

      this.node.dialRouter(peer.routerAddr);

      // One SYNC_REQ per namespace with our current storeSeq as the `since`
      // cursor. The peer responds with all entries > since. This is O(missing),
      // not O(total) — the critical property for large stores.
      for (const name of ns) {
        const since = digest.get(name) ?? 0n;
        const payload: SyncReqPayload = { ns: name, since };
        await this.node.rpcSend(MessageType.SYNC_REQ, payload);
      }
    } catch (err) {
      // Emit but don't throw — a single failing peer must not stall the round.
      this.emit("error", err instanceof Error ? err : new Error(String(err)));
    } finally {
      this.inFlight.delete(peerId);
    }
  }

  // Fisher-Yates in-place partial shuffle: O(FANOUT) instead of O(N).
  // We only need the first `fanout` elements uniformly distributed —
  // there is no need to shuffle the entire array.
  private pickPeers(): PeerEntry[] {
    const all = Array.from(this.node.peers).filter((p) => p.state === PeerState.ALIVE);
    const k   = Math.min(this.cfg.fanout, all.length);

    for (let i = 0; i < k; i++) {
      const j        = i + Math.floor(Math.random() * (all.length - i));
      const tmp      = all[i]!;
      all[i]         = all[j]!;
      all[j]         = tmp;
    }

    return all.slice(0, k);
  }

  // Returns true if the store diverged (new data arrived since last round).
  private adaptInterval(): boolean {
    const { maxStoreSeq } = this.store.stats;
    const diverged        = maxStoreSeq > this.lastMaxSeq;

    if (diverged) {
      this.stableRounds    = 0;
      this.currentInterval = this.cfg.minIntervalMs;
      this.lastMaxSeq      = maxStoreSeq;
    } else {
      this.stableRounds++;
      if (this.stableRounds > this.cfg.stableRoundsBeforeBackoff) {
        // Multiplicative increase mirrors TCP slow-start in reverse:
        // aggressive when active, conservative when idle.
        this.currentInterval = Math.min(
          Math.floor(this.currentInterval * 1.5),
          this.cfg.maxIntervalMs,
        );
      }
    }

    return diverged;
  }
}
