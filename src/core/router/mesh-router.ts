import { EventEmitter }   from "node:events";
import type { Node }       from "../node/index.js";
import type { PeerEntry }  from "../node/types.js";
import { PeerState }       from "../node/types.js";
import type { StateStore } from "../crdt/state-store.js";
import type { DeltaPayload, SyncReqPayload, Envelope } from "../transport/types.js";
import { nodeIdToHex }     from "../transport/index.js";
import { MessageType }     from "../transport/constants.js";
import { LifecycleError }  from "../../errors.js";
import { TokenBucket }     from "./token-bucket.js";
import { RouteTable }      from "./route-table.js";
import { BackpressureLevel, DEFAULT_ROUTER_CFG } from "./types.js";
import type { RouterCfg, RouteEntry } from "./types.js";

export interface MeshRouterEvents {
  // Fired once when fill ratio drops below warnThreshold.
  "backpressure:warn":  [level: BackpressureLevel, fillRatio: number];
  // Fired once when tokens are fully exhausted.
  "backpressure:full":  [level: BackpressureLevel];
  // Fired when back-pressure clears after previously being WARN or FULL.
  "backpressure:clear": [level: BackpressureLevel];
  // RTT sample recorded for a peer.
  "rtt:sample":         [peerId: string, rttMs: number, ewmaMs: number];
}

export class MeshRouter extends EventEmitter<MeshRouterEvents> {
  private readonly node:   Node;
  private readonly store:  StateStore;
  private readonly bucket: TokenBucket;
  private readonly routes: RouteTable;
  private readonly cfg:    Required<RouterCfg>;

  private _bp     = BackpressureLevel.NONE;
  private live     = false;

  // Last probe dispatch time per peer (hex → hrtime ns).
  // Matched against the next syncRes from that sender to compute RTT.
  private readonly probeNs = new Map<string, bigint>();

  constructor(node: Node, store: StateStore, cfg?: Partial<RouterCfg>) {
    super();
    this.node   = node;
    this.store  = store;
    this.cfg    = { ...DEFAULT_ROUTER_CFG, ...cfg };
    this.bucket = new TokenBucket(this.cfg.maxTokens, this.cfg.refillRatePerMs);
    this.routes = new RouteTable();
  }

  start(): void {
    if (this.live) throw new LifecycleError("MeshRouter already started");
    this.live = true;

    // RTT measurement: record dispatch time before probe, match on syncRes.
    this.node.on("syncRes", (env) => this.onSyncRes(env));

    // Evict route entries when a peer dies — stale RTT scores cause bad routing.
    this.node.on("peer:down", (entry) => this.routes.remove(nodeIdToHex(entry.id)));
  }

  stop(): void {
    this.live = false;
  }

  // Rate-limited delta publish. Returns true if the message was sent.
  // Returns false and emits backpressure:full when the bucket is exhausted.
  // HEARTBEAT / SYNC traffic bypasses this — callers use Node.publishDelta
  // directly for liveness-critical messages.
  async tryPublishDelta(payload: DeltaPayload): Promise<boolean> {
    if (!this.bucket.consume()) {
      this.setBackpressure(BackpressureLevel.FULL);
      return false;
    }
    this.assessWarn();
    await this.node.publishDelta(payload);
    return true;
  }

  // Send a timed SYNC_REQ probe to a specific peer.
  // Records hrtime before dispatch so onSyncRes can compute RTT.
  async probe(peer: PeerEntry, ns: string, since: bigint): Promise<void> {
    if (peer.state !== PeerState.ALIVE) return;
    const peerId = nodeIdToHex(peer.id);
    this.probeNs.set(peerId, process.hrtime.bigint());
    try {
      this.node.dialRouter(peer.routerAddr);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EINVAL") throw err;
    }
    const payload: SyncReqPayload = { ns, since };
    await this.node.rpcSend(MessageType.SYNC_REQ, payload);
  }

  // Peer with the highest route score (lowest observed RTT).
  // Falls back to any ALIVE peer if the route table is empty.
  bestPeer(): PeerEntry | undefined {
    const best = this.routes.best();

    for (const peer of this.node.peers) {
      if (peer.state !== PeerState.ALIVE) continue;
      if (!best) return peer;
      if (nodeIdToHex(peer.id) === best.peerId) return peer;
    }

    return undefined;
  }

  // Sorted list of peers by score — used by GossipEngine for ranked selection.
  rankedPeers(): PeerEntry[] {
    const scored: Array<[PeerEntry, number]> = [];

    for (const peer of this.node.peers) {
      if (peer.state !== PeerState.ALIVE) continue;
      const entry = this.routes.get(nodeIdToHex(peer.id));
      scored.push([peer, entry?.score ?? 0.5]);
    }

    return scored
      .sort(([, a], [, b]) => b - a)
      .map(([p]) => p);
  }

  get backpressureLevel(): BackpressureLevel { return this._bp; }
  get tokenFillRatio():    number            { return this.bucket.fillRatio; }
  get tokensAvailable():   number            { return this.bucket.available; }

  routeScores(): Map<string, RouteEntry> {
    const m = new Map<string, RouteEntry>();
    for (const e of this.routes.all()) m.set(e.peerId, e);
    return m;
  }

  // ── Private ──────────────────────────────────────────────────────────────────

  private onSyncRes(env: Envelope): void {
    const peerId  = nodeIdToHex(env.senderId);
    const sentNs  = this.probeNs.get(peerId);
    if (!sentNs) return;

    this.probeNs.delete(peerId);
    const rttMs = Number(process.hrtime.bigint() - sentNs) / 1_000_000;
    const entry = this.routes.recordRtt(peerId, rttMs);
    this.emit("rtt:sample", peerId, rttMs, entry.ewmaRttMs);
  }

  private assessWarn(): void {
    if (this.bucket.fillRatio < this.cfg.warnThreshold) {
      this.setBackpressure(BackpressureLevel.WARN);
    } else if (this._bp === BackpressureLevel.WARN) {
      this.setBackpressure(BackpressureLevel.NONE);
    }
  }

  private setBackpressure(level: BackpressureLevel): void {
    if (level === this._bp) return;
    const prev = this._bp;
    this._bp   = level;

    if (level === BackpressureLevel.WARN) {
      this.emit("backpressure:warn", level, this.bucket.fillRatio);
    } else if (level === BackpressureLevel.FULL) {
      this.emit("backpressure:full", level);
    } else if (prev !== BackpressureLevel.NONE) {
      this.emit("backpressure:clear", level);
    }
  }
}
