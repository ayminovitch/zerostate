import { EventEmitter } from "node:events";
import { Transport, decodePayload } from "../transport/index.js";
import { nodeIdToHex } from "../transport/index.js";
import { MessageType } from "../transport/constants.js";
import type { HeartbeatPayload, PeerPayload, DeltaPayload } from "../transport/types.js";
import type { NodeId, Envelope } from "../transport/types.js";
import { mkNodeId } from "../transport/identity.js";
import { PeerRegistry } from "./peer-registry.js";
import { HeartbeatManager } from "./heartbeat.js";
import { PeerState } from "./types.js";
import type { NodeCfg, PeerEntry } from "./types.js";
import { LifecycleError, NodeError } from "../../errors.js";

export interface NodeEvents {
  // Emitted when a peer transitions CONNECTING → ALIVE (first heartbeat).
  "peer:up":      [entry: PeerEntry];
  // Emitted when ALIVE → SUSPECT (missed heartbeat window).
  "peer:suspect": [entry: PeerEntry];
  // Emitted when SUSPECT → DEAD (evicted). Transport disconnects automatically.
  "peer:down":    [entry: PeerEntry];
  // Delta envelope forwarded from transport — consumers attach here.
  "delta":        [env: Envelope];
  // Emitted when a sync request arrives — caller must call respond().
  "syncReq":      [env: Envelope, respond: (body: unknown) => Promise<void>];
  "fatal":        [err: Error];
}

export class Node extends EventEmitter<NodeEvents> {
  readonly id:       NodeId;
  private readonly transport: Transport;
  private readonly reg:       PeerRegistry;
  private readonly hb:        HeartbeatManager;
  private readonly cfg:       NodeCfg;

  // Monotonically increasing logical clock (Lamport). Incremented on every
  // send and on every received message with a higher clock value.
  private clock: bigint = 0n;
  // Approximate total key count — updated by the CRDT layer via setKeyCount().
  private keyCount = 0;

  private live    = false;
  private closing = false;

  constructor(cfg: NodeCfg) {
    super();
    this.cfg       = cfg;
    this.id        = cfg.nodeId ?? mkNodeId();
    this.transport = new Transport({ ...cfg, nodeId: this.id });
    this.reg       = new PeerRegistry();

    this.hb = new HeartbeatManager(this.transport, this.reg, {
      ...(cfg.hbIntervalMs !== undefined ? { hbIntervalMs: cfg.hbIntervalMs } : {}),
      ...(cfg.suspectMs    !== undefined ? { suspectMs:    cfg.suspectMs }    : {}),
      ...(cfg.deadMs       !== undefined ? { deadMs:       cfg.deadMs }       : {}),
      keyCount:     () => this.keyCount,
      logicalClock: () => this.clock,
    });

    this.reg.on("state:change", (entry, prev) => this.onStateChange(entry, prev));
    this.wireTransport();
  }

  async start(): Promise<void> {
    if (this.live)    throw new LifecycleError("node already started");
    if (this.closing) throw new LifecycleError("node is shutting down");

    await this.transport.start();
    this.live = true;

    // Announce ourselves to any already-connected peers.
    await this.announceJoin();
    this.hb.start();
  }

  async stop(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    this.live    = false;

    this.hb.stop();
    try { await this.announceLeave(); } catch { /* best-effort */ }
    await this.transport.stop();
  }

  // Simulates a crash — no LEAVE announcement. Peers detect failure
  // via liveness timeout (ALIVE → SUSPECT → DEAD).
  async crash(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    this.live    = false;
    this.hb.stop();
    await this.transport.stop();
  }

  // Connect to a peer by providing its PUB and ROUTER addresses.
  // This is the primary peer-discovery entry point; in a full cluster
  // the discovery layer calls this on bootstrap (Step 6).
  async connect(pubAddr: string, routerAddr: string): Promise<void> {
    this.assertLive("connect");
    this.transport.subscribeTo(pubAddr);
    this.transport.dialRouter(routerAddr);
    // We don't add the peer to the registry yet. We wait for their JOIN
    // or first heartbeat — that's when we learn their NodeId.
  }

  async disconnect(id: NodeId): Promise<void> {
    const entry = this.reg.get(id);
    if (!entry) throw new NodeError(`disconnect: unknown peer ${nodeIdToHex(id)}`);
    this.transport.unsubscribeFrom(entry.pubAddr);
    this.reg.remove(id);
  }

  // Called by the CRDT layer after every state mutation so heartbeats
  // carry an accurate key count for anti-entropy prioritisation.
  setKeyCount(n: number): void {
    this.keyCount = n;
  }

  get peers(): IterableIterator<PeerEntry> {
    return this.reg.all();
  }

  get peerCount(): number {
    return this.reg.size();
  }

  get logicalClock(): bigint {
    return this.clock;
  }

  get transportStats() {
    return this.transport.stats;
  }

  // Publish a delta to all subscribers. Advances the logical clock.
  async publishDelta(payload: DeltaPayload): Promise<bigint> {
    this.assertLive("publishDelta");
    this.tickClock();
    return this.transport.publish(MessageType.DELTA, payload);
  }

  // ── Private: Transport wiring ──────────────────────────────────────────────

  private wireTransport(): void {
    this.transport.on("heartbeat", (env) => this.onHeartbeat(env));
    this.transport.on("join",      (env) => this.onJoin(env));
    this.transport.on("leave",     (env) => this.onLeave(env));
    this.transport.on("delta",     (env) => { this.tickClockFromPeer(env.seq); this.emit("delta", env); });
    this.transport.on("syncReq",   (env, respond) => { this.tickClockFromPeer(env.seq); this.emit("syncReq", env, respond); });
    this.transport.on("syncRes",   (env) => { this.tickClockFromPeer(env.seq); });
    this.transport.on("fatal",     (err) => this.emit("fatal", err));
  }

  private onHeartbeat(env: Envelope): void {
    const body = decodePayload<HeartbeatPayload>(env.payload);
    this.tickClockFromPeer(body.clock);

    if (!this.reg.has(env.senderId)) {
      // Heartbeat from an unknown peer — they may have joined before we
      // connected, or their JOIN was lost. Add them optimistically.
      // We don't have their routerAddr yet; it arrives via JOIN payload.
      this.reg.add({
        id:          env.senderId,
        pubAddr:     "",
        routerAddr:  "",
        state:       PeerState.CONNECTING,
        lastHbNs:    process.hrtime.bigint(),
        logicalClock: body.clock,
        clockSkewMs: 0,
      });
    }

    this.reg.applyHeartbeat(env.senderId, body.clock, body.ts);

    const entry = this.reg.get(env.senderId)!;
    if (entry.state === PeerState.CONNECTING || entry.state === PeerState.SUSPECT) {
      this.reg.transition(env.senderId, PeerState.ALIVE);
    }
  }

  private onJoin(env: Envelope): void {
    const body = decodePayload<PeerPayload>(env.payload);
    this.tickClockFromPeer(env.seq);

    if (this.reg.has(env.senderId)) {
      // Already known — update addresses in case they changed (restart
      // on a different port) and reset to ALIVE.
      const entry     = this.reg.get(env.senderId)!;
      entry.pubAddr    = body.pubAddr;
      entry.routerAddr = body.routerAddr;
      if (entry.state !== PeerState.ALIVE) {
        this.reg.transition(env.senderId, PeerState.ALIVE);
      }
      return;
    }

    this.reg.add({
      id:           env.senderId,
      pubAddr:      body.pubAddr,
      routerAddr:   body.routerAddr,
      state:        PeerState.CONNECTING,
      lastHbNs:     process.hrtime.bigint(),
      logicalClock: 0n,
      clockSkewMs:  0,
    });
  }

  private onLeave(env: Envelope): void {
    if (!this.reg.has(env.senderId)) return;
    const entry = this.reg.get(env.senderId);
    this.reg.transition(env.senderId, PeerState.DEAD);
    if (entry?.pubAddr) this.transport.unsubscribeFrom(entry.pubAddr);
  }

  private onStateChange(entry: PeerEntry, prev: PeerState): void {
    switch (entry.state) {
      case PeerState.ALIVE:
        this.emit("peer:up", entry);
        break;
      case PeerState.SUSPECT:
        this.emit("peer:suspect", entry);
        break;
      case PeerState.DEAD:
        // Disconnect before emitting so peerCount is accurate in the handler.
        if (entry.pubAddr) this.transport.unsubscribeFrom(entry.pubAddr);
        this.emit("peer:down", entry);
        break;
    }
    void prev; // suppress unused warning — kept for future audit logging
  }

  private async announceJoin(): Promise<void> {
    const body: PeerPayload = {
      pubAddr:    this.cfg.pubAddr,
      routerAddr: this.cfg.routerAddr,
      ts:         Date.now(),
    };
    this.tickClock();
    await this.transport.publish(MessageType.JOIN, body);
  }

  private async announceLeave(): Promise<void> {
    const body: PeerPayload = {
      pubAddr:    this.cfg.pubAddr,
      routerAddr: this.cfg.routerAddr,
      ts:         Date.now(),
    };
    this.tickClock();
    await this.transport.publish(MessageType.LEAVE, body);
  }

  // ── Logical clock ──────────────────────────────────────────────────────────

  private tickClock(): void {
    this.clock += 1n;
  }

  // Lamport rule: clock = max(local, received) + 1
  private tickClockFromPeer(peerClock: bigint): void {
    if (peerClock > this.clock) this.clock = peerClock;
    this.clock += 1n;
  }

  private assertLive(op: string): void {
    if (!this.live)   throw new LifecycleError(`node not started; call start() before ${op}()`);
    if (this.closing) throw new LifecycleError(`node stopping; cannot call ${op}()`);
  }
}
