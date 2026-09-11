import type { Transport, NodeId } from "../transport/index.js";
import { MessageType } from "../transport/index.js";
import type { HeartbeatPayload } from "../transport/types.js";
import type { PeerRegistry } from "./peer-registry.js";
import { PeerState, DEFAULT_HB_INTERVAL_MS, DEFAULT_SUSPECT_MS, DEFAULT_DEAD_MS } from "./types.js";

interface HbCfg {
  hbIntervalMs: number;
  suspectMs:    number;
  deadMs:       number;
  keyCount:     () => number;
  logicalClock: () => bigint;
}

export class HeartbeatManager {
  private readonly transport: Transport;
  private readonly reg:       PeerRegistry;
  private readonly cfg:       HbCfg;

  private broadcastTimer: NodeJS.Timeout | null = null;
  private livenessTimer:  NodeJS.Timeout | null = null;

  constructor(transport: Transport, reg: PeerRegistry, cfg: Partial<HbCfg> & Pick<HbCfg, "keyCount" | "logicalClock">) {
    this.transport = transport;
    this.reg       = reg;
    this.cfg       = {
      hbIntervalMs: cfg.hbIntervalMs ?? DEFAULT_HB_INTERVAL_MS,
      suspectMs:    cfg.suspectMs    ?? DEFAULT_SUSPECT_MS,
      deadMs:       cfg.deadMs       ?? DEFAULT_DEAD_MS,
      keyCount:     cfg.keyCount,
      logicalClock: cfg.logicalClock,
    };
  }

  start(): void {
    // Two separate timers — broadcast and liveness check run independently.
    // Coupling them to a single tick would mean a slow broadcast loop
    // (e.g. blocked on a full HWM queue) delays liveness decisions.
    this.broadcastTimer = setInterval(() => void this.broadcast(), this.cfg.hbIntervalMs);
    this.livenessTimer  = setInterval(() => this.checkLiveness(),  this.cfg.hbIntervalMs);

    this.broadcastTimer.unref();
    this.livenessTimer.unref();
  }

  stop(): void {
    if (this.broadcastTimer) { clearInterval(this.broadcastTimer); this.broadcastTimer = null; }
    if (this.livenessTimer)  { clearInterval(this.livenessTimer);  this.livenessTimer  = null; }
  }

  private async broadcast(): Promise<void> {
    const body: HeartbeatPayload = {
      clock:    this.cfg.logicalClock(),
      keyCount: this.cfg.keyCount(),
      ts:       Date.now(),
    };
    try {
      await this.transport.publish(MessageType.HEARTBEAT, body);
    } catch {
      // Suppress send errors — a failed heartbeat is not fatal.
      // The remote peer will detect our absence via its own liveness check.
    }
  }

  private checkLiveness(): void {
    const nowNs      = process.hrtime.bigint();
    const suspectNs  = BigInt(this.cfg.suspectMs)  * 1_000_000n;
    const deadNs     = BigInt(this.cfg.deadMs)      * 1_000_000n;

    for (const peer of this.reg.all()) {
      const age = nowNs - peer.lastHbNs;

      if (peer.state === PeerState.ALIVE && age > suspectNs) {
        this.reg.transition(peer.id, PeerState.SUSPECT);
        continue;
      }

      // Dead threshold is measured from lastHbNs, not from when we
      // entered SUSPECT. This means suspectMs and deadMs are independent
      // windows from the last good heartbeat, not chained delays.
      if (peer.state === PeerState.SUSPECT && age > deadNs) {
        this.reg.transition(peer.id, PeerState.DEAD);
      }
    }
  }
}
