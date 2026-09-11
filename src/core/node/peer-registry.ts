import { EventEmitter } from "node:events";
import type { NodeId } from "../transport/index.js";
import { nodeIdToHex } from "../transport/index.js";
import type { PeerEntry } from "./types.js";
import { PeerState } from "./types.js";
import { NodeError } from "../../errors.js";

export interface RegistryEvents {
  "state:change": [entry: PeerEntry, prev: PeerState];
}

export class PeerRegistry extends EventEmitter<RegistryEvents> {
  // Keyed on hex(nodeId) — stable string keys for a Map are faster than
  // binary comparisons on Uint8Array at the cost of one hex encode per op.
  private readonly peers = new Map<string, PeerEntry>();

  add(entry: PeerEntry): void {
    const key = nodeIdToHex(entry.id);
    if (this.peers.has(key)) throw new NodeError(`peer ${key} already registered`);
    this.peers.set(key, entry);
  }

  remove(id: NodeId): boolean {
    return this.peers.delete(nodeIdToHex(id));
  }

  get(id: NodeId): PeerEntry | undefined {
    return this.peers.get(nodeIdToHex(id));
  }

  has(id: NodeId): boolean {
    return this.peers.has(nodeIdToHex(id));
  }

  all(): IterableIterator<PeerEntry> {
    return this.peers.values();
  }

  size(): number {
    return this.peers.size;
  }

  transition(id: NodeId, next: PeerState): void {
    const key   = nodeIdToHex(id);
    const entry = this.peers.get(key);
    if (!entry) throw new NodeError(`transition: unknown peer ${key}`);

    const prev  = entry.state;
    if (prev === next) return;

    entry.state = next;

    if (next === PeerState.DEAD) {
      this.peers.delete(key);
    }

    this.emit("state:change", entry, prev);
  }

  // Apply a received heartbeat payload to an existing peer entry.
  // Returns the instantaneous clock skew (ms) before EWMA smoothing
  // so the caller can decide whether to log anomalies.
  applyHeartbeat(
    id: NodeId,
    logicalClock: bigint,
    peerWallMs: number,
  ): number {
    const entry = this.peers.get(nodeIdToHex(id));
    if (!entry) throw new NodeError(`applyHeartbeat: unknown peer ${nodeIdToHex(id)}`);

    const instant = Date.now() - peerWallMs;
    // α = 0.125 matches TCP's RTT smoothing. Converges in ~8 samples,
    // robust to single-sample spikes from GC or scheduling jitter.
    entry.clockSkewMs  = entry.clockSkewMs === 0
      ? instant
      : 0.875 * entry.clockSkewMs + 0.125 * instant;
    entry.logicalClock = logicalClock > entry.logicalClock ? logicalClock : entry.logicalClock;
    entry.lastHbNs     = process.hrtime.bigint();

    return instant;
  }
}
