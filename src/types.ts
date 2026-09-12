import type { NodeId }        from "./core/transport/types.js";
import type { KeyStore }      from "./core/security/key-store.js";
import type { GossipCfg }    from "./core/sync/types.js";
import type { RouterCfg, BackpressureLevel, RouteEntry } from "./core/router/types.js";
import type { TransportStats } from "./core/transport/types.js";
import type { PeerEntry, PeerState } from "./core/node/types.js";

/**
 * Configuration for a ZeroState mesh node.
 */
export interface ZeroStateCfg {
  /** TCP address this node's PUB socket binds to. e.g. `"tcp://0.0.0.0:5551"` */
  pubAddr:    string;
  /** TCP address this node's ROUTER socket binds to. e.g. `"tcp://0.0.0.0:6551"` */
  routerAddr: string;
  /**
   * Pre-generated node identity (32-byte branded Uint8Array).
   * Generated automatically from `crypto.randomBytes(32)` if omitted.
   */
  nodeId?:    NodeId;
  /**
   * CURVE security configuration.
   * Omit for plaintext — **development only**. All production deployments
   * must provide a `KeyStore` to enable ZAP authentication and CURVE encryption.
   */
  security?:  { keyStore: KeyStore };
  /** Gossip anti-entropy protocol tuning. */
  gossip?:    Partial<GossipCfg>;
  /** Back-pressure and routing tuning. */
  router?:    Partial<RouterCfg>;
  /** Liveness detection tuning. */
  heartbeat?: {
    /** Heartbeat broadcast interval in ms. Default: 1000. */
    intervalMs?: number;
    /** No heartbeat within this window → SUSPECT. Default: 3000. */
    suspectMs?:  number;
    /** No heartbeat after SUSPECT within this window → DEAD. Default: 6000. */
    deadMs?:     number;
  };
}

/**
 * Events emitted by a `ZeroState` instance.
 * @example
 * ```ts
 * node.on("change", (ns, key, value) => console.log(ns, key, value));
 * node.on("peer:up", (peer) => console.log("joined:", peer.routerAddr));
 * ```
 */
export interface ZeroStateEvents {
  /**
   * A state mutation was applied — either from a remote delta or a local `set`/`delete`.
   * `value` is `null` for tombstones (deletions).
   */
  change:              [ns: string, key: Uint8Array, value: unknown];
  /** A peer transitioned to ALIVE (first heartbeat received). */
  "peer:up":           [peer: PeerEntry];
  /** A peer missed a heartbeat window — now SUSPECT. */
  "peer:suspect":      [peer: PeerEntry];
  /** A peer was evicted from the registry after timeout. */
  "peer:down":         [peer: PeerEntry];
  /** Token bucket below {@link RouterCfg.warnThreshold}. */
  "backpressure:warn": [fillRatio: number];
  /** Token bucket exhausted — outbound delta messages are being dropped. */
  "backpressure:full": [];
  /** Back-pressure cleared after previously WARN or FULL. */
  "backpressure:clear": [];
  /** Fatal or unrecoverable internal error. */
  error:               [err: Error];
}

/**
 * Runtime statistics snapshot from all internal subsystems.
 * Retrieved synchronously via {@link ZeroState.stats}.
 */
export interface ZeroStateStats {
  transport: TransportStats;
  crdt: {
    namespaces: number;
    totalKeys:  number;
    tombstones: number;
    maxStoreSeq: bigint;
  };
  gossip: {
    currentIntervalMs: number;
  };
  router: {
    backpressureLevel: BackpressureLevel;
    tokenFillRatio:    number;
    tokensAvailable:   number;
    routeScores:       Map<string, RouteEntry>;
  };
  peers: {
    alive:   number;
    suspect: number;
    dead:    number;
    total:   number;
  };
}

export type { NodeId, KeyStore, GossipCfg, RouterCfg, BackpressureLevel, RouteEntry,
              TransportStats, PeerEntry, PeerState };
