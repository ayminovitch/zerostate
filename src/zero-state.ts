import { EventEmitter }  from "node:events";
import { Node }           from "./core/node/index.js";
import { StateStore }     from "./core/crdt/state-store.js";
import { GossipEngine }   from "./core/sync/gossip.js";
import { MeshRouter }     from "./core/router/mesh-router.js";
import { BackpressureLevel } from "./core/router/types.js";
import { decodePayload }  from "./core/transport/index.js";
import type { DeltaPayload } from "./core/transport/types.js";
import { LifecycleError } from "./errors.js";
import type {
  ZeroStateCfg, ZeroStateEvents, ZeroStateStats,
} from "./types.js";

const enc = new TextEncoder();

/**
 * A ZeroState mesh node — the primary entry point for all SDK consumers.
 *
 * Manages the full lifecycle of the transport, CRDT store, gossip engine,
 * and mesh router behind a single, stable API surface.
 *
 * @example
 * ```ts
 * import { ZeroState, generateKeypair, KeyStore } from "zerostate";
 *
 * const kp = generateKeypair();
 * const ks = new KeyStore(kp).allow(peerPublicKey);
 *
 * const node = new ZeroState({
 *   pubAddr:    "tcp://0.0.0.0:5551",
 *   routerAddr: "tcp://0.0.0.0:6551",
 *   security:   { keyStore: ks },
 * });
 *
 * await node.start();
 * await node.connect("tcp://peer:5552", "tcp://peer:6552", peerKey);
 *
 * await node.set("game:state", "player:1", { x: 10, y: 20 });
 * const pos = node.get("game:state", "player:1");
 *
 * node.on("change", (ns, key, value) => console.log(ns, key, value));
 *
 * await node.stop();
 * ```
 */
export class ZeroState extends EventEmitter<ZeroStateEvents> {
  private readonly _node:   Node;
  private readonly _store:  StateStore;
  private readonly _gossip: GossipEngine;
  private readonly _router: MeshRouter;

  private live    = false;
  private closing = false;

  private readonly _pubKey: string | undefined;

  constructor(cfg: ZeroStateCfg) {
    super();
    this._pubKey = cfg.security?.keyStore.publicKey;
    this._node = new Node({
      pubAddr:    cfg.pubAddr,
      routerAddr: cfg.routerAddr,
      ...(cfg.nodeId   ? { nodeId:       cfg.nodeId }                   : {}),
      ...(cfg.security ? { security:     cfg.security }                 : {}),
      ...(cfg.heartbeat?.intervalMs !== undefined ? { hbIntervalMs: cfg.heartbeat.intervalMs } : {}),
      ...(cfg.heartbeat?.suspectMs  !== undefined ? { suspectMs:    cfg.heartbeat.suspectMs  } : {}),
      ...(cfg.heartbeat?.deadMs     !== undefined ? { deadMs:       cfg.heartbeat.deadMs     } : {}),
    });

    this._store  = new StateStore(this._node);
    this._gossip = new GossipEngine(this._node, this._store, cfg.gossip);
    this._router = new MeshRouter(this._node, this._store, cfg.router);

    this.wirePeerEvents();
    this.wireStateEvents();
    this.wireBackpressureEvents();
  }

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  /**
   * Start the node — binds all sockets, starts the gossip engine and router.
   * Must be called before `connect()`, `set()`, or `get()`.
   */
  async start(): Promise<void> {
    if (this.live)    throw new LifecycleError("ZeroState already started");
    if (this.closing) throw new LifecycleError("ZeroState is shutting down");

    await this._node.start();
    this._store.start();
    this._gossip.start();
    this._router.start();
    this.live = true;
  }

  /**
   * Gracefully shut down: drains in-flight messages, stops gossip, closes sockets.
   */
  async stop(): Promise<void> {
    if (this.closing) return;
    this.closing = true;

    this._gossip.stop();
    this._router.stop();
    this._store.stop();
    await this._node.stop();
    this.live = false;
  }

  // ── Peer management ─────────────────────────────────────────────────────────

  /**
   * Connect to a peer node.
   *
   * @param pubAddr    - The peer's PUB socket address. e.g. `"tcp://10.0.0.2:5551"`
   * @param routerAddr - The peer's ROUTER socket address. e.g. `"tcp://10.0.0.2:6551"`
   * @param peerKey    - The peer's Z85-encoded CURVE public key. Required when
   *                     security is configured on this node.
   */
  async connect(pubAddr: string, routerAddr: string, peerKey?: string): Promise<void> {
    this.assertLive("connect");
    await this._node.connect(pubAddr, routerAddr, peerKey);
  }

  /**
   * The Z85 CURVE public key of this node's transport.
   * Distribute this to peers so they can authenticate connections to you.
   * Returns `undefined` if security is not configured.
   */
  get publicKey(): string | undefined {
    return this._pubKey;
  }

  // ── State operations ─────────────────────────────────────────────────────────

  /**
   * Write a value into the mesh. The value is JSON-serialized and broadcast
   * as a rate-limited DELTA to all connected peers.
   *
   * @param ns    - Namespace (logical partition). e.g. `"game:positions"`
   * @param key   - Key within the namespace. String keys are UTF-8 encoded.
   * @param value - Any JSON-serializable value.
   * @param ts    - Optional wall-clock timestamp (ms). Defaults to `Date.now()`.
   * @returns `true` if the delta was published, `false` if back-pressure dropped it.
   */
  async set(ns: string, key: string | Uint8Array, value: unknown, ts?: number): Promise<boolean> {
    this.assertLive("set");
    const k   = toKey(key);
    const v   = this.serialize(value);
    const row = [k, v, ts ?? Date.now()] as [Uint8Array, Uint8Array, number];

    await this._store.mutate(ns, k, v, ts);
    this.emit("change", ns, k, value);

    return this._router.tryPublishDelta({ ns, mutations: [row] });
  }

  /**
   * Write a raw binary value — bypasses JSON serialization.
   * Change events for this mutation will carry a `Uint8Array` as the value.
   */
  async setRaw(
    ns: string, key: Uint8Array, value: Uint8Array | null, ts?: number,
  ): Promise<boolean> {
    this.assertLive("setRaw");
    await this._store.mutate(ns, key, value, ts);
    this.emit("change", ns, key, value);
    return this._router.tryPublishDelta({
      ns,
      mutations: [[key, value, ts ?? Date.now()]],
    });
  }

  /**
   * Read the current value for a key, deserialized from JSON.
   *
   * @returns The stored value, `null` if the key was deleted (tombstone),
   *          or `undefined` if the key has never been set.
   */
  get<T = unknown>(ns: string, key: string | Uint8Array): T | null | undefined {
    const raw = this._store.get(ns, toKey(key));
    if (raw === undefined) return undefined;
    if (raw === null)      return null;
    return this.deserialize(raw) as T;
  }

  /**
   * Read the raw binary value without JSON deserialization.
   */
  getRaw(ns: string, key: Uint8Array): Uint8Array | null | undefined {
    return this._store.get(ns, key);
  }

  /**
   * Delete a key. Writes an LWW tombstone that propagates to all peers.
   * Tombstoned keys return `null` from `get()` until GC removes them (~30s).
   */
  async delete(ns: string, key: string | Uint8Array): Promise<boolean> {
    this.assertLive("delete");
    const k = toKey(key);
    await this._store.delete(ns, k);
    this.emit("change", ns, k, null);
    return this._router.tryPublishDelta({
      ns,
      mutations: [[k, null, Date.now()]],
    });
  }

  /**
   * Returns `true` if the key exists and is not a tombstone.
   */
  has(ns: string, key: string | Uint8Array): boolean {
    return this._store.has(ns, toKey(key));
  }

  /**
   * Request a full anti-entropy sync from a specific peer for a namespace.
   * Useful for bootstrapping a namespace that may have data predating your connection.
   */
  async requestSync(ns: string, peerRouterAddr: string): Promise<void> {
    this.assertLive("requestSync");
    await this._store.requestSync(ns, peerRouterAddr);
  }

  // ── Observability ────────────────────────────────────────────────────────────

  /**
   * Synchronous snapshot of all runtime metrics.
   */
  get stats(): ZeroStateStats {
    const crdt    = this._store.stats;
    const rScores = this._router.routeScores();

    let alive = 0, suspect = 0, dead = 0;
    for (const p of this._node.peers) {
      if (p.state === 1) alive++;
      else if (p.state === 2) suspect++;
      else if (p.state === 3) dead++;
    }

    return {
      transport: this._node.transportStats,
      crdt: {
        namespaces:  crdt.namespaces,
        totalKeys:   crdt.totalKeys,
        tombstones:  crdt.tombstones,
        maxStoreSeq: crdt.maxStoreSeq,
      },
      gossip: {
        currentIntervalMs: this._gossip.currentIntervalMs,
      },
      router: {
        backpressureLevel: this._router.backpressureLevel,
        tokenFillRatio:    this._router.tokenFillRatio,
        tokensAvailable:   this._router.tokensAvailable,
        routeScores:       rScores,
      },
      peers: { alive, suspect, dead, total: alive + suspect + dead },
    };
  }

  /**
   * The node's unique identity (32-byte branded Uint8Array).
   */
  get nodeId(): Uint8Array { return this._node.id; }

  // ── Private wiring ───────────────────────────────────────────────────────────

  private wirePeerEvents(): void {
    this._node.on("peer:up",      (p) => this.emit("peer:up", p));
    this._node.on("peer:suspect", (p) => this.emit("peer:suspect", p));
    this._node.on("peer:down",    (p) => this.emit("peer:down", p));
    this._node.on("fatal",        (e) => this.emit("error", e));
  }

  private wireStateEvents(): void {
    // Emit "change" for every inbound delta mutation after StateStore has merged it.
    // StateStore wires its listener in store.start() — always before this listener.
    this._node.on("delta", (env) => {
      try {
        const body = decodePayload<DeltaPayload>(env.payload);
        for (const [key, value] of body.mutations) {
          const decoded = value === null ? null : this.tryDeserialize(value);
          this.emit("change", body.ns, key, decoded);
        }
      } catch {
        // Malformed delta — StateStore will have rejected it too.
      }
    });
  }

  private wireBackpressureEvents(): void {
    this._router.on("backpressure:warn",  (_, r) => this.emit("backpressure:warn", r));
    this._router.on("backpressure:full",  ()      => this.emit("backpressure:full"));
    this._router.on("backpressure:clear", ()      => this.emit("backpressure:clear"));
  }

  private serialize(value: unknown): Uint8Array {
    return enc.encode(JSON.stringify(value));
  }

  private deserialize(bytes: Uint8Array): unknown {
    return JSON.parse(Buffer.from(bytes).toString("utf-8"));
  }

  // Soft-fail: returns raw bytes if the value was written with setRaw (non-JSON).
  private tryDeserialize(bytes: Uint8Array): unknown {
    try {
      return this.deserialize(bytes);
    } catch {
      return bytes;
    }
  }

  private assertLive(method: string): void {
    if (!this.live) throw new LifecycleError(`ZeroState.${method}: node is not running`);
  }
}

function toKey(key: string | Uint8Array): Uint8Array {
  return typeof key === "string" ? enc.encode(key) : key;
}
