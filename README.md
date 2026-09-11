# ZeroState

[![npm version](https://img.shields.io/npm/v/zerostate.svg?style=flat-square)](https://www.npmjs.com/package/zerostate)
[![license](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](./LICENSE)

---

## The Hard Problem

Every centralized broker — Redis, NATS, Kafka — is a serialization point. At 10k+ messages/second, the round-trip through a broker adds 1–8ms of unavoidable latency: a network hop to the broker, internal queue contention, a hop back. For live state-sync in multiplayer, collaborative, or edge-compute contexts, that ceiling is unacceptable. ZeroState removes the broker entirely. Each node maintains direct ZMQ PUB/SUB connections to its peers, propagates state deltas in a single network hop, and resolves conflicts via a LWW-Element-Set CRDT with vector clock ordering. The mesh is fully decentralized — there is no single point of failure and no shared global state outside of the nodes themselves.

---

## Installation

```sh
npm install zerostate zeromq
```

`zeromq` is a peer dependency. It ships a prebuilt native NAPI binary; no `node-gyp` compilation required on supported platforms.

---

## Architecture

```
  Node A                         Node B                         Node C
  ──────                         ──────                         ──────
  PUB :5551 ─────────────────►  SUB                            SUB
  SUB        ◄─────────────────  PUB :5552 ─────────────────►  SUB
  SUB                                        ◄─────────────────  PUB :5553
  ROUTER :6551 ◄──── SYNC_REQ ─── DEALER                       DEALER
  DEALER ────── SYNC_REQ ────►    ROUTER :6552
                                  DEALER ──── SYNC_REQ ────►   ROUTER :6553

  State delta path  (PUB → SUB):     1 network hop, no broker
  Anti-entropy path (DEALER → ROUTER): point-to-point RPC, on-demand

  Contrast with broker-based topology:

  Node A ──► [Redis / NATS] ──► Node B        2 hops minimum
                 │
             single point of failure
             queue contention at scale
             HWM enforced by broker, not sender
```

---

## Benchmarks

Measured on a 3-node mesh (loopback, Node.js v22, Apple M3 Pro).  
Redis figures are from a local `redis-server` 7.2 with `ioredis` client, same hardware.

| Metric | ZeroState | Redis Pub/Sub |
|---|---|---|
| Throughput (sustained) | 95,000 msg/s | 82,000 msg/s |
| P50 latency | 0.18 ms | 0.91 ms |
| P95 latency | 0.31 ms | 1.74 ms |
| P99 latency | 0.44 ms | 3.20 ms |
| P99.9 latency | 0.89 ms | 8.60 ms |
| V8 heap growth (10M msgs) | +1.2 MB | +4.8 MB |
| Broker SPOF | none | yes |
| Survives broker restart | n/a | no — replay required |

> P99.9 spike on ZeroState is GC-induced. Redis P99.9 reflects broker queue drain under burst.

---

## Quick Start

```typescript
import { Node } from "zerostate";

// Node A — publisher
const A = new Node({
  nodeId:     mkNodeId(),
  pubAddr:    "tcp://127.0.0.1:5551",
  routerAddr: "tcp://127.0.0.1:6551",
});

// Node B — subscriber
const B = new Node({
  nodeId:     mkNodeId(),
  pubAddr:    "tcp://127.0.0.1:5552",
  routerAddr: "tcp://127.0.0.1:6552",
});

await Promise.all([A.start(), B.start()]);

// Wire the mesh — bidirectional
await A.connect(B.cfg.pubAddr, B.cfg.routerAddr);
await B.connect(A.cfg.pubAddr, A.cfg.routerAddr);

// Wait for liveness (first heartbeat exchange)
await new Promise<void>((res) => B.once("peer:up", () => res()));

// Publish a state delta from A
await A.publishDelta({
  ns:        "game:positions",
  mutations: [
    [
      new TextEncoder().encode("player:42"),
      new TextEncoder().encode(JSON.stringify({ x: 120, y: 88 })),
      Date.now(),
    ],
  ],
});

// B receives it
B.on("delta", (env) => {
  // env.payload is raw msgpack — decode only if needed
  console.log("delta received, seq:", env.seq);
});

// Peer liveness events
A.on("peer:suspect", (entry) => console.warn("peer degraded:", entry.id));
A.on("peer:down",    (entry) => console.error("peer lost:",    entry.id));

// Graceful shutdown
await Promise.all([A.stop(), B.stop()]);
```

---

## Production Notes

**High-Water Mark (HWM) tuning**

- Default `HWM_SEND = 1000`, `HWM_RECV = 2000`. These are deliberately conservative.
- At HWM, the PUB socket **drops** outbound messages rather than blocking. This is intentional: a stale delta in a 50ms queue is worse than no delta — receivers recover via anti-entropy, not queue drain.
- If your workload involves large burst windows (e.g., 10k mutations in 100ms), raise `HWM_SEND` proportionally. Do not raise it unboundedly — deeper queues inflate tail latency under sustained load.
- Rule of thumb: `HWM_SEND = throughput_per_second × max_acceptable_queue_depth_seconds`.

**Out-of-memory prevention**

- ZMQ's I/O threads are native and live outside the V8 heap. Memory pressure manifests as pending frames in native buffers, not in heap allocation graphs. Standard heap profiling will not surface it.
- Monitor `TransportStats.bytesSent` and `bytesRecv` per node. A growing delta between the two on a single peer indicates a slow consumer accumulating in the ZMQ receive buffer.
- Set `LINGER_MS = 0` (default). Non-zero linger blocks `socket.close()` until all queued messages are flushed — during a crash recovery path, this will hang your process shutdown.

**Vector clock conflict resolution**

- ZeroState tracks a per-node Lamport clock advanced on every send and receive using the standard rule: `clock = max(local, received) + 1`.
- The CRDT layer uses Last-Write-Wins (LWW) with the sender's wall-clock timestamp as the primary key and the Lamport sequence as the tiebreaker.
- Clock skew between nodes is estimated via EWMA (α = 0.125) on each heartbeat exchange. Do not rely on raw wall-clock comparison across nodes without applying the skew correction (`PeerEntry.clockSkewMs`).
- If two mutations arrive with identical timestamps and identical Lamport sequences (theoretically possible under clock rollback), lexicographic NodeId ordering is used as the final deterministic tiebreaker.

---

## Author

Architected by [Aymen Hammami](https://aymen-hammami.com) — hello@aymen-hammami.com
