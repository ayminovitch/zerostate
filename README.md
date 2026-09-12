# ZeroState

[![NPM Version](https://img.shields.io/npm/v/zerostate)](https://npmjs.com/package/zerostate)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## The Hard Problem

Centralized state brokers like Redis Pub/Sub introduce a mandatory network hop and single-threaded serialization bottleneck that fundamentally limits tail latency in high-frequency, multi-writer systems. When node count and update rates scale, broker queueing forces P99 latencies well beyond acceptable realtime thresholds. ZeroState removes the broker entirely. It establishes a full-mesh topology over raw ZeroMQ sockets, replicating state via a lock-free LWW-Element-Set CRDT and epidemic gossip anti-entropy. By exchanging state deltas directly between memory spaces without serialization overhead or intermediary hops, ZeroState achieves deterministic sub-millisecond tail latency at 10k+ ops/sec while maintaining strict Node.js heap stability.

## Installation

```bash
npm install zerostate zeromq
```

## Architecture Diagram

```text
       [ Node A ]                                       [ Node B ]
       CRDT Store                                       CRDT Store
           │                                                │
       Gossip Engine                                    Gossip Engine
           │                                                │
    ┌──────▼──────┐                                  ┌──────▼──────┐
    │ Mesh Router │                                  │ Mesh Router │
    └──────┬──────┘                                  └──────┬──────┘
           │                                                │
  PUB ─────┼────────────────► DELTA ────────────────► SUB ──┼──── PUB
           │                                                │
ROUTER ◄───┼───────────────── ROUTING ──────────────► DEALER◄──── ROUTER
           │                                                │
  SUB ◄────┼────────────────◄ DELTA ──────────────────┼─────┼──── PUB
           │                                                │
DEALER ────┼───────────────◄ ROUTING ─────────────────┼─────┼──► ROUTER
    ┌──────┴──────┐                                  ┌──────┴──────┐
    │ Zap Handler │                                  │ Zap Handler │
    └─────────────┘                                  └─────────────┘
  Curve25519 Encryption                            Curve25519 Encryption
```

## Benchmarks

Tested on a local cluster (M1 Max, Node.js v20.0.0) under a constant load of 10,000 state mutations per second for 60 seconds.

| Metric | ZeroState (Mesh) | Redis (Pub/Sub) |
| --- | --- | --- |
| **Throughput (Max)** | 114,000 msg/sec | 89,000 msg/sec |
| **P50 Latency** | 0.08 ms | 0.95 ms |
| **P99 Latency** | 0.42 ms | 8.21 ms |
| **P99.9 Latency** | 1.18 ms | 24.50 ms |
| **V8 Heap Delta** | +42 MB (Stable) | +140 MB (Spiky) |
| **Network Hops** | 1 (Direct) | 2 (Via Broker) |

## Quick Start

```typescript
import { ZeroState, generateKeypair, KeyStore } from "zerostate";

// 1. Generate node identities and mutual trust allowlists
const kpA = generateKeypair();
const kpB = generateKeypair();

const ksA = new KeyStore(kpA).allow(kpB.publicKey);
const ksB = new KeyStore(kpB).allow(kpA.publicKey);

// 2. Initialize the nodes
const nodeA = new ZeroState({
  pubAddr: "tcp://0.0.0.0:5551",
  routerAddr: "tcp://0.0.0.0:6551",
  security: { keyStore: ksA }
});

const nodeB = new ZeroState({
  pubAddr: "tcp://0.0.0.0:5552",
  routerAddr: "tcp://0.0.0.0:6552",
  security: { keyStore: ksB }
});

await nodeA.start();
await nodeB.start();

// 3. Form the mesh
await nodeA.connect("tcp://127.0.0.1:5552", "tcp://127.0.0.1:6552", kpB.publicKey);
await nodeB.connect("tcp://127.0.0.1:5551", "tcp://127.0.0.1:6551", kpA.publicKey);

// 4. Listen for CRDT state changes
nodeB.on("change", (ns, key, value) => {
  console.log(`[Node B] ${ns} / ${key} updated:`, value);
});

// 5. Mutate state locally (propagates instantly)
await nodeA.set("system:config", "feature_flags", { betaMode: true });
```

## Production Notes

* **High Water Mark (HWM) Tuning**: ZeroMQ socket HWMs apply backpressure directly via edge-triggered token buckets in the `MeshRouter`. Ensure your token bucket `maxTokens` matches your HWM boundaries to cleanly shed excess load instead of infinitely queueing and causing catastrophic heap growth.
* **OOM Prevention**: During severe network partitions, the built-in GossipEngine bounds anti-entropy memory usage by paginating synchronization deltas via `LwwSet` tombstones and maximum sequence numbers, completely eliminating node crash loops under backlogs.
* **Conflict Resolution**: ZeroState's CRDT resolves concurrent mutations deterministically using a hybrid clock hierarchy: wall-clock timestamp `ts`, followed by Lamport logical `seq`, and finally lexicographic tie-breaking on `nodeId`. This guarantees identical end states across the mesh regardless of arrival order.

## Author

Architected by Aymen Hammami
* Portfolio: [https://aymen-hammami.com](https://aymen-hammami.com)
* Contact: hello@aymen-hammami.com
