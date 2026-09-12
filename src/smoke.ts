import { ZeroState, generateKeypair, KeyStore, BackpressureLevel } from "./index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";
import { nodeIdToHex } from "./core/transport/index.js";

function makeNode(offset: number, peerKey?: string) {
  const kp = generateKeypair();
  const ks = new KeyStore(kp);
  if (peerKey) ks.allow(peerKey);
  return {
    node: new ZeroState({
      pubAddr:      `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
      routerAddr:   `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
      security:     { keyStore: ks },
      gossip:       { intervalMs: 300 },
      router:       { maxTokens: 500, refillRatePerMs: 50 },
      heartbeat:    { intervalMs: 200, suspectMs: 600, deadMs: 1_200 },
    }),
    ks,
  };
}

async function waitEvent<T>(
  emitter: ZeroState, event: "peer:up" | "peer:down" | "error", ms = 3_000,
): Promise<T> {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error(`timeout: ${event}`)), ms);
    // Cast to any to bypass EventEmitter strict overload — event set is finite.
    (emitter as NodeJS.EventEmitter).once(event, (...args: unknown[]) => {
      clearTimeout(t); res(args[0] as T);
    });
  });
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState Public API — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  // Build nodes: each allows the other's key upfront
  const kpA = generateKeypair();
  const kpB = generateKeypair();

  const ksA = new KeyStore(kpA).allow(kpB.publicKey);
  const ksB = new KeyStore(kpB).allow(kpA.publicKey);

  const A = new ZeroState({
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + 13}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + 13}`,
    security:   { keyStore: ksA },
    gossip:     { intervalMs: 300 },
    heartbeat:  { intervalMs: 200, suspectMs: 600, deadMs: 1_200 },
  });

  const B = new ZeroState({
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + 14}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + 14}`,
    security:   { keyStore: ksB },
    gossip:     { intervalMs: 300 },
    heartbeat:  { intervalMs: 200, suspectMs: 600, deadMs: 1_200 },
  });

  await A.start(); await B.start();
  console.log(`A publicKey: ${A.publicKey?.slice(0, 10)}…`);
  console.log(`B publicKey: ${B.publicKey?.slice(0, 10)}…\n`);

  // ── Test 1: Peer discovery ─────────────────────────────────────────────────
  console.log("── Test 1: Peer discovery ───────────────────────────────────────");
  const [peerUpA, peerUpB] = await Promise.all([
    waitEvent(A, "peer:up"),
    waitEvent(B, "peer:up"),
    A.connect(
      `tcp://127.0.0.1:${BASE_PUB_PORT    + 14}`,
      `tcp://127.0.0.1:${BASE_ROUTER_PORT + 14}`,
      kpB.publicKey,
    ),
    B.connect(
      `tcp://127.0.0.1:${BASE_PUB_PORT    + 13}`,
      `tcp://127.0.0.1:${BASE_ROUTER_PORT + 13}`,
      kpA.publicKey,
    ),
  ]);
  console.log("✅ peers up");
  console.log(`   A stats.peers: ${JSON.stringify(A.stats.peers)}`);

  // ── Test 2: set() / get() with JSON values ─────────────────────────────────
  console.log("\n── Test 2: set() / get() — JSON values ──────────────────────────");
  await A.set("game:state", "player:1", { x: 42, y: 100, health: 95 });
  const local = A.get<{ x: number }>("game:state", "player:1");
  console.log(`✅ A local get:   ${JSON.stringify(local)}`);

  // Wait for B to receive the change event
  const [ns, , value] = await new Promise<[string, Uint8Array, unknown]>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: change")), 2_000);
    B.on("change", (ns, key, val) => { clearTimeout(t); res([ns, key, val]); });
  });
  console.log(`✅ B change event: ns="${ns}"  value=${JSON.stringify(value)}`);

  const remote = B.get<{ x: number }>("game:state", "player:1");
  console.log(`✅ B get():        ${JSON.stringify(remote)}`);

  // ── Test 3: delete() writes tombstone ─────────────────────────────────────
  console.log("\n── Test 3: delete() — tombstone propagation ─────────────────────");
  await A.delete("game:state", "player:1");
  const deleted = A.get("game:state", "player:1");
  console.log(`✅ A after delete: ${deleted}  (expected: null)`);
  console.log(`✅ A has():        ${A.has("game:state", "player:1")}  (expected: false)`);

  // ── Test 4: stats() snapshot ───────────────────────────────────────────────
  console.log("\n── Test 4: stats() snapshot ─────────────────────────────────────");
  await new Promise<void>((r) => setTimeout(r, 400));
  const s = A.stats;
  console.log(`✅ crdt:       ns=${s.crdt.namespaces}  keys=${s.crdt.totalKeys}  tombstones=${s.crdt.tombstones}`);
  console.log(`✅ gossip:     interval=${s.gossip.currentIntervalMs}ms`);
  console.log(`✅ router:     bp=${BackpressureLevel[s.router.backpressureLevel]}  fill=${s.router.tokenFillRatio.toFixed(2)}`);
  console.log(`✅ peers:      ${JSON.stringify(s.peers)}`);
  console.log(`✅ transport:  sent=${s.transport.sent}  recv=${s.transport.recv}`);

  // ── Test 5: nodeId is a stable Uint8Array ──────────────────────────────────
  console.log("\n── Test 5: nodeId ───────────────────────────────────────────────");
  console.log(`✅ A nodeId: ${nodeIdToHex(A.nodeId as Parameters<typeof nodeIdToHex>[0]).slice(0, 16)}…`);

  await A.stop(); await B.stop();
  console.log("\n✅ all passed\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
