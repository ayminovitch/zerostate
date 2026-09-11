import { Node }          from "./core/node/index.js";
import { StateStore }     from "./core/crdt/index.js";
import { MeshRouter, BackpressureLevel } from "./core/router/index.js";
import { mkNodeId, nodeIdToHex } from "./core/transport/index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";

const enc = new TextEncoder();
const key = (s: string) => enc.encode(s);
const val = (s: string) => enc.encode(s);

import type { RouterCfg } from "./core/router/index.js";

function makeStack(offset: number, routerCfg?: Partial<RouterCfg>) {
  const nodeId = mkNodeId();
  const cfg = {
    nodeId,
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
    hbIntervalMs: 200, suspectMs: 600, deadMs: 1200,
  };
  const node   = new Node(cfg);
  const store  = new StateStore(node);
  const router = new MeshRouter(node, store, routerCfg);
  return { node, store, router, cfg };
}

async function waitPeerUp(observer: Node, target: Uint8Array): Promise<void> {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: peer:up")), 3_000);
    observer.on("peer:up", (e) => {
      if (nodeIdToHex(e.id) !== nodeIdToHex(target as Parameters<typeof nodeIdToHex>[0])) return;
      clearTimeout(t); res();
    });
  });
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState MeshRouter — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  // Tiny bucket so we can trigger back-pressure in tests without flooding
  const A = makeStack(9,  { maxTokens: 10, refillRatePerMs: 0.5, warnThreshold: 0.30 });
  const B = makeStack(10);

  A.node.on("fatal", (e) => console.error("A FATAL:", e));
  B.node.on("fatal", (e) => console.error("B FATAL:", e));

  await Promise.all([A.node.start(), B.node.start()]);
  A.store.start(); B.store.start();
  A.router.start(); B.router.start();

  await A.node.connect(B.cfg.pubAddr, B.cfg.routerAddr);
  await B.node.connect(A.cfg.pubAddr, A.cfg.routerAddr);
  await Promise.all([
    waitPeerUp(A.node, B.cfg.nodeId),
    waitPeerUp(B.node, A.cfg.nodeId),
  ]);
  console.log("peers up\n");

  // ── Test 1: successful rate-limited sends ──────────────────────────────────
  console.log("── Test 1: Rate-limited delta publishing ───────────────────────");
  let sent = 0;
  for (let i = 0; i < 8; i++) {
    const ok = await A.router.tryPublishDelta({
      ns:        "test:ns",
      mutations: [[key(`k${i}`), val(`v${i}`), Date.now()]],
    });
    if (ok) sent++;
  }
  console.log(`✅ sent ${sent}/8 deltas  tokens left=${A.router.tokensAvailable}  ratio=${A.router.tokenFillRatio.toFixed(2)}`);

  // ── Test 2: back-pressure WARN then FULL ─────────────────────────────────
  console.log("\n── Test 2: Back-pressure detection ─────────────────────────────");
  const events: string[] = [];
  A.router.on("backpressure:warn",  () => events.push("WARN"));
  A.router.on("backpressure:full",  () => events.push("FULL"));
  A.router.on("backpressure:clear", () => events.push("CLEAR"));

  // Drain remaining tokens
  let dropped = 0;
  for (let i = 0; i < 20; i++) {
    const ok = await A.router.tryPublishDelta({
      ns:        "test:ns",
      mutations: [[key(`drain${i}`), val("x"), Date.now()]],
    });
    if (!ok) dropped++;
  }
  console.log(`✅ backpressure events: [${events.join(", ")}]`);
  console.log(`✅ dropped ${dropped} messages at FULL  level=${BackpressureLevel[A.router.backpressureLevel]}`);

  // ── Test 3: RTT probe and route scoring ──────────────────────────────────
  console.log("\n── Test 3: RTT probing and route scoring ────────────────────────");
  const rttSamples: number[] = [];
  B.router.on("rtt:sample", (pid, rttMs, ewma) => {
    rttSamples.push(rttMs);
    console.log(`   RTT sample: ${rttMs.toFixed(2)}ms  ewma=${ewma.toFixed(2)}ms  peer=${pid.slice(0, 8)}…`);
  });

  // Send 3 probes from B → A to build up RTT samples
  const alivePeers = Array.from(B.node.peers);
  const peerA = alivePeers[0];
  if (peerA) {
    for (const ns of ["probe:ns"]) {
      await B.router.probe(peerA, ns, 0n);
      await new Promise<void>((r) => setTimeout(r, 100));
      await B.router.probe(peerA, ns, 0n);
      await new Promise<void>((r) => setTimeout(r, 100));
      await B.router.probe(peerA, ns, 0n);
      await new Promise<void>((r) => setTimeout(r, 200));
    }
  }

  console.log(`✅ RTT samples collected: ${rttSamples.length}`);
  console.log("   Route scores:", Object.fromEntries(
    [...B.router.routeScores().entries()].map(([k, v]) => [k.slice(0, 8), v.score.toFixed(1)])
  ));

  const best = B.router.bestPeer();
  console.log(`✅ bestPeer: ${best ? nodeIdToHex(best.id).slice(0, 8) + "…" : "none"}`);

  // ── Stats ─────────────────────────────────────────────────────────────────
  console.log("\n── Stats ────────────────────────────────────────────────────────");
  console.log(`A tokens: ${A.router.tokensAvailable}/${10}  bp=${BackpressureLevel[A.router.backpressureLevel]}`);
  console.log(`B tokens: ${B.router.tokensAvailable}  bp=${BackpressureLevel[B.router.backpressureLevel]}`);
  console.log("A transport:", A.node.transportStats);

  A.router.stop(); B.router.stop();
  A.store.stop();  B.store.stop();
  await Promise.all([A.node.stop(), B.node.stop()]);
  console.log("\n✅ all passed\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
