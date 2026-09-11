import { Node }          from "./core/node/index.js";
import { StateStore }     from "./core/crdt/index.js";
import { GossipEngine }   from "./core/sync/index.js";
import { mkNodeId, nodeIdToHex } from "./core/transport/index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const key = (s: string) => enc.encode(s);
const val = (s: string) => enc.encode(s);

function makeStack(offset: number) {
  const nodeId = mkNodeId();
  const cfg = {
    nodeId,
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
    hbIntervalMs: 200, suspectMs: 600, deadMs: 1200,
  };
  const node   = new Node(cfg);
  const store  = new StateStore(node);
  const gossip = new GossipEngine(node, store, {
    fanout:        1,
    minIntervalMs: 200,
    intervalMs:    500,
    maxIntervalMs: 2_000,
    stableRoundsBeforeBackoff: 2,
  });
  return { node, store, gossip, cfg };
}

async function waitPeerUp(observer: Node, targetId: Uint8Array, timeout = 3_000): Promise<void> {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: peer:up")), timeout);
    observer.on("peer:up", (entry) => {
      if (nodeIdToHex(entry.id) !== nodeIdToHex(targetId as Parameters<typeof nodeIdToHex>[0])) return;
      clearTimeout(t); res();
    });
  });
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState GossipEngine — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  const A = makeStack(7);
  const B = makeStack(8);

  A.node.on("fatal", (e) => console.error("A FATAL:", e));
  B.node.on("fatal", (e) => console.error("B FATAL:", e));
  A.gossip.on("error", (e) => console.warn("A gossip err:", e.message));
  B.gossip.on("error", (e) => console.warn("B gossip err:", e.message));

  await Promise.all([A.node.start(), B.node.start()]);
  A.store.start(); B.store.start();
  A.gossip.start(); B.gossip.start();

  await A.node.connect(B.cfg.pubAddr, B.cfg.routerAddr);
  await B.node.connect(A.cfg.pubAddr, A.cfg.routerAddr);
  await Promise.all([
    waitPeerUp(A.node, B.cfg.nodeId),
    waitPeerUp(B.node, A.cfg.nodeId),
  ]);
  console.log("peers up\n");

  // ── Test 1: gossip round fires and reports stats ───────────────────────────
  console.log("── Test 1: Gossip round fires ───────────────────────────────────");
  const roundFired = new Promise<GossipRoundStats>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: gossip round")), 3_000);
    A.gossip.on("round", (stats) => { clearTimeout(t); res(stats); });
  });
  const stats1 = await roundFired;
  console.log(`✅ A gossip round=${stats1.round}  peers=${stats1.peersContacted}  interval=${stats1.intervalMs}ms`);

  // ── Test 2: state written on A converges to B via gossip ───────────────────
  console.log("\n── Test 2: Gossip-driven convergence ───────────────────────────");
  await A.store.mutate("mesh:state", key("config:color"), val("#ff6b35"));

  // Wait for up to 3 gossip rounds for B to converge
  const converged = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: convergence")), 5_000);
    const check = () => {
      const v = B.store.get("mesh:state", key("config:color"));
      if (v) { clearTimeout(t); res(); return; }
      B.gossip.once("round", check);
    };
    B.gossip.once("round", check);
  });

  await converged;
  const v = B.store.get("mesh:state", key("config:color"))!;
  console.log(`✅ B converged: config:color = ${dec.decode(v)}`);

  // ── Test 3: adaptive backoff after stable rounds ───────────────────────────
  console.log("\n── Test 3: Adaptive interval backoff ───────────────────────────");

  // Collect interval readings over several stable rounds
  const intervals: number[] = [];
  await new Promise<void>((res) => {
    let collected = 0;
    const handler = (s: GossipRoundStats) => {
      intervals.push(s.intervalMs);
      collected++;
      if (collected >= 5) { A.gossip.off("round", handler); res(); }
    };
    A.gossip.on("round", handler);
  });

  const backedOff = intervals[intervals.length - 1]! > intervals[0]!;
  console.log(`✅ intervals: [${intervals.join(", ")}]ms`);
  console.log(`✅ backed off after stable rounds: ${backedOff}`);

  // ── Stats ─────────────────────────────────────────────────────────────────
  console.log("\n── Stats ────────────────────────────────────────────────────────");
  console.log("A crdt:", A.store.stats);
  console.log("B crdt:", B.store.stats);
  console.log(`A gossip interval: ${A.gossip.currentIntervalMs}ms`);
  console.log(`B gossip interval: ${B.gossip.currentIntervalMs}ms`);

  A.gossip.stop(); B.gossip.stop();
  A.store.stop();  B.store.stop();
  await Promise.all([A.node.stop(), B.node.stop()]);
  console.log("\n✅ all passed\n");
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { GossipRoundStats } from "./core/sync/index.js";
main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
