import { Node } from "./core/node/index.js";
import { mkNodeId, nodeIdToHex } from "./core/transport/index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";
import { MessageType } from "./core/transport/constants.js";

function makeCfg(offset: number) {
  return {
    nodeId:     mkNodeId(),
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
    hbIntervalMs: 200,
    suspectMs:    600,
    deadMs:       1200,
  };
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState Node — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  const cfgA = makeCfg(3);
  const cfgB = makeCfg(4);

  const A = new Node(cfgA);
  const B = new Node(cfgB);

  console.log(`A: ${nodeIdToHex(cfgA.nodeId)}`);
  console.log(`B: ${nodeIdToHex(cfgB.nodeId)}\n`);

  // ── Test 1: peer:up fires when first heartbeat arrives ──────────────────────
  const peerUpA = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: A did not see B as peer:up")), 3_000);
    A.on("peer:up", (entry) => {
      if (nodeIdToHex(entry.id) !== nodeIdToHex(cfgB.nodeId)) return;
      clearTimeout(t);
      console.log(`✅ A sees B as ALIVE  skew=${entry.clockSkewMs.toFixed(1)}ms  clock=${entry.logicalClock}`);
      res();
    });
  });

  const peerUpB = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: B did not see A as peer:up")), 3_000);
    B.on("peer:up", (entry) => {
      if (nodeIdToHex(entry.id) !== nodeIdToHex(cfgA.nodeId)) return;
      clearTimeout(t);
      console.log(`✅ B sees A as ALIVE  skew=${entry.clockSkewMs.toFixed(1)}ms  clock=${entry.logicalClock}`);
      res();
    });
  });

  A.on("fatal", (e) => console.error("A FATAL:", e));
  B.on("fatal", (e) => console.error("B FATAL:", e));

  await Promise.all([A.start(), B.start()]);
  console.log("── Test 1: Peer discovery and ALIVE transition ─────────────────");

  // Connect A → B and B → A (bidirectional mesh)
  await A.connect(cfgB.pubAddr, cfgB.routerAddr);
  await B.connect(cfgA.pubAddr, cfgA.routerAddr);

  await Promise.all([peerUpA, peerUpB]);

  console.log(`\n   A peerCount=${A.peerCount}  clock=${A.logicalClock}`);
  console.log(`   B peerCount=${B.peerCount}  clock=${B.logicalClock}`);

  // ── Test 2: Delta propagation via Node.publishDelta ─────────────────────────
  console.log("\n── Test 2: Delta propagation ───────────────────────────────────");
  const deltaRecv = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: delta")), 2_000);
    B.on("delta", (env) => {
      clearTimeout(t);
      console.log(`✅ B recv DELTA  seq=${env.seq}  senderClock tracked`);
      res();
    });
  });

  await A.publishDelta({
    ns:        "test:ns",
    mutations: [[new Uint8Array([0x01]), new Uint8Array([0x41, 0x42]), Date.now()]],
  });
  await deltaRecv;

  // ── Test 3: SUSPECT + DEAD on peer stop ─────────────────────────────────────
  console.log("\n── Test 3: Peer SUSPECT → DEAD on stop ─────────────────────────");
  const suspectFired = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: suspect")), 3_000);
    A.on("peer:suspect", (entry) => {
      clearTimeout(t);
      console.log(`✅ A suspects B  (${nodeIdToHex(entry.id).slice(0, 8)}…)`);
      res();
    });
  });

  const deadFired = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: dead")), 5_000);
    A.on("peer:down", (entry) => {
      clearTimeout(t);
      console.log(`✅ A declares B DEAD  peerCount=${A.peerCount}`);
      res();
    });
  });

  // Stop B without announcing — simulates a crash.
  await B.crash();
  console.log("   B crashed (no LEAVE broadcast)");

  await Promise.all([suspectFired, deadFired]);

  console.log(`\n   A final peerCount=${A.peerCount}  clock=${A.logicalClock}`);

  await A.stop();
  console.log("\n✅ all passed\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
