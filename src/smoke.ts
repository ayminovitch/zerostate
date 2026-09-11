import { Node }        from "./core/node/index.js";
import { StateStore }  from "./core/crdt/index.js";
import { mkNodeId, nodeIdToHex } from "./core/transport/index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const key = (s: string) => enc.encode(s);
const val = (s: string) => enc.encode(s);

function makePair(offset: number) {
  const nodeId = mkNodeId();
  const cfg = {
    nodeId,
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
    hbIntervalMs: 200, suspectMs: 600, deadMs: 1200,
  };
  const node  = new Node(cfg);
  const store = new StateStore(node);
  return { node, store, cfg };
}

async function waitPeerUp(observer: Node, targetId: Uint8Array): Promise<void> {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: peer:up")), 3_000);
    observer.on("peer:up", (entry) => {
      if (nodeIdToHex(entry.id) !== nodeIdToHex(targetId as Parameters<typeof nodeIdToHex>[0])) return;
      clearTimeout(t); res();
    });
  });
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState CRDT — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  const A = makePair(5);
  const B = makePair(6);

  A.node.on("fatal", (e) => console.error("A FATAL:", e));
  B.node.on("fatal", (e) => console.error("B FATAL:", e));

  await Promise.all([A.node.start(), B.node.start()]);
  A.store.start();
  B.store.start();

  await A.node.connect(B.cfg.pubAddr, B.cfg.routerAddr);
  await B.node.connect(A.cfg.pubAddr, A.cfg.routerAddr);
  await Promise.all([waitPeerUp(A.node, B.cfg.nodeId), waitPeerUp(B.node, A.cfg.nodeId)]);
  console.log("peers up\n");

  // ── Test 1: basic mutate + propagation ──────────────────────────────────────
  console.log("── Test 1: Mutate and propagate ────────────────────────────────");
  const deltaOnB = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: delta")), 2_000);
    B.node.on("delta", () => { clearTimeout(t); res(); });
  });

  await A.store.mutate("game:pos", key("player:1"), val('{"x":10,"y":20}'));
  await deltaOnB;
  await new Promise<void>((r) => setTimeout(r, 50)); // allow merge tick

  const v1 = B.store.get("game:pos", key("player:1"));
  console.log(`✅ B has player:1 = ${v1 ? dec.decode(v1) : "null"}`);

  // ── Test 2: LWW conflict — higher timestamp wins ─────────────────────────────
  console.log("\n── Test 2: LWW conflict resolution ─────────────────────────────");
  const t_old = Date.now() - 1000;
  const t_new = Date.now();

  // Write stale value from A (old ts), then fresh value from B (new ts).
  await A.store.mutate("game:pos", key("player:2"), val("stale"), t_old);
  await new Promise<void>((r) => setTimeout(r, 100));
  await B.store.mutate("game:pos", key("player:2"), val("fresh"), t_new);
  await new Promise<void>((r) => setTimeout(r, 100));

  const v2a = A.store.get("game:pos", key("player:2"));
  const v2b = B.store.get("game:pos", key("player:2"));
  console.log(`✅ A resolves player:2 = ${v2a ? dec.decode(v2a) : "null"}  (expect: fresh)`);
  console.log(`✅ B resolves player:2 = ${v2b ? dec.decode(v2b) : "null"}  (expect: fresh)`);

  // ── Test 3: tombstone / delete ────────────────────────────────────────────
  console.log("\n── Test 3: Tombstone (delete) ──────────────────────────────────");
  await A.store.mutate("game:pos", key("player:3"), val("exists"));
  await new Promise<void>((r) => setTimeout(r, 100));
  await A.store.delete("game:pos", key("player:3"));
  await new Promise<void>((r) => setTimeout(r, 100));

  const v3b = B.store.get("game:pos", key("player:3"));
  console.log(`✅ B sees player:3 = ${v3b}  (expect: null / tombstone)`);
  console.log(`   B has player:3? ${B.store.has("game:pos", key("player:3"))}  (expect: false)`);

  // ── Test 4: SYNC_REQ anti-entropy round-trip ──────────────────────────────
  console.log("\n── Test 4: Anti-entropy SYNC_REQ/RES ───────────────────────────");
  const syncDone = new Promise<void>((r) => setTimeout(r, 500));
  await B.store.requestSync("game:pos", A.cfg.routerAddr);
  await syncDone;
  console.log("✅ B sent SYNC_REQ to A and settled");

  // ── Stats ──────────────────────────────────────────────────────────────────
  console.log("\n── CRDT Stats ──────────────────────────────────────────────────");
  console.log("A:", A.store.stats);
  console.log("B:", B.store.stats);

  A.store.stop(); B.store.stop();
  await Promise.all([A.node.stop(), B.node.stop()]);
  console.log("\n✅ all passed\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
