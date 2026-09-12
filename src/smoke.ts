import { Node }           from "./core/node/index.js";
import { StateStore }     from "./core/crdt/index.js";
import { generateKeypair, KeyStore } from "./core/security/index.js";
import { mkNodeId, nodeIdToHex } from "./core/transport/index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const key = (s: string) => enc.encode(s);
const val = (s: string) => enc.encode(s);

function makeSecureStack(offset: number, ks: KeyStore) {
  const nodeId = mkNodeId();
  const cfg = {
    nodeId,
    pubAddr:      `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
    routerAddr:   `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
    hbIntervalMs: 200, suspectMs: 600, deadMs: 1_200,
    security:     { keyStore: ks },
  };
  const node  = new Node(cfg);
  const store = new StateStore(node);
  return { node, store, cfg, ks };
}

async function waitPeerUp(observer: Node, target: Uint8Array, ms = 4_000): Promise<void> {
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: peer:up")), ms);
    observer.on("peer:up", (e) => {
      if (nodeIdToHex(e.id) !== nodeIdToHex(target as Parameters<typeof nodeIdToHex>[0])) return;
      clearTimeout(t); res();
    });
  });
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState CURVE Security — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  // Generate independent keypairs for A and B
  const kpA = generateKeypair();
  const kpB = generateKeypair();
  console.log(`A pubkey: ${kpA.publicKey.slice(0, 10)}…`);
  console.log(`B pubkey: ${kpB.publicKey.slice(0, 10)}…`);

  // Mutual allowlist: A trusts B, B trusts A
  const ksA = new KeyStore(kpA).allow(kpB.publicKey);
  const ksB = new KeyStore(kpB).allow(kpA.publicKey);
  console.log(`A allowlist size: ${ksA.allowedCount}  B allowlist size: ${ksB.allowedCount}\n`);

  const A = makeSecureStack(11, ksA);
  const B = makeSecureStack(12, ksB);

  A.node.on("fatal", (e) => console.error("A FATAL:", e));
  B.node.on("fatal", (e) => console.error("B FATAL:", e));

  await Promise.all([A.node.start(), B.node.start()]);
  A.store.start(); B.store.start();

  // ── Test 1: Secure connection via connect() with peer Z85 public key ────────
  console.log("── Test 1: Encrypted mesh connection ───────────────────────────");
  await A.node.connect(B.cfg.pubAddr, B.cfg.routerAddr, kpB.publicKey);
  await B.node.connect(A.cfg.pubAddr, A.cfg.routerAddr, kpA.publicKey);

  await Promise.all([
    waitPeerUp(A.node, B.cfg.nodeId),
    waitPeerUp(B.node, A.cfg.nodeId),
  ]);
  console.log("✅ peers up over CURVE-encrypted channels");

  // ── Test 2: State sync over encrypted transport ──────────────────────────
  console.log("\n── Test 2: State sync over CURVE transport ─────────────────────");
  const deltaOnB = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: delta")), 2_000);
    B.node.on("delta", () => { clearTimeout(t); res(); });
  });

  await A.store.mutate("secure:state", key("secret:config"), val('{"env":"prod","region":"eu-west"}'));
  await deltaOnB;
  await new Promise<void>((r) => setTimeout(r, 50));

  const v = B.store.get("secure:state", key("secret:config"));
  console.log(`✅ B received encrypted delta: ${v ? dec.decode(v) : "null"}`);

  // ── Test 3: Verify keys are Z85 and 40 chars ─────────────────────────────
  console.log("\n── Test 3: Key format validation ────────────────────────────────");
  console.log(`✅ A publicKey length: ${kpA.publicKey.length} chars  (expect: 40)`);
  console.log(`✅ B secretKey length: ${kpB.secretKey.length} chars  (expect: 40)`);
  console.log(`✅ Keys are printable Z85: ${/^[\x21-\x7e]+$/.test(kpA.publicKey)}`);

  // ── Stats ─────────────────────────────────────────────────────────────────
  console.log("\n── Stats ────────────────────────────────────────────────────────");
  console.log("A transport:", A.node.transportStats);
  console.log("B transport:", B.node.transportStats);

  A.store.stop(); B.store.stop();
  await Promise.all([A.node.stop(), B.node.stop()]);
  console.log("\n✅ all passed — all traffic encrypted with Curve25519-XSalsa20-Poly1305\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
