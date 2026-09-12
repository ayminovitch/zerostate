import test from "node:test";
import assert from "node:assert";
import { ZeroState, generateKeypair, KeyStore } from "../src/index.js";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function makeNode(pubPort: number, rtrPort: number, peerKey?: string) {
  const kp = generateKeypair();
  const ks = new KeyStore(kp);
  if (peerKey) ks.allow(peerKey);
  
  const node = new ZeroState({
    pubAddr: `tcp://127.0.0.1:${pubPort}`,
    routerAddr: `tcp://127.0.0.1:${rtrPort}`,
    security: { keyStore: ks },
    gossip: { intervalMs: 200 }
  });
  
  return { node, kp, ks };
}

test("Mesh Integration - 2 Node TCP Sync", { timeout: 5000 }, async () => {
  const A = makeNode(24551, 24651);
  const B = makeNode(24552, 24652, A.kp.publicKey);
  A.ks.allow(B.kp.publicKey);

  try {
    await A.node.start();
    await B.node.start();

    await Promise.all([
      new Promise<void>(r => A.node.once("peer:up", () => r())),
      new Promise<void>(r => B.node.once("peer:up", () => r())),
      A.node.connect(`tcp://127.0.0.1:24552`, `tcp://127.0.0.1:24652`, B.kp.publicKey),
      B.node.connect(`tcp://127.0.0.1:24551`, `tcp://127.0.0.1:24651`, A.kp.publicKey)
    ]);

    await A.node.set("mesh_test", "key_1", { hello: "world" });
    
    // Wait for delta propagation or gossip to arrive at B
    await sleep(300);
    
    const valB = B.node.get("mesh_test", "key_1");
    assert.deepStrictEqual(valB, { hello: "world" });
  } finally {
    await A.node.stop();
    await B.node.stop();
  }
});
