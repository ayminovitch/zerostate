import test from "node:test";
import assert from "node:assert";
import { ZeroState, generateKeypair, KeyStore } from "../src/index.js";

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function makeTestNode(id: string, peerKs?: KeyStore) {
  const kp = generateKeypair();
  const ks = new KeyStore(kp);
  if (peerKs) ks.allow(peerKs.publicKey);
  
  const node = new ZeroState({
    pubAddr: `inproc://pub-${id}`,
    routerAddr: `inproc://rtr-${id}`,
    security: { keyStore: ks },
    gossip: { intervalMs: 100 },
    heartbeat: { intervalMs: 100, suspectMs: 300, deadMs: 600 }
  });
  return { node, ks, kp };
}

test("CRDT State Merging & Vector Clock Ties", async (t) => {
  const A = makeTestNode("A-1");
  const B = makeTestNode("B-1");
  A.ks.allow(B.kp.publicKey);
  B.ks.allow(A.kp.publicKey);

  await A.node.start();
  await B.node.start();

  t.after(async () => {
    await A.node.stop();
    await B.node.stop();
  });

  await Promise.all([
    new Promise<void>(r => A.node.once("peer:up", () => r())),
    new Promise<void>(r => B.node.once("peer:up", () => r())),
    A.node.connect(`inproc://pub-B-1`, `inproc://rtr-B-1`, B.kp.publicKey),
    B.node.connect(`inproc://pub-A-1`, `inproc://rtr-A-1`, A.kp.publicKey),
  ]);

  // Test 1: Basic sync
  await A.node.set("config", "theme", "dark");
  await sleep(150); // wait for delta/gossip
  assert.strictEqual(B.node.get("config", "theme"), "dark");

  // Test 2: Concurrent identical ts (simulated)
  // We can't perfectly simulate ts tied across process.hrtime in a live run without mocking,
  // but we can rapidly fire mutations and rely on gossip convergence.
  const pA = A.node.set("config", "race", "value-A");
  const pB = B.node.set("config", "race", "value-B");
  await Promise.all([pA, pB]);
  
  await sleep(300); // Allow anti-entropy to resolve
  const valA = A.node.get("config", "race");
  const valB = B.node.get("config", "race");
  assert.strictEqual(valA, valB, "Nodes must deterministically converge to the exact same value");
});

test("Network Partition Healing via Gossip", async (t) => {
  const A = makeTestNode("A-2");
  const B = makeTestNode("B-2");
  A.ks.allow(B.kp.publicKey);
  B.ks.allow(A.kp.publicKey);

  await A.node.start();
  await B.node.start();

  t.after(async () => {
    await A.node.stop();
    await B.node.stop();
  });

  // Connect and verify
  await A.node.connect(`inproc://pub-B-2`, `inproc://rtr-B-2`, B.kp.publicKey);
  await B.node.connect(`inproc://pub-A-2`, `inproc://rtr-A-2`, A.kp.publicKey);
  await sleep(200);

  // Cause Partition
  // We simulate partition by stopping the transport or disconnecting
  // But wait, ZMQ `disconnect` removes it. Let's disconnect.
  (A.node as any)._node.transport.unsubscribeFrom(`inproc://pub-B-2`);
  (B.node as any)._node.transport.unsubscribeFrom(`inproc://pub-A-2`);
  await sleep(100);

  // Mutate while partitioned
  await A.node.set("ns", "key-a", 1);
  await B.node.set("ns", "key-b", 2);

  assert.strictEqual(B.node.get("ns", "key-a"), null);
  assert.strictEqual(A.node.get("ns", "key-b"), null);

  // Heal Partition
  (A.node as any)._node.transport.subscribeTo(`inproc://pub-B-2`, B.kp.publicKey);
  (B.node as any)._node.transport.subscribeTo(`inproc://pub-A-2`, A.kp.publicKey);

  // Gossip should heal it within a few intervals
  await sleep(500);
  assert.strictEqual(B.node.get("ns", "key-a"), 1);
  assert.strictEqual(A.node.get("ns", "key-b"), 2);
});
