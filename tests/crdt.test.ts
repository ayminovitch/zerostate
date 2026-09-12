import test from "node:test";
import assert from "node:assert";
import { LwwSet } from "../src/core/crdt/lww-set.js";
import { mkNodeId } from "../src/core/transport/identity.js";

test("CRDT State Merging - deterministic concurrent resolution", () => {
  const storeA = new LwwSet();
  
  const nodeAId = mkNodeId();
  const nodeBId = mkNodeId();
  
  const key = Buffer.from("test-key");
  const valA = Buffer.from("A");
  const valB = Buffer.from("B");
  
  const tsA = Date.now();
  const tsB = tsA + 10;
  
  storeA.merge(key, { value: valA, ts: tsA, seq: 1n, nodeId: nodeAId });
  const val1 = storeA.get(key);
  assert.strictEqual(val1?.value?.toString(), "A");
  
  // B merges in, has higher TS, should win
  storeA.merge(key, { value: valB, ts: tsB, seq: 2n, nodeId: nodeBId });
  const val2 = storeA.get(key);
  assert.strictEqual(val2?.value?.toString(), "B");
  
  // An older mutation arrives late, should be ignored
  storeA.merge(key, { value: Buffer.from("OLD"), ts: tsA - 100, seq: 1n, nodeId: nodeAId });
  
  const val3 = storeA.get(key);
  assert.strictEqual(val3?.value?.toString(), "B", "Older mutation should not displace newer state");
});

test("CRDT State Merging - vector clock ties resolved via NodeId", () => {
  const store = new LwwSet();
  const key = Buffer.from("tie-key");
  
  // Two distinct nodes
  const id1 = Buffer.alloc(16, 1);
  const id2 = Buffer.alloc(16, 2);
  
  // Identical wall clock timestamp and Lamport seq
  const ts = Date.now();
  
  // id1 merges
  store.merge(key, { value: Buffer.from("VAL1"), ts, seq: 1n, nodeId: id1 });
  
  // id2 merges at exact same TS and seq. Because id2 > id1 lexicographically, it should win.
  store.merge(key, { value: Buffer.from("VAL2"), ts, seq: 1n, nodeId: id2 });
  assert.strictEqual(store.get(key)?.value?.toString(), "VAL2", "id2 must win tie-breaker");
  
  // If id1 merges again with same clock, it should be rejected
  const displaced = store.merge(key, { value: Buffer.from("VAL1"), ts, seq: 1n, nodeId: id1 });
  assert.strictEqual(displaced, false, "id1 must not displace id2");
  assert.strictEqual(store.get(key)?.value?.toString(), "VAL2");
});

test("CRDT State Delta - generating and paginating deltas", () => {
  const store = new LwwSet();
  const nodeId = mkNodeId();
  const ts = Date.now();
  
  store.merge(Buffer.from("k1"), { value: Buffer.from("v1"), ts, seq: 1n, nodeId });
  store.merge(Buffer.from("k2"), { value: Buffer.from("v2"), ts, seq: 2n, nodeId });
  store.merge(Buffer.from("k3"), { value: Buffer.from("v3"), ts, seq: 3n, nodeId });
  
  // Fetch all
  const [d1, hasMore1] = store.delta(0n, 10);
  assert.strictEqual(d1.length, 3);
  assert.strictEqual(hasMore1, false);
  
  // Fetch with pagination
  const [d2, hasMore2] = store.delta(0n, 2);
  assert.strictEqual(d2.length, 2);
  assert.strictEqual(hasMore2, true);
  
  // Fetch from storeSeq offset
  const maxSeq = d2.reduce((max, entry) => entry[1].storeSeq > max ? entry[1].storeSeq : max, 0n);
  const [d3, hasMore3] = store.delta(maxSeq, 10);
  assert.strictEqual(d3.length, 1);
  assert.strictEqual(hasMore3, false);
});
