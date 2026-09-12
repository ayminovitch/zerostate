import { ZeroState, generateKeypair, KeyStore } from "./index.js";
import { BASE_PUB_PORT, BASE_ROUTER_PORT } from "./core/transport/constants.js";

const TARGET_RATE = 10_000;
const DURATION_SEC = 10;
const TOTAL_MESSAGES = TARGET_RATE * DURATION_SEC;

function makeNode(offset: number, peerKey?: string) {
  const kp = generateKeypair();
  const ks = new KeyStore(kp);
  if (peerKey) ks.allow(peerKey);
  return {
    node: new ZeroState({
      pubAddr:      `tcp://127.0.0.1:${BASE_PUB_PORT    + offset}`,
      routerAddr:   `tcp://127.0.0.1:${BASE_ROUTER_PORT + offset}`,
      security:     { keyStore: ks },
      // Tune for high throughput
      router:       { maxTokens: TARGET_RATE, refillRatePerMs: Math.ceil(TARGET_RATE / 1000) },
      gossip:       { intervalMs: 1000 },
      heartbeat:    { intervalMs: 1000, suspectMs: 3000, deadMs: 6000 },
    }),
    ks,
  };
}

// Quick sleep
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  console.log("═══════════════════════════════════════");
  console.log(`  ZeroState Benchmark`);
  console.log(`  Target: ${TARGET_RATE} msg/sec for ${DURATION_SEC}s (${TOTAL_MESSAGES} total)`);
  console.log("═══════════════════════════════════════\n");

  const kpA = generateKeypair();
  const kpB = generateKeypair();
  const ksA = new KeyStore(kpA).allow(kpB.publicKey);
  const ksB = new KeyStore(kpB).allow(kpA.publicKey);

  const A = new ZeroState({
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + 21}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + 21}`,
    security:   { keyStore: ksA },
    router:     { maxTokens: TARGET_RATE * 2, refillRatePerMs: Math.ceil(TARGET_RATE / 1000) * 2 },
  });

  const B = new ZeroState({
    pubAddr:    `tcp://127.0.0.1:${BASE_PUB_PORT    + 22}`,
    routerAddr: `tcp://127.0.0.1:${BASE_ROUTER_PORT + 22}`,
    security:   { keyStore: ksB },
  });

  await A.start();
  await B.start();

  await Promise.all([
    new Promise<void>((r) => A.once("peer:up", () => r())),
    new Promise<void>((r) => B.once("peer:up", () => r())),
    A.connect(`tcp://127.0.0.1:${BASE_PUB_PORT + 22}`, `tcp://127.0.0.1:${BASE_ROUTER_PORT + 22}`, kpB.publicKey),
    B.connect(`tcp://127.0.0.1:${BASE_PUB_PORT + 21}`, `tcp://127.0.0.1:${BASE_ROUTER_PORT + 21}`, kpA.publicKey),
  ]);

  console.log("✅ Peers connected. Stabilizing...");
  await sleep(1000);
  
  global.gc?.();
  const memBefore = process.memoryUsage();

  console.log("🚀 Starting benchmark...\n");

  const latencies = new Float64Array(TOTAL_MESSAGES);
  let received = 0;
  
  const done = new Promise<void>((resolve) => {
    B.on("change", (ns, key, value) => {
      if (ns === "bench" && value instanceof Uint8Array && value.length === 8) {
        const sendTime = new DataView(value.buffer, value.byteOffset, value.byteLength).getBigUint64(0, true);
        const recvTime = process.hrtime.bigint();
        const latencyMs = Number(recvTime - sendTime) / 1_000_000;
        
        const idx = Number(new DataView(key.buffer, key.byteOffset, key.byteLength).getUint32(0, true));
        latencies[idx] = latencyMs;
        received++;

        if (received === TOTAL_MESSAGES) {
          resolve();
        }
      }
    });
  });

  const startTime = process.hrtime.bigint();
  
  // Rate limiter / sender
  const batchSize = Math.ceil(TARGET_RATE / 100); // 10ms batches
  const intervalMs = 10;
  let sent = 0;

  const sendInterval = setInterval(async () => {
    for (let i = 0; i < batchSize && sent < TOTAL_MESSAGES; i++) {
      const idx = sent++;
      const keyBuf = Buffer.allocUnsafe(4);
      keyBuf.writeUInt32LE(idx, 0);
      
      const valBuf = Buffer.allocUnsafe(8);
      valBuf.writeBigUInt64LE(process.hrtime.bigint(), 0);
      
      // Async setRaw doesn't block event loop, but tryPublishDelta can drop if backpressure hits
      void A.setRaw("bench", keyBuf, valBuf);
    }

    if (sent >= TOTAL_MESSAGES) {
      clearInterval(sendInterval);
    }
  }, intervalMs);

  // Wait for all messages to be received or timeout
  const timeout = sleep(DURATION_SEC * 1000 + 5000).then(() => {
    if (received < TOTAL_MESSAGES) {
      console.warn(`\n⚠️ Timeout! Only received ${received}/${TOTAL_MESSAGES} messages`);
    }
  });

  await Promise.race([done, timeout]);
  clearInterval(sendInterval);

  const endTime = process.hrtime.bigint();
  const durationMs = Number(endTime - startTime) / 1_000_000;
  const actualRate = (received / (durationMs / 1000)).toFixed(2);
  
  global.gc?.();
  const memAfter = process.memoryUsage();
  
  // Compute percentiles
  const sorted = latencies.slice(0, received).sort();
  const p50 = sorted[Math.floor(received * 0.50)] || 0;
  const p90 = sorted[Math.floor(received * 0.90)] || 0;
  const p99 = sorted[Math.floor(received * 0.99)] || 0;
  const p999 = sorted[Math.floor(received * 0.999)] || 0;
  const p100 = sorted[received - 1] || 0;

  console.log("── Results ──────────────────────────────────────────────");
  console.log(`Throughput:   ${actualRate} msg/sec`);
  console.log(`Total Recv:   ${received} / ${TOTAL_MESSAGES}`);
  console.log(`Lost/Dropped: ${TOTAL_MESSAGES - received}`);
  console.log("\nLatency (ms):");
  console.log(`  p50:   ${p50.toFixed(3)} ms`);
  console.log(`  p90:   ${p90.toFixed(3)} ms`);
  console.log(`  p99:   ${p99.toFixed(3)} ms`);
  console.log(`  p99.9: ${p999.toFixed(3)} ms`);
  console.log(`  Max:   ${p100.toFixed(3)} ms`);
  
  console.log("\nMemory (Heap Used):");
  console.log(`  Before: ${(memBefore.heapUsed / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  After:  ${(memAfter.heapUsed / 1024 / 1024).toFixed(2)} MB`);
  console.log(`  Delta:  ${((memAfter.heapUsed - memBefore.heapUsed) / 1024 / 1024).toFixed(2)} MB`);

  console.log("\n── Stats Snapshot ───────────────────────────────────────");
  const sA = A.stats;
  console.log(`A token bucket: ${sA.router.tokensAvailable} / ${sA.router.tokenFillRatio.toFixed(2)}`);
  
  await A.stop();
  await B.stop();
  process.exit(0);
}

main().catch(console.error);
