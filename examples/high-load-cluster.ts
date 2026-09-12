import { ZeroState, generateKeypair, KeyStore } from "../src/index.js";

async function main() {
  console.log("Starting high-load cluster example...");

  const kpA = generateKeypair();
  const kpB = generateKeypair();

  const ksA = new KeyStore(kpA).allow(kpB.publicKey);
  const ksB = new KeyStore(kpB).allow(kpA.publicKey);

  const nodeA = new ZeroState({
    pubAddr: "tcp://127.0.0.1:47551",
    routerAddr: "tcp://127.0.0.1:48551",
    security: { keyStore: ksA },
    router: { maxTokens: 5000, refillRatePerMs: 5 } // High throughput config
  });

  const nodeB = new ZeroState({
    pubAddr: "tcp://127.0.0.1:47552",
    routerAddr: "tcp://127.0.0.1:48552",
    security: { keyStore: ksB }
  });

  await nodeA.start();
  await nodeB.start();
  
  await nodeA.connect("tcp://127.0.0.1:47552", "tcp://127.0.0.1:48552", kpB.publicKey);
  await nodeB.connect("tcp://127.0.0.1:47551", "tcp://127.0.0.1:48551", kpA.publicKey);
  await new Promise(r => setTimeout(r, 1000));

  console.log("Pushing 10,000 updates as fast as possible...");
  
  let received = 0;
  nodeB.on("change", () => {
    received++;
  });

  const startTime = Date.now();
  for (let i = 0; i < 10000; i++) {
    // We do not await to push maximum throughput locally
    void nodeA.set("load_test", `key-${i}`, { timestamp: Date.now() });
  }

  while (received < 10000 && Date.now() - startTime < 10000) {
    await new Promise(r => setTimeout(r, 100));
  }
  
  console.log(`Received ${received}/10000 updates in ${Date.now() - startTime}ms.`);
  
  console.log("Closing...");
  await nodeA.stop();
  await nodeB.stop();
}

main().catch(console.error);
