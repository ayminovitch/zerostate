import { ZeroState, generateKeypair, KeyStore } from "../src/index.js";

async function main() {
  console.log("Starting basic sync example...");

  const kpA = generateKeypair();
  const kpB = generateKeypair();

  const ksA = new KeyStore(kpA).allow(kpB.publicKey);
  const ksB = new KeyStore(kpB).allow(kpA.publicKey);

  const nodeA = new ZeroState({
    pubAddr: "tcp://127.0.0.1:45551",
    routerAddr: "tcp://127.0.0.1:46551",
    security: { keyStore: ksA }
  });

  const nodeB = new ZeroState({
    pubAddr: "tcp://127.0.0.1:45552",
    routerAddr: "tcp://127.0.0.1:46552",
    security: { keyStore: ksB }
  });

  await nodeA.start();
  await nodeB.start();

  nodeB.on("change", (ns, key, val) => {
    console.log(`[Node B] Data updated: ${ns}/${key} =`, val);
  });

  await nodeA.connect("tcp://127.0.0.1:45552", "tcp://127.0.0.1:46552", kpB.publicKey);
  await nodeB.connect("tcp://127.0.0.1:45551", "tcp://127.0.0.1:46551", kpA.publicKey);
  
  // Wait for connections to stabilize
  await new Promise(r => setTimeout(r, 1000));

  console.log("[Node A] Mutating state...");
  await nodeA.set("config", "theme", "dark");
  await nodeA.set("config", "version", 1);
  
  await new Promise(r => setTimeout(r, 1000));
  console.log("Closing...");
  await nodeA.stop();
  await nodeB.stop();
}

main().catch(console.error);
