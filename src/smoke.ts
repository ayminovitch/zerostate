import { Transport, mkNodeId, nodeIdToHex, MessageType } from "./core/transport/index.js";
import type { TransportCfg } from "./core/transport/index.js";

function makeCfg(offset: number): TransportCfg {
  return {
    nodeId:     mkNodeId(),
    pubAddr:    `tcp://127.0.0.1:${5550 + offset}`,
    routerAddr: `tcp://127.0.0.1:${6550 + offset}`,
  };
}

async function main(): Promise<void> {
  console.log("═══════════════════════════════════════");
  console.log("  ZeroState Transport — Smoke Test");
  console.log("═══════════════════════════════════════\n");

  const cfgA = makeCfg(1);
  const cfgB = makeCfg(2);
  const nodeA = new Transport(cfgA);
  const nodeB = new Transport(cfgB);

  console.log(`A: ${nodeIdToHex(cfgA.nodeId)}`);
  console.log(`B: ${nodeIdToHex(cfgB.nodeId)}\n`);

  const deltaRecv = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: DELTA")), 3_000);
    nodeA.on("delta", (env) => {
      clearTimeout(t);
      console.log(`✅ A recv DELTA  seq=${env.seq}  from=${nodeIdToHex(env.senderId)}`);
      res();
    });
  });

  const rpcRecv = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: SYNC_REQ")), 3_000);
    nodeB.on("syncReq", async (env, respond) => {
      clearTimeout(t);
      console.log(`✅ B recv SYNC_REQ  seq=${env.seq}`);
      await respond({ status: "ok", entries: [] });
      res();
    });
  });

  const syncResRecv = new Promise<void>((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout: SYNC_RES")), 3_000);
    nodeA.on("syncRes", (env) => {
      clearTimeout(t);
      console.log(`✅ A recv SYNC_RES  seq=${env.seq}`);
      res();
    });
  });

  nodeA.on("frameError", (e) => console.error("A frameError:", e.message));
  nodeB.on("frameError", (e) => console.error("B frameError:", e.message));
  nodeA.on("fatal", (e) => console.error("A FATAL:", e));
  nodeB.on("fatal", (e) => console.error("B FATAL:", e));

  await Promise.all([nodeA.start(), nodeB.start()]);
  console.log("nodes up\n");

  nodeA.subscribeTo(cfgB.pubAddr);
  nodeA.dialRouter(cfgB.routerAddr);
  await new Promise<void>((r) => setTimeout(r, 200));

  console.log("── PUB/SUB ────────────────────────────────");
  const seq = await nodeB.publish(MessageType.DELTA, {
    ns: "room:chat",
    mutations: [[new Uint8Array([0x01, 0x02]), new Uint8Array([0x48, 0x69]), Date.now()]],
  });
  console.log(`   B published DELTA seq=${seq}`);
  await deltaRecv;

  console.log("\n── DEALER/ROUTER RPC ──────────────────────");
  const rseq = await nodeA.rpcSend(MessageType.SYNC_REQ, { ns: "room:chat", since: 0n });
  console.log(`   A sent SYNC_REQ seq=${rseq}`);
  await Promise.all([rpcRecv, syncResRecv]);

  console.log("\n── Stats ───────────────────────────────────");
  console.log("A:", nodeA.stats);
  console.log("B:", nodeB.stats);

  await Promise.all([nodeA.stop(), nodeB.stop()]);
  console.log("\n✅ all passed\n");
}

main().catch((e) => { console.error("FAIL:", e); process.exit(1); });
