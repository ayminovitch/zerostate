import { Router } from "zeromq";
import type { KeyStore } from "./key-store.js";

const ZAP_ADDR = "inproc://zeromq.zap.01";

// ZAP request frame layout (ZMQ spec §27):
//   [0] routing-id  (set by ROUTER automatically)
//   [1] ""          (delimiter)
//   [2] "1.0"       (version)
//   [3] request-id  (opaque, echo back)
//   [4] domain
//   [5] address     (IP of connecting peer)
//   [6] identity
//   [7] mechanism   ("CURVE", "PLAIN", "NULL")
//   [8] credentials (32-byte public key for CURVE)
//
// ZAP response frame layout:
//   [0] routing-id  (must match request[0])
//   [1] ""          (delimiter)
//   [2] "1.0"       (version)
//   [3] request-id  (echo)
//   [4] status      ("200" allow / "400" deny)
//   [5] status-text
//   [6] user-id
//   [7] metadata
//
// Only ONE ZAP handler may be bound per ZMQ context (inproc is context-scoped).
// In multi-node processes, all KeyStores must be registered with the singleton
// before start(), and the handler fans out by domain or by allowlist union.

let instance: ZapHandler | null = null;

export function getZapHandler(): ZapHandler {
  if (!instance) instance = new ZapHandler();
  return instance;
}

export class ZapHandler {
  private sock: Router | null = null;
  private readonly stores: Set<KeyStore> = new Set();
  private refcount = 0;

  // Register an additional KeyStore. All registered stores are checked
  // in order on each ZAP request — any single allow wins.
  register(store: KeyStore): void {
    this.stores.add(store);
  }

  unregister(store: KeyStore): void {
    this.stores.delete(store);
  }

  async start(): Promise<void> {
    this.refcount++;
    if (this.refcount > 1) return; // already running

    this.sock = new Router();
    this.sock.linger = 0;
    await this.sock.bind(ZAP_ADDR);
    void this.loop();
  }

  stop(): void {
    this.refcount = Math.max(0, this.refcount - 1);
    if (this.refcount > 0) return; // other transports still active

    if (!this.sock) return;
    this.sock.close();
    this.sock = null;
    instance   = null;
  }

  private async loop(): Promise<void> {
    if (!this.sock) return;
    try {
      for await (const frames of this.sock) {
        void this.handle(frames as Buffer[]);
      }
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== "ETERM" && code !== "EAGAIN") throw err;
    }
  }

  private async handle(frames: Buffer[]): Promise<void> {
    if (!this.sock || frames.length < 9) return;

    const routingId = frames[0]!;
    const requestId = frames[3]!;
    const mechanism = frames[7]?.toString() ?? "";
    const creds     = frames[8];

    // Union allowlist: any registered KeyStore that allows the peer = granted.
    const allowed = mechanism === "CURVE" && creds !== undefined
      ? [...this.stores].some((s) => s.isAllowed(creds))
      : false;

    await this.sock.send([
      routingId,
      Buffer.alloc(0),
      Buffer.from("1.0"),
      requestId,
      Buffer.from(allowed ? "200" : "400"),
      Buffer.from(allowed ? "OK"  : "Unauthorized"),
      Buffer.alloc(0),
      Buffer.alloc(0),
    ]);
  }
}
