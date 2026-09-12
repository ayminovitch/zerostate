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

export class ZapHandler {
  private sock: Router | null = null;

  constructor(private readonly store: KeyStore) {}

  async start(): Promise<void> {
    this.sock = new Router();
    // ZAP handler must never buffer requests — linger 0 ensures clean shutdown.
    this.sock.linger = 0;
    await this.sock.bind(ZAP_ADDR);
    void this.loop();
  }

  stop(): void {
    if (!this.sock) return;
    this.sock.close();
    this.sock = null;
  }

  private async loop(): Promise<void> {
    if (!this.sock) return;
    try {
      for await (const frames of this.sock) {
        void this.handle(frames as Buffer[]);
      }
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      // ETERM = context terminated (normal shutdown). EAGAIN = no more messages.
      if (code !== "ETERM" && code !== "EAGAIN") throw err;
    }
  }

  private async handle(frames: Buffer[]): Promise<void> {
    if (!this.sock || frames.length < 9) return;

    const routingId = frames[0]!;
    const requestId = frames[3]!;
    const mechanism = frames[7]?.toString() ?? "";
    const creds     = frames[8];

    const allowed = mechanism === "CURVE" && creds !== undefined
      ? this.store.isAllowed(creds)
      : false;

    await this.sock.send([
      routingId,
      Buffer.alloc(0),
      Buffer.from("1.0"),
      requestId,
      Buffer.from(allowed ? "200" : "400"),
      Buffer.from(allowed ? "OK"  : "Unauthorized"),
      Buffer.alloc(0),  // user-id
      Buffer.alloc(0),  // metadata
    ]);
  }
}
