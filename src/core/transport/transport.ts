import { Publisher, Subscriber, Router, Dealer } from "zeromq";
import { EventEmitter } from "node:events";
import type { Envelope, TransportCfg, TransportStats } from "./types.js";
import { LifecycleError, TransportError, FrameError } from "../../errors.js";
import { assemble, parseEnvelope } from "./codec.js";
import {
  MessageType,
  HWM_SEND, HWM_RECV, LINGER_MS, RECONNECT_IVL_MS,
} from "./constants.js";
import { getZapHandler } from "../security/zap-handler.js";

export interface TransportEvents {
  delta:      [env: Envelope];
  heartbeat:  [env: Envelope];
  syncReq:    [env: Envelope, respond: (body: unknown) => Promise<void>];
  syncRes:    [env: Envelope];
  join:       [env: Envelope];
  leave:      [env: Envelope];
  frameError: [err: FrameError, raw: Buffer[]];
  fatal:      [err: Error];
  ready:      [];
  stopped:    [];
}

export class Transport extends EventEmitter<TransportEvents> {
  private readonly cfg:  TransportCfg;
  private readonly pub:  Publisher;
  private readonly sub:  Subscriber;
  private readonly rtr:  Router;
  private readonly dlr:  Dealer;
  private readonly ac:   AbortController = new AbortController();
  // null when security is disabled. Shared singleton per ZMQ context.
  private readonly zap = getZapHandler();

  private live    = false;
  private closing = false;

  private readonly seqs = new Map<MessageType, bigint>([
    [MessageType.DELTA,     0n],
    [MessageType.HEARTBEAT, 0n],
    [MessageType.SYNC_REQ,  0n],
    [MessageType.SYNC_RES,  0n],
    [MessageType.JOIN,      0n],
    [MessageType.LEAVE,     0n],
    [MessageType.ACK,       0n],
  ]);

  private _sent    = 0n;
  private _recv    = 0n;
  private _bsent   = 0n;
  private _brecv   = 0n;
  private _dropped = 0n;
  private _lastTx  = 0n;
  private _lastRx  = 0n;

  constructor(cfg: TransportCfg) {
    super();
    this.cfg = cfg;

    this.pub = new Publisher();
    this.pub.sendHighWaterMark = HWM_SEND;
    this.pub.linger = LINGER_MS;

    this.sub = new Subscriber();
    this.sub.receiveHighWaterMark = HWM_RECV;
    this.sub.linger = LINGER_MS;
    this.sub.reconnectInterval = RECONNECT_IVL_MS;

    this.rtr = new Router();
    this.rtr.sendHighWaterMark    = HWM_SEND;
    this.rtr.receiveHighWaterMark = HWM_RECV;
    this.rtr.linger    = LINGER_MS;
    this.rtr.mandatory = true;

    this.dlr = new Dealer();
    this.dlr.sendHighWaterMark    = HWM_SEND;
    this.dlr.receiveHighWaterMark = HWM_RECV;
    this.dlr.linger = LINGER_MS;
    this.dlr.reconnectInterval = RECONNECT_IVL_MS;
    this.dlr.routingId = Buffer.from(cfg.nodeId).toString("binary");

    if (cfg.security) {
      this.zap.register(cfg.security.keyStore);
      this.applyServerCurve(cfg.security.keyStore);
    }
  }

  // Sets CURVE server options on bind sockets. Must run before start().
  // ZAP handler is started first so it is ready when the bind triggers
  // the first ZMQ internal auth check.
  private applyServerCurve(ks: { publicKey: string; secretKey: string }): void {
    this.pub.curveServer    = true;
    this.pub.curvePublicKey = ks.publicKey;
    this.pub.curveSecretKey = ks.secretKey;

    this.rtr.curveServer    = true;
    this.rtr.curvePublicKey = ks.publicKey;
    this.rtr.curveSecretKey = ks.secretKey;
  }

  async start(): Promise<void> {
    if (this.live)    throw new LifecycleError("transport already started");
    if (this.closing) throw new LifecycleError("transport is shutting down");

    // ZAP handler must be bound before any CURVE-enabled socket binds.
    if (this.cfg.security) await this.zap.start();

    this.sub.subscribe("");
    await Promise.all([
      this.pub.bind(this.cfg.pubAddr),
      this.rtr.bind(this.cfg.routerAddr),
    ]);

    this.live = true;
    void this.loopSub();
    void this.loopDlr();
    void this.loopRtr();
    this.emit("ready");
  }

  async stop(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    this.live    = false;

    this.ac.abort();
    await new Promise<void>((r) => setImmediate(r));

    this.pub.close();
    this.sub.close();
    this.rtr.close();
    this.dlr.close();
    // ZAP handler ref-counted: only closes after last Transport.stop().
    if (this.cfg.security) this.zap.stop();
    if (this.cfg.security) this.zap.unregister(this.cfg.security.keyStore);
    this.emit("stopped");
  }

  // serverKey is the peer's Z85 public key (as returned by curveKeyPair).
  subscribeTo(addr: string, serverKey?: string): void {
    this.assertLive("subscribeTo");
    if (serverKey && this.cfg.security) {
      this.sub.curvePublicKey = this.cfg.security.keyStore.publicKey;
      this.sub.curveSecretKey = this.cfg.security.keyStore.secretKey;
      this.sub.curveServerKey = serverKey;
    }
    void this.sub.connect(addr);
  }

  unsubscribeFrom(addr: string): void {
    this.assertLive("unsubscribeFrom");
    void this.sub.disconnect(addr);
  }

  dialRouter(addr: string, serverKey?: string): void {
    this.assertLive("dialRouter");
    if (serverKey && this.cfg.security) {
      this.dlr.curvePublicKey = this.cfg.security.keyStore.publicKey;
      this.dlr.curveSecretKey = this.cfg.security.keyStore.secretKey;
      this.dlr.curveServerKey = serverKey;
    }
    void this.dlr.connect(addr);
  }

  async publish(type: MessageType, body: unknown): Promise<bigint> {
    this.assertLive("publish");
    const seq    = this.nextSeq(type);
    const frames = assemble(this.cfg.nodeId, type, seq, body);
    await this.pub.send(frames);
    this.trackTx(frames);
    return seq;
  }

  async rpcSend(type: MessageType, body: unknown): Promise<bigint> {
    this.assertLive("rpcSend");
    const seq    = this.nextSeq(type);
    const frames = assemble(this.cfg.nodeId, type, seq, body);
    await this.dlr.send(frames);
    this.trackTx(frames);
    return seq;
  }

  get stats(): TransportStats {
    return {
      sent:       this._sent,
      recv:       this._recv,
      bytesSent:  this._bsent,
      bytesRecv:  this._brecv,
      dropped:    this._dropped,
      lastSentNs: this._lastTx,
      lastRecvNs: this._lastRx,
    };
  }

  private async loopSub(): Promise<void> {
    try {
      for await (const raw of this.sub) {
        if (this.ac.signal.aborted) break;
        this.ingest(raw as Buffer[], undefined);
      }
    } catch (e) { this.onLoopErr("SUB", e); }
  }

  private async loopDlr(): Promise<void> {
    try {
      for await (const raw of this.dlr) {
        if (this.ac.signal.aborted) break;
        this.ingest(raw as Buffer[], undefined);
      }
    } catch (e) { this.onLoopErr("DLR", e); }
  }

  private async loopRtr(): Promise<void> {
    try {
      for await (const raw of this.rtr) {
        if (this.ac.signal.aborted) break;
        const frames = raw as Buffer[];

        if (frames.length < 1) { this._dropped++; continue; }

        // ROUTER prepends the sender's routing identity as frame[0].
        const rid     = frames[0]!;
        const payload = frames.slice(1) as Buffer[];

        this.ingest(payload, async (resBody) => {
          const seq    = this.nextSeq(MessageType.SYNC_RES);
          const rFrames = assemble(this.cfg.nodeId, MessageType.SYNC_RES, seq, resBody);
          await this.rtr.send([rid, ...rFrames]);
          this.trackTx(rFrames);
        });
      }
    } catch (e) { this.onLoopErr("RTR", e); }
  }

  private ingest(
    frames: Buffer[],
    respond: ((body: unknown) => Promise<void>) | undefined,
  ): void {
    let env: Envelope;
    try {
      env = parseEnvelope(frames);
    } catch (e) {
      this._dropped++;
      if (e instanceof FrameError) this.emit("frameError", e, frames);
      return;
    }

    const bytes = frames.reduce((a, f) => a + f.byteLength, 0);
    this._recv++;
    this._brecv  += BigInt(bytes);
    this._lastRx  = process.hrtime.bigint();

    this.dispatch(env, respond);
  }

  private dispatch(
    env: Envelope,
    respond: ((body: unknown) => Promise<void>) | undefined,
  ): void {
    switch (env.type) {
      case MessageType.DELTA:     return void this.emit("delta",     env);
      case MessageType.HEARTBEAT: return void this.emit("heartbeat", env);
      case MessageType.SYNC_REQ:  return void this.emit("syncReq",   env, respond ?? this.noopRespond);
      case MessageType.SYNC_RES:  return void this.emit("syncRes",   env);
      case MessageType.JOIN:      return void this.emit("join",      env);
      case MessageType.LEAVE:     return void this.emit("leave",     env);
      default:                    this._dropped++;
    }
  }

  private nextSeq(type: MessageType): bigint {
    const n = (this.seqs.get(type) ?? 0n) + 1n;
    this.seqs.set(type, n);
    return n;
  }

  private trackTx(frames: readonly Uint8Array[]): void {
    this._sent++;
    this._bsent  += BigInt(frames.reduce((a, f) => a + f.byteLength, 0));
    this._lastTx  = process.hrtime.bigint();
  }

  private assertLive(op: string): void {
    if (!this.live)    throw new LifecycleError(`transport not started; call start() before ${op}()`);
    if (this.closing)  throw new LifecycleError(`transport stopping; cannot call ${op}()`);
  }

  private onLoopErr(sock: string, e: unknown): void {
    if (this.closing) return;
    const err = e instanceof Error ? e : new TransportError(`${sock} loop error: ${String(e)}`);
    // ETERM = context terminated cleanly — not a crash.
    if ("code" in err && (err as NodeJS.ErrnoException).code === "ETERM") {
      void this.stop(); return;
    }
    this.emit("fatal", err);
    void this.stop();
  }

  private readonly noopRespond = async (_: unknown): Promise<void> => {
    this._dropped++;
  };
}
