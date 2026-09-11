import { context as _global, type Context } from "zeromq";
import { ContextError } from "../../errors.js";
import { ZMQ_IO_THREADS, ZMQ_MAX_SOCKETS } from "./constants.js";

let _ctx: Context | null = null;

export function getCtx(): Context {
  if (_ctx !== null) return _ctx;

  _ctx = _global;
  // Must be set before the first socket is created; ignored after.
  try {
    _ctx.ioThreads  = ZMQ_IO_THREADS;
    _ctx.maxSockets = ZMQ_MAX_SOCKETS;
  } catch (e) {
    throw new ContextError(`failed to configure ZMQ context: ${String(e)}`);
  }

  return _ctx;
}

// For test injection only — swap before any socket is allocated.
export function setCtx(ctx: Context): void {
  _ctx = ctx;
}

export function destroyCtx(): void {
  if (_ctx === null) return;
  _ctx.blocky = false;
  _ctx = null;
}
