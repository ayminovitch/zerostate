// V8 captures a stack trace at Error construction time. Subclasses that call
// captureStackTrace(this, Ctor) trim the constructor frame from the trace,
// which makes the reported origin accurate without manual slicing.

export class ZeroStateError extends Error {
  override readonly name: string = "ZeroStateError";
  constructor(msg: string) {
    super(msg);
    Error.captureStackTrace(this, new.target);
  }
}

export class TransportError extends ZeroStateError {
  override readonly name: string = "TransportError";
}

export class FrameError extends TransportError {
  override readonly name: string = "FrameError";
  readonly frameCount: number;
  constructor(msg: string, frameCount: number) {
    super(msg);
    this.frameCount = frameCount;
  }
}

export class IdentityError extends TransportError {
  override readonly name: string = "IdentityError";
}

export class LifecycleError extends TransportError {
  override readonly name: string = "LifecycleError";
}

export class ContextError extends ZeroStateError {
  override readonly name: string = "ContextError";
}
