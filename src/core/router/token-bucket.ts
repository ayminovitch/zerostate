export class TokenBucket {
  private tokens:       number;
  private lastRefillNs: bigint;

  constructor(
    private readonly max:         number,
    private readonly ratePerMs:   number,
  ) {
    this.tokens       = max;
    this.lastRefillNs = process.hrtime.bigint();
  }

  // Returns true and deducts `count` tokens if available.
  // Never blocks — callers decide whether to drop or queue on false.
  consume(count = 1): boolean {
    this.refill();
    if (this.tokens < count) return false;
    this.tokens -= count;
    return true;
  }

  // Partial-consume for oversized messages: consume proportional tokens
  // based on byte size relative to an expected average message size.
  consumeWeighted(bytes: number, avgMsgBytes = 256): boolean {
    const weight = Math.ceil(bytes / avgMsgBytes);
    return this.consume(weight);
  }

  get available():  number { return Math.floor(this.tokens); }
  get capacity():   number { return this.max; }

  // 0.0 (empty) → 1.0 (full). Used for WARN threshold comparison.
  get fillRatio():  number { return this.tokens / this.max; }

  private refill(): void {
    const now       = process.hrtime.bigint();
    // hrtime subtraction in BigInt ns, converted to ms for token math.
    const elapsedMs = Number(now - this.lastRefillNs) / 1_000_000;
    this.tokens     = Math.min(this.max, this.tokens + elapsedMs * this.ratePerMs);
    this.lastRefillNs = now;
  }
}
