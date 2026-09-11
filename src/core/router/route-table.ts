import type { RouteEntry } from "./types.js";

export class RouteTable {
  private readonly routes = new Map<string, RouteEntry>();

  upsert(peerId: string): RouteEntry {
    let e = this.routes.get(peerId);
    if (!e) {
      e = { peerId, ewmaRttMs: 0, baselineRttMs: 0, sampleCount: 0, score: 1 };
      this.routes.set(peerId, e);
    }
    return e;
  }

  recordRtt(peerId: string, rttMs: number): RouteEntry {
    const e = this.upsert(peerId);
    e.sampleCount++;

    e.ewmaRttMs = e.ewmaRttMs === 0
      ? rttMs
      : 0.875 * e.ewmaRttMs + 0.125 * rttMs;

    // Baseline locked in at sample 8 — the EWMA convergence point (α=0.125).
    // Samples before this are too noisy to use as a stable reference.
    if (e.sampleCount === 8) e.baselineRttMs = e.ewmaRttMs;

    // Clamp to 1ms minimum to avoid Infinity score on sub-millisecond loopback.
    e.score = 1_000 / Math.max(e.ewmaRttMs, 1);

    return e;
  }

  remove(peerId: string): void {
    this.routes.delete(peerId);
  }

  // Peer with the highest score (lowest observed RTT). Undefined if table is empty.
  best(): RouteEntry | undefined {
    let top: RouteEntry | undefined;
    for (const e of this.routes.values()) {
      if (!top || e.score > top.score) top = e;
    }
    return top;
  }

  get(peerId: string): RouteEntry | undefined {
    return this.routes.get(peerId);
  }

  all(): IterableIterator<RouteEntry> {
    return this.routes.values();
  }

  size(): number {
    return this.routes.size;
  }
}
