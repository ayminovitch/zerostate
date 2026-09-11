import { randomFillSync } from "node:crypto";
import type { NodeId } from "./types.js";

export function mkNodeId(): NodeId {
  const buf = new Uint8Array(16);
  randomFillSync(buf);
  return buf as NodeId;
}

export function nodeIdsEq(a: NodeId, b: NodeId): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

// Lexicographic order used to deterministically elect which peer initiates
// anti-entropy when two nodes discover each other simultaneously.
export function cmpNodeId(a: NodeId, b: NodeId): number {
  const len = Math.min(a.byteLength, b.byteLength);
  for (let i = 0; i < len; i++) {
    const d = (a[i] ?? 0) - (b[i] ?? 0);
    if (d !== 0) return d;
  }
  return a.byteLength - b.byteLength;
}
