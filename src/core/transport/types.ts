import type { MessageType } from "./constants.js";
import type { KeyStore } from "../security/key-store.js";

// Branded type prevents passing a plain Uint8Array where a NodeId is expected.
// We stay binary throughout the hot path; hex conversion is diagnostics-only.
export type NodeId = Uint8Array & { readonly __brand: "NodeId" };

export interface Envelope {
  readonly senderId: NodeId;
  readonly type:     MessageType;
  // BigInt because uint64 overflows Number.MAX_SAFE_INTEGER at ~9 petabytes of
  // sequence space — reachable at 1M msg/s in ~104 days.
  readonly seq:      bigint;
  // Raw msgpack bytes. Relay nodes forward this without ever calling decode().
  readonly payload:  Uint8Array;
}

export interface DeltaPayload {
  readonly ns:        string;
  // Tuple array encodes ~30% smaller in msgpack vs. array-of-objects.
  // [key, value, wallClockMs] — null value is a tombstone.
  readonly mutations: ReadonlyArray<[key: Uint8Array, val: Uint8Array | null, ts: number]>;
}

export interface HeartbeatPayload {
  readonly clock:     bigint;
  readonly keyCount:  number;
  readonly ts:        number;
}

export interface SyncReqPayload {
  readonly ns:    string;
  readonly since: bigint;
  readonly range?: { readonly from: Uint8Array; readonly to: Uint8Array };
}

export interface SyncResPayload {
  readonly ns:         string;
  readonly entries:    ReadonlyArray<[key: Uint8Array, val: Uint8Array | null, ts: number, seq: bigint]>;
  readonly isComplete: boolean;
}

export interface PeerPayload {
  readonly pubAddr:    string;
  readonly routerAddr: string;
  readonly ts:         number;
}

export interface TransportCfg {
  readonly nodeId:     NodeId;
  readonly pubAddr:    string;
  readonly routerAddr: string;
  // When provided, all sockets are CURVE-encrypted and ZAP-authenticated.
  readonly security?:  { readonly keyStore: KeyStore };
}

export interface TransportStats {
  readonly sent:        bigint;
  readonly recv:        bigint;
  readonly bytesSent:   bigint;
  readonly bytesRecv:   bigint;
  readonly dropped:     bigint;
  readonly lastSentNs:  bigint;
  readonly lastRecvNs:  bigint;
}
