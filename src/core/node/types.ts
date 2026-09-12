import type { NodeId, TransportCfg } from "../transport/index.js";
import type { MessageType } from "../transport/constants.js";

export const enum PeerState {
  CONNECTING = 0,
  ALIVE      = 1,
  SUSPECT    = 2,
  DEAD       = 3,
}

export interface PeerEntry {
  readonly id:     NodeId;
  pubAddr:         string;
  routerAddr:      string;
  state:           PeerState;
  // hrtime ns of the last received heartbeat. Compared against hrtime
  // (not Date.now) to avoid wall-clock jumps causing false SUSPECT transitions.
  lastHbNs:        bigint;
  logicalClock:    bigint;
  // EWMA of (ourWallMs - peerWallMs). Fed to the LWW CRDT to correct
  // timestamp comparisons across nodes with drifting clocks.
  clockSkewMs:     number;
}

export interface NodeCfg extends Omit<TransportCfg, "nodeId"> {
  nodeId?:      NodeId;  // auto-generated if omitted
  hbIntervalMs?:      number;
  suspectMs?:         number;
  deadMs?:            number;
  // When provided, the node's transport uses CURVE encryption and ZAP auth.
  // security is forwarded directly to TransportCfg.
}

// Defaults exposed so the liveness checker can reference them without a Node instance.
export const DEFAULT_HB_INTERVAL_MS = 1_000;
export const DEFAULT_SUSPECT_MS     = 3_000;
export const DEFAULT_DEAD_MS        = 6_000;
