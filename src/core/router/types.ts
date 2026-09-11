export enum BackpressureLevel {
  NONE = 0,
  WARN = 1,
  FULL = 2,
}

export interface RouterCfg {
  // Token bucket burst capacity — max queued messages before FULL
  maxTokens:       number;
  // Tokens added per millisecond — sets the sustained throughput ceiling
  refillRatePerMs: number;
  // Fill ratio below which WARN fires. 0.20 = warn at 80% consumed.
  warnThreshold:   number;
  // RTT multiplier above baseline that triggers WARN for a specific peer
  rttWarnFactor:   number;
}

export const DEFAULT_ROUTER_CFG: Required<RouterCfg> = {
  // 1000 burst / 10 per ms = 10k msg/s sustained, 1000 msg burst headroom.
  // Tune maxTokens upward for workloads with large bursty flushes.
  maxTokens:       1_000,
  refillRatePerMs: 10,
  warnThreshold:   0.20,
  // RTT must exceed 3× baseline before the peer is de-prioritised for routing.
  rttWarnFactor:   3,
};

export interface RouteEntry {
  readonly peerId:      string;  // hex NodeId
  ewmaRttMs:            number;
  baselineRttMs:        number;  // established after 8 samples
  sampleCount:          number;
  score:                number;  // higher = prefer this peer
}
