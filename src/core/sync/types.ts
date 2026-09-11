export interface GossipCfg {
  // Peers contacted per gossip round. O(log N) convergence at fanout ≥ 3.
  fanout:        number;
  // Interval used when the store is actively changing (fast convergence mode).
  minIntervalMs: number;
  // Baseline interval.
  intervalMs:    number;
  // Interval after 3+ consecutive stable rounds (store not advancing).
  maxIntervalMs: number;
  // Stable rounds required before the interval starts backing off.
  stableRoundsBeforeBackoff: number;
}

export const DEFAULT_GOSSIP_CFG: Required<GossipCfg> = {
  fanout:                    3,
  minIntervalMs:             1_000,
  intervalMs:                5_000,
  maxIntervalMs:             15_000,
  // 3 rounds of stability before backing off. At 5s interval that's 15s
  // of silence before we consider the mesh fully converged.
  stableRoundsBeforeBackoff: 3,
};

export interface GossipRoundStats {
  round:          number;
  peersContacted: number;
  intervalMs:     number;
  stableRounds:   number;
  diverged:       boolean;
}
