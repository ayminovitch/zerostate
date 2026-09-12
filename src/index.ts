// Primary SDK surface — start here.
export { ZeroState }         from "./zero-state.js";
export type { ZeroStateCfg, ZeroStateEvents, ZeroStateStats } from "./types.js";

// Security primitives — needed to configure CURVE authentication.
export { KeyStore, generateKeypair } from "./core/security/key-store.js";
export type { KeyPair }              from "./core/security/types.js";
export { BackpressureLevel }         from "./core/router/types.js";

// Advanced: individual layers for testing and custom composition.
// These are considered stable internal APIs, not the primary SDK contract.
export * from "./core/transport/index.js";
export * from "./core/node/index.js";
export * from "./core/crdt/index.js";
export * from "./core/sync/index.js";
export * from "./core/router/index.js";
export * from "./core/security/index.js";
export * from "./errors.js";
