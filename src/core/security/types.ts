export interface KeyPair {
  readonly publicKey: Uint8Array;  // 32-byte Curve25519 public key (z85-decodable)
  readonly secretKey: Uint8Array;  // 32-byte Curve25519 secret key
}

export interface SecurityCfg {
  readonly keyStore: KeyStore;
}

// Re-export so Transport can accept SecurityCfg without importing KeyStore directly.
import type { KeyStore } from "./key-store.js";
