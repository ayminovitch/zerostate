export interface KeyPair {
  readonly publicKey: string;  // Z85-encoded, 40 chars
  readonly secretKey: string;  // Z85-encoded, 40 chars
}

export interface SecurityCfg {
  readonly keyStore: KeyStore;
}

import type { KeyStore } from "./key-store.js";
