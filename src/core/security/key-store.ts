import { curveKeypair } from "zeromq";
import { SecurityError } from "../../errors.js";
import type { KeyPair } from "./types.js";

// Generate a fresh Curve25519 keypair via libzmq/libsodium.
// Throws SecurityError if the zeromq binary was compiled without CURVE support.
export function generateKeypair(): KeyPair {
  try {
    const kp = curveKeypair();
    return {
      publicKey: new Uint8Array(kp.publicKey),
      secretKey: new Uint8Array(kp.secretKey),
    };
  } catch (err) {
    throw new SecurityError(
      `curveKeypair() failed — zeromq binary may be missing libsodium: ${String(err)}`,
    );
  }
}

export class KeyStore {
  readonly publicKey: Uint8Array;
  readonly secretKey: Uint8Array;

  // hex-keyed allowlist so Set membership is value-based, not reference-based.
  private readonly allowed = new Set<string>();

  constructor(kp: KeyPair) {
    this.publicKey = kp.publicKey;
    this.secretKey = kp.secretKey;
  }

  // Allow a peer public key. Chainable.
  allow(pk: Uint8Array): this {
    this.allowed.add(Buffer.from(pk).toString("hex"));
    return this;
  }

  revoke(pk: Uint8Array): void {
    this.allowed.delete(Buffer.from(pk).toString("hex"));
  }

  isAllowed(pk: Uint8Array): boolean {
    return this.allowed.has(Buffer.from(pk).toString("hex"));
  }

  get allowedCount(): number {
    return this.allowed.size;
  }

  publicKeyHex(): string {
    return Buffer.from(this.publicKey).toString("hex");
  }
}
