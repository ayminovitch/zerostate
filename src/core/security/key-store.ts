import { curveKeyPair } from "zeromq";
import { SecurityError } from "../../errors.js";
import type { KeyPair } from "./types.js";

// Keys are Z85-encoded strings (40 chars) — the native format zeromq uses.
// Never store or compare raw binary key bytes on the ZMQ API surface.
export function generateKeypair(): KeyPair {
  try {
    return curveKeyPair() as KeyPair;
  } catch (err) {
    throw new SecurityError(
      `curveKeyPair() failed — zeromq binary may lack CURVE support: ${String(err)}`,
    );
  }
}

export class KeyStore {
  readonly publicKey: string;  // Z85-encoded, 40 chars
  readonly secretKey: string;  // Z85-encoded, 40 chars

  // Set<string> is safe: Z85 keys are already unique printable strings.
  private readonly allowed = new Set<string>();

  constructor(kp: KeyPair) {
    this.publicKey = kp.publicKey;
    this.secretKey = kp.secretKey;
  }

  // Allow a peer by its Z85 public key. Chainable.
  allow(pk: string): this {
    this.allowed.add(pk);
    return this;
  }

  revoke(pk: string): void {
    this.allowed.delete(pk);
  }

  // ZAP delivers credentials as raw 32-byte Curve25519 key. We Z85-encode
  // it here before the allowlist lookup since our store holds Z85 strings.
  isAllowed(rawPk: Uint8Array): boolean {
    const z85 = encodeZ85(rawPk);
    return z85 !== null && this.allowed.has(z85);
  }

  get allowedCount(): number { return this.allowed.size; }
}

// Z85 encoder per ZMQ RFC 32. ZAP delivers raw 32-byte keys, not Z85.
// 32 bytes → 5 groups of 5 Z85 chars = 40 chars.
const Z85_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

function encodeZ85(raw: Uint8Array): string | null {
  if (raw.length !== 32) return null;
  const out = new Array<string>(40);
  for (let i = 0; i < 8; i++) {
    const base = i * 4;
    let v = (raw[base]! << 24 | raw[base + 1]! << 16 | raw[base + 2]! << 8 | raw[base + 3]!) >>> 0;
    for (let j = 4; j >= 0; j--) {
      out[i * 5 + j] = Z85_CHARS[v % 85]!;
      v = Math.floor(v / 85);
    }
  }
  return out.join("");
}
