import { encode, decode, ExtensionCodec } from "@msgpack/msgpack";
import type { NodeId, Envelope } from "./types.js";
import { FrameError, IdentityError } from "../../errors.js";
import {
  MessageType,
  FRAME_COUNT, F_IDENTITY, F_TYPE, F_SEQ, F_PAYLOAD,
  ID_LEN, TYPE_LEN, SEQ_LEN,
} from "./constants.js";

// Extension type 1: BigInt as 8-byte signed int64 big-endian.
// Fixed width preserves numeric ordering for range queries; strings don't.
const BIGINT_EXT = 1;
const bigintCodec = new ExtensionCodec();
bigintCodec.register({
  type: BIGINT_EXT,
  encode(v: unknown): Uint8Array | null {
    if (typeof v !== "bigint") return null;
    const buf  = new ArrayBuffer(8);
    const view = new DataView(buf);
    view.setInt32(0,  Number((v >> 32n) & 0xffff_ffffn), false);
    view.setUint32(4, Number(v & 0xffff_ffffn), false);
    return new Uint8Array(buf);
  },
  decode(data: Uint8Array): bigint {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    return (BigInt(view.getInt32(0, false)) << 32n) | BigInt(view.getUint32(4, false));
  },
});

const ENC_OPTS = { extensionCodec: bigintCodec, forceIntegerToFloat: false } as const;
const DEC_OPTS = { extensionCodec: bigintCodec } as const;

function writeu64be(view: DataView, off: number, v: bigint): void {
  view.setUint32(off,     Number((v >> 32n) & 0xffff_ffffn), false);
  view.setUint32(off + 4, Number(v & 0xffff_ffffn), false);
}

function readu64be(view: DataView, off: number): bigint {
  return (BigInt(view.getUint32(off, false)) << 32n) | BigInt(view.getUint32(off + 4, false));
}

export function assemble(
  id: NodeId,
  type: MessageType,
  seq: bigint,
  body: unknown,
): [Uint8Array, Uint8Array, Uint8Array, Uint8Array] {
  const typeBuf = new Uint8Array(TYPE_LEN);
  typeBuf[0] = type;

  const seqBuf = new ArrayBuffer(SEQ_LEN);
  writeu64be(new DataView(seqBuf), 0, seq);

  return [id, typeBuf, new Uint8Array(seqBuf), encode(body, ENC_OPTS)];
}

export function parseEnvelope(frames: Buffer[]): Envelope {
  if (frames.length !== FRAME_COUNT) {
    throw new FrameError(`expected ${FRAME_COUNT} frames, got ${frames.length}`, frames.length);
  }

  const idFrame  = frames[F_IDENTITY]!;
  const typFrame = frames[F_TYPE]!;
  const seqFrame = frames[F_SEQ]!;
  const payFrame = frames[F_PAYLOAD]!;

  if (idFrame.length !== ID_LEN) {
    throw new IdentityError(`identity frame: expected ${ID_LEN}B, got ${idFrame.length}B`);
  }
  if (typFrame.length !== TYPE_LEN || seqFrame.length !== SEQ_LEN) {
    throw new FrameError(`malformed header frames`, frames.length);
  }

  const type = typFrame[0] as MessageType;
  if ((type as number) < 0x01 || (type as number) > 0x07) {
    throw new FrameError(`unknown type byte 0x${(type as number).toString(16)}`, frames.length);
  }

  return {
    senderId: new Uint8Array(idFrame.buffer, idFrame.byteOffset, ID_LEN) as NodeId,
    type,
    seq:     readu64be(new DataView(seqFrame.buffer, seqFrame.byteOffset, SEQ_LEN), 0),
    payload: new Uint8Array(payFrame.buffer, payFrame.byteOffset, payFrame.byteLength),
  };
}

export function decode_payload<T>(raw: Uint8Array): T {
  return decode(raw, DEC_OPTS) as T;
}

export function nodeIdToHex(id: NodeId): string {
  return Buffer.from(id).toString("hex");
}

export function hexToNodeId(hex: string): NodeId {
  if (hex.length !== 32) throw new IdentityError(`nodeId hex must be 32 chars, got ${hex.length}`);
  return Buffer.from(hex, "hex") as unknown as NodeId;
}
