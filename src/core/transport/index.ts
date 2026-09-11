export { Transport }                          from "./transport.js";
export type { TransportEvents }               from "./transport.js";
export { mkNodeId, nodeIdsEq, cmpNodeId }     from "./identity.js";
export { nodeIdToHex, hexToNodeId, decode_payload as decodePayload } from "./codec.js";
export { getCtx, setCtx, destroyCtx }         from "./context.js";
export type {
  NodeId, Envelope,
  DeltaPayload, HeartbeatPayload,
  SyncReqPayload, SyncResPayload,
  PeerPayload, TransportCfg, TransportStats,
} from "./types.js";
export { MessageType }            from "./constants.js";
export {
  ZMQ_IO_THREADS, ZMQ_MAX_SOCKETS,
  HWM_SEND, HWM_RECV, LINGER_MS,
  BASE_PUB_PORT, BASE_ROUTER_PORT,
} from "./constants.js";
