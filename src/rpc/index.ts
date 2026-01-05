/**
 * Redois RPC Layer
 *
 * Provides JSON-RPC 2.0 support for Redis commands over HTTP/WebSocket.
 *
 * Components:
 * - RedisRpcTarget: Abstract class defining all Redis commands (DO IS the RpcTarget)
 * - RpcClient: Client with batching, deduplication, and WebSocket support
 * - Endpoint handlers: HTTP and WebSocket request processing
 *
 * @packageDocumentation
 */

// ─────────────────────────────────────────────────────────────────
// RPC Target
// ─────────────────────────────────────────────────────────────────

export {
  RedisRpcTarget,
  ALLOWED_REDIS_COMMANDS,
  REDIS_COMMAND_CATEGORIES,
  READ_ONLY_COMMANDS,
  isReadOnlyCommand,
} from './redis-rpc-target'

// ─────────────────────────────────────────────────────────────────
// RPC Client
// ─────────────────────────────────────────────────────────────────

export {
  RpcClient,
  RpcPipeline,
  RpcClientError,
  createRpcClient,
  type RpcClientOptions,
} from './rpc-client'

// ─────────────────────────────────────────────────────────────────
// RPC Endpoint
// ─────────────────────────────────────────────────────────────────

export {
  handleRpcEndpoint,
  handleRpcWebSocket,
  createRpcHandler,
  RpcEndpointError,
  RPC_ERROR_CODES,
  type RpcEndpointOptions,
  type RpcContext,
} from './endpoint'
