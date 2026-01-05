/**
 * Redois - Redis-compatible database on Cloudflare Workers
 *
 * @packageDocumentation
 */

// Durable Objects
export { RedisShard } from './durable-objects/redis-shard'
export { RedisPubSub } from './durable-objects/redis-pubsub'
export { RedisCoordinator } from './durable-objects/redis-coordinator'

// Worker Entrypoint
export { RedoisEntrypoint } from './entrypoint'

// RPC
export {
  RedisRpcTarget,
  ALLOWED_REDIS_COMMANDS,
  REDIS_COMMAND_CATEGORIES,
  READ_ONLY_COMMANDS,
  isReadOnlyCommand,
  RpcClient,
  RpcPipeline,
  RpcClientError,
  createRpcClient,
  handleRpcEndpoint,
  handleRpcWebSocket,
  createRpcHandler,
  RpcEndpointError,
  RPC_ERROR_CODES,
} from './rpc'
export type { RpcClientOptions, RpcEndpointOptions, RpcContext } from './rpc'

// Client
export { Redis, Pipeline } from './client'
export type { PipelineCommand, PipelineExecResult } from './client'

// MCP (Model Context Protocol)
export {
  McpServer,
  createMcpServer,
  textContent,
  successResponse,
  errorResponse,
  createHttpMcpHandler,
  McpSseSession,
  WorkerEvaluator,
  createWorkerEvaluator,
  validateUserCode,
  prepareUserCode,
  JSON_RPC_ERROR_CODES,
} from './mcp'
export type {
  RedisAccess,
  CodeLoader,
  CreateMcpServerOptions,
  HttpMcpHandlerOptions,
  AuthResult,
  McpToolCallResult,
  McpToolContext,
  McpToolHandler,
  McpToolInputSchema,
} from './mcp'

// Types
export type {
  Env,
  SetOptions,
  ScanOptions,
  ZAddOptions,
  ZRangeOptions,
  RedisType,
  KeyMetadata,
  RpcRequest,
  RpcResponse,
  McpToolDefinition,
  McpToolResponse,
  RedisOptions,
} from './types'
