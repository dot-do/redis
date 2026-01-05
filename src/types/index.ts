/**
 * Redois Type Definitions
 */

import type { DurableObjectNamespace } from '@cloudflare/workers-types'

// ─────────────────────────────────────────────────────────────────
// Environment Types
// ─────────────────────────────────────────────────────────────────

export interface Env {
  REDIS_SHARDS: DurableObjectNamespace
  REDIS_PUBSUB: DurableObjectNamespace
  REDIS_COORDINATOR: DurableObjectNamespace
  LOADER?: WorkerLoader
  AUTH_TOKEN?: string
  ENVIRONMENT?: string
}

// ─────────────────────────────────────────────────────────────────
// Worker Loader Types (for sandboxed code execution)
// ─────────────────────────────────────────────────────────────────

export interface WorkerLoader {
  get(id: string, getCode: () => Promise<WorkerCode>): WorkerStub
}

export interface WorkerCode {
  compatibilityDate: string
  compatibilityFlags: string[]
  mainModule: string
  modules: Record<string, string>
  env: Record<string, unknown>
  globalOutbound: null
}

export interface WorkerStub {
  getEntrypoint(name?: string): WorkerEntrypoint
}

export interface WorkerEntrypoint {
  fetch(request: Request): Promise<Response>
}

// ─────────────────────────────────────────────────────────────────
// Redis Command Options
// ─────────────────────────────────────────────────────────────────

export interface SetOptions {
  ex?: number      // Seconds
  px?: number      // Milliseconds
  exat?: number    // Unix timestamp seconds
  pxat?: number    // Unix timestamp milliseconds
  nx?: boolean     // Only set if not exists
  xx?: boolean     // Only set if exists
  keepttl?: boolean
  get?: boolean    // Return old value
}

export interface ScanOptions {
  match?: string
  count?: number
  type?: string
}

export interface ZAddOptions {
  nx?: boolean
  xx?: boolean
  gt?: boolean
  lt?: boolean
  ch?: boolean
}

export interface ZRangeOptions {
  withScores?: boolean
  rev?: boolean
  byScore?: boolean
  byLex?: boolean
  limit?: { offset: number; count: number }
}

// ─────────────────────────────────────────────────────────────────
// Redis Data Types
// ─────────────────────────────────────────────────────────────────

export type RedisType = 'string' | 'hash' | 'list' | 'set' | 'zset' | 'stream' | 'none'

export interface KeyMetadata {
  key: string
  type: RedisType
  expiresAt: number | null
  createdAt: number
  updatedAt: number
}

// ─────────────────────────────────────────────────────────────────
// RPC Types
// ─────────────────────────────────────────────────────────────────

export interface RpcRequest {
  jsonrpc: '2.0'
  id: string | number
  method: string
  params?: Record<string, unknown>
}

export interface RpcResponse {
  jsonrpc: '2.0'
  id: string | number
  result?: unknown
  error?: RpcError
}

export interface RpcError {
  code: number
  message: string
  data?: unknown
}

// ─────────────────────────────────────────────────────────────────
// MCP Types
// ─────────────────────────────────────────────────────────────────

export interface McpToolDefinition {
  name: string
  description?: string
  inputSchema: object
  annotations?: {
    title?: string
    readOnlyHint?: boolean
    destructiveHint?: boolean
  }
}

export interface McpToolResponse {
  content: Array<{ type: 'text'; text: string }>
  isError?: boolean
}

export type McpToolHandler<T = Record<string, unknown>> = (args: T) => Promise<McpToolResponse>

// ─────────────────────────────────────────────────────────────────
// Client Types
// ─────────────────────────────────────────────────────────────────

export interface RedisOptions {
  /** Base URL for the Redois endpoint */
  url: string
  /** Authorization token */
  token?: string
  /** Enable automatic request pipelining (batches commands in microtask) */
  enableAutoPipelining?: boolean
  /** Auto-pipeline batch window in milliseconds (default: 0 - microtask) */
  autoPipelineWindowMs?: number
  /** Maximum batch size for auto-pipelining (default: 100) */
  maxAutoPipelineBatchSize?: number
  /** Use WebSocket transport for real-time operations */
  useWebSocket?: boolean
  /** WebSocket reconnect interval in milliseconds (default: 1000) */
  wsReconnectIntervalMs?: number
  /** Request timeout in milliseconds (default: 30000) */
  timeoutMs?: number
  /** Enable request deduplication for identical concurrent requests */
  enableDeduplication?: boolean
  /** Deduplication TTL in milliseconds (default: 100) */
  deduplicationTtlMs?: number
  /** Custom fetch implementation (for testing or custom transports) */
  fetch?: typeof globalThis.fetch
  /** Retry failed requests (default: true) */
  retryOnError?: boolean
  /** Maximum number of retries (default: 3) */
  maxRetries?: number
  /** Retry delay in milliseconds (default: 100) */
  retryDelayMs?: number
  /** Key prefix to prepend to all keys */
  keyPrefix?: string
  /** Enable read-only mode (rejects write commands) */
  readOnly?: boolean
  /** Connection name for debugging */
  name?: string
  /** Enable latency monitoring */
  enableLatencyMonitoring?: boolean
}

export interface PipelineResult {
  error: Error | null
  value: unknown
}

export interface RedisClientEvents {
  connect: () => void
  ready: () => void
  error: (error: Error) => void
  close: () => void
  reconnecting: (delay: number) => void
  end: () => void
}
