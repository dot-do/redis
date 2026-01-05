/**
 * MCP Protocol Types
 *
 * JSON-RPC 2.0 based Model Context Protocol type definitions
 * @see https://modelcontextprotocol.io/specification
 */

// ─────────────────────────────────────────────────────────────────
// JSON-RPC Base Types
// ─────────────────────────────────────────────────────────────────

export type JsonRpcId = string | number | null

export interface JsonRpcRequest {
  jsonrpc: '2.0'
  id?: JsonRpcId
  method: string
  params?: Record<string, unknown> | unknown[]
}

export interface JsonRpcResponse {
  jsonrpc: '2.0'
  id: JsonRpcId
  result?: unknown
  error?: JsonRpcError
}

export interface JsonRpcError {
  code: number
  message: string
  data?: unknown
}

export interface JsonRpcNotification {
  jsonrpc: '2.0'
  method: string
  params?: Record<string, unknown>
}

// Standard JSON-RPC error codes
export const JSON_RPC_ERROR_CODES = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
} as const

// ─────────────────────────────────────────────────────────────────
// MCP Protocol Types
// ─────────────────────────────────────────────────────────────────

export interface McpRequest extends JsonRpcRequest {
  method: McpMethod
}

export interface McpResponse extends JsonRpcResponse {
  result?: McpResult
}

export type McpMethod =
  | 'initialize'
  | 'initialized'
  | 'ping'
  | 'tools/list'
  | 'tools/call'
  | 'resources/list'
  | 'resources/read'
  | 'prompts/list'
  | 'prompts/get'
  | 'logging/setLevel'
  | 'completion/complete'
  | 'notifications/cancelled'

export type McpResult =
  | McpInitializeResult
  | McpToolsListResult
  | McpToolCallResult
  | McpResourcesListResult
  | McpResourceReadResult
  | McpPromptsListResult
  | McpPromptGetResult
  | McpPingResult
  | McpEmptyResult

// ─────────────────────────────────────────────────────────────────
// Initialize Types
// ─────────────────────────────────────────────────────────────────

export interface McpInitializeParams {
  protocolVersion: string
  capabilities: McpClientCapabilities
  clientInfo: McpClientInfo
}

export interface McpClientInfo {
  name: string
  version: string
}

export interface McpServerInfo {
  name: string
  version: string
}

export interface McpClientCapabilities {
  experimental?: Record<string, unknown>
  roots?: {
    listChanged?: boolean
  }
  sampling?: Record<string, unknown>
}

export interface McpServerCapabilities {
  experimental?: Record<string, unknown>
  logging?: Record<string, unknown>
  prompts?: {
    listChanged?: boolean
  }
  resources?: {
    subscribe?: boolean
    listChanged?: boolean
  }
  tools?: {
    listChanged?: boolean
  }
}

export interface McpInitializeResult {
  protocolVersion: string
  capabilities: McpServerCapabilities
  serverInfo: McpServerInfo
  instructions?: string | undefined
}

// ─────────────────────────────────────────────────────────────────
// Tool Types
// ─────────────────────────────────────────────────────────────────

export interface McpToolDefinition {
  name: string
  description?: string | undefined
  inputSchema: McpToolInputSchema
  annotations?: McpToolAnnotations | undefined
}

export interface McpToolInputSchema {
  type: 'object'
  properties?: Record<string, McpJsonSchema>
  required?: string[]
  additionalProperties?: boolean
}

export interface McpJsonSchema {
  type: 'string' | 'number' | 'integer' | 'boolean' | 'array' | 'object' | 'null'
  description?: string
  enum?: unknown[]
  items?: McpJsonSchema
  properties?: Record<string, McpJsonSchema>
  required?: string[]
  default?: unknown
  minimum?: number
  maximum?: number
  minLength?: number
  maxLength?: number
  pattern?: string
}

export interface McpToolAnnotations {
  title?: string
  readOnlyHint?: boolean
  destructiveHint?: boolean
  idempotentHint?: boolean
  openWorldHint?: boolean
}

export interface McpToolsListParams {
  cursor?: string
}

export interface McpToolsListResult {
  tools: McpToolDefinition[]
  nextCursor?: string
}

export interface McpToolCallParams {
  name: string
  arguments?: Record<string, unknown>
}

export interface McpToolCallResult {
  content: McpContent[]
  isError?: boolean
}

// ─────────────────────────────────────────────────────────────────
// Content Types
// ─────────────────────────────────────────────────────────────────

export type McpContent = McpTextContent | McpImageContent | McpResourceContent

export interface McpTextContent {
  type: 'text'
  text: string
}

export interface McpImageContent {
  type: 'image'
  data: string // base64
  mimeType: string
}

export interface McpResourceContent {
  type: 'resource'
  resource: McpEmbeddedResource
}

export interface McpEmbeddedResource {
  uri: string
  mimeType?: string
  text?: string
  blob?: string // base64
}

// ─────────────────────────────────────────────────────────────────
// Resource Types
// ─────────────────────────────────────────────────────────────────

export interface McpResourceDefinition {
  uri: string
  name: string
  description?: string
  mimeType?: string
}

export interface McpResourceTemplate {
  uriTemplate: string
  name: string
  description?: string
  mimeType?: string
}

export interface McpResourcesListParams {
  cursor?: string
}

export interface McpResourcesListResult {
  resources: McpResourceDefinition[]
  nextCursor?: string
}

export interface McpResourceReadParams {
  uri: string
}

export interface McpResourceReadResult {
  contents: McpResourceContents[]
}

export interface McpResourceContents {
  uri: string
  mimeType?: string
  text?: string
  blob?: string // base64
}

// ─────────────────────────────────────────────────────────────────
// Prompt Types
// ─────────────────────────────────────────────────────────────────

export interface McpPromptDefinition {
  name: string
  description?: string
  arguments?: McpPromptArgument[]
}

export interface McpPromptArgument {
  name: string
  description?: string
  required?: boolean
}

export interface McpPromptsListParams {
  cursor?: string
}

export interface McpPromptsListResult {
  prompts: McpPromptDefinition[]
  nextCursor?: string
}

export interface McpPromptGetParams {
  name: string
  arguments?: Record<string, string>
}

export interface McpPromptGetResult {
  description?: string
  messages: McpPromptMessage[]
}

export interface McpPromptMessage {
  role: 'user' | 'assistant'
  content: McpTextContent | McpImageContent | McpResourceContent
}

// ─────────────────────────────────────────────────────────────────
// Other Response Types
// ─────────────────────────────────────────────────────────────────

export interface McpPingResult {
  // Empty object for ping response
}

export interface McpEmptyResult {
  // Empty result
}

// ─────────────────────────────────────────────────────────────────
// Logging Types
// ─────────────────────────────────────────────────────────────────

export type McpLogLevel = 'debug' | 'info' | 'notice' | 'warning' | 'error' | 'critical' | 'alert' | 'emergency'

export interface McpLogMessage {
  level: McpLogLevel
  logger?: string
  data: unknown
}

export interface McpSetLevelParams {
  level: McpLogLevel
}

// ─────────────────────────────────────────────────────────────────
// Server Configuration Types
// ─────────────────────────────────────────────────────────────────

export interface McpServerConfig {
  name: string
  version: string
  instructions?: string | undefined
  capabilities?: Partial<McpServerCapabilities> | undefined
}

export interface McpToolHandler<T = Record<string, unknown>> {
  (args: T, context: McpToolContext): Promise<McpToolCallResult>
}

export interface McpToolContext {
  requestId: JsonRpcId
  signal?: AbortSignal
}

export interface McpRegisteredTool {
  definition: McpToolDefinition
  handler: McpToolHandler
}

// ─────────────────────────────────────────────────────────────────
// Transport Types
// ─────────────────────────────────────────────────────────────────

export interface McpTransport {
  start(): Promise<void>
  send(message: JsonRpcResponse | JsonRpcNotification): Promise<void>
  close(): Promise<void>
  onMessage?: (message: JsonRpcRequest) => void
  onClose?: () => void
  onError?: (error: Error) => void
}

export interface McpTransportOptions {
  timeout?: number
}

export interface McpHttpTransportOptions extends McpTransportOptions {
  cors?: boolean
  corsOrigins?: string[]
  auth?: McpAuthConfig
  rateLimit?: McpRateLimitConfig
}

export interface McpAuthConfig {
  type: 'bearer' | 'basic' | 'custom'
  validate: (credentials: string) => Promise<boolean>
}

export interface McpRateLimitConfig {
  windowMs: number
  maxRequests: number
}

export interface McpStdioTransportOptions extends McpTransportOptions {
  stdin?: NodeJS.ReadableStream
  stdout?: NodeJS.WritableStream
}

// ─────────────────────────────────────────────────────────────────
// SSE Types
// ─────────────────────────────────────────────────────────────────

export interface McpSseMessage {
  event?: string
  data: string
  id?: string
  retry?: number
}

// ─────────────────────────────────────────────────────────────────
// Sandbox Types
// ─────────────────────────────────────────────────────────────────

export interface SandboxOptions {
  timeout?: number | undefined
  memoryLimit?: number | undefined
  allowedGlobals?: string[] | undefined
}

export interface SandboxResult {
  success: boolean
  result?: unknown
  error?: string
  logs: string[]
  executionTime: number
}

export interface RedisProxy {
  get(key: string): Promise<string | null>
  set(key: string, value: string, options?: SetOptionsProxy): Promise<string>
  del(...keys: string[]): Promise<number>
  exists(...keys: string[]): Promise<number>
  expire(key: string, seconds: number): Promise<number>
  ttl(key: string): Promise<number>
  keys(pattern: string): Promise<string[]>
  scan(cursor: number, options?: ScanOptionsProxy): Promise<[string, string[]]>
  hget(key: string, field: string): Promise<string | null>
  hset(key: string, field: string, value: string): Promise<number>
  hgetall(key: string): Promise<Record<string, string>>
  hdel(key: string, ...fields: string[]): Promise<number>
  lpush(key: string, ...values: string[]): Promise<number>
  rpush(key: string, ...values: string[]): Promise<number>
  lpop(key: string): Promise<string | null>
  rpop(key: string): Promise<string | null>
  lrange(key: string, start: number, stop: number): Promise<string[]>
  sadd(key: string, ...members: string[]): Promise<number>
  smembers(key: string): Promise<string[]>
  srem(key: string, ...members: string[]): Promise<number>
  sismember(key: string, member: string): Promise<number>
  zadd(key: string, score: number, member: string): Promise<number>
  zrange(key: string, start: number, stop: number, options?: ZRangeOptionsProxy): Promise<string[]>
  zrem(key: string, ...members: string[]): Promise<number>
  zscore(key: string, member: string): Promise<string | null>
  incr(key: string): Promise<number>
  incrby(key: string, increment: number): Promise<number>
  decr(key: string): Promise<number>
  decrby(key: string, decrement: number): Promise<number>
  append(key: string, value: string): Promise<number>
  strlen(key: string): Promise<number>
  type(key: string): Promise<string>
  rename(key: string, newKey: string): Promise<string>
  dbsize(): Promise<number>
  flushdb(): Promise<string>
  info(section?: string): Promise<string>
  ping(message?: string): Promise<string>
}

export interface SetOptionsProxy {
  ex?: number | undefined
  px?: number | undefined
  nx?: boolean | undefined
  xx?: boolean | undefined
}

export interface ScanOptionsProxy {
  match?: string | undefined
  count?: number | undefined
  type?: string | undefined
}

export interface ZRangeOptionsProxy {
  withScores?: boolean | undefined
}

export interface WorkerEvaluatorOptions extends SandboxOptions {
  compatibilityDate?: string | undefined
  compatibilityFlags?: string[] | undefined
}

export interface WorkerEvaluatorResult extends SandboxResult {
  workerId: string
}
