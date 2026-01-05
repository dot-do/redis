/**
 * Redois MCP (Model Context Protocol) Module
 *
 * Provides MCP server implementation with Redis tools, HTTP/SSE transport,
 * and sandboxed code execution capabilities.
 *
 * @packageDocumentation
 */

// ─────────────────────────────────────────────────────────────────
// MCP Server
// ─────────────────────────────────────────────────────────────────

export {
  McpServer,
  createMcpServer,
  textContent,
  successResponse,
  errorResponse,
} from './server'

export type {
  RedisAccess,
  CodeLoader,
  CreateMcpServerOptions,
} from './server'

// ─────────────────────────────────────────────────────────────────
// HTTP Transport
// ─────────────────────────────────────────────────────────────────

export {
  createHttpMcpHandler,
  McpSseSession,
  createMessageHandler,
} from './transport/http'

export type {
  HttpMcpHandlerOptions,
  AuthResult,
  RateLimitConfig,
} from './transport/http'

// ─────────────────────────────────────────────────────────────────
// Sandbox / Worker Evaluator
// ─────────────────────────────────────────────────────────────────

export {
  WorkerEvaluator,
  createWorkerEvaluator,
  executeInBasicSandbox,
} from './sandbox/worker-evaluator'

export {
  validateUserCode,
  generateSandboxCode,
  sanitizeCode,
  wrapAsyncCode,
  prepareUserCode,
  generateRedisApiStub,
} from './sandbox/template'

export type { ValidationResult } from './sandbox/template'

// ─────────────────────────────────────────────────────────────────
// Stdio Transport
// ─────────────────────────────────────────────────────────────────

export {
  StdioTransport,
  createStdioTransport,
  runStdioServer,
  formatLogMessage,
  createStdioLogger,
} from './transport/stdio'

export type { StdioServerOptions } from './transport/stdio'

// ─────────────────────────────────────────────────────────────────
// CLI (for stdio transport)
// ─────────────────────────────────────────────────────────────────

export { McpStdioClient, StdioTransport as CliStdioTransport, parseArgs } from './cli'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

export type {
  // JSON-RPC Types
  JsonRpcId,
  JsonRpcRequest,
  JsonRpcResponse,
  JsonRpcError,
  JsonRpcNotification,

  // MCP Protocol Types
  McpRequest,
  McpResponse,
  McpMethod,
  McpResult,

  // Initialize Types
  McpInitializeParams,
  McpInitializeResult,
  McpClientInfo,
  McpServerInfo,
  McpClientCapabilities,
  McpServerCapabilities,

  // Tool Types
  McpToolDefinition,
  McpToolInputSchema,
  McpJsonSchema,
  McpToolAnnotations,
  McpToolsListParams,
  McpToolsListResult,
  McpToolCallParams,
  McpToolCallResult,
  McpToolHandler,
  McpToolContext,
  McpRegisteredTool,

  // Content Types
  McpContent,
  McpTextContent,
  McpImageContent,
  McpResourceContent,
  McpEmbeddedResource,

  // Resource Types
  McpResourceDefinition,
  McpResourceTemplate,
  McpResourcesListParams,
  McpResourcesListResult,
  McpResourceReadParams,
  McpResourceReadResult,
  McpResourceContents,

  // Prompt Types
  McpPromptDefinition,
  McpPromptArgument,
  McpPromptsListParams,
  McpPromptsListResult,
  McpPromptGetParams,
  McpPromptGetResult,
  McpPromptMessage,

  // Logging Types
  McpLogLevel,
  McpLogMessage,
  McpSetLevelParams,

  // Server Config Types
  McpServerConfig,

  // Transport Types
  McpTransport,
  McpTransportOptions,
  McpHttpTransportOptions,
  McpAuthConfig,
  McpRateLimitConfig,
  McpStdioTransportOptions,
  McpSseMessage,

  // Sandbox Types
  SandboxOptions,
  SandboxResult,
  RedisProxy,
  SetOptionsProxy,
  ScanOptionsProxy,
  ZRangeOptionsProxy,
  WorkerEvaluatorOptions,
  WorkerEvaluatorResult,
} from './types'

// Re-export error codes
export { JSON_RPC_ERROR_CODES } from './types'
