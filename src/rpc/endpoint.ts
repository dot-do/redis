/**
 * RPC Endpoint - HTTP POST handler for /rpc endpoint
 *
 * Features:
 * - JSON-RPC 2.0 compliant
 * - Single and batch request handling
 * - CORS headers for cross-origin requests
 * - Method validation and error handling
 */

import type { RpcRequest, RpcResponse, Env } from '../types'
import { RedisRpcTarget, ALLOWED_REDIS_COMMANDS } from './redis-rpc-target'

// ─────────────────────────────────────────────────────────────────
// JSON-RPC 2.0 Error Codes
// ─────────────────────────────────────────────────────────────────

export const RPC_ERROR_CODES = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
  // Server errors: -32000 to -32099
  SERVER_ERROR: -32000,
  UNAUTHORIZED: -32001,
  FORBIDDEN: -32002,
  NOT_FOUND: -32003,
  TIMEOUT: -32004,
} as const

// ─────────────────────────────────────────────────────────────────
// CORS Configuration
// ─────────────────────────────────────────────────────────────────

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Request-ID',
  'Access-Control-Max-Age': '86400',
  'Access-Control-Expose-Headers': 'X-Request-ID, X-RPC-Batch-Size',
} as const

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

export interface RpcEndpointOptions {
  /** Maximum batch size allowed (default: 1000) */
  maxBatchSize?: number
  /** Request timeout in milliseconds (default: 30000) */
  timeoutMs?: number
  /** Enable detailed error messages (default: false in production) */
  detailedErrors?: boolean
  /** Custom CORS origins (default: '*') */
  allowedOrigins?: string[]
}

export interface RpcContext {
  /** Environment bindings */
  env: Env
  /** Execution context */
  ctx: ExecutionContext
  /** Authenticated user/token info */
  auth?: {
    token: string
    valid: boolean
  }
}

type RpcTargetMethod = (...args: unknown[]) => Promise<unknown>

// ─────────────────────────────────────────────────────────────────
// RPC Endpoint Handler
// ─────────────────────────────────────────────────────────────────

/**
 * Handle RPC endpoint requests
 */
export async function handleRpcEndpoint(
  request: Request,
  target: RedisRpcTarget,
  _context: RpcContext,
  options: RpcEndpointOptions = {}
): Promise<Response> {
  const { maxBatchSize = 1000, timeoutMs = 30000, detailedErrors = false } = options

  // Handle CORS preflight
  if (request.method === 'OPTIONS') {
    return createCorsPreflightResponse(options.allowedOrigins)
  }

  // Only allow POST
  if (request.method !== 'POST') {
    return createErrorResponse(
      null,
      RPC_ERROR_CODES.INVALID_REQUEST,
      'Method not allowed. Use POST.',
      405
    )
  }

  // Validate content type
  const contentType = request.headers.get('Content-Type')
  if (!contentType?.includes('application/json')) {
    return createErrorResponse(
      null,
      RPC_ERROR_CODES.INVALID_REQUEST,
      'Content-Type must be application/json',
      400
    )
  }

  // Parse request body
  let body: unknown
  try {
    body = await request.json()
  } catch {
    return createErrorResponse(
      null,
      RPC_ERROR_CODES.PARSE_ERROR,
      'Invalid JSON',
      400
    )
  }

  // Handle batch or single request
  const isBatch = Array.isArray(body)

  if (isBatch) {
    const requests = body as unknown[]

    // Validate batch size
    if (requests.length > maxBatchSize) {
      return createErrorResponse(
        null,
        RPC_ERROR_CODES.INVALID_REQUEST,
        `Batch size exceeds maximum of ${maxBatchSize}`,
        400
      )
    }

    if (requests.length === 0) {
      return createErrorResponse(
        null,
        RPC_ERROR_CODES.INVALID_REQUEST,
        'Empty batch',
        400
      )
    }

    // Process batch
    const results = await Promise.all(
      requests.map((req) =>
        processRequest(req as RpcRequest, target, timeoutMs, detailedErrors)
      )
    )

    return createJsonResponse(results, 200, {
      'X-RPC-Batch-Size': String(requests.length),
    })
  } else {
    // Single request
    const result = await processRequest(
      body as RpcRequest,
      target,
      timeoutMs,
      detailedErrors
    )
    return createJsonResponse(result, result.error ? 400 : 200)
  }
}

// ─────────────────────────────────────────────────────────────────
// Request Processing
// ─────────────────────────────────────────────────────────────────

/**
 * Process a single RPC request
 */
async function processRequest(
  request: unknown,
  target: RedisRpcTarget,
  timeoutMs: number,
  detailedErrors: boolean
): Promise<RpcResponse> {
  // Validate request structure
  const validation = validateRequest(request)
  if (validation.error) {
    return createRpcError(
      (request as RpcRequest)?.id ?? null,
      validation.error.code,
      validation.error.message
    )
  }

  const rpcRequest = request as RpcRequest
  const { id, method, params } = rpcRequest

  // Validate method is allowed
  if (!ALLOWED_REDIS_COMMANDS.has(method.toLowerCase())) {
    return createRpcError(
      id,
      RPC_ERROR_CODES.METHOD_NOT_FOUND,
      `Method '${method}' not found or not allowed`
    )
  }

  // Get method from target
  const methodFn = (target as unknown as Record<string, RpcTargetMethod>)[method.toLowerCase()]
  if (typeof methodFn !== 'function') {
    return createRpcError(
      id,
      RPC_ERROR_CODES.METHOD_NOT_FOUND,
      `Method '${method}' not implemented`
    )
  }

  // Execute method with timeout
  try {
    const result = await executeWithTimeout(
      () => invokeMethod(methodFn.bind(target), params),
      timeoutMs
    )

    return {
      jsonrpc: '2.0',
      id,
      result,
    }
  } catch (error) {
    const errorInfo = normalizeError(error, detailedErrors)
    return createRpcError(id, errorInfo.code, errorInfo.message, errorInfo.data)
  }
}

/**
 * Invoke a method with parameters
 */
function invokeMethod(
  method: RpcTargetMethod,
  params?: Record<string, unknown>
): Promise<unknown> {
  if (!params || Object.keys(params).length === 0) {
    return method()
  }

  // Convert params object to ordered arguments based on common patterns
  // Redis commands typically use positional args, so we support both
  // named params (for complex options) and array params (for positional)
  if (Array.isArray(params)) {
    return method(...params)
  }

  // For named params, try to extract common Redis argument patterns
  const args = extractMethodArgs(params)
  return method(...args)
}

/**
 * Extract method arguments from named parameters
 */
function extractMethodArgs(params: Record<string, unknown>): unknown[] {
  const args: unknown[] = []

  // Extract in order of common Redis command patterns
  if ('key' in params) args.push(params['key'])
  if ('keys' in params && Array.isArray(params['keys'])) {
    args.push(...(params['keys'] as unknown[]))
  }
  if ('field' in params) args.push(params['field'])
  if ('fields' in params && Array.isArray(params['fields'])) {
    args.push(...(params['fields'] as unknown[]))
  }
  if ('value' in params) args.push(params['value'])
  if ('values' in params && Array.isArray(params['values'])) {
    args.push(...(params['values'] as unknown[]))
  }
  if ('member' in params) args.push(params['member'])
  if ('members' in params && Array.isArray(params['members'])) {
    args.push(...(params['members'] as unknown[]))
  }
  if ('score' in params) args.push(params['score'])
  if ('cursor' in params) args.push(params['cursor'])
  if ('pattern' in params) args.push(params['pattern'])
  if ('start' in params) args.push(params['start'])
  if ('stop' in params) args.push(params['stop'])
  if ('min' in params) args.push(params['min'])
  if ('max' in params) args.push(params['max'])
  if ('count' in params) args.push(params['count'])
  if ('options' in params) args.push(params['options'])

  // If we couldn't extract known patterns, just use values in iteration order
  if (args.length === 0) {
    for (const value of Object.values(params)) {
      args.push(value)
    }
  }

  return args
}

// ─────────────────────────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────────────────────────

interface ValidationResult {
  error?: {
    code: number
    message: string
  }
}

/**
 * Validate JSON-RPC 2.0 request structure
 */
function validateRequest(request: unknown): ValidationResult {
  if (!request || typeof request !== 'object') {
    return {
      error: {
        code: RPC_ERROR_CODES.INVALID_REQUEST,
        message: 'Request must be an object',
      },
    }
  }

  const req = request as Record<string, unknown>

  // Check jsonrpc version
  if (req['jsonrpc'] !== '2.0') {
    return {
      error: {
        code: RPC_ERROR_CODES.INVALID_REQUEST,
        message: 'Invalid JSON-RPC version. Must be "2.0"',
      },
    }
  }

  // Check id (required for requests expecting response)
  if (!('id' in req) || (typeof req['id'] !== 'string' && typeof req['id'] !== 'number')) {
    return {
      error: {
        code: RPC_ERROR_CODES.INVALID_REQUEST,
        message: 'Request id must be a string or number',
      },
    }
  }

  // Check method
  if (typeof req['method'] !== 'string' || req['method'].length === 0) {
    return {
      error: {
        code: RPC_ERROR_CODES.INVALID_REQUEST,
        message: 'Method must be a non-empty string',
      },
    }
  }

  // Check params (optional, but must be object or array if present)
  if ('params' in req && req['params'] !== undefined) {
    if (typeof req['params'] !== 'object') {
      return {
        error: {
          code: RPC_ERROR_CODES.INVALID_PARAMS,
          message: 'Params must be an object or array',
        },
      }
    }
  }

  return {}
}

// ─────────────────────────────────────────────────────────────────
// Error Handling
// ─────────────────────────────────────────────────────────────────

interface NormalizedError {
  code: number
  message: string
  data?: unknown
}

/**
 * Normalize error to RPC error format
 */
function normalizeError(error: unknown, detailed: boolean): NormalizedError {
  if (error instanceof RpcEndpointError) {
    return {
      code: error.code,
      message: error.message,
      data: detailed ? error.data : undefined,
    }
  }

  if (error instanceof Error) {
    // Check for known error types
    if (error.name === 'TimeoutError') {
      return {
        code: RPC_ERROR_CODES.TIMEOUT,
        message: 'Request timeout',
      }
    }

    return {
      code: RPC_ERROR_CODES.INTERNAL_ERROR,
      message: detailed ? error.message : 'Internal error',
      data: detailed ? { stack: error.stack } : undefined,
    }
  }

  return {
    code: RPC_ERROR_CODES.INTERNAL_ERROR,
    message: 'Unknown error',
  }
}

/**
 * Create an RPC error response
 */
function createRpcError(
  id: string | number | null,
  code: number,
  message: string,
  data?: unknown
): RpcResponse {
  return {
    jsonrpc: '2.0',
    id: id ?? 0,
    error: {
      code,
      message,
      data,
    },
  }
}

// ─────────────────────────────────────────────────────────────────
// Response Helpers
// ─────────────────────────────────────────────────────────────────

/**
 * Create a JSON response with CORS headers
 */
function createJsonResponse(
  data: unknown,
  status: number,
  additionalHeaders?: Record<string, string>
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...CORS_HEADERS,
      ...additionalHeaders,
    },
  })
}

/**
 * Create an error response
 */
function createErrorResponse(
  id: string | number | null,
  code: number,
  message: string,
  httpStatus: number
): Response {
  return createJsonResponse(createRpcError(id, code, message), httpStatus)
}

/**
 * Create CORS preflight response
 */
function createCorsPreflightResponse(allowedOrigins?: string[]): Response {
  const headers: Record<string, string> = { ...CORS_HEADERS }

  if (allowedOrigins && allowedOrigins.length > 0) {
    headers['Access-Control-Allow-Origin'] = allowedOrigins.join(', ')
  }

  return new Response(null, {
    status: 204,
    headers,
  })
}

// ─────────────────────────────────────────────────────────────────
// Timeout Utility
// ─────────────────────────────────────────────────────────────────

/**
 * Execute a function with timeout
 */
async function executeWithTimeout<T>(
  fn: () => Promise<T>,
  timeoutMs: number
): Promise<T> {
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      const error = new Error('Request timeout')
      error.name = 'TimeoutError'
      reject(error)
    }, timeoutMs)
  })

  return Promise.race([fn(), timeoutPromise])
}

// ─────────────────────────────────────────────────────────────────
// Custom Error Class
// ─────────────────────────────────────────────────────────────────

/**
 * Custom error for RPC endpoint
 */
export class RpcEndpointError extends Error {
  readonly code: number
  readonly data?: unknown

  constructor(message: string, code: number, data?: unknown) {
    super(message)
    this.name = 'RpcEndpointError'
    this.code = code
    this.data = data
  }
}

// ─────────────────────────────────────────────────────────────────
// WebSocket Handler
// ─────────────────────────────────────────────────────────────────

/**
 * Handle WebSocket upgrade for RPC
 */
export function handleRpcWebSocket(
  request: Request,
  target: RedisRpcTarget,
  _context: RpcContext,
  options: RpcEndpointOptions = {}
): Response {
  const { timeoutMs = 30000, detailedErrors = false } = options

  const upgradeHeader = request.headers.get('Upgrade')
  if (upgradeHeader !== 'websocket') {
    return new Response('Expected WebSocket', { status: 426 })
  }

  const webSocketPair = new WebSocketPair()
  const [client, server] = Object.values(webSocketPair)

  server.accept()

  server.addEventListener('message', async (event) => {
    try {
      const message = JSON.parse(event.data as string) as {
        type: string
        payload: unknown
      }

      if (message.type === 'request') {
        const result = await processRequest(
          message.payload,
          target,
          timeoutMs,
          detailedErrors
        )
        server.send(JSON.stringify({ type: 'response', payload: result }))
      } else if (message.type === 'ping') {
        server.send(JSON.stringify({ type: 'pong' }))
      }
    } catch {
      server.send(
        JSON.stringify({
          type: 'error',
          payload: { message: 'Invalid message format' },
        })
      )
    }
  })

  server.addEventListener('close', () => {
    // Cleanup if needed
  })

  return new Response(null, {
    status: 101,
    webSocket: client,
  })
}

// ─────────────────────────────────────────────────────────────────
// Route Handler Factory
// ─────────────────────────────────────────────────────────────────

/**
 * Create a route handler for the RPC endpoint
 */
export function createRpcHandler(options: RpcEndpointOptions = {}) {
  return async (
    request: Request,
    target: RedisRpcTarget,
    context: RpcContext
  ): Promise<Response> => {
    const url = new URL(request.url)

    // Handle WebSocket upgrade
    if (url.pathname.endsWith('/ws')) {
      return handleRpcWebSocket(request, target, context, options)
    }

    return handleRpcEndpoint(request, target, context, options)
  }
}
