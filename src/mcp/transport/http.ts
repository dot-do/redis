/**
 * HTTP Transport for MCP Server
 *
 * Handles MCP protocol over HTTP with SSE for streaming responses.
 * Supports both regular JSON-RPC requests and Server-Sent Events.
 * Includes CORS, authentication, and rate limiting support.
 */

import type {
  JsonRpcRequest,
  JsonRpcResponse,
  McpSseMessage,
} from '../types'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

export interface RateLimitConfig {
  /** Time window in milliseconds */
  windowMs: number
  /** Maximum requests per window */
  maxRequests: number
}

export interface HttpMcpHandlerOptions {
  /** Custom authentication handler */
  authenticate?: (request: Request) => Promise<AuthResult>
  /** CORS origins (default: '*') */
  corsOrigins?: string[]
  /** Request timeout in ms (default: 30000) */
  timeout?: number
  /** Enable SSE mode for responses */
  enableSse?: boolean
  /** Rate limiting configuration */
  rateLimit?: RateLimitConfig
  /** Base path for MCP endpoints (default: '/mcp') */
  basePath?: string
}

export interface AuthResult {
  authenticated: boolean
  userId?: string
  error?: string
}

export interface McpServerInterface {
  handleRequest(request: JsonRpcRequest): Promise<JsonRpcResponse | null>
  getServerInfo(): { name: string; version: string }
}

// ─────────────────────────────────────────────────────────────────
// CORS Headers
// ─────────────────────────────────────────────────────────────────

function getCorsHeaders(origins?: string[]): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': origins?.join(', ') ?? '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Request-ID, X-Session-ID',
    'Access-Control-Max-Age': '86400',
    'Access-Control-Expose-Headers': 'X-Request-ID, X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset',
  }
}

// ─────────────────────────────────────────────────────────────────
// Rate Limiter
// ─────────────────────────────────────────────────────────────────

interface RateLimitEntry {
  count: number
  resetAt: number
}

class RateLimiter {
  private entries: Map<string, RateLimitEntry> = new Map()
  private config: RateLimitConfig

  constructor(config: RateLimitConfig) {
    this.config = config
  }

  /**
   * Check if request should be allowed and update counter
   */
  check(clientId: string): { allowed: boolean; remaining: number; resetAt: number } {
    const now = Date.now()
    let entry = this.entries.get(clientId)

    // Clean up expired entry
    if (entry && entry.resetAt <= now) {
      this.entries.delete(clientId)
      entry = undefined
    }

    // Create new entry if needed
    if (!entry) {
      entry = {
        count: 0,
        resetAt: now + this.config.windowMs,
      }
      this.entries.set(clientId, entry)
    }

    // Check limit
    const allowed = entry.count < this.config.maxRequests
    if (allowed) {
      entry.count++
    }

    return {
      allowed,
      remaining: Math.max(0, this.config.maxRequests - entry.count),
      resetAt: entry.resetAt,
    }
  }

  /**
   * Clean up expired entries
   */
  cleanup(): void {
    const now = Date.now()
    for (const [clientId, entry] of this.entries) {
      if (entry.resetAt <= now) {
        this.entries.delete(clientId)
      }
    }
  }
}

/**
 * Get client identifier for rate limiting
 */
function getClientId(request: Request): string {
  // Try various headers for client IP
  const cfConnectingIp = request.headers.get('CF-Connecting-IP')
  const xForwardedFor = request.headers.get('X-Forwarded-For')
  const xRealIp = request.headers.get('X-Real-IP')

  return (
    cfConnectingIp ||
    (xForwardedFor ? xForwardedFor.split(',')[0].trim() : null) ||
    xRealIp ||
    'unknown'
  )
}

// ─────────────────────────────────────────────────────────────────
// SSE Helpers
// ─────────────────────────────────────────────────────────────────

function formatSseMessage(message: McpSseMessage): string {
  let result = ''

  if (message.event) {
    result += `event: ${message.event}\n`
  }

  if (message.id) {
    result += `id: ${message.id}\n`
  }

  if (message.retry) {
    result += `retry: ${message.retry}\n`
  }

  // Data must be last, and each line prefixed with "data: "
  const dataLines = message.data.split('\n')
  for (const line of dataLines) {
    result += `data: ${line}\n`
  }

  result += '\n' // End of message
  return result
}

// ─────────────────────────────────────────────────────────────────
// HTTP Handler Factory
// ─────────────────────────────────────────────────────────────────

/**
 * Create an HTTP handler for MCP server
 */
export function createHttpMcpHandler(
  server: McpServerInterface,
  options: HttpMcpHandlerOptions = {}
): (request: Request) => Promise<Response> {
  const {
    timeout = 30000,
    corsOrigins,
    authenticate,
    enableSse: _enableSse = true,
    rateLimit,
    basePath: _basePath = '/mcp',
  } = options

  const corsHeaders = getCorsHeaders(corsOrigins)

  // Set up rate limiter if configured
  const rateLimiter = rateLimit ? new RateLimiter(rateLimit) : null

  // Periodically clean up rate limiter entries
  if (rateLimiter) {
    setInterval(() => rateLimiter.cleanup(), 60000)
  }

  return async (request: Request): Promise<Response> => {
    const url = new URL(request.url)

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders })
    }

    // Check rate limit
    if (rateLimiter) {
      const clientId = getClientId(request)
      const rateLimitResult = rateLimiter.check(clientId)

      if (!rateLimitResult.allowed) {
        return new Response('Too Many Requests', {
          status: 429,
          headers: {
            ...corsHeaders,
            'Retry-After': String(Math.ceil((rateLimitResult.resetAt - Date.now()) / 1000)),
            'X-RateLimit-Limit': String(rateLimit!.maxRequests),
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': String(Math.ceil(rateLimitResult.resetAt / 1000)),
          },
        })
      }
    }

    // Handle SSE endpoint
    if (url.pathname.endsWith('/sse') && request.method === 'GET') {
      return handleSseConnection(request, server, { corsHeaders, authenticate })
    }

    // Handle health check
    if (url.pathname.endsWith('/health') && request.method === 'GET') {
      const serverInfo = server.getServerInfo()
      return createJsonResponse(
        { status: 'ok', server: serverInfo.name, version: serverInfo.version },
        200,
        corsHeaders
      )
    }

    // Only allow POST for regular requests
    if (request.method !== 'POST') {
      return createJsonResponse(
        { error: 'Method not allowed. Use POST or GET /mcp/sse' },
        405,
        corsHeaders
      )
    }

    // Authenticate if configured
    if (authenticate) {
      const authResult = await authenticate(request)
      if (!authResult.authenticated) {
        return createJsonResponse(
          { error: authResult.error ?? 'Unauthorized' },
          401,
          corsHeaders
        )
      }
    }

    // Validate content type
    const contentType = request.headers.get('Content-Type')
    if (!contentType?.includes('application/json')) {
      return createJsonResponse(
        { error: 'Content-Type must be application/json' },
        400,
        corsHeaders
      )
    }

    // Parse request body
    let body: unknown
    try {
      body = await request.json()
    } catch {
      return createJsonResponse(
        {
          jsonrpc: '2.0',
          id: null,
          error: { code: -32700, message: 'Parse error: Invalid JSON' },
        },
        400,
        corsHeaders
      )
    }

    // Handle batch or single request
    const isBatch = Array.isArray(body)

    try {
      if (isBatch) {
        const requests = body as JsonRpcRequest[]
        if (requests.length === 0) {
          return createJsonResponse(
            {
              jsonrpc: '2.0',
              id: null,
              error: { code: -32600, message: 'Invalid Request: Empty batch' },
            },
            400,
            corsHeaders
          )
        }

        const results = await Promise.all(
          requests.map((req) => processRequestWithTimeout(server, req, timeout))
        )

        // Filter out null responses (notifications)
        const responses = results.filter((r): r is JsonRpcResponse => r !== null)

        return createJsonResponse(responses, 200, corsHeaders)
      } else {
        const result = await processRequestWithTimeout(
          server,
          body as JsonRpcRequest,
          timeout
        )

        if (result === null) {
          // Notification - no response needed
          return new Response(null, { status: 204, headers: corsHeaders })
        }

        return createJsonResponse(result, result.error ? 400 : 200, corsHeaders)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Internal error'
      return createJsonResponse(
        {
          jsonrpc: '2.0',
          id: null,
          error: { code: -32603, message },
        },
        500,
        corsHeaders
      )
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Request Processing
// ─────────────────────────────────────────────────────────────────

async function processRequestWithTimeout(
  server: McpServerInterface,
  request: JsonRpcRequest,
  timeout: number
): Promise<JsonRpcResponse | null> {
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      reject(new Error('Request timeout'))
    }, timeout)
  })

  return Promise.race([server.handleRequest(request), timeoutPromise])
}

function createJsonResponse(
  data: unknown,
  status: number,
  headers: Record<string, string>
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  })
}

// ─────────────────────────────────────────────────────────────────
// SSE Handler
// ─────────────────────────────────────────────────────────────────

interface SseHandlerOptions {
  corsHeaders: Record<string, string>
  authenticate?: ((request: Request) => Promise<AuthResult>) | undefined
}

async function handleSseConnection(
  request: Request,
  server: McpServerInterface,
  options: SseHandlerOptions
): Promise<Response> {
  const { corsHeaders, authenticate } = options

  // Authenticate if configured
  if (authenticate) {
    const authResult = await authenticate(request)
    if (!authResult.authenticated) {
      return createJsonResponse(
        { error: authResult.error ?? 'Unauthorized' },
        401,
        corsHeaders
      )
    }
  }

  // Create a TransformStream for SSE
  const { readable, writable } = new TransformStream()
  const writer = writable.getWriter()
  const encoder = new TextEncoder()

  // Send initial connection message
  const serverInfo = server.getServerInfo()
  const connectMessage = formatSseMessage({
    event: 'connect',
    data: JSON.stringify({
      server: serverInfo.name,
      version: serverInfo.version,
      protocol: '2024-11-05',
    }),
  })

  writer.write(encoder.encode(connectMessage))

  // Set up periodic keepalive
  const keepaliveInterval = setInterval(() => {
    const keepalive = formatSseMessage({
      event: 'keepalive',
      data: JSON.stringify({ timestamp: Date.now() }),
    })
    writer.write(encoder.encode(keepalive)).catch(() => {
      clearInterval(keepaliveInterval)
    })
  }, 30000)

  // Handle request close
  request.signal?.addEventListener('abort', () => {
    clearInterval(keepaliveInterval)
    writer.close().catch(() => {})
  })

  return new Response(readable, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      ...corsHeaders,
    },
  })
}

// ─────────────────────────────────────────────────────────────────
// SSE Session Handler (for bidirectional communication)
// ─────────────────────────────────────────────────────────────────

/**
 * Create an SSE session that can receive messages and send responses
 */
export class McpSseSession {
  private writer: WritableStreamDefaultWriter<Uint8Array>
  private encoder = new TextEncoder()
  private messageId = 0

  constructor(
    private server: McpServerInterface,
    writable: WritableStream<Uint8Array>
  ) {
    this.writer = writable.getWriter()
  }

  /**
   * Send an SSE message
   */
  async send(event: string, data: unknown): Promise<void> {
    const message = formatSseMessage({
      event,
      id: String(++this.messageId),
      data: JSON.stringify(data),
    })
    await this.writer.write(this.encoder.encode(message))
  }

  /**
   * Handle an incoming JSON-RPC request and send response via SSE
   */
  async handleRequest(request: JsonRpcRequest): Promise<void> {
    try {
      const response = await this.server.handleRequest(request)
      if (response) {
        await this.send('response', response)
      }
    } catch (error) {
      const errorResponse: JsonRpcResponse = {
        jsonrpc: '2.0',
        id: request.id ?? null,
        error: {
          code: -32603,
          message: error instanceof Error ? error.message : 'Internal error',
        },
      }
      await this.send('response', errorResponse)
    }
  }

  /**
   * Close the session
   */
  async close(): Promise<void> {
    try {
      await this.writer.close()
    } catch {
      // Already closed
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Message Endpoint Handler (for SSE message receiving)
// ─────────────────────────────────────────────────────────────────

/**
 * Create a message handler for SSE sessions
 * This handles POST requests with JSON-RPC messages for an SSE session
 */
export function createMessageHandler(
  sessions: Map<string, McpSseSession>
): (request: Request) => Promise<Response> {
  return async (request: Request): Promise<Response> => {
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 })
    }

    const sessionId = request.headers.get('X-Session-ID')
    if (!sessionId) {
      return new Response('Missing X-Session-ID header', { status: 400 })
    }

    const session = sessions.get(sessionId)
    if (!session) {
      return new Response('Session not found', { status: 404 })
    }

    try {
      const body = await request.json() as JsonRpcRequest
      await session.handleRequest(body)
      return new Response(null, { status: 202 })
    } catch (error) {
      return new Response(
        JSON.stringify({ error: 'Invalid request' }),
        { status: 400, headers: { 'Content-Type': 'application/json' } }
      )
    }
  }
}
