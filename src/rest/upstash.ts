/**
 * Upstash-Compatible REST API Handler
 *
 * Implements the Upstash Redis REST API format:
 * - POST / with JSON body: ["COMMAND", "arg1", "arg2", ...]
 * - POST /pipeline with JSON body: [["CMD1", ...], ["CMD2", ...]]
 * - Response format: { result: ... } or { results: [...] } for pipelines
 * - Authorization: Bearer <token> header
 *
 * @see https://upstash.com/docs/redis/features/restapi
 * @packageDocumentation
 */

import type { Env, SetOptions, ScanOptions } from '../types'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

export interface UpstashContext {
  env: Env
  executeCommand: (cmd: string, args: string[]) => Promise<unknown>
}

export interface UpstashRestOptions {
  /** Custom CORS origins (default: '*') */
  allowedOrigins?: string[]
  /** Enable detailed error messages */
  detailedErrors?: boolean
}

interface UpstashSuccessResponse {
  result: unknown
}

interface UpstashPipelineResponse {
  results?: { result: unknown }[]
}

interface UpstashErrorResponse {
  error: string
}

type UpstashResponse = UpstashSuccessResponse | UpstashPipelineResponse | UpstashErrorResponse

// ─────────────────────────────────────────────────────────────────
// CORS Configuration
// ─────────────────────────────────────────────────────────────────

const DEFAULT_CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Upstash-Response-Format',
  'Access-Control-Max-Age': '86400',
} as const

// ─────────────────────────────────────────────────────────────────
// Main Handler
// ─────────────────────────────────────────────────────────────────

/**
 * Handle Upstash-compatible REST API requests
 */
export async function handleUpstashRest(
  request: Request,
  url: URL,
  context: UpstashContext,
  options: UpstashRestOptions = {}
): Promise<Response> {
  const corsHeaders = getCorsHeaders(options.allowedOrigins)

  // Handle CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: corsHeaders })
  }

  // Validate HTTP method
  if (!['GET', 'POST', 'PUT', 'HEAD'].includes(request.method)) {
    return createErrorResponse('Method not allowed', 405, corsHeaders)
  }

  // Authenticate
  const authResult = authenticate(request, context.env)
  if (!authResult.valid) {
    return createErrorResponse('Unauthorized', 401, corsHeaders)
  }

  try {
    // Check if this is a pipeline request
    if (url.pathname === '/pipeline' || url.pathname.endsWith('/pipeline')) {
      return handlePipeline(request, context, corsHeaders)
    }

    // Check for URL-based command (GET /COMMAND/arg1/arg2)
    const pathParts = url.pathname.slice(1).split('/').filter(Boolean)
    if (pathParts.length > 0 && request.method === 'GET') {
      return handleUrlCommand(pathParts, url, context, corsHeaders)
    }

    // Handle POST with JSON body or URL command with POST body as value
    if (request.method === 'POST' || request.method === 'PUT') {
      return handlePostCommand(request, url, pathParts, context, corsHeaders)
    }

    // HEAD request - just return 200 for health check
    if (request.method === 'HEAD') {
      return new Response(null, { status: 200, headers: corsHeaders })
    }

    return createErrorResponse('No command provided', 400, corsHeaders)
  } catch (error) {
    const message = options.detailedErrors && error instanceof Error
      ? error.message
      : 'Internal server error'
    return createErrorResponse(message, 400, corsHeaders)
  }
}

// ─────────────────────────────────────────────────────────────────
// Command Handlers
// ─────────────────────────────────────────────────────────────────

/**
 * Handle URL-based commands: GET /COMMAND/arg1/arg2/...
 */
async function handleUrlCommand(
  pathParts: string[],
  url: URL,
  context: UpstashContext,
  corsHeaders: Record<string, string>
): Promise<Response> {
  const [cmd, ...args] = pathParts.map(decodeURIComponent)

  // Append query parameters as additional args
  const queryArgs = parseQueryArgs(url.searchParams)
  const allArgs = [...args, ...queryArgs]

  try {
    const result = await context.executeCommand(cmd.toUpperCase(), allArgs)
    return createSuccessResponse({ result }, corsHeaders)
  } catch (error) {
    return createErrorResponse(
      error instanceof Error ? error.message : 'Command execution failed',
      400,
      corsHeaders
    )
  }
}

/**
 * Handle POST commands
 * - If body is JSON array: treat as ["COMMAND", "arg1", ...]
 * - If path has command and body is value: treat as POST value
 */
async function handlePostCommand(
  request: Request,
  url: URL,
  pathParts: string[],
  context: UpstashContext,
  corsHeaders: Record<string, string>
): Promise<Response> {
  const contentType = request.headers.get('Content-Type') || ''

  // If path has command parts, treat body as the value
  if (pathParts.length > 0) {
    const [cmd, ...args] = pathParts.map(decodeURIComponent)
    const queryArgs = parseQueryArgs(url.searchParams)

    // Get body as value
    const body = await request.text()
    const allArgs = [...args, body, ...queryArgs]

    try {
      const result = await context.executeCommand(cmd.toUpperCase(), allArgs)
      return createSuccessResponse({ result }, corsHeaders)
    } catch (error) {
      return createErrorResponse(
        error instanceof Error ? error.message : 'Command execution failed',
        400,
        corsHeaders
      )
    }
  }

  // Parse JSON body
  if (!contentType.includes('application/json')) {
    // Try to parse as JSON anyway (Upstash is lenient)
    try {
      const body = await request.text()
      if (body.trim().startsWith('[')) {
        const command = JSON.parse(body) as string[]
        return executeJsonCommand(command, context, corsHeaders)
      }
    } catch {
      return createErrorResponse('Invalid request body', 400, corsHeaders)
    }
  }

  try {
    const body = await request.json()

    if (!Array.isArray(body)) {
      return createErrorResponse('Request body must be a JSON array', 400, corsHeaders)
    }

    return executeJsonCommand(body as string[], context, corsHeaders)
  } catch {
    return createErrorResponse('Invalid JSON', 400, corsHeaders)
  }
}

/**
 * Execute a JSON command array: ["COMMAND", "arg1", "arg2", ...]
 */
async function executeJsonCommand(
  command: string[],
  context: UpstashContext,
  corsHeaders: Record<string, string>
): Promise<Response> {
  if (command.length === 0) {
    return createErrorResponse('Empty command', 400, corsHeaders)
  }

  const [cmd, ...args] = command

  if (typeof cmd !== 'string') {
    return createErrorResponse('Command must be a string', 400, corsHeaders)
  }

  // Convert all args to strings (Upstash sends everything as strings in JSON)
  const stringArgs = args.map(arg => String(arg))

  try {
    const result = await context.executeCommand(cmd.toUpperCase(), stringArgs)
    return createSuccessResponse({ result }, corsHeaders)
  } catch (error) {
    return createErrorResponse(
      error instanceof Error ? error.message : 'Command execution failed',
      400,
      corsHeaders
    )
  }
}

/**
 * Handle pipeline requests: POST /pipeline with [["CMD1", ...], ["CMD2", ...]]
 */
async function handlePipeline(
  request: Request,
  context: UpstashContext,
  corsHeaders: Record<string, string>
): Promise<Response> {
  try {
    const body = await request.json()

    if (!Array.isArray(body)) {
      return createErrorResponse('Pipeline body must be a JSON array', 400, corsHeaders)
    }

    if (body.length === 0) {
      return createSuccessResponse({ results: [] }, corsHeaders)
    }

    // Validate all commands are arrays
    for (const cmd of body) {
      if (!Array.isArray(cmd)) {
        return createErrorResponse('Each pipeline command must be an array', 400, corsHeaders)
      }
      if (cmd.length === 0) {
        return createErrorResponse('Empty command in pipeline', 400, corsHeaders)
      }
    }

    // Execute all commands
    const results: { result: unknown }[] = []

    for (const command of body as string[][]) {
      const [cmd, ...args] = command
      const stringArgs = args.map(arg => String(arg))

      try {
        const result = await context.executeCommand(cmd.toUpperCase(), stringArgs)
        results.push({ result })
      } catch (error) {
        // In pipeline, errors are included in results (not thrown)
        results.push({
          result: error instanceof Error ? `ERR ${error.message}` : 'ERR Unknown error'
        })
      }
    }

    return createSuccessResponse({ results }, corsHeaders)
  } catch {
    return createErrorResponse('Invalid JSON', 400, corsHeaders)
  }
}

// ─────────────────────────────────────────────────────────────────
// Authentication
// ─────────────────────────────────────────────────────────────────

interface AuthResult {
  valid: boolean
  token?: string
}

/**
 * Authenticate request using Bearer token
 */
function authenticate(request: Request, env: Env): AuthResult {
  // If no AUTH_TOKEN configured, allow all requests
  if (!env.AUTH_TOKEN) {
    return { valid: true }
  }

  const auth = request.headers.get('Authorization')

  if (!auth) {
    return { valid: false }
  }

  // Support both "Bearer <token>" and just "<token>"
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : auth

  if (token === env.AUTH_TOKEN) {
    return { valid: true, token }
  }

  return { valid: false }
}

// ─────────────────────────────────────────────────────────────────
// Response Helpers
// ─────────────────────────────────────────────────────────────────

/**
 * Create a success response
 */
function createSuccessResponse(
  data: UpstashSuccessResponse | UpstashPipelineResponse,
  corsHeaders: Record<string, string>
): Response {
  return new Response(JSON.stringify(data), {
    status: 200,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders,
    },
  })
}

/**
 * Create an error response
 */
function createErrorResponse(
  message: string,
  status: number,
  corsHeaders: Record<string, string>
): Response {
  const body: UpstashErrorResponse = { error: message }
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders,
    },
  })
}

/**
 * Get CORS headers based on configuration
 */
function getCorsHeaders(allowedOrigins?: string[]): Record<string, string> {
  if (allowedOrigins && allowedOrigins.length > 0) {
    return {
      ...DEFAULT_CORS_HEADERS,
      'Access-Control-Allow-Origin': allowedOrigins.join(', '),
    }
  }
  return { ...DEFAULT_CORS_HEADERS }
}

/**
 * Parse query parameters into command arguments
 * Converts ?EX=100&NX to ["EX", "100", "NX"]
 */
function parseQueryArgs(searchParams: URLSearchParams): string[] {
  const args: string[] = []

  for (const [key, value] of searchParams.entries()) {
    args.push(key.toUpperCase())
    if (value && value !== '') {
      args.push(value)
    }
  }

  return args
}

// ─────────────────────────────────────────────────────────────────
// Factory Function
// ─────────────────────────────────────────────────────────────────

/**
 * Create an Upstash REST handler with the given command executor
 */
export function createUpstashRestHandler(
  executeCommand: (cmd: string, args: string[]) => Promise<unknown>,
  options: UpstashRestOptions = {}
) {
  return async (request: Request, env: Env): Promise<Response> => {
    const url = new URL(request.url)
    const context: UpstashContext = {
      env,
      executeCommand,
    }
    return handleUpstashRest(request, url, context, options)
  }
}
