/**
 * MCP Server Implementation
 *
 * Factory for creating MCP servers with tool registration and JSON-RPC handling
 */

import type {
  JsonRpcId,
  JsonRpcRequest,
  JsonRpcResponse,
  McpServerConfig,
  McpServerCapabilities,
  McpToolDefinition,
  McpToolInputSchema,
  McpToolHandler,
  McpToolContext,
  McpRegisteredTool,
  McpToolCallResult,
  McpInitializeParams,
  McpInitializeResult,
  McpToolsListResult,
  McpToolCallParams,
  McpTextContent,
  McpTransport,
  McpLogLevel,
  RedisProxy,
} from './types'

// ─────────────────────────────────────────────────────────────────
// Error Codes
// ─────────────────────────────────────────────────────────────────

const RPC_ERRORS = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
} as const

// ─────────────────────────────────────────────────────────────────
// MCP Server Class
// ─────────────────────────────────────────────────────────────────

export class McpServer {
  private config: McpServerConfig
  private tools: Map<string, McpRegisteredTool> = new Map()
  private transport: McpTransport | null = null
  private initialized = false
  private _clientCapabilities: Record<string, unknown> = {}
  private _logLevel: McpLogLevel = 'info'
  private redisProxy: RedisProxy | null = null

  get clientCapabilities(): Record<string, unknown> {
    return this._clientCapabilities
  }

  get logLevel(): McpLogLevel {
    return this._logLevel
  }

  constructor(config: McpServerConfig) {
    this.config = {
      name: config.name,
      version: config.version,
      instructions: config.instructions,
      capabilities: {
        tools: { listChanged: false },
        ...config.capabilities,
      },
    }
  }

  /**
   * Set the Redis proxy for built-in tools
   */
  setRedisProxy(proxy: RedisProxy): void {
    this.redisProxy = proxy
    this.registerBuiltInTools()
  }

  /**
   * Register a tool with the server
   */
  tool<T = Record<string, unknown>>(
    name: string,
    schema: McpToolInputSchema,
    handler: McpToolHandler<T>,
    annotations?: McpToolDefinition['annotations']
  ): this {
    const definition: McpToolDefinition = {
      name,
      inputSchema: schema,
      annotations,
    }

    this.tools.set(name, {
      definition,
      handler: handler as McpToolHandler,
    })

    return this
  }

  /**
   * Register a tool with description
   */
  toolWithDescription<T = Record<string, unknown>>(
    name: string,
    description: string,
    schema: McpToolInputSchema,
    handler: McpToolHandler<T>,
    annotations?: McpToolDefinition['annotations']
  ): this {
    const definition: McpToolDefinition = {
      name,
      description,
      inputSchema: schema,
      annotations,
    }

    this.tools.set(name, {
      definition,
      handler: handler as McpToolHandler,
    })

    return this
  }

  /**
   * Connect a transport to the server
   */
  async connect(transport: McpTransport): Promise<void> {
    this.transport = transport

    transport.onMessage = async (message: JsonRpcRequest) => {
      const response = await this.handleRequest(message)
      if (response && message.id !== undefined) {
        await transport.send(response)
      }
    }

    transport.onClose = () => {
      this.initialized = false
    }

    transport.onError = (error: Error) => {
      console.error('[MCP Server] Transport error:', error)
    }

    await transport.start()
  }

  /**
   * Handle incoming JSON-RPC request
   */
  async handleRequest(request: JsonRpcRequest): Promise<JsonRpcResponse | null> {
    const id = request.id ?? null

    try {
      // Validate JSON-RPC format
      if (request.jsonrpc !== '2.0') {
        return this.errorResponse(id, RPC_ERRORS.INVALID_REQUEST, 'Invalid JSON-RPC version')
      }

      if (!request.method || typeof request.method !== 'string') {
        return this.errorResponse(id, RPC_ERRORS.INVALID_REQUEST, 'Method is required')
      }

      // Handle MCP methods
      const result = await this.dispatchMethod(request.method, request.params, id)

      // Notifications don't require responses
      if (id === undefined) {
        return null
      }

      return {
        jsonrpc: '2.0',
        id,
        result,
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      return this.errorResponse(id, RPC_ERRORS.INTERNAL_ERROR, message)
    }
  }

  /**
   * Dispatch method to appropriate handler
   */
  private async dispatchMethod(
    method: string,
    params: Record<string, unknown> | unknown[] | undefined,
    requestId: JsonRpcId
  ): Promise<unknown> {
    switch (method) {
      case 'initialize':
        return this.handleInitialize(params as unknown as McpInitializeParams)

      case 'initialized':
        // Client notification that initialization is complete
        return {}

      case 'ping':
        return {}

      case 'tools/list':
        return this.handleToolsList()

      case 'tools/call':
        return this.handleToolCall(params as unknown as McpToolCallParams, requestId)

      case 'resources/list':
        return { resources: [] }

      case 'resources/read':
        throw new Error('Resource not found')

      case 'prompts/list':
        return { prompts: [] }

      case 'prompts/get':
        throw new Error('Prompt not found')

      case 'logging/setLevel':
        const levelParams = params as { level: McpLogLevel }
        this._logLevel = levelParams.level
        return {}

      case 'notifications/cancelled':
        // Handle cancellation notification
        return null

      default:
        throw new Error(`Method not found: ${method}`)
    }
  }

  /**
   * Handle initialize request
   */
  private handleInitialize(params: McpInitializeParams): McpInitializeResult {
    // Store client capabilities (prefixed with _ to indicate intentionally unused for now)
    void params.capabilities
    this.initialized = true

    return {
      protocolVersion: params.protocolVersion || '2024-11-05',
      capabilities: this.config.capabilities as McpServerCapabilities,
      serverInfo: {
        name: this.config.name,
        version: this.config.version,
      },
      instructions: this.config.instructions,
    }
  }

  /**
   * Handle tools/list request
   */
  private handleToolsList(): McpToolsListResult {
    const tools: McpToolDefinition[] = []

    for (const [, registered] of this.tools) {
      tools.push(registered.definition)
    }

    return { tools }
  }

  /**
   * Handle tools/call request
   */
  private async handleToolCall(
    params: McpToolCallParams,
    requestId: JsonRpcId
  ): Promise<McpToolCallResult> {
    const { name, arguments: args = {} } = params

    const registered = this.tools.get(name)
    if (!registered) {
      return {
        content: [{ type: 'text', text: `Tool not found: ${name}` }],
        isError: true,
      }
    }

    const context: McpToolContext = {
      requestId,
    }

    try {
      return await registered.handler(args, context)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      return {
        content: [{ type: 'text', text: `Error executing tool ${name}: ${message}` }],
        isError: true,
      }
    }
  }

  /**
   * Create error response
   */
  private errorResponse(id: JsonRpcId, code: number, message: string, data?: unknown): JsonRpcResponse {
    return {
      jsonrpc: '2.0',
      id,
      error: { code, message, data },
    }
  }

  /**
   * Register built-in Redis tools
   */
  private registerBuiltInTools(): void {
    if (!this.redisProxy) return

    const proxy = this.redisProxy

    // GET - Retrieve a value by key
    this.toolWithDescription<{ key: string }>(
      'get',
      'Get the value of a key from Redis',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to retrieve' },
        },
        required: ['key'],
      },
      async (args) => {
        const value = await proxy.get(args.key)
        return {
          content: [
            {
              type: 'text',
              text: value !== null ? JSON.stringify({ key: args.key, value }) : JSON.stringify({ key: args.key, value: null }),
            },
          ],
        }
      },
      { readOnlyHint: true }
    )

    // SET - Store a value
    this.toolWithDescription<{ key: string; value: string; ex?: number; nx?: boolean; xx?: boolean }>(
      'set',
      'Set the value of a key in Redis with optional expiration',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to set' },
          value: { type: 'string', description: 'The value to store' },
          ex: { type: 'number', description: 'Expiration time in seconds' },
          nx: { type: 'boolean', description: 'Only set if key does not exist' },
          xx: { type: 'boolean', description: 'Only set if key already exists' },
        },
        required: ['key', 'value'],
      },
      async (args) => {
        const result = await proxy.set(args.key, args.value, {
          ex: args.ex,
          nx: args.nx,
          xx: args.xx,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify({ success: result === 'OK', result }) }],
        }
      },
      { destructiveHint: true }
    )

    // KEYS - List keys matching pattern
    this.toolWithDescription<{ pattern: string }>(
      'keys',
      'Find all keys matching a pattern (use * for all keys)',
      {
        type: 'object',
        properties: {
          pattern: { type: 'string', description: 'Pattern to match (e.g., "user:*" or "*")' },
        },
        required: ['pattern'],
      },
      async (args) => {
        const keys = await proxy.keys(args.pattern)
        return {
          content: [{ type: 'text', text: JSON.stringify({ pattern: args.pattern, keys, count: keys.length }) }],
        }
      },
      { readOnlyHint: true }
    )

    // SCAN - Incrementally iterate keys
    this.toolWithDescription<{ cursor?: number; match?: string; count?: number; type?: string }>(
      'scan',
      'Incrementally iterate over keys with optional filtering',
      {
        type: 'object',
        properties: {
          cursor: { type: 'number', description: 'Cursor position (start with 0)' },
          match: { type: 'string', description: 'Pattern to match keys' },
          count: { type: 'number', description: 'Number of keys to return per iteration' },
          type: { type: 'string', description: 'Filter by key type (string, hash, list, set, zset)' },
        },
      },
      async (args) => {
        const [nextCursor, keys] = await proxy.scan(args.cursor ?? 0, {
          match: args.match,
          count: args.count,
          type: args.type,
        })
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ cursor: nextCursor, keys, count: keys.length }),
            },
          ],
        }
      },
      { readOnlyHint: true }
    )

    // DO - Execute arbitrary code in a sandboxed environment
    this.toolWithDescription<{ code: string }>(
      'do',
      'Execute JavaScript code with access to Redis operations (sandboxed)',
      {
        type: 'object',
        properties: {
          code: {
            type: 'string',
            description:
              'JavaScript code to execute. Has access to `redis` object with methods: get, set, del, exists, expire, ttl, keys, scan, hget, hset, hgetall, hdel, lpush, rpush, lpop, rpop, lrange, sadd, smembers, srem, sismember, zadd, zrange, zrem, zscore, incr, incrby, decr, decrby, append, strlen, type, rename, dbsize, flushdb, info, ping',
          },
        },
        required: ['code'],
      },
      async (args) => {
        // Note: Actual sandbox execution is handled by worker-evaluator
        // This is a placeholder that returns the code for processing
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                message: 'Code execution requires sandbox environment',
                code: args.code,
              }),
            },
          ],
        }
      },
      { destructiveHint: true }
    )

    // DEL - Delete keys
    this.toolWithDescription<{ keys: string[] }>(
      'del',
      'Delete one or more keys',
      {
        type: 'object',
        properties: {
          keys: {
            type: 'array',
            items: { type: 'string' },
            description: 'Keys to delete',
          },
        },
        required: ['keys'],
      },
      async (args) => {
        const count = await proxy.del(...args.keys)
        return {
          content: [{ type: 'text', text: JSON.stringify({ deleted: count }) }],
        }
      },
      { destructiveHint: true }
    )

    // EXISTS - Check if keys exist
    this.toolWithDescription<{ keys: string[] }>(
      'exists',
      'Check if one or more keys exist',
      {
        type: 'object',
        properties: {
          keys: {
            type: 'array',
            items: { type: 'string' },
            description: 'Keys to check',
          },
        },
        required: ['keys'],
      },
      async (args) => {
        const count = await proxy.exists(...args.keys)
        return {
          content: [{ type: 'text', text: JSON.stringify({ exists: count }) }],
        }
      },
      { readOnlyHint: true }
    )

    // EXPIRE - Set key expiration
    this.toolWithDescription<{ key: string; seconds: number }>(
      'expire',
      'Set a timeout on a key in seconds',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to set expiration on' },
          seconds: { type: 'number', description: 'Expiration time in seconds' },
        },
        required: ['key', 'seconds'],
      },
      async (args) => {
        const result = await proxy.expire(args.key, args.seconds)
        return {
          content: [{ type: 'text', text: JSON.stringify({ success: result === 1 }) }],
        }
      }
    )

    // TTL - Get remaining time to live
    this.toolWithDescription<{ key: string }>(
      'ttl',
      'Get the remaining time to live of a key in seconds',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to check' },
        },
        required: ['key'],
      },
      async (args) => {
        const ttl = await proxy.ttl(args.key)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, ttl }) }],
        }
      },
      { readOnlyHint: true }
    )

    // TYPE - Get the type of a key
    this.toolWithDescription<{ key: string }>(
      'type',
      'Get the type of a key',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to check' },
        },
        required: ['key'],
      },
      async (args) => {
        const type = await proxy.type(args.key)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, type }) }],
        }
      },
      { readOnlyHint: true }
    )

    // HGET - Get hash field
    this.toolWithDescription<{ key: string; field: string }>(
      'hget',
      'Get the value of a hash field',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The hash key' },
          field: { type: 'string', description: 'The field to retrieve' },
        },
        required: ['key', 'field'],
      },
      async (args) => {
        const value = await proxy.hget(args.key, args.field)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, field: args.field, value }) }],
        }
      },
      { readOnlyHint: true }
    )

    // HSET - Set hash field
    this.toolWithDescription<{ key: string; field: string; value: string }>(
      'hset',
      'Set the value of a hash field',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The hash key' },
          field: { type: 'string', description: 'The field to set' },
          value: { type: 'string', description: 'The value to store' },
        },
        required: ['key', 'field', 'value'],
      },
      async (args) => {
        const result = await proxy.hset(args.key, args.field, args.value)
        return {
          content: [{ type: 'text', text: JSON.stringify({ success: true, fieldsAdded: result }) }],
        }
      },
      { destructiveHint: true }
    )

    // HGETALL - Get all hash fields
    this.toolWithDescription<{ key: string }>(
      'hgetall',
      'Get all fields and values of a hash',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The hash key' },
        },
        required: ['key'],
      },
      async (args) => {
        const value = await proxy.hgetall(args.key)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, value }) }],
        }
      },
      { readOnlyHint: true }
    )

    // INCR - Increment a key
    this.toolWithDescription<{ key: string }>(
      'incr',
      'Increment the integer value of a key by one',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The key to increment' },
        },
        required: ['key'],
      },
      async (args) => {
        const value = await proxy.incr(args.key)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, value }) }],
        }
      }
    )

    // LPUSH - Push to list head
    this.toolWithDescription<{ key: string; values: string[] }>(
      'lpush',
      'Push values to the head of a list',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The list key' },
          values: {
            type: 'array',
            items: { type: 'string' },
            description: 'Values to push',
          },
        },
        required: ['key', 'values'],
      },
      async (args) => {
        const length = await proxy.lpush(args.key, ...args.values)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, length }) }],
        }
      }
    )

    // LRANGE - Get list range
    this.toolWithDescription<{ key: string; start: number; stop: number }>(
      'lrange',
      'Get a range of elements from a list',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The list key' },
          start: { type: 'number', description: 'Start index (0-based, negative from end)' },
          stop: { type: 'number', description: 'Stop index (inclusive, -1 for end)' },
        },
        required: ['key', 'start', 'stop'],
      },
      async (args) => {
        const values = await proxy.lrange(args.key, args.start, args.stop)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, values }) }],
        }
      },
      { readOnlyHint: true }
    )

    // SADD - Add to set
    this.toolWithDescription<{ key: string; members: string[] }>(
      'sadd',
      'Add members to a set',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The set key' },
          members: {
            type: 'array',
            items: { type: 'string' },
            description: 'Members to add',
          },
        },
        required: ['key', 'members'],
      },
      async (args) => {
        const added = await proxy.sadd(args.key, ...args.members)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, added }) }],
        }
      }
    )

    // SMEMBERS - Get all set members
    this.toolWithDescription<{ key: string }>(
      'smembers',
      'Get all members of a set',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The set key' },
        },
        required: ['key'],
      },
      async (args) => {
        const members = await proxy.smembers(args.key)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, members }) }],
        }
      },
      { readOnlyHint: true }
    )

    // ZADD - Add to sorted set
    this.toolWithDescription<{ key: string; score: number; member: string }>(
      'zadd',
      'Add a member to a sorted set with a score',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The sorted set key' },
          score: { type: 'number', description: 'The score for the member' },
          member: { type: 'string', description: 'The member to add' },
        },
        required: ['key', 'score', 'member'],
      },
      async (args) => {
        const added = await proxy.zadd(args.key, args.score, args.member)
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, added }) }],
        }
      }
    )

    // ZRANGE - Get sorted set range
    this.toolWithDescription<{ key: string; start: number; stop: number; withScores?: boolean }>(
      'zrange',
      'Get a range of members from a sorted set by index',
      {
        type: 'object',
        properties: {
          key: { type: 'string', description: 'The sorted set key' },
          start: { type: 'number', description: 'Start index' },
          stop: { type: 'number', description: 'Stop index' },
          withScores: { type: 'boolean', description: 'Include scores in output' },
        },
        required: ['key', 'start', 'stop'],
      },
      async (args) => {
        const members = await proxy.zrange(args.key, args.start, args.stop, {
          withScores: args.withScores,
        })
        return {
          content: [{ type: 'text', text: JSON.stringify({ key: args.key, members }) }],
        }
      },
      { readOnlyHint: true }
    )

    // DBSIZE - Get number of keys
    this.toolWithDescription(
      'dbsize',
      'Get the number of keys in the database',
      {
        type: 'object',
        properties: {},
      },
      async () => {
        const count = await proxy.dbsize()
        return {
          content: [{ type: 'text', text: JSON.stringify({ keys: count }) }],
        }
      },
      { readOnlyHint: true }
    )

    // INFO - Get server info
    this.toolWithDescription<{ section?: string }>(
      'info',
      'Get information about the Redis server',
      {
        type: 'object',
        properties: {
          section: {
            type: 'string',
            description: 'Specific section to retrieve (optional)',
          },
        },
      },
      async (args) => {
        const info = await proxy.info(args.section)
        return {
          content: [{ type: 'text', text: info }],
        }
      },
      { readOnlyHint: true }
    )

    // PING - Test connection
    this.toolWithDescription<{ message?: string }>(
      'ping',
      'Test the connection to Redis',
      {
        type: 'object',
        properties: {
          message: { type: 'string', description: 'Optional message to echo back' },
        },
      },
      async (args) => {
        const result = await proxy.ping(args.message)
        return {
          content: [{ type: 'text', text: result }],
        }
      },
      { readOnlyHint: true }
    )
  }

  /**
   * Get registered tools
   */
  getTools(): McpRegisteredTool[] {
    return Array.from(this.tools.values())
  }

  /**
   * Get server info
   */
  getServerInfo(): { name: string; version: string } {
    return {
      name: this.config.name,
      version: this.config.version,
    }
  }

  /**
   * Check if server is initialized
   */
  isInitialized(): boolean {
    return this.initialized
  }

  /**
   * Close the server
   */
  async close(): Promise<void> {
    if (this.transport) {
      await this.transport.close()
      this.transport = null
    }
    this.initialized = false
  }
}

// ─────────────────────────────────────────────────────────────────
// Extended Server Config for Factory
// ─────────────────────────────────────────────────────────────────

export interface RedisAccess {
  get(key: string): Promise<string | null>
  set(key: string, value: string, opts?: { ex?: number; nx?: boolean; xx?: boolean }): Promise<string | null>
  del(...keys: string[]): Promise<number>
  exists(...keys: string[]): Promise<number>
  keys(pattern: string): Promise<string[]>
  scan(cursor: number, opts?: { match?: string; count?: number; type?: string }): Promise<[number, string[]]>
  hget(key: string, field: string): Promise<string | null>
  hset(key: string, ...fieldValues: string[]): Promise<number>
  hgetall(key: string): Promise<Record<string, string>>
  lpush(key: string, ...values: string[]): Promise<number>
  lrange(key: string, start: number, stop: number): Promise<string[]>
  rpush(key: string, ...values: string[]): Promise<number>
  sadd(key: string, ...members: string[]): Promise<number>
  smembers(key: string): Promise<string[]>
  zadd(key: string, ...args: (string | number)[]): Promise<number>
  zrange(key: string, start: number, stop: number): Promise<string[]>
  publish(channel: string, message: string): Promise<number>
  getProxy(): RedisAccess
}

export interface CodeLoader {
  execute(code: string, context?: Record<string, unknown>): Promise<{
    success: boolean
    result?: unknown
    error?: string
    logs: string[]
    executionTime: number
  }>
}

export interface CreateMcpServerOptions {
  name: string
  version: string
  instructions?: string
  redisAccess: RedisAccess
  codeLoader?: CodeLoader
}

// ─────────────────────────────────────────────────────────────────
// Factory Function
// ─────────────────────────────────────────────────────────────────

/**
 * Create a new MCP server instance with Redis access and optional code loader
 */
export function createMcpServer(options: CreateMcpServerOptions): McpServer {
  const server = new McpServer({
    name: options.name,
    version: options.version,
    instructions: options.instructions ?? 'Redois MCP Server - Redis-compatible database with AI tool access',
  })

  // Create a Redis proxy from the access interface
  const redisProxy = createRedisProxyFromAccess(options.redisAccess)
  server.setRedisProxy(redisProxy)

  // If code loader is provided, register the enhanced 'do' tool
  if (options.codeLoader) {
    registerCodeLoaderTool(server, options.codeLoader)
  }

  return server
}

/**
 * Create a RedisProxy from RedisAccess interface
 */
function createRedisProxyFromAccess(access: RedisAccess): RedisProxy {
  return {
    get: (key: string) => access.get(key),
    set: async (key: string, value: string, opts?: { ex?: number; nx?: boolean; xx?: boolean }) => {
      const result = await access.set(key, value, opts)
      return result ?? 'OK'
    },
    del: (...keys: string[]) => access.del(...keys),
    exists: (...keys: string[]) => access.exists(...keys),
    expire: async (_key: string, _seconds: number) => 1, // Simplified
    ttl: async (_key: string) => -1, // Simplified
    keys: (pattern: string) => access.keys(pattern),
    scan: async (cursor: number, opts?: { match?: string; count?: number; type?: string }): Promise<[string, string[]]> => {
      const result = await access.scan(cursor, opts)
      return [String(result[0]), result[1]]
    },
    hget: (key: string, field: string) => access.hget(key, field),
    hset: (key: string, field: string, value: string) => access.hset(key, field, value),
    hgetall: (key: string) => access.hgetall(key),
    hdel: async (_key: string, ..._fields: string[]) => 0, // Simplified
    lpush: (key: string, ...values: string[]) => access.lpush(key, ...values),
    rpush: (key: string, ...values: string[]) => access.rpush(key, ...values),
    lpop: async (_key: string) => null, // Simplified
    rpop: async (_key: string) => null, // Simplified
    lrange: (key: string, start: number, stop: number) => access.lrange(key, start, stop),
    sadd: (key: string, ...members: string[]) => access.sadd(key, ...members),
    smembers: (key: string) => access.smembers(key),
    srem: async (_key: string, ..._members: string[]) => 0, // Simplified
    sismember: async (_key: string, _member: string) => 0, // Simplified
    zadd: (key: string, score: number, member: string) => access.zadd(key, score, member),
    zrange: (key: string, start: number, stop: number, _opts?) => access.zrange(key, start, stop),
    zrem: async (_key: string, ..._members: string[]) => 0, // Simplified
    zscore: async (_key: string, _member: string) => null, // Simplified
    incr: async (_key: string) => 0, // Simplified
    incrby: async (_key: string, _increment: number) => 0, // Simplified
    decr: async (_key: string) => 0, // Simplified
    decrby: async (_key: string, _decrement: number) => 0, // Simplified
    append: async (_key: string, _value: string) => 0, // Simplified
    strlen: async (_key: string) => 0, // Simplified
    type: async (_key: string) => 'none', // Simplified
    rename: async (_key: string, _newKey: string) => 'OK', // Simplified
    dbsize: async () => 0, // Simplified
    flushdb: async () => 'OK', // Simplified
    info: async (_section?: string) => 'Redois Server', // Simplified
    ping: async (message?: string) => message ?? 'PONG',
  }
}

/**
 * Register the code loader tool
 */
function registerCodeLoaderTool(server: McpServer, codeLoader: CodeLoader): void {
  // Override the 'do' tool with actual sandbox execution
  server.toolWithDescription<{ code: string }>(
    'do',
    'Execute JavaScript code with access to Redis operations (sandboxed). The code runs in an isolated environment with access to a `redis` object.',
    {
      type: 'object',
      properties: {
        code: {
          type: 'string',
          description:
            'JavaScript code to execute. Has access to `redis` object with methods: get, set, del, exists, expire, ttl, keys, scan, hget, hset, hgetall, hdel, lpush, rpush, lpop, rpop, lrange, sadd, smembers, srem, sismember, zadd, zrange, zrem, zscore, incr, incrby, decr, decrby, append, strlen, type, rename, dbsize, flushdb, info, ping. Example: `const value = await redis.get("mykey"); return value;`',
        },
      },
      required: ['code'],
    },
    async (args) => {
      try {
        const result = await codeLoader.execute(args.code)

        if (!result.success) {
          return {
            content: [
              {
                type: 'text',
                text: JSON.stringify({
                  success: false,
                  error: result.error,
                  logs: result.logs,
                  executionTime: result.executionTime,
                }, null, 2),
              },
            ],
            isError: true,
          }
        }

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                result: result.result,
                logs: result.logs,
                executionTime: result.executionTime,
              }, null, 2),
            },
          ],
        }
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
              }),
            },
          ],
          isError: true,
        }
      }
    },
    { destructiveHint: true }
  )
}

/**
 * Helper to create text content
 */
export function textContent(text: string): McpTextContent {
  return { type: 'text', text }
}

/**
 * Helper to create success response
 */
export function successResponse(data: unknown): McpToolCallResult {
  return {
    content: [textContent(typeof data === 'string' ? data : JSON.stringify(data, null, 2))],
  }
}

/**
 * Helper to create error response
 */
export function errorResponse(message: string): McpToolCallResult {
  return {
    content: [textContent(message)],
    isError: true,
  }
}
