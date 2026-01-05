/**
 * MCP Server Tests
 *
 * Tests for the MCP (Model Context Protocol) server implementation
 * Covers tool registration, listing, execution, error handling, and request/response formats
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  McpServer,
  createMcpServer,
  textContent,
  successResponse,
  errorResponse,
} from '../../src/mcp/server'
import type {
  JsonRpcRequest,
  McpTransport,
  McpToolCallResult,
  McpToolContext,
  RedisProxy,
} from '../../src/mcp/types'

// ─────────────────────────────────────────────────────────────────
// Mock Transport
// ─────────────────────────────────────────────────────────────────

class MockTransport implements McpTransport {
  public messages: unknown[] = []
  public started = false
  public closed = false
  public onMessage?: (message: JsonRpcRequest) => void
  public onClose?: () => void
  public onError?: (error: Error) => void

  async start(): Promise<void> {
    this.started = true
  }

  async send(message: unknown): Promise<void> {
    this.messages.push(message)
  }

  async close(): Promise<void> {
    this.closed = true
    this.onClose?.()
  }

  // Helper to simulate incoming messages
  simulateMessage(message: JsonRpcRequest): void {
    this.onMessage?.(message)
  }
}

// ─────────────────────────────────────────────────────────────────
// Mock Redis Proxy
// ─────────────────────────────────────────────────────────────────

function createMockRedisProxy(): RedisProxy {
  const store = new Map<string, string>()
  const hashStore = new Map<string, Map<string, string>>()
  const listStore = new Map<string, string[]>()
  const setStore = new Map<string, Set<string>>()
  const zsetStore = new Map<string, Map<string, number>>()

  return {
    get: vi.fn(async (key: string) => store.get(key) ?? null),
    set: vi.fn(async (key: string, value: string) => {
      store.set(key, value)
      return 'OK'
    }),
    del: vi.fn(async (...keys: string[]) => {
      let count = 0
      for (const key of keys) {
        if (store.delete(key)) count++
      }
      return count
    }),
    exists: vi.fn(async (...keys: string[]) => {
      let count = 0
      for (const key of keys) {
        if (store.has(key)) count++
      }
      return count
    }),
    expire: vi.fn(async () => 1),
    ttl: vi.fn(async () => -1),
    keys: vi.fn(async (pattern: string) => {
      const keys: string[] = []
      const regex = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$')
      for (const key of store.keys()) {
        if (regex.test(key)) keys.push(key)
      }
      return keys
    }),
    scan: vi.fn(async (cursor: number, opts?: { match?: string; count?: number }) => {
      const keys: string[] = []
      const regex = opts?.match
        ? new RegExp('^' + opts.match.replace(/\*/g, '.*') + '$')
        : null
      for (const key of store.keys()) {
        if (!regex || regex.test(key)) keys.push(key)
      }
      return ['0', keys.slice(0, opts?.count ?? 10)] as [string, string[]]
    }),
    hget: vi.fn(async (key: string, field: string) => {
      const hash = hashStore.get(key)
      return hash?.get(field) ?? null
    }),
    hset: vi.fn(async (key: string, field: string, value: string) => {
      let hash = hashStore.get(key)
      if (!hash) {
        hash = new Map()
        hashStore.set(key, hash)
      }
      const isNew = !hash.has(field)
      hash.set(field, value)
      return isNew ? 1 : 0
    }),
    hgetall: vi.fn(async (key: string) => {
      const hash = hashStore.get(key)
      if (!hash) return {}
      return Object.fromEntries(hash)
    }),
    hdel: vi.fn(async () => 0),
    lpush: vi.fn(async (key: string, ...values: string[]) => {
      let list = listStore.get(key)
      if (!list) {
        list = []
        listStore.set(key, list)
      }
      list.unshift(...values)
      return list.length
    }),
    rpush: vi.fn(async (key: string, ...values: string[]) => {
      let list = listStore.get(key)
      if (!list) {
        list = []
        listStore.set(key, list)
      }
      list.push(...values)
      return list.length
    }),
    lpop: vi.fn(async () => null),
    rpop: vi.fn(async () => null),
    lrange: vi.fn(async (key: string, start: number, stop: number) => {
      const list = listStore.get(key) ?? []
      const end = stop === -1 ? list.length : stop + 1
      return list.slice(start, end)
    }),
    sadd: vi.fn(async (key: string, ...members: string[]) => {
      let set = setStore.get(key)
      if (!set) {
        set = new Set()
        setStore.set(key, set)
      }
      let added = 0
      for (const member of members) {
        if (!set.has(member)) {
          set.add(member)
          added++
        }
      }
      return added
    }),
    smembers: vi.fn(async (key: string) => {
      const set = setStore.get(key)
      return set ? Array.from(set) : []
    }),
    srem: vi.fn(async () => 0),
    sismember: vi.fn(async () => 0),
    zadd: vi.fn(async (key: string, score: number, member: string) => {
      let zset = zsetStore.get(key)
      if (!zset) {
        zset = new Map()
        zsetStore.set(key, zset)
      }
      const isNew = !zset.has(member)
      zset.set(member, score)
      return isNew ? 1 : 0
    }),
    zrange: vi.fn(async (key: string, start: number, stop: number) => {
      const zset = zsetStore.get(key)
      if (!zset) return []
      const sorted = Array.from(zset.entries()).sort((a, b) => a[1] - b[1])
      const end = stop === -1 ? sorted.length : stop + 1
      return sorted.slice(start, end).map(([member]) => member)
    }),
    zrem: vi.fn(async () => 0),
    zscore: vi.fn(async () => null),
    incr: vi.fn(async (key: string) => {
      const value = parseInt(store.get(key) ?? '0', 10) + 1
      store.set(key, String(value))
      return value
    }),
    incrby: vi.fn(async () => 0),
    decr: vi.fn(async () => 0),
    decrby: vi.fn(async () => 0),
    append: vi.fn(async () => 0),
    strlen: vi.fn(async () => 0),
    type: vi.fn(async () => 'none'),
    rename: vi.fn(async () => 'OK'),
    dbsize: vi.fn(async () => store.size),
    flushdb: vi.fn(async () => {
      store.clear()
      return 'OK'
    }),
    info: vi.fn(async () => 'Redois Server'),
    ping: vi.fn(async (message?: string) => message ?? 'PONG'),
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('McpServer', () => {
  let server: McpServer
  let transport: MockTransport

  beforeEach(() => {
    server = new McpServer({
      name: 'test-server',
      version: '1.0.0',
      instructions: 'Test MCP server',
    })
    transport = new MockTransport()
  })

  // ─────────────────────────────────────────────────────────────────
  // Server Configuration
  // ─────────────────────────────────────────────────────────────────

  describe('Server Configuration', () => {
    it('should create server with name and version', () => {
      const info = server.getServerInfo()
      expect(info.name).toBe('test-server')
      expect(info.version).toBe('1.0.0')
    })

    it('should initialize with default capabilities', () => {
      expect(server.isInitialized()).toBe(false)
    })

    it('should return empty tools array when no tools registered', () => {
      const tools = server.getTools()
      expect(tools).toEqual([])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Tool Registration
  // ─────────────────────────────────────────────────────────────────

  describe('Tool Registration', () => {
    it('should register a tool without description', () => {
      server.tool(
        'echo',
        {
          type: 'object',
          properties: {
            message: { type: 'string' },
          },
          required: ['message'],
        },
        async (args: { message: string }) => ({
          content: [{ type: 'text', text: args.message }],
        })
      )

      const tools = server.getTools()
      expect(tools).toHaveLength(1)
      expect(tools[0].definition.name).toBe('echo')
      expect(tools[0].definition.description).toBeUndefined()
    })

    it('should register a tool with description', () => {
      server.toolWithDescription(
        'echo',
        'Echoes the message back',
        {
          type: 'object',
          properties: {
            message: { type: 'string' },
          },
          required: ['message'],
        },
        async (args: { message: string }) => ({
          content: [{ type: 'text', text: args.message }],
        })
      )

      const tools = server.getTools()
      expect(tools).toHaveLength(1)
      expect(tools[0].definition.name).toBe('echo')
      expect(tools[0].definition.description).toBe('Echoes the message back')
    })

    it('should register a tool with annotations', () => {
      server.toolWithDescription(
        'read-only-tool',
        'A read-only tool',
        { type: 'object', properties: {} },
        async () => ({ content: [{ type: 'text', text: 'done' }] }),
        { readOnlyHint: true }
      )

      const tools = server.getTools()
      expect(tools[0].definition.annotations?.readOnlyHint).toBe(true)
    })

    it('should support chaining tool registration', () => {
      const result = server
        .tool('tool1', { type: 'object', properties: {} }, async () => ({
          content: [],
        }))
        .tool('tool2', { type: 'object', properties: {} }, async () => ({
          content: [],
        }))

      expect(result).toBe(server)
      expect(server.getTools()).toHaveLength(2)
    })

    it('should overwrite tool with same name', () => {
      server.tool('echo', { type: 'object', properties: {} }, async () => ({
        content: [{ type: 'text', text: 'first' }],
      }))

      server.tool('echo', { type: 'object', properties: {} }, async () => ({
        content: [{ type: 'text', text: 'second' }],
      }))

      const tools = server.getTools()
      expect(tools).toHaveLength(1)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Transport Connection
  // ─────────────────────────────────────────────────────────────────

  describe('Transport Connection', () => {
    it('should connect to transport', async () => {
      await server.connect(transport)
      expect(transport.started).toBe(true)
    })

    it('should close transport on server close', async () => {
      await server.connect(transport)
      await server.close()
      expect(transport.closed).toBe(true)
      expect(server.isInitialized()).toBe(false)
    })

    it('should handle transport close event', async () => {
      await server.connect(transport)

      // Simulate initialization first
      await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          clientInfo: { name: 'test', version: '1.0.0' },
        },
      })

      expect(server.isInitialized()).toBe(true)

      // Trigger close
      transport.onClose?.()
      expect(server.isInitialized()).toBe(false)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // JSON-RPC Request Handling
  // ─────────────────────────────────────────────────────────────────

  describe('JSON-RPC Request Handling', () => {
    describe('Request Validation', () => {
      it('should reject invalid JSON-RPC version', async () => {
        const response = await server.handleRequest({
          jsonrpc: '1.0' as '2.0',
          id: 1,
          method: 'ping',
        })

        expect(response?.error).toBeDefined()
        expect(response?.error?.code).toBe(-32600)
        expect(response?.error?.message).toBe('Invalid JSON-RPC version')
      })

      it('should reject missing method', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: '',
        })

        expect(response?.error).toBeDefined()
        expect(response?.error?.code).toBe(-32600)
        expect(response?.error?.message).toBe('Method is required')
      })

      it('should return proper JSON-RPC response format', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'ping',
        })

        expect(response?.jsonrpc).toBe('2.0')
        expect(response?.id).toBe(1)
      })

      it('should handle requests without id (notifications)', async () => {
        // Note: The server implementation returns a response with id: null
        // for requests without an id field (per the current implementation)
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          method: 'initialized',
        })

        // Server returns response with null id (not strictly null for notifications)
        expect(response?.jsonrpc).toBe('2.0')
        expect(response?.id).toBeNull()
        expect(response?.result).toEqual({})
      })
    })

    describe('Initialize Method', () => {
      it('should handle initialize request', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'initialize',
          params: {
            protocolVersion: '2024-11-05',
            capabilities: {},
            clientInfo: { name: 'test-client', version: '1.0.0' },
          },
        })

        expect(response?.result).toBeDefined()
        const result = response?.result as {
          protocolVersion: string
          serverInfo: { name: string; version: string }
          capabilities: { tools: { listChanged: boolean } }
          instructions: string
        }
        expect(result.protocolVersion).toBe('2024-11-05')
        expect(result.serverInfo.name).toBe('test-server')
        expect(result.serverInfo.version).toBe('1.0.0')
        expect(result.capabilities.tools).toEqual({ listChanged: false })
        expect(result.instructions).toBe('Test MCP server')
      })

      it('should set initialized state after initialize', async () => {
        expect(server.isInitialized()).toBe(false)

        await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'initialize',
          params: {
            protocolVersion: '2024-11-05',
            capabilities: {},
            clientInfo: { name: 'test', version: '1.0.0' },
          },
        })

        expect(server.isInitialized()).toBe(true)
      })

      it('should handle initialized notification', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'initialized',
        })

        expect(response?.result).toEqual({})
      })
    })

    describe('Ping Method', () => {
      it('should handle ping request', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'ping',
        })

        expect(response?.result).toEqual({})
      })
    })

    describe('Logging Methods', () => {
      it('should handle logging/setLevel request', async () => {
        expect(server.logLevel).toBe('info')

        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'logging/setLevel',
          params: { level: 'debug' },
        })

        expect(response?.result).toEqual({})
        expect(server.logLevel).toBe('debug')
      })
    })

    describe('Unknown Methods', () => {
      it('should return error for unknown method', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'unknown/method',
        })

        expect(response?.error).toBeDefined()
        expect(response?.error?.code).toBe(-32603)
        expect(response?.error?.message).toContain('Method not found')
      })
    })

    describe('Resource and Prompt Methods', () => {
      it('should return empty resources list', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'resources/list',
        })

        expect(response?.result).toEqual({ resources: [] })
      })

      it('should throw error for resources/read', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'resources/read',
          params: { uri: 'test://resource' },
        })

        expect(response?.error).toBeDefined()
        expect(response?.error?.message).toContain('Resource not found')
      })

      it('should return empty prompts list', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'prompts/list',
        })

        expect(response?.result).toEqual({ prompts: [] })
      })

      it('should throw error for prompts/get', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'prompts/get',
          params: { name: 'test-prompt' },
        })

        expect(response?.error).toBeDefined()
        expect(response?.error?.message).toContain('Prompt not found')
      })
    })

    describe('Cancellation Notification', () => {
      it('should handle notifications/cancelled', async () => {
        const response = await server.handleRequest({
          jsonrpc: '2.0',
          id: 1,
          method: 'notifications/cancelled',
          params: { requestId: '123' },
        })

        // Cancellation returns null result
        expect(response?.result).toBeNull()
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Tool Listing
  // ─────────────────────────────────────────────────────────────────

  describe('Tool Listing (tools/list)', () => {
    it('should return empty tools list when no tools registered', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/list',
      })

      expect(response?.result).toEqual({ tools: [] })
    })

    it('should return registered tools', async () => {
      server.toolWithDescription(
        'echo',
        'Echoes the message back',
        {
          type: 'object',
          properties: {
            message: { type: 'string', description: 'Message to echo' },
          },
          required: ['message'],
        },
        async (args: { message: string }) => ({
          content: [{ type: 'text', text: args.message }],
        })
      )

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/list',
      })

      const result = response?.result as { tools: unknown[] }
      expect(result.tools).toHaveLength(1)
      expect(result.tools[0]).toMatchObject({
        name: 'echo',
        description: 'Echoes the message back',
        inputSchema: {
          type: 'object',
          properties: {
            message: { type: 'string', description: 'Message to echo' },
          },
          required: ['message'],
        },
      })
    })

    it('should return multiple registered tools', async () => {
      server
        .tool('tool1', { type: 'object', properties: {} }, async () => ({
          content: [],
        }))
        .tool('tool2', { type: 'object', properties: {} }, async () => ({
          content: [],
        }))
        .tool('tool3', { type: 'object', properties: {} }, async () => ({
          content: [],
        }))

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/list',
      })

      const result = response?.result as { tools: unknown[] }
      expect(result.tools).toHaveLength(3)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Tool Execution
  // ─────────────────────────────────────────────────────────────────

  describe('Tool Execution (tools/call)', () => {
    beforeEach(() => {
      server.toolWithDescription<{ message: string }>(
        'echo',
        'Echoes the message back',
        {
          type: 'object',
          properties: {
            message: { type: 'string' },
          },
          required: ['message'],
        },
        async (args) => ({
          content: [{ type: 'text', text: args.message }],
        })
      )

      server.toolWithDescription<{ a: number; b: number }>(
        'add',
        'Adds two numbers',
        {
          type: 'object',
          properties: {
            a: { type: 'number' },
            b: { type: 'number' },
          },
          required: ['a', 'b'],
        },
        async (args) => ({
          content: [{ type: 'text', text: String(args.a + args.b) }],
        })
      )
    })

    it('should execute tool and return result', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'echo',
          arguments: { message: 'Hello, World!' },
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.content).toHaveLength(1)
      expect(result.content[0]).toEqual({ type: 'text', text: 'Hello, World!' })
      expect(result.isError).toBeUndefined()
    })

    it('should execute tool with numeric arguments', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'add',
          arguments: { a: 5, b: 3 },
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.content[0]).toEqual({ type: 'text', text: '8' })
    })

    it('should handle tool with default arguments', async () => {
      server.toolWithDescription(
        'greet',
        'Greets someone',
        {
          type: 'object',
          properties: {
            name: { type: 'string' },
          },
        },
        async (args: { name?: string }) => ({
          content: [{ type: 'text', text: `Hello, ${args.name ?? 'World'}!` }],
        })
      )

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'greet',
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.content[0]).toEqual({ type: 'text', text: 'Hello, World!' })
    })

    it('should return error for unknown tool', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'nonexistent',
          arguments: {},
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.isError).toBe(true)
      expect(result.content[0]).toEqual({ type: 'text', text: 'Tool not found: nonexistent' })
    })

    it('should handle tool execution error', async () => {
      server.tool(
        'failing-tool',
        { type: 'object', properties: {} },
        async () => {
          throw new Error('Something went wrong')
        }
      )

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'failing-tool',
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.isError).toBe(true)
      expect(result.content[0]).toMatchObject({
        type: 'text',
        text: expect.stringContaining('Error executing tool failing-tool'),
      })
    })

    it('should pass context to tool handler', async () => {
      let capturedContext: McpToolContext | null = null

      server.tool(
        'context-tool',
        { type: 'object', properties: {} },
        async (_args, context) => {
          capturedContext = context
          return { content: [{ type: 'text', text: 'done' }] }
        }
      )

      await server.handleRequest({
        jsonrpc: '2.0',
        id: 42,
        method: 'tools/call',
        params: {
          name: 'context-tool',
        },
      })

      expect(capturedContext).toBeDefined()
      expect(capturedContext?.requestId).toBe(42)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Error Handling
  // ─────────────────────────────────────────────────────────────────

  describe('Error Handling', () => {
    it('should handle null id in error response', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: null,
        method: 'unknown/method',
      })

      expect(response?.id).toBeNull()
      expect(response?.error).toBeDefined()
    })

    it('should handle string id', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 'request-123',
        method: 'ping',
      })

      expect(response?.id).toBe('request-123')
    })

    it('should catch and wrap handler exceptions', async () => {
      server.tool(
        'throws',
        { type: 'object', properties: {} },
        async () => {
          throw new Error('Unexpected error')
        }
      )

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: { name: 'throws' },
      })

      expect(response?.error).toBeUndefined()
      const result = response?.result as McpToolCallResult
      expect(result.isError).toBe(true)
    })

    it('should handle non-Error exceptions', async () => {
      server.tool(
        'throws-string',
        { type: 'object', properties: {} },
        async () => {
          throw 'String error'
        }
      )

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: { name: 'throws-string' },
      })

      const result = response?.result as McpToolCallResult
      expect(result.isError).toBe(true)
      expect(result.content[0]).toMatchObject({
        type: 'text',
        text: expect.stringContaining('Unknown error'),
      })
    })
  })
})

// ─────────────────────────────────────────────────────────────────
// Built-in Redis Tools Tests
// ─────────────────────────────────────────────────────────────────

describe('McpServer with Redis Proxy', () => {
  let server: McpServer
  let redisProxy: RedisProxy

  beforeEach(() => {
    server = new McpServer({
      name: 'redis-server',
      version: '1.0.0',
    })
    redisProxy = createMockRedisProxy()
    server.setRedisProxy(redisProxy)
  })

  describe('Built-in Tool Registration', () => {
    it('should register built-in Redis tools', () => {
      const tools = server.getTools()
      const toolNames = tools.map((t) => t.definition.name)

      expect(toolNames).toContain('get')
      expect(toolNames).toContain('set')
      expect(toolNames).toContain('keys')
      expect(toolNames).toContain('scan')
      expect(toolNames).toContain('do')
      expect(toolNames).toContain('del')
      expect(toolNames).toContain('exists')
      expect(toolNames).toContain('expire')
      expect(toolNames).toContain('ttl')
      expect(toolNames).toContain('type')
      expect(toolNames).toContain('hget')
      expect(toolNames).toContain('hset')
      expect(toolNames).toContain('hgetall')
      expect(toolNames).toContain('incr')
      expect(toolNames).toContain('lpush')
      expect(toolNames).toContain('lrange')
      expect(toolNames).toContain('sadd')
      expect(toolNames).toContain('smembers')
      expect(toolNames).toContain('zadd')
      expect(toolNames).toContain('zrange')
      expect(toolNames).toContain('dbsize')
      expect(toolNames).toContain('info')
      expect(toolNames).toContain('ping')
    })

    it('should have proper annotations on read-only tools', () => {
      const tools = server.getTools()
      const getTool = tools.find((t) => t.definition.name === 'get')
      const setTool = tools.find((t) => t.definition.name === 'set')

      expect(getTool?.definition.annotations?.readOnlyHint).toBe(true)
      expect(setTool?.definition.annotations?.destructiveHint).toBe(true)
    })
  })

  describe('GET Tool Execution', () => {
    it('should execute get tool and return value', async () => {
      // Set a value first
      await redisProxy.set('mykey', 'myvalue')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'get',
          arguments: { key: 'mykey' },
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.content[0].type).toBe('text')
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.key).toBe('mykey')
      expect(data.value).toBe('myvalue')
    })

    it('should return null for non-existent key', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'get',
          arguments: { key: 'nonexistent' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.value).toBeNull()
    })
  })

  describe('SET Tool Execution', () => {
    it('should execute set tool', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'set',
          arguments: { key: 'newkey', value: 'newvalue' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.success).toBe(true)
      expect(data.result).toBe('OK')

      // Verify the value was set
      const value = await redisProxy.get('newkey')
      expect(value).toBe('newvalue')
    })

    it('should execute set with options', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'set',
          arguments: { key: 'exkey', value: 'exvalue', ex: 60 },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.success).toBe(true)
    })
  })

  describe('KEYS Tool Execution', () => {
    it('should execute keys tool', async () => {
      await redisProxy.set('user:1', 'alice')
      await redisProxy.set('user:2', 'bob')
      await redisProxy.set('post:1', 'hello')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'keys',
          arguments: { pattern: 'user:*' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.pattern).toBe('user:*')
      expect(data.keys).toContain('user:1')
      expect(data.keys).toContain('user:2')
      expect(data.keys).not.toContain('post:1')
    })
  })

  describe('SCAN Tool Execution', () => {
    it('should execute scan tool', async () => {
      await redisProxy.set('key1', 'val1')
      await redisProxy.set('key2', 'val2')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'scan',
          arguments: { cursor: 0 },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.cursor).toBeDefined()
      expect(Array.isArray(data.keys)).toBe(true)
      expect(data.count).toBeGreaterThanOrEqual(0)
    })

    it('should execute scan with match pattern', async () => {
      await redisProxy.set('user:1', 'alice')
      await redisProxy.set('post:1', 'hello')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'scan',
          arguments: { cursor: 0, match: 'user:*' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.keys).toContain('user:1')
    })
  })

  describe('DO Tool Execution', () => {
    it('should return code execution placeholder', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'do',
          arguments: { code: 'return redis.get("key")' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.message).toContain('sandbox')
      expect(data.code).toBe('return redis.get("key")')
    })
  })

  describe('Hash Tools Execution', () => {
    it('should execute hset and hget', async () => {
      // HSET
      const setResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'hset',
          arguments: { key: 'user:1', field: 'name', value: 'Alice' },
        },
      })

      const setResult = setResponse?.result as McpToolCallResult
      const setData = JSON.parse((setResult.content[0] as { type: 'text'; text: string }).text)
      expect(setData.success).toBe(true)

      // HGET
      const getResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: {
          name: 'hget',
          arguments: { key: 'user:1', field: 'name' },
        },
      })

      const getResult = getResponse?.result as McpToolCallResult
      const getData = JSON.parse((getResult.content[0] as { type: 'text'; text: string }).text)
      expect(getData.value).toBe('Alice')
    })

    it('should execute hgetall', async () => {
      await redisProxy.hset('user:1', 'name', 'Alice')
      await redisProxy.hset('user:1', 'email', 'alice@example.com')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'hgetall',
          arguments: { key: 'user:1' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.value.name).toBe('Alice')
      expect(data.value.email).toBe('alice@example.com')
    })
  })

  describe('List Tools Execution', () => {
    it('should execute lpush and lrange', async () => {
      // LPUSH
      const pushResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'lpush',
          arguments: { key: 'mylist', values: ['a', 'b', 'c'] },
        },
      })

      const pushResult = pushResponse?.result as McpToolCallResult
      const pushData = JSON.parse((pushResult.content[0] as { type: 'text'; text: string }).text)
      expect(pushData.length).toBe(3)

      // LRANGE
      const rangeResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: {
          name: 'lrange',
          arguments: { key: 'mylist', start: 0, stop: -1 },
        },
      })

      const rangeResult = rangeResponse?.result as McpToolCallResult
      const rangeData = JSON.parse((rangeResult.content[0] as { type: 'text'; text: string }).text)
      expect(rangeData.values).toHaveLength(3)
    })
  })

  describe('Set Tools Execution', () => {
    it('should execute sadd and smembers', async () => {
      // SADD
      const addResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'sadd',
          arguments: { key: 'myset', members: ['a', 'b', 'c'] },
        },
      })

      const addResult = addResponse?.result as McpToolCallResult
      const addData = JSON.parse((addResult.content[0] as { type: 'text'; text: string }).text)
      expect(addData.added).toBe(3)

      // SMEMBERS
      const membersResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: {
          name: 'smembers',
          arguments: { key: 'myset' },
        },
      })

      const membersResult = membersResponse?.result as McpToolCallResult
      const membersData = JSON.parse((membersResult.content[0] as { type: 'text'; text: string }).text)
      expect(membersData.members).toContain('a')
      expect(membersData.members).toContain('b')
      expect(membersData.members).toContain('c')
    })
  })

  describe('Sorted Set Tools Execution', () => {
    it('should execute zadd and zrange', async () => {
      // ZADD
      const addResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'zadd',
          arguments: { key: 'leaderboard', score: 100, member: 'player1' },
        },
      })

      const addResult = addResponse?.result as McpToolCallResult
      const addData = JSON.parse((addResult.content[0] as { type: 'text'; text: string }).text)
      expect(addData.added).toBe(1)

      // Add more members
      await redisProxy.zadd('leaderboard', 200, 'player2')
      await redisProxy.zadd('leaderboard', 150, 'player3')

      // ZRANGE
      const rangeResponse = await server.handleRequest({
        jsonrpc: '2.0',
        id: 2,
        method: 'tools/call',
        params: {
          name: 'zrange',
          arguments: { key: 'leaderboard', start: 0, stop: -1 },
        },
      })

      const rangeResult = rangeResponse?.result as McpToolCallResult
      const rangeData = JSON.parse((rangeResult.content[0] as { type: 'text'; text: string }).text)
      expect(rangeData.members).toHaveLength(3)
    })
  })

  describe('Server Info Tools', () => {
    it('should execute dbsize', async () => {
      await redisProxy.set('key1', 'val1')
      await redisProxy.set('key2', 'val2')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'dbsize',
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.keys).toBe(2)
    })

    it('should execute info', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'info',
        },
      })

      const result = response?.result as McpToolCallResult
      expect(result.content[0].type).toBe('text')
      expect((result.content[0] as { type: 'text'; text: string }).text).toContain('Redois')
    })

    it('should execute ping', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'ping',
        },
      })

      const result = response?.result as McpToolCallResult
      expect((result.content[0] as { type: 'text'; text: string }).text).toBe('PONG')
    })

    it('should execute ping with custom message', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'ping',
          arguments: { message: 'Hello!' },
        },
      })

      const result = response?.result as McpToolCallResult
      expect((result.content[0] as { type: 'text'; text: string }).text).toBe('Hello!')
    })
  })

  describe('DEL and EXISTS Tools', () => {
    it('should execute del', async () => {
      await redisProxy.set('key1', 'val1')
      await redisProxy.set('key2', 'val2')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'del',
          arguments: { keys: ['key1', 'key2'] },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.deleted).toBe(2)
    })

    it('should execute exists', async () => {
      await redisProxy.set('key1', 'val1')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'exists',
          arguments: { keys: ['key1', 'nonexistent'] },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.exists).toBe(1)
    })
  })

  describe('EXPIRE and TTL Tools', () => {
    it('should execute expire', async () => {
      await redisProxy.set('tempkey', 'tempval')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'expire',
          arguments: { key: 'tempkey', seconds: 60 },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.success).toBe(true)
    })

    it('should execute ttl', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'ttl',
          arguments: { key: 'somekey' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.ttl).toBeDefined()
    })
  })

  describe('TYPE Tool', () => {
    it('should execute type', async () => {
      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'type',
          arguments: { key: 'nonexistent' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.type).toBeDefined()
    })
  })

  describe('INCR Tool', () => {
    it('should execute incr', async () => {
      await redisProxy.set('counter', '10')

      const response = await server.handleRequest({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: {
          name: 'incr',
          arguments: { key: 'counter' },
        },
      })

      const result = response?.result as McpToolCallResult
      const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
      expect(data.value).toBe(11)
    })
  })
})

// ─────────────────────────────────────────────────────────────────
// Factory Function Tests
// ─────────────────────────────────────────────────────────────────

describe('createMcpServer Factory', () => {
  it('should create server with Redis access', () => {
    const mockAccess = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => 'OK'),
      del: vi.fn(async () => 0),
      exists: vi.fn(async () => 0),
      keys: vi.fn(async () => []),
      scan: vi.fn(async () => [0, []] as [number, string[]]),
      hget: vi.fn(async () => null),
      hset: vi.fn(async () => 0),
      hgetall: vi.fn(async () => ({})),
      lpush: vi.fn(async () => 0),
      lrange: vi.fn(async () => []),
      rpush: vi.fn(async () => 0),
      sadd: vi.fn(async () => 0),
      smembers: vi.fn(async () => []),
      zadd: vi.fn(async () => 0),
      zrange: vi.fn(async () => []),
      publish: vi.fn(async () => 0),
      getProxy: vi.fn(),
    }

    const server = createMcpServer({
      name: 'factory-server',
      version: '2.0.0',
      redisAccess: mockAccess,
    })

    expect(server).toBeInstanceOf(McpServer)
    const info = server.getServerInfo()
    expect(info.name).toBe('factory-server')
    expect(info.version).toBe('2.0.0')
  })

  it('should register code loader tool when provided', () => {
    const mockAccess = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => 'OK'),
      del: vi.fn(async () => 0),
      exists: vi.fn(async () => 0),
      keys: vi.fn(async () => []),
      scan: vi.fn(async () => [0, []] as [number, string[]]),
      hget: vi.fn(async () => null),
      hset: vi.fn(async () => 0),
      hgetall: vi.fn(async () => ({})),
      lpush: vi.fn(async () => 0),
      lrange: vi.fn(async () => []),
      rpush: vi.fn(async () => 0),
      sadd: vi.fn(async () => 0),
      smembers: vi.fn(async () => []),
      zadd: vi.fn(async () => 0),
      zrange: vi.fn(async () => []),
      publish: vi.fn(async () => 0),
      getProxy: vi.fn(),
    }

    const mockCodeLoader = {
      execute: vi.fn(async () => ({
        success: true,
        result: 'executed',
        logs: [],
        executionTime: 10,
      })),
    }

    const server = createMcpServer({
      name: 'code-server',
      version: '1.0.0',
      redisAccess: mockAccess,
      codeLoader: mockCodeLoader,
    })

    const tools = server.getTools()
    const doTool = tools.find((t) => t.definition.name === 'do')
    expect(doTool).toBeDefined()
  })

  it('should execute code through code loader', async () => {
    const mockAccess = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => 'OK'),
      del: vi.fn(async () => 0),
      exists: vi.fn(async () => 0),
      keys: vi.fn(async () => []),
      scan: vi.fn(async () => [0, []] as [number, string[]]),
      hget: vi.fn(async () => null),
      hset: vi.fn(async () => 0),
      hgetall: vi.fn(async () => ({})),
      lpush: vi.fn(async () => 0),
      lrange: vi.fn(async () => []),
      rpush: vi.fn(async () => 0),
      sadd: vi.fn(async () => 0),
      smembers: vi.fn(async () => []),
      zadd: vi.fn(async () => 0),
      zrange: vi.fn(async () => []),
      publish: vi.fn(async () => 0),
      getProxy: vi.fn(),
    }

    const mockCodeLoader = {
      execute: vi.fn(async (code: string) => ({
        success: true,
        result: { evaluated: code },
        logs: ['log1'],
        executionTime: 15,
      })),
    }

    const server = createMcpServer({
      name: 'code-server',
      version: '1.0.0',
      redisAccess: mockAccess,
      codeLoader: mockCodeLoader,
    })

    const response = await server.handleRequest({
      jsonrpc: '2.0',
      id: 1,
      method: 'tools/call',
      params: {
        name: 'do',
        arguments: { code: 'return 1 + 1' },
      },
    })

    expect(mockCodeLoader.execute).toHaveBeenCalledWith('return 1 + 1')

    const result = response?.result as McpToolCallResult
    const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
    expect(data.success).toBe(true)
    expect(data.result).toEqual({ evaluated: 'return 1 + 1' })
    expect(data.logs).toEqual(['log1'])
  })

  it('should handle code loader errors', async () => {
    const mockAccess = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => 'OK'),
      del: vi.fn(async () => 0),
      exists: vi.fn(async () => 0),
      keys: vi.fn(async () => []),
      scan: vi.fn(async () => [0, []] as [number, string[]]),
      hget: vi.fn(async () => null),
      hset: vi.fn(async () => 0),
      hgetall: vi.fn(async () => ({})),
      lpush: vi.fn(async () => 0),
      lrange: vi.fn(async () => []),
      rpush: vi.fn(async () => 0),
      sadd: vi.fn(async () => 0),
      smembers: vi.fn(async () => []),
      zadd: vi.fn(async () => 0),
      zrange: vi.fn(async () => []),
      publish: vi.fn(async () => 0),
      getProxy: vi.fn(),
    }

    const mockCodeLoader = {
      execute: vi.fn(async () => ({
        success: false,
        error: 'Syntax error',
        logs: [],
        executionTime: 5,
      })),
    }

    const server = createMcpServer({
      name: 'code-server',
      version: '1.0.0',
      redisAccess: mockAccess,
      codeLoader: mockCodeLoader,
    })

    const response = await server.handleRequest({
      jsonrpc: '2.0',
      id: 1,
      method: 'tools/call',
      params: {
        name: 'do',
        arguments: { code: 'invalid code' },
      },
    })

    const result = response?.result as McpToolCallResult
    expect(result.isError).toBe(true)
    const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
    expect(data.success).toBe(false)
    expect(data.error).toBe('Syntax error')
  })

  it('should handle code loader exceptions', async () => {
    const mockAccess = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => 'OK'),
      del: vi.fn(async () => 0),
      exists: vi.fn(async () => 0),
      keys: vi.fn(async () => []),
      scan: vi.fn(async () => [0, []] as [number, string[]]),
      hget: vi.fn(async () => null),
      hset: vi.fn(async () => 0),
      hgetall: vi.fn(async () => ({})),
      lpush: vi.fn(async () => 0),
      lrange: vi.fn(async () => []),
      rpush: vi.fn(async () => 0),
      sadd: vi.fn(async () => 0),
      smembers: vi.fn(async () => []),
      zadd: vi.fn(async () => 0),
      zrange: vi.fn(async () => []),
      publish: vi.fn(async () => 0),
      getProxy: vi.fn(),
    }

    const mockCodeLoader = {
      execute: vi.fn(async () => {
        throw new Error('Worker crashed')
      }),
    }

    const server = createMcpServer({
      name: 'code-server',
      version: '1.0.0',
      redisAccess: mockAccess,
      codeLoader: mockCodeLoader,
    })

    const response = await server.handleRequest({
      jsonrpc: '2.0',
      id: 1,
      method: 'tools/call',
      params: {
        name: 'do',
        arguments: { code: 'some code' },
      },
    })

    const result = response?.result as McpToolCallResult
    expect(result.isError).toBe(true)
    const data = JSON.parse((result.content[0] as { type: 'text'; text: string }).text)
    expect(data.success).toBe(false)
    expect(data.error).toBe('Worker crashed')
  })
})

// ─────────────────────────────────────────────────────────────────
// Helper Function Tests
// ─────────────────────────────────────────────────────────────────

describe('Helper Functions', () => {
  describe('textContent', () => {
    it('should create text content object', () => {
      const content = textContent('Hello, World!')
      expect(content).toEqual({ type: 'text', text: 'Hello, World!' })
    })

    it('should handle empty string', () => {
      const content = textContent('')
      expect(content).toEqual({ type: 'text', text: '' })
    })
  })

  describe('successResponse', () => {
    it('should create success response with string', () => {
      const response = successResponse('Operation completed')
      expect(response.content).toHaveLength(1)
      expect(response.content[0]).toEqual({ type: 'text', text: 'Operation completed' })
      expect(response.isError).toBeUndefined()
    })

    it('should create success response with object (JSON formatted)', () => {
      const response = successResponse({ result: 'success', count: 5 })
      expect(response.content).toHaveLength(1)
      const text = (response.content[0] as { type: 'text'; text: string }).text
      expect(JSON.parse(text)).toEqual({ result: 'success', count: 5 })
    })
  })

  describe('errorResponse', () => {
    it('should create error response', () => {
      const response = errorResponse('Something went wrong')
      expect(response.content).toHaveLength(1)
      expect(response.content[0]).toEqual({ type: 'text', text: 'Something went wrong' })
      expect(response.isError).toBe(true)
    })
  })
})
