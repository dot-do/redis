#!/usr/bin/env node
/**
 * Redois MCP CLI
 *
 * Command-line interface for MCP (Model Context Protocol) communication
 * Uses stdio transport for JSON-RPC messages.
 *
 * Usage:
 *   npx redois-mcp --url https://your-redois.workers.dev --token your-token
 *
 * Or via stdin/stdout for MCP clients:
 *   echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | npx redois-mcp
 */

import * as readline from 'readline'
import type { JsonRpcRequest, JsonRpcResponse, McpToolCallResult } from './types'

// ─────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────

interface CliConfig {
  url: string
  token?: string
  debug?: boolean
}

// ─────────────────────────────────────────────────────────────────
// MCP Client for CLI
// ─────────────────────────────────────────────────────────────────

class McpStdioClient {
  private config: CliConfig
  private serverInfo: { name: string; version: string } | null = null
  private _initialized = false

  constructor(config: CliConfig) {
    this.config = config
  }

  get isInitialized(): boolean {
    return this._initialized
  }

  /**
   * Handle a JSON-RPC request from stdin
   */
  async handleRequest(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    const { method, params } = request
    const id = request.id ?? null

    try {
      switch (method) {
        case 'initialize':
          return this.handleInitialize(id, params as Record<string, unknown>)

        case 'initialized':
          // Notification - no response needed
          return { jsonrpc: '2.0', id, result: {} }

        case 'ping':
          return { jsonrpc: '2.0', id, result: {} }

        case 'tools/list':
          return this.handleToolsList(id)

        case 'tools/call':
          return this.handleToolCall(id, params as { name: string; arguments?: Record<string, unknown> })

        case 'resources/list':
          return { jsonrpc: '2.0', id, result: { resources: [] } }

        case 'prompts/list':
          return { jsonrpc: '2.0', id, result: { prompts: [] } }

        default:
          return this.errorResponse(id, -32601, `Method not found: ${method}`)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      return this.errorResponse(id, -32603, message)
    }
  }

  /**
   * Handle initialize request
   */
  private handleInitialize(
    id: string | number | null,
    params: Record<string, unknown>
  ): JsonRpcResponse {
    this._initialized = true
    this.serverInfo = {
      name: 'redois-mcp-cli',
      version: '0.1.0',
    }

    return {
      jsonrpc: '2.0',
      id,
      result: {
        protocolVersion: (params?.protocolVersion as string) || '2024-11-05',
        capabilities: {
          tools: { listChanged: false },
        },
        serverInfo: this.serverInfo,
        instructions: 'Redois MCP CLI - Connect to a Redois instance via HTTP RPC',
      },
    }
  }

  /**
   * Handle tools/list request
   */
  private async handleToolsList(id: string | number | null): Promise<JsonRpcResponse> {
    const tools = [
      {
        name: 'get',
        description: 'Get the value of a key from Redis',
        inputSchema: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'The key to retrieve' },
          },
          required: ['key'],
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'set',
        description: 'Set the value of a key in Redis with optional expiration',
        inputSchema: {
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
        annotations: { destructiveHint: true },
      },
      {
        name: 'del',
        description: 'Delete one or more keys',
        inputSchema: {
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
        annotations: { destructiveHint: true },
      },
      {
        name: 'keys',
        description: 'Find all keys matching a pattern (use * for all keys)',
        inputSchema: {
          type: 'object',
          properties: {
            pattern: { type: 'string', description: 'Pattern to match (e.g., "user:*" or "*")' },
          },
          required: ['pattern'],
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'scan',
        description: 'Incrementally iterate over keys with optional filtering',
        inputSchema: {
          type: 'object',
          properties: {
            cursor: { type: 'number', description: 'Cursor position (start with 0)' },
            match: { type: 'string', description: 'Pattern to match keys' },
            count: { type: 'number', description: 'Number of keys to return per iteration' },
            type: { type: 'string', description: 'Filter by key type (string, hash, list, set, zset)' },
          },
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'hget',
        description: 'Get the value of a hash field',
        inputSchema: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'The hash key' },
            field: { type: 'string', description: 'The field to retrieve' },
          },
          required: ['key', 'field'],
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'hset',
        description: 'Set the value of a hash field',
        inputSchema: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'The hash key' },
            field: { type: 'string', description: 'The field to set' },
            value: { type: 'string', description: 'The value to store' },
          },
          required: ['key', 'field', 'value'],
        },
        annotations: { destructiveHint: true },
      },
      {
        name: 'hgetall',
        description: 'Get all fields and values of a hash',
        inputSchema: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'The hash key' },
          },
          required: ['key'],
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'lpush',
        description: 'Push values to the head of a list',
        inputSchema: {
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
        annotations: { destructiveHint: true },
      },
      {
        name: 'rpush',
        description: 'Push values to the tail of a list',
        inputSchema: {
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
        annotations: { destructiveHint: true },
      },
      {
        name: 'lrange',
        description: 'Get a range of elements from a list',
        inputSchema: {
          type: 'object',
          properties: {
            key: { type: 'string', description: 'The list key' },
            start: { type: 'number', description: 'Start index (0-based, negative from end)' },
            stop: { type: 'number', description: 'Stop index (inclusive, -1 for end)' },
          },
          required: ['key', 'start', 'stop'],
        },
        annotations: { readOnlyHint: true },
      },
      {
        name: 'do',
        description: 'Execute JavaScript code with access to Redis operations (sandboxed)',
        inputSchema: {
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
        annotations: { destructiveHint: true },
      },
    ]

    return {
      jsonrpc: '2.0',
      id,
      result: { tools },
    }
  }

  /**
   * Handle tools/call request - forward to Redois server
   */
  private async handleToolCall(
    id: string | number | null,
    params: { name: string; arguments?: Record<string, unknown> }
  ): Promise<JsonRpcResponse> {
    const { name, arguments: args = {} } = params

    try {
      // Make RPC call to Redois server
      const result = await this.executeRpc(name, args)

      const toolResult: McpToolCallResult = {
        content: [
          {
            type: 'text',
            text: typeof result === 'string' ? result : JSON.stringify(result, null, 2),
          },
        ],
      }

      return {
        jsonrpc: '2.0',
        id,
        result: toolResult,
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'

      const toolResult: McpToolCallResult = {
        content: [{ type: 'text', text: `Error: ${message}` }],
        isError: true,
      }

      return {
        jsonrpc: '2.0',
        id,
        result: toolResult,
      }
    }
  }

  /**
   * Execute an RPC call to the Redois server
   */
  private async executeRpc(method: string, params: Record<string, unknown>): Promise<unknown> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }

    if (this.config.token) {
      headers['Authorization'] = `Bearer ${this.config.token}`
    }

    const request = {
      jsonrpc: '2.0',
      id: Date.now(),
      method,
      params,
    }

    const response = await fetch(`${this.config.url}/rpc`, {
      method: 'POST',
      headers,
      body: JSON.stringify(request),
    })

    if (!response.ok) {
      throw new Error(`HTTP error: ${response.status} ${response.statusText}`)
    }

    const result = (await response.json()) as JsonRpcResponse

    if (result.error) {
      throw new Error(result.error.message)
    }

    return result.result
  }

  /**
   * Create an error response
   */
  private errorResponse(
    id: string | number | null,
    code: number,
    message: string
  ): JsonRpcResponse {
    return {
      jsonrpc: '2.0',
      id,
      error: { code, message },
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Stdio Transport
// ─────────────────────────────────────────────────────────────────

class StdioTransport {
  private client: McpStdioClient
  private rl: readline.Interface
  private debug: boolean

  constructor(client: McpStdioClient, debug = false) {
    this.client = client
    this.debug = debug

    this.rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false,
    })
  }

  /**
   * Start the stdio transport
   */
  start(): void {
    this.rl.on('line', async (line) => {
      if (!line.trim()) return

      try {
        const request = JSON.parse(line) as JsonRpcRequest

        if (this.debug) {
          console.error('[DEBUG] Received:', JSON.stringify(request))
        }

        const response = await this.client.handleRequest(request)

        const responseJson = JSON.stringify(response)
        process.stdout.write(responseJson + '\n')

        if (this.debug) {
          console.error('[DEBUG] Sent:', responseJson)
        }
      } catch (error) {
        const errorResponse: JsonRpcResponse = {
          jsonrpc: '2.0',
          id: null,
          error: {
            code: -32700,
            message: error instanceof Error ? error.message : 'Parse error',
          },
        }
        process.stdout.write(JSON.stringify(errorResponse) + '\n')
      }
    })

    this.rl.on('close', () => {
      process.exit(0)
    })

    // Handle errors
    process.stdin.on('error', () => {
      process.exit(1)
    })

    process.stdout.on('error', () => {
      process.exit(1)
    })
  }
}

// ─────────────────────────────────────────────────────────────────
// CLI Entry Point
// ─────────────────────────────────────────────────────────────────

function parseArgs(): CliConfig {
  const args = process.argv.slice(2)
  const config: Partial<CliConfig> = {}

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    switch (arg) {
      case '--url':
      case '-u':
        config.url = args[++i]
        break
      case '--token':
      case '-t':
        config.token = args[++i]
        break
      case '--debug':
      case '-d':
        config.debug = true
        break
      case '--help':
      case '-h':
        printHelp()
        process.exit(0)
        break
      case '--version':
      case '-v':
        console.log('redois-mcp-cli v0.1.0')
        process.exit(0)
        break
    }
  }

  // Check environment variables
  if (!config.url && process.env.REDOIS_URL) {
    config.url = process.env.REDOIS_URL
  }
  if (!config.token && process.env.REDOIS_TOKEN) {
    config.token = process.env.REDOIS_TOKEN
  }

  if (!config.url) {
    console.error('Error: --url is required or set REDOIS_URL environment variable')
    printHelp()
    process.exit(1)
  }

  return config as CliConfig
}

function printHelp(): void {
  console.log(`
Redois MCP CLI - Model Context Protocol interface for Redois

Usage:
  redois-mcp [options]

Options:
  -u, --url <url>      Redois server URL (required, or set REDOIS_URL)
  -t, --token <token>  Authentication token (or set REDOIS_TOKEN)
  -d, --debug          Enable debug logging to stderr
  -h, --help           Show this help message
  -v, --version        Show version

Examples:
  # Start MCP server with stdio transport
  redois-mcp --url https://redois.example.com --token abc123

  # Using environment variables
  REDOIS_URL=https://redois.example.com REDOIS_TOKEN=abc123 redois-mcp

  # With Claude Desktop or other MCP clients
  echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | redois-mcp --url https://redois.example.com

MCP Configuration (claude_desktop_config.json):
  {
    "mcpServers": {
      "redois": {
        "command": "npx",
        "args": ["redois-mcp", "--url", "https://your-redois.workers.dev"],
        "env": {
          "REDOIS_TOKEN": "your-token"
        }
      }
    }
  }
`)
}

// ─────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const config = parseArgs()

  if (config.debug) {
    console.error(`[DEBUG] Starting MCP CLI with URL: ${config.url}`)
  }

  const client = new McpStdioClient(config)
  const transport = new StdioTransport(client, config.debug)

  transport.start()
}

// Only run if this is the main module
if (require.main === module) {
  main().catch((error) => {
    console.error('Fatal error:', error)
    process.exit(1)
  })
}

// Export for testing
export { McpStdioClient, StdioTransport, parseArgs }
