/**
 * Stdio Transport for MCP
 *
 * Provides stdio-based transport for MCP communication:
 * - Read JSON-RPC from stdin
 * - Write responses to stdout
 * - Graceful shutdown handling
 */

import type {
  McpTransport,
  McpStdioTransportOptions,
  JsonRpcRequest,
  JsonRpcResponse,
  JsonRpcNotification,
} from '../types'

// ─────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────

// Timeout constant reserved for future request timeout implementation
const _DEFAULT_TIMEOUT = 30000 // 30 seconds
void _DEFAULT_TIMEOUT

// ─────────────────────────────────────────────────────────────────
// Line Reader
// ─────────────────────────────────────────────────────────────────

/**
 * Simple line reader for stdin
 */
class LineReader {
  private buffer = ''
  private lines: string[] = []
  private resolvers: Array<(line: string) => void> = []
  private closed = false

  constructor(stream: NodeJS.ReadableStream) {
    stream.setEncoding('utf8')

    stream.on('data', (chunk: string) => {
      this.buffer += chunk
      this.processBuffer()
    })

    stream.on('end', () => {
      this.closed = true
      // Process any remaining data as final line
      if (this.buffer.length > 0) {
        this.lines.push(this.buffer)
        this.buffer = ''
        this.processResolvers()
      }
    })

    stream.on('error', (error) => {
      console.error('[Stdio] Stream error:', error)
      this.closed = true
    })
  }

  private processBuffer(): void {
    const parts = this.buffer.split('\n')
    this.buffer = parts.pop() || ''

    for (const part of parts) {
      if (part.trim()) {
        this.lines.push(part)
      }
    }

    this.processResolvers()
  }

  private processResolvers(): void {
    while (this.lines.length > 0 && this.resolvers.length > 0) {
      const resolver = this.resolvers.shift()!
      const line = this.lines.shift()!
      resolver(line)
    }
  }

  async readLine(): Promise<string | null> {
    if (this.lines.length > 0) {
      return this.lines.shift()!
    }

    if (this.closed) {
      return null
    }

    return new Promise((resolve) => {
      this.resolvers.push(resolve)
    })
  }

  isClosed(): boolean {
    return this.closed && this.lines.length === 0
  }
}

// ─────────────────────────────────────────────────────────────────
// Stdio Transport Class
// ─────────────────────────────────────────────────────────────────

export class StdioTransport implements McpTransport {
  private stdin: NodeJS.ReadableStream
  private stdout: NodeJS.WritableStream
  private reader: LineReader | null = null
  private running = false

  onMessage?: (message: JsonRpcRequest) => void
  onClose?: () => void
  onError?: (error: Error) => void

  constructor(options: McpStdioTransportOptions = {}) {
    this.stdin = options.stdin || process.stdin
    this.stdout = options.stdout || process.stdout
    // Note: timeout from options could be used in future for request timeouts
    void options.timeout
  }

  /**
   * Start the transport
   */
  async start(): Promise<void> {
    if (this.running) {
      return
    }

    this.running = true
    this.reader = new LineReader(this.stdin)

    // Start reading messages
    this.readLoop().catch((error) => {
      this.onError?.(error)
    })
  }

  /**
   * Main read loop
   */
  private async readLoop(): Promise<void> {
    while (this.running && this.reader && !this.reader.isClosed()) {
      try {
        const line = await this.reader.readLine()

        if (line === null) {
          break
        }

        try {
          const message = JSON.parse(line) as JsonRpcRequest

          // Validate basic JSON-RPC structure
          if (message.jsonrpc !== '2.0' || !message.method) {
            this.sendError(message.id ?? null, -32600, 'Invalid Request')
            continue
          }

          // Dispatch to handler
          this.onMessage?.(message)
        } catch (parseError) {
          this.sendError(null, -32700, 'Parse error')
        }
      } catch (error) {
        if (this.running) {
          this.onError?.(error instanceof Error ? error : new Error(String(error)))
        }
      }
    }

    this.running = false
    this.onClose?.()
  }

  /**
   * Send a message to stdout
   */
  async send(message: JsonRpcResponse | JsonRpcNotification): Promise<void> {
    if (!this.running) {
      throw new Error('Transport is not running')
    }

    const line = JSON.stringify(message) + '\n'

    return new Promise((resolve, reject) => {
      this.stdout.write(line, (error) => {
        if (error) {
          reject(error)
        } else {
          resolve()
        }
      })
    })
  }

  /**
   * Send error response
   */
  private sendError(id: string | number | null, code: number, message: string): void {
    const response: JsonRpcResponse = {
      jsonrpc: '2.0',
      id,
      error: { code, message },
    }

    this.send(response).catch((error) => {
      this.onError?.(error)
    })
  }

  /**
   * Close the transport
   */
  async close(): Promise<void> {
    this.running = false

    // Give time for pending writes to complete
    await new Promise((resolve) => setTimeout(resolve, 100))

    this.onClose?.()
  }

  /**
   * Check if transport is running
   */
  isRunning(): boolean {
    return this.running
  }
}

// ─────────────────────────────────────────────────────────────────
// Factory Function
// ─────────────────────────────────────────────────────────────────

/**
 * Create a stdio transport
 */
export function createStdioTransport(
  options?: McpStdioTransportOptions
): StdioTransport {
  return new StdioTransport(options)
}

// ─────────────────────────────────────────────────────────────────
// Stdio Server Runner
// ─────────────────────────────────────────────────────────────────

export interface StdioServerOptions extends McpStdioTransportOptions {
  /** Shutdown timeout in ms */
  shutdownTimeout?: number
}

/**
 * Run MCP server on stdio with graceful shutdown handling
 */
export async function runStdioServer(
  server: {
    connect(transport: McpTransport): Promise<void>
    close(): Promise<void>
    getServerInfo(): { name: string; version: string }
  },
  options: StdioServerOptions = {}
): Promise<void> {
  const { shutdownTimeout = 5000 } = options

  const transport = createStdioTransport(options)

  let shuttingDown = false

  // Graceful shutdown handler
  async function shutdown(signal: string): Promise<void> {
    if (shuttingDown) {
      return
    }

    shuttingDown = true
    const serverInfo = server.getServerInfo()
    console.error(`[${serverInfo.name}] Received ${signal}, shutting down...`)

    // Create shutdown timeout
    const timeoutPromise = new Promise<void>((resolve) => {
      setTimeout(() => {
        console.error(`[${serverInfo.name}] Shutdown timeout, forcing exit`)
        resolve()
      }, shutdownTimeout)
    })

    // Attempt graceful shutdown
    const shutdownPromise = (async () => {
      await transport.close()
      await server.close()
    })()

    await Promise.race([shutdownPromise, timeoutPromise])
    process.exit(0)
  }

  // Set up signal handlers
  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))

  // Handle uncaught errors
  process.on('uncaughtException', (error) => {
    console.error('[Stdio] Uncaught exception:', error)
    shutdown('uncaughtException').catch(() => process.exit(1))
  })

  process.on('unhandledRejection', (reason) => {
    console.error('[Stdio] Unhandled rejection:', reason)
    shutdown('unhandledRejection').catch(() => process.exit(1))
  })

  // Connect and start
  await server.connect(transport)

  const serverInfo = server.getServerInfo()
  console.error(`[${serverInfo.name}] Server started on stdio`)

  // Keep process alive
  await new Promise<void>((resolve) => {
    transport.onClose = resolve
  })
}

// ─────────────────────────────────────────────────────────────────
// Message Formatting Utilities
// ─────────────────────────────────────────────────────────────────

/**
 * Format a log message for stderr (to avoid mixing with JSON-RPC output)
 */
export function formatLogMessage(
  level: 'debug' | 'info' | 'warn' | 'error',
  message: string,
  data?: unknown
): string {
  const timestamp = new Date().toISOString()
  const prefix = `[${timestamp}] [${level.toUpperCase()}]`

  if (data !== undefined) {
    return `${prefix} ${message} ${JSON.stringify(data)}`
  }

  return `${prefix} ${message}`
}

/**
 * Create a logger that writes to stderr
 */
export function createStdioLogger(prefix?: string) {
  const format = (level: string, message: string) => {
    const timestamp = new Date().toISOString()
    const prefixStr = prefix ? `[${prefix}]` : ''
    return `[${timestamp}] ${prefixStr} [${level}] ${message}\n`
  }

  return {
    debug: (message: string) => process.stderr.write(format('DEBUG', message)),
    info: (message: string) => process.stderr.write(format('INFO', message)),
    warn: (message: string) => process.stderr.write(format('WARN', message)),
    error: (message: string) => process.stderr.write(format('ERROR', message)),
  }
}
