/**
 * RPC Client - HTTP batch protocol with deduplication and WebSocket support
 *
 * Features:
 * - HTTP batch protocol for efficient multi-command requests
 * - Request deduplication with configurable TTL
 * - WebSocket transport for real-time operations
 * - Promise pipelining for automatic batching
 */

import type { RpcRequest, RpcResponse, RpcError } from '../types'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

export interface RpcClientOptions {
  /** Base URL for RPC endpoint */
  url: string
  /** Authorization token */
  token?: string
  /** Enable automatic request batching */
  enableBatching?: boolean
  /** Batch window in milliseconds (default: 2ms) */
  batchWindowMs?: number
  /** Maximum batch size (default: 100) */
  maxBatchSize?: number
  /** Enable request deduplication */
  enableDeduplication?: boolean
  /** Deduplication TTL in milliseconds (default: 100ms) */
  deduplicationTtlMs?: number
  /** Use WebSocket transport */
  useWebSocket?: boolean
  /** WebSocket reconnect interval in milliseconds */
  wsReconnectIntervalMs?: number
  /** Request timeout in milliseconds (default: 30000) */
  timeoutMs?: number
  /** Custom fetch implementation */
  fetch?: typeof globalThis.fetch
}

interface PendingRequest {
  request: RpcRequest
  resolve: (value: unknown) => void
  reject: (error: Error) => void
}

interface DeduplicationEntry {
  promise: Promise<unknown>
  timestamp: number
}

interface WebSocketMessage {
  type: 'request' | 'response' | 'error' | 'ping' | 'pong'
  payload: RpcRequest | RpcResponse | { message: string }
}

// ─────────────────────────────────────────────────────────────────
// RPC Client Implementation
// ─────────────────────────────────────────────────────────────────

export class RpcClient {
  private readonly url: string
  private readonly token: string | undefined
  private readonly enableBatching: boolean
  private readonly batchWindowMs: number
  private readonly maxBatchSize: number
  private readonly enableDeduplication: boolean
  private readonly deduplicationTtlMs: number
  private readonly useWebSocket: boolean
  private readonly wsReconnectIntervalMs: number
  private readonly timeoutMs: number
  private readonly fetchFn: typeof globalThis.fetch

  private pendingRequests: PendingRequest[] = []
  private batchTimer: ReturnType<typeof setTimeout> | null = null
  private requestId = 0
  private deduplicationCache = new Map<string, DeduplicationEntry>()
  private deduplicationCleanupTimer: ReturnType<typeof setInterval> | null = null

  // WebSocket state
  private ws: WebSocket | null = null
  private wsConnected = false
  private wsReconnectTimer: ReturnType<typeof setTimeout> | null = null
  private wsPendingRequests = new Map<string | number, PendingRequest>()

  constructor(options: RpcClientOptions) {
    this.url = options.url.replace(/\/$/, '')
    this.token = options.token
    this.enableBatching = options.enableBatching ?? true
    this.batchWindowMs = options.batchWindowMs ?? 2
    this.maxBatchSize = options.maxBatchSize ?? 100
    this.enableDeduplication = options.enableDeduplication ?? true
    this.deduplicationTtlMs = options.deduplicationTtlMs ?? 100
    this.useWebSocket = options.useWebSocket ?? false
    this.wsReconnectIntervalMs = options.wsReconnectIntervalMs ?? 1000
    this.timeoutMs = options.timeoutMs ?? 30000
    this.fetchFn = options.fetch ?? globalThis.fetch.bind(globalThis)

    // Start deduplication cleanup if enabled
    if (this.enableDeduplication) {
      this.startDeduplicationCleanup()
    }

    // Connect WebSocket if enabled
    if (this.useWebSocket) {
      this.connectWebSocket()
    }
  }

  // ─────────────────────────────────────────────────────────────
  // Public API
  // ─────────────────────────────────────────────────────────────

  /**
   * Call a single RPC method
   */
  async call<T = unknown>(method: string, params?: Record<string, unknown>): Promise<T> {
    // Check deduplication cache
    if (this.enableDeduplication) {
      const cacheKey = this.getDeduplicationKey(method, params)
      const cached = this.deduplicationCache.get(cacheKey)
      if (cached && Date.now() - cached.timestamp < this.deduplicationTtlMs) {
        return cached.promise as Promise<T>
      }
    }

    const request: RpcRequest = {
      jsonrpc: '2.0',
      id: ++this.requestId,
      method,
      ...(params !== undefined && { params }),
    }

    const promise = this.executeRequest<T>(request)

    // Store in deduplication cache
    if (this.enableDeduplication) {
      const cacheKey = this.getDeduplicationKey(method, params)
      this.deduplicationCache.set(cacheKey, {
        promise: promise as Promise<unknown>,
        timestamp: Date.now(),
      })
    }

    return promise
  }

  /**
   * Execute multiple RPC calls in a batch
   */
  async batch<T extends unknown[] = unknown[]>(
    requests: Array<{ method: string; params?: Record<string, unknown> }>
  ): Promise<T> {
    const rpcRequests: RpcRequest[] = requests.map((req) => ({
      jsonrpc: '2.0' as const,
      id: ++this.requestId,
      method: req.method,
      ...(req.params !== undefined && { params: req.params }),
    }))

    return this.executeBatch<T>(rpcRequests)
  }

  /**
   * Create a pipeline for chaining commands
   */
  pipeline(): RpcPipeline {
    return new RpcPipeline(this)
  }

  /**
   * Close the client and cleanup resources
   */
  close(): void {
    // Clear timers
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }
    if (this.deduplicationCleanupTimer) {
      clearInterval(this.deduplicationCleanupTimer)
      this.deduplicationCleanupTimer = null
    }
    if (this.wsReconnectTimer) {
      clearTimeout(this.wsReconnectTimer)
      this.wsReconnectTimer = null
    }

    // Close WebSocket
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }

    // Reject pending requests
    for (const pending of this.pendingRequests) {
      pending.reject(new Error('Client closed'))
    }
    this.pendingRequests = []

    for (const pending of this.wsPendingRequests.values()) {
      pending.reject(new Error('Client closed'))
    }
    this.wsPendingRequests.clear()

    // Clear deduplication cache
    this.deduplicationCache.clear()
  }

  // ─────────────────────────────────────────────────────────────
  // Internal Methods
  // ─────────────────────────────────────────────────────────────

  private async executeRequest<T>(request: RpcRequest): Promise<T> {
    if (this.useWebSocket && this.wsConnected) {
      return this.executeWebSocketRequest<T>(request)
    }

    if (this.enableBatching) {
      return this.enqueueBatchRequest<T>(request)
    }

    return this.executeHttpRequest<T>(request)
  }

  private async executeHttpRequest<T>(request: RpcRequest): Promise<T> {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs)

    try {
      const response = await this.fetchFn(`${this.url}/rpc`, {
        method: 'POST',
        headers: this.getHeaders(),
        body: JSON.stringify(request),
        signal: controller.signal,
      })

      if (!response.ok) {
        throw new RpcClientError(
          `HTTP ${response.status}: ${response.statusText}`,
          -32000,
          { status: response.status }
        )
      }

      const result = (await response.json()) as RpcResponse

      if (result.error) {
        throw new RpcClientError(
          result.error.message,
          result.error.code,
          result.error.data
        )
      }

      return result.result as T
    } finally {
      clearTimeout(timeoutId)
    }
  }

  private enqueueBatchRequest<T>(request: RpcRequest): Promise<T> {
    return new Promise((resolve, reject) => {
      this.pendingRequests.push({
        request,
        resolve: resolve as (value: unknown) => void,
        reject,
      })

      // Start batch timer if not already running
      if (!this.batchTimer) {
        this.batchTimer = setTimeout(() => {
          this.flushBatch()
        }, this.batchWindowMs)
      }

      // Flush immediately if batch is full
      if (this.pendingRequests.length >= this.maxBatchSize) {
        this.flushBatch()
      }
    })
  }

  private async flushBatch(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }

    const requests = this.pendingRequests
    this.pendingRequests = []

    if (requests.length === 0) return

    const rpcRequests = requests.map((r) => r.request)

    try {
      const results = await this.executeBatchHttp(rpcRequests)

      // Map results back to pending requests
      const resultMap = new Map<string | number, RpcResponse>()
      for (const result of results) {
        resultMap.set(result.id, result)
      }

      for (const pending of requests) {
        const result = resultMap.get(pending.request.id)
        if (!result) {
          pending.reject(new Error('No response for request'))
        } else if (result.error) {
          pending.reject(
            new RpcClientError(result.error.message, result.error.code, result.error.data)
          )
        } else {
          pending.resolve(result.result)
        }
      }
    } catch (error) {
      // Reject all pending requests on network error
      for (const pending of requests) {
        pending.reject(error as Error)
      }
    }
  }

  private async executeBatch<T extends unknown[]>(requests: RpcRequest[]): Promise<T> {
    const results = await this.executeBatchHttp(requests)
    const resultMap = new Map<string | number, RpcResponse>()
    for (const result of results) {
      resultMap.set(result.id, result)
    }

    return requests.map((req) => {
      const result = resultMap.get(req.id)
      if (!result) {
        throw new Error('No response for request')
      }
      if (result.error) {
        throw new RpcClientError(result.error.message, result.error.code, result.error.data)
      }
      return result.result
    }) as T
  }

  private async executeBatchHttp(requests: RpcRequest[]): Promise<RpcResponse[]> {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs)

    try {
      const response = await this.fetchFn(`${this.url}/rpc`, {
        method: 'POST',
        headers: this.getHeaders(),
        body: JSON.stringify(requests),
        signal: controller.signal,
      })

      if (!response.ok) {
        throw new RpcClientError(
          `HTTP ${response.status}: ${response.statusText}`,
          -32000,
          { status: response.status }
        )
      }

      return (await response.json()) as RpcResponse[]
    } finally {
      clearTimeout(timeoutId)
    }
  }

  // ─────────────────────────────────────────────────────────────
  // WebSocket Transport
  // ─────────────────────────────────────────────────────────────

  private connectWebSocket(): void {
    if (this.ws) return

    const wsUrl = this.url.replace(/^http/, 'ws') + '/rpc/ws'
    this.ws = new WebSocket(wsUrl)

    this.ws.addEventListener('open', () => {
      this.wsConnected = true
      // Send authentication if token is present
      if (this.token) {
        this.ws?.send(JSON.stringify({
          type: 'auth',
          payload: { token: this.token },
        }))
      }
    })

    this.ws.addEventListener('message', (event: MessageEvent) => {
      this.handleWebSocketMessage(event.data as string)
    })

    this.ws.addEventListener('close', () => {
      this.wsConnected = false
      this.ws = null

      // Reject pending WebSocket requests
      for (const pending of this.wsPendingRequests.values()) {
        pending.reject(new Error('WebSocket disconnected'))
      }
      this.wsPendingRequests.clear()

      // Attempt reconnection
      this.scheduleWebSocketReconnect()
    })

    this.ws.addEventListener('error', () => {
      // Error handling is done in onclose
    })
  }

  private scheduleWebSocketReconnect(): void {
    if (this.wsReconnectTimer) return

    this.wsReconnectTimer = setTimeout(() => {
      this.wsReconnectTimer = null
      if (this.useWebSocket) {
        this.connectWebSocket()
      }
    }, this.wsReconnectIntervalMs)
  }

  private handleWebSocketMessage(data: string): void {
    try {
      const message = JSON.parse(data) as WebSocketMessage

      if (message.type === 'response') {
        const response = message.payload as RpcResponse
        const pending = this.wsPendingRequests.get(response.id)
        if (pending) {
          this.wsPendingRequests.delete(response.id)
          if (response.error) {
            pending.reject(
              new RpcClientError(response.error.message, response.error.code, response.error.data)
            )
          } else {
            pending.resolve(response.result)
          }
        }
      } else if (message.type === 'ping') {
        this.ws?.send(JSON.stringify({ type: 'pong' }))
      }
    } catch {
      // Ignore malformed messages
    }
  }

  private executeWebSocketRequest<T>(request: RpcRequest): Promise<T> {
    return new Promise((resolve, reject) => {
      if (!this.ws || !this.wsConnected) {
        reject(new Error('WebSocket not connected'))
        return
      }

      this.wsPendingRequests.set(request.id, {
        request,
        resolve: resolve as (value: unknown) => void,
        reject,
      })

      // Set timeout for WebSocket request
      setTimeout(() => {
        const pending = this.wsPendingRequests.get(request.id)
        if (pending) {
          this.wsPendingRequests.delete(request.id)
          pending.reject(new Error('WebSocket request timeout'))
        }
      }, this.timeoutMs)

      this.ws.send(
        JSON.stringify({
          type: 'request',
          payload: request,
        })
      )
    })
  }

  // ─────────────────────────────────────────────────────────────
  // Deduplication
  // ─────────────────────────────────────────────────────────────

  private getDeduplicationKey(method: string, params?: Record<string, unknown>): string {
    return `${method}:${params ? JSON.stringify(params) : ''}`
  }

  private startDeduplicationCleanup(): void {
    this.deduplicationCleanupTimer = setInterval(() => {
      const now = Date.now()
      for (const [key, entry] of this.deduplicationCache) {
        if (now - entry.timestamp > this.deduplicationTtlMs) {
          this.deduplicationCache.delete(key)
        }
      }
    }, this.deduplicationTtlMs * 2)
  }

  // ─────────────────────────────────────────────────────────────
  // Utility Methods
  // ─────────────────────────────────────────────────────────────

  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    }

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }

    return headers
  }
}

// ─────────────────────────────────────────────────────────────────
// Pipeline Implementation
// ─────────────────────────────────────────────────────────────────

/**
 * Pipeline for chaining multiple RPC commands
 */
export class RpcPipeline {
  private readonly client: RpcClient
  private readonly commands: Array<{ method: string; params?: Record<string, unknown> }> = []

  constructor(client: RpcClient) {
    this.client = client
  }

  /**
   * Add a command to the pipeline
   */
  call(method: string, params?: Record<string, unknown>): this {
    if (params !== undefined) {
      this.commands.push({ method, params })
    } else {
      this.commands.push({ method })
    }
    return this
  }

  /**
   * Execute all commands in the pipeline
   */
  async exec<T extends unknown[] = unknown[]>(): Promise<T> {
    if (this.commands.length === 0) {
      return [] as unknown as T
    }
    return this.client.batch<T>(this.commands)
  }

  /**
   * Get the number of commands in the pipeline
   */
  get length(): number {
    return this.commands.length
  }
}

// ─────────────────────────────────────────────────────────────────
// Error Classes
// ─────────────────────────────────────────────────────────────────

/**
 * Error thrown by RPC client
 */
export class RpcClientError extends Error {
  readonly code: number
  readonly data?: unknown

  constructor(message: string, code: number, data?: unknown) {
    super(message)
    this.name = 'RpcClientError'
    this.code = code
    this.data = data
  }

  toJSON(): RpcError {
    return {
      code: this.code,
      message: this.message,
      data: this.data,
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Factory Function
// ─────────────────────────────────────────────────────────────────

/**
 * Create a new RPC client
 */
export function createRpcClient(options: RpcClientOptions): RpcClient {
  return new RpcClient(options)
}
