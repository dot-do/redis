/**
 * Redis Client - ioredis-compatible client for Redis.do
 *
 * Provides a Redis client that communicates with Redis.do over HTTP/JSON-RPC or WebSocket.
 * Supports both promise and callback styles, with full command coverage.
 *
 * Features:
 * - Full ioredis-compatible API
 * - HTTP (fetch-based) and WebSocket transports
 * - Auto-pipelining for automatic request batching
 * - Request deduplication
 * - Retry logic with exponential backoff
 * - Key prefix support
 * - Latency monitoring
 */

import type { RedisOptions, SetOptions, ScanOptions, ZRangeOptions, RpcRequest, RpcResponse } from '../types'
import { Pipeline, PipelineCommand, PipelineExecResult } from './pipeline'

type Callback<T> = (err: Error | null, result: T) => void

type EventHandler = (...args: unknown[]) => void

interface DeduplicationEntry {
  promise: Promise<unknown>
  timestamp: number
}

interface WebSocketMessage {
  type: 'request' | 'response' | 'error' | 'ping' | 'pong' | 'auth'
  payload: RpcRequest | RpcResponse | { message: string } | { token: string }
  id?: string | number
}

interface PendingWebSocketRequest {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
  timeout: ReturnType<typeof setTimeout>
}

/**
 * Redis client class with ioredis-compatible API
 *
 * @example
 * ```ts
 * // Basic usage
 * const redis = new Redis({
 *   url: 'https://your-redis.do.workers.dev',
 *   token: 'your-auth-token'
 * })
 *
 * await redis.set('key', 'value')
 * const value = await redis.get('key')
 *
 * // With auto-pipelining (batches concurrent requests)
 * const redis = new Redis({
 *   url: 'https://your-redis.do.workers.dev',
 *   token: 'your-auth-token',
 *   enableAutoPipelining: true
 * })
 *
 * // These will be batched into a single request
 * const [a, b, c] = await Promise.all([
 *   redis.get('a'),
 *   redis.get('b'),
 *   redis.get('c')
 * ])
 *
 * // With WebSocket transport
 * const redis = new Redis({
 *   url: 'https://your-redis.do.workers.dev',
 *   token: 'your-auth-token',
 *   useWebSocket: true
 * })
 *
 * // With key prefix
 * const redis = new Redis({
 *   url: 'https://your-redis.do.workers.dev',
 *   token: 'your-auth-token',
 *   keyPrefix: 'myapp:'
 * })
 * await redis.set('user:1', 'data') // Actually sets 'myapp:user:1'
 * ```
 */
export class Redis {
  // Configuration
  private readonly url: string
  private readonly token?: string
  private readonly enableAutoPipelining: boolean
  private readonly autoPipelineWindowMs: number
  private readonly maxAutoPipelineBatchSize: number
  private readonly useWebSocket: boolean
  private readonly wsReconnectIntervalMs: number
  private readonly timeoutMs: number
  private readonly enableDeduplication: boolean
  private readonly deduplicationTtlMs: number
  private readonly fetchFn: typeof globalThis.fetch
  private readonly retryOnError: boolean
  private readonly maxRetries: number
  private readonly retryDelayMs: number
  private readonly keyPrefix: string
  private readonly readOnly: boolean
  private _name: string
  private readonly enableLatencyMonitoring: boolean

  // Auto-pipelining state
  private autoPipelineQueue: PipelineCommand[] = []
  private autoPipelinePromises: Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }> = new Map()
  private autoPipelineTimer: ReturnType<typeof setTimeout> | null = null
  private commandIndex = 0

  // Deduplication state
  private deduplicationCache = new Map<string, DeduplicationEntry>()
  private deduplicationCleanupTimer: ReturnType<typeof setInterval> | null = null

  // WebSocket state
  private ws: WebSocket | null = null
  private wsConnected = false
  private wsReconnectTimer: ReturnType<typeof setTimeout> | null = null
  private wsPendingRequests = new Map<string | number, PendingWebSocketRequest>()
  private wsRequestId = 0

  // Event emitter
  private eventHandlers = new Map<string, Set<EventHandler>>()

  // Latency monitoring
  private latencyStats: { min: number; max: number; avg: number; count: number } = {
    min: Infinity,
    max: 0,
    avg: 0,
    count: 0
  }

  // Status
  private _status: 'connecting' | 'connect' | 'ready' | 'close' | 'end' = 'close'

  constructor(urlOrOptions?: RedisOptions | string, options?: Partial<RedisOptions>) {
    // Support ioredis-style constructor: new Redis(), new Redis(url), new Redis(url, options), new Redis(options)
    let opts: RedisOptions

    if (typeof urlOrOptions === 'string') {
      // Parse connection string: redis://token@host/path or https://host/path
      const url = new URL(urlOrOptions)
      const token = url.username || url.password || ''
      url.username = ''
      url.password = ''
      opts = {
        url: url.toString().replace(/\/$/, ''),
        token,
        ...options
      }
    } else if (urlOrOptions) {
      opts = urlOrOptions
    } else {
      opts = { url: 'http://localhost:8787' }
    }

    this.url = opts.url.replace(/\/$/, '')
    this.token = opts.token ?? ''
    this.enableAutoPipelining = opts.enableAutoPipelining ?? false
    this.autoPipelineWindowMs = opts.autoPipelineWindowMs ?? 0
    this.maxAutoPipelineBatchSize = opts.maxAutoPipelineBatchSize ?? 100
    this.useWebSocket = opts.useWebSocket ?? false
    this.wsReconnectIntervalMs = opts.wsReconnectIntervalMs ?? 1000
    this.timeoutMs = opts.timeoutMs ?? 30000
    this.enableDeduplication = opts.enableDeduplication ?? false
    this.deduplicationTtlMs = opts.deduplicationTtlMs ?? 100
    this.fetchFn = opts.fetch ?? globalThis.fetch.bind(globalThis)
    this.retryOnError = opts.retryOnError ?? true
    this.maxRetries = opts.maxRetries ?? 3
    this.retryDelayMs = opts.retryDelayMs ?? 100
    this.keyPrefix = opts.keyPrefix ?? ''
    this.readOnly = opts.readOnly ?? false
    this._name = opts.name ?? ''
    this.enableLatencyMonitoring = opts.enableLatencyMonitoring ?? false

    // Start deduplication cleanup if enabled
    if (this.enableDeduplication) {
      this.startDeduplicationCleanup()
    }

    // Connect WebSocket if enabled
    if (this.useWebSocket) {
      this.connectWebSocket()
    } else {
      // For HTTP, we're immediately ready
      this._status = 'ready'
      this.emit('ready')
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Connection & Configuration
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get the current connection status
   */
  get status(): string {
    return this._status
  }

  /**
   * Get the connection name
   */
  get name(): string {
    return this._name
  }

  /**
   * Returns a duplicate of the client with the same options
   */
  duplicate(overrideOptions?: Partial<RedisOptions>): Redis {
    const opts: RedisOptions = {
      url: this.url,
      enableAutoPipelining: this.enableAutoPipelining,
      autoPipelineWindowMs: this.autoPipelineWindowMs,
      maxAutoPipelineBatchSize: this.maxAutoPipelineBatchSize,
      useWebSocket: this.useWebSocket,
      wsReconnectIntervalMs: this.wsReconnectIntervalMs,
      timeoutMs: this.timeoutMs,
      enableDeduplication: this.enableDeduplication,
      deduplicationTtlMs: this.deduplicationTtlMs,
      retryOnError: this.retryOnError,
      maxRetries: this.maxRetries,
      retryDelayMs: this.retryDelayMs,
      keyPrefix: this.keyPrefix,
      readOnly: this.readOnly,
      enableLatencyMonitoring: this.enableLatencyMonitoring,
      ...overrideOptions
    }
    if (this.token) opts.token = this.token
    if (this._name) opts.name = this._name
    return new Redis(opts)
  }

  /**
   * Gracefully disconnect from the server
   */
  async quit(): Promise<'OK'> {
    await this.cleanup()
    this._status = 'end'
    this.emit('end')
    return 'OK'
  }

  /**
   * Forcefully disconnect from the server
   */
  async disconnect(): Promise<void> {
    await this.cleanup()
    this._status = 'close'
    this.emit('close')
  }

  /**
   * Connect to the server (for WebSocket mode)
   */
  async connect(): Promise<void> {
    if (this.useWebSocket && !this.wsConnected) {
      await this.connectWebSocket()
    }
  }

  private async cleanup(): Promise<void> {
    // Clear timers
    if (this.autoPipelineTimer) {
      clearTimeout(this.autoPipelineTimer)
      this.autoPipelineTimer = null
    }
    if (this.deduplicationCleanupTimer) {
      clearInterval(this.deduplicationCleanupTimer)
      this.deduplicationCleanupTimer = null
    }
    if (this.wsReconnectTimer) {
      clearTimeout(this.wsReconnectTimer)
      this.wsReconnectTimer = null
    }

    // Reject pending auto-pipeline requests
    for (const [, pending] of this.autoPipelinePromises) {
      pending.reject(new Error('Client disconnected'))
    }
    this.autoPipelinePromises.clear()
    this.autoPipelineQueue = []

    // Close WebSocket
    if (this.ws) {
      this.ws.close()
      this.ws = null
      this.wsConnected = false
    }

    // Reject pending WebSocket requests
    for (const [, pending] of this.wsPendingRequests) {
      clearTimeout(pending.timeout)
      pending.reject(new Error('Client disconnected'))
    }
    this.wsPendingRequests.clear()

    // Clear deduplication cache
    this.deduplicationCache.clear()
  }

  // ─────────────────────────────────────────────────────────────────
  // Event Emitter
  // ─────────────────────────────────────────────────────────────────

  /**
   * Register an event handler
   */
  on(event: string, handler: EventHandler): this {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set())
    }
    this.eventHandlers.get(event)!.add(handler)
    return this
  }

  /**
   * Register a one-time event handler
   */
  once(event: string, handler: EventHandler): this {
    const wrapper: EventHandler = (...args) => {
      this.off(event, wrapper)
      handler(...args)
    }
    return this.on(event, wrapper)
  }

  /**
   * Remove an event handler
   */
  off(event: string, handler: EventHandler): this {
    const handlers = this.eventHandlers.get(event)
    if (handlers) {
      handlers.delete(handler)
    }
    return this
  }

  /**
   * Alias for on() - ioredis compatibility
   */
  addListener(event: string, handler: EventHandler): this {
    return this.on(event, handler)
  }

  /**
   * Alias for off() - ioredis compatibility
   */
  removeListener(event: string, handler: EventHandler): this {
    return this.off(event, handler)
  }

  /**
   * Get the number of listeners for an event - ioredis compatibility
   */
  listenerCount(event: string): number {
    return this.eventHandlers.get(event)?.size ?? 0
  }

  /**
   * Remove all listeners for an event or all events - ioredis compatibility
   */
  removeAllListeners(event?: string): this {
    if (event) {
      this.eventHandlers.delete(event)
    } else {
      this.eventHandlers.clear()
    }
    return this
  }

  private emit(event: string, ...args: unknown[]): void {
    const handlers = this.eventHandlers.get(event)
    if (handlers) {
      for (const handler of handlers) {
        try {
          handler(...args)
        } catch {
          // Ignore handler errors
        }
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Latency Monitoring
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get latency statistics
   */
  getLatencyStats(): { min: number; max: number; avg: number; count: number } {
    return { ...this.latencyStats }
  }

  /**
   * Reset latency statistics
   */
  resetLatencyStats(): void {
    this.latencyStats = { min: Infinity, max: 0, avg: 0, count: 0 }
  }

  private recordLatency(latency: number): void {
    if (!this.enableLatencyMonitoring) return

    this.latencyStats.min = Math.min(this.latencyStats.min, latency)
    this.latencyStats.max = Math.max(this.latencyStats.max, latency)
    this.latencyStats.count++
    this.latencyStats.avg =
      (this.latencyStats.avg * (this.latencyStats.count - 1) + latency) / this.latencyStats.count
  }

  // ─────────────────────────────────────────────────────────────────
  // Pipeline
  // ─────────────────────────────────────────────────────────────────

  /**
   * Create a new pipeline for batching commands
   */
  pipeline(): Pipeline {
    return new Pipeline((commands) => this.executeBatch(commands))
  }

  /**
   * Creates a pipeline for batching commands (alias for pipeline()).
   *
   * **Important Limitation:** Redis.do does NOT provide true MULTI/EXEC transaction
   * atomicity. Unlike Redis, where MULTI/EXEC guarantees that all commands in a
   * transaction are executed atomically without interleaving from other clients,
   * Redis.do executes batched commands sequentially but without isolation guarantees.
   *
   * This means:
   * - Commands are sent together for efficiency (reduced round trips)
   * - Commands execute in order within the batch
   * - Other clients' commands may interleave between your batch commands
   * - There is no rollback on failure - partial execution is possible
   * - WATCH/UNWATCH for optimistic locking is not supported
   *
   * For use cases requiring true atomicity, consider using Lua scripts via EVAL
   * or restructuring your data access patterns.
   *
   * @returns A Pipeline instance for chaining commands
   * @example
   * ```typescript
   * const multi = redis.multi()
   * multi.set('key1', 'value1')
   * multi.incr('counter')
   * multi.get('key1')
   * const results = await multi.exec()
   * ```
   */
  multi(): Pipeline {
    return this.pipeline()
  }

  // ─────────────────────────────────────────────────────────────────
  // WebSocket Transport
  // ─────────────────────────────────────────────────────────────────

  private connectWebSocket(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.ws) {
        if (this.wsConnected) {
          resolve()
        }
        return
      }

      this._status = 'connecting'
      this.emit('connecting')

      const wsUrl = this.url.replace(/^http/, 'ws') + '/rpc/ws'

      try {
        this.ws = new WebSocket(wsUrl)
      } catch (error) {
        this.emit('error', error as Error)
        reject(error)
        return
      }

      this.ws.addEventListener('open', () => {
        this.wsConnected = true
        this._status = 'connect'
        this.emit('connect')

        // Send authentication if token is present
        if (this.token) {
          this.ws?.send(JSON.stringify({
            type: 'auth',
            payload: { token: this.token }
          }))
        }

        this._status = 'ready'
        this.emit('ready')
        resolve()
      })

      this.ws.addEventListener('message', (event: MessageEvent) => {
        this.handleWebSocketMessage(event.data as string)
      })

      this.ws.addEventListener('close', () => {
        this.wsConnected = false
        this._status = 'close'
        this.emit('close')

        // Reject pending requests
        for (const [, pending] of this.wsPendingRequests) {
          clearTimeout(pending.timeout)
          pending.reject(new Error('WebSocket disconnected'))
        }
        this.wsPendingRequests.clear()

        this.ws = null
        this.scheduleWebSocketReconnect()
      })

      this.ws.addEventListener('error', () => {
        const error = new Error('WebSocket error')
        this.emit('error', error)
        // Note: close event will follow, which handles reconnection
      })
    })
  }

  private scheduleWebSocketReconnect(): void {
    if (this.wsReconnectTimer) return
    if (this._status === 'end') return // Don't reconnect if intentionally closed

    this.emit('reconnecting', this.wsReconnectIntervalMs)

    this.wsReconnectTimer = setTimeout(() => {
      this.wsReconnectTimer = null
      if (this.useWebSocket && this._status !== 'end') {
        this.connectWebSocket().catch(() => {
          // Reconnection failed, will try again
        })
      }
    }, this.wsReconnectIntervalMs)
  }

  private handleWebSocketMessage(data: string): void {
    try {
      const message = JSON.parse(data) as WebSocketMessage

      if (message.type === 'response' && message.id !== undefined) {
        const pending = this.wsPendingRequests.get(message.id)
        if (pending) {
          this.wsPendingRequests.delete(message.id)
          clearTimeout(pending.timeout)

          const response = message.payload as RpcResponse
          if (response.error) {
            const error = new RedisError(response.error.message, response.error.code)
            pending.reject(error)
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

  private executeWebSocketRequest<T>(method: string, params: Record<string, unknown>): Promise<T> {
    return new Promise((resolve, reject) => {
      if (!this.ws || !this.wsConnected) {
        reject(new Error('WebSocket not connected'))
        return
      }

      const id = ++this.wsRequestId
      const startTime = this.enableLatencyMonitoring ? Date.now() : 0

      const timeout = setTimeout(() => {
        this.wsPendingRequests.delete(id)
        reject(new Error('WebSocket request timeout'))
      }, this.timeoutMs)

      this.wsPendingRequests.set(id, {
        resolve: (value) => {
          if (this.enableLatencyMonitoring) {
            this.recordLatency(Date.now() - startTime)
          }
          resolve(value as T)
        },
        reject,
        timeout
      })

      const request: RpcRequest = {
        jsonrpc: '2.0',
        id,
        method,
        params
      }

      this.ws.send(JSON.stringify({
        type: 'request',
        id,
        payload: request
      }))
    })
  }

  // ─────────────────────────────────────────────────────────────────
  // Deduplication
  // ─────────────────────────────────────────────────────────────────

  private getDeduplicationKey(method: string, params: Record<string, unknown>): string {
    return `${method}:${JSON.stringify(params)}`
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

  // ─────────────────────────────────────────────────────────────────
  // Internal RPC Methods
  // ─────────────────────────────────────────────────────────────────

  private async rpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
    // Check deduplication cache for read operations
    if (this.enableDeduplication && this.isReadOnlyCommand(method)) {
      const cacheKey = this.getDeduplicationKey(method, params)
      const cached = this.deduplicationCache.get(cacheKey)
      if (cached && Date.now() - cached.timestamp < this.deduplicationTtlMs) {
        return cached.promise as Promise<T>
      }
    }

    // Check read-only mode
    if (this.readOnly && !this.isReadOnlyCommand(method)) {
      throw new RedisError(`Command ${method} not allowed in read-only mode`, -32600)
    }

    const promise = this.executeRpc<T>(method, params)

    // Cache for deduplication
    if (this.enableDeduplication && this.isReadOnlyCommand(method)) {
      const cacheKey = this.getDeduplicationKey(method, params)
      this.deduplicationCache.set(cacheKey, {
        promise: promise as Promise<unknown>,
        timestamp: Date.now()
      })
    }

    return promise
  }

  private isReadOnlyCommand(method: string): boolean {
    const readOnlyCommands = new Set([
      'get', 'mget', 'strlen', 'getrange', 'exists', 'type', 'ttl', 'pttl',
      'keys', 'scan', 'randomkey', 'dbsize', 'info', 'time',
      'hget', 'hmget', 'hgetall', 'hkeys', 'hvals', 'hlen', 'hexists', 'hscan', 'hstrlen', 'hrandfield',
      'lrange', 'lindex', 'llen', 'lpos',
      'smembers', 'sismember', 'smismember', 'scard', 'srandmember', 'sdiff', 'sinter', 'sunion', 'sscan',
      'zscore', 'zmscore', 'zrank', 'zrevrank', 'zcard', 'zcount', 'zlexcount', 'zrange', 'zrevrange',
      'zrangebyscore', 'zrevrangebyscore', 'zrangebylex', 'zrevrangebylex', 'zscan', 'zrandmember',
      'ping', 'echo', 'dump', 'object', 'expiretime', 'pexpiretime'
    ])
    return readOnlyCommands.has(method)
  }

  private async executeRpc<T>(method: string, params: Record<string, unknown>): Promise<T> {
    // Use WebSocket if connected
    if (this.useWebSocket && this.wsConnected) {
      return this.executeWebSocketRequest<T>(method, params)
    }

    // Fall back to HTTP
    return this.executeHttpRequest<T>(method, params)
  }

  private async executeHttpRequest<T>(method: string, params: Record<string, unknown>, retryCount = 0): Promise<T> {
    const request: RpcRequest = {
      jsonrpc: '2.0',
      id: Date.now().toString() + Math.random().toString(36).slice(2),
      method,
      params
    }

    const headers: Record<string, string> = {
      'Content-Type': 'application/json'
    }

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs)
    const startTime = this.enableLatencyMonitoring ? Date.now() : 0

    try {
      const response = await this.fetchFn(`${this.url}/rpc`, {
        method: 'POST',
        headers,
        body: JSON.stringify(request),
        signal: controller.signal
      })

      if (this.enableLatencyMonitoring) {
        this.recordLatency(Date.now() - startTime)
      }

      if (!response.ok) {
        const error = new RedisError(`HTTP error: ${response.status} ${response.statusText}`, -32000)

        // Retry on server errors
        if (this.retryOnError && retryCount < this.maxRetries && response.status >= 500) {
          await this.delay(this.retryDelayMs * Math.pow(2, retryCount))
          return this.executeHttpRequest<T>(method, params, retryCount + 1)
        }

        throw error
      }

      const result = await response.json() as RpcResponse

      if (result.error) {
        throw new RedisError(result.error.message, result.error.code)
      }

      return result.result as T
    } catch (error) {
      if ((error as Error).name === 'AbortError') {
        throw new RedisError('Request timeout', -32000)
      }

      // Retry on network errors
      if (this.retryOnError && retryCount < this.maxRetries && this.isNetworkError(error)) {
        await this.delay(this.retryDelayMs * Math.pow(2, retryCount))
        return this.executeHttpRequest<T>(method, params, retryCount + 1)
      }

      throw error
    } finally {
      clearTimeout(timeoutId)
    }
  }

  private isNetworkError(error: unknown): boolean {
    if (error instanceof Error) {
      return error.message.includes('fetch') ||
             error.message.includes('network') ||
             error.message.includes('ECONNREFUSED') ||
             error.message.includes('ETIMEDOUT')
    }
    return false
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  private async executeBatch(commands: PipelineCommand[]): Promise<PipelineExecResult> {
    const requests: RpcRequest[] = commands.map((cmd, index) => ({
      jsonrpc: '2.0' as const,
      id: index,
      method: cmd.method,
      params: this.argsToParams(cmd.method, cmd.args)
    }))

    const headers: Record<string, string> = {
      'Content-Type': 'application/json'
    }

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }

    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs)
    const startTime = this.enableLatencyMonitoring ? Date.now() : 0

    try {
      const response = await this.fetchFn(`${this.url}/rpc`, {
        method: 'POST',
        headers,
        body: JSON.stringify(requests),
        signal: controller.signal
      })

      if (this.enableLatencyMonitoring) {
        this.recordLatency(Date.now() - startTime)
      }

      if (!response.ok) {
        const error = new Error(`HTTP error: ${response.status} ${response.statusText}`)
        return commands.map(() => [error, null])
      }

      const results = await response.json() as RpcResponse[]

      // Sort results by id to maintain order
      const sortedResults = results.sort((a, b) => Number(a.id) - Number(b.id))

      return sortedResults.map((result) => {
        if (result.error) {
          return [new RedisError(result.error.message, result.error.code), null]
        }
        return [null, result.result]
      })
    } catch (error) {
      return commands.map(() => [error as Error, null])
    } finally {
      clearTimeout(timeoutId)
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Key Prefix Handling
  // ─────────────────────────────────────────────────────────────────

  private prefixKey(key: string): string {
    return this.keyPrefix ? this.keyPrefix + key : key
  }

  private prefixKeys(keys: string[]): string[] {
    return this.keyPrefix ? keys.map(k => this.keyPrefix + k) : keys
  }

  // ─────────────────────────────────────────────────────────────────
  // Argument Conversion
  // ─────────────────────────────────────────────────────────────────

  private argsToParams(method: string, args: unknown[]): Record<string, unknown> {
    // Apply key prefix where applicable
    switch (method) {
      case 'get':
      case 'strlen':
      case 'type':
      case 'ttl':
      case 'pttl':
      case 'persist':
      case 'dump':
      case 'getdel':
      case 'hgetall':
      case 'hkeys':
      case 'hvals':
      case 'hlen':
      case 'llen':
      case 'smembers':
      case 'scard':
      case 'zcard':
      case 'randomkey':
      case 'expiretime':
      case 'pexpiretime':
        return { key: this.prefixKey(args[0] as string) }

      case 'set':
        return this.parseSetArgs(args)

      case 'setnx':
      case 'getset':
      case 'append':
        return { key: this.prefixKey(args[0] as string), value: args[1] }

      case 'setex':
        return { key: this.prefixKey(args[0] as string), seconds: args[1], value: args[2] }

      case 'psetex':
        return { key: this.prefixKey(args[0] as string), milliseconds: args[1], value: args[2] }

      case 'getex':
        return { key: this.prefixKey(args[0] as string), options: args[1] }

      case 'setrange':
        return { key: this.prefixKey(args[0] as string), offset: args[1], value: args[2] }

      case 'getrange':
        return { key: this.prefixKey(args[0] as string), start: args[1], end: args[2] }

      case 'incr':
      case 'decr':
        return { key: this.prefixKey(args[0] as string) }

      case 'incrby':
      case 'decrby':
        return { key: this.prefixKey(args[0] as string), increment: args[1] }

      case 'incrbyfloat':
        return { key: this.prefixKey(args[0] as string), increment: args[1] }

      case 'mget':
        return { keys: this.prefixKeys(args as string[]) }

      case 'mset':
      case 'msetnx':
        return { keyValues: this.prefixMsetArgs(args as (string | number)[]) }

      case 'hget':
        return { key: this.prefixKey(args[0] as string), field: args[1] }

      case 'hset':
      case 'hmset':
        if (args.length === 3 && typeof args[1] === 'string') {
          return { key: this.prefixKey(args[0] as string), fieldValues: { [args[1] as string]: args[2] } }
        }
        return { key: this.prefixKey(args[0] as string), fieldValues: this.arrayToObject(args.slice(1) as (string | number)[]) }

      case 'hsetnx':
        return { key: this.prefixKey(args[0] as string), field: args[1], value: args[2] }

      case 'hmget':
        return { key: this.prefixKey(args[0] as string), fields: args.slice(1) }

      case 'hdel':
        return { key: this.prefixKey(args[0] as string), fields: args.slice(1) }

      case 'hexists':
        return { key: this.prefixKey(args[0] as string), field: args[1] }

      case 'hincrby':
      case 'hincrbyfloat':
        return { key: this.prefixKey(args[0] as string), field: args[1], increment: args[2] }

      case 'hstrlen':
        return { key: this.prefixKey(args[0] as string), field: args[1] }

      case 'hscan':
        return { key: this.prefixKey(args[0] as string), cursor: args[1], options: args[2] }

      case 'hrandfield':
        return { key: this.prefixKey(args[0] as string), count: args[1], withValues: args[2] }

      case 'lpush':
      case 'lpushx':
      case 'rpush':
      case 'rpushx':
        return { key: this.prefixKey(args[0] as string), values: args.slice(1) }

      case 'lpop':
      case 'rpop':
        return { key: this.prefixKey(args[0] as string), count: args[1] }

      case 'lrange':
        return { key: this.prefixKey(args[0] as string), start: args[1], stop: args[2] }

      case 'lindex':
        return { key: this.prefixKey(args[0] as string), index: args[1] }

      case 'lset':
        return { key: this.prefixKey(args[0] as string), index: args[1], value: args[2] }

      case 'lrem':
        return { key: this.prefixKey(args[0] as string), count: args[1], value: args[2] }

      case 'ltrim':
        return { key: this.prefixKey(args[0] as string), start: args[1], stop: args[2] }

      case 'linsert':
        return { key: this.prefixKey(args[0] as string), position: args[1], pivot: args[2], value: args[3] }

      case 'lpos':
        return { key: this.prefixKey(args[0] as string), element: args[1], options: args[2] }

      case 'lmove':
        return { source: this.prefixKey(args[0] as string), destination: this.prefixKey(args[1] as string), from: args[2], to: args[3] }

      case 'rpoplpush':
        return { source: this.prefixKey(args[0] as string), destination: this.prefixKey(args[1] as string) }

      case 'sadd':
      case 'srem':
        return { key: this.prefixKey(args[0] as string), members: args.slice(1) }

      case 'sismember':
        return { key: this.prefixKey(args[0] as string), member: args[1] }

      case 'smismember':
        return { key: this.prefixKey(args[0] as string), members: args.slice(1) }

      case 'spop':
      case 'srandmember':
        return { key: this.prefixKey(args[0] as string), count: args[1] }

      case 'smove':
        return { source: this.prefixKey(args[0] as string), destination: this.prefixKey(args[1] as string), member: args[2] }

      case 'sdiff':
      case 'sinter':
      case 'sunion':
        return { keys: this.prefixKeys(args as string[]) }

      case 'sdiffstore':
      case 'sinterstore':
      case 'sunionstore':
        return { destination: this.prefixKey(args[0] as string), keys: this.prefixKeys(args.slice(1) as string[]) }

      case 'sintercard':
        return { numkeys: args[0], keys: this.prefixKeys(args.slice(1) as string[]) }

      case 'sscan':
        return { key: this.prefixKey(args[0] as string), cursor: args[1], options: args[2] }

      case 'zadd':
        return this.parseZAddArgs(args)

      case 'zrem':
        return { key: this.prefixKey(args[0] as string), members: args.slice(1) }

      case 'zscore':
        return { key: this.prefixKey(args[0] as string), member: args[1] }

      case 'zmscore':
        return { key: this.prefixKey(args[0] as string), members: args.slice(1) }

      case 'zrank':
      case 'zrevrank':
        return { key: this.prefixKey(args[0] as string), member: args[1] }

      case 'zcount':
      case 'zlexcount':
        return { key: this.prefixKey(args[0] as string), min: args[1], max: args[2] }

      case 'zincrby':
        return { key: this.prefixKey(args[0] as string), increment: args[1], member: args[2] }

      case 'zrange':
        return this.parseZRangeArgs(args)

      case 'zrevrange':
        return { key: this.prefixKey(args[0] as string), start: args[1], stop: args[2], withScores: args[3] === 'WITHSCORES' }

      case 'zrangebyscore':
      case 'zrevrangebyscore':
        return this.parseZRangeByScoreArgs(args)

      case 'zrangebylex':
      case 'zrevrangebylex':
        return this.parseZRangeByLexArgs(args)

      case 'zrangestore':
        return { destination: this.prefixKey(args[0] as string), source: this.prefixKey(args[1] as string), start: args[2], stop: args[3], options: args[4] }

      case 'zpopmin':
      case 'zpopmax':
        return { key: this.prefixKey(args[0] as string), count: args[1] }

      case 'zrandmember':
        return { key: this.prefixKey(args[0] as string), count: args[1], withScores: args[2] }

      case 'zremrangebyrank':
        return { key: this.prefixKey(args[0] as string), start: args[1], stop: args[2] }

      case 'zremrangebyscore':
      case 'zremrangebylex':
        return { key: this.prefixKey(args[0] as string), min: args[1], max: args[2] }

      case 'zunion':
      case 'zinter':
      case 'zdiff':
        return this.parseZSetOpArgs(args)

      case 'zunionstore':
      case 'zinterstore':
      case 'zdiffstore':
        return { destination: this.prefixKey(args[0] as string), ...this.parseZSetOpArgs(args.slice(1)) }

      case 'zscan':
        return { key: this.prefixKey(args[0] as string), cursor: args[1], options: args[2] }

      case 'del':
      case 'unlink':
      case 'exists':
      case 'touch':
        return { keys: this.prefixKeys(args as string[]) }

      case 'expire':
      case 'pexpire':
        return { key: this.prefixKey(args[0] as string), seconds: args[1], mode: args[2] }

      case 'expireat':
      case 'pexpireat':
        return { key: this.prefixKey(args[0] as string), timestamp: args[1], mode: args[2] }

      case 'rename':
      case 'renamenx':
        return { key: this.prefixKey(args[0] as string), newkey: this.prefixKey(args[1] as string) }

      case 'copy':
        return { source: this.prefixKey(args[0] as string), destination: this.prefixKey(args[1] as string), options: args[2] }

      case 'keys':
        return { pattern: this.keyPrefix ? this.keyPrefix + (args[0] as string) : args[0] }

      case 'scan':
        return { cursor: args[0], options: this.keyPrefix ? { ...(args[1] as object || {}), match: this.keyPrefix + ((args[1] as ScanOptions)?.match || '*') } : args[1] }

      case 'object':
        return { subcommand: args[0], args: args.slice(1) }

      case 'ping':
        return args[0] !== undefined ? { message: args[0] } : {}

      case 'echo':
        return { message: args[0] }

      case 'flushdb':
      case 'flushall':
        return args[0] ? { mode: args[0] } : {}

      case 'info':
        return args[0] !== undefined ? { section: args[0] } : {}

      default:
        // For unknown commands, pass args as-is
        return { args }
    }
  }

  private prefixMsetArgs(args: (string | number)[]): Record<string, string | number> {
    const result: Record<string, string | number> = {}
    for (let i = 0; i < args.length; i += 2) {
      result[this.prefixKey(String(args[i]))] = args[i + 1]
    }
    return result
  }

  private parseSetArgs(args: unknown[]): Record<string, unknown> {
    const key = this.prefixKey(args[0] as string)
    const value = args[1] as string | number
    const result: Record<string, unknown> = { key, value }

    let i = 2
    while (i < args.length) {
      const arg = args[i]

      // Object-style options
      if (typeof arg === 'object' && arg !== null) {
        const opts = arg as SetOptions
        if (opts.ex !== undefined) result.ex = opts.ex
        if (opts.px !== undefined) result.px = opts.px
        if (opts.exat !== undefined) result.exat = opts.exat
        if (opts.pxat !== undefined) result.pxat = opts.pxat
        if (opts.nx) result.nx = true
        if (opts.xx) result.xx = true
        if (opts.keepttl) result.keepttl = true
        if (opts.get) result.get = true
        i++
        continue
      }

      // String-style options (ioredis format)
      const argStr = String(arg).toUpperCase()
      switch (argStr) {
        case 'EX':
          result.ex = args[++i]
          break
        case 'PX':
          result.px = args[++i]
          break
        case 'EXAT':
          result.exat = args[++i]
          break
        case 'PXAT':
          result.pxat = args[++i]
          break
        case 'NX':
          result.nx = true
          break
        case 'XX':
          result.xx = true
          break
        case 'KEEPTTL':
          result.keepttl = true
          break
        case 'GET':
          result.get = true
          break
      }
      i++
    }

    return result
  }

  private parseZAddArgs(args: unknown[]): Record<string, unknown> {
    const key = this.prefixKey(args[0] as string)
    const result: Record<string, unknown> = { key }
    const scoreMembers: Array<{ score: number; member: string }> = []

    let i = 1

    // Parse options
    while (i < args.length) {
      const arg = String(args[i]).toUpperCase()
      if (arg === 'NX') {
        result.nx = true
        i++
      } else if (arg === 'XX') {
        result.xx = true
        i++
      } else if (arg === 'GT') {
        result.gt = true
        i++
      } else if (arg === 'LT') {
        result.lt = true
        i++
      } else if (arg === 'CH') {
        result.ch = true
        i++
      } else {
        break
      }
    }

    // Parse score-member pairs
    while (i < args.length - 1) {
      const score = Number(args[i])
      const member = String(args[i + 1])
      scoreMembers.push({ score, member })
      i += 2
    }

    result.scoreMembers = scoreMembers
    return result
  }

  private parseZRangeArgs(args: unknown[]): Record<string, unknown> {
    const result: Record<string, unknown> = {
      key: this.prefixKey(args[0] as string),
      start: args[1],
      stop: args[2]
    }

    const options = args[3]
    if (typeof options === 'object' && options !== null) {
      const opts = options as ZRangeOptions
      if (opts.withScores) result.withScores = true
      if (opts.rev) result.rev = true
      if (opts.byScore) result.byScore = true
      if (opts.byLex) result.byLex = true
      if (opts.limit) result.limit = opts.limit
    } else if (options === 'WITHSCORES') {
      result.withScores = true
    }

    return result
  }

  private parseZRangeByScoreArgs(args: unknown[]): Record<string, unknown> {
    const result: Record<string, unknown> = {
      key: this.prefixKey(args[0] as string),
      min: args[1],
      max: args[2]
    }

    let i = 3
    while (i < args.length) {
      const arg = String(args[i]).toUpperCase()
      if (arg === 'WITHSCORES') {
        result.withScores = true
        i++
      } else if (arg === 'LIMIT') {
        result.limit = { offset: Number(args[i + 1]), count: Number(args[i + 2]) }
        i += 3
      } else {
        i++
      }
    }

    return result
  }

  private parseZRangeByLexArgs(args: unknown[]): Record<string, unknown> {
    const result: Record<string, unknown> = {
      key: this.prefixKey(args[0] as string),
      min: args[1],
      max: args[2]
    }

    let i = 3
    while (i < args.length) {
      const arg = String(args[i]).toUpperCase()
      if (arg === 'LIMIT') {
        result.limit = { offset: Number(args[i + 1]), count: Number(args[i + 2]) }
        i += 3
      } else {
        i++
      }
    }

    return result
  }

  private parseZSetOpArgs(args: unknown[]): Record<string, unknown> {
    const numkeys = Number(args[0])
    const keys = this.prefixKeys(args.slice(1, 1 + numkeys) as string[])
    const result: Record<string, unknown> = { numkeys, keys }

    let i = 1 + numkeys
    while (i < args.length) {
      const arg = String(args[i]).toUpperCase()
      if (arg === 'WEIGHTS') {
        const weights: number[] = []
        for (let j = 0; j < numkeys; j++) {
          weights.push(Number(args[i + 1 + j]))
        }
        result.weights = weights
        i += 1 + numkeys
      } else if (arg === 'AGGREGATE') {
        result.aggregate = String(args[i + 1]).toUpperCase()
        i += 2
      } else if (arg === 'WITHSCORES') {
        result.withScores = true
        i++
      } else {
        i++
      }
    }

    return result
  }

  private arrayToObject(arr: (string | number)[]): Record<string, string | number> {
    const result: Record<string, string | number> = {}
    for (let i = 0; i < arr.length; i += 2) {
      result[String(arr[i])] = arr[i + 1]
    }
    return result
  }

  // ─────────────────────────────────────────────────────────────────
  // Auto-pipelining support
  // ─────────────────────────────────────────────────────────────────

  private async executeWithAutoPipeline<T>(method: string, args: unknown[]): Promise<T> {
    if (!this.enableAutoPipelining) {
      return this.rpc<T>(method, this.argsToParams(method, args))
    }

    const index = this.commandIndex++
    this.autoPipelineQueue.push({ method, args })

    return new Promise<T>((resolve, reject) => {
      this.autoPipelinePromises.set(index, {
        resolve: resolve as (value: unknown) => void,
        reject
      })

      if (!this.autoPipelineTimer) {
        if (this.autoPipelineWindowMs > 0) {
          this.autoPipelineTimer = setTimeout(() => this.flushAutoPipeline(), this.autoPipelineWindowMs)
        } else {
          // Use microtask for minimal latency
          this.autoPipelineTimer = setTimeout(() => this.flushAutoPipeline(), 0)
        }
      }

      // Flush immediately if batch is full
      if (this.autoPipelineQueue.length >= this.maxAutoPipelineBatchSize) {
        this.flushAutoPipeline()
      }
    })
  }

  private async flushAutoPipeline(): Promise<void> {
    if (this.autoPipelineTimer) {
      clearTimeout(this.autoPipelineTimer)
      this.autoPipelineTimer = null
    }

    const commands = this.autoPipelineQueue
    const startIndex = this.commandIndex - commands.length
    const promises = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>()

    // Copy promises for this batch
    for (let i = 0; i < commands.length; i++) {
      const promiseIndex = startIndex + i
      const promise = this.autoPipelinePromises.get(promiseIndex)
      if (promise) {
        promises.set(i, promise)
        this.autoPipelinePromises.delete(promiseIndex)
      }
    }

    this.autoPipelineQueue = []

    if (commands.length === 0) return

    try {
      const results = await this.executeBatch(commands)

      for (let i = 0; i < results.length; i++) {
        const [error, result] = results[i]
        const promise = promises.get(i)
        if (promise) {
          if (error) {
            promise.reject(error)
          } else {
            promise.resolve(result)
          }
        }
      }
    } catch (error) {
      for (const [, promise] of promises) {
        promise.reject(error as Error)
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // String Commands
  // ─────────────────────────────────────────────────────────────────

  async get(key: string, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('get', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async set(key: string, value: string | number, ...args: unknown[]): Promise<string | null> {
    // Handle callback as last argument
    let callback: Callback<string | null> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as Callback<string | null>
    }

    try {
      const result = await this.executeWithAutoPipeline<string | null>('set', [key, value, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async mget(...args: string[]): Promise<(string | null)[]> {
    // Handle callback as last argument
    let callback: Callback<(string | null)[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<(string | null)[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<(string | null)[]>('mget', args)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async mset(...args: (string | number)[]): Promise<'OK'> {
    let callback: Callback<'OK'> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<'OK'>
    }

    try {
      const result = await this.executeWithAutoPipeline<'OK'>('mset', args)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async msetnx(...args: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('msetnx', args)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async setnx(key: string, value: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('setnx', [key, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async setex(key: string, seconds: number, value: string | number, callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('setex', [key, seconds, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async psetex(key: string, milliseconds: number, value: string | number, callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('psetex', [key, milliseconds, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async getset(key: string, value: string | number, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('getset', [key, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async getdel(key: string, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('getdel', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async getex(key: string, options?: SetOptions, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('getex', [key, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async append(key: string, value: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('append', [key, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async strlen(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('strlen', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async setrange(key: string, offset: number, value: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('setrange', [key, offset, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async getrange(key: string, start: number, end: number, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('getrange', [key, start, end])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '')
      throw error
    }
  }

  async incr(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('incr', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async incrby(key: string, increment: number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('incrby', [key, increment])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async incrbyfloat(key: string, increment: number, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('incrbyfloat', [key, increment])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '0')
      throw error
    }
  }

  async decr(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('decr', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async decrby(key: string, decrement: number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('decrby', [key, decrement])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Hash Commands
  // ─────────────────────────────────────────────────────────────────

  async hget(key: string, field: string, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('hget', [key, field])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async hset(key: string, ...args: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('hset', [key, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hsetnx(key: string, field: string, value: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('hsetnx', [key, field, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
    let callback: Callback<(string | null)[]> | undefined
    if (typeof fields[fields.length - 1] === 'function') {
      callback = fields.pop() as unknown as Callback<(string | null)[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<(string | null)[]>('hmget', [key, ...fields])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async hmset(key: string, ...args: (string | number)[]): Promise<'OK'> {
    let callback: Callback<'OK'> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<'OK'>
    }

    try {
      const result = await this.executeWithAutoPipeline<'OK'>('hmset', [key, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async hgetall(key: string, callback?: Callback<Record<string, string>>): Promise<Record<string, string>> {
    try {
      const result = await this.executeWithAutoPipeline<Record<string, string>>('hgetall', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, {})
      throw error
    }
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof fields[fields.length - 1] === 'function') {
      callback = fields.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('hdel', [key, ...fields])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hexists(key: string, field: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('hexists', [key, field])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hlen(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('hlen', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hkeys(key: string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('hkeys', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async hvals(key: string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('hvals', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async hincrby(key: string, field: string, increment: number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('hincrby', [key, field, increment])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hincrbyfloat(key: string, field: string, increment: number, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('hincrbyfloat', [key, field, increment])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '0')
      throw error
    }
  }

  async hstrlen(key: string, field: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('hstrlen', [key, field])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async hscan(key: string, cursor: string | number, options?: ScanOptions, callback?: Callback<[string, string[]]>): Promise<[string, string[]]> {
    try {
      const result = await this.executeWithAutoPipeline<[string, string[]]>('hscan', [key, cursor, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, ['0', []])
      throw error
    }
  }

  async hrandfield(key: string, count?: number, withValues?: boolean, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('hrandfield', [key, count, withValues])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // List Commands
  // ─────────────────────────────────────────────────────────────────

  async lpush(key: string, ...values: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof values[values.length - 1] === 'function') {
      callback = values.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('lpush', [key, ...values])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async lpushx(key: string, ...values: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof values[values.length - 1] === 'function') {
      callback = values.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('lpushx', [key, ...values])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async rpush(key: string, ...values: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof values[values.length - 1] === 'function') {
      callback = values.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('rpush', [key, ...values])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async rpushx(key: string, ...values: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof values[values.length - 1] === 'function') {
      callback = values.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('rpushx', [key, ...values])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async lpop(key: string, count?: number, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('lpop', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async rpop(key: string, count?: number, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('rpop', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async lrange(key: string, start: number, stop: number, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('lrange', [key, start, stop])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async lindex(key: string, index: number, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('lindex', [key, index])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async lset(key: string, index: number, value: string | number, callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('lset', [key, index, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async llen(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('llen', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async lrem(key: string, count: number, value: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('lrem', [key, count, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async ltrim(key: string, start: number, stop: number, callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('ltrim', [key, start, stop])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async linsert(key: string, position: 'BEFORE' | 'AFTER', pivot: string | number, value: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('linsert', [key, position, pivot, value])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async lpos(key: string, element: string | number, options?: { rank?: number; count?: number; maxlen?: number }, callback?: Callback<number | number[] | null>): Promise<number | number[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<number | number[] | null>('lpos', [key, element, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async lmove(source: string, destination: string, from: 'LEFT' | 'RIGHT', to: 'LEFT' | 'RIGHT', callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('lmove', [source, destination, from, to])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async rpoplpush(source: string, destination: string, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('rpoplpush', [source, destination])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Set Commands
  // ─────────────────────────────────────────────────────────────────

  async sadd(key: string, ...members: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof members[members.length - 1] === 'function') {
      callback = members.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('sadd', [key, ...members])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async srem(key: string, ...members: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof members[members.length - 1] === 'function') {
      callback = members.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('srem', [key, ...members])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async smembers(key: string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('smembers', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async sismember(key: string, member: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('sismember', [key, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async smismember(key: string, ...members: (string | number)[]): Promise<number[]> {
    let callback: Callback<number[]> | undefined
    if (typeof members[members.length - 1] === 'function') {
      callback = members.pop() as unknown as Callback<number[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<number[]>('smismember', [key, ...members])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async scard(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('scard', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async spop(key: string, count?: number, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('spop', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async srandmember(key: string, count?: number, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('srandmember', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async smove(source: string, destination: string, member: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('smove', [source, destination, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async sdiff(...keys: string[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('sdiff', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async sdiffstore(destination: string, ...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('sdiffstore', [destination, ...keys])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async sinter(...keys: string[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('sinter', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async sinterstore(destination: string, ...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('sinterstore', [destination, ...keys])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async sintercard(numkeys: number, ...args: unknown[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('sintercard', [numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async sunion(...keys: string[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('sunion', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async sunionstore(destination: string, ...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('sunionstore', [destination, ...keys])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async sscan(key: string, cursor: string | number, options?: ScanOptions, callback?: Callback<[string, string[]]>): Promise<[string, string[]]> {
    try {
      const result = await this.executeWithAutoPipeline<[string, string[]]>('sscan', [key, cursor, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, ['0', []])
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Sorted Set Commands
  // ─────────────────────────────────────────────────────────────────

  async zadd(key: string, ...args: unknown[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('zadd', [key, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zrem(key: string, ...members: (string | number)[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof members[members.length - 1] === 'function') {
      callback = members.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('zrem', [key, ...members])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zscore(key: string, member: string | number, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('zscore', [key, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async zmscore(key: string, ...members: (string | number)[]): Promise<(string | null)[]> {
    let callback: Callback<(string | null)[]> | undefined
    if (typeof members[members.length - 1] === 'function') {
      callback = members.pop() as unknown as Callback<(string | null)[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<(string | null)[]>('zmscore', [key, ...members])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrank(key: string, member: string | number, callback?: Callback<number | null>): Promise<number | null> {
    try {
      const result = await this.executeWithAutoPipeline<number | null>('zrank', [key, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async zrevrank(key: string, member: string | number, callback?: Callback<number | null>): Promise<number | null> {
    try {
      const result = await this.executeWithAutoPipeline<number | null>('zrevrank', [key, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async zcard(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zcard', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zcount(key: string, min: string | number, max: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zcount', [key, min, max])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zlexcount(key: string, min: string, max: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zlexcount', [key, min, max])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zincrby(key: string, increment: number, member: string | number, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('zincrby', [key, increment, member])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '0')
      throw error
    }
  }

  async zrange(key: string, start: number | string, stop: number | string, options?: ZRangeOptions | string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrange', [key, start, stop, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrevrange(key: string, start: number, stop: number, withScores?: 'WITHSCORES', callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrevrange', withScores ? [key, start, stop, withScores] : [key, start, stop])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrangebyscore(key: string, min: string | number, max: string | number, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrangebyscore', [key, min, max, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrevrangebyscore(key: string, max: string | number, min: string | number, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrevrangebyscore', [key, max, min, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrangebylex(key: string, min: string, max: string, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrangebylex', [key, min, max, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrevrangebylex(key: string, max: string, min: string, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zrevrangebylex', [key, max, min, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrangestore(destination: string, source: string, start: number | string, stop: number | string, options?: ZRangeOptions, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zrangestore', [destination, source, start, stop, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zpopmin(key: string, count?: number, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('zpopmin', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zpopmax(key: string, count?: number, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('zpopmax', count !== undefined ? [key, count] : [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zrandmember(key: string, count?: number, withScores?: boolean, callback?: Callback<string | string[] | null>): Promise<string | string[] | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | string[] | null>('zrandmember', [key, count, withScores])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async zremrangebyrank(key: string, start: number, stop: number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zremrangebyrank', [key, start, stop])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zremrangebyscore(key: string, min: string | number, max: string | number, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zremrangebyscore', [key, min, max])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zremrangebylex(key: string, min: string, max: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('zremrangebylex', [key, min, max])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zunion(numkeys: number, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zunion', [numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zunionstore(destination: string, numkeys: number, ...args: unknown[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('zunionstore', [destination, numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zinter(numkeys: number, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zinter', [numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zinterstore(destination: string, numkeys: number, ...args: unknown[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('zinterstore', [destination, numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zdiff(numkeys: number, ...args: unknown[]): Promise<string[]> {
    let callback: Callback<string[]> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<string[]>
    }

    try {
      const result = await this.executeWithAutoPipeline<string[]>('zdiff', [numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async zdiffstore(destination: string, numkeys: number, ...args: unknown[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('zdiffstore', [destination, numkeys, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async zscan(key: string, cursor: string | number, options?: ScanOptions, callback?: Callback<[string, string[]]>): Promise<[string, string[]]> {
    try {
      const result = await this.executeWithAutoPipeline<[string, string[]]>('zscan', [key, cursor, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, ['0', []])
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Key Commands
  // ─────────────────────────────────────────────────────────────────

  async del(...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('del', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async unlink(...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('unlink', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async exists(...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('exists', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async expire(key: string, seconds: number, mode?: 'NX' | 'XX' | 'GT' | 'LT', callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('expire', mode ? [key, seconds, mode] : [key, seconds])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async expireat(key: string, timestamp: number, mode?: 'NX' | 'XX' | 'GT' | 'LT', callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('expireat', mode ? [key, timestamp, mode] : [key, timestamp])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async expiretime(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('expiretime', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, -2)
      throw error
    }
  }

  async pexpire(key: string, milliseconds: number, mode?: 'NX' | 'XX' | 'GT' | 'LT', callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('pexpire', mode ? [key, milliseconds, mode] : [key, milliseconds])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async pexpireat(key: string, timestamp: number, mode?: 'NX' | 'XX' | 'GT' | 'LT', callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('pexpireat', mode ? [key, timestamp, mode] : [key, timestamp])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async pexpiretime(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('pexpiretime', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, -2)
      throw error
    }
  }

  async ttl(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('ttl', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, -2)
      throw error
    }
  }

  async pttl(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('pttl', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, -2)
      throw error
    }
  }

  async persist(key: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('persist', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async type(key: string, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('type', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'none')
      throw error
    }
  }

  async rename(key: string, newkey: string, callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('rename', [key, newkey])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async renamenx(key: string, newkey: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('renamenx', [key, newkey])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async copy(source: string, destination: string, options?: { replace?: boolean; db?: number }, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('copy', [source, destination, options])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async keys(pattern: string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.executeWithAutoPipeline<string[]>('keys', [pattern])
      // Remove key prefix from results if present
      const unprefixedResult = this.keyPrefix
        ? result.map(k => k.startsWith(this.keyPrefix) ? k.slice(this.keyPrefix.length) : k)
        : result
      callback?.(null, unprefixedResult)
      return unprefixedResult
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  async scan(cursor: string | number, options?: ScanOptions, callback?: Callback<[string, string[]]>): Promise<[string, string[]]> {
    try {
      const result = await this.executeWithAutoPipeline<[string, string[]]>('scan', [cursor, options])
      // Remove key prefix from results if present
      if (this.keyPrefix && result[1]) {
        result[1] = result[1].map(k => k.startsWith(this.keyPrefix) ? k.slice(this.keyPrefix.length) : k)
      }
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, ['0', []])
      throw error
    }
  }

  async randomkey(callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('randomkey', [])
      // Remove key prefix from result if present
      const unprefixedResult = result && this.keyPrefix && result.startsWith(this.keyPrefix)
        ? result.slice(this.keyPrefix.length)
        : result
      callback?.(null, unprefixedResult)
      return unprefixedResult
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async touch(...keys: string[]): Promise<number> {
    let callback: Callback<number> | undefined
    if (typeof keys[keys.length - 1] === 'function') {
      callback = keys.pop() as unknown as Callback<number>
    }

    try {
      const result = await this.executeWithAutoPipeline<number>('touch', keys)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async object(subcommand: string, ...args: unknown[]): Promise<unknown> {
    let callback: Callback<unknown> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as Callback<unknown>
    }

    try {
      const result = await this.executeWithAutoPipeline<unknown>('object', [subcommand, ...args])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  async dump(key: string, callback?: Callback<string | null>): Promise<string | null> {
    try {
      const result = await this.executeWithAutoPipeline<string | null>('dump', [key])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Server Commands
  // ─────────────────────────────────────────────────────────────────

  async ping(message?: string, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('ping', message !== undefined ? [message] : [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'PONG')
      throw error
    }
  }

  async echo(message: string, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('echo', [message])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '')
      throw error
    }
  }

  async dbsize(callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.executeWithAutoPipeline<number>('dbsize', [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  async flushdb(mode?: 'ASYNC' | 'SYNC', callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('flushdb', mode ? [mode] : [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async flushall(mode?: 'ASYNC' | 'SYNC', callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.executeWithAutoPipeline<'OK'>('flushall', mode ? [mode] : [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  async info(section?: string, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.executeWithAutoPipeline<string>('info', section !== undefined ? [section] : [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '')
      throw error
    }
  }

  async time(callback?: Callback<[string, string]>): Promise<[string, string]> {
    try {
      const result = await this.executeWithAutoPipeline<[string, string]>('time', [])
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, ['0', '0'])
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Generic Command
  // ─────────────────────────────────────────────────────────────────

  /**
   * Execute any Redis command
   */
  async call(command: string, ...args: unknown[]): Promise<unknown> {
    let callback: Callback<unknown> | undefined
    if (typeof args[args.length - 1] === 'function') {
      callback = args.pop() as Callback<unknown>
    }

    try {
      const result = await this.executeWithAutoPipeline<unknown>(command.toLowerCase(), args)
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * Send a raw command (alias for call)
   */
  async sendCommand(command: string, ...args: unknown[]): Promise<unknown> {
    return this.call(command, ...args)
  }

  // ─────────────────────────────────────────────────────────────────
  // Buffer Methods (ioredis compatibility)
  // ─────────────────────────────────────────────────────────────────

  /**
   * GET command returning Buffer (ioredis compatibility)
   */
  async getBuffer(key: string, callback?: Callback<Buffer | null>): Promise<Buffer | null> {
    try {
      const result = await this.get(key)
      const buffer = result !== null ? Buffer.from(result) : null
      callback?.(null, buffer)
      return buffer
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * SET command with Buffer value (ioredis compatibility)
   */
  async setBuffer(key: string, value: Buffer, ...args: unknown[]): Promise<string | null> {
    return this.set(key, value.toString('base64'), ...args)
  }

  /**
   * HGET command returning Buffer (ioredis compatibility)
   */
  async hgetBuffer(key: string, field: string, callback?: Callback<Buffer | null>): Promise<Buffer | null> {
    try {
      const result = await this.hget(key, field)
      const buffer = result !== null ? Buffer.from(result) : null
      callback?.(null, buffer)
      return buffer
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * HGETALL command returning Record with Buffer values (ioredis compatibility)
   */
  async hgetallBuffer(key: string, callback?: Callback<Record<string, Buffer>>): Promise<Record<string, Buffer>> {
    try {
      const result = await this.hgetall(key)
      const bufferResult: Record<string, Buffer> = {}
      for (const [k, v] of Object.entries(result)) {
        bufferResult[k] = Buffer.from(v)
      }
      callback?.(null, bufferResult)
      return bufferResult
    } catch (error) {
      callback?.(error as Error, {})
      throw error
    }
  }

  /**
   * LRANGE command returning Buffer[] (ioredis compatibility)
   */
  async lrangeBuffer(key: string, start: number, stop: number, callback?: Callback<Buffer[]>): Promise<Buffer[]> {
    try {
      const result = await this.lrange(key, start, stop)
      const bufferResult = result.map(v => Buffer.from(v))
      callback?.(null, bufferResult)
      return bufferResult
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  /**
   * LPOP command returning Buffer (ioredis compatibility)
   */
  async lpopBuffer(key: string, count?: number, callback?: Callback<Buffer | Buffer[] | null>): Promise<Buffer | Buffer[] | null> {
    try {
      const result = await this.lpop(key, count)
      if (result === null) {
        callback?.(null, null)
        return null
      }
      if (Array.isArray(result)) {
        const bufferResult = result.map(v => Buffer.from(v))
        callback?.(null, bufferResult)
        return bufferResult
      }
      const buffer = Buffer.from(result)
      callback?.(null, buffer)
      return buffer
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * RPOP command returning Buffer (ioredis compatibility)
   */
  async rpopBuffer(key: string, count?: number, callback?: Callback<Buffer | Buffer[] | null>): Promise<Buffer | Buffer[] | null> {
    try {
      const result = await this.rpop(key, count)
      if (result === null) {
        callback?.(null, null)
        return null
      }
      if (Array.isArray(result)) {
        const bufferResult = result.map(v => Buffer.from(v))
        callback?.(null, bufferResult)
        return bufferResult
      }
      const buffer = Buffer.from(result)
      callback?.(null, buffer)
      return buffer
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * SMEMBERS command returning Buffer[] (ioredis compatibility)
   */
  async smembersBuffer(key: string, callback?: Callback<Buffer[]>): Promise<Buffer[]> {
    try {
      const result = await this.smembers(key)
      const bufferResult = result.map(v => Buffer.from(v))
      callback?.(null, bufferResult)
      return bufferResult
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  /**
   * ZRANGE command returning Buffer[] (ioredis compatibility)
   */
  async zrangeBuffer(key: string, start: number | string, stop: number | string, options?: ZRangeOptions | string, callback?: Callback<Buffer[]>): Promise<Buffer[]> {
    try {
      const result = await this.zrange(key, start, stop, options)
      const bufferResult = result.map(v => Buffer.from(v))
      callback?.(null, bufferResult)
      return bufferResult
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  /**
   * MGET command returning Buffer[] (ioredis compatibility)
   */
  async mgetBuffer(...keys: string[]): Promise<(Buffer | null)[]> {
    const result = await this.mget(...keys)
    return result.map(v => v !== null ? Buffer.from(v) : null)
  }

  /**
   * GETRANGE command returning Buffer (ioredis compatibility)
   */
  async getrangeBuffer(key: string, start: number, end: number, callback?: Callback<Buffer>): Promise<Buffer> {
    try {
      const result = await this.getrange(key, start, end)
      const buffer = Buffer.from(result)
      callback?.(null, buffer)
      return buffer
    } catch (error) {
      callback?.(error as Error, Buffer.alloc(0))
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Cluster/Sentinel Stubs (ioredis compatibility)
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get cluster mode status (ioredis compatibility)
   * Always returns false as Redis.do doesn't use cluster mode
   */
  get isCluster(): boolean {
    return false
  }

  /**
   * Get the options used to create this client
   */
  get options(): RedisOptions {
    return {
      url: this.url,
      token: this.token,
      enableAutoPipelining: this.enableAutoPipelining,
      autoPipelineWindowMs: this.autoPipelineWindowMs,
      maxAutoPipelineBatchSize: this.maxAutoPipelineBatchSize,
      useWebSocket: this.useWebSocket,
      wsReconnectIntervalMs: this.wsReconnectIntervalMs,
      timeoutMs: this.timeoutMs,
      enableDeduplication: this.enableDeduplication,
      deduplicationTtlMs: this.deduplicationTtlMs,
      retryOnError: this.retryOnError,
      maxRetries: this.maxRetries,
      retryDelayMs: this.retryDelayMs,
      keyPrefix: this.keyPrefix,
      readOnly: this.readOnly,
      name: this._name,
      enableLatencyMonitoring: this.enableLatencyMonitoring
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Scripting Commands (ioredis compatibility)
  // ─────────────────────────────────────────────────────────────────

  /**
   * EVAL command for Lua scripting (limited support)
   */
  async eval(script: string, numkeys: number, ...keysAndArgs: (string | number | Callback<unknown>)[]): Promise<unknown> {
    let callback: Callback<unknown> | undefined
    if (typeof keysAndArgs[keysAndArgs.length - 1] === 'function') {
      callback = keysAndArgs.pop() as unknown as Callback<unknown>
    }

    try {
      const keys = keysAndArgs.slice(0, numkeys) as string[]
      const args = keysAndArgs.slice(numkeys)
      const result = await this.rpc<unknown>('eval', {
        script,
        keys: this.prefixKeys(keys),
        args
      })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * EVALSHA command for Lua scripting (limited support)
   */
  async evalsha(sha1: string, numkeys: number, ...keysAndArgs: (string | number | Callback<unknown>)[]): Promise<unknown> {
    let callback: Callback<unknown> | undefined
    if (typeof keysAndArgs[keysAndArgs.length - 1] === 'function') {
      callback = keysAndArgs.pop() as unknown as Callback<unknown>
    }

    try {
      const keys = keysAndArgs.slice(0, numkeys) as string[]
      const args = keysAndArgs.slice(numkeys)
      const result = await this.rpc<unknown>('evalsha', {
        sha1,
        keys: this.prefixKeys(keys),
        args
      })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * SCRIPT LOAD command
   */
  async scriptLoad(script: string, callback?: Callback<string>): Promise<string> {
    try {
      const result = await this.rpc<string>('script', { subcommand: 'load', script })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, '')
      throw error
    }
  }

  /**
   * SCRIPT EXISTS command
   */
  async scriptExists(...sha1s: string[]): Promise<number[]> {
    let callback: Callback<number[]> | undefined
    if (typeof sha1s[sha1s.length - 1] === 'function') {
      callback = sha1s.pop() as unknown as Callback<number[]>
    }

    try {
      const result = await this.rpc<number[]>('script', { subcommand: 'exists', sha1s })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  /**
   * SCRIPT FLUSH command
   */
  async scriptFlush(mode?: 'ASYNC' | 'SYNC', callback?: Callback<'OK'>): Promise<'OK'> {
    try {
      const result = await this.rpc<'OK'>('script', { subcommand: 'flush', mode })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 'OK')
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Pub/Sub Commands (ioredis compatibility)
  // ─────────────────────────────────────────────────────────────────

  /**
   * PUBLISH command
   */
  async publish(channel: string, message: string, callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.rpc<number>('publish', { channel, message })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  /**
   * PUBSUB CHANNELS command
   */
  async pubsubChannels(pattern?: string, callback?: Callback<string[]>): Promise<string[]> {
    try {
      const result = await this.rpc<string[]>('pubsub', { subcommand: 'channels', pattern })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, [])
      throw error
    }
  }

  /**
   * PUBSUB NUMSUB command
   */
  async pubsubNumsub(...channels: string[]): Promise<Record<string, number>> {
    let callback: Callback<Record<string, number>> | undefined
    if (typeof channels[channels.length - 1] === 'function') {
      callback = channels.pop() as unknown as Callback<Record<string, number>>
    }

    try {
      const result = await this.rpc<Record<string, number>>('pubsub', { subcommand: 'numsub', channels })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, {})
      throw error
    }
  }

  /**
   * PUBSUB NUMPAT command
   */
  async pubsubNumpat(callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.rpc<number>('pubsub', { subcommand: 'numpat' })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Additional ioredis-compatible methods
  // ─────────────────────────────────────────────────────────────────

  /**
   * SELECT database (no-op for Redis.do, but provided for compatibility)
   */
  async select(_db: number, callback?: Callback<'OK'>): Promise<'OK'> {
    // Redis.do uses a single namespace per deployment, so SELECT is a no-op
    callback?.(null, 'OK')
    return 'OK'
  }

  /**
   * CLIENT ID command
   */
  async clientId(callback?: Callback<number>): Promise<number> {
    try {
      const result = await this.rpc<number>('client', { subcommand: 'id' })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, 0)
      throw error
    }
  }

  /**
   * CLIENT GETNAME command
   */
  async clientGetname(callback?: Callback<string | null>): Promise<string | null> {
    callback?.(null, this._name || null)
    return this._name || null
  }

  /**
   * CLIENT SETNAME command (updates local name only)
   */
  async clientSetname(connectionName: string, callback?: Callback<'OK'>): Promise<'OK'> {
    // For HTTP-based client, we just track name locally
    this._name = connectionName
    callback?.(null, 'OK')
    return 'OK'
  }

  /**
   * DEBUG SLEEP command (for testing)
   */
  async debugSleep(seconds: number, callback?: Callback<'OK'>): Promise<'OK'> {
    await this.delay(seconds * 1000)
    callback?.(null, 'OK')
    return 'OK'
  }

  /**
   * MEMORY USAGE command
   */
  async memoryUsage(key: string, options?: { samples?: number }, callback?: Callback<number | null>): Promise<number | null> {
    try {
      const result = await this.rpc<number | null>('memory', {
        subcommand: 'usage',
        key: this.prefixKey(key),
        samples: options?.samples
      })
      callback?.(null, result)
      return result
    } catch (error) {
      callback?.(error as Error, null)
      throw error
    }
  }

  /**
   * Wait for client to be ready (ioredis compatibility)
   */
  async waitForReady(): Promise<void> {
    if (this._status === 'ready') return

    return new Promise((resolve, reject) => {
      const onReady = () => {
        clearTimeout(timeout)
        this.off('error', onError as EventHandler)
        resolve()
      }

      const timeout = setTimeout(() => {
        this.off('ready', onReady)
        this.off('error', onError as EventHandler)
        reject(new Error('Connection timeout'))
      }, this.timeoutMs)

      const onError = (err: unknown) => {
        clearTimeout(timeout)
        this.off('ready', onReady)
        reject(err instanceof Error ? err : new Error(String(err)))
      }

      this.once('ready', onReady)
      this.once('error', onError as EventHandler)
    })
  }
}

// ─────────────────────────────────────────────────────────────────
// Error Classes
// ─────────────────────────────────────────────────────────────────

/**
 * Error class for Redis errors with error code
 */
export class RedisError extends Error {
  readonly code: number

  constructor(message: string, code: number = -1) {
    super(message)
    this.name = 'RedisError'
    this.code = code
  }
}
