/**
 * Worker Evaluator
 *
 * Executes user code in a sandboxed Cloudflare Worker environment
 * using the Worker Loader for isolation.
 *
 * Security Features:
 * - Execution time limits with configurable timeout
 * - Dangerous API restrictions (no fetch, eval, etc.)
 * - Console output capture and limiting
 * - Memory usage monitoring
 * - Redis-only external access via controlled proxy
 */

import type {
  WorkerLoader,
  WorkerCode,
  WorkerStub,
  WorkerEntrypoint,
} from '../../types'
import type {
  RedisProxy,
  WorkerEvaluatorOptions,
  WorkerEvaluatorResult,
  SandboxResult,
} from '../types'
import { prepareUserCode, validateUserCode } from './template'

// ─────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────

const DEFAULT_TIMEOUT = 30000 // 30 seconds
const DEFAULT_COMPATIBILITY_DATE = '2024-01-01'
const DEFAULT_COMPATIBILITY_FLAGS = ['nodejs_compat']
const DEFAULT_MEMORY_LIMIT = 128 * 1024 * 1024 // 128MB
const MAX_LOG_ENTRIES = 1000
const MAX_LOG_ENTRY_LENGTH = 10000

// Destructive commands that are blocked by default in sandbox mode
const BLOCKED_SANDBOX_COMMANDS = ['flushdb', 'flushall']

// ─────────────────────────────────────────────────────────────────
// Worker Evaluator Class
// ─────────────────────────────────────────────────────────────────

export class WorkerEvaluator {
  private loader: WorkerLoader
  private redisProxy: RedisProxy
  private options: Required<WorkerEvaluatorOptions>
  private workerId: string
  private workerStub: WorkerStub | null = null
  private executionCount: number = 0

  constructor(
    loader: WorkerLoader,
    redisProxy: RedisProxy,
    options: WorkerEvaluatorOptions = {}
  ) {
    this.loader = loader
    this.redisProxy = redisProxy
    this.options = {
      timeout: options.timeout ?? DEFAULT_TIMEOUT,
      compatibilityDate: options.compatibilityDate ?? DEFAULT_COMPATIBILITY_DATE,
      compatibilityFlags: options.compatibilityFlags ?? DEFAULT_COMPATIBILITY_FLAGS,
      memoryLimit: options.memoryLimit ?? DEFAULT_MEMORY_LIMIT,
      allowedGlobals: options.allowedGlobals ?? [],
    }
    this.workerId = `sandbox-${Date.now()}-${Math.random().toString(36).slice(2, 11)}`
  }

  /**
   * Execute user code in the sandbox
   *
   * @param userCode - User-provided JavaScript code to execute
   * @returns Promise with execution result including success status, result, logs, and timing
   */
  async execute(userCode: string): Promise<WorkerEvaluatorResult> {
    const startTime = Date.now()
    this.executionCount++
    const executionId = `${this.workerId}-${this.executionCount}`

    // Validate and prepare code
    const { code: sandboxedCode, validation } = prepareUserCode(userCode)

    if (!validation.valid) {
      return {
        success: false,
        error: `Code validation failed: ${validation.errors.join('; ')}`,
        logs: [],
        executionTime: Date.now() - startTime,
        workerId: this.workerId,
      }
    }

    // Log warnings if any
    const logs: string[] = []
    if (validation.warnings.length > 0) {
      logs.push(`Warnings: ${validation.warnings.join('; ')}`)
    }

    try {
      // Create worker code bundle
      const workerCode = this.createWorkerCode(sandboxedCode, executionId)

      // Get or create worker stub with unique ID per execution for isolation
      this.workerStub = this.loader.get(executionId, async () => workerCode)

      // Get entrypoint
      const entrypoint = this.workerStub.getEntrypoint()

      // Execute with timeout
      const result = await this.executeWithTimeout(entrypoint)

      // Truncate logs if necessary
      const truncatedLogs = this.truncateLogs([...logs, ...result.logs])

      return {
        ...result,
        logs: truncatedLogs,
        workerId: this.workerId,
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      return {
        success: false,
        error: message,
        logs,
        executionTime: Date.now() - startTime,
        workerId: this.workerId,
      }
    }
  }

  /**
   * Truncate logs to prevent memory exhaustion
   */
  private truncateLogs(logs: string[]): string[] {
    const truncated = logs.slice(0, MAX_LOG_ENTRIES)
    return truncated.map(log =>
      log.length > MAX_LOG_ENTRY_LENGTH
        ? log.slice(0, MAX_LOG_ENTRY_LENGTH) + '... (truncated)'
        : log
    )
  }

  /**
   * Create worker code bundle with security constraints
   *
   * @param sandboxedCode - The prepared/sandboxed user code
   * @param executionId - Unique identifier for this execution
   */
  private createWorkerCode(sandboxedCode: string, executionId: string): WorkerCode {
    // Create Redis proxy module that will be injected
    const redisProxyModule = this.createRedisProxyModule()

    return {
      compatibilityDate: this.options.compatibilityDate,
      compatibilityFlags: this.options.compatibilityFlags,
      mainModule: 'index.js',
      modules: {
        'index.js': sandboxedCode,
        'redis-proxy.js': redisProxyModule,
      },
      env: {
        __redisProxy: this.createProxyEnv(),
        __executionId: executionId,
        __memoryLimit: this.options.memoryLimit,
        __timeout: this.options.timeout,
      },
      // Block all outbound network access - critical for sandbox security
      globalOutbound: null,
    }
  }

  /**
   * Create Redis proxy module code
   */
  private createRedisProxyModule(): string {
    return `
// Redis Proxy Module
// Provides the redis object for sandbox code

export function createRedisProxy(rpc) {
  return {
    // String commands
    async get(key) {
      return rpc.call('get', { key });
    },
    async set(key, value, options = {}) {
      return rpc.call('set', { key, value, ...options });
    },
    async del(...keys) {
      return rpc.call('del', { keys });
    },
    async exists(...keys) {
      return rpc.call('exists', { keys });
    },
    async expire(key, seconds) {
      return rpc.call('expire', { key, seconds });
    },
    async ttl(key) {
      return rpc.call('ttl', { key });
    },
    async keys(pattern) {
      return rpc.call('keys', { pattern });
    },
    async scan(cursor, options = {}) {
      return rpc.call('scan', { cursor, ...options });
    },
    async incr(key) {
      return rpc.call('incr', { key });
    },
    async incrby(key, increment) {
      return rpc.call('incrby', { key, increment });
    },
    async decr(key) {
      return rpc.call('decr', { key });
    },
    async decrby(key, decrement) {
      return rpc.call('decrby', { key, decrement });
    },
    async append(key, value) {
      return rpc.call('append', { key, value });
    },
    async strlen(key) {
      return rpc.call('strlen', { key });
    },
    async type(key) {
      return rpc.call('type', { key });
    },
    async rename(key, newKey) {
      return rpc.call('rename', { key, newKey });
    },

    // Hash commands
    async hget(key, field) {
      return rpc.call('hget', { key, field });
    },
    async hset(key, field, value) {
      return rpc.call('hset', { key, field, value });
    },
    async hgetall(key) {
      return rpc.call('hgetall', { key });
    },
    async hdel(key, ...fields) {
      return rpc.call('hdel', { key, fields });
    },

    // List commands
    async lpush(key, ...values) {
      return rpc.call('lpush', { key, values });
    },
    async rpush(key, ...values) {
      return rpc.call('rpush', { key, values });
    },
    async lpop(key) {
      return rpc.call('lpop', { key });
    },
    async rpop(key) {
      return rpc.call('rpop', { key });
    },
    async lrange(key, start, stop) {
      return rpc.call('lrange', { key, start, stop });
    },

    // Set commands
    async sadd(key, ...members) {
      return rpc.call('sadd', { key, members });
    },
    async smembers(key) {
      return rpc.call('smembers', { key });
    },
    async srem(key, ...members) {
      return rpc.call('srem', { key, members });
    },
    async sismember(key, member) {
      return rpc.call('sismember', { key, member });
    },

    // Sorted set commands
    async zadd(key, score, member) {
      return rpc.call('zadd', { key, score, member });
    },
    async zrange(key, start, stop, options = {}) {
      return rpc.call('zrange', { key, start, stop, ...options });
    },
    async zrem(key, ...members) {
      return rpc.call('zrem', { key, members });
    },
    async zscore(key, member) {
      return rpc.call('zscore', { key, member });
    },

    // Server commands
    async dbsize() {
      return rpc.call('dbsize', {});
    },
    async flushdb() {
      return rpc.call('flushdb', {});
    },
    async info(section) {
      return rpc.call('info', { section });
    },
    async ping(message) {
      return rpc.call('ping', { message });
    },
  };
}
`
  }

  /**
   * Create proxy environment bindings
   */
  private createProxyEnv(): Record<string, unknown> {
    // Create a serializable RPC interface for the proxy
    // The actual Redis calls will be made through this interface
    return {
      call: async (method: string, params: Record<string, unknown>) => {
        return this.executeRedisCommand(method, params)
      },
    }
  }

  /**
   * Execute a Redis command through the proxy
   */
  private async executeRedisCommand(
    method: string,
    params: Record<string, unknown>
  ): Promise<unknown> {
    // Block destructive commands in sandbox mode
    if (BLOCKED_SANDBOX_COMMANDS.includes(method.toLowerCase())) {
      throw new Error(`Command '${method}' is blocked in sandbox mode for security reasons`)
    }

    const proxy = this.redisProxy

    switch (method) {
      case 'get':
        return proxy.get(params.key as string)
      case 'set':
        return proxy.set(
          params.key as string,
          params.value as string,
          {
            ex: params.ex as number | undefined,
            px: params.px as number | undefined,
            nx: params.nx as boolean | undefined,
            xx: params.xx as boolean | undefined,
          }
        )
      case 'del':
        return proxy.del(...(params.keys as string[]))
      case 'exists':
        return proxy.exists(...(params.keys as string[]))
      case 'expire':
        return proxy.expire(params.key as string, params.seconds as number)
      case 'ttl':
        return proxy.ttl(params.key as string)
      case 'keys':
        return proxy.keys(params.pattern as string)
      case 'scan':
        return proxy.scan(params.cursor as number, {
          match: params.match as string | undefined,
          count: params.count as number | undefined,
          type: params.type as string | undefined,
        })
      case 'incr':
        return proxy.incr(params.key as string)
      case 'incrby':
        return proxy.incrby(params.key as string, params.increment as number)
      case 'decr':
        return proxy.decr(params.key as string)
      case 'decrby':
        return proxy.decrby(params.key as string, params.decrement as number)
      case 'append':
        return proxy.append(params.key as string, params.value as string)
      case 'strlen':
        return proxy.strlen(params.key as string)
      case 'type':
        return proxy.type(params.key as string)
      case 'rename':
        return proxy.rename(params.key as string, params.newKey as string)
      case 'hget':
        return proxy.hget(params.key as string, params.field as string)
      case 'hset':
        return proxy.hset(
          params.key as string,
          params.field as string,
          params.value as string
        )
      case 'hgetall':
        return proxy.hgetall(params.key as string)
      case 'hdel':
        return proxy.hdel(params.key as string, ...(params.fields as string[]))
      case 'lpush':
        return proxy.lpush(params.key as string, ...(params.values as string[]))
      case 'rpush':
        return proxy.rpush(params.key as string, ...(params.values as string[]))
      case 'lpop':
        return proxy.lpop(params.key as string)
      case 'rpop':
        return proxy.rpop(params.key as string)
      case 'lrange':
        return proxy.lrange(
          params.key as string,
          params.start as number,
          params.stop as number
        )
      case 'sadd':
        return proxy.sadd(params.key as string, ...(params.members as string[]))
      case 'smembers':
        return proxy.smembers(params.key as string)
      case 'srem':
        return proxy.srem(params.key as string, ...(params.members as string[]))
      case 'sismember':
        return proxy.sismember(params.key as string, params.member as string)
      case 'zadd':
        return proxy.zadd(
          params.key as string,
          params.score as number,
          params.member as string
        )
      case 'zrange':
        return proxy.zrange(
          params.key as string,
          params.start as number,
          params.stop as number,
          { withScores: params.withScores as boolean | undefined }
        )
      case 'zrem':
        return proxy.zrem(params.key as string, ...(params.members as string[]))
      case 'zscore':
        return proxy.zscore(params.key as string, params.member as string)
      case 'dbsize':
        return proxy.dbsize()
      case 'flushdb':
        return proxy.flushdb()
      case 'info':
        return proxy.info(params.section as string | undefined)
      case 'ping':
        return proxy.ping(params.message as string | undefined)
      default:
        throw new Error(`Unknown Redis command: ${method}`)
    }
  }

  /**
   * Execute with timeout enforcement
   */
  private async executeWithTimeout(
    entrypoint: WorkerEntrypoint
  ): Promise<SandboxResult> {
    const timeout = this.options.timeout!

    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(new Error(`Execution timed out after ${timeout}ms`))
      }, timeout)
    })

    const executionPromise = async (): Promise<SandboxResult> => {
      const request = new Request('http://sandbox/execute', {
        method: 'POST',
      })

      const response = await entrypoint.fetch(request)
      const result = await response.json() as SandboxResult

      return result
    }

    return Promise.race([executionPromise(), timeoutPromise])
  }

  /**
   * Get the worker ID
   */
  getWorkerId(): string {
    return this.workerId
  }

  /**
   * Cleanup resources
   */
  async cleanup(): Promise<void> {
    this.workerStub = null
  }
}

// ─────────────────────────────────────────────────────────────────
// Factory Function
// ─────────────────────────────────────────────────────────────────

/**
 * Create a new worker evaluator instance
 */
export function createWorkerEvaluator(
  loader: WorkerLoader,
  redisProxy: RedisProxy,
  options?: WorkerEvaluatorOptions
): WorkerEvaluator {
  return new WorkerEvaluator(loader, redisProxy, options)
}

// ─────────────────────────────────────────────────────────────────
// Sandbox Executor (for environments without Worker Loader)
// ─────────────────────────────────────────────────────────────────

/**
 * Options for basic sandbox execution
 */
export interface BasicSandboxOptions {
  /** Execution timeout in milliseconds */
  timeout?: number
  /** Maximum log entries to capture */
  maxLogEntries?: number
  /** Maximum length of each log entry */
  maxLogEntryLength?: number
}

/**
 * Execute code in a basic sandbox (for testing/development)
 *
 * WARNING: This is NOT as secure as Worker-based sandbox.
 * It uses code validation to block dangerous patterns but runs
 * in the same V8 isolate. Use only for development/testing.
 *
 * Security Features:
 * - Code validation for dangerous patterns
 * - Execution timeout enforcement
 * - Console output capture with limits
 * - Frozen objects to prevent prototype pollution
 *
 * @param userCode - User code to execute
 * @param redisProxy - Redis proxy for controlled database access
 * @param options - Execution options
 */
export async function executeInBasicSandbox(
  userCode: string,
  redisProxy: RedisProxy,
  options: BasicSandboxOptions = {}
): Promise<SandboxResult> {
  const startTime = Date.now()
  const timeout = options.timeout ?? DEFAULT_TIMEOUT
  const maxLogEntries = options.maxLogEntries ?? MAX_LOG_ENTRIES
  const maxLogEntryLength = options.maxLogEntryLength ?? MAX_LOG_ENTRY_LENGTH

  // Validate code
  const validation = validateUserCode(userCode)
  if (!validation.valid) {
    return {
      success: false,
      error: `Code validation failed: ${validation.errors.join('; ')}`,
      logs: [],
      executionTime: Date.now() - startTime,
    }
  }

  const logs: string[] = []
  let logCount = 0

  // Helper to add log with limits
  const addLog = (message: string): void => {
    if (logCount >= maxLogEntries) return
    logCount++
    const truncated = message.length > maxLogEntryLength
      ? message.slice(0, maxLogEntryLength) + '... (truncated)'
      : message
    logs.push(truncated)
  }

  // Create console capture with limits
  const captureConsole = Object.freeze({
    log: (...args: unknown[]) => addLog(args.map(String).join(' ')),
    info: (...args: unknown[]) => addLog(`[INFO] ${args.map(String).join(' ')}`),
    warn: (...args: unknown[]) => addLog(`[WARN] ${args.map(String).join(' ')}`),
    error: (...args: unknown[]) => addLog(`[ERROR] ${args.map(String).join(' ')}`),
    debug: (...args: unknown[]) => addLog(`[DEBUG] ${args.map(String).join(' ')}`),
    trace: (...args: unknown[]) => addLog(`[TRACE] ${args.map(String).join(' ')}`),
    time: () => {},
    timeEnd: () => {},
    assert: (condition: boolean, ...args: unknown[]) => {
      if (!condition) addLog(`[ASSERT] ${args.map(String).join(' ')}`)
    },
    clear: () => {},
    count: () => {},
    countReset: () => {},
    group: () => {},
    groupCollapsed: () => {},
    groupEnd: () => {},
    table: () => {},
    dir: () => {},
    dirxml: () => {},
  })

  // Create frozen Redis proxy wrapper to prevent modification
  const frozenRedisProxy = Object.freeze({ ...redisProxy })

  try {
    // Create async function with captured console and redis
    // The user code runs with only redis and console in scope
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor
    const fn = new AsyncFunction('redis', 'console', userCode)

    // Execute with timeout
    const timeoutPromise = new Promise<never>((_, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error(`Execution timed out after ${timeout}ms`))
      }, timeout)
      // Prevent timeout from keeping the process alive
      if (typeof timeoutId === 'object' && 'unref' in timeoutId) {
        (timeoutId as NodeJS.Timeout).unref()
      }
    })

    const result = await Promise.race([
      fn(frozenRedisProxy, captureConsole),
      timeoutPromise,
    ])

    return {
      success: true,
      result,
      logs,
      executionTime: Date.now() - startTime,
    }
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
      logs,
      executionTime: Date.now() - startTime,
    }
  }
}

/**
 * Create a sandboxed Redis proxy that wraps a RedisProxy
 * with additional security checks
 */
export function createSandboxedRedisProxy(
  proxy: RedisProxy,
  options: {
    keyPrefix?: string
    allowedCommands?: string[]
    blockedKeyPatterns?: RegExp[]
  } = {}
): RedisProxy {
  const { keyPrefix = '', allowedCommands, blockedKeyPatterns = [] } = options

  // Helper to validate and transform keys
  const transformKey = (key: string): string => {
    for (const pattern of blockedKeyPatterns) {
      if (pattern.test(key)) {
        throw new Error(`Access to key matching pattern ${pattern} is blocked`)
      }
    }
    return keyPrefix + key
  }

  // Helper to check if command is allowed
  const checkCommand = (command: string): void => {
    if (allowedCommands && !allowedCommands.includes(command)) {
      throw new Error(`Command '${command}' is not allowed in sandbox`)
    }
  }

  return {
    async get(key: string) {
      checkCommand('get')
      return proxy.get(transformKey(key))
    },
    async set(key: string, value: string, opts) {
      checkCommand('set')
      return proxy.set(transformKey(key), value, opts)
    },
    async del(...keys: string[]) {
      checkCommand('del')
      return proxy.del(...keys.map(transformKey))
    },
    async exists(...keys: string[]) {
      checkCommand('exists')
      return proxy.exists(...keys.map(transformKey))
    },
    async expire(key: string, seconds: number) {
      checkCommand('expire')
      return proxy.expire(transformKey(key), seconds)
    },
    async ttl(key: string) {
      checkCommand('ttl')
      return proxy.ttl(transformKey(key))
    },
    async keys(pattern: string) {
      checkCommand('keys')
      const results = await proxy.keys(keyPrefix + pattern)
      return results.map(k => k.slice(keyPrefix.length))
    },
    async scan(cursor: number, opts) {
      checkCommand('scan')
      const [newCursor, results] = await proxy.scan(cursor, {
        ...opts,
        match: opts?.match ? keyPrefix + opts.match : keyPrefix + '*',
      })
      return [newCursor, results.map(k => k.slice(keyPrefix.length))]
    },
    async hget(key: string, field: string) {
      checkCommand('hget')
      return proxy.hget(transformKey(key), field)
    },
    async hset(key: string, field: string, value: string) {
      checkCommand('hset')
      return proxy.hset(transformKey(key), field, value)
    },
    async hgetall(key: string) {
      checkCommand('hgetall')
      return proxy.hgetall(transformKey(key))
    },
    async hdel(key: string, ...fields: string[]) {
      checkCommand('hdel')
      return proxy.hdel(transformKey(key), ...fields)
    },
    async lpush(key: string, ...values: string[]) {
      checkCommand('lpush')
      return proxy.lpush(transformKey(key), ...values)
    },
    async rpush(key: string, ...values: string[]) {
      checkCommand('rpush')
      return proxy.rpush(transformKey(key), ...values)
    },
    async lpop(key: string) {
      checkCommand('lpop')
      return proxy.lpop(transformKey(key))
    },
    async rpop(key: string) {
      checkCommand('rpop')
      return proxy.rpop(transformKey(key))
    },
    async lrange(key: string, start: number, stop: number) {
      checkCommand('lrange')
      return proxy.lrange(transformKey(key), start, stop)
    },
    async sadd(key: string, ...members: string[]) {
      checkCommand('sadd')
      return proxy.sadd(transformKey(key), ...members)
    },
    async smembers(key: string) {
      checkCommand('smembers')
      return proxy.smembers(transformKey(key))
    },
    async srem(key: string, ...members: string[]) {
      checkCommand('srem')
      return proxy.srem(transformKey(key), ...members)
    },
    async sismember(key: string, member: string) {
      checkCommand('sismember')
      return proxy.sismember(transformKey(key), member)
    },
    async zadd(key: string, score: number, member: string) {
      checkCommand('zadd')
      return proxy.zadd(transformKey(key), score, member)
    },
    async zrange(key: string, start: number, stop: number, opts) {
      checkCommand('zrange')
      return proxy.zrange(transformKey(key), start, stop, opts)
    },
    async zrem(key: string, ...members: string[]) {
      checkCommand('zrem')
      return proxy.zrem(transformKey(key), ...members)
    },
    async zscore(key: string, member: string) {
      checkCommand('zscore')
      return proxy.zscore(transformKey(key), member)
    },
    async incr(key: string) {
      checkCommand('incr')
      return proxy.incr(transformKey(key))
    },
    async incrby(key: string, increment: number) {
      checkCommand('incrby')
      return proxy.incrby(transformKey(key), increment)
    },
    async decr(key: string) {
      checkCommand('decr')
      return proxy.decr(transformKey(key))
    },
    async decrby(key: string, decrement: number) {
      checkCommand('decrby')
      return proxy.decrby(transformKey(key), decrement)
    },
    async append(key: string, value: string) {
      checkCommand('append')
      return proxy.append(transformKey(key), value)
    },
    async strlen(key: string) {
      checkCommand('strlen')
      return proxy.strlen(transformKey(key))
    },
    async type(key: string) {
      checkCommand('type')
      return proxy.type(transformKey(key))
    },
    async rename(key: string, newKey: string) {
      checkCommand('rename')
      return proxy.rename(transformKey(key), transformKey(newKey))
    },
    async dbsize() {
      checkCommand('dbsize')
      return proxy.dbsize()
    },
    async flushdb() {
      throw new Error("Command 'flushdb' is blocked in sandbox mode for security reasons")
    },
    async info(section?: string) {
      checkCommand('info')
      return proxy.info(section)
    },
    async ping(message?: string) {
      checkCommand('ping')
      return proxy.ping(message)
    },
  }
}
