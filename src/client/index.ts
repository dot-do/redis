/**
 * Redois Client Library
 *
 * An ioredis-compatible client for Redois - Redis on Cloudflare Workers.
 *
 * @example
 * ```ts
 * import { Redis } from 'redois/client'
 *
 * const redis = new Redis({
 *   url: 'https://your-redois.workers.dev',
 *   token: 'your-auth-token'
 * })
 *
 * // String commands
 * await redis.set('key', 'value')
 * const value = await redis.get('key')
 *
 * // With options
 * await redis.set('key', 'value', 'EX', 3600)
 * await redis.set('key', 'value', { ex: 3600, nx: true })
 *
 * // Pipelining
 * const pipeline = redis.pipeline()
 * pipeline.set('a', '1')
 * pipeline.get('a')
 * pipeline.incr('counter')
 * const results = await pipeline.exec()
 * // results = [[null, 'OK'], [null, '1'], [null, 1]]
 *
 * // Auto-pipelining (batches concurrent requests)
 * const redis = new Redis({
 *   url: 'https://your-redois.workers.dev',
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
 * // WebSocket transport
 * const redis = new Redis({
 *   url: 'https://your-redois.workers.dev',
 *   token: 'your-auth-token',
 *   useWebSocket: true
 * })
 *
 * // Key prefix
 * const redis = new Redis({
 *   url: 'https://your-redois.workers.dev',
 *   token: 'your-auth-token',
 *   keyPrefix: 'myapp:'
 * })
 * await redis.set('user:1', 'data') // Actually sets 'myapp:user:1'
 * ```
 *
 * @packageDocumentation
 */

import { Redis } from './redis'
export { Redis, RedisError } from './redis'
export { Pipeline } from './pipeline'

// Default export for ioredis compatibility (import Redis from 'redisdo/client')
export default Redis
export type { PipelineCommand, PipelineExecResult } from './pipeline'

// Re-export types from types module
export type {
  RedisOptions,
  SetOptions,
  ScanOptions,
  ZAddOptions,
  ZRangeOptions,
  RedisClientEvents,
} from '../types'
