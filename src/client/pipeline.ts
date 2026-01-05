/**
 * Pipeline - Batch Redis commands for efficient execution
 *
 * Provides ioredis-compatible API for batching multiple commands
 * and executing them in a single round-trip.
 */

import type { SetOptions, ZRangeOptions, ScanOptions } from '../types'

export type PipelineCommand = {
  method: string
  args: unknown[]
}

export type PipelineExecResult = [Error | null, unknown][]

/**
 * Pipeline class for batching Redis commands
 *
 * @example
 * ```ts
 * const pipeline = redis.pipeline()
 * pipeline.set('key1', 'value1')
 * pipeline.get('key1')
 * pipeline.incr('counter')
 * const results = await pipeline.exec()
 * // results = [[null, 'OK'], [null, 'value1'], [null, 1]]
 * ```
 */
export class Pipeline {
  private commands: PipelineCommand[] = []
  private executor: (commands: PipelineCommand[]) => Promise<PipelineExecResult>

  constructor(executor: (commands: PipelineCommand[]) => Promise<PipelineExecResult>) {
    this.executor = executor
  }

  /**
   * Get the number of queued commands
   */
  get length(): number {
    return this.commands.length
  }

  /**
   * Execute all queued commands
   * @returns Array of [error, result] tuples
   */
  async exec(): Promise<PipelineExecResult> {
    if (this.commands.length === 0) {
      return []
    }
    const result = await this.executor(this.commands)
    this.commands = []
    return result
  }

  /**
   * Alias for exec()
   */
  execBuffer(): Promise<PipelineExecResult> {
    return this.exec()
  }

  // ─────────────────────────────────────────────────────────────────
  // String Commands
  // ─────────────────────────────────────────────────────────────────

  get(key: string): this {
    this.commands.push({ method: 'get', args: [key] })
    return this
  }

  set(key: string, value: string | number, ...args: unknown[]): this {
    this.commands.push({ method: 'set', args: [key, value, ...args] })
    return this
  }

  mget(...keys: string[]): this {
    this.commands.push({ method: 'mget', args: keys })
    return this
  }

  mset(...args: (string | number)[]): this {
    this.commands.push({ method: 'mset', args })
    return this
  }

  msetnx(...args: (string | number)[]): this {
    this.commands.push({ method: 'msetnx', args })
    return this
  }

  setnx(key: string, value: string | number): this {
    this.commands.push({ method: 'setnx', args: [key, value] })
    return this
  }

  setex(key: string, seconds: number, value: string | number): this {
    this.commands.push({ method: 'setex', args: [key, seconds, value] })
    return this
  }

  psetex(key: string, milliseconds: number, value: string | number): this {
    this.commands.push({ method: 'psetex', args: [key, milliseconds, value] })
    return this
  }

  getset(key: string, value: string | number): this {
    this.commands.push({ method: 'getset', args: [key, value] })
    return this
  }

  getdel(key: string): this {
    this.commands.push({ method: 'getdel', args: [key] })
    return this
  }

  getex(key: string, options?: SetOptions): this {
    this.commands.push({ method: 'getex', args: [key, options] })
    return this
  }

  append(key: string, value: string): this {
    this.commands.push({ method: 'append', args: [key, value] })
    return this
  }

  strlen(key: string): this {
    this.commands.push({ method: 'strlen', args: [key] })
    return this
  }

  setrange(key: string, offset: number, value: string): this {
    this.commands.push({ method: 'setrange', args: [key, offset, value] })
    return this
  }

  getrange(key: string, start: number, end: number): this {
    this.commands.push({ method: 'getrange', args: [key, start, end] })
    return this
  }

  incr(key: string): this {
    this.commands.push({ method: 'incr', args: [key] })
    return this
  }

  incrby(key: string, increment: number): this {
    this.commands.push({ method: 'incrby', args: [key, increment] })
    return this
  }

  incrbyfloat(key: string, increment: number): this {
    this.commands.push({ method: 'incrbyfloat', args: [key, increment] })
    return this
  }

  decr(key: string): this {
    this.commands.push({ method: 'decr', args: [key] })
    return this
  }

  decrby(key: string, decrement: number): this {
    this.commands.push({ method: 'decrby', args: [key, decrement] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Hash Commands
  // ─────────────────────────────────────────────────────────────────

  hget(key: string, field: string): this {
    this.commands.push({ method: 'hget', args: [key, field] })
    return this
  }

  hset(key: string, ...fieldValues: (string | number)[]): this {
    this.commands.push({ method: 'hset', args: [key, ...fieldValues] })
    return this
  }

  hsetnx(key: string, field: string, value: string | number): this {
    this.commands.push({ method: 'hsetnx', args: [key, field, value] })
    return this
  }

  hmget(key: string, ...fields: string[]): this {
    this.commands.push({ method: 'hmget', args: [key, ...fields] })
    return this
  }

  hmset(key: string, ...fieldValues: (string | number)[]): this {
    this.commands.push({ method: 'hmset', args: [key, ...fieldValues] })
    return this
  }

  hgetall(key: string): this {
    this.commands.push({ method: 'hgetall', args: [key] })
    return this
  }

  hdel(key: string, ...fields: string[]): this {
    this.commands.push({ method: 'hdel', args: [key, ...fields] })
    return this
  }

  hexists(key: string, field: string): this {
    this.commands.push({ method: 'hexists', args: [key, field] })
    return this
  }

  hlen(key: string): this {
    this.commands.push({ method: 'hlen', args: [key] })
    return this
  }

  hkeys(key: string): this {
    this.commands.push({ method: 'hkeys', args: [key] })
    return this
  }

  hvals(key: string): this {
    this.commands.push({ method: 'hvals', args: [key] })
    return this
  }

  hincrby(key: string, field: string, increment: number): this {
    this.commands.push({ method: 'hincrby', args: [key, field, increment] })
    return this
  }

  hincrbyfloat(key: string, field: string, increment: number): this {
    this.commands.push({ method: 'hincrbyfloat', args: [key, field, increment] })
    return this
  }

  hstrlen(key: string, field: string): this {
    this.commands.push({ method: 'hstrlen', args: [key, field] })
    return this
  }

  hscan(key: string, cursor: string | number, options?: ScanOptions): this {
    this.commands.push({ method: 'hscan', args: [key, cursor, options] })
    return this
  }

  hrandfield(key: string, count?: number, withValues?: boolean): this {
    this.commands.push({ method: 'hrandfield', args: [key, count, withValues] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // List Commands
  // ─────────────────────────────────────────────────────────────────

  lpush(key: string, ...values: (string | number)[]): this {
    this.commands.push({ method: 'lpush', args: [key, ...values] })
    return this
  }

  lpushx(key: string, ...values: (string | number)[]): this {
    this.commands.push({ method: 'lpushx', args: [key, ...values] })
    return this
  }

  rpush(key: string, ...values: (string | number)[]): this {
    this.commands.push({ method: 'rpush', args: [key, ...values] })
    return this
  }

  rpushx(key: string, ...values: (string | number)[]): this {
    this.commands.push({ method: 'rpushx', args: [key, ...values] })
    return this
  }

  lpop(key: string, count?: number): this {
    this.commands.push({ method: 'lpop', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  rpop(key: string, count?: number): this {
    this.commands.push({ method: 'rpop', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  lrange(key: string, start: number, stop: number): this {
    this.commands.push({ method: 'lrange', args: [key, start, stop] })
    return this
  }

  lindex(key: string, index: number): this {
    this.commands.push({ method: 'lindex', args: [key, index] })
    return this
  }

  lset(key: string, index: number, value: string | number): this {
    this.commands.push({ method: 'lset', args: [key, index, value] })
    return this
  }

  llen(key: string): this {
    this.commands.push({ method: 'llen', args: [key] })
    return this
  }

  lrem(key: string, count: number, value: string | number): this {
    this.commands.push({ method: 'lrem', args: [key, count, value] })
    return this
  }

  ltrim(key: string, start: number, stop: number): this {
    this.commands.push({ method: 'ltrim', args: [key, start, stop] })
    return this
  }

  linsert(key: string, position: 'BEFORE' | 'AFTER', pivot: string | number, value: string | number): this {
    this.commands.push({ method: 'linsert', args: [key, position, pivot, value] })
    return this
  }

  lpos(key: string, element: string | number, options?: { rank?: number; count?: number; maxlen?: number }): this {
    this.commands.push({ method: 'lpos', args: [key, element, options] })
    return this
  }

  lmove(source: string, destination: string, from: 'LEFT' | 'RIGHT', to: 'LEFT' | 'RIGHT'): this {
    this.commands.push({ method: 'lmove', args: [source, destination, from, to] })
    return this
  }

  rpoplpush(source: string, destination: string): this {
    this.commands.push({ method: 'rpoplpush', args: [source, destination] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Set Commands
  // ─────────────────────────────────────────────────────────────────

  sadd(key: string, ...members: (string | number)[]): this {
    this.commands.push({ method: 'sadd', args: [key, ...members] })
    return this
  }

  srem(key: string, ...members: (string | number)[]): this {
    this.commands.push({ method: 'srem', args: [key, ...members] })
    return this
  }

  smembers(key: string): this {
    this.commands.push({ method: 'smembers', args: [key] })
    return this
  }

  sismember(key: string, member: string | number): this {
    this.commands.push({ method: 'sismember', args: [key, member] })
    return this
  }

  smismember(key: string, ...members: (string | number)[]): this {
    this.commands.push({ method: 'smismember', args: [key, ...members] })
    return this
  }

  scard(key: string): this {
    this.commands.push({ method: 'scard', args: [key] })
    return this
  }

  spop(key: string, count?: number): this {
    this.commands.push({ method: 'spop', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  srandmember(key: string, count?: number): this {
    this.commands.push({ method: 'srandmember', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  smove(source: string, destination: string, member: string | number): this {
    this.commands.push({ method: 'smove', args: [source, destination, member] })
    return this
  }

  sdiff(...keys: string[]): this {
    this.commands.push({ method: 'sdiff', args: keys })
    return this
  }

  sdiffstore(destination: string, ...keys: string[]): this {
    this.commands.push({ method: 'sdiffstore', args: [destination, ...keys] })
    return this
  }

  sinter(...keys: string[]): this {
    this.commands.push({ method: 'sinter', args: keys })
    return this
  }

  sinterstore(destination: string, ...keys: string[]): this {
    this.commands.push({ method: 'sinterstore', args: [destination, ...keys] })
    return this
  }

  sintercard(numkeys: number, ...keys: string[]): this {
    this.commands.push({ method: 'sintercard', args: [numkeys, ...keys] })
    return this
  }

  sunion(...keys: string[]): this {
    this.commands.push({ method: 'sunion', args: keys })
    return this
  }

  sunionstore(destination: string, ...keys: string[]): this {
    this.commands.push({ method: 'sunionstore', args: [destination, ...keys] })
    return this
  }

  sscan(key: string, cursor: string | number, options?: ScanOptions): this {
    this.commands.push({ method: 'sscan', args: [key, cursor, options] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Sorted Set Commands
  // ─────────────────────────────────────────────────────────────────

  zadd(key: string, ...args: unknown[]): this {
    this.commands.push({ method: 'zadd', args: [key, ...args] })
    return this
  }

  zrem(key: string, ...members: (string | number)[]): this {
    this.commands.push({ method: 'zrem', args: [key, ...members] })
    return this
  }

  zscore(key: string, member: string | number): this {
    this.commands.push({ method: 'zscore', args: [key, member] })
    return this
  }

  zmscore(key: string, ...members: (string | number)[]): this {
    this.commands.push({ method: 'zmscore', args: [key, ...members] })
    return this
  }

  zrank(key: string, member: string | number): this {
    this.commands.push({ method: 'zrank', args: [key, member] })
    return this
  }

  zrevrank(key: string, member: string | number): this {
    this.commands.push({ method: 'zrevrank', args: [key, member] })
    return this
  }

  zcard(key: string): this {
    this.commands.push({ method: 'zcard', args: [key] })
    return this
  }

  zcount(key: string, min: string | number, max: string | number): this {
    this.commands.push({ method: 'zcount', args: [key, min, max] })
    return this
  }

  zlexcount(key: string, min: string, max: string): this {
    this.commands.push({ method: 'zlexcount', args: [key, min, max] })
    return this
  }

  zincrby(key: string, increment: number, member: string | number): this {
    this.commands.push({ method: 'zincrby', args: [key, increment, member] })
    return this
  }

  zrange(key: string, start: number | string, stop: number | string, options?: ZRangeOptions | string): this {
    this.commands.push({ method: 'zrange', args: [key, start, stop, options] })
    return this
  }

  zrevrange(key: string, start: number, stop: number, withScores?: 'WITHSCORES'): this {
    this.commands.push({ method: 'zrevrange', args: withScores ? [key, start, stop, withScores] : [key, start, stop] })
    return this
  }

  zrangebyscore(key: string, min: string | number, max: string | number, ...args: unknown[]): this {
    this.commands.push({ method: 'zrangebyscore', args: [key, min, max, ...args] })
    return this
  }

  zrevrangebyscore(key: string, max: string | number, min: string | number, ...args: unknown[]): this {
    this.commands.push({ method: 'zrevrangebyscore', args: [key, max, min, ...args] })
    return this
  }

  zrangebylex(key: string, min: string, max: string, ...args: unknown[]): this {
    this.commands.push({ method: 'zrangebylex', args: [key, min, max, ...args] })
    return this
  }

  zrevrangebylex(key: string, max: string, min: string, ...args: unknown[]): this {
    this.commands.push({ method: 'zrevrangebylex', args: [key, max, min, ...args] })
    return this
  }

  zrangestore(destination: string, source: string, start: number | string, stop: number | string, options?: ZRangeOptions): this {
    this.commands.push({ method: 'zrangestore', args: [destination, source, start, stop, options] })
    return this
  }

  zpopmin(key: string, count?: number): this {
    this.commands.push({ method: 'zpopmin', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  zpopmax(key: string, count?: number): this {
    this.commands.push({ method: 'zpopmax', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  zrandmember(key: string, count?: number, withScores?: boolean): this {
    this.commands.push({ method: 'zrandmember', args: [key, count, withScores] })
    return this
  }

  zremrangebyrank(key: string, start: number, stop: number): this {
    this.commands.push({ method: 'zremrangebyrank', args: [key, start, stop] })
    return this
  }

  zremrangebyscore(key: string, min: string | number, max: string | number): this {
    this.commands.push({ method: 'zremrangebyscore', args: [key, min, max] })
    return this
  }

  zremrangebylex(key: string, min: string, max: string): this {
    this.commands.push({ method: 'zremrangebylex', args: [key, min, max] })
    return this
  }

  zunion(numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zunion', args: [numkeys, ...args] })
    return this
  }

  zunionstore(destination: string, numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zunionstore', args: [destination, numkeys, ...args] })
    return this
  }

  zinter(numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zinter', args: [numkeys, ...args] })
    return this
  }

  zinterstore(destination: string, numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zinterstore', args: [destination, numkeys, ...args] })
    return this
  }

  zdiff(numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zdiff', args: [numkeys, ...args] })
    return this
  }

  zdiffstore(destination: string, numkeys: number, ...args: unknown[]): this {
    this.commands.push({ method: 'zdiffstore', args: [destination, numkeys, ...args] })
    return this
  }

  zscan(key: string, cursor: string | number, options?: ScanOptions): this {
    this.commands.push({ method: 'zscan', args: [key, cursor, options] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Key Commands
  // ─────────────────────────────────────────────────────────────────

  del(...keys: string[]): this {
    this.commands.push({ method: 'del', args: keys })
    return this
  }

  unlink(...keys: string[]): this {
    this.commands.push({ method: 'unlink', args: keys })
    return this
  }

  exists(...keys: string[]): this {
    this.commands.push({ method: 'exists', args: keys })
    return this
  }

  expire(key: string, seconds: number, mode?: 'NX' | 'XX' | 'GT' | 'LT'): this {
    this.commands.push({ method: 'expire', args: mode ? [key, seconds, mode] : [key, seconds] })
    return this
  }

  expireat(key: string, timestamp: number, mode?: 'NX' | 'XX' | 'GT' | 'LT'): this {
    this.commands.push({ method: 'expireat', args: mode ? [key, timestamp, mode] : [key, timestamp] })
    return this
  }

  expiretime(key: string): this {
    this.commands.push({ method: 'expiretime', args: [key] })
    return this
  }

  pexpire(key: string, milliseconds: number, mode?: 'NX' | 'XX' | 'GT' | 'LT'): this {
    this.commands.push({ method: 'pexpire', args: mode ? [key, milliseconds, mode] : [key, milliseconds] })
    return this
  }

  pexpireat(key: string, timestamp: number, mode?: 'NX' | 'XX' | 'GT' | 'LT'): this {
    this.commands.push({ method: 'pexpireat', args: mode ? [key, timestamp, mode] : [key, timestamp] })
    return this
  }

  pexpiretime(key: string): this {
    this.commands.push({ method: 'pexpiretime', args: [key] })
    return this
  }

  ttl(key: string): this {
    this.commands.push({ method: 'ttl', args: [key] })
    return this
  }

  pttl(key: string): this {
    this.commands.push({ method: 'pttl', args: [key] })
    return this
  }

  persist(key: string): this {
    this.commands.push({ method: 'persist', args: [key] })
    return this
  }

  type(key: string): this {
    this.commands.push({ method: 'type', args: [key] })
    return this
  }

  rename(key: string, newkey: string): this {
    this.commands.push({ method: 'rename', args: [key, newkey] })
    return this
  }

  renamenx(key: string, newkey: string): this {
    this.commands.push({ method: 'renamenx', args: [key, newkey] })
    return this
  }

  copy(source: string, destination: string, options?: { replace?: boolean; db?: number }): this {
    this.commands.push({ method: 'copy', args: [source, destination, options] })
    return this
  }

  keys(pattern: string): this {
    this.commands.push({ method: 'keys', args: [pattern] })
    return this
  }

  scan(cursor: string | number, options?: ScanOptions): this {
    this.commands.push({ method: 'scan', args: [cursor, options] })
    return this
  }

  randomkey(): this {
    this.commands.push({ method: 'randomkey', args: [] })
    return this
  }

  touch(...keys: string[]): this {
    this.commands.push({ method: 'touch', args: keys })
    return this
  }

  object(subcommand: string, ...args: unknown[]): this {
    this.commands.push({ method: 'object', args: [subcommand, ...args] })
    return this
  }

  dump(key: string): this {
    this.commands.push({ method: 'dump', args: [key] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Server Commands
  // ─────────────────────────────────────────────────────────────────

  ping(message?: string): this {
    this.commands.push({ method: 'ping', args: message !== undefined ? [message] : [] })
    return this
  }

  echo(message: string): this {
    this.commands.push({ method: 'echo', args: [message] })
    return this
  }

  dbsize(): this {
    this.commands.push({ method: 'dbsize', args: [] })
    return this
  }

  flushdb(mode?: 'ASYNC' | 'SYNC'): this {
    this.commands.push({ method: 'flushdb', args: mode ? [mode] : [] })
    return this
  }

  flushall(mode?: 'ASYNC' | 'SYNC'): this {
    this.commands.push({ method: 'flushall', args: mode ? [mode] : [] })
    return this
  }

  info(section?: string): this {
    this.commands.push({ method: 'info', args: section !== undefined ? [section] : [] })
    return this
  }

  time(): this {
    this.commands.push({ method: 'time', args: [] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Generic Command
  // ─────────────────────────────────────────────────────────────────

  /**
   * Execute any Redis command
   */
  call(command: string, ...args: unknown[]): this {
    this.commands.push({ method: command.toLowerCase(), args })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Buffer Methods (ioredis compatibility)
  // ─────────────────────────────────────────────────────────────────

  getBuffer(key: string): this {
    this.commands.push({ method: 'get', args: [key] })
    return this
  }

  hgetBuffer(key: string, field: string): this {
    this.commands.push({ method: 'hget', args: [key, field] })
    return this
  }

  hgetallBuffer(key: string): this {
    this.commands.push({ method: 'hgetall', args: [key] })
    return this
  }

  lrangeBuffer(key: string, start: number, stop: number): this {
    this.commands.push({ method: 'lrange', args: [key, start, stop] })
    return this
  }

  lpopBuffer(key: string, count?: number): this {
    this.commands.push({ method: 'lpop', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  rpopBuffer(key: string, count?: number): this {
    this.commands.push({ method: 'rpop', args: count !== undefined ? [key, count] : [key] })
    return this
  }

  smembersBuffer(key: string): this {
    this.commands.push({ method: 'smembers', args: [key] })
    return this
  }

  zrangeBuffer(key: string, start: number | string, stop: number | string, options?: ZRangeOptions | string): this {
    this.commands.push({ method: 'zrange', args: [key, start, stop, options] })
    return this
  }

  mgetBuffer(...keys: string[]): this {
    this.commands.push({ method: 'mget', args: keys })
    return this
  }

  getrangeBuffer(key: string, start: number, end: number): this {
    this.commands.push({ method: 'getrange', args: [key, start, end] })
    return this
  }

  // ─────────────────────────────────────────────────────────────────
  // Additional ioredis compatibility methods
  // ─────────────────────────────────────────────────────────────────

  select(db: number): this {
    // No-op for Redois, but included for compatibility
    this.commands.push({ method: 'select', args: [db] })
    return this
  }

  publish(channel: string, message: string): this {
    this.commands.push({ method: 'publish', args: [channel, message] })
    return this
  }

  eval(script: string, numkeys: number, ...keysAndArgs: (string | number)[]): this {
    this.commands.push({ method: 'eval', args: [script, numkeys, ...keysAndArgs] })
    return this
  }

  evalsha(sha1: string, numkeys: number, ...keysAndArgs: (string | number)[]): this {
    this.commands.push({ method: 'evalsha', args: [sha1, numkeys, ...keysAndArgs] })
    return this
  }
}
