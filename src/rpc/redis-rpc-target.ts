/**
 * Redis RPC Target - Abstract class defining all Redis commands
 *
 * The Durable Object IS the RpcTarget. This abstract class defines the contract
 * for all Redis commands that can be called via RPC.
 *
 * Following capnweb patterns from MondoDB where the DO serves as the RPC target.
 */

import type { SetOptions, ScanOptions, ZAddOptions, ZRangeOptions } from '../types'

// ─────────────────────────────────────────────────────────────────
// Allowed Methods Registry
// ─────────────────────────────────────────────────────────────────

/**
 * Whitelist of allowed Redis commands for RPC
 * Commands not in this set will be rejected
 */
export const ALLOWED_REDIS_COMMANDS = new Set<string>([
  // Connection
  'ping',
  'echo',
  'quit',
  'select',

  // String commands
  'get',
  'set',
  'getex',
  'getdel',
  'mget',
  'mset',
  'msetnx',
  'setnx',
  'setex',
  'psetex',
  'append',
  'strlen',
  'incr',
  'incrby',
  'incrbyfloat',
  'decr',
  'decrby',
  'getrange',
  'setrange',
  'getset',

  // Key commands
  'del',
  'exists',
  'expire',
  'expireat',
  'expiretime',
  'pexpire',
  'pexpireat',
  'pexpiretime',
  'ttl',
  'pttl',
  'persist',
  'type',
  'rename',
  'renamenx',
  'keys',
  'scan',
  'randomkey',
  'dbsize',
  'touch',
  'unlink',
  'copy',
  'dump',
  'restore',
  'object',

  // Hash commands
  'hget',
  'hset',
  'hsetnx',
  'hmget',
  'hmset',
  'hgetall',
  'hdel',
  'hexists',
  'hincrby',
  'hincrbyfloat',
  'hkeys',
  'hvals',
  'hlen',
  'hstrlen',
  'hscan',
  'hrandfield',

  // List commands
  'lpush',
  'rpush',
  'lpushx',
  'rpushx',
  'lpop',
  'rpop',
  'llen',
  'lrange',
  'lindex',
  'lset',
  'linsert',
  'lrem',
  'ltrim',
  'lpos',
  'lmove',
  'blpop',
  'brpop',
  'blmove',

  // Set commands
  'sadd',
  'srem',
  'smembers',
  'sismember',
  'smismember',
  'scard',
  'spop',
  'srandmember',
  'sdiff',
  'sdiffstore',
  'sinter',
  'sinterstore',
  'sintercard',
  'sunion',
  'sunionstore',
  'smove',
  'sscan',

  // Sorted set commands
  'zadd',
  'zrem',
  'zscore',
  'zrank',
  'zrevrank',
  'zrange',
  'zrevrange',
  'zrangebyscore',
  'zrevrangebyscore',
  'zrangebylex',
  'zrevrangebylex',
  'zcard',
  'zcount',
  'zlexcount',
  'zincrby',
  'zpopmin',
  'zpopmax',
  'bzpopmin',
  'bzpopmax',
  'zrangestore',
  'zunion',
  'zunionstore',
  'zinter',
  'zinterstore',
  'zintercard',
  'zdiff',
  'zdiffstore',
  'zrandmember',
  'zmscore',
  'zscan',
  'zmpop',
  'bzmpop',

  // Stream commands
  'xadd',
  'xread',
  'xrange',
  'xrevrange',
  'xlen',
  'xtrim',
  'xdel',
  'xinfo',
  'xgroup',
  'xreadgroup',
  'xack',
  'xclaim',
  'xautoclaim',
  'xpending',
  'xsetid',

  // Pub/Sub commands
  'publish',
  'subscribe',
  'unsubscribe',
  'psubscribe',
  'punsubscribe',
  'pubsub',

  // Transaction commands
  'multi',
  'exec',
  'discard',
  'watch',
  'unwatch',

  // Script commands
  'eval',
  'evalsha',
  'script',
  'fcall',
  'fcall_ro',
  'function',

  // Server commands
  'info',
  'time',
  'flushdb',
  'flushall',
  'dbsize',
  'lastsave',
  'config',
  'client',
  'memory',
  'debug',
  'slowlog',
  'acl',
  'command',

  // Cluster commands (for future multi-shard support)
  'cluster',

  // Geo commands
  'geoadd',
  'geodist',
  'geohash',
  'geopos',
  'georadius',
  'georadiusbymember',
  'geosearch',
  'geosearchstore',

  // HyperLogLog commands
  'pfadd',
  'pfcount',
  'pfmerge',

  // Bitmap commands
  'setbit',
  'getbit',
  'bitcount',
  'bitop',
  'bitpos',
  'bitfield',
  'bitfield_ro',
])

// ─────────────────────────────────────────────────────────────────
// RPC Target Abstract Class
// ─────────────────────────────────────────────────────────────────

/**
 * Abstract class that defines the Redis RPC interface
 *
 * The Durable Object (RedisShard) extends this class and implements
 * all Redis commands. RPC calls are made directly to the DO methods.
 */
export abstract class RedisRpcTarget {
  /**
   * Set of allowed method names for RPC calls
   */
  static readonly allowedMethods = ALLOWED_REDIS_COMMANDS

  /**
   * Check if a method is allowed to be called via RPC
   */
  static isAllowedMethod(method: string): boolean {
    return ALLOWED_REDIS_COMMANDS.has(method.toLowerCase())
  }

  // ─────────────────────────────────────────────────────────────
  // Connection Commands
  // ─────────────────────────────────────────────────────────────

  abstract ping(message?: string): Promise<string>
  abstract echo(message: string): Promise<string>

  // ─────────────────────────────────────────────────────────────
  // String Commands
  // ─────────────────────────────────────────────────────────────

  abstract get(key: string): Promise<string | null>
  abstract set(key: string, value: string, options?: SetOptions): Promise<string | null>
  abstract getex(key: string, options?: { ex?: number; px?: number; exat?: number; pxat?: number; persist?: boolean }): Promise<string | null>
  abstract getdel(key: string): Promise<string | null>
  abstract mget(...keys: string[]): Promise<(string | null)[]>
  abstract mset(keyValues: Record<string, string>): Promise<'OK'>
  abstract msetnx(keyValues: Record<string, string>): Promise<0 | 1>
  abstract setnx(key: string, value: string): Promise<0 | 1>
  abstract setex(key: string, seconds: number, value: string): Promise<'OK'>
  abstract psetex(key: string, milliseconds: number, value: string): Promise<'OK'>
  abstract append(key: string, value: string): Promise<number>
  abstract strlen(key: string): Promise<number>
  abstract incr(key: string): Promise<number>
  abstract incrby(key: string, increment: number): Promise<number>
  abstract incrbyfloat(key: string, increment: number): Promise<string>
  abstract decr(key: string): Promise<number>
  abstract decrby(key: string, decrement: number): Promise<number>
  abstract getrange(key: string, start: number, end: number): Promise<string>
  abstract setrange(key: string, offset: number, value: string): Promise<number>
  abstract getset(key: string, value: string): Promise<string | null>

  // ─────────────────────────────────────────────────────────────
  // Key Commands
  // ─────────────────────────────────────────────────────────────

  abstract del(...keys: string[]): Promise<number>
  abstract exists(...keys: string[]): Promise<number>
  abstract expire(key: string, seconds: number): Promise<0 | 1>
  abstract expireat(key: string, timestamp: number): Promise<0 | 1>
  abstract expiretime(key: string): Promise<number>
  abstract pexpire(key: string, milliseconds: number): Promise<0 | 1>
  abstract pexpireat(key: string, timestamp: number): Promise<0 | 1>
  abstract pexpiretime(key: string): Promise<number>
  abstract ttl(key: string): Promise<number>
  abstract pttl(key: string): Promise<number>
  abstract persist(key: string): Promise<0 | 1>
  abstract type(key: string): Promise<string>
  abstract rename(key: string, newkey: string): Promise<'OK'>
  abstract renamenx(key: string, newkey: string): Promise<0 | 1>
  abstract keys(pattern: string): Promise<string[]>
  abstract scan(cursor: number, options?: ScanOptions): Promise<[string, string[]]>
  abstract randomkey(): Promise<string | null>
  abstract dbsize(): Promise<number>
  abstract touch(...keys: string[]): Promise<number>
  abstract unlink(...keys: string[]): Promise<number>
  abstract copy(source: string, destination: string, replace?: boolean): Promise<0 | 1>

  // ─────────────────────────────────────────────────────────────
  // Hash Commands
  // ─────────────────────────────────────────────────────────────

  abstract hget(key: string, field: string): Promise<string | null>
  abstract hset(key: string, fieldValues: Record<string, string>): Promise<number>
  abstract hsetnx(key: string, field: string, value: string): Promise<0 | 1>
  abstract hmget(key: string, ...fields: string[]): Promise<(string | null)[]>
  abstract hmset(key: string, fieldValues: Record<string, string>): Promise<'OK'>
  abstract hgetall(key: string): Promise<Record<string, string>>
  abstract hdel(key: string, ...fields: string[]): Promise<number>
  abstract hexists(key: string, field: string): Promise<0 | 1>
  abstract hincrby(key: string, field: string, increment: number): Promise<number>
  abstract hincrbyfloat(key: string, field: string, increment: number): Promise<string>
  abstract hkeys(key: string): Promise<string[]>
  abstract hvals(key: string): Promise<string[]>
  abstract hlen(key: string): Promise<number>
  abstract hstrlen(key: string, field: string): Promise<number>
  abstract hscan(key: string, cursor: number, options?: ScanOptions): Promise<[string, string[]]>
  abstract hrandfield(key: string, count?: number, withValues?: boolean): Promise<string | string[] | null>

  // ─────────────────────────────────────────────────────────────
  // List Commands
  // ─────────────────────────────────────────────────────────────

  abstract lpush(key: string, ...elements: string[]): Promise<number>
  abstract rpush(key: string, ...elements: string[]): Promise<number>
  abstract lpushx(key: string, ...elements: string[]): Promise<number>
  abstract rpushx(key: string, ...elements: string[]): Promise<number>
  abstract lpop(key: string, count?: number): Promise<string | string[] | null>
  abstract rpop(key: string, count?: number): Promise<string | string[] | null>
  abstract llen(key: string): Promise<number>
  abstract lrange(key: string, start: number, stop: number): Promise<string[]>
  abstract lindex(key: string, index: number): Promise<string | null>
  abstract lset(key: string, index: number, element: string): Promise<'OK'>
  abstract linsert(key: string, position: 'BEFORE' | 'AFTER', pivot: string, element: string): Promise<number>
  abstract lrem(key: string, count: number, element: string): Promise<number>
  abstract ltrim(key: string, start: number, stop: number): Promise<'OK'>
  abstract lpos(key: string, element: string, options?: { rank?: number; count?: number; maxlen?: number }): Promise<number | number[] | null>
  abstract lmove(source: string, destination: string, whereFrom: 'LEFT' | 'RIGHT', whereTo: 'LEFT' | 'RIGHT'): Promise<string | null>

  // ─────────────────────────────────────────────────────────────
  // Set Commands
  // ─────────────────────────────────────────────────────────────

  abstract sadd(key: string, ...members: string[]): Promise<number>
  abstract srem(key: string, ...members: string[]): Promise<number>
  abstract smembers(key: string): Promise<string[]>
  abstract sismember(key: string, member: string): Promise<0 | 1>
  abstract smismember(key: string, ...members: string[]): Promise<(0 | 1)[]>
  abstract scard(key: string): Promise<number>
  abstract spop(key: string, count?: number): Promise<string | string[] | null>
  abstract srandmember(key: string, count?: number): Promise<string | string[] | null>
  abstract sdiff(...keys: string[]): Promise<string[]>
  abstract sdiffstore(destination: string, ...keys: string[]): Promise<number>
  abstract sinter(...keys: string[]): Promise<string[]>
  abstract sinterstore(destination: string, ...keys: string[]): Promise<number>
  abstract sintercard(numkeys: number, ...keysAndLimit: (string | number)[]): Promise<number>
  abstract sunion(...keys: string[]): Promise<string[]>
  abstract sunionstore(destination: string, ...keys: string[]): Promise<number>
  abstract smove(source: string, destination: string, member: string): Promise<0 | 1>
  abstract sscan(key: string, cursor: number, options?: ScanOptions): Promise<[string, string[]]>

  // ─────────────────────────────────────────────────────────────
  // Sorted Set Commands
  // ─────────────────────────────────────────────────────────────

  abstract zadd(key: string, scoreMembers: Array<{ score: number; member: string }>, options?: ZAddOptions): Promise<number>
  abstract zrem(key: string, ...members: string[]): Promise<number>
  abstract zscore(key: string, member: string): Promise<string | null>
  abstract zrank(key: string, member: string): Promise<number | null>
  abstract zrevrank(key: string, member: string): Promise<number | null>
  abstract zrange(key: string, start: number | string, stop: number | string, options?: ZRangeOptions): Promise<string[] | Array<{ member: string; score: number }>>
  abstract zrevrange(key: string, start: number, stop: number, withScores?: boolean): Promise<string[] | Array<{ member: string; score: number }>>
  abstract zrangebyscore(key: string, min: number | string, max: number | string, options?: { withScores?: boolean; limit?: { offset: number; count: number } }): Promise<string[] | Array<{ member: string; score: number }>>
  abstract zrevrangebyscore(key: string, max: number | string, min: number | string, options?: { withScores?: boolean; limit?: { offset: number; count: number } }): Promise<string[] | Array<{ member: string; score: number }>>
  abstract zrangebylex(key: string, min: string, max: string, options?: { limit?: { offset: number; count: number } }): Promise<string[]>
  abstract zrevrangebylex(key: string, max: string, min: string, options?: { limit?: { offset: number; count: number } }): Promise<string[]>
  abstract zcard(key: string): Promise<number>
  abstract zcount(key: string, min: number | string, max: number | string): Promise<number>
  abstract zlexcount(key: string, min: string, max: string): Promise<number>
  abstract zincrby(key: string, increment: number, member: string): Promise<string>
  abstract zpopmin(key: string, count?: number): Promise<Array<{ member: string; score: number }>>
  abstract zpopmax(key: string, count?: number): Promise<Array<{ member: string; score: number }>>
  abstract zrangestore(dst: string, src: string, min: number | string, max: number | string, options?: ZRangeOptions): Promise<number>
  abstract zunionstore(destination: string, numkeys: number, ...keysAndOptions: (string | number)[]): Promise<number>
  abstract zinterstore(destination: string, numkeys: number, ...keysAndOptions: (string | number)[]): Promise<number>
  abstract zrandmember(key: string, count?: number, withScores?: boolean): Promise<string | string[] | Array<{ member: string; score: number }> | null>
  abstract zmscore(key: string, ...members: string[]): Promise<(string | null)[]>
  abstract zscan(key: string, cursor: number, options?: ScanOptions): Promise<[string, Array<{ member: string; score: string }>]>

  // ─────────────────────────────────────────────────────────────
  // Stream Commands
  // ─────────────────────────────────────────────────────────────

  abstract xadd(key: string, id: string, fields: Record<string, string>, options?: { nomkstream?: boolean; maxlen?: number | { exact: boolean; value: number }; minid?: string }): Promise<string | null>
  abstract xread(streams: Array<{ key: string; id: string }>, options?: { count?: number; block?: number }): Promise<Array<{ stream: string; messages: Array<{ id: string; fields: Record<string, string> }> }> | null>
  abstract xrange(key: string, start: string, end: string, count?: number): Promise<Array<{ id: string; fields: Record<string, string> }>>
  abstract xrevrange(key: string, end: string, start: string, count?: number): Promise<Array<{ id: string; fields: Record<string, string> }>>
  abstract xlen(key: string): Promise<number>
  abstract xtrim(key: string, options: { maxlen?: number | { exact: boolean; value: number }; minid?: string }): Promise<number>
  abstract xdel(key: string, ...ids: string[]): Promise<number>

  // ─────────────────────────────────────────────────────────────
  // Pub/Sub Commands
  // ─────────────────────────────────────────────────────────────

  abstract publish(channel: string, message: string): Promise<number>

  // ─────────────────────────────────────────────────────────────
  // Transaction Commands
  // ─────────────────────────────────────────────────────────────

  abstract multi(): Promise<'OK'>
  abstract exec(): Promise<unknown[] | null>
  abstract discard(): Promise<'OK'>

  // ─────────────────────────────────────────────────────────────
  // Script Commands
  // ─────────────────────────────────────────────────────────────

  abstract eval(script: string, numkeys: number, ...keysAndArgs: string[]): Promise<unknown>
  abstract evalsha(sha1: string, numkeys: number, ...keysAndArgs: string[]): Promise<unknown>

  // ─────────────────────────────────────────────────────────────
  // Server Commands
  // ─────────────────────────────────────────────────────────────

  abstract info(section?: string): Promise<string>
  abstract time(): Promise<[string, string]>
  abstract flushdb(): Promise<'OK'>
  abstract flushall(): Promise<'OK'>

  // ─────────────────────────────────────────────────────────────
  // Geo Commands
  // ─────────────────────────────────────────────────────────────

  abstract geoadd(key: string, members: Array<{ longitude: number; latitude: number; member: string }>, options?: { nx?: boolean; xx?: boolean; ch?: boolean }): Promise<number>
  abstract geodist(key: string, member1: string, member2: string, unit?: 'm' | 'km' | 'mi' | 'ft'): Promise<string | null>
  abstract geohash(key: string, ...members: string[]): Promise<(string | null)[]>
  abstract geopos(key: string, ...members: string[]): Promise<([string, string] | null)[]>
  abstract geosearch(key: string, options: { frommember?: string; fromlonlat?: { longitude: number; latitude: number }; byradius?: { radius: number; unit: 'm' | 'km' | 'mi' | 'ft' }; bybox?: { width: number; height: number; unit: 'm' | 'km' | 'mi' | 'ft' }; count?: number; any?: boolean; asc?: boolean; desc?: boolean; withcoord?: boolean; withdist?: boolean; withhash?: boolean }): Promise<string[] | Array<{ member: string; coord?: [string, string]; dist?: string; hash?: number }>>

  // ─────────────────────────────────────────────────────────────
  // HyperLogLog Commands
  // ─────────────────────────────────────────────────────────────

  abstract pfadd(key: string, ...elements: string[]): Promise<0 | 1>
  abstract pfcount(...keys: string[]): Promise<number>
  abstract pfmerge(destkey: string, ...sourcekeys: string[]): Promise<'OK'>

  // ─────────────────────────────────────────────────────────────
  // Bitmap Commands
  // ─────────────────────────────────────────────────────────────

  abstract setbit(key: string, offset: number, value: 0 | 1): Promise<0 | 1>
  abstract getbit(key: string, offset: number): Promise<0 | 1>
  abstract bitcount(key: string, start?: number, end?: number, mode?: 'BYTE' | 'BIT'): Promise<number>
  abstract bitop(operation: 'AND' | 'OR' | 'XOR' | 'NOT', destkey: string, ...keys: string[]): Promise<number>
  abstract bitpos(key: string, bit: 0 | 1, start?: number, end?: number, mode?: 'BYTE' | 'BIT'): Promise<number>
}

// ─────────────────────────────────────────────────────────────────
// Command Categories (for documentation and tooling)
// ─────────────────────────────────────────────────────────────────

export const REDIS_COMMAND_CATEGORIES = {
  string: ['get', 'set', 'getex', 'getdel', 'mget', 'mset', 'msetnx', 'setnx', 'setex', 'psetex', 'append', 'strlen', 'incr', 'incrby', 'incrbyfloat', 'decr', 'decrby', 'getrange', 'setrange', 'getset'],
  key: ['del', 'exists', 'expire', 'expireat', 'expiretime', 'pexpire', 'pexpireat', 'pexpiretime', 'ttl', 'pttl', 'persist', 'type', 'rename', 'renamenx', 'keys', 'scan', 'randomkey', 'dbsize', 'touch', 'unlink', 'copy'],
  hash: ['hget', 'hset', 'hsetnx', 'hmget', 'hmset', 'hgetall', 'hdel', 'hexists', 'hincrby', 'hincrbyfloat', 'hkeys', 'hvals', 'hlen', 'hstrlen', 'hscan', 'hrandfield'],
  list: ['lpush', 'rpush', 'lpushx', 'rpushx', 'lpop', 'rpop', 'llen', 'lrange', 'lindex', 'lset', 'linsert', 'lrem', 'ltrim', 'lpos', 'lmove'],
  set: ['sadd', 'srem', 'smembers', 'sismember', 'smismember', 'scard', 'spop', 'srandmember', 'sdiff', 'sdiffstore', 'sinter', 'sinterstore', 'sintercard', 'sunion', 'sunionstore', 'smove', 'sscan'],
  zset: ['zadd', 'zrem', 'zscore', 'zrank', 'zrevrank', 'zrange', 'zrevrange', 'zrangebyscore', 'zrevrangebyscore', 'zrangebylex', 'zrevrangebylex', 'zcard', 'zcount', 'zlexcount', 'zincrby', 'zpopmin', 'zpopmax', 'zrangestore', 'zunionstore', 'zinterstore', 'zrandmember', 'zmscore', 'zscan'],
  stream: ['xadd', 'xread', 'xrange', 'xrevrange', 'xlen', 'xtrim', 'xdel'],
  pubsub: ['publish'],
  transaction: ['multi', 'exec', 'discard'],
  script: ['eval', 'evalsha'],
  server: ['ping', 'echo', 'info', 'time', 'flushdb', 'flushall', 'dbsize'],
  geo: ['geoadd', 'geodist', 'geohash', 'geopos', 'geosearch'],
  hyperloglog: ['pfadd', 'pfcount', 'pfmerge'],
  bitmap: ['setbit', 'getbit', 'bitcount', 'bitop', 'bitpos'],
} as const

/**
 * Read-only commands that don't modify data
 */
export const READ_ONLY_COMMANDS = new Set([
  'ping', 'echo', 'get', 'mget', 'strlen', 'getrange', 'exists', 'ttl', 'pttl',
  'type', 'keys', 'scan', 'randomkey', 'dbsize', 'hget', 'hmget', 'hgetall',
  'hexists', 'hkeys', 'hvals', 'hlen', 'hstrlen', 'hscan', 'hrandfield',
  'llen', 'lrange', 'lindex', 'lpos', 'sismember', 'smismember', 'smembers',
  'scard', 'srandmember', 'sdiff', 'sinter', 'sunion', 'sscan', 'zscore',
  'zrank', 'zrevrank', 'zrange', 'zrevrange', 'zrangebyscore', 'zrevrangebyscore',
  'zrangebylex', 'zrevrangebylex', 'zcard', 'zcount', 'zlexcount', 'zrandmember',
  'zmscore', 'zscan', 'xread', 'xrange', 'xrevrange', 'xlen', 'info', 'time',
  'geodist', 'geohash', 'geopos', 'geosearch', 'pfcount', 'getbit', 'bitcount', 'bitpos',
  'expiretime', 'pexpiretime',
])

/**
 * Check if a command is read-only
 */
export function isReadOnlyCommand(command: string): boolean {
  return READ_ONLY_COMMANDS.has(command.toLowerCase())
}
