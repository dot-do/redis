/**
 * Redois Entrypoint
 *
 * Main worker entrypoint that handles:
 * - RPC requests at /rpc
 * - MCP requests at /mcp
 * - Upstash-compatible REST API
 * - Health checks
 */

import type { Env, SetOptions, ScanOptions } from './types'
import { createMcpServer } from './mcp/server'
import { createHttpMcpHandler } from './mcp/transport/http'
import { createWorkerEvaluator } from './mcp/sandbox/worker-evaluator'
import { createRpcHandler, type RpcContext } from './rpc/endpoint'
import { handleUpstashRest } from './rest/upstash'

export class RedoisEntrypoint {
  private ctx: ExecutionContext
  private env: Env
  private mcpHandler: ((request: Request) => Promise<Response>) | null = null
  private rpcHandler: ReturnType<typeof createRpcHandler> | null = null

  constructor(ctx: ExecutionContext, env: Env) {
    this.ctx = ctx
    this.env = env
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)

    // Health check
    if (url.pathname === '/health') {
      return Response.json({ status: 'ok', version: '0.1.0' })
    }

    // MCP endpoint
    if (url.pathname === '/mcp' || url.pathname.startsWith('/mcp/')) {
      return this.handleMcp(request)
    }

    // RPC endpoint (capnweb-style batch protocol)
    if (url.pathname === '/rpc' || url.pathname.startsWith('/rpc/')) {
      return this.handleRpc(request)
    }

    // Upstash-compatible REST API
    return this.handleRest(request, url)
  }

  private async handleMcp(request: Request): Promise<Response> {
    if (!this.mcpHandler) {
      const redisAccess = this.createRedisAccess()

      // Create code loader if Worker Loader is available
      const codeLoader = this.env.LOADER
        ? {
            execute: async (code: string, _context?: Record<string, unknown>) => {
              // Create a RedisProxy from RedisAccess for the evaluator
              const redisProxy = {
                get: redisAccess.get,
                set: async (key: string, value: string, opts?: SetOptions) => {
                  const result = await redisAccess.set(key, value, opts)
                  return result ?? 'OK'
                },
                del: redisAccess.del,
                exists: redisAccess.exists,
                expire: async (key: string, seconds: number) => {
                  return (await this.getShardForKey(key).expire(key, seconds)) as number
                },
                ttl: async (key: string) => {
                  return (await this.getShardForKey(key).ttl(key)) as number
                },
                keys: redisAccess.keys,
                scan: async (cursor: number, opts?: ScanOptions) => {
                  const result = await redisAccess.scan(cursor, opts)
                  return [String(result[0]), result[1]] as [string, string[]]
                },
                hget: redisAccess.hget,
                hset: redisAccess.hset,
                hgetall: redisAccess.hgetall,
                hdel: async (key: string, ...fields: string[]) => {
                  return (await this.getShardForKey(key).hdel(key, ...fields)) as number
                },
                lpush: redisAccess.lpush,
                rpush: async (key: string, ...values: string[]) => {
                  return (await this.getShardForKey(key).rpush(key, ...values)) as number
                },
                lpop: async (key: string) => {
                  return (await this.getShardForKey(key).lpop(key)) as string | null
                },
                rpop: async (key: string) => {
                  return (await this.getShardForKey(key).rpop(key)) as string | null
                },
                lrange: redisAccess.lrange,
                sadd: redisAccess.sadd,
                smembers: redisAccess.smembers,
                srem: async (key: string, ...members: string[]) => {
                  return (await this.getShardForKey(key).srem(key, ...members)) as number
                },
                sismember: async (key: string, member: string) => {
                  return (await this.getShardForKey(key).sismember(key, member)) as number
                },
                zadd: async (key: string, score: number, member: string) => {
                  return (await this.getShardForKey(key).zadd(key, score, member)) as number
                },
                zrange: redisAccess.zrange,
                zrem: async (key: string, ...members: string[]) => {
                  return (await this.getShardForKey(key).zrem(key, ...members)) as number
                },
                zscore: async (key: string, member: string) => {
                  return (await this.getShardForKey(key).zscore(key, member)) as string | null
                },
                incr: async (key: string) => {
                  return (await this.getShardForKey(key).incr(key)) as number
                },
                incrby: async (key: string, increment: number) => {
                  return (await this.getShardForKey(key).incrby(key, increment)) as number
                },
                decr: async (key: string) => {
                  return (await this.getShardForKey(key).decr(key)) as number
                },
                decrby: async (key: string, decrement: number) => {
                  return (await this.getShardForKey(key).decrby(key, decrement)) as number
                },
                append: async (key: string, value: string) => {
                  return (await this.getShardForKey(key).append(key, value)) as number
                },
                strlen: async (key: string) => {
                  return (await this.getShardForKey(key).strlen(key)) as number
                },
                type: async (key: string) => {
                  return (await this.getShardForKey(key).type(key)) as string
                },
                rename: async (_key: string, _newKey: string) => 'OK' as const,
                dbsize: async () => await this.dbsize(),
                flushdb: async () => await this.flushdb(),
                info: async (_section?: string) => 'Redois Server v0.1.0',
                ping: async (message?: string) => message ?? 'PONG',
              }
              const evaluator = createWorkerEvaluator(
                this.env.LOADER!,
                redisProxy,
                { timeout: 30000 }
              )
              return evaluator.execute(code)
            },
          }
        : undefined

      const server = createMcpServer({
        redisAccess,
        codeLoader,
        name: 'redois',
        version: '0.1.0',
      })

      this.mcpHandler = createHttpMcpHandler(server, {
        authenticate: this.env.AUTH_TOKEN
          ? async (req) => {
              const auth = req.headers.get('Authorization')
              return { authenticated: auth === `Bearer ${this.env.AUTH_TOKEN}` }
            }
          : undefined,
      })
    }

    return this.mcpHandler(request)
  }

  private async handleRpc(request: Request): Promise<Response> {
    if (!this.rpcHandler) {
      this.rpcHandler = createRpcHandler()
    }
    // Create a Redis RPC target from this entrypoint's methods
    const target = this.createRedisRpcTarget()
    const context: RpcContext = { env: this.env, ctx: this.ctx }
    return this.rpcHandler(request, target, context)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private createRedisRpcTarget(): any {
    const self = this
    return {
      get: (key: string) => self.getShardForKey(key).get(key) as Promise<string | null>,
      set: (key: string, value: string, opts?: SetOptions) =>
        self.getShardForKey(key).set(key, value, opts) as Promise<string | null>,
      del: (...keys: string[]) => self.delMultiple(keys),
      exists: (...keys: string[]) => self.existsMultiple(keys),
      incr: (key: string) => self.getShardForKey(key).incr(key) as Promise<number>,
      decr: (key: string) => self.getShardForKey(key).decr(key) as Promise<number>,
      incrby: (key: string, increment: number) => self.getShardForKey(key).incrby(key, increment) as Promise<number>,
      decrby: (key: string, decrement: number) => self.getShardForKey(key).decrby(key, decrement) as Promise<number>,
      append: (key: string, value: string) => self.getShardForKey(key).append(key, value) as Promise<number>,
      strlen: (key: string) => self.getShardForKey(key).strlen(key) as Promise<number>,
      hget: (key: string, field: string) => self.getShardForKey(key).hget(key, field) as Promise<string | null>,
      hset: (key: string, ...fieldValues: string[]) => self.getShardForKey(key).hset(key, ...fieldValues) as Promise<number>,
      hmget: (key: string, ...fields: string[]) => self.getShardForKey(key).hmget(key, ...fields) as Promise<(string | null)[]>,
      hgetall: (key: string) => self.getShardForKey(key).hgetall(key) as Promise<Record<string, string>>,
      hdel: (key: string, ...fields: string[]) => self.getShardForKey(key).hdel(key, ...fields) as Promise<number>,
      hexists: (key: string, field: string) => self.getShardForKey(key).hexists(key, field) as Promise<number>,
      hkeys: (key: string) => self.getShardForKey(key).hkeys(key) as Promise<string[]>,
      hvals: (key: string) => self.getShardForKey(key).hvals(key) as Promise<string[]>,
      hlen: (key: string) => self.getShardForKey(key).hlen(key) as Promise<number>,
      hincrby: (key: string, field: string, increment: number) =>
        self.getShardForKey(key).hincrby(key, field, increment) as Promise<number>,
      lpush: (key: string, ...values: string[]) => self.getShardForKey(key).lpush(key, ...values) as Promise<number>,
      rpush: (key: string, ...values: string[]) => self.getShardForKey(key).rpush(key, ...values) as Promise<number>,
      lpop: (key: string, count?: number) => self.getShardForKey(key).lpop(key, count) as Promise<string | string[] | null>,
      rpop: (key: string, count?: number) => self.getShardForKey(key).rpop(key, count) as Promise<string | string[] | null>,
      lrange: (key: string, start: number, stop: number) =>
        self.getShardForKey(key).lrange(key, start, stop) as Promise<string[]>,
      llen: (key: string) => self.getShardForKey(key).llen(key) as Promise<number>,
      lindex: (key: string, index: number) => self.getShardForKey(key).lindex(key, index) as Promise<string | null>,
      lset: (key: string, index: number, value: string) =>
        self.getShardForKey(key).lset(key, index, value) as Promise<string>,
      sadd: (key: string, ...members: string[]) => self.getShardForKey(key).sadd(key, ...members) as Promise<number>,
      srem: (key: string, ...members: string[]) => self.getShardForKey(key).srem(key, ...members) as Promise<number>,
      smembers: (key: string) => self.getShardForKey(key).smembers(key) as Promise<string[]>,
      sismember: (key: string, member: string) => self.getShardForKey(key).sismember(key, member) as Promise<number>,
      scard: (key: string) => self.getShardForKey(key).scard(key) as Promise<number>,
      spop: (key: string, count?: number) =>
        self.getShardForKey(key).spop(key, count) as Promise<string | string[] | null>,
      zadd: (key: string, ...args: (string | number)[]) =>
        self.getShardForKey(key).zadd(key, ...args) as Promise<number>,
      zrem: (key: string, ...members: string[]) => self.getShardForKey(key).zrem(key, ...members) as Promise<number>,
      zscore: (key: string, member: string) => self.getShardForKey(key).zscore(key, member) as Promise<string | null>,
      zrank: (key: string, member: string) => self.getShardForKey(key).zrank(key, member) as Promise<number | null>,
      zrange: (key: string, start: number, stop: number) =>
        self.getShardForKey(key).zrange(key, start, stop) as Promise<string[]>,
      zcard: (key: string) => self.getShardForKey(key).zcard(key) as Promise<number>,
      expire: (key: string, seconds: number) => self.getShardForKey(key).expire(key, seconds) as Promise<number>,
      ttl: (key: string) => self.getShardForKey(key).ttl(key) as Promise<number>,
      pttl: (key: string) => self.getShardForKey(key).pttl(key) as Promise<number>,
      persist: (key: string) => self.getShardForKey(key).persist(key) as Promise<number>,
      type: (key: string) => self.getShardForKey(key).type(key) as Promise<string>,
      keys: (pattern: string) => self.keys(pattern),
      scan: (cursor: number, opts?: ScanOptions) => self.scan(cursor, opts || {}),
      dbsize: () => self.dbsize(),
      flushdb: () => self.flushdb(),
      publish: (channel: string, message: string) => self.publish(channel, message),
      ping: (message?: string) => Promise.resolve(message || 'PONG'),
    }
  }

  private async handleRest(request: Request, url: URL): Promise<Response> {
    // Use the Upstash-compatible REST API handler
    return handleUpstashRest(request, url, {
      env: this.env,
      executeCommand: (cmd: string, args: string[]) => this.executeCommand(cmd, args),
    })
  }

  private async executeCommand(cmd: string, args: string[]): Promise<unknown> {
    const key = args[0]
    const shard = this.getShardForKey(key)

    switch (cmd) {
      // String commands
      case 'GET':
        return shard.get(args[0])
      case 'SET':
        return shard.set(args[0], args[1], this.parseSetOptions(args.slice(2)))
      case 'MGET':
        return Promise.all(args.map((k) => this.getShardForKey(k).get(k)))
      case 'INCR':
        return shard.incr(args[0])
      case 'DECR':
        return shard.decr(args[0])
      case 'INCRBY':
        return shard.incrby(args[0], parseInt(args[1]))
      case 'DECRBY':
        return shard.decrby(args[0], parseInt(args[1]))
      case 'APPEND':
        return shard.append(args[0], args[1])
      case 'STRLEN':
        return shard.strlen(args[0])

      // Hash commands
      case 'HGET':
        return shard.hget(args[0], args[1])
      case 'HSET':
        return shard.hset(args[0], ...args.slice(1))
      case 'HMGET':
        return shard.hmget(args[0], ...args.slice(1))
      case 'HGETALL':
        return shard.hgetall(args[0])
      case 'HDEL':
        return shard.hdel(args[0], ...args.slice(1))
      case 'HEXISTS':
        return shard.hexists(args[0], args[1])
      case 'HKEYS':
        return shard.hkeys(args[0])
      case 'HVALS':
        return shard.hvals(args[0])
      case 'HLEN':
        return shard.hlen(args[0])
      case 'HINCRBY':
        return shard.hincrby(args[0], args[1], parseInt(args[2]))

      // List commands
      case 'LPUSH':
        return shard.lpush(args[0], ...args.slice(1))
      case 'RPUSH':
        return shard.rpush(args[0], ...args.slice(1))
      case 'LPOP':
        return shard.lpop(args[0], args[1] ? parseInt(args[1]) : undefined)
      case 'RPOP':
        return shard.rpop(args[0], args[1] ? parseInt(args[1]) : undefined)
      case 'LRANGE':
        return shard.lrange(args[0], parseInt(args[1]), parseInt(args[2]))
      case 'LLEN':
        return shard.llen(args[0])
      case 'LINDEX':
        return shard.lindex(args[0], parseInt(args[1]))
      case 'LSET':
        return shard.lset(args[0], parseInt(args[1]), args[2])

      // Set commands
      case 'SADD':
        return shard.sadd(args[0], ...args.slice(1))
      case 'SREM':
        return shard.srem(args[0], ...args.slice(1))
      case 'SMEMBERS':
        return shard.smembers(args[0])
      case 'SISMEMBER':
        return shard.sismember(args[0], args[1])
      case 'SCARD':
        return shard.scard(args[0])
      case 'SPOP':
        return shard.spop(args[0], args[1] ? parseInt(args[1]) : undefined)

      // Sorted Set commands
      case 'ZADD':
        return shard.zadd(args[0], ...args.slice(1))
      case 'ZREM':
        return shard.zrem(args[0], ...args.slice(1))
      case 'ZSCORE':
        return shard.zscore(args[0], args[1])
      case 'ZRANK':
        return shard.zrank(args[0], args[1])
      case 'ZRANGE':
        return shard.zrange(args[0], parseInt(args[1]), parseInt(args[2]))
      case 'ZCARD':
        return shard.zcard(args[0])

      // Key commands
      case 'DEL':
        return this.delMultiple(args)
      case 'EXISTS':
        return this.existsMultiple(args)
      case 'EXPIRE':
        return shard.expire(args[0], parseInt(args[1]))
      case 'TTL':
        return shard.ttl(args[0])
      case 'PTTL':
        return shard.pttl(args[0])
      case 'PERSIST':
        return shard.persist(args[0])
      case 'TYPE':
        return shard.type(args[0])
      case 'KEYS':
        return this.keys(args[0])
      case 'SCAN':
        return this.scan(parseInt(args[0]), this.parseScanOptions(args.slice(1)))

      // Pub/Sub
      case 'PUBLISH':
        return this.publish(args[0], args[1])

      // Server
      case 'PING':
        return args[0] || 'PONG'
      case 'ECHO':
        return args[0]
      case 'DBSIZE':
        return this.dbsize()
      case 'FLUSHDB':
        return this.flushdb()

      default:
        throw new Error(`ERR unknown command '${cmd}'`)
    }
  }

  private getShardForKey(key: string): RedisShardStub {
    const hash = this.hashKey(key)
    const shardId = this.env.REDIS_SHARDS.idFromName(`shard-${hash % 256}`)
    return this.env.REDIS_SHARDS.get(shardId) as unknown as RedisShardStub
  }

  private hashKey(key: string): number {
    let hash = 0
    for (let i = 0; i < key.length; i++) {
      const char = key.charCodeAt(i)
      hash = (hash << 5) - hash + char
      hash = hash & hash
    }
    return Math.abs(hash)
  }

  private parseSetOptions(args: string[]): SetOptions {
    const opts: SetOptions = {}
    for (let i = 0; i < args.length; i++) {
      const arg = args[i]?.toUpperCase()
      switch (arg) {
        case 'EX':
          opts.ex = parseInt(args[++i])
          break
        case 'PX':
          opts.px = parseInt(args[++i])
          break
        case 'EXAT':
          opts.exat = parseInt(args[++i])
          break
        case 'PXAT':
          opts.pxat = parseInt(args[++i])
          break
        case 'NX':
          opts.nx = true
          break
        case 'XX':
          opts.xx = true
          break
        case 'KEEPTTL':
          opts.keepttl = true
          break
        case 'GET':
          opts.get = true
          break
      }
    }
    return opts
  }

  private parseScanOptions(args: string[]): ScanOptions {
    const opts: ScanOptions = {}
    for (let i = 0; i < args.length; i++) {
      const arg = args[i]?.toUpperCase()
      switch (arg) {
        case 'MATCH':
          opts.match = args[++i]
          break
        case 'COUNT':
          opts.count = parseInt(args[++i])
          break
        case 'TYPE':
          opts.type = args[++i]
          break
      }
    }
    return opts
  }

  private async delMultiple(keys: string[]): Promise<number> {
    const results = await Promise.all(keys.map((k) => this.getShardForKey(k).del(k)))
    return results.reduce<number>((a, b) => a + (b as number), 0)
  }

  private async existsMultiple(keys: string[]): Promise<number> {
    const results = await Promise.all(keys.map((k) => this.getShardForKey(k).exists(k)))
    return results.reduce<number>((a, b) => a + (b as number), 0)
  }

  private async keys(pattern: string): Promise<string[]> {
    // For now, query all shards (not efficient for production)
    const allKeys: string[] = []
    for (let i = 0; i < 256; i++) {
      const shardId = this.env.REDIS_SHARDS.idFromName(`shard-${i}`)
      const shard = this.env.REDIS_SHARDS.get(shardId) as unknown as RedisShardStub
      const keys = (await shard.keys(pattern)) as string[]
      allKeys.push(...keys)
    }
    return allKeys
  }

  private async scan(cursor: number, opts: ScanOptions): Promise<[number, string[]]> {
    // Simplified scan - in production would need proper cursor handling
    const keys = await this.keys(opts.match || '*')
    const count = opts.count || 10
    const start = cursor
    const end = Math.min(start + count, keys.length)
    const nextCursor = end >= keys.length ? 0 : end
    return [nextCursor, keys.slice(start, end)]
  }

  private async publish(channel: string, message: string): Promise<number> {
    const pubsubId = this.env.REDIS_PUBSUB.idFromName('global')
    const pubsub = this.env.REDIS_PUBSUB.get(pubsubId) as unknown as RedisShardStub
    return (await pubsub.publish(channel, message)) as number
  }

  private async dbsize(): Promise<number> {
    // Sum keys across all shards
    let total = 0
    for (let i = 0; i < 256; i++) {
      const shardId = this.env.REDIS_SHARDS.idFromName(`shard-${i}`)
      const shard = this.env.REDIS_SHARDS.get(shardId) as unknown as RedisShardStub
      total += (await shard.dbsize()) as number
    }
    return total
  }

  private async flushdb(): Promise<'OK'> {
    // Flush all shards
    const promises: Promise<unknown>[] = []
    for (let i = 0; i < 256; i++) {
      const shardId = this.env.REDIS_SHARDS.idFromName(`shard-${i}`)
      const shard = this.env.REDIS_SHARDS.get(shardId) as unknown as RedisShardStub
      promises.push(shard.flushdb())
    }
    await Promise.all(promises)
    return 'OK'
  }

  private createRedisAccess() {
    const self = this
    return {
      get: (key: string) => self.getShardForKey(key).get(key) as Promise<string | null>,
      set: (key: string, value: string, opts?: SetOptions) =>
        self.getShardForKey(key).set(key, value, opts) as Promise<'OK' | null>,
      del: (...keys: string[]) => self.delMultiple(keys),
      exists: (...keys: string[]) => self.existsMultiple(keys),
      keys: (pattern: string) => self.keys(pattern),
      scan: (cursor: number, opts?: ScanOptions) => self.scan(cursor, opts || {}),
      hget: (key: string, field: string) => self.getShardForKey(key).hget(key, field) as Promise<string | null>,
      hset: (key: string, ...fieldValues: string[]) =>
        self.getShardForKey(key).hset(key, ...fieldValues) as Promise<number>,
      hgetall: (key: string) => self.getShardForKey(key).hgetall(key) as Promise<Record<string, string>>,
      lpush: (key: string, ...values: string[]) =>
        self.getShardForKey(key).lpush(key, ...values) as Promise<number>,
      rpush: (key: string, ...values: string[]) =>
        self.getShardForKey(key).rpush(key, ...values) as Promise<number>,
      lrange: (key: string, start: number, stop: number) =>
        self.getShardForKey(key).lrange(key, start, stop) as Promise<string[]>,
      sadd: (key: string, ...members: string[]) =>
        self.getShardForKey(key).sadd(key, ...members) as Promise<number>,
      smembers: (key: string) => self.getShardForKey(key).smembers(key) as Promise<string[]>,
      zadd: (key: string, ...args: (string | number)[]) =>
        self.getShardForKey(key).zadd(key, ...args) as Promise<number>,
      zrange: (key: string, start: number, stop: number) =>
        self.getShardForKey(key).zrange(key, start, stop) as Promise<string[]>,
      publish: (channel: string, message: string) => self.publish(channel, message),
      getProxy: () => self.createRedisAccess(),
    }
  }
}

// Type declaration for RedisShard Durable Object methods (via RPC)
interface RedisShardStub {
  get(key: string): Promise<unknown>
  set(key: string, value: string, opts?: SetOptions): Promise<unknown>
  del(...keys: string[]): Promise<unknown>
  exists(...keys: string[]): Promise<unknown>
  incr(key: string): Promise<unknown>
  decr(key: string): Promise<unknown>
  incrby(key: string, increment: number): Promise<unknown>
  decrby(key: string, decrement: number): Promise<unknown>
  append(key: string, value: string): Promise<unknown>
  strlen(key: string): Promise<unknown>
  hget(key: string, field: string): Promise<unknown>
  hset(key: string, ...fieldValues: string[]): Promise<unknown>
  hmget(key: string, ...fields: string[]): Promise<unknown>
  hgetall(key: string): Promise<unknown>
  hdel(key: string, ...fields: string[]): Promise<unknown>
  hexists(key: string, field: string): Promise<unknown>
  hkeys(key: string): Promise<unknown>
  hvals(key: string): Promise<unknown>
  hlen(key: string): Promise<unknown>
  hincrby(key: string, field: string, increment: number): Promise<unknown>
  lpush(key: string, ...values: string[]): Promise<unknown>
  rpush(key: string, ...values: string[]): Promise<unknown>
  lpop(key: string, count?: number): Promise<unknown>
  rpop(key: string, count?: number): Promise<unknown>
  lrange(key: string, start: number, stop: number): Promise<unknown>
  llen(key: string): Promise<unknown>
  lindex(key: string, index: number): Promise<unknown>
  lset(key: string, index: number, value: string): Promise<unknown>
  sadd(key: string, ...members: string[]): Promise<unknown>
  srem(key: string, ...members: string[]): Promise<unknown>
  smembers(key: string): Promise<unknown>
  sismember(key: string, member: string): Promise<unknown>
  scard(key: string): Promise<unknown>
  spop(key: string, count?: number): Promise<unknown>
  zadd(key: string, ...args: (string | number)[]): Promise<unknown>
  zrem(key: string, ...members: string[]): Promise<unknown>
  zscore(key: string, member: string): Promise<unknown>
  zrank(key: string, member: string): Promise<unknown>
  zrange(key: string, start: number, stop: number): Promise<unknown>
  zcard(key: string): Promise<unknown>
  expire(key: string, seconds: number): Promise<unknown>
  ttl(key: string): Promise<unknown>
  pttl(key: string): Promise<unknown>
  persist(key: string): Promise<unknown>
  type(key: string): Promise<unknown>
  keys(pattern: string): Promise<unknown>
  dbsize(): Promise<unknown>
  flushdb(): Promise<unknown>
  publish(channel: string, message: string): Promise<unknown>
}
