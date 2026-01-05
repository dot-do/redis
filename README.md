# Redis.do

<!-- TODO: Add logo/banner -->
<!-- <p align="center">
  <img src="./assets/logo.svg" alt="Redis.do Logo" width="200" />
</p> -->

<p align="center">
  <strong>100% Redis-compatible database on Cloudflare Workers</strong>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/redis.do"><img src="https://img.shields.io/npm/v/redis.do.svg" alt="npm version" /></a>
  <a href="https://www.npmjs.com/package/redis.do"><img src="https://img.shields.io/npm/dm/redis.do.svg" alt="npm downloads" /></a>
  <a href="https://github.com/dot-do/redis/blob/main/LICENSE"><img src="https://img.shields.io/npm/l/redis.do.svg" alt="license" /></a>
</p>

---

Redis.do is a fully Redis-compatible database built on Cloudflare Workers, Durable Objects, and SQLite. Deploy your own Redis instance at the edge with zero operational overhead.

## Features

- **100% Redis Protocol Compatible** - Drop-in replacement for Redis with support for all major data types
- **Edge-Native** - Runs on Cloudflare's global network with sub-millisecond latency worldwide
- **Durable Storage** - Data persisted in SQLite-backed Durable Objects for reliability
- **Serverless** - No servers to manage, scales automatically with demand
- **MCP Integration** - Built-in Model Context Protocol server for AI agent integration
- **Upstash-Compatible REST API** - Use existing Upstash clients seamlessly
- **ioredis-Compatible Client** - Familiar API with pipelining support
- **Secure Code Execution** - Sandboxed Worker Loader for running custom code safely

### Supported Commands

**Strings**: GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, APPEND, STRLEN, SETEX, SETNX, GETSET

**Hashes**: HGET, HSET, HMGET, HMSET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY

**Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LREM, LTRIM, LINSERT

**Sets**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SDIFF, SINTER, SUNION

**Sorted Sets**: ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZREVRANGE, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX

**Keys**: DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, TYPE, KEYS, SCAN, RENAME

**Server**: PING, ECHO, DBSIZE, FLUSHDB, INFO

**Pub/Sub**: PUBLISH (subscribe via WebSocket)

## Quick Start

### 1. Install

```bash
npm install redis.do
```

### 2. Deploy to Cloudflare

Clone and deploy your own Redis.do instance:

```bash
git clone https://github.com/dot-do/redis.git
cd redis.do
npm install
npm run deploy
```

### 3. Connect

```typescript
import { Redis } from 'redis.do/client'

const redis = new Redis({
  url: 'https://your-redis.do.workers.dev',
  token: 'your-auth-token' // optional
})

// Use like any Redis client
await redis.set('key', 'value')
const value = await redis.get('key')
```

## Installation

```bash
# npm
npm install redis.do

# pnpm
pnpm add redis.do

# yarn
yarn add redis.do
```

## Usage

### Basic Operations

```typescript
import { Redis } from 'redis.do/client'

const redis = new Redis({ url: 'https://your-redis.do.workers.dev' })

// Strings
await redis.set('name', 'Alice')
await redis.set('session', 'abc123', { ex: 3600 }) // expires in 1 hour
const name = await redis.get('name') // 'Alice'

// Counters
await redis.set('views', '0')
await redis.incr('views') // 1
await redis.incrby('views', 10) // 11

// Hashes
await redis.hset('user:1', 'name', 'Alice', 'email', 'alice@example.com')
const user = await redis.hgetall('user:1')
// { name: 'Alice', email: 'alice@example.com' }

// Lists
await redis.lpush('queue', 'job1', 'job2', 'job3')
const job = await redis.rpop('queue') // 'job1'
const jobs = await redis.lrange('queue', 0, -1) // ['job3', 'job2']

// Sets
await redis.sadd('tags', 'redis', 'cloudflare', 'serverless')
const isMember = await redis.sismember('tags', 'redis') // 1
const tags = await redis.smembers('tags')

// Sorted Sets
await redis.zadd('leaderboard', 100, 'alice', 200, 'bob', 150, 'charlie')
const top = await redis.zrevrange('leaderboard', 0, 2) // ['bob', 'charlie', 'alice']
```

### Pipelining

Batch multiple commands for efficient execution:

```typescript
const pipeline = redis.pipeline()
pipeline.set('key1', 'value1')
pipeline.set('key2', 'value2')
pipeline.get('key1')
pipeline.get('key2')
pipeline.incr('counter')

const results = await pipeline.exec()
// [[null, 'OK'], [null, 'OK'], [null, 'value1'], [null, 'value2'], [null, 1]]
```

### Key Expiration

```typescript
// Set with expiration
await redis.set('temp', 'data', { ex: 60 }) // expires in 60 seconds
await redis.set('temp2', 'data', { px: 5000 }) // expires in 5000ms

// Set expiration on existing key
await redis.expire('mykey', 300) // expires in 5 minutes

// Check TTL
const ttl = await redis.ttl('mykey') // seconds remaining
const pttl = await redis.pttl('mykey') // milliseconds remaining

// Remove expiration
await redis.persist('mykey')
```

### REST API (Upstash-Compatible)

Redis.do exposes an Upstash-compatible REST API:

```bash
# GET request
curl https://your-redis.do.workers.dev/GET/mykey

# POST request with JSON array
curl -X POST https://your-redis.do.workers.dev \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '["SET", "mykey", "myvalue"]'
```

## API Overview

### Client API

```typescript
import { Redis } from 'redis.do/client'

const redis = new Redis({
  url: string,           // Your Redis.do worker URL
  token?: string,        // Optional auth token
  enableAutoPipelining?: boolean  // Auto-batch commands (default: false)
})
```

### Worker Exports

When using Redis.do as a library in your Cloudflare Worker:

```typescript
// Main exports
export { RedisShard, RedisPubSub, RedisCoordinator } from 'redis.do'
export { Redis.doEntrypoint } from 'redis.do'

// Types
export type { Env, SetOptions, ScanOptions, ZAddOptions, ZRangeOptions } from 'redis.do'
```

## MCP Integration

Redis.do includes a built-in Model Context Protocol (MCP) server, enabling AI agents to interact with your Redis data directly.

### MCP Endpoint

The MCP server is available at `/mcp` on your Redis.do deployment:

```
https://your-redis.do.workers.dev/mcp
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "redis.do": {
      "command": "npx",
      "args": ["redis.do-mcp"],
      "env": {
        "REDOIS_URL": "https://your-redis.do.workers.dev",
        "REDOIS_TOKEN": "your-auth-token"
      }
    }
  }
}
```

### Available MCP Tools

The MCP server exposes Redis commands as tools:

- **redis_get** - Get a string value
- **redis_set** - Set a string value with optional expiration
- **redis_del** - Delete keys
- **redis_keys** - List keys matching a pattern
- **redis_hget** / **redis_hset** / **redis_hgetall** - Hash operations
- **redis_lpush** / **redis_rpush** / **redis_lrange** - List operations
- **redis_sadd** / **redis_smembers** - Set operations
- **redis_zadd** / **redis_zrange** - Sorted set operations
- **redis_do** - Execute arbitrary Redis commands or JavaScript with Redis access

### Secure Code Execution

The `redis_do` tool allows executing sandboxed JavaScript with full Redis access:

```javascript
// Example: Complex data processing
const keys = await redis.keys('user:*')
const users = await Promise.all(
  keys.map(async (key) => ({
    id: key.split(':')[1],
    ...await redis.hgetall(key)
  }))
)
return users.filter(u => u.active === 'true')
```

## Deployment

### Prerequisites

- [Cloudflare account](https://dash.cloudflare.com/sign-up)
- [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/)
- Node.js 18+

### Deploy Your Own Instance

1. **Clone the repository**

```bash
git clone https://github.com/dot-do/redis.git
cd redis.do
```

2. **Install dependencies**

```bash
npm install
```

3. **Configure authentication (optional)**

Create a `.dev.vars` file for local development:

```
AUTH_TOKEN=your-secret-token
```

For production, set the secret:

```bash
wrangler secret put AUTH_TOKEN
```

4. **Deploy**

```bash
# Development
npm run dev

# Production
npm run deploy
```

### Environment Configuration

Configure via `wrangler.jsonc`:

```jsonc
{
  "name": "redis.do",
  "main": "src/worker.ts",
  "compatibility_date": "2025-01-01",
  "compatibility_flags": ["nodejs_compat"],
  "durable_objects": {
    "bindings": [
      { "name": "REDIS_SHARDS", "class_name": "RedisShard" },
      { "name": "REDIS_PUBSUB", "class_name": "RedisPubSub" },
      { "name": "REDIS_COORDINATOR", "class_name": "RedisCoordinator" }
    ]
  }
}
```

### Health Check

Verify your deployment:

```bash
curl https://your-redis.do.workers.dev/health
# {"status":"ok","version":"0.1.0"}
```

## Documentation

- [Getting Started](./docs/index.mdx)
- [Client Library](./docs/client.mdx)
- [Command Reference](./docs/commands.mdx)
- [MCP Integration](./docs/mcp.mdx)
- [Deployment Guide](./docs/deployment.mdx)
- [Architecture](./docs/architecture.mdx)

## Limitations

### Transaction Atomicity (MULTI/EXEC)

Redis.do does **NOT** provide true MULTI/EXEC transaction atomicity. In standard Redis, MULTI/EXEC guarantees that all commands in a transaction are executed atomically without interleaving from other clients. Redis.do does not provide this guarantee.

When using `multi()` or `pipeline()` in Redis.do:

- **Commands are batched** - Sent together for reduced network round trips
- **Sequential execution** - Commands execute in order within the batch
- **No isolation** - Other clients' commands may interleave between your batch commands
- **No rollback** - Partial execution is possible if a command fails mid-batch
- **No WATCH/UNWATCH** - Optimistic locking is not supported

```typescript
// This batches commands but does NOT provide atomicity
const multi = redis.multi()
multi.set('key1', 'value1')
multi.incr('counter')
const results = await multi.exec()
```

**Workarounds for atomicity requirements:**

1. **Lua scripts via EVAL** - For operations that must be atomic, use Lua scripting
2. **Single Durable Object** - Structure data to live within a single shard for stronger consistency
3. **Application-level locking** - Implement distributed locks for critical sections

### Other Limitations

- **Pub/Sub** - SUBSCRIBE is only available via WebSocket; use PUBLISH for sending messages
- **Cluster mode** - Redis.do handles sharding internally; Redis Cluster commands are not supported
- **Persistence** - Data is persisted to SQLite in Durable Objects, not RDB/AOF files
- **Lua scripting** - EVAL/EVALSHA support is limited compared to standard Redis

## Contributing

Contributions are welcome! Please read our [Contributing Guide](./CONTRIBUTING.md) for details.

## License

[MIT](./LICENSE) - see LICENSE file for details.

---

<p align="center">
  Built with Cloudflare Workers, Durable Objects, and SQLite
</p>
