# redis.do

> In-Memory Data Store. Edge-Native. Natural Language. AI-First.

AWS ElastiCache charges by the hour. Upstash charges by the request. Both require configuration, connection strings, and boilerplate. Every Redis command looks like machine code.

**redis.do** is the human alternative. Talk to your cache like you talk to a colleague. Deploy in one line. Zero configuration. AI that understands what you mean.

## AI-Native API

```typescript
import { redis } from 'redis.do'           // Full SDK
import { redis } from 'redis.do/tiny'      // Minimal client
import { redis } from 'redis.do/streams'   // Pub/Sub + Streams
```

Natural language for caching and data:

```typescript
import { redis } from 'redis.do'

// Talk to it like a colleague
const name = await redis`get user:name`
await redis`set session to abc123 expire in 1 hour`
await redis`increment page views`

// Leaderboards read like sentences
await redis`leaderboard: alice 100, bob 200, charlie 150`
const top = await redis`top 3 on leaderboard`

// Chain operations naturally
await redis`users online today`
  .map(user => redis`last activity for ${user}`)
```

## The Problem

Redis is essential but painful:

| What They Charge | The Reality |
|------------------|-------------|
| **ElastiCache** | $0.017-0.068/hour per node |
| **Upstash** | $0.20 per 100K requests |
| **MemoryDB** | $0.12/GB-hour plus I/O |
| **Configuration** | VPCs, security groups, connection pools |
| **API** | `ZADD leaderboard 100 alice` - what? |

### The Redis Tax

- Connection management across regions
- Cluster mode complexity
- Memory limits and eviction policies
- Snapshot and backup configuration
- Security groups and VPC peering

### The API Problem

```typescript
// This is not how humans think
await redis.zadd('leaderboard', 100, 'alice', 200, 'bob')
await redis.hset('user:1', 'name', 'Alice', 'email', 'alice@example.com')
await redis.lpush('queue', 'job1', 'job2', 'job3')
```

Commands designed for machines, not people.

## The Solution

**redis.do** reimagines caching for humans:

```
Traditional Redis              redis.do
-----------------------------------------------------------------
Connection strings             Just import and use
ZADD leaderboard 100 alice     leaderboard: alice 100
HSET user:1 name Alice         user:1 is Alice
SETEX session 3600 abc         set session expire in 1 hour
Pipeline boilerplate           Chain with .map()
```

## One-Click Deploy

```bash
npx create-dotdo redis
```

A global cache. Running on infrastructure you control. Natural language from day one.

```typescript
import { Redis } from 'redis.do'

export default Redis({
  name: 'my-cache',
  domain: 'cache.myapp.com',
})
```

## Features

### Strings

```typescript
// Just say it
await redis`set name to Alice`
await redis`get name`                        // 'Alice'
await redis`set session to abc123 expire in 1 hour`

// AI infers what you need
await redis`name`                            // returns value
await redis`name is Bob`                     // sets value
await redis`session expires when?`           // returns TTL
```

### Counters

```typescript
// Natural counting
await redis`increment views`
await redis`add 10 to views`
await redis`subtract 5 from stock`
await redis`views count`                     // returns current value

// Or with context
await redis`page views for /home`
await redis`increment /home views`
```

### Hashes

```typescript
// Store objects naturally
await redis`user:alice is name Alice, email alice@example.com`
await redis`user:alice`                      // { name: 'Alice', email: '...' }

// Update fields
await redis`update user:alice email to alice@newdomain.com`
await redis`user:alice email`                // returns just the email

// Delete fields
await redis`remove email from user:alice`
```

### Lists

```typescript
// Queues are intuitive
await redis`add job1, job2, job3 to queue`
await redis`next from queue`                 // pops and returns
await redis`queue contents`                  // returns all items

// Stack operations
await redis`push task onto stack`
await redis`pop from stack`
```

### Sets

```typescript
// Collections without duplicates
await redis`add redis, cloudflare, serverless to tags`
await redis`is redis in tags?`               // true
await redis`all tags`                        // returns members

// Set math reads like English
await redis`tags shared between user:1 and user:2`
await redis`tags only in user:1`
```

### Sorted Sets

```typescript
// Leaderboards in one line
await redis`leaderboard: alice 100, bob 200, charlie 150`
await redis`top 10 on leaderboard`
await redis`alice rank on leaderboard`
await redis`alice score on leaderboard`

// Add scores naturally
await redis`add 50 to alice on leaderboard`
await redis`alice now has 300 on leaderboard`
```

### Expiration

```typescript
// Time-based expiration
await redis`set temp to data expire in 60 seconds`
await redis`set cache to result expire in 5 minutes`
await redis`mykey expires in 1 hour`
await redis`how long until mykey expires?`

// Remove expiration
await redis`mykey never expires`
```

## Promise Pipelining

Chain operations without `Promise.all` boilerplate:

```typescript
// One network round trip
const results = await redis`keys user:*`
  .map(key => redis`get ${key}`)

// Parallel operations chain naturally
await redis`active sessions`
  .map(session => redis`extend ${session} by 1 hour`)

// Complex pipelines read like prose
await redis`users in Austin`
  .map(user => redis`${user} preferences`)
  .map(prefs => redis`cache ${prefs} for 1 hour`)
```

## Pub/Sub

```typescript
// Publish messages naturally
await redis`publish user:login to events`
await redis`broadcast system update to all`

// Subscribe via WebSocket
const channel = await redis`subscribe to events`
channel.on('message', (msg) => {
  console.log('received:', msg)
})
```

## MCP Integration

AI agents interact with your cache directly via Model Context Protocol:

```
https://your-redis.do.workers.dev/mcp
```

### Claude Desktop Configuration

```json
{
  "mcpServers": {
    "redis.do": {
      "command": "npx",
      "args": ["redis.do-mcp"],
      "env": {
        "REDIS_URL": "https://your-redis.do.workers.dev",
        "REDIS_TOKEN": "your-auth-token"
      }
    }
  }
}
```

### AI Tool Examples

```typescript
// AI agents can query naturally
await redis`active users in the last hour`
await redis`cache stats`
await redis`memory usage by key pattern`

// Complex operations in one call
await redis`
  get all user:* keys
  filter where active is true
  return with their last_login
`
```

## Architecture

### Durable Object per Shard

```
RedisDO (coordinator, routing)
  |
  +-- ShardDO (partition 0)
  |     |-- SQLite: Key-value store (encrypted)
  |     +-- Hot data in memory
  |
  +-- ShardDO (partition 1)
  |     |-- SQLite: Key-value store (encrypted)
  |     +-- Hot data in memory
  |
  +-- PubSubDO (channels, subscriptions)
        |-- WebSocket connections
        +-- Message routing
```

### Storage Tiers

| Tier | Storage | Use Case | Query Speed |
|------|---------|----------|-------------|
| **Hot** | Memory | Active keys, counters | <1ms |
| **Warm** | SQLite | Persistent data | <10ms |
| **Archive** | R2 | Backup, export | <100ms |

## vs Traditional Redis

| Feature | ElastiCache / Upstash | redis.do |
|---------|----------------------|----------|
| **Setup** | VPC, security groups | One line |
| **Pricing** | Per-hour or per-request | Usage-based |
| **API** | Machine commands | Natural language |
| **Pipelining** | Manual batching | `.map()` chains |
| **Global** | Single region or replicas | Edge-native |
| **AI** | None | MCP built-in |

## Deployment Options

### Cloudflare Workers (Recommended)

```bash
npx create-dotdo redis
```

### Self-Hosted

```bash
git clone https://github.com/dotdo/redis.do
cd redis.do
pnpm install
pnpm deploy
```

## Limitations

### No MULTI/EXEC Atomicity

Pipeline operations batch commands but do not provide true transaction isolation:

```typescript
// Batched, not atomic
await redis`
  set key1 to value1
  increment counter
  get key1
`
```

For atomic operations, structure data within a single shard or use application-level locking.

### Other Notes

- **Pub/Sub** - SUBSCRIBE via WebSocket only
- **Cluster mode** - Sharding is automatic and internal
- **Persistence** - SQLite in Durable Objects, not RDB/AOF

## Supported Commands

**Strings**: GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, APPEND, STRLEN, SETEX, SETNX, GETSET

**Hashes**: HGET, HSET, HMGET, HMSET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY

**Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LREM, LTRIM, LINSERT

**Sets**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SDIFF, SINTER, SUNION

**Sorted Sets**: ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZREVRANGE, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX

**Keys**: DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, TYPE, KEYS, SCAN, RENAME

**Server**: PING, ECHO, DBSIZE, FLUSHDB, INFO

**Pub/Sub**: PUBLISH, SUBSCRIBE (WebSocket)

## Contributing

redis.do is open source under the MIT license.

```bash
git clone https://github.com/dotdo/redis.do
cd redis.do
pnpm install
pnpm test
```

## License

MIT License - Cache everything.

---

<p align="center">
  <strong>Redis without the Redis tax.</strong>
  <br />
  Natural language. Edge-native. AI-first.
  <br /><br />
  <a href="https://redis.do">Website</a> |
  <a href="https://docs.redis.do">Docs</a> |
  <a href="https://discord.gg/dotdo">Discord</a> |
  <a href="https://github.com/dotdo/redis.do">GitHub</a>
</p>
