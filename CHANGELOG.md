# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-04

### Added

- **Core Redis Commands**
  - String operations: GET, SET, MGET, MSET, INCR, DECR, INCRBY, DECRBY, APPEND, STRLEN, SETEX, SETNX, GETSET, GETDEL, GETEX
  - Hash operations: HGET, HSET, HMGET, HMSET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT
  - List operations: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LREM, LTRIM, LINSERT, LMOVE
  - Set operations: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SDIFF, SINTER, SUNION
  - Sorted Set operations: ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX
  - Key operations: DEL, EXISTS, EXPIRE, EXPIREAT, TTL, PTTL, PERSIST, TYPE, KEYS, SCAN, RENAME, COPY
  - Server operations: PING, ECHO, DBSIZE, FLUSHDB, INFO, TIME

- **Cloudflare Workers Integration**
  - Durable Objects for persistent storage (RedisShard, RedisPubSub, RedisCoordinator)
  - SQLite-backed storage for durability
  - Automatic sharding across 256 Durable Objects
  - Worker Loader integration for sandboxed code execution

- **REST API**
  - Upstash-compatible REST API at root endpoint
  - GET and POST methods for command execution
  - Bearer token authentication support
  - CORS support for browser clients

- **Client Library**
  - ioredis-compatible client API
  - Pipeline support for batching commands
  - TypeScript types included
  - Configurable auto-pipelining

- **MCP Integration**
  - Model Context Protocol server at /mcp endpoint
  - Redis command tools for AI agents
  - Secure sandboxed code execution via `redis_do` tool
  - HTTP and stdio transport support
  - Bearer token authentication

- **RPC Protocol**
  - JSON-RPC 2.0 based RPC endpoint at /rpc
  - Batch command execution
  - Efficient binary protocol support

- **Developer Experience**
  - Full TypeScript support
  - Comprehensive type definitions
  - Local development with wrangler dev
  - Multiple environment support (dev, staging, production)

### Infrastructure

- Cloudflare Workers runtime
- Durable Objects with SQLite storage
- Automatic key distribution via consistent hashing
- Global edge deployment

[0.1.0]: https://github.com/nathanclevenger/redois/releases/tag/v0.1.0
