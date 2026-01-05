/**
 * Integration Tests for Durable Object Operations
 *
 * Tests the RedisShard Durable Object implementation using Miniflare/vitest-pool-workers
 * Covers CRUD operations, TTL/expiration, concurrent operations, and error scenarios
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { env, SELF } from 'cloudflare:test'

// Helper to get a unique shard stub for each test
function getShardStub(shardId = 'test-shard') {
  const id = env.REDIS_SHARDS.idFromName(shardId)
  return env.REDIS_SHARDS.get(id)
}

// ─────────────────────────────────────────────────────────────────
// String Commands - Basic CRUD
// ─────────────────────────────────────────────────────────────────

describe('String Commands - Basic CRUD', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    // Get a fresh shard for each test
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should SET and GET a string value', async () => {
    const result = await shard.set('mykey', 'myvalue')
    expect(result).toBe('OK')

    const value = await shard.get('mykey')
    expect(value).toBe('myvalue')
  })

  it('should return null for non-existent key', async () => {
    const value = await shard.get('nonexistent')
    expect(value).toBeNull()
  })

  it('should overwrite existing value', async () => {
    await shard.set('mykey', 'value1')
    await shard.set('mykey', 'value2')
    const value = await shard.get('mykey')
    expect(value).toBe('value2')
  })

  it('should handle empty string value', async () => {
    await shard.set('emptykey', '')
    const value = await shard.get('emptykey')
    expect(value).toBe('')
  })

  it('should handle unicode characters', async () => {
    await shard.set('unicodekey', 'Hello World! Emoji')
    const value = await shard.get('unicodekey')
    expect(value).toBe('Hello World! Emoji')
  })

  it('should DEL a key', async () => {
    await shard.set('delkey', 'value')
    const deleted = await shard.del('delkey')
    expect(deleted).toBe(1)

    const value = await shard.get('delkey')
    expect(value).toBeNull()
  })

  it('should return 0 when DEL non-existent key', async () => {
    const deleted = await shard.del('nonexistent')
    expect(deleted).toBe(0)
  })

  it('should check EXISTS for existing key', async () => {
    await shard.set('existskey', 'value')
    const exists = await shard.exists('existskey')
    expect(exists).toBe(1)
  })

  it('should check EXISTS for non-existent key', async () => {
    const exists = await shard.exists('nonexistent')
    expect(exists).toBe(0)
  })
})

// ─────────────────────────────────────────────────────────────────
// String Commands - SET Options
// ─────────────────────────────────────────────────────────────────

describe('String Commands - SET Options', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should SET with NX when key does not exist', async () => {
    const result = await shard.set('nxkey', 'value', { nx: true })
    expect(result).toBe('OK')
    expect(await shard.get('nxkey')).toBe('value')
  })

  it('should not SET with NX when key exists', async () => {
    await shard.set('nxkey', 'original')
    const result = await shard.set('nxkey', 'newvalue', { nx: true })
    expect(result).toBeNull()
    expect(await shard.get('nxkey')).toBe('original')
  })

  it('should SET with XX when key exists', async () => {
    await shard.set('xxkey', 'original')
    const result = await shard.set('xxkey', 'newvalue', { xx: true })
    expect(result).toBe('OK')
    expect(await shard.get('xxkey')).toBe('newvalue')
  })

  it('should not SET with XX when key does not exist', async () => {
    const result = await shard.set('xxkey', 'value', { xx: true })
    expect(result).toBeNull()
    expect(await shard.get('xxkey')).toBeNull()
  })

  it('should return old value with GET option', async () => {
    await shard.set('getoptkey', 'oldvalue')
    const result = await shard.set('getoptkey', 'newvalue', { get: true })
    expect(result).toBe('oldvalue')
    expect(await shard.get('getoptkey')).toBe('newvalue')
  })

  it('should return null with GET option for new key', async () => {
    const result = await shard.set('newgetkey', 'value', { get: true })
    expect(result).toBeNull()
    expect(await shard.get('newgetkey')).toBe('value')
  })
})

// ─────────────────────────────────────────────────────────────────
// String Commands - INCR/DECR
// ─────────────────────────────────────────────────────────────────

describe('String Commands - INCR/DECR', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should INCR non-existent key starting from 0', async () => {
    const result = await shard.incr('counter')
    expect(result).toBe(1)
  })

  it('should INCR existing numeric value', async () => {
    await shard.set('counter', '10')
    const result = await shard.incr('counter')
    expect(result).toBe(11)
  })

  it('should DECR non-existent key starting from 0', async () => {
    const result = await shard.decr('counter')
    expect(result).toBe(-1)
  })

  it('should INCRBY specified amount', async () => {
    await shard.set('counter', '10')
    const result = await shard.incrby('counter', 5)
    expect(result).toBe(15)
  })

  it('should DECRBY specified amount', async () => {
    await shard.set('counter', '10')
    const result = await shard.decrby('counter', 3)
    expect(result).toBe(7)
  })

  it('should throw error for non-numeric value on INCR', async () => {
    await shard.set('strkey', 'notanumber')
    await expect(shard.incr('strkey')).rejects.toThrow('not an integer')
  })

  it('should INCRBYFLOAT', async () => {
    await shard.set('floatkey', '10.5')
    const result = await shard.incrbyfloat('floatkey', 0.5)
    expect(parseFloat(result)).toBeCloseTo(11.0)
  })
})

// ─────────────────────────────────────────────────────────────────
// String Commands - MGET/MSET
// ─────────────────────────────────────────────────────────────────

describe('String Commands - MGET/MSET', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should MSET multiple key-value pairs', async () => {
    const result = await shard.mset('key1', 'val1', 'key2', 'val2', 'key3', 'val3')
    expect(result).toBe('OK')

    expect(await shard.get('key1')).toBe('val1')
    expect(await shard.get('key2')).toBe('val2')
    expect(await shard.get('key3')).toBe('val3')
  })

  it('should MGET multiple keys', async () => {
    await shard.mset('k1', 'v1', 'k2', 'v2', 'k3', 'v3')
    const result = await shard.mget('k1', 'k2', 'k3')
    expect(result).toEqual(['v1', 'v2', 'v3'])
  })

  it('should MGET with some non-existent keys', async () => {
    await shard.set('exist1', 'val1')
    await shard.set('exist3', 'val3')
    const result = await shard.mget('exist1', 'nonexist', 'exist3')
    expect(result).toEqual(['val1', null, 'val3'])
  })

  it('should MSETNX when no keys exist', async () => {
    const result = await shard.msetnx('nx1', 'v1', 'nx2', 'v2')
    expect(result).toBe(1)
    expect(await shard.get('nx1')).toBe('v1')
    expect(await shard.get('nx2')).toBe('v2')
  })

  it('should not MSETNX when any key exists', async () => {
    await shard.set('nx1', 'existing')
    const result = await shard.msetnx('nx1', 'new1', 'nx2', 'new2')
    expect(result).toBe(0)
    expect(await shard.get('nx1')).toBe('existing')
    expect(await shard.get('nx2')).toBeNull()
  })
})

// ─────────────────────────────────────────────────────────────────
// String Commands - APPEND/STRLEN/GETRANGE
// ─────────────────────────────────────────────────────────────────

describe('String Commands - APPEND/STRLEN/GETRANGE', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should APPEND to existing key', async () => {
    await shard.set('appendkey', 'Hello')
    const len = await shard.append('appendkey', ' World')
    expect(len).toBe(11)
    expect(await shard.get('appendkey')).toBe('Hello World')
  })

  it('should APPEND to non-existent key', async () => {
    const len = await shard.append('newappend', 'value')
    expect(len).toBe(5)
    expect(await shard.get('newappend')).toBe('value')
  })

  it('should get STRLEN of existing key', async () => {
    await shard.set('strlenkey', 'Hello World')
    const len = await shard.strlen('strlenkey')
    expect(len).toBe(11)
  })

  it('should return 0 STRLEN for non-existent key', async () => {
    const len = await shard.strlen('nonexistent')
    expect(len).toBe(0)
  })

  it('should GETRANGE with positive indices', async () => {
    await shard.set('rangekey', 'Hello World')
    const sub = await shard.getrange('rangekey', 0, 4)
    expect(sub).toBe('Hello')
  })

  it('should GETRANGE with negative indices', async () => {
    await shard.set('rangekey', 'Hello World')
    const sub = await shard.getrange('rangekey', -5, -1)
    expect(sub).toBe('World')
  })
})

// ─────────────────────────────────────────────────────────────────
// Hash Commands
// ─────────────────────────────────────────────────────────────────

describe('Hash Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should HSET and HGET', async () => {
    const added = await shard.hset('myhash', 'field1', 'value1')
    expect(added).toBe(1)

    const value = await shard.hget('myhash', 'field1')
    expect(value).toBe('value1')
  })

  it('should return null for non-existent field', async () => {
    await shard.hset('myhash', 'field1', 'value1')
    const value = await shard.hget('myhash', 'nonexistent')
    expect(value).toBeNull()
  })

  it('should HSET multiple fields', async () => {
    const added = await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3')
    expect(added).toBe(3)

    expect(await shard.hget('myhash', 'f1')).toBe('v1')
    expect(await shard.hget('myhash', 'f2')).toBe('v2')
    expect(await shard.hget('myhash', 'f3')).toBe('v3')
  })

  it('should update existing fields with HSET', async () => {
    await shard.hset('myhash', 'field', 'original')
    const added = await shard.hset('myhash', 'field', 'updated')
    expect(added).toBe(0) // No new fields added

    expect(await shard.hget('myhash', 'field')).toBe('updated')
  })

  it('should HGETALL', async () => {
    await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2')
    const all = await shard.hgetall('myhash')
    expect(all).toEqual({ f1: 'v1', f2: 'v2' })
  })

  it('should return empty object for HGETALL on non-existent key', async () => {
    const all = await shard.hgetall('nonexistent')
    expect(all).toEqual({})
  })

  it('should HDEL fields', async () => {
    await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3')
    const deleted = await shard.hdel('myhash', 'f1', 'f2')
    expect(deleted).toBe(2)

    expect(await shard.hget('myhash', 'f1')).toBeNull()
    expect(await shard.hget('myhash', 'f3')).toBe('v3')
  })

  it('should HEXISTS', async () => {
    await shard.hset('myhash', 'field', 'value')
    expect(await shard.hexists('myhash', 'field')).toBe(1)
    expect(await shard.hexists('myhash', 'nonexistent')).toBe(0)
  })

  it('should HKEYS and HVALS', async () => {
    await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2')
    const keys = await shard.hkeys('myhash')
    const vals = await shard.hvals('myhash')

    expect(keys.sort()).toEqual(['f1', 'f2'])
    expect(vals.sort()).toEqual(['v1', 'v2'])
  })

  it('should HLEN', async () => {
    await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2', 'f3', 'v3')
    expect(await shard.hlen('myhash')).toBe(3)
  })

  it('should HINCRBY', async () => {
    await shard.hset('myhash', 'counter', '10')
    const result = await shard.hincrby('myhash', 'counter', 5)
    expect(result).toBe(15)
  })

  it('should HSETNX', async () => {
    expect(await shard.hsetnx('myhash', 'field', 'value')).toBe(1)
    expect(await shard.hsetnx('myhash', 'field', 'newvalue')).toBe(0)
    expect(await shard.hget('myhash', 'field')).toBe('value')
  })

  it('should HMGET', async () => {
    await shard.hset('myhash', 'f1', 'v1', 'f2', 'v2')
    const result = await shard.hmget('myhash', 'f1', 'nonexist', 'f2')
    expect(result).toEqual(['v1', null, 'v2'])
  })
})

// ─────────────────────────────────────────────────────────────────
// List Commands
// ─────────────────────────────────────────────────────────────────

describe('List Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should LPUSH and LRANGE', async () => {
    await shard.lpush('mylist', 'a', 'b', 'c')
    const result = await shard.lrange('mylist', 0, -1)
    // LPUSH adds in reverse order: c, b, a
    expect(result).toEqual(['c', 'b', 'a'])
  })

  it('should RPUSH and LRANGE', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    const result = await shard.lrange('mylist', 0, -1)
    expect(result).toEqual(['a', 'b', 'c'])
  })

  it('should LPOP', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    const popped = await shard.lpop('mylist')
    expect(popped).toBe('a')
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['b', 'c'])
  })

  it('should RPOP', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    const popped = await shard.rpop('mylist')
    expect(popped).toBe('c')
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['a', 'b'])
  })

  it('should LPOP with count', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c', 'd')
    const popped = await shard.lpop('mylist', 2)
    expect(popped).toEqual(['a', 'b'])
  })

  it('should LLEN', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    expect(await shard.llen('mylist')).toBe(3)
  })

  it('should return 0 for LLEN on non-existent key', async () => {
    expect(await shard.llen('nonexistent')).toBe(0)
  })

  it('should LINDEX', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    expect(await shard.lindex('mylist', 0)).toBe('a')
    expect(await shard.lindex('mylist', 2)).toBe('c')
    expect(await shard.lindex('mylist', -1)).toBe('c')
  })

  it('should LSET', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c')
    await shard.lset('mylist', 1, 'B')
    expect(await shard.lindex('mylist', 1)).toBe('B')
  })

  it('should LREM', async () => {
    await shard.rpush('mylist', 'a', 'b', 'a', 'c', 'a')
    const removed = await shard.lrem('mylist', 2, 'a')
    expect(removed).toBe(2)
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['b', 'c', 'a'])
  })

  it('should LTRIM', async () => {
    await shard.rpush('mylist', 'a', 'b', 'c', 'd', 'e')
    await shard.ltrim('mylist', 1, 3)
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['b', 'c', 'd'])
  })

  it('should LPUSHX only when key exists', async () => {
    expect(await shard.lpushx('mylist', 'a')).toBe(0)

    await shard.rpush('mylist', 'b')
    expect(await shard.lpushx('mylist', 'a')).toBe(2)
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['a', 'b'])
  })

  it('should LINSERT BEFORE', async () => {
    await shard.rpush('mylist', 'a', 'c')
    const newLen = await shard.linsert('mylist', 'BEFORE', 'c', 'b')
    expect(newLen).toBe(3)
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c'])
  })

  it('should LINSERT AFTER', async () => {
    await shard.rpush('mylist', 'a', 'c')
    const newLen = await shard.linsert('mylist', 'AFTER', 'a', 'b')
    expect(newLen).toBe(3)
    expect(await shard.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c'])
  })
})

// ─────────────────────────────────────────────────────────────────
// Set Commands
// ─────────────────────────────────────────────────────────────────

describe('Set Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should SADD and SMEMBERS', async () => {
    const added = await shard.sadd('myset', 'a', 'b', 'c')
    expect(added).toBe(3)

    const members = await shard.smembers('myset')
    expect(members.sort()).toEqual(['a', 'b', 'c'])
  })

  it('should not add duplicate members', async () => {
    await shard.sadd('myset', 'a', 'b')
    const added = await shard.sadd('myset', 'b', 'c')
    expect(added).toBe(1) // Only 'c' was new
  })

  it('should SREM', async () => {
    await shard.sadd('myset', 'a', 'b', 'c')
    const removed = await shard.srem('myset', 'a', 'b')
    expect(removed).toBe(2)
    expect(await shard.smembers('myset')).toEqual(['c'])
  })

  it('should SISMEMBER', async () => {
    await shard.sadd('myset', 'a', 'b')
    expect(await shard.sismember('myset', 'a')).toBe(1)
    expect(await shard.sismember('myset', 'c')).toBe(0)
  })

  it('should SMISMEMBER', async () => {
    await shard.sadd('myset', 'a', 'b')
    const result = await shard.smismember('myset', 'a', 'b', 'c')
    expect(result).toEqual([1, 1, 0])
  })

  it('should SCARD', async () => {
    await shard.sadd('myset', 'a', 'b', 'c')
    expect(await shard.scard('myset')).toBe(3)
  })

  it('should SPOP', async () => {
    await shard.sadd('myset', 'a', 'b', 'c')
    const popped = await shard.spop('myset')
    expect(['a', 'b', 'c']).toContain(popped)
    expect(await shard.scard('myset')).toBe(2)
  })

  it('should SRANDMEMBER', async () => {
    await shard.sadd('myset', 'a', 'b', 'c')
    const member = await shard.srandmember('myset')
    expect(['a', 'b', 'c']).toContain(member)
    expect(await shard.scard('myset')).toBe(3) // Set unchanged
  })

  it('should SDIFF', async () => {
    await shard.sadd('set1', 'a', 'b', 'c')
    await shard.sadd('set2', 'b', 'c', 'd')
    const diff = await shard.sdiff('set1', 'set2')
    expect(diff).toEqual(['a'])
  })

  it('should SINTER', async () => {
    await shard.sadd('set1', 'a', 'b', 'c')
    await shard.sadd('set2', 'b', 'c', 'd')
    const inter = await shard.sinter('set1', 'set2')
    expect(inter.sort()).toEqual(['b', 'c'])
  })

  it('should SUNION', async () => {
    await shard.sadd('set1', 'a', 'b')
    await shard.sadd('set2', 'c', 'd')
    const union = await shard.sunion('set1', 'set2')
    expect(union.sort()).toEqual(['a', 'b', 'c', 'd'])
  })

  it('should SMOVE', async () => {
    await shard.sadd('src', 'a', 'b', 'c')
    await shard.sadd('dst', 'x')
    const moved = await shard.smove('src', 'dst', 'b')
    expect(moved).toBe(1)
    expect(await shard.smembers('src')).not.toContain('b')
    expect(await shard.smembers('dst')).toContain('b')
  })
})

// ─────────────────────────────────────────────────────────────────
// Sorted Set Commands
// ─────────────────────────────────────────────────────────────────

describe('Sorted Set Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should ZADD and ZRANGE', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    const members = await shard.zrange('myzset', 0, -1)
    expect(members).toEqual(['a', 'b', 'c'])
  })

  it('should ZRANGE with WITHSCORES', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    const result = await shard.zrange('myzset', 0, -1, { withScores: true })
    expect(result).toEqual([
      { member: 'a', score: 1 },
      { member: 'b', score: 2 },
      { member: 'c', score: 3 },
    ])
  })

  it('should ZREVRANGE', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    const members = await shard.zrevrange('myzset', 0, -1)
    expect(members).toEqual(['c', 'b', 'a'])
  })

  it('should ZSCORE', async () => {
    await shard.zadd('myzset', 1.5, 'member')
    const score = await shard.zscore('myzset', 'member')
    expect(score).toBe(1.5)
  })

  it('should return null ZSCORE for non-existent member', async () => {
    await shard.zadd('myzset', 1, 'a')
    const score = await shard.zscore('myzset', 'nonexistent')
    expect(score).toBeNull()
  })

  it('should ZRANK', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    expect(await shard.zrank('myzset', 'a')).toBe(0)
    expect(await shard.zrank('myzset', 'b')).toBe(1)
    expect(await shard.zrank('myzset', 'c')).toBe(2)
  })

  it('should ZREVRANK', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    expect(await shard.zrevrank('myzset', 'c')).toBe(0)
    expect(await shard.zrevrank('myzset', 'a')).toBe(2)
  })

  it('should ZCARD', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    expect(await shard.zcard('myzset')).toBe(3)
  })

  it('should ZCOUNT', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c', 4, 'd')
    expect(await shard.zcount('myzset', 2, 3)).toBe(2)
  })

  it('should ZINCRBY', async () => {
    await shard.zadd('myzset', 5, 'member')
    const newScore = await shard.zincrby('myzset', 3, 'member')
    expect(newScore).toBe(8)
  })

  it('should ZREM', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    const removed = await shard.zrem('myzset', 'a', 'b')
    expect(removed).toBe(2)
    expect(await shard.zrange('myzset', 0, -1)).toEqual(['c'])
  })

  it('should ZPOPMIN', async () => {
    await shard.zadd('myzset', 3, 'c', 1, 'a', 2, 'b')
    const popped = await shard.zpopmin('myzset', 2)
    expect(popped).toEqual([
      { member: 'a', score: 1 },
      { member: 'b', score: 2 },
    ])
    expect(await shard.zcard('myzset')).toBe(1)
  })

  it('should ZPOPMAX', async () => {
    await shard.zadd('myzset', 1, 'a', 2, 'b', 3, 'c')
    const popped = await shard.zpopmax('myzset', 2)
    expect(popped).toEqual([
      { member: 'c', score: 3 },
      { member: 'b', score: 2 },
    ])
  })

  it('should ZADD with NX option', async () => {
    await shard.zadd('myzset', 1, 'a')
    const added = await shard.zadd('myzset', { nx: true }, 2, 'a', 3, 'b')
    expect(added).toBe(1) // Only 'b' was added
    expect(await shard.zscore('myzset', 'a')).toBe(1) // Score unchanged
  })

  it('should ZADD with XX option', async () => {
    await shard.zadd('myzset', 1, 'a')
    const updated = await shard.zadd('myzset', { xx: true }, 2, 'a', 3, 'b')
    expect(updated).toBe(0) // 'b' not added, 'a' updated
    expect(await shard.zscore('myzset', 'a')).toBe(2)
    expect(await shard.zscore('myzset', 'b')).toBeNull()
  })
})

// ─────────────────────────────────────────────────────────────────
// TTL/Expiration
// ─────────────────────────────────────────────────────────────────

describe('TTL and Expiration', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should SET with EX and get TTL', async () => {
    await shard.set('exkey', 'value', { ex: 100 })
    const ttl = await shard.ttl('exkey')
    expect(ttl).toBeGreaterThan(95)
    expect(ttl).toBeLessThanOrEqual(100)
  })

  it('should SET with PX and get PTTL', async () => {
    await shard.set('pxkey', 'value', { px: 10000 })
    const pttl = await shard.pttl('pxkey')
    expect(pttl).toBeGreaterThan(9500)
    expect(pttl).toBeLessThanOrEqual(10000)
  })

  it('should return -1 TTL for key without expiration', async () => {
    await shard.set('noexpkey', 'value')
    const ttl = await shard.ttl('noexpkey')
    expect(ttl).toBe(-1)
  })

  it('should return -2 TTL for non-existent key', async () => {
    const ttl = await shard.ttl('nonexistent')
    expect(ttl).toBe(-2)
  })

  it('should EXPIRE a key', async () => {
    await shard.set('expirekey', 'value')
    const result = await shard.expire('expirekey', 100)
    expect(result).toBe(1)

    const ttl = await shard.ttl('expirekey')
    expect(ttl).toBeGreaterThan(95)
  })

  it('should PEXPIRE a key', async () => {
    await shard.set('pexpirekey', 'value')
    const result = await shard.pexpire('pexpirekey', 10000)
    expect(result).toBe(1)

    const pttl = await shard.pttl('pexpirekey')
    expect(pttl).toBeGreaterThan(9500)
  })

  it('should PERSIST remove TTL', async () => {
    await shard.set('persistkey', 'value', { ex: 100 })
    expect(await shard.ttl('persistkey')).toBeGreaterThan(0)

    const result = await shard.persist('persistkey')
    expect(result).toBe(1)
    expect(await shard.ttl('persistkey')).toBe(-1)
  })

  it('should EXPIREAT set absolute expiration', async () => {
    await shard.set('expireatkey', 'value')
    const futureTimestamp = Math.floor(Date.now() / 1000) + 100
    await shard.expireat('expireatkey', futureTimestamp)

    const ttl = await shard.ttl('expireatkey')
    expect(ttl).toBeGreaterThan(95)
    expect(ttl).toBeLessThanOrEqual(100)
  })

  it('should SETEX set value with expiration', async () => {
    await shard.setex('setexkey', 100, 'value')
    expect(await shard.get('setexkey')).toBe('value')
    expect(await shard.ttl('setexkey')).toBeGreaterThan(95)
  })

  it('should PSETEX set value with ms expiration', async () => {
    await shard.psetex('psetexkey', 10000, 'value')
    expect(await shard.get('psetexkey')).toBe('value')
    expect(await shard.pttl('psetexkey')).toBeGreaterThan(9500)
  })

  it('should preserve TTL with KEEPTTL option', async () => {
    await shard.set('keepttlkey', 'value1', { ex: 100 })
    const originalTtl = await shard.ttl('keepttlkey')

    await shard.set('keepttlkey', 'value2', { keepttl: true })
    expect(await shard.get('keepttlkey')).toBe('value2')

    const newTtl = await shard.ttl('keepttlkey')
    expect(newTtl).toBeGreaterThan(originalTtl - 5)
    expect(newTtl).toBeLessThanOrEqual(originalTtl)
  })

  it('should GETEX and set expiration', async () => {
    await shard.set('getexkey', 'value')
    expect(await shard.ttl('getexkey')).toBe(-1)

    const value = await shard.getex('getexkey', { ex: 100 })
    expect(value).toBe('value')
    expect(await shard.ttl('getexkey')).toBeGreaterThan(95)
  })

  it('should GETEX with persist option', async () => {
    await shard.set('getexkey', 'value', { ex: 100 })
    expect(await shard.ttl('getexkey')).toBeGreaterThan(0)

    const value = await shard.getex('getexkey', { persist: true })
    expect(value).toBe('value')
    expect(await shard.ttl('getexkey')).toBe(-1)
  })
})

// ─────────────────────────────────────────────────────────────────
// Key Commands
// ─────────────────────────────────────────────────────────────────

describe('Key Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should TYPE return correct type', async () => {
    await shard.set('strkey', 'value')
    await shard.hset('hashkey', 'f', 'v')
    await shard.rpush('listkey', 'a')
    await shard.sadd('setkey', 'a')
    await shard.zadd('zsetkey', 1, 'a')

    expect(await shard.type('strkey')).toBe('string')
    expect(await shard.type('hashkey')).toBe('hash')
    expect(await shard.type('listkey')).toBe('list')
    expect(await shard.type('setkey')).toBe('set')
    expect(await shard.type('zsetkey')).toBe('zset')
    expect(await shard.type('nonexistent')).toBe('none')
  })

  it('should RENAME key', async () => {
    await shard.set('oldkey', 'value')
    await shard.rename('oldkey', 'newkey')

    expect(await shard.get('oldkey')).toBeNull()
    expect(await shard.get('newkey')).toBe('value')
  })

  it('should RENAME overwrite existing key', async () => {
    await shard.set('key1', 'value1')
    await shard.set('key2', 'value2')
    await shard.rename('key1', 'key2')

    expect(await shard.get('key1')).toBeNull()
    expect(await shard.get('key2')).toBe('value1')
  })

  it('should throw on RENAME non-existent key', async () => {
    await expect(shard.rename('nonexistent', 'newkey')).rejects.toThrow('no such key')
  })

  it('should RENAMENX only if target does not exist', async () => {
    await shard.set('key1', 'value1')
    await shard.set('key2', 'value2')

    expect(await shard.renamenx('key1', 'key2')).toBe(0)
    expect(await shard.get('key1')).toBe('value1')

    expect(await shard.renamenx('key1', 'key3')).toBe(1)
    expect(await shard.get('key1')).toBeNull()
    expect(await shard.get('key3')).toBe('value1')
  })

  it('should KEYS with pattern', async () => {
    await shard.set('user:1', 'a')
    await shard.set('user:2', 'b')
    await shard.set('session:1', 'c')

    const userKeys = await shard.keys('user:*')
    expect(userKeys.sort()).toEqual(['user:1', 'user:2'])

    const allKeys = await shard.keys('*')
    expect(allKeys.sort()).toEqual(['session:1', 'user:1', 'user:2'])
  })

  it('should SCAN keys', async () => {
    await shard.mset('k1', 'v1', 'k2', 'v2', 'k3', 'v3', 'k4', 'v4', 'k5', 'v5')

    const [cursor1, keys1] = await shard.scan(0, { count: 3 })
    expect(keys1.length).toBeLessThanOrEqual(3)

    // Iterate to get all keys
    let allKeys: string[] = [...keys1]
    let cursor = cursor1
    while (cursor !== 0) {
      const [nextCursor, nextKeys] = await shard.scan(cursor, { count: 3 })
      allKeys = [...allKeys, ...nextKeys]
      cursor = nextCursor
    }

    expect(allKeys.sort()).toEqual(['k1', 'k2', 'k3', 'k4', 'k5'])
  })

  it('should DBSIZE return key count', async () => {
    expect(await shard.dbsize()).toBe(0)

    await shard.mset('k1', 'v1', 'k2', 'v2', 'k3', 'v3')
    expect(await shard.dbsize()).toBe(3)
  })

  it('should FLUSHDB delete all keys', async () => {
    await shard.mset('k1', 'v1', 'k2', 'v2', 'k3', 'v3')
    expect(await shard.dbsize()).toBe(3)

    await shard.flushdb()
    expect(await shard.dbsize()).toBe(0)
  })

  it('should RANDOMKEY return a random key', async () => {
    await shard.mset('k1', 'v1', 'k2', 'v2', 'k3', 'v3')
    const key = await shard.randomkey()
    expect(['k1', 'k2', 'k3']).toContain(key)
  })

  it('should RANDOMKEY return null when no keys', async () => {
    const key = await shard.randomkey()
    expect(key).toBeNull()
  })
})

// ─────────────────────────────────────────────────────────────────
// Error Scenarios
// ─────────────────────────────────────────────────────────────────

describe('Error Scenarios', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should throw WRONGTYPE for GET on hash key', async () => {
    await shard.hset('hashkey', 'field', 'value')
    await expect(shard.get('hashkey')).rejects.toThrow('WRONGTYPE')
  })

  it('should throw WRONGTYPE for HGET on string key', async () => {
    await shard.set('strkey', 'value')
    await expect(shard.hget('strkey', 'field')).rejects.toThrow('WRONGTYPE')
  })

  it('should throw WRONGTYPE for LPUSH on string key', async () => {
    await shard.set('strkey', 'value')
    await expect(shard.lpush('strkey', 'elem')).rejects.toThrow('WRONGTYPE')
  })

  it('should throw WRONGTYPE for SADD on list key', async () => {
    await shard.rpush('listkey', 'elem')
    await expect(shard.sadd('listkey', 'member')).rejects.toThrow('WRONGTYPE')
  })

  it('should throw WRONGTYPE for ZADD on set key', async () => {
    await shard.sadd('setkey', 'member')
    await expect(shard.zadd('setkey', 1, 'member')).rejects.toThrow('WRONGTYPE')
  })

  it('should throw error for INCR on non-numeric value', async () => {
    await shard.set('strkey', 'notanumber')
    await expect(shard.incr('strkey')).rejects.toThrow('not an integer')
  })

  it('should throw error for HINCRBY on non-numeric hash value', async () => {
    await shard.hset('hashkey', 'field', 'notanumber')
    await expect(shard.hincrby('hashkey', 'field', 5)).rejects.toThrow('not an integer')
  })

  it('should throw error for LSET on non-existent key', async () => {
    await expect(shard.lset('nonexistent', 0, 'value')).rejects.toThrow('no such key')
  })

  it('should throw error for LSET with out of range index', async () => {
    await shard.rpush('mylist', 'a', 'b')
    await expect(shard.lset('mylist', 10, 'value')).rejects.toThrow('index out of range')
  })

  it('should throw error for MSET with odd number of arguments', async () => {
    await expect(shard.mset('k1', 'v1', 'k2')).rejects.toThrow('wrong number of arguments')
  })
})

// ─────────────────────────────────────────────────────────────────
// Concurrent Operations
// ─────────────────────────────────────────────────────────────────

describe('Concurrent Operations', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should handle concurrent INCR operations atomically', async () => {
    await shard.set('counter', '0')

    // Run 10 concurrent increments
    const promises = Array.from({ length: 10 }, () => shard.incr('counter'))
    await Promise.all(promises)

    const value = await shard.get('counter')
    expect(value).toBe('10')
  })

  it('should handle concurrent LPUSH operations', async () => {
    const promises = Array.from({ length: 10 }, (_, i) => shard.lpush('mylist', `item${i}`))
    await Promise.all(promises)

    const len = await shard.llen('mylist')
    expect(len).toBe(10)
  })

  it('should handle concurrent SADD operations', async () => {
    const promises = Array.from({ length: 10 }, (_, i) => shard.sadd('myset', `member${i}`))
    await Promise.all(promises)

    const card = await shard.scard('myset')
    expect(card).toBe(10)
  })

  it('should handle concurrent ZADD operations', async () => {
    const promises = Array.from({ length: 10 }, (_, i) => shard.zadd('myzset', i, `member${i}`))
    await Promise.all(promises)

    const card = await shard.zcard('myzset')
    expect(card).toBe(10)
  })

  it('should handle concurrent mixed operations', async () => {
    const promises = [
      shard.set('key1', 'value1'),
      shard.set('key2', 'value2'),
      shard.hset('hash1', 'f1', 'v1'),
      shard.lpush('list1', 'item1'),
      shard.sadd('set1', 'member1'),
      shard.zadd('zset1', 1, 'member1'),
    ]
    await Promise.all(promises)

    expect(await shard.get('key1')).toBe('value1')
    expect(await shard.get('key2')).toBe('value2')
    expect(await shard.hget('hash1', 'f1')).toBe('v1')
    expect(await shard.llen('list1')).toBe(1)
    expect(await shard.scard('set1')).toBe(1)
    expect(await shard.zcard('zset1')).toBe(1)
  })

  it('should handle concurrent read and write operations', async () => {
    await shard.set('counter', '100')

    const operations = [
      shard.incr('counter'),
      shard.get('counter'),
      shard.incr('counter'),
      shard.get('counter'),
      shard.decr('counter'),
      shard.get('counter'),
    ]

    const results = await Promise.all(operations)
    // The final value should be 100 + 1 + 1 - 1 = 101
    const finalValue = await shard.get('counter')
    expect(finalValue).toBe('101')
  })
})

// ─────────────────────────────────────────────────────────────────
// GETDEL and GETSET Commands
// ─────────────────────────────────────────────────────────────────

describe('GETDEL and GETSET Commands', () => {
  let shard: DurableObjectStub

  beforeEach(async () => {
    const uniqueId = `shard-${Date.now()}-${Math.random().toString(36).slice(2)}`
    shard = getShardStub(uniqueId)
  })

  it('should GETDEL return value and delete key', async () => {
    await shard.set('mykey', 'myvalue')
    const value = await shard.getdel('mykey')
    expect(value).toBe('myvalue')
    expect(await shard.get('mykey')).toBeNull()
  })

  it('should GETDEL return null for non-existent key', async () => {
    const value = await shard.getdel('nonexistent')
    expect(value).toBeNull()
  })

  it('should GETSET return old value and set new', async () => {
    await shard.set('mykey', 'oldvalue')
    const value = await shard.getset('mykey', 'newvalue')
    expect(value).toBe('oldvalue')
    expect(await shard.get('mykey')).toBe('newvalue')
  })

  it('should GETSET return null for non-existent key', async () => {
    const value = await shard.getset('newkey', 'value')
    expect(value).toBeNull()
    expect(await shard.get('newkey')).toBe('value')
  })

  it('should SETNX set only if key does not exist', async () => {
    expect(await shard.setnx('nxkey', 'value')).toBe(1)
    expect(await shard.setnx('nxkey', 'newvalue')).toBe(0)
    expect(await shard.get('nxkey')).toBe('value')
  })
})
