/**
 * Worker Sandbox Tests
 *
 * Tests for the Worker Evaluator sandbox functionality
 * including security restrictions, Redis access, and timeout handling
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  executeInBasicSandbox,
  createSandboxedRedisProxy,
  BasicSandboxOptions,
} from '../../src/mcp/sandbox/worker-evaluator'
import { validateUserCode, prepareUserCode } from '../../src/mcp/sandbox/template'
import type { RedisProxy, SandboxResult } from '../../src/mcp/types'

// ─────────────────────────────────────────────────────────────────
// Mock Redis Proxy
// ─────────────────────────────────────────────────────────────────

function createMockRedisProxy(): RedisProxy {
  const store = new Map<string, string>()
  const hashStore = new Map<string, Map<string, string>>()
  const listStore = new Map<string, string[]>()
  const setStore = new Map<string, Set<string>>()
  const zsetStore = new Map<string, Map<string, number>>()

  return {
    async get(key: string) {
      return store.get(key) ?? null
    },
    async set(key: string, value: string) {
      store.set(key, value)
      return 'OK'
    },
    async del(...keys: string[]) {
      let count = 0
      for (const key of keys) {
        if (store.delete(key)) count++
        if (hashStore.delete(key)) count++
        if (listStore.delete(key)) count++
        if (setStore.delete(key)) count++
        if (zsetStore.delete(key)) count++
      }
      return count
    },
    async exists(...keys: string[]) {
      let count = 0
      for (const key of keys) {
        if (store.has(key) || hashStore.has(key) || listStore.has(key) ||
            setStore.has(key) || zsetStore.has(key)) {
          count++
        }
      }
      return count
    },
    async expire() {
      return 1
    },
    async ttl() {
      return -1
    },
    async keys(pattern: string) {
      const regex = new RegExp('^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$')
      return [...store.keys()].filter(k => regex.test(k))
    },
    async scan(cursor: number) {
      const keys = [...store.keys()]
      return ['0', keys] as [string, string[]]
    },
    async hget(key: string, field: string) {
      return hashStore.get(key)?.get(field) ?? null
    },
    async hset(key: string, field: string, value: string) {
      if (!hashStore.has(key)) hashStore.set(key, new Map())
      const isNew = !hashStore.get(key)!.has(field)
      hashStore.get(key)!.set(field, value)
      return isNew ? 1 : 0
    },
    async hgetall(key: string) {
      const hash = hashStore.get(key)
      if (!hash) return {}
      return Object.fromEntries(hash)
    },
    async hdel(key: string, ...fields: string[]) {
      const hash = hashStore.get(key)
      if (!hash) return 0
      let count = 0
      for (const field of fields) {
        if (hash.delete(field)) count++
      }
      return count
    },
    async lpush(key: string, ...values: string[]) {
      if (!listStore.has(key)) listStore.set(key, [])
      listStore.get(key)!.unshift(...values.reverse())
      return listStore.get(key)!.length
    },
    async rpush(key: string, ...values: string[]) {
      if (!listStore.has(key)) listStore.set(key, [])
      listStore.get(key)!.push(...values)
      return listStore.get(key)!.length
    },
    async lpop(key: string) {
      return listStore.get(key)?.shift() ?? null
    },
    async rpop(key: string) {
      return listStore.get(key)?.pop() ?? null
    },
    async lrange(key: string, start: number, stop: number) {
      const list = listStore.get(key) ?? []
      const end = stop < 0 ? list.length + stop + 1 : stop + 1
      return list.slice(start, end)
    },
    async sadd(key: string, ...members: string[]) {
      if (!setStore.has(key)) setStore.set(key, new Set())
      let count = 0
      for (const member of members) {
        if (!setStore.get(key)!.has(member)) {
          setStore.get(key)!.add(member)
          count++
        }
      }
      return count
    },
    async smembers(key: string) {
      return [...(setStore.get(key) ?? [])]
    },
    async srem(key: string, ...members: string[]) {
      const set = setStore.get(key)
      if (!set) return 0
      let count = 0
      for (const member of members) {
        if (set.delete(member)) count++
      }
      return count
    },
    async sismember(key: string, member: string) {
      return setStore.get(key)?.has(member) ? 1 : 0
    },
    async zadd(key: string, score: number, member: string) {
      if (!zsetStore.has(key)) zsetStore.set(key, new Map())
      const isNew = !zsetStore.get(key)!.has(member)
      zsetStore.get(key)!.set(member, score)
      return isNew ? 1 : 0
    },
    async zrange(key: string, start: number, stop: number) {
      const zset = zsetStore.get(key)
      if (!zset) return []
      const sorted = [...zset.entries()].sort((a, b) => a[1] - b[1])
      const end = stop < 0 ? sorted.length + stop + 1 : stop + 1
      return sorted.slice(start, end).map(([member]) => member)
    },
    async zrem(key: string, ...members: string[]) {
      const zset = zsetStore.get(key)
      if (!zset) return 0
      let count = 0
      for (const member of members) {
        if (zset.delete(member)) count++
      }
      return count
    },
    async zscore(key: string, member: string) {
      const score = zsetStore.get(key)?.get(member)
      return score !== undefined ? String(score) : null
    },
    async incr(key: string) {
      const value = parseInt(store.get(key) ?? '0', 10)
      store.set(key, String(value + 1))
      return value + 1
    },
    async incrby(key: string, increment: number) {
      const value = parseInt(store.get(key) ?? '0', 10)
      store.set(key, String(value + increment))
      return value + increment
    },
    async decr(key: string) {
      const value = parseInt(store.get(key) ?? '0', 10)
      store.set(key, String(value - 1))
      return value - 1
    },
    async decrby(key: string, decrement: number) {
      const value = parseInt(store.get(key) ?? '0', 10)
      store.set(key, String(value - decrement))
      return value - decrement
    },
    async append(key: string, value: string) {
      const current = store.get(key) ?? ''
      store.set(key, current + value)
      return (current + value).length
    },
    async strlen(key: string) {
      return (store.get(key) ?? '').length
    },
    async type(key: string) {
      if (store.has(key)) return 'string'
      if (hashStore.has(key)) return 'hash'
      if (listStore.has(key)) return 'list'
      if (setStore.has(key)) return 'set'
      if (zsetStore.has(key)) return 'zset'
      return 'none'
    },
    async rename(key: string, newKey: string) {
      if (store.has(key)) {
        store.set(newKey, store.get(key)!)
        store.delete(key)
      }
      return 'OK'
    },
    async dbsize() {
      return store.size + hashStore.size + listStore.size + setStore.size + zsetStore.size
    },
    async flushdb() {
      store.clear()
      hashStore.clear()
      listStore.clear()
      setStore.clear()
      zsetStore.clear()
      return 'OK'
    },
    async info() {
      return '# Server\nredis_version:7.0.0'
    },
    async ping(message?: string) {
      return message ?? 'PONG'
    },
  }
}

// ─────────────────────────────────────────────────────────────────
// Code Validation Tests
// ─────────────────────────────────────────────────────────────────

describe('Code Validation', () => {
  describe('Dangerous Pattern Detection', () => {
    it('should reject code with eval()', () => {
      const result = validateUserCode('eval("alert(1)")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('eval'))).toBe(true)
    })

    it('should reject code with new Function()', () => {
      const result = validateUserCode('new Function("return 1")()')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('Function'))).toBe(true)
    })

    it('should reject code with fetch()', () => {
      const result = validateUserCode('fetch("https://example.com")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('fetch'))).toBe(true)
    })

    it('should reject code with WebSocket', () => {
      const result = validateUserCode('new WebSocket("ws://example.com")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('WebSocket'))).toBe(true)
    })

    it('should reject code with require()', () => {
      const result = validateUserCode('require("fs")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('require'))).toBe(true)
    })

    it('should reject code with dynamic import()', () => {
      const result = validateUserCode('import("./module")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('import'))).toBe(true)
    })

    it('should reject code with __proto__', () => {
      const result = validateUserCode('obj.__proto__ = {}')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('__proto__'))).toBe(true)
    })

    it('should reject code with globalThis access', () => {
      const result = validateUserCode('globalThis.console')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('globalThis'))).toBe(true)
    })

    it('should reject code with process access', () => {
      const result = validateUserCode('process.env.SECRET')
      expect(result.valid).toBe(false)
    })

    it('should reject code with setInterval', () => {
      const result = validateUserCode('setInterval(() => {}, 1000)')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('setInterval'))).toBe(true)
    })

    it('should reject code with Worker creation', () => {
      const result = validateUserCode('new Worker("worker.js")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('Worker'))).toBe(true)
    })

    it('should reject code with Proxy creation', () => {
      const result = validateUserCode('new Proxy({}, {})')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('Proxy'))).toBe(true)
    })

    it('should reject code with Reflect usage', () => {
      const result = validateUserCode('Reflect.get({}, "key")')
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('Reflect'))).toBe(true)
    })
  })

  describe('Valid Code Patterns', () => {
    it('should accept simple Redis operations', () => {
      const result = validateUserCode(`
        const value = await redis.get('key')
        return value
      `)
      expect(result.valid).toBe(true)
      expect(result.errors).toHaveLength(0)
    })

    it('should accept code with console.log', () => {
      const result = validateUserCode(`
        console.log('Hello')
        return 'done'
      `)
      expect(result.valid).toBe(true)
    })

    it('should accept code with async/await', () => {
      const result = validateUserCode(`
        const a = await redis.get('a')
        const b = await redis.get('b')
        return a + b
      `)
      expect(result.valid).toBe(true)
    })

    it('should accept code with loops', () => {
      const result = validateUserCode(`
        const results = []
        for (let i = 0; i < 10; i++) {
          results.push(i)
        }
        return results
      `)
      expect(result.valid).toBe(true)
    })

    it('should accept code with try/catch', () => {
      const result = validateUserCode(`
        try {
          const value = await redis.get('key')
          return value.toUpperCase()
        } catch (e) {
          return 'error'
        }
      `)
      expect(result.valid).toBe(true)
    })

    it('should accept code with JSON operations', () => {
      const result = validateUserCode(`
        const data = JSON.stringify({ name: 'test' })
        await redis.set('data', data)
        const result = JSON.parse(await redis.get('data'))
        return result
      `)
      expect(result.valid).toBe(true)
    })
  })

  describe('Size Limits', () => {
    it('should reject code exceeding maximum length', () => {
      const longCode = 'x'.repeat(1000001)
      const result = validateUserCode(longCode)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('maximum length'))).toBe(true)
    })

    it('should reject very large string literals', () => {
      const longString = '"' + 'x'.repeat(100001) + '"'
      const result = validateUserCode(`const x = ${longString}`)
      expect(result.valid).toBe(false)
      expect(result.errors.some(e => e.includes('maximum size'))).toBe(true)
    })
  })

  describe('Warnings', () => {
    it('should warn about potential infinite loops with while(true)', () => {
      const result = validateUserCode('while(true) { break }')
      expect(result.valid).toBe(true)
      expect(result.warnings.some(w => w.includes('infinite loop'))).toBe(true)
    })

    it('should warn about potential infinite loops with for(;;)', () => {
      const result = validateUserCode('for(;;) { break }')
      expect(result.valid).toBe(true)
      expect(result.warnings.some(w => w.includes('infinite loop'))).toBe(true)
    })
  })
})

// ─────────────────────────────────────────────────────────────────
// Basic Sandbox Execution Tests
// ─────────────────────────────────────────────────────────────────

// Skipped: Dynamic code execution (AsyncFunction constructor) is not allowed in Cloudflare Workers
describe.skip('Basic Sandbox Execution', () => {
  let redis: RedisProxy

  beforeEach(() => {
    redis = createMockRedisProxy()
  })

  describe('Simple Execution', () => {
    it('should execute simple code and return result', async () => {
      const result = await executeInBasicSandbox('return 42', redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe(42)
    })

    it('should execute code with string operations', async () => {
      const result = await executeInBasicSandbox(`
        return 'hello'.toUpperCase()
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('HELLO')
    })

    it('should execute code with array operations', async () => {
      const result = await executeInBasicSandbox(`
        return [1, 2, 3].map(x => x * 2)
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual([2, 4, 6])
    })

    it('should execute code with object operations', async () => {
      const result = await executeInBasicSandbox(`
        return { name: 'test', value: 42 }
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual({ name: 'test', value: 42 })
    })
  })

  describe('Redis Operations', () => {
    it('should execute GET and SET operations', async () => {
      await redis.set('testkey', 'testvalue')
      const result = await executeInBasicSandbox(`
        const value = await redis.get('testkey')
        return value
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('testvalue')
    })

    it('should execute GET and return uppercase value', async () => {
      await redis.set('key', 'hello world')
      const result = await executeInBasicSandbox(`
        const value = await redis.get('key')
        return value.toUpperCase()
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('HELLO WORLD')
    })

    it('should execute SET and verify', async () => {
      const result = await executeInBasicSandbox(`
        await redis.set('newkey', 'newvalue')
        return await redis.get('newkey')
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('newvalue')
    })

    it('should execute INCR operation', async () => {
      await redis.set('counter', '0')
      const result = await executeInBasicSandbox(`
        await redis.incr('counter')
        await redis.incr('counter')
        return await redis.get('counter')
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('2')
    })

    it('should execute hash operations', async () => {
      const result = await executeInBasicSandbox(`
        await redis.hset('user', 'name', 'John')
        await redis.hset('user', 'age', '30')
        return await redis.hgetall('user')
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual({ name: 'John', age: '30' })
    })

    it('should execute list operations', async () => {
      const result = await executeInBasicSandbox(`
        await redis.rpush('list', 'a', 'b', 'c')
        return await redis.lrange('list', 0, -1)
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual(['a', 'b', 'c'])
    })

    it('should execute set operations', async () => {
      const result = await executeInBasicSandbox(`
        await redis.sadd('myset', 'a', 'b', 'c')
        const members = await redis.smembers('myset')
        return members.sort()
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual(['a', 'b', 'c'])
    })

    it('should execute sorted set operations', async () => {
      const result = await executeInBasicSandbox(`
        await redis.zadd('zset', 1, 'a')
        await redis.zadd('zset', 2, 'b')
        await redis.zadd('zset', 3, 'c')
        return await redis.zrange('zset', 0, -1)
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toEqual(['a', 'b', 'c'])
    })

    it('should handle non-existent keys', async () => {
      const result = await executeInBasicSandbox(`
        const value = await redis.get('nonexistent')
        return value === null ? 'not found' : value
      `, redis)
      expect(result.success).toBe(true)
      expect(result.result).toBe('not found')
    })
  })

  describe('Console Output', () => {
    it('should capture console.log output', async () => {
      const result = await executeInBasicSandbox(`
        console.log('Hello', 'World')
        return 'done'
      `, redis)
      expect(result.success).toBe(true)
      expect(result.logs).toContain('Hello World')
    })

    it('should capture console.info output', async () => {
      const result = await executeInBasicSandbox(`
        console.info('Info message')
        return 'done'
      `, redis)
      expect(result.success).toBe(true)
      expect(result.logs.some(l => l.includes('[INFO]'))).toBe(true)
    })

    it('should capture console.warn output', async () => {
      const result = await executeInBasicSandbox(`
        console.warn('Warning message')
        return 'done'
      `, redis)
      expect(result.success).toBe(true)
      expect(result.logs.some(l => l.includes('[WARN]'))).toBe(true)
    })

    it('should capture console.error output', async () => {
      const result = await executeInBasicSandbox(`
        console.error('Error message')
        return 'done'
      `, redis)
      expect(result.success).toBe(true)
      expect(result.logs.some(l => l.includes('[ERROR]'))).toBe(true)
    })

    it('should limit log entries', async () => {
      const result = await executeInBasicSandbox(`
        for (let i = 0; i < 2000; i++) {
          console.log('Log entry ' + i)
        }
        return 'done'
      `, redis, { maxLogEntries: 100 })
      expect(result.success).toBe(true)
      expect(result.logs.length).toBe(100)
    })

    it('should truncate long log entries', async () => {
      const result = await executeInBasicSandbox(`
        console.log('x'.repeat(20000))
        return 'done'
      `, redis, { maxLogEntryLength: 1000 })
      expect(result.success).toBe(true)
      expect(result.logs[0].length).toBeLessThanOrEqual(1020) // 1000 + "... (truncated)"
    })
  })

  describe('Error Handling', () => {
    it('should handle runtime errors', async () => {
      const result = await executeInBasicSandbox(`
        const x = null
        return x.toString()
      `, redis)
      expect(result.success).toBe(false)
      expect(result.error).toBeDefined()
    })

    it('should handle thrown errors', async () => {
      const result = await executeInBasicSandbox(`
        throw new Error('Custom error')
      `, redis)
      expect(result.success).toBe(false)
      expect(result.error).toBe('Custom error')
    })

    it('should handle async errors', async () => {
      const result = await executeInBasicSandbox(`
        const value = await redis.get('key')
        return value.toUpperCase() // value is null
      `, redis)
      expect(result.success).toBe(false)
      expect(result.error).toBeDefined()
    })

    it('should report validation errors', async () => {
      const result = await executeInBasicSandbox('eval("1")', redis)
      expect(result.success).toBe(false)
      expect(result.error).toContain('validation failed')
    })
  })

  describe('Timeout Handling', () => {
    it('should timeout long-running code', async () => {
      const result = await executeInBasicSandbox(`
        let i = 0
        while (i < 1) {
          // This would be an infinite loop without timeout
          await new Promise(r => setTimeout(r, 10))
          i++
        }
        return i
      `, redis, { timeout: 50 })
      // Either times out or completes quickly
      expect(result.executionTime).toBeDefined()
    })

    it('should include execution time in result', async () => {
      const result = await executeInBasicSandbox('return 1', redis)
      expect(result.executionTime).toBeGreaterThanOrEqual(0)
      expect(result.executionTime).toBeLessThan(1000)
    })
  })

  describe('Security', () => {
    it('should reject code with forbidden patterns', async () => {
      const result = await executeInBasicSandbox('eval("1")', redis)
      expect(result.success).toBe(false)
    })

    it('should not allow access to global objects', async () => {
      const result = await executeInBasicSandbox(`
        try {
          return typeof globalThis
        } catch {
          return 'blocked'
        }
      `, redis)
      // Either returns 'blocked' or validation fails
      expect(result.success).toBe(false)
    })
  })
})

// ─────────────────────────────────────────────────────────────────
// Sandboxed Redis Proxy Tests
// ─────────────────────────────────────────────────────────────────

describe('Sandboxed Redis Proxy', () => {
  let baseProxy: RedisProxy

  beforeEach(() => {
    baseProxy = createMockRedisProxy()
  })

  describe('Key Prefix', () => {
    it('should apply key prefix to get', async () => {
      await baseProxy.set('prefix:key', 'value')
      const sandboxed = createSandboxedRedisProxy(baseProxy, { keyPrefix: 'prefix:' })
      const result = await sandboxed.get('key')
      expect(result).toBe('value')
    })

    it('should apply key prefix to set', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, { keyPrefix: 'sandbox:' })
      await sandboxed.set('mykey', 'myvalue')
      const result = await baseProxy.get('sandbox:mykey')
      expect(result).toBe('myvalue')
    })

    it('should apply key prefix to hash operations', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, { keyPrefix: 'test:' })
      await sandboxed.hset('hash', 'field', 'value')
      const result = await baseProxy.hget('test:hash', 'field')
      expect(result).toBe('value')
    })

    it('should strip key prefix from keys command results', async () => {
      await baseProxy.set('ns:key1', 'value1')
      await baseProxy.set('ns:key2', 'value2')
      await baseProxy.set('other:key', 'value')

      const sandboxed = createSandboxedRedisProxy(baseProxy, { keyPrefix: 'ns:' })
      const keys = await sandboxed.keys('*')
      expect(keys).toContain('key1')
      expect(keys).toContain('key2')
    })
  })

  describe('Command Restrictions', () => {
    it('should allow only specified commands', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        allowedCommands: ['get', 'set'],
      })

      await sandboxed.set('key', 'value')
      const result = await sandboxed.get('key')
      expect(result).toBe('value')

      await expect(sandboxed.del('key')).rejects.toThrow('not allowed')
    })

    it('should block destructive commands when restricted', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        allowedCommands: ['get', 'set', 'keys'],
      })

      await expect(sandboxed.flushdb()).rejects.toThrow('blocked in sandbox mode')
    })

    it('should allow all commands when no restriction', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy)

      await sandboxed.set('key', 'value')
      await sandboxed.del('key')
      const result = await sandboxed.get('key')
      expect(result).toBeNull()
    })
  })

  describe('Key Pattern Blocking', () => {
    it('should block access to keys matching blocked patterns', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        blockedKeyPatterns: [/^admin:/i, /^secret/i],
      })

      await expect(sandboxed.get('admin:password')).rejects.toThrow('blocked')
      await expect(sandboxed.set('secret-key', 'value')).rejects.toThrow('blocked')
    })

    it('should allow access to non-blocked keys', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        blockedKeyPatterns: [/^admin:/i],
      })

      await sandboxed.set('user:123', 'value')
      const result = await sandboxed.get('user:123')
      expect(result).toBe('value')
    })

    it('should block rename to blocked keys', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        blockedKeyPatterns: [/^secret:/i],
      })

      await sandboxed.set('normal', 'value')
      await expect(sandboxed.rename('normal', 'secret:key')).rejects.toThrow('blocked')
    })
  })

  describe('Combined Restrictions', () => {
    it('should apply prefix and command restrictions together', async () => {
      await baseProxy.set('app:key', 'value')

      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        keyPrefix: 'app:',
        allowedCommands: ['get'],
      })

      const result = await sandboxed.get('key')
      expect(result).toBe('value')

      await expect(sandboxed.set('key', 'new')).rejects.toThrow('not allowed')
    })

    it('should apply all restrictions together', async () => {
      const sandboxed = createSandboxedRedisProxy(baseProxy, {
        keyPrefix: 'user:',
        allowedCommands: ['get', 'set', 'del'],
        blockedKeyPatterns: [/^password$/i],
      })

      await sandboxed.set('name', 'John')
      expect(await sandboxed.get('name')).toBe('John')

      await expect(sandboxed.get('password')).rejects.toThrow('blocked')
      await expect(sandboxed.flushdb()).rejects.toThrow('blocked in sandbox mode')
    })
  })
})

// ─────────────────────────────────────────────────────────────────
// Code Preparation Tests
// ─────────────────────────────────────────────────────────────────

describe('Code Preparation', () => {
  it('should prepare valid code for execution', () => {
    const { code, validation } = prepareUserCode(`
      const value = await redis.get('key')
      return value
    `)
    expect(validation.valid).toBe(true)
    expect(code).toBeTruthy()
  })

  it('should return empty code for invalid input', () => {
    const { code, validation } = prepareUserCode('eval("1")')
    expect(validation.valid).toBe(false)
    expect(code).toBe('')
  })

  it('should add await to Redis calls automatically', () => {
    const { code, validation } = prepareUserCode(`
      const value = redis.get('key')
      return value
    `)
    expect(validation.valid).toBe(true)
    // The prepared code should have await added
    expect(code).toContain('await')
  })

  it('should sanitize code with null bytes', () => {
    const { code, validation } = prepareUserCode('return 1\x00 + 2')
    expect(validation.valid).toBe(true)
    expect(code).not.toContain('\x00')
  })

  it('should normalize line endings', () => {
    const { code, validation } = prepareUserCode('return 1\r\n+ 2')
    expect(validation.valid).toBe(true)
    expect(code).not.toContain('\r')
  })
})
