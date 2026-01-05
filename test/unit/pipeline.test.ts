/**
 * Pipeline Tests
 *
 * Tests for Redis pipeline/batch command execution
 * Covers queuing commands, exec(), and result handling
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { Pipeline, PipelineCommand, PipelineExecResult } from '../../src/client/pipeline'

// ─────────────────────────────────────────────────────────────────
// Mock Executor for Pipeline Testing
// ─────────────────────────────────────────────────────────────────

/**
 * Mock Redis store for testing pipeline execution
 */
class MockRedisStore {
  private store: Map<string, { value: unknown; type: string }> = new Map()

  get(key: string): string | null {
    const entry = this.store.get(key)
    if (!entry || entry.type !== 'string') return null
    return entry.value as string
  }

  set(key: string, value: string): 'OK' {
    this.store.set(key, { value, type: 'string' })
    return 'OK'
  }

  incr(key: string): number {
    const entry = this.store.get(key)
    let value = 0
    if (entry) {
      value = parseInt(entry.value as string, 10)
      if (isNaN(value)) {
        throw new Error('ERR value is not an integer or out of range')
      }
    }
    value += 1
    this.store.set(key, { value: String(value), type: 'string' })
    return value
  }

  decr(key: string): number {
    const entry = this.store.get(key)
    let value = 0
    if (entry) {
      value = parseInt(entry.value as string, 10)
      if (isNaN(value)) {
        throw new Error('ERR value is not an integer or out of range')
      }
    }
    value -= 1
    this.store.set(key, { value: String(value), type: 'string' })
    return value
  }

  del(...keys: string[]): number {
    let count = 0
    for (const key of keys) {
      if (this.store.delete(key)) count++
    }
    return count
  }

  exists(...keys: string[]): number {
    let count = 0
    for (const key of keys) {
      if (this.store.has(key)) count++
    }
    return count
  }

  lpush(key: string, ...values: string[]): number {
    const entry = this.store.get(key)
    let list: string[] = []
    if (entry) {
      if (entry.type !== 'list') {
        throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
      }
      list = [...(entry.value as string[])]
    }
    list.unshift(...values.reverse())
    this.store.set(key, { value: list, type: 'list' })
    return list.length
  }

  rpush(key: string, ...values: string[]): number {
    let entry = this.store.get(key)
    let list: string[] = []
    if (entry) {
      if (entry.type !== 'list') {
        throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
      }
      list = entry.value as string[]
    }
    list.push(...values)
    this.store.set(key, { value: list, type: 'list' })
    return list.length
  }

  lrange(key: string, start: number, stop: number): string[] {
    const entry = this.store.get(key)
    if (!entry || entry.type !== 'list') return []
    const list = entry.value as string[]
    const len = list.length
    let startIdx = start < 0 ? len + start : start
    let stopIdx = stop < 0 ? len + stop : stop
    startIdx = Math.max(0, startIdx)
    stopIdx = Math.min(len - 1, stopIdx)
    if (startIdx > stopIdx) return []
    return list.slice(startIdx, stopIdx + 1)
  }

  sadd(key: string, ...members: string[]): number {
    let entry = this.store.get(key)
    let set = new Set<string>()
    if (entry) {
      if (entry.type !== 'set') {
        throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
      }
      set = entry.value as Set<string>
    }
    let added = 0
    for (const member of members) {
      if (!set.has(member)) {
        set.add(member)
        added++
      }
    }
    this.store.set(key, { value: set, type: 'set' })
    return added
  }

  smembers(key: string): string[] {
    const entry = this.store.get(key)
    if (!entry || entry.type !== 'set') return []
    return Array.from(entry.value as Set<string>)
  }

  hset(key: string, field: string, value: string): number {
    let entry = this.store.get(key)
    let hash = new Map<string, string>()
    if (entry) {
      if (entry.type !== 'hash') {
        throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
      }
      hash = entry.value as Map<string, string>
    }
    const isNew = !hash.has(field)
    hash.set(field, value)
    this.store.set(key, { value: hash, type: 'hash' })
    return isNew ? 1 : 0
  }

  hget(key: string, field: string): string | null {
    const entry = this.store.get(key)
    if (!entry || entry.type !== 'hash') return null
    const hash = entry.value as Map<string, string>
    return hash.get(field) ?? null
  }

  hgetall(key: string): Record<string, string> {
    const entry = this.store.get(key)
    if (!entry || entry.type !== 'hash') return {}
    const hash = entry.value as Map<string, string>
    const result: Record<string, string> = {}
    for (const [k, v] of hash) {
      result[k] = v
    }
    return result
  }

  ping(message?: string): string {
    return message ?? 'PONG'
  }

  echo(message: string): string {
    return message
  }

  clear(): void {
    this.store.clear()
  }
}

/**
 * Creates a mock executor that simulates Redis command execution
 */
function createMockExecutor(store: MockRedisStore): (commands: PipelineCommand[]) => Promise<PipelineExecResult> {
  return async (commands: PipelineCommand[]): Promise<PipelineExecResult> => {
    const results: PipelineExecResult = []

    for (const cmd of commands) {
      try {
        let result: unknown

        switch (cmd.method) {
          case 'get':
            result = store.get(cmd.args[0] as string)
            break
          case 'set':
            result = store.set(cmd.args[0] as string, String(cmd.args[1]))
            break
          case 'incr':
            result = store.incr(cmd.args[0] as string)
            break
          case 'decr':
            result = store.decr(cmd.args[0] as string)
            break
          case 'del':
            result = store.del(...(cmd.args as string[]))
            break
          case 'exists':
            result = store.exists(...(cmd.args as string[]))
            break
          case 'lpush':
            result = store.lpush(cmd.args[0] as string, ...(cmd.args.slice(1) as string[]))
            break
          case 'rpush':
            result = store.rpush(cmd.args[0] as string, ...(cmd.args.slice(1) as string[]))
            break
          case 'lrange':
            result = store.lrange(cmd.args[0] as string, cmd.args[1] as number, cmd.args[2] as number)
            break
          case 'sadd':
            result = store.sadd(cmd.args[0] as string, ...(cmd.args.slice(1) as string[]))
            break
          case 'smembers':
            result = store.smembers(cmd.args[0] as string)
            break
          case 'hset':
            result = store.hset(cmd.args[0] as string, cmd.args[1] as string, String(cmd.args[2]))
            break
          case 'hget':
            result = store.hget(cmd.args[0] as string, cmd.args[1] as string)
            break
          case 'hgetall':
            result = store.hgetall(cmd.args[0] as string)
            break
          case 'ping':
            result = store.ping(cmd.args[0] as string | undefined)
            break
          case 'echo':
            result = store.echo(cmd.args[0] as string)
            break
          default:
            throw new Error(`ERR unknown command '${cmd.method}'`)
        }

        results.push([null, result])
      } catch (error) {
        results.push([error as Error, null])
      }
    }

    return results
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('Pipeline', () => {
  let store: MockRedisStore
  let pipeline: Pipeline

  beforeEach(() => {
    store = new MockRedisStore()
    pipeline = new Pipeline(createMockExecutor(store))
  })

  // ─────────────────────────────────────────────────────────────────
  // Basic Pipeline Operations
  // ─────────────────────────────────────────────────────────────────

  describe('Basic Pipeline Operations', () => {
    it('should queue commands without executing', () => {
      pipeline.set('key1', 'value1')
      pipeline.get('key1')
      pipeline.incr('counter')

      expect(pipeline.length).toBe(3)
      // Commands not executed yet, store should be empty
      expect(store.get('key1')).toBeNull()
    })

    it('should execute queued commands and return results in order', async () => {
      pipeline.set('key1', 'value1')
      pipeline.get('key1')
      pipeline.incr('counter')

      const results = await pipeline.exec()

      expect(results).toHaveLength(3)
      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 'value1'])
      expect(results[2]).toEqual([null, 1])
    })

    it('should clear commands after exec', async () => {
      pipeline.set('key1', 'value1')
      await pipeline.exec()

      expect(pipeline.length).toBe(0)
    })

    it('should return empty array when exec called with no commands', async () => {
      const results = await pipeline.exec()
      expect(results).toEqual([])
    })

    it('should support method chaining', async () => {
      const results = await pipeline
        .set('key1', 'value1')
        .set('key2', 'value2')
        .get('key1')
        .get('key2')
        .exec()

      expect(results).toHaveLength(4)
      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 'OK'])
      expect(results[2]).toEqual([null, 'value1'])
      expect(results[3]).toEqual([null, 'value2'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // String Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('String Commands in Pipeline', () => {
    it('should handle SET and GET', async () => {
      pipeline.set('mykey', 'myvalue')
      pipeline.get('mykey')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 'myvalue'])
    })

    it('should handle INCR and DECR', async () => {
      pipeline.set('counter', '10')
      pipeline.incr('counter')
      pipeline.incr('counter')
      pipeline.decr('counter')
      pipeline.get('counter')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 11])
      expect(results[2]).toEqual([null, 12])
      expect(results[3]).toEqual([null, 11])
      expect(results[4]).toEqual([null, '11'])
    })

    it('should handle GET on non-existent key', async () => {
      pipeline.get('nonexistent')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, null])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Key Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('Key Commands in Pipeline', () => {
    it('should handle DEL', async () => {
      store.set('key1', 'value1')
      store.set('key2', 'value2')

      pipeline.del('key1', 'key2', 'nonexistent')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 2])
    })

    it('should handle EXISTS', async () => {
      store.set('key1', 'value1')
      store.set('key2', 'value2')

      pipeline.exists('key1', 'key2', 'nonexistent')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 2])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // List Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('List Commands in Pipeline', () => {
    it('should handle LPUSH and RPUSH', async () => {
      pipeline.lpush('mylist', 'a', 'b', 'c')
      pipeline.rpush('mylist', 'x', 'y', 'z')
      pipeline.lrange('mylist', 0, -1)

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 3])
      expect(results[1]).toEqual([null, 6])
      expect(results[2]).toEqual([null, ['c', 'b', 'a', 'x', 'y', 'z']])
    })

    it('should handle LRANGE', async () => {
      store.lpush('mylist', 'three', 'two', 'one')

      pipeline.lrange('mylist', 0, 1)
      pipeline.lrange('mylist', -2, -1)

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, ['one', 'two']])
      expect(results[1]).toEqual([null, ['two', 'three']])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Set Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('Set Commands in Pipeline', () => {
    it('should handle SADD and SMEMBERS', async () => {
      pipeline.sadd('myset', 'a', 'b', 'c')
      pipeline.sadd('myset', 'a', 'd')
      pipeline.smembers('myset')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 3])
      expect(results[1]).toEqual([null, 1]) // only 'd' is new
      expect(results[2][0]).toBeNull()
      expect((results[2][1] as string[]).sort()).toEqual(['a', 'b', 'c', 'd'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Hash Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('Hash Commands in Pipeline', () => {
    it('should handle HSET and HGET', async () => {
      pipeline.hset('myhash', 'field1', 'value1')
      pipeline.hset('myhash', 'field2', 'value2')
      pipeline.hget('myhash', 'field1')
      pipeline.hget('myhash', 'field2')
      pipeline.hget('myhash', 'nonexistent')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 1])
      expect(results[1]).toEqual([null, 1])
      expect(results[2]).toEqual([null, 'value1'])
      expect(results[3]).toEqual([null, 'value2'])
      expect(results[4]).toEqual([null, null])
    })

    it('should handle HGETALL', async () => {
      store.hset('myhash', 'field1', 'value1')
      store.hset('myhash', 'field2', 'value2')

      pipeline.hgetall('myhash')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, { field1: 'value1', field2: 'value2' }])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Server Commands in Pipeline
  // ─────────────────────────────────────────────────────────────────

  describe('Server Commands in Pipeline', () => {
    it('should handle PING', async () => {
      pipeline.ping()
      pipeline.ping('hello')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 'PONG'])
      expect(results[1]).toEqual([null, 'hello'])
    })

    it('should handle ECHO', async () => {
      pipeline.echo('Hello World')

      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 'Hello World'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Error Handling
  // ─────────────────────────────────────────────────────────────────

  describe('Error Handling', () => {
    it('should handle errors for individual commands', async () => {
      store.set('mykey', 'notanumber')

      pipeline.incr('mykey')
      pipeline.set('otherkey', 'value')
      pipeline.get('otherkey')

      const results = await pipeline.exec()

      // First command should have an error
      expect(results[0][0]).toBeInstanceOf(Error)
      expect((results[0][0] as Error).message).toContain('not an integer')
      expect(results[0][1]).toBeNull()

      // Other commands should succeed
      expect(results[1]).toEqual([null, 'OK'])
      expect(results[2]).toEqual([null, 'value'])
    })

    it('should handle type errors', async () => {
      // Store a string value
      store.set('mystring', 'value')

      // Try to use list command on string key
      pipeline.lpush('mystring', 'item')

      const results = await pipeline.exec()

      expect(results[0][0]).toBeInstanceOf(Error)
      expect((results[0][0] as Error).message).toContain('WRONGTYPE')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Multiple Pipeline Executions
  // ─────────────────────────────────────────────────────────────────

  describe('Multiple Pipeline Executions', () => {
    it('should support multiple exec calls on same pipeline', async () => {
      pipeline.set('key1', 'value1')
      const results1 = await pipeline.exec()

      pipeline.get('key1')
      pipeline.set('key2', 'value2')
      const results2 = await pipeline.exec()

      expect(results1).toHaveLength(1)
      expect(results1[0]).toEqual([null, 'OK'])

      expect(results2).toHaveLength(2)
      expect(results2[0]).toEqual([null, 'value1'])
      expect(results2[1]).toEqual([null, 'OK'])
    })

    it('should maintain state between exec calls', async () => {
      pipeline.set('counter', '0')
      await pipeline.exec()

      pipeline.incr('counter')
      pipeline.incr('counter')
      pipeline.incr('counter')
      const results = await pipeline.exec()

      expect(results[0]).toEqual([null, 1])
      expect(results[1]).toEqual([null, 2])
      expect(results[2]).toEqual([null, 3])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Pipeline Length Property
  // ─────────────────────────────────────────────────────────────────

  describe('Pipeline Length Property', () => {
    it('should track command count correctly', () => {
      expect(pipeline.length).toBe(0)

      pipeline.set('key1', 'value1')
      expect(pipeline.length).toBe(1)

      pipeline.get('key1')
      expect(pipeline.length).toBe(2)

      pipeline.incr('counter')
      pipeline.decr('counter')
      expect(pipeline.length).toBe(4)
    })

    it('should reset length after exec', async () => {
      pipeline.set('key1', 'value1')
      pipeline.get('key1')
      expect(pipeline.length).toBe(2)

      await pipeline.exec()
      expect(pipeline.length).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // execBuffer Alias
  // ─────────────────────────────────────────────────────────────────

  describe('execBuffer Alias', () => {
    it('should work the same as exec', async () => {
      pipeline.set('key1', 'value1')
      pipeline.get('key1')

      const results = await pipeline.execBuffer()

      expect(results).toHaveLength(2)
      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 'value1'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Complex Scenarios
  // ─────────────────────────────────────────────────────────────────

  describe('Complex Scenarios', () => {
    it('should handle mixed command types in single pipeline', async () => {
      pipeline.set('string_key', 'hello')
      pipeline.lpush('list_key', 'a', 'b', 'c')
      pipeline.sadd('set_key', 'x', 'y', 'z')
      pipeline.hset('hash_key', 'field', 'value')
      pipeline.get('string_key')
      pipeline.lrange('list_key', 0, -1)
      pipeline.smembers('set_key')
      pipeline.hget('hash_key', 'field')

      const results = await pipeline.exec()

      expect(results).toHaveLength(8)
      expect(results[0]).toEqual([null, 'OK'])
      expect(results[1]).toEqual([null, 3])
      expect(results[2]).toEqual([null, 3])
      expect(results[3]).toEqual([null, 1])
      expect(results[4]).toEqual([null, 'hello'])
      expect(results[5]).toEqual([null, ['c', 'b', 'a']])
      expect((results[6][1] as string[]).sort()).toEqual(['x', 'y', 'z'])
      expect(results[7]).toEqual([null, 'value'])
    })

    it('should handle large number of commands', async () => {
      const count = 100

      for (let i = 0; i < count; i++) {
        pipeline.set(`key${i}`, `value${i}`)
      }
      for (let i = 0; i < count; i++) {
        pipeline.get(`key${i}`)
      }

      const results = await pipeline.exec()

      expect(results).toHaveLength(count * 2)

      // Check all SET commands succeeded
      for (let i = 0; i < count; i++) {
        expect(results[i]).toEqual([null, 'OK'])
      }

      // Check all GET commands returned correct values
      for (let i = 0; i < count; i++) {
        expect(results[count + i]).toEqual([null, `value${i}`])
      }
    })
  })
})
