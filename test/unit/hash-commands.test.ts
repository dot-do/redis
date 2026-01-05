/**
 * Hash Commands Tests
 *
 * Tests for Redis hash command implementations
 * Covers HGET, HSET, HMGET, HMSET, HGETALL, HDEL, HEXISTS, HINCRBY, etc.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { ScanOptions } from '../../src/types'

// ─────────────────────────────────────────────────────────────────
// Mock Redis Hash Store
// ─────────────────────────────────────────────────────────────────

interface KeyEntry {
  value: Map<string, string>
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisHashStore {
  private store: Map<string, KeyEntry> = new Map()

  private isExpired(key: string): boolean {
    const entry = this.store.get(key)
    if (!entry || entry.expiresAt === null) return false
    return Date.now() > entry.expiresAt
  }

  private cleanExpired(key: string): void {
    if (this.isExpired(key)) {
      this.store.delete(key)
    }
  }

  private assertType(key: string, expectedType: 'hash'): void {
    const entry = this.store.get(key)
    if (entry && entry.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
  }

  private getOrCreateHash(key: string): Map<string, string> {
    this.cleanExpired(key)
    let entry = this.store.get(key)

    if (!entry) {
      entry = {
        value: new Map(),
        type: 'hash',
        expiresAt: null,
      }
      this.store.set(key, entry)
    } else {
      this.assertType(key, 'hash')
    }

    return entry.value
  }

  // ─────────────────────────────────────────────────────────────────
  // HGET/HSET
  // ─────────────────────────────────────────────────────────────────

  hget(key: string, field: string): string | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'hash')
    return entry.value.get(field) ?? null
  }

  hset(key: string, fieldValues: Record<string, string>): number {
    const hash = this.getOrCreateHash(key)
    let added = 0

    for (const [field, value] of Object.entries(fieldValues)) {
      if (!hash.has(field)) {
        added++
      }
      hash.set(field, String(value))
    }

    return added
  }

  hsetnx(key: string, field: string, value: string): 0 | 1 {
    const hash = this.getOrCreateHash(key)
    if (hash.has(field)) {
      return 0
    }
    hash.set(field, value)
    return 1
  }

  // ─────────────────────────────────────────────────────────────────
  // HMGET/HMSET/HGETALL
  // ─────────────────────────────────────────────────────────────────

  hmget(key: string, ...fields: string[]): (string | null)[] {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) {
      return fields.map(() => null)
    }
    this.assertType(key, 'hash')

    return fields.map((field) => entry.value.get(field) ?? null)
  }

  hmset(key: string, fieldValues: Record<string, string>): 'OK' {
    this.hset(key, fieldValues)
    return 'OK'
  }

  hgetall(key: string): Record<string, string> {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return {}
    this.assertType(key, 'hash')

    const result: Record<string, string> = {}
    for (const [field, value] of entry.value) {
      result[field] = value
    }
    return result
  }

  // ─────────────────────────────────────────────────────────────────
  // HDEL/HEXISTS
  // ─────────────────────────────────────────────────────────────────

  hdel(key: string, ...fields: string[]): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'hash')

    let deleted = 0
    for (const field of fields) {
      if (entry.value.delete(field)) {
        deleted++
      }
    }

    // Remove key if hash is empty
    if (entry.value.size === 0) {
      this.store.delete(key)
    }

    return deleted
  }

  hexists(key: string, field: string): 0 | 1 {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'hash')
    return entry.value.has(field) ? 1 : 0
  }

  // ─────────────────────────────────────────────────────────────────
  // HINCRBY/HINCRBYFLOAT
  // ─────────────────────────────────────────────────────────────────

  hincrby(key: string, field: string, increment: number): number {
    const hash = this.getOrCreateHash(key)
    const current = hash.get(field)

    let value = 0
    if (current !== undefined) {
      value = parseInt(current, 10)
      if (isNaN(value)) {
        throw new Error('ERR hash value is not an integer')
      }
    }

    const newValue = value + increment
    hash.set(field, String(newValue))
    return newValue
  }

  hincrbyfloat(key: string, field: string, increment: number): string {
    const hash = this.getOrCreateHash(key)
    const current = hash.get(field)

    let value = 0
    if (current !== undefined) {
      value = parseFloat(current)
      if (isNaN(value)) {
        throw new Error('ERR hash value is not a float')
      }
    }

    const newValue = value + increment
    const stringValue = newValue.toString()
    hash.set(field, stringValue)
    return stringValue
  }

  // ─────────────────────────────────────────────────────────────────
  // HKEYS/HVALS/HLEN
  // ─────────────────────────────────────────────────────────────────

  hkeys(key: string): string[] {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return []
    this.assertType(key, 'hash')
    return Array.from(entry.value.keys())
  }

  hvals(key: string): string[] {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return []
    this.assertType(key, 'hash')
    return Array.from(entry.value.values())
  }

  hlen(key: string): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'hash')
    return entry.value.size
  }

  // ─────────────────────────────────────────────────────────────────
  // HSTRLEN
  // ─────────────────────────────────────────────────────────────────

  hstrlen(key: string, field: string): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'hash')
    const value = entry.value.get(field)
    return value?.length ?? 0
  }

  // ─────────────────────────────────────────────────────────────────
  // HSCAN
  // ─────────────────────────────────────────────────────────────────

  hscan(key: string, cursor: number, options: ScanOptions = {}): [string, string[]] {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return ['0', []]
    this.assertType(key, 'hash')

    const { match, count = 10 } = options
    const allEntries = Array.from(entry.value.entries())
    const matchedEntries: string[] = []

    for (const [field, value] of allEntries) {
      if (match) {
        const regex = new RegExp('^' + match.replace(/\*/g, '.*').replace(/\?/g, '.') + '$')
        if (!regex.test(field)) continue
      }
      matchedEntries.push(field, value)
    }

    const start = cursor
    const end = Math.min(start + count * 2, matchedEntries.length)
    const result = matchedEntries.slice(start, end)
    const nextCursor = end >= matchedEntries.length ? '0' : String(end)

    return [nextCursor, result]
  }

  // ─────────────────────────────────────────────────────────────────
  // HRANDFIELD
  // ─────────────────────────────────────────────────────────────────

  hrandfield(key: string, count?: number, withValues?: boolean): string | string[] | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return count !== undefined ? [] : null
    this.assertType(key, 'hash')

    const fields = Array.from(entry.value.keys())
    if (fields.length === 0) return count !== undefined ? [] : null

    if (count === undefined) {
      // Return single random field
      const randomIndex = Math.floor(Math.random() * fields.length)
      return fields[randomIndex]
    }

    const allowDuplicates = count < 0
    const absCount = Math.abs(count)
    const result: string[] = []

    if (allowDuplicates) {
      for (let i = 0; i < absCount; i++) {
        const randomIndex = Math.floor(Math.random() * fields.length)
        const field = fields[randomIndex]
        result.push(field)
        if (withValues) {
          result.push(entry.value.get(field)!)
        }
      }
    } else {
      const shuffled = [...fields].sort(() => Math.random() - 0.5)
      const selected = shuffled.slice(0, absCount)
      for (const field of selected) {
        result.push(field)
        if (withValues) {
          result.push(entry.value.get(field)!)
        }
      }
    }

    return result
  }

  // ─────────────────────────────────────────────────────────────────
  // Helper to simulate other types for type checking tests
  // ─────────────────────────────────────────────────────────────────

  setStringKey(key: string, value: string): void {
    this.store.set(key, {
      value: new Map([['__string__', value]]),
      type: 'string',
      expiresAt: null,
    })
  }

  // Helper to clear store
  clear(): void {
    this.store.clear()
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('Hash Commands', () => {
  let redis: MockRedisHashStore

  beforeEach(() => {
    redis = new MockRedisHashStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // HGET/HSET Basic Operations
  // ─────────────────────────────────────────────────────────────────

  describe('HGET/HSET Basic Operations', () => {
    it('should HSET and HGET a single field', () => {
      expect(redis.hset('myhash', { field1: 'value1' })).toBe(1)
      expect(redis.hget('myhash', 'field1')).toBe('value1')
    })

    it('should HSET multiple fields at once', () => {
      expect(redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })).toBe(3)
      expect(redis.hget('myhash', 'field1')).toBe('value1')
      expect(redis.hget('myhash', 'field2')).toBe('value2')
      expect(redis.hget('myhash', 'field3')).toBe('value3')
    })

    it('should HGET return null for non-existent field', () => {
      redis.hset('myhash', { field1: 'value1' })
      expect(redis.hget('myhash', 'nonexistent')).toBeNull()
    })

    it('should HGET return null for non-existent key', () => {
      expect(redis.hget('nonexistent', 'field')).toBeNull()
    })

    it('should HSET overwrite existing field', () => {
      redis.hset('myhash', { field1: 'value1' })
      expect(redis.hset('myhash', { field1: 'newvalue' })).toBe(0) // 0 new fields
      expect(redis.hget('myhash', 'field1')).toBe('newvalue')
    })

    it('should HSET return count of new fields only', () => {
      redis.hset('myhash', { field1: 'value1' })
      expect(redis.hset('myhash', { field1: 'new1', field2: 'value2' })).toBe(1) // Only field2 is new
    })

    it('should handle empty string value', () => {
      redis.hset('myhash', { field1: '' })
      expect(redis.hget('myhash', 'field1')).toBe('')
    })

    it('should handle special characters in value', () => {
      redis.hset('myhash', { field1: 'hello\nworld\ttab' })
      expect(redis.hget('myhash', 'field1')).toBe('hello\nworld\ttab')
    })

    it('should handle unicode in field and value', () => {
      redis.hset('myhash', { 'field': 'value' })
      expect(redis.hget('myhash', 'field')).toBe('value')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HSETNX
  // ─────────────────────────────────────────────────────────────────

  describe('HSETNX', () => {
    it('should HSETNX when field does not exist', () => {
      expect(redis.hsetnx('myhash', 'field1', 'value1')).toBe(1)
      expect(redis.hget('myhash', 'field1')).toBe('value1')
    })

    it('should not HSETNX when field exists', () => {
      redis.hset('myhash', { field1: 'original' })
      expect(redis.hsetnx('myhash', 'field1', 'newvalue')).toBe(0)
      expect(redis.hget('myhash', 'field1')).toBe('original')
    })

    it('should HSETNX create hash if key does not exist', () => {
      expect(redis.hsetnx('newhash', 'field1', 'value1')).toBe(1)
      expect(redis.hget('newhash', 'field1')).toBe('value1')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HMGET/HMSET/HGETALL
  // ─────────────────────────────────────────────────────────────────

  describe('HMGET/HMSET/HGETALL', () => {
    describe('HMGET', () => {
      it('should HMGET multiple existing fields', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        const result = redis.hmget('myhash', 'field1', 'field2', 'field3')
        expect(result).toEqual(['value1', 'value2', 'value3'])
      })

      it('should HMGET with some non-existent fields', () => {
        redis.hset('myhash', { field1: 'value1', field3: 'value3' })
        const result = redis.hmget('myhash', 'field1', 'field2', 'field3')
        expect(result).toEqual(['value1', null, 'value3'])
      })

      it('should HMGET return all nulls for non-existent key', () => {
        const result = redis.hmget('nonexistent', 'a', 'b', 'c')
        expect(result).toEqual([null, null, null])
      })

      it('should HMGET single field', () => {
        redis.hset('myhash', { field1: 'value1' })
        const result = redis.hmget('myhash', 'field1')
        expect(result).toEqual(['value1'])
      })
    })

    describe('HMSET', () => {
      it('should HMSET multiple fields', () => {
        expect(redis.hmset('myhash', { field1: 'value1', field2: 'value2' })).toBe('OK')
        expect(redis.hget('myhash', 'field1')).toBe('value1')
        expect(redis.hget('myhash', 'field2')).toBe('value2')
      })

      it('should HMSET always return OK', () => {
        redis.hset('myhash', { field1: 'original' })
        expect(redis.hmset('myhash', { field1: 'new', field2: 'value2' })).toBe('OK')
      })
    })

    describe('HGETALL', () => {
      it('should HGETALL return all field-value pairs', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        const result = redis.hgetall('myhash')
        expect(result).toEqual({
          field1: 'value1',
          field2: 'value2',
          field3: 'value3',
        })
      })

      it('should HGETALL return empty object for non-existent key', () => {
        const result = redis.hgetall('nonexistent')
        expect(result).toEqual({})
      })

      it('should HGETALL return empty object after all fields deleted', () => {
        redis.hset('myhash', { field1: 'value1' })
        redis.hdel('myhash', 'field1')
        const result = redis.hgetall('myhash')
        expect(result).toEqual({})
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HDEL/HEXISTS
  // ─────────────────────────────────────────────────────────────────

  describe('HDEL/HEXISTS', () => {
    describe('HDEL', () => {
      it('should HDEL single field', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2' })
        expect(redis.hdel('myhash', 'field1')).toBe(1)
        expect(redis.hget('myhash', 'field1')).toBeNull()
        expect(redis.hget('myhash', 'field2')).toBe('value2')
      })

      it('should HDEL multiple fields', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        expect(redis.hdel('myhash', 'field1', 'field2')).toBe(2)
        expect(redis.hget('myhash', 'field1')).toBeNull()
        expect(redis.hget('myhash', 'field2')).toBeNull()
        expect(redis.hget('myhash', 'field3')).toBe('value3')
      })

      it('should HDEL return 0 for non-existent field', () => {
        redis.hset('myhash', { field1: 'value1' })
        expect(redis.hdel('myhash', 'nonexistent')).toBe(0)
      })

      it('should HDEL return 0 for non-existent key', () => {
        expect(redis.hdel('nonexistent', 'field')).toBe(0)
      })

      it('should HDEL return count of actually deleted fields', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2' })
        expect(redis.hdel('myhash', 'field1', 'nonexistent', 'field2')).toBe(2)
      })
    })

    describe('HEXISTS', () => {
      it('should HEXISTS return 1 for existing field', () => {
        redis.hset('myhash', { field1: 'value1' })
        expect(redis.hexists('myhash', 'field1')).toBe(1)
      })

      it('should HEXISTS return 0 for non-existent field', () => {
        redis.hset('myhash', { field1: 'value1' })
        expect(redis.hexists('myhash', 'nonexistent')).toBe(0)
      })

      it('should HEXISTS return 0 for non-existent key', () => {
        expect(redis.hexists('nonexistent', 'field')).toBe(0)
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HINCRBY/HINCRBYFLOAT
  // ─────────────────────────────────────────────────────────────────

  describe('HINCRBY/HINCRBYFLOAT', () => {
    describe('HINCRBY', () => {
      it('should HINCRBY existing numeric field', () => {
        redis.hset('myhash', { counter: '10' })
        expect(redis.hincrby('myhash', 'counter', 5)).toBe(15)
        expect(redis.hget('myhash', 'counter')).toBe('15')
      })

      it('should HINCRBY non-existent field (starts from 0)', () => {
        redis.hset('myhash', { other: 'value' })
        expect(redis.hincrby('myhash', 'counter', 5)).toBe(5)
      })

      it('should HINCRBY non-existent key (creates hash)', () => {
        expect(redis.hincrby('newhash', 'counter', 10)).toBe(10)
        expect(redis.hget('newhash', 'counter')).toBe('10')
      })

      it('should HINCRBY with negative increment', () => {
        redis.hset('myhash', { counter: '10' })
        expect(redis.hincrby('myhash', 'counter', -3)).toBe(7)
      })

      it('should throw error for HINCRBY on non-integer field', () => {
        redis.hset('myhash', { field: 'notanumber' })
        expect(() => redis.hincrby('myhash', 'field', 1)).toThrow('ERR hash value is not an integer')
      })
    })

    describe('HINCRBYFLOAT', () => {
      it('should HINCRBYFLOAT existing numeric field', () => {
        redis.hset('myhash', { counter: '10.5' })
        expect(parseFloat(redis.hincrbyfloat('myhash', 'counter', 0.1))).toBeCloseTo(10.6)
      })

      it('should HINCRBYFLOAT on integer field', () => {
        redis.hset('myhash', { counter: '10' })
        expect(parseFloat(redis.hincrbyfloat('myhash', 'counter', 0.5))).toBeCloseTo(10.5)
      })

      it('should HINCRBYFLOAT non-existent field (starts from 0)', () => {
        expect(parseFloat(redis.hincrbyfloat('myhash', 'counter', 3.14))).toBeCloseTo(3.14)
      })

      it('should HINCRBYFLOAT with negative increment', () => {
        redis.hset('myhash', { counter: '10.0' })
        expect(parseFloat(redis.hincrbyfloat('myhash', 'counter', -0.5))).toBeCloseTo(9.5)
      })

      it('should throw error for HINCRBYFLOAT on non-numeric field', () => {
        redis.hset('myhash', { field: 'notanumber' })
        expect(() => redis.hincrbyfloat('myhash', 'field', 1.0)).toThrow('ERR hash value is not a float')
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HKEYS/HVALS/HLEN
  // ─────────────────────────────────────────────────────────────────

  describe('HKEYS/HVALS/HLEN', () => {
    describe('HKEYS', () => {
      it('should HKEYS return all field names', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        const keys = redis.hkeys('myhash')
        expect(keys).toHaveLength(3)
        expect(keys).toContain('field1')
        expect(keys).toContain('field2')
        expect(keys).toContain('field3')
      })

      it('should HKEYS return empty array for non-existent key', () => {
        expect(redis.hkeys('nonexistent')).toEqual([])
      })

      it('should HKEYS return empty array for empty hash', () => {
        redis.hset('myhash', { field1: 'value1' })
        redis.hdel('myhash', 'field1')
        expect(redis.hkeys('myhash')).toEqual([])
      })
    })

    describe('HVALS', () => {
      it('should HVALS return all values', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        const vals = redis.hvals('myhash')
        expect(vals).toHaveLength(3)
        expect(vals).toContain('value1')
        expect(vals).toContain('value2')
        expect(vals).toContain('value3')
      })

      it('should HVALS return empty array for non-existent key', () => {
        expect(redis.hvals('nonexistent')).toEqual([])
      })
    })

    describe('HLEN', () => {
      it('should HLEN return number of fields', () => {
        redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
        expect(redis.hlen('myhash')).toBe(3)
      })

      it('should HLEN return 0 for non-existent key', () => {
        expect(redis.hlen('nonexistent')).toBe(0)
      })

      it('should HLEN update after adding/removing fields', () => {
        redis.hset('myhash', { field1: 'value1' })
        expect(redis.hlen('myhash')).toBe(1)

        redis.hset('myhash', { field2: 'value2' })
        expect(redis.hlen('myhash')).toBe(2)

        redis.hdel('myhash', 'field1')
        expect(redis.hlen('myhash')).toBe(1)
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HSTRLEN
  // ─────────────────────────────────────────────────────────────────

  describe('HSTRLEN', () => {
    it('should HSTRLEN return length of field value', () => {
      redis.hset('myhash', { field1: 'Hello World' })
      expect(redis.hstrlen('myhash', 'field1')).toBe(11)
    })

    it('should HSTRLEN return 0 for non-existent field', () => {
      redis.hset('myhash', { field1: 'value1' })
      expect(redis.hstrlen('myhash', 'nonexistent')).toBe(0)
    })

    it('should HSTRLEN return 0 for non-existent key', () => {
      expect(redis.hstrlen('nonexistent', 'field')).toBe(0)
    })

    it('should HSTRLEN return 0 for empty string value', () => {
      redis.hset('myhash', { field1: '' })
      expect(redis.hstrlen('myhash', 'field1')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HSCAN
  // ─────────────────────────────────────────────────────────────────

  describe('HSCAN', () => {
    it('should HSCAN return all field-value pairs', () => {
      redis.hset('myhash', { field1: 'value1', field2: 'value2' })
      const [cursor, result] = redis.hscan('myhash', 0)
      expect(cursor).toBe('0')
      expect(result).toHaveLength(4) // 2 field-value pairs
      expect(result).toContain('field1')
      expect(result).toContain('value1')
      expect(result).toContain('field2')
      expect(result).toContain('value2')
    })

    it('should HSCAN with MATCH pattern', () => {
      redis.hset('myhash', { name: 'Alice', age: '30', city: 'NYC', nickname: 'Ali' })
      const [cursor, result] = redis.hscan('myhash', 0, { match: 'n*' })
      expect(result).toContain('name')
      expect(result).toContain('nickname')
      expect(result).not.toContain('age')
      expect(result).not.toContain('city')
    })

    it('should HSCAN return empty for non-existent key', () => {
      const [cursor, result] = redis.hscan('nonexistent', 0)
      expect(cursor).toBe('0')
      expect(result).toEqual([])
    })

    it('should HSCAN with COUNT limit', () => {
      for (let i = 0; i < 20; i++) {
        redis.hset('myhash', { [`field${i}`]: `value${i}` })
      }
      const [cursor, result] = redis.hscan('myhash', 0, { count: 5 })
      expect(result.length).toBeLessThanOrEqual(10) // 5 pairs = 10 elements max
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HRANDFIELD
  // ─────────────────────────────────────────────────────────────────

  describe('HRANDFIELD', () => {
    it('should HRANDFIELD return a random field', () => {
      redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
      const result = redis.hrandfield('myhash')
      expect(['field1', 'field2', 'field3']).toContain(result)
    })

    it('should HRANDFIELD return null for non-existent key', () => {
      expect(redis.hrandfield('nonexistent')).toBeNull()
    })

    it('should HRANDFIELD with count return multiple fields', () => {
      redis.hset('myhash', { field1: 'value1', field2: 'value2', field3: 'value3' })
      const result = redis.hrandfield('myhash', 2) as string[]
      expect(result).toHaveLength(2)
    })

    it('should HRANDFIELD with WITHVALUES return field-value pairs', () => {
      redis.hset('myhash', { field1: 'value1', field2: 'value2' })
      const result = redis.hrandfield('myhash', 1, true) as string[]
      expect(result).toHaveLength(2) // 1 field + 1 value
    })

    it('should HRANDFIELD with negative count allow duplicates', () => {
      redis.hset('myhash', { field1: 'value1' })
      const result = redis.hrandfield('myhash', -5) as string[]
      expect(result).toHaveLength(5)
      // All should be 'field1' since it's the only field
      result.forEach((f) => expect(f).toBe('field1'))
    })

    it('should HRANDFIELD return empty array for non-existent key with count', () => {
      const result = redis.hrandfield('nonexistent', 3)
      expect(result).toEqual([])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Type Checking
  // ─────────────────────────────────────────────────────────────────

  describe('Type Checking', () => {
    it('should throw WRONGTYPE for hash command on string key', () => {
      redis.setStringKey('mystring', 'value')
      expect(() => redis.hget('mystring', 'field')).toThrow('WRONGTYPE')
    })

    it('should throw WRONGTYPE for HSET on string key', () => {
      redis.setStringKey('mystring', 'value')
      expect(() => redis.hset('mystring', { field: 'value' })).toThrow('WRONGTYPE')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Edge Cases
  // ─────────────────────────────────────────────────────────────────

  describe('Edge Cases', () => {
    it('should handle field with special characters', () => {
      redis.hset('myhash', { 'field:with:colons': 'value' })
      expect(redis.hget('myhash', 'field:with:colons')).toBe('value')
    })

    it('should handle field with spaces', () => {
      redis.hset('myhash', { 'field with spaces': 'value' })
      expect(redis.hget('myhash', 'field with spaces')).toBe('value')
    })

    it('should handle many fields', () => {
      const fields: Record<string, string> = {}
      for (let i = 0; i < 1000; i++) {
        fields[`field${i}`] = `value${i}`
      }
      redis.hset('myhash', fields)
      expect(redis.hlen('myhash')).toBe(1000)
      expect(redis.hget('myhash', 'field500')).toBe('value500')
    })

    it('should handle multiple operations in sequence', () => {
      redis.hset('myhash', { counter: '0' })
      for (let i = 0; i < 10; i++) {
        redis.hincrby('myhash', 'counter', 1)
      }
      expect(redis.hget('myhash', 'counter')).toBe('10')
    })

    it('should delete hash when last field is removed', () => {
      redis.hset('myhash', { field1: 'value1' })
      redis.hdel('myhash', 'field1')
      expect(redis.hlen('myhash')).toBe(0)
      expect(redis.hgetall('myhash')).toEqual({})
    })
  })
})
