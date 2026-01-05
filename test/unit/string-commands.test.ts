/**
 * String Commands Tests
 *
 * Tests for Redis string command implementations
 * Covers GET, SET, INCR, DECR, MGET, MSET, APPEND, STRLEN, GETRANGE
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { SetOptions } from '../../src/types'

// ─────────────────────────────────────────────────────────────────
// Mock Redis String Store
// ─────────────────────────────────────────────────────────────────

interface KeyEntry {
  value: string
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisStringStore {
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

  private assertType(key: string, expectedType: 'string'): void {
    const entry = this.store.get(key)
    if (entry && entry.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // GET/SET
  // ─────────────────────────────────────────────────────────────────

  get(key: string): string | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'string')
    return entry.value
  }

  set(key: string, value: string, options: SetOptions = {}): string | null {
    this.cleanExpired(key)

    const existing = this.store.get(key)
    const oldValue = existing?.value ?? null

    // NX: Only set if key does not exist
    if (options.nx && existing) {
      return options.get ? oldValue : null
    }

    // XX: Only set if key exists
    if (options.xx && !existing) {
      return options.get ? null : null
    }

    // Calculate expiration
    let expiresAt: number | null = null
    if (options.ex !== undefined) {
      expiresAt = Date.now() + options.ex * 1000
    } else if (options.px !== undefined) {
      expiresAt = Date.now() + options.px
    } else if (options.exat !== undefined) {
      expiresAt = options.exat * 1000
    } else if (options.pxat !== undefined) {
      expiresAt = options.pxat
    } else if (options.keepttl && existing) {
      expiresAt = existing.expiresAt
    }

    this.store.set(key, {
      value: String(value),
      type: 'string',
      expiresAt,
    })

    return options.get ? oldValue : 'OK'
  }

  // ─────────────────────────────────────────────────────────────────
  // INCR/DECR
  // ─────────────────────────────────────────────────────────────────

  incr(key: string): number {
    return this.incrby(key, 1)
  }

  decr(key: string): number {
    return this.decrby(key, 1)
  }

  incrby(key: string, increment: number): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)

    let value = 0
    if (entry) {
      this.assertType(key, 'string')
      value = parseInt(entry.value, 10)
      if (isNaN(value)) {
        throw new Error('ERR value is not an integer or out of range')
      }
    }

    const newValue = value + increment
    this.store.set(key, {
      value: String(newValue),
      type: 'string',
      expiresAt: entry?.expiresAt ?? null,
    })

    return newValue
  }

  decrby(key: string, decrement: number): number {
    return this.incrby(key, -decrement)
  }

  incrbyfloat(key: string, increment: number): string {
    this.cleanExpired(key)
    const entry = this.store.get(key)

    let value = 0
    if (entry) {
      this.assertType(key, 'string')
      value = parseFloat(entry.value)
      if (isNaN(value)) {
        throw new Error('ERR value is not a valid float')
      }
    }

    const newValue = value + increment
    const stringValue = newValue.toString()

    this.store.set(key, {
      value: stringValue,
      type: 'string',
      expiresAt: entry?.expiresAt ?? null,
    })

    return stringValue
  }

  // ─────────────────────────────────────────────────────────────────
  // MGET/MSET
  // ─────────────────────────────────────────────────────────────────

  mget(...keys: string[]): (string | null)[] {
    return keys.map((key) => {
      try {
        return this.get(key)
      } catch {
        return null
      }
    })
  }

  mset(keyValues: Record<string, string>): 'OK' {
    for (const [key, value] of Object.entries(keyValues)) {
      this.set(key, value)
    }
    return 'OK'
  }

  msetnx(keyValues: Record<string, string>): 0 | 1 {
    // Check if any key exists
    for (const key of Object.keys(keyValues)) {
      this.cleanExpired(key)
      if (this.store.has(key)) {
        return 0
      }
    }

    // Set all keys
    for (const [key, value] of Object.entries(keyValues)) {
      this.set(key, value)
    }
    return 1
  }

  // ─────────────────────────────────────────────────────────────────
  // APPEND/STRLEN/GETRANGE
  // ─────────────────────────────────────────────────────────────────

  append(key: string, value: string): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)

    if (entry) {
      this.assertType(key, 'string')
      const newValue = entry.value + value
      this.store.set(key, {
        value: newValue,
        type: 'string',
        expiresAt: entry.expiresAt,
      })
      return newValue.length
    }

    this.store.set(key, {
      value,
      type: 'string',
      expiresAt: null,
    })
    return value.length
  }

  strlen(key: string): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'string')
    return entry.value.length
  }

  getrange(key: string, start: number, end: number): string {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return ''
    this.assertType(key, 'string')

    const value = entry.value
    const len = value.length

    // Handle negative indices
    let startIdx = start < 0 ? len + start : start
    let endIdx = end < 0 ? len + end : end

    // Clamp to valid range
    startIdx = Math.max(0, startIdx)
    endIdx = Math.min(len - 1, endIdx)

    if (startIdx > endIdx) return ''
    return value.slice(startIdx, endIdx + 1)
  }

  setrange(key: string, offset: number, value: string): number {
    this.cleanExpired(key)
    let entry = this.store.get(key)

    if (entry) {
      this.assertType(key, 'string')
    }

    let currentValue = entry?.value ?? ''

    // Pad with null bytes if offset is beyond current length
    if (offset > currentValue.length) {
      currentValue = currentValue.padEnd(offset, '\x00')
    }

    // Replace portion of string
    const before = currentValue.slice(0, offset)
    const after = currentValue.slice(offset + value.length)
    const newValue = before + value + after

    this.store.set(key, {
      value: newValue,
      type: 'string',
      expiresAt: entry?.expiresAt ?? null,
    })

    return newValue.length
  }

  // ─────────────────────────────────────────────────────────────────
  // SETNX/SETEX/PSETEX/GETSET/GETDEL/GETEX
  // ─────────────────────────────────────────────────────────────────

  setnx(key: string, value: string): 0 | 1 {
    this.cleanExpired(key)
    if (this.store.has(key)) {
      return 0
    }
    this.set(key, value)
    return 1
  }

  setex(key: string, seconds: number, value: string): 'OK' {
    return this.set(key, value, { ex: seconds }) as 'OK'
  }

  psetex(key: string, milliseconds: number, value: string): 'OK' {
    return this.set(key, value, { px: milliseconds }) as 'OK'
  }

  getset(key: string, value: string): string | null {
    const oldValue = this.get(key)
    this.set(key, value)
    return oldValue
  }

  getdel(key: string): string | null {
    const value = this.get(key)
    if (value !== null) {
      this.store.delete(key)
    }
    return value
  }

  getex(key: string, options: { ex?: number; px?: number; exat?: number; pxat?: number; persist?: boolean } = {}): string | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'string')

    // Update TTL if options provided
    if (options.ex !== undefined) {
      entry.expiresAt = Date.now() + options.ex * 1000
    } else if (options.px !== undefined) {
      entry.expiresAt = Date.now() + options.px
    } else if (options.exat !== undefined) {
      entry.expiresAt = options.exat * 1000
    } else if (options.pxat !== undefined) {
      entry.expiresAt = options.pxat
    } else if (options.persist) {
      entry.expiresAt = null
    }

    return entry.value
  }

  // Helper to clear store
  clear(): void {
    this.store.clear()
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('String Commands', () => {
  let redis: MockRedisStringStore

  beforeEach(() => {
    redis = new MockRedisStringStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // GET/SET Basic Operations
  // ─────────────────────────────────────────────────────────────────

  describe('GET/SET Basic Operations', () => {
    it('should SET and GET a string value', () => {
      expect(redis.set('mykey', 'myvalue')).toBe('OK')
      expect(redis.get('mykey')).toBe('myvalue')
    })

    it('should GET return null for non-existent key', () => {
      expect(redis.get('nonexistent')).toBeNull()
    })

    it('should overwrite existing value', () => {
      redis.set('mykey', 'value1')
      redis.set('mykey', 'value2')
      expect(redis.get('mykey')).toBe('value2')
    })

    it('should handle empty string value', () => {
      redis.set('mykey', '')
      expect(redis.get('mykey')).toBe('')
    })

    it('should handle numeric string value', () => {
      redis.set('mykey', '12345')
      expect(redis.get('mykey')).toBe('12345')
    })

    it('should handle special characters', () => {
      redis.set('mykey', 'hello\nworld\ttab')
      expect(redis.get('mykey')).toBe('hello\nworld\ttab')
    })

    it('should handle unicode characters', () => {
      redis.set('mykey', 'Hello World')
      expect(redis.get('mykey')).toBe('Hello World')
    })

    it('should handle binary-like strings', () => {
      redis.set('mykey', '\x00\x01\x02\x03')
      expect(redis.get('mykey')).toBe('\x00\x01\x02\x03')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // SET with EX, PX, NX, XX Options
  // ─────────────────────────────────────────────────────────────────

  describe('SET with Options', () => {
    describe('EX option (seconds)', () => {
      it('should SET with EX expiration', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'myvalue', { ex: 10 })
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(5000) // 5 seconds
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(6000) // 11 seconds total
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('PX option (milliseconds)', () => {
      it('should SET with PX expiration', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'myvalue', { px: 500 })
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(300)
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(300) // 600ms total
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('EXAT option (unix timestamp seconds)', () => {
      it('should SET with EXAT expiration', () => {
        vi.useFakeTimers()
        const futureTimestamp = Math.floor(Date.now() / 1000) + 10 // 10 seconds from now
        redis.set('mykey', 'myvalue', { exat: futureTimestamp })
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(11000)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('PXAT option (unix timestamp milliseconds)', () => {
      it('should SET with PXAT expiration', () => {
        vi.useFakeTimers()
        const futureTimestamp = Date.now() + 1000 // 1 second from now
        redis.set('mykey', 'myvalue', { pxat: futureTimestamp })
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(1100)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('NX option', () => {
      it('should SET with NX when key does not exist', () => {
        expect(redis.set('mykey', 'myvalue', { nx: true })).toBe('OK')
        expect(redis.get('mykey')).toBe('myvalue')
      })

      it('should not SET with NX when key exists', () => {
        redis.set('mykey', 'original')
        expect(redis.set('mykey', 'newvalue', { nx: true })).toBeNull()
        expect(redis.get('mykey')).toBe('original')
      })
    })

    describe('XX option', () => {
      it('should SET with XX when key exists', () => {
        redis.set('mykey', 'original')
        expect(redis.set('mykey', 'newvalue', { xx: true })).toBe('OK')
        expect(redis.get('mykey')).toBe('newvalue')
      })

      it('should not SET with XX when key does not exist', () => {
        expect(redis.set('mykey', 'myvalue', { xx: true })).toBeNull()
        expect(redis.get('mykey')).toBeNull()
      })
    })

    describe('KEEPTTL option', () => {
      it('should preserve TTL with KEEPTTL', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'value1', { ex: 10 })
        vi.advanceTimersByTime(5000)

        redis.set('mykey', 'value2', { keepttl: true })
        expect(redis.get('mykey')).toBe('value2')

        vi.advanceTimersByTime(6000)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('GET option', () => {
      it('should return old value with GET option', () => {
        redis.set('mykey', 'oldvalue')
        const result = redis.set('mykey', 'newvalue', { get: true })
        expect(result).toBe('oldvalue')
        expect(redis.get('mykey')).toBe('newvalue')
      })

      it('should return null with GET option for new key', () => {
        const result = redis.set('newkey', 'value', { get: true })
        expect(result).toBeNull()
        expect(redis.get('newkey')).toBe('value')
      })
    })

    describe('Combined options', () => {
      it('should handle NX with EX', () => {
        vi.useFakeTimers()
        expect(redis.set('mykey', 'myvalue', { nx: true, ex: 5 })).toBe('OK')
        expect(redis.set('mykey', 'newvalue', { nx: true, ex: 5 })).toBeNull()

        vi.advanceTimersByTime(6000)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })

      it('should handle XX with PX', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'original')
        expect(redis.set('mykey', 'newvalue', { xx: true, px: 1000 })).toBe('OK')

        vi.advanceTimersByTime(1100)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // INCR/DECR Operations
  // ─────────────────────────────────────────────────────────────────

  describe('INCR/DECR Operations', () => {
    describe('INCR', () => {
      it('should INCR non-existent key starting from 0', () => {
        expect(redis.incr('counter')).toBe(1)
        expect(redis.get('counter')).toBe('1')
      })

      it('should INCR existing numeric value', () => {
        redis.set('counter', '10')
        expect(redis.incr('counter')).toBe(11)
        expect(redis.get('counter')).toBe('11')
      })

      it('should throw error for non-numeric value', () => {
        redis.set('mykey', 'notanumber')
        expect(() => redis.incr('mykey')).toThrow('ERR value is not an integer or out of range')
      })

      it('should handle negative numbers', () => {
        redis.set('counter', '-5')
        expect(redis.incr('counter')).toBe(-4)
      })
    })

    describe('DECR', () => {
      it('should DECR non-existent key starting from 0', () => {
        expect(redis.decr('counter')).toBe(-1)
        expect(redis.get('counter')).toBe('-1')
      })

      it('should DECR existing numeric value', () => {
        redis.set('counter', '10')
        expect(redis.decr('counter')).toBe(9)
      })

      it('should throw error for non-numeric value', () => {
        redis.set('mykey', 'notanumber')
        expect(() => redis.decr('mykey')).toThrow('ERR value is not an integer or out of range')
      })
    })

    describe('INCRBY', () => {
      it('should INCRBY specified amount', () => {
        redis.set('counter', '10')
        expect(redis.incrby('counter', 5)).toBe(15)
      })

      it('should INCRBY negative amount', () => {
        redis.set('counter', '10')
        expect(redis.incrby('counter', -3)).toBe(7)
      })

      it('should INCRBY on non-existent key', () => {
        expect(redis.incrby('counter', 100)).toBe(100)
      })
    })

    describe('DECRBY', () => {
      it('should DECRBY specified amount', () => {
        redis.set('counter', '10')
        expect(redis.decrby('counter', 3)).toBe(7)
      })

      it('should DECRBY on non-existent key', () => {
        expect(redis.decrby('counter', 5)).toBe(-5)
      })
    })

    describe('INCRBYFLOAT', () => {
      it('should INCRBYFLOAT on integer value', () => {
        redis.set('counter', '10')
        expect(parseFloat(redis.incrbyfloat('counter', 0.5))).toBeCloseTo(10.5)
      })

      it('should INCRBYFLOAT on float value', () => {
        redis.set('counter', '10.5')
        expect(parseFloat(redis.incrbyfloat('counter', 0.1))).toBeCloseTo(10.6)
      })

      it('should INCRBYFLOAT negative amount', () => {
        redis.set('counter', '10.0')
        expect(parseFloat(redis.incrbyfloat('counter', -0.5))).toBeCloseTo(9.5)
      })

      it('should INCRBYFLOAT on non-existent key', () => {
        expect(parseFloat(redis.incrbyfloat('counter', 3.14))).toBeCloseTo(3.14)
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // MGET/MSET Operations
  // ─────────────────────────────────────────────────────────────────

  describe('MGET/MSET Operations', () => {
    describe('MGET', () => {
      it('should MGET multiple existing keys', () => {
        redis.set('key1', 'value1')
        redis.set('key2', 'value2')
        redis.set('key3', 'value3')

        const result = redis.mget('key1', 'key2', 'key3')
        expect(result).toEqual(['value1', 'value2', 'value3'])
      })

      it('should MGET with some non-existent keys', () => {
        redis.set('key1', 'value1')
        redis.set('key3', 'value3')

        const result = redis.mget('key1', 'key2', 'key3')
        expect(result).toEqual(['value1', null, 'value3'])
      })

      it('should MGET all non-existent keys', () => {
        const result = redis.mget('a', 'b', 'c')
        expect(result).toEqual([null, null, null])
      })

      it('should MGET single key', () => {
        redis.set('key1', 'value1')
        const result = redis.mget('key1')
        expect(result).toEqual(['value1'])
      })

      it('should MGET empty list', () => {
        const result = redis.mget()
        expect(result).toEqual([])
      })
    })

    describe('MSET', () => {
      it('should MSET multiple key-value pairs', () => {
        expect(redis.mset({ key1: 'value1', key2: 'value2', key3: 'value3' })).toBe('OK')
        expect(redis.get('key1')).toBe('value1')
        expect(redis.get('key2')).toBe('value2')
        expect(redis.get('key3')).toBe('value3')
      })

      it('should MSET single key-value pair', () => {
        expect(redis.mset({ key1: 'value1' })).toBe('OK')
        expect(redis.get('key1')).toBe('value1')
      })

      it('should MSET overwrite existing keys', () => {
        redis.set('key1', 'old')
        redis.mset({ key1: 'new', key2: 'value2' })
        expect(redis.get('key1')).toBe('new')
      })
    })

    describe('MSETNX', () => {
      it('should MSETNX when no keys exist', () => {
        expect(redis.msetnx({ key1: 'value1', key2: 'value2' })).toBe(1)
        expect(redis.get('key1')).toBe('value1')
        expect(redis.get('key2')).toBe('value2')
      })

      it('should not MSETNX when any key exists', () => {
        redis.set('key1', 'existing')
        expect(redis.msetnx({ key1: 'value1', key2: 'value2' })).toBe(0)
        expect(redis.get('key1')).toBe('existing')
        expect(redis.get('key2')).toBeNull()
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // APPEND, STRLEN, GETRANGE
  // ─────────────────────────────────────────────────────────────────

  describe('APPEND, STRLEN, GETRANGE', () => {
    describe('APPEND', () => {
      it('should APPEND to existing key', () => {
        redis.set('mykey', 'Hello')
        expect(redis.append('mykey', ' World')).toBe(11)
        expect(redis.get('mykey')).toBe('Hello World')
      })

      it('should APPEND to non-existent key (creates it)', () => {
        expect(redis.append('mykey', 'value')).toBe(5)
        expect(redis.get('mykey')).toBe('value')
      })

      it('should APPEND empty string', () => {
        redis.set('mykey', 'value')
        expect(redis.append('mykey', '')).toBe(5)
        expect(redis.get('mykey')).toBe('value')
      })
    })

    describe('STRLEN', () => {
      it('should return STRLEN of existing key', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.strlen('mykey')).toBe(11)
      })

      it('should return 0 for non-existent key', () => {
        expect(redis.strlen('nonexistent')).toBe(0)
      })

      it('should return 0 for empty string', () => {
        redis.set('mykey', '')
        expect(redis.strlen('mykey')).toBe(0)
      })
    })

    describe('GETRANGE', () => {
      it('should GETRANGE with positive indices', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.getrange('mykey', 0, 4)).toBe('Hello')
      })

      it('should GETRANGE with negative indices', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.getrange('mykey', -5, -1)).toBe('World')
      })

      it('should GETRANGE with mixed indices', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.getrange('mykey', 6, -1)).toBe('World')
      })

      it('should GETRANGE entire string', () => {
        redis.set('mykey', 'Hello')
        expect(redis.getrange('mykey', 0, -1)).toBe('Hello')
      })

      it('should GETRANGE return empty for out of range', () => {
        redis.set('mykey', 'Hello')
        expect(redis.getrange('mykey', 10, 20)).toBe('')
      })

      it('should GETRANGE return empty for non-existent key', () => {
        expect(redis.getrange('nonexistent', 0, 5)).toBe('')
      })

      it('should GETRANGE handle start > end', () => {
        redis.set('mykey', 'Hello')
        expect(redis.getrange('mykey', 4, 2)).toBe('')
      })
    })

    describe('SETRANGE', () => {
      it('should SETRANGE in middle of string', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.setrange('mykey', 6, 'Redis')).toBe(11)
        expect(redis.get('mykey')).toBe('Hello Redis')
      })

      it('should SETRANGE at beginning', () => {
        redis.set('mykey', 'Hello World')
        expect(redis.setrange('mykey', 0, 'Greet')).toBe(11)
        expect(redis.get('mykey')).toBe('Greet World')
      })

      it('should SETRANGE extend string', () => {
        redis.set('mykey', 'Hello')
        expect(redis.setrange('mykey', 5, ' World')).toBe(11)
        expect(redis.get('mykey')).toBe('Hello World')
      })

      it('should SETRANGE with padding', () => {
        redis.set('mykey', 'Hi')
        const len = redis.setrange('mykey', 5, 'World')
        expect(len).toBe(10)
        const value = redis.get('mykey')
        expect(value?.substring(0, 2)).toBe('Hi')
        expect(value?.substring(5)).toBe('World')
      })

      it('should SETRANGE on non-existent key', () => {
        const len = redis.setrange('mykey', 0, 'Hello')
        expect(len).toBe(5)
        expect(redis.get('mykey')).toBe('Hello')
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Additional Commands
  // ─────────────────────────────────────────────────────────────────

  describe('Additional Commands', () => {
    describe('SETNX', () => {
      it('should SETNX when key does not exist', () => {
        expect(redis.setnx('mykey', 'myvalue')).toBe(1)
        expect(redis.get('mykey')).toBe('myvalue')
      })

      it('should not SETNX when key exists', () => {
        redis.set('mykey', 'existing')
        expect(redis.setnx('mykey', 'newvalue')).toBe(0)
        expect(redis.get('mykey')).toBe('existing')
      })
    })

    describe('SETEX', () => {
      it('should SETEX with expiration', () => {
        vi.useFakeTimers()
        expect(redis.setex('mykey', 10, 'myvalue')).toBe('OK')
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(11000)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('PSETEX', () => {
      it('should PSETEX with millisecond expiration', () => {
        vi.useFakeTimers()
        expect(redis.psetex('mykey', 500, 'myvalue')).toBe('OK')
        expect(redis.get('mykey')).toBe('myvalue')

        vi.advanceTimersByTime(600)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })
    })

    describe('GETSET', () => {
      it('should GETSET return old value and set new', () => {
        redis.set('mykey', 'oldvalue')
        expect(redis.getset('mykey', 'newvalue')).toBe('oldvalue')
        expect(redis.get('mykey')).toBe('newvalue')
      })

      it('should GETSET return null for non-existent key', () => {
        expect(redis.getset('mykey', 'value')).toBeNull()
        expect(redis.get('mykey')).toBe('value')
      })
    })

    describe('GETDEL', () => {
      it('should GETDEL return value and delete key', () => {
        redis.set('mykey', 'myvalue')
        expect(redis.getdel('mykey')).toBe('myvalue')
        expect(redis.get('mykey')).toBeNull()
      })

      it('should GETDEL return null for non-existent key', () => {
        expect(redis.getdel('nonexistent')).toBeNull()
      })
    })

    describe('GETEX', () => {
      it('should GETEX and set expiration with EX', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'myvalue')
        expect(redis.getex('mykey', { ex: 10 })).toBe('myvalue')

        vi.advanceTimersByTime(11000)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })

      it('should GETEX and set expiration with PX', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'myvalue')
        expect(redis.getex('mykey', { px: 500 })).toBe('myvalue')

        vi.advanceTimersByTime(600)
        expect(redis.get('mykey')).toBeNull()
        vi.useRealTimers()
      })

      it('should GETEX with PERSIST option', () => {
        vi.useFakeTimers()
        redis.set('mykey', 'myvalue', { ex: 10 })
        expect(redis.getex('mykey', { persist: true })).toBe('myvalue')

        vi.advanceTimersByTime(11000)
        expect(redis.get('mykey')).toBe('myvalue') // Should still exist
        vi.useRealTimers()
      })

      it('should GETEX return null for non-existent key', () => {
        expect(redis.getex('nonexistent')).toBeNull()
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Type Checking
  // ─────────────────────────────────────────────────────────────────

  describe('Type Checking', () => {
    it('should throw WRONGTYPE for GET on non-string key', () => {
      // Simulate a non-string type
      const store = redis as any
      store.store = new Map([['hashkey', { value: 'test', type: 'hash', expiresAt: null }]])

      expect(() => redis.get('hashkey')).toThrow('WRONGTYPE')
    })
  })
})
