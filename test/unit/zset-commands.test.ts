/**
 * Sorted Set Commands Tests
 *
 * Tests for Redis sorted set command implementations
 * Covers ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZCARD, ZSCAN, etc.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { ScanOptions } from '../../src/types'

// ─────────────────────────────────────────────────────────────────
// Mock Redis Sorted Set Store
// ─────────────────────────────────────────────────────────────────

interface ZSetMember {
  member: string
  score: number
}

interface KeyEntry {
  members: Map<string, number> // member -> score
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisZSetStore {
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

  private assertType(key: string, expectedType: 'zset'): void {
    const entry = this.store.get(key)
    if (entry && entry.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
  }

  private getOrCreateZSet(key: string): Map<string, number> {
    this.cleanExpired(key)
    let entry = this.store.get(key)

    if (!entry) {
      entry = { members: new Map(), type: 'zset', expiresAt: null }
      this.store.set(key, entry)
    } else {
      this.assertType(key, 'zset')
    }

    return entry.members
  }

  private getZSet(key: string): Map<string, number> | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'zset')
    return entry.members
  }

  // ─────────────────────────────────────────────────────────────────
  // ZADD
  // ─────────────────────────────────────────────────────────────────

  zadd(key: string, ...scoreMembers: (number | string)[]): number {
    if (scoreMembers.length % 2 !== 0) {
      throw new Error('ERR syntax error')
    }

    const zset = this.getOrCreateZSet(key)
    let added = 0

    for (let i = 0; i < scoreMembers.length; i += 2) {
      const score = scoreMembers[i] as number
      const member = scoreMembers[i + 1] as string

      if (!zset.has(member)) {
        added++
      }
      zset.set(member, score)
    }

    return added
  }

  // ─────────────────────────────────────────────────────────────────
  // ZREM
  // ─────────────────────────────────────────────────────────────────

  zrem(key: string, ...members: string[]): number {
    const zset = this.getZSet(key)
    if (!zset) return 0

    let removed = 0
    for (const member of members) {
      if (zset.delete(member)) {
        removed++
      }
    }

    if (zset.size === 0) {
      this.store.delete(key)
    }

    return removed
  }

  // ─────────────────────────────────────────────────────────────────
  // ZSCORE
  // ─────────────────────────────────────────────────────────────────

  zscore(key: string, member: string): number | null {
    const zset = this.getZSet(key)
    if (!zset) return null
    return zset.get(member) ?? null
  }

  // ─────────────────────────────────────────────────────────────────
  // ZCARD
  // ─────────────────────────────────────────────────────────────────

  zcard(key: string): number {
    const zset = this.getZSet(key)
    return zset ? zset.size : 0
  }

  // ─────────────────────────────────────────────────────────────────
  // ZRANK
  // ─────────────────────────────────────────────────────────────────

  zrank(key: string, member: string): number | null {
    const zset = this.getZSet(key)
    if (!zset || !zset.has(member)) return null

    const sorted = Array.from(zset.entries())
      .sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]))

    return sorted.findIndex(([m]) => m === member)
  }

  // ─────────────────────────────────────────────────────────────────
  // ZRANGE
  // ─────────────────────────────────────────────────────────────────

  zrange(key: string, start: number, stop: number, opts?: { withScores?: boolean }): string[] | ZSetMember[] {
    const zset = this.getZSet(key)
    if (!zset) return []

    const sorted = Array.from(zset.entries())
      .sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]))

    const len = sorted.length
    const normalizedStart = start < 0 ? Math.max(0, len + start) : start
    const normalizedStop = stop < 0 ? len + stop : stop

    const result = sorted.slice(normalizedStart, normalizedStop + 1)

    if (opts?.withScores) {
      return result.map(([member, score]) => ({ member, score }))
    }
    return result.map(([member]) => member)
  }

  // ─────────────────────────────────────────────────────────────────
  // ZSCAN
  // ─────────────────────────────────────────────────────────────────

  zscan(key: string, cursor: number, options: ScanOptions = {}): [string, string[]] {
    const zset = this.getZSet(key)
    if (!zset) return ['0', []]

    const { match, count = 10 } = options

    // Get all members sorted by score and member for consistent iteration
    const sortedMembers = Array.from(zset.entries())
      .sort((a, b) => a[1] - b[1] || a[0].localeCompare(b[0]))

    const matchedEntries: string[] = []

    for (const [member, score] of sortedMembers) {
      if (match) {
        const regex = new RegExp('^' + match.replace(/\*/g, '.*').replace(/\?/g, '.') + '$')
        if (!regex.test(member)) continue
      }
      matchedEntries.push(member, String(score))
    }

    const start = cursor
    const end = Math.min(start + count * 2, matchedEntries.length)
    const result = matchedEntries.slice(start, end)
    const nextCursor = end >= matchedEntries.length ? '0' : String(end)

    return [nextCursor, result]
  }

  // ─────────────────────────────────────────────────────────────────
  // Helpers
  // ─────────────────────────────────────────────────────────────────

  del(key: string): number {
    if (this.store.delete(key)) return 1
    return 0
  }

  expire(key: string, seconds: number): 0 | 1 {
    const entry = this.store.get(key)
    if (!entry) return 0
    entry.expiresAt = Date.now() + seconds * 1000
    return 1
  }

  setStringKey(key: string, value: string): void {
    this.store.set(key, {
      members: new Map(),
      type: 'string',
      expiresAt: null,
    })
  }

  clear(): void {
    this.store.clear()
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('Sorted Set Commands', () => {
  let redis: MockRedisZSetStore

  beforeEach(() => {
    redis = new MockRedisZSetStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // ZADD Basic Operations
  // ─────────────────────────────────────────────────────────────────

  describe('ZADD', () => {
    it('should ZADD single member to new sorted set', () => {
      expect(redis.zadd('myzset', 1, 'one')).toBe(1)
      expect(redis.zscore('myzset', 'one')).toBe(1)
    })

    it('should ZADD multiple members', () => {
      expect(redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')).toBe(3)
      expect(redis.zcard('myzset')).toBe(3)
    })

    it('should ZADD return count of new members only', () => {
      redis.zadd('myzset', 1, 'one', 2, 'two')
      expect(redis.zadd('myzset', 2.5, 'two', 3, 'three')).toBe(1) // Only 'three' is new
    })

    it('should ZADD update score of existing member', () => {
      redis.zadd('myzset', 1, 'one')
      redis.zadd('myzset', 10, 'one')
      expect(redis.zscore('myzset', 'one')).toBe(10)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // ZREM
  // ─────────────────────────────────────────────────────────────────

  describe('ZREM', () => {
    beforeEach(() => {
      redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')
    })

    it('should ZREM single member', () => {
      expect(redis.zrem('myzset', 'one')).toBe(1)
      expect(redis.zscore('myzset', 'one')).toBeNull()
    })

    it('should ZREM multiple members', () => {
      expect(redis.zrem('myzset', 'one', 'two')).toBe(2)
      expect(redis.zcard('myzset')).toBe(1)
    })

    it('should ZREM return 0 for non-existent member', () => {
      expect(redis.zrem('myzset', 'nonexistent')).toBe(0)
    })

    it('should ZREM return 0 for non-existent key', () => {
      expect(redis.zrem('nonexistent', 'one')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // ZSCORE/ZCARD/ZRANK
  // ─────────────────────────────────────────────────────────────────

  describe('ZSCORE', () => {
    it('should ZSCORE return score of member', () => {
      redis.zadd('myzset', 1.5, 'one')
      expect(redis.zscore('myzset', 'one')).toBe(1.5)
    })

    it('should ZSCORE return null for non-existent member', () => {
      redis.zadd('myzset', 1, 'one')
      expect(redis.zscore('myzset', 'nonexistent')).toBeNull()
    })

    it('should ZSCORE return null for non-existent key', () => {
      expect(redis.zscore('nonexistent', 'one')).toBeNull()
    })
  })

  describe('ZCARD', () => {
    it('should ZCARD return sorted set cardinality', () => {
      redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')
      expect(redis.zcard('myzset')).toBe(3)
    })

    it('should ZCARD return 0 for non-existent key', () => {
      expect(redis.zcard('nonexistent')).toBe(0)
    })
  })

  describe('ZRANK', () => {
    beforeEach(() => {
      redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')
    })

    it('should ZRANK return rank of member', () => {
      expect(redis.zrank('myzset', 'one')).toBe(0)
      expect(redis.zrank('myzset', 'two')).toBe(1)
      expect(redis.zrank('myzset', 'three')).toBe(2)
    })

    it('should ZRANK return null for non-existent member', () => {
      expect(redis.zrank('myzset', 'nonexistent')).toBeNull()
    })

    it('should ZRANK return null for non-existent key', () => {
      expect(redis.zrank('nonexistent', 'one')).toBeNull()
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // ZRANGE
  // ─────────────────────────────────────────────────────────────────

  describe('ZRANGE', () => {
    beforeEach(() => {
      redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')
    })

    it('should ZRANGE return members in range', () => {
      expect(redis.zrange('myzset', 0, -1)).toEqual(['one', 'two', 'three'])
    })

    it('should ZRANGE with positive indices', () => {
      expect(redis.zrange('myzset', 0, 1)).toEqual(['one', 'two'])
    })

    it('should ZRANGE with negative indices', () => {
      expect(redis.zrange('myzset', -2, -1)).toEqual(['two', 'three'])
    })

    it('should ZRANGE with WITHSCORES', () => {
      const result = redis.zrange('myzset', 0, -1, { withScores: true }) as ZSetMember[]
      expect(result).toEqual([
        { member: 'one', score: 1 },
        { member: 'two', score: 2 },
        { member: 'three', score: 3 },
      ])
    })

    it('should ZRANGE return empty for non-existent key', () => {
      expect(redis.zrange('nonexistent', 0, -1)).toEqual([])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // ZSCAN
  // ─────────────────────────────────────────────────────────────────

  describe('ZSCAN', () => {
    beforeEach(() => {
      for (let i = 0; i < 20; i++) {
        redis.zadd('myzset', i, `member${i}`)
      }
    })

    it('should ZSCAN return all member-score pairs', () => {
      const [cursor, result] = redis.zscan('myzset', 0, { count: 100 })
      expect(cursor).toBe('0')
      expect(result.length).toBe(40) // 20 members * 2 (member + score)
    })

    it('should ZSCAN return member-score pairs in format [member1, score1, member2, score2, ...]', () => {
      redis.clear()
      redis.zadd('myzset', 1, 'one', 2, 'two')
      const [cursor, result] = redis.zscan('myzset', 0)
      expect(result).toContain('one')
      expect(result).toContain('1')
      expect(result).toContain('two')
      expect(result).toContain('2')
    })

    it('should ZSCAN with MATCH pattern', () => {
      const [cursor, result] = redis.zscan('myzset', 0, { match: 'member1*', count: 100 })
      // Should match member1, member10-19
      for (let i = 0; i < result.length; i += 2) {
        expect(result[i].startsWith('member1')).toBe(true)
      }
    })

    it('should ZSCAN return empty for non-existent key', () => {
      const [cursor, result] = redis.zscan('nonexistent', 0)
      expect(cursor).toBe('0')
      expect(result).toEqual([])
    })

    it('should ZSCAN with COUNT limit', () => {
      const [cursor, result] = redis.zscan('myzset', 0, { count: 5 })
      expect(result.length).toBeLessThanOrEqual(10) // 5 pairs = 10 elements max
    })

    it('should ZSCAN iterate with cursor', () => {
      const allResults: string[] = []
      let cursor = 0

      // First iteration
      const [nextCursor1, result1] = redis.zscan('myzset', cursor, { count: 5 })
      allResults.push(...result1)
      cursor = parseInt(nextCursor1)

      // Continue if there are more results
      while (cursor !== 0) {
        const [nextCursor, result] = redis.zscan('myzset', cursor, { count: 5 })
        allResults.push(...result)
        cursor = parseInt(nextCursor)
      }

      // Should have all 20 members and their scores
      expect(allResults.length).toBe(40) // 20 members * 2
    })

    it('should ZSCAN match single character wildcard', () => {
      redis.clear()
      redis.zadd('myzset', 1, 'a1', 2, 'a2', 3, 'a10', 4, 'b1')
      const [cursor, result] = redis.zscan('myzset', 0, { match: 'a?', count: 100 })
      // Should match a1, a2 but not a10 or b1
      const members = result.filter((_, i) => i % 2 === 0)
      expect(members).toContain('a1')
      expect(members).toContain('a2')
      expect(members).not.toContain('a10')
      expect(members).not.toContain('b1')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Type Checking
  // ─────────────────────────────────────────────────────────────────

  describe('Type Checking', () => {
    it('should throw WRONGTYPE for zset command on string key', () => {
      redis.setStringKey('mystring', 'value')
      expect(() => redis.zadd('mystring', 1, 'one')).toThrow('WRONGTYPE')
    })

    it('should throw WRONGTYPE for ZSCAN on string key', () => {
      redis.setStringKey('mystring', 'value')
      expect(() => redis.zscan('mystring', 0)).toThrow('WRONGTYPE')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Expiration
  // ─────────────────────────────────────────────────────────────────

  describe('Sorted Set Expiration', () => {
    it('should respect TTL expiration', () => {
      vi.useFakeTimers()
      redis.zadd('myzset', 1, 'one', 2, 'two', 3, 'three')
      redis.expire('myzset', 10)

      expect(redis.zcard('myzset')).toBe(3)

      vi.advanceTimersByTime(11000)
      expect(redis.zcard('myzset')).toBe(0)
      vi.useRealTimers()
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Edge Cases
  // ─────────────────────────────────────────────────────────────────

  describe('Edge Cases', () => {
    it('should handle many members', () => {
      for (let i = 0; i < 1000; i++) {
        redis.zadd('myzset', i, `member${i}`)
      }
      expect(redis.zcard('myzset')).toBe(1000)
    })

    it('should handle float scores', () => {
      redis.zadd('myzset', 1.5, 'one', 2.7, 'two', 3.14159, 'pi')
      expect(redis.zscore('myzset', 'pi')).toBe(3.14159)
    })

    it('should handle negative scores', () => {
      redis.zadd('myzset', -10, 'negative', 0, 'zero', 10, 'positive')
      expect(redis.zrange('myzset', 0, -1)).toEqual(['negative', 'zero', 'positive'])
    })

    it('should handle special characters in member', () => {
      redis.zadd('myzset', 1, 'hello world', 2, 'foo:bar', 3, 'a\nb')
      expect(redis.zscore('myzset', 'hello world')).toBe(1)
      expect(redis.zscore('myzset', 'foo:bar')).toBe(2)
      expect(redis.zscore('myzset', 'a\nb')).toBe(3)
    })
  })
})
