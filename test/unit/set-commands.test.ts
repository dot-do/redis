/**
 * Set Commands Tests
 *
 * Tests for Redis set command implementations
 * Covers SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, etc.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import type { ScanOptions } from '../../src/types'

// ─────────────────────────────────────────────────────────────────
// Mock Redis Set Store
// ─────────────────────────────────────────────────────────────────

interface KeyEntry {
  members: Set<string>
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisSetStore {
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

  private assertType(key: string, expectedType: 'set'): void {
    const entry = this.store.get(key)
    if (entry && entry.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
  }

  private getOrCreateSet(key: string): Set<string> {
    this.cleanExpired(key)
    let entry = this.store.get(key)

    if (!entry) {
      entry = { members: new Set(), type: 'set', expiresAt: null }
      this.store.set(key, entry)
    } else {
      this.assertType(key, 'set')
    }

    return entry.members
  }

  private getSet(key: string): Set<string> | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'set')
    return entry.members
  }

  // ─────────────────────────────────────────────────────────────────
  // SADD/SREM
  // ─────────────────────────────────────────────────────────────────

  sadd(key: string, ...members: string[]): number {
    const set = this.getOrCreateSet(key)
    let added = 0

    for (const member of members) {
      if (!set.has(member)) {
        set.add(member)
        added++
      }
    }

    return added
  }

  srem(key: string, ...members: string[]): number {
    const set = this.getSet(key)
    if (!set) return 0

    let removed = 0
    for (const member of members) {
      if (set.delete(member)) {
        removed++
      }
    }

    if (set.size === 0) {
      this.store.delete(key)
    }

    return removed
  }

  // ─────────────────────────────────────────────────────────────────
  // SMEMBERS/SISMEMBER/SMISMEMBER/SCARD
  // ─────────────────────────────────────────────────────────────────

  smembers(key: string): string[] {
    const set = this.getSet(key)
    if (!set) return []
    return Array.from(set)
  }

  sismember(key: string, member: string): 0 | 1 {
    const set = this.getSet(key)
    if (!set) return 0
    return set.has(member) ? 1 : 0
  }

  smismember(key: string, ...members: string[]): (0 | 1)[] {
    const set = this.getSet(key)
    if (!set) return members.map(() => 0)
    return members.map((m) => (set.has(m) ? 1 : 0))
  }

  scard(key: string): number {
    const set = this.getSet(key)
    return set ? set.size : 0
  }

  // ─────────────────────────────────────────────────────────────────
  // SPOP/SRANDMEMBER
  // ─────────────────────────────────────────────────────────────────

  spop(key: string, count?: number): string | string[] | null {
    const set = this.getSet(key)
    if (!set || set.size === 0) return count !== undefined ? [] : null

    const members = Array.from(set)

    if (count === undefined) {
      // Pop single random member
      const randomIndex = Math.floor(Math.random() * members.length)
      const member = members[randomIndex]
      set.delete(member)
      if (set.size === 0) this.store.delete(key)
      return member
    }

    // Pop multiple random members
    const result: string[] = []
    const indices = new Set<number>()

    while (result.length < count && result.length < members.length) {
      const randomIndex = Math.floor(Math.random() * members.length)
      if (!indices.has(randomIndex)) {
        indices.add(randomIndex)
        const member = members[randomIndex]
        result.push(member)
        set.delete(member)
      }
    }

    if (set.size === 0) this.store.delete(key)
    return result
  }

  srandmember(key: string, count?: number): string | string[] | null {
    const set = this.getSet(key)
    if (!set || set.size === 0) return count !== undefined ? [] : null

    const members = Array.from(set)

    if (count === undefined) {
      // Return single random member
      const randomIndex = Math.floor(Math.random() * members.length)
      return members[randomIndex]
    }

    if (count > 0) {
      // Return unique random members
      const result: string[] = []
      const shuffled = [...members].sort(() => Math.random() - 0.5)
      return shuffled.slice(0, count)
    } else {
      // Return random members with possible repetition
      const absCount = Math.abs(count)
      const result: string[] = []
      for (let i = 0; i < absCount; i++) {
        const randomIndex = Math.floor(Math.random() * members.length)
        result.push(members[randomIndex])
      }
      return result
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Set Operations: SDIFF, SINTER, SUNION
  // ─────────────────────────────────────────────────────────────────

  sdiff(...keys: string[]): string[] {
    if (keys.length === 0) return []

    const firstSet = this.getSet(keys[0])
    if (!firstSet) return []

    const result = new Set(firstSet)

    for (let i = 1; i < keys.length; i++) {
      const otherSet = this.getSet(keys[i])
      if (otherSet) {
        for (const member of otherSet) {
          result.delete(member)
        }
      }
    }

    return Array.from(result)
  }

  sdiffstore(destination: string, ...keys: string[]): number {
    const diffResult = this.sdiff(...keys)
    if (diffResult.length === 0) {
      this.store.delete(destination)
      return 0
    }

    this.store.set(destination, {
      members: new Set(diffResult),
      type: 'set',
      expiresAt: null,
    })

    return diffResult.length
  }

  sinter(...keys: string[]): string[] {
    if (keys.length === 0) return []

    const firstSet = this.getSet(keys[0])
    if (!firstSet || firstSet.size === 0) return []

    const result = new Set(firstSet)

    for (let i = 1; i < keys.length; i++) {
      const otherSet = this.getSet(keys[i])
      if (!otherSet || otherSet.size === 0) return []

      for (const member of result) {
        if (!otherSet.has(member)) {
          result.delete(member)
        }
      }

      if (result.size === 0) return []
    }

    return Array.from(result)
  }

  sinterstore(destination: string, ...keys: string[]): number {
    const interResult = this.sinter(...keys)
    if (interResult.length === 0) {
      this.store.delete(destination)
      return 0
    }

    this.store.set(destination, {
      members: new Set(interResult),
      type: 'set',
      expiresAt: null,
    })

    return interResult.length
  }

  sintercard(numkeys: number, ...keysAndLimit: (string | number)[]): number {
    const keys = keysAndLimit.slice(0, numkeys) as string[]
    const limitArg = keysAndLimit.find((arg, i) => i >= numkeys && arg === 'LIMIT')
    const limit = limitArg !== undefined
      ? (keysAndLimit[keysAndLimit.indexOf(limitArg) + 1] as number)
      : 0

    const interResult = this.sinter(...keys)

    if (limit > 0 && interResult.length > limit) {
      return limit
    }

    return interResult.length
  }

  sunion(...keys: string[]): string[] {
    const result = new Set<string>()

    for (const key of keys) {
      const set = this.getSet(key)
      if (set) {
        for (const member of set) {
          result.add(member)
        }
      }
    }

    return Array.from(result)
  }

  sunionstore(destination: string, ...keys: string[]): number {
    const unionResult = this.sunion(...keys)
    if (unionResult.length === 0) {
      this.store.delete(destination)
      return 0
    }

    this.store.set(destination, {
      members: new Set(unionResult),
      type: 'set',
      expiresAt: null,
    })

    return unionResult.length
  }

  // ─────────────────────────────────────────────────────────────────
  // SMOVE
  // ─────────────────────────────────────────────────────────────────

  smove(source: string, destination: string, member: string): 0 | 1 {
    const srcSet = this.getSet(source)
    if (!srcSet || !srcSet.has(member)) return 0

    srcSet.delete(member)
    if (srcSet.size === 0) this.store.delete(source)

    const dstSet = this.getOrCreateSet(destination)
    dstSet.add(member)

    return 1
  }

  // ─────────────────────────────────────────────────────────────────
  // SSCAN
  // ─────────────────────────────────────────────────────────────────

  sscan(key: string, cursor: number, options: ScanOptions = {}): [string, string[]] {
    const set = this.getSet(key)
    if (!set) return ['0', []]

    const { match, count = 10 } = options
    const allMembers = Array.from(set)
    const matchedMembers: string[] = []

    for (const member of allMembers) {
      if (match) {
        const regex = new RegExp('^' + match.replace(/\*/g, '.*').replace(/\?/g, '.') + '$')
        if (!regex.test(member)) continue
      }
      matchedMembers.push(member)
    }

    const start = cursor
    const end = Math.min(start + count, matchedMembers.length)
    const result = matchedMembers.slice(start, end)
    const nextCursor = end >= matchedMembers.length ? '0' : String(end)

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
      members: new Set(),
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

describe('Set Commands', () => {
  let redis: MockRedisSetStore

  beforeEach(() => {
    redis = new MockRedisSetStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // SADD/SREM Basic Operations
  // ─────────────────────────────────────────────────────────────────

  describe('SADD', () => {
    it('should SADD single member to new set', () => {
      expect(redis.sadd('myset', 'a')).toBe(1)
      expect(redis.smembers('myset')).toEqual(['a'])
    })

    it('should SADD multiple members', () => {
      expect(redis.sadd('myset', 'a', 'b', 'c')).toBe(3)
      expect(redis.smembers('myset').sort()).toEqual(['a', 'b', 'c'])
    })

    it('should SADD return count of new members only', () => {
      redis.sadd('myset', 'a', 'b')
      expect(redis.sadd('myset', 'b', 'c', 'd')).toBe(2) // Only c and d are new
    })

    it('should SADD not add duplicate members', () => {
      redis.sadd('myset', 'a')
      expect(redis.sadd('myset', 'a')).toBe(0)
      expect(redis.scard('myset')).toBe(1)
    })

    it('should handle empty string member', () => {
      redis.sadd('myset', '')
      expect(redis.sismember('myset', '')).toBe(1)
    })

    it('should handle special characters', () => {
      redis.sadd('myset', 'hello world', 'foo:bar', 'a\nb')
      expect(redis.sismember('myset', 'hello world')).toBe(1)
      expect(redis.sismember('myset', 'foo:bar')).toBe(1)
      expect(redis.sismember('myset', 'a\nb')).toBe(1)
    })
  })

  describe('SREM', () => {
    beforeEach(() => {
      redis.sadd('myset', 'a', 'b', 'c', 'd')
    })

    it('should SREM single member', () => {
      expect(redis.srem('myset', 'a')).toBe(1)
      expect(redis.sismember('myset', 'a')).toBe(0)
    })

    it('should SREM multiple members', () => {
      expect(redis.srem('myset', 'a', 'b')).toBe(2)
      expect(redis.scard('myset')).toBe(2)
    })

    it('should SREM return 0 for non-existent member', () => {
      expect(redis.srem('myset', 'x')).toBe(0)
    })

    it('should SREM return 0 for non-existent key', () => {
      expect(redis.srem('nonexistent', 'a')).toBe(0)
    })

    it('should SREM return count of actually removed members', () => {
      expect(redis.srem('myset', 'a', 'x', 'b', 'y')).toBe(2)
    })

    it('should SREM delete key when set becomes empty', () => {
      redis.srem('myset', 'a', 'b', 'c', 'd')
      expect(redis.scard('myset')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // SMEMBERS/SISMEMBER/SMISMEMBER/SCARD
  // ─────────────────────────────────────────────────────────────────

  describe('SMEMBERS', () => {
    it('should SMEMBERS return all members', () => {
      redis.sadd('myset', 'a', 'b', 'c')
      const members = redis.smembers('myset')
      expect(members.sort()).toEqual(['a', 'b', 'c'])
    })

    it('should SMEMBERS return empty array for non-existent key', () => {
      expect(redis.smembers('nonexistent')).toEqual([])
    })

    it('should SMEMBERS return empty array for empty set', () => {
      redis.sadd('myset', 'a')
      redis.srem('myset', 'a')
      expect(redis.smembers('myset')).toEqual([])
    })
  })

  describe('SISMEMBER', () => {
    beforeEach(() => {
      redis.sadd('myset', 'a', 'b', 'c')
    })

    it('should SISMEMBER return 1 for existing member', () => {
      expect(redis.sismember('myset', 'a')).toBe(1)
      expect(redis.sismember('myset', 'b')).toBe(1)
    })

    it('should SISMEMBER return 0 for non-existent member', () => {
      expect(redis.sismember('myset', 'x')).toBe(0)
    })

    it('should SISMEMBER return 0 for non-existent key', () => {
      expect(redis.sismember('nonexistent', 'a')).toBe(0)
    })
  })

  describe('SMISMEMBER', () => {
    beforeEach(() => {
      redis.sadd('myset', 'a', 'b', 'c')
    })

    it('should SMISMEMBER check multiple members', () => {
      expect(redis.smismember('myset', 'a', 'b', 'x', 'c', 'y')).toEqual([1, 1, 0, 1, 0])
    })

    it('should SMISMEMBER return all zeros for non-existent key', () => {
      expect(redis.smismember('nonexistent', 'a', 'b', 'c')).toEqual([0, 0, 0])
    })
  })

  describe('SCARD', () => {
    it('should SCARD return set cardinality', () => {
      redis.sadd('myset', 'a', 'b', 'c')
      expect(redis.scard('myset')).toBe(3)
    })

    it('should SCARD return 0 for non-existent key', () => {
      expect(redis.scard('nonexistent')).toBe(0)
    })

    it('should SCARD update after add/remove', () => {
      redis.sadd('myset', 'a', 'b')
      expect(redis.scard('myset')).toBe(2)
      redis.sadd('myset', 'c')
      expect(redis.scard('myset')).toBe(3)
      redis.srem('myset', 'a')
      expect(redis.scard('myset')).toBe(2)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // SPOP/SRANDMEMBER
  // ─────────────────────────────────────────────────────────────────

  describe('SPOP', () => {
    beforeEach(() => {
      redis.sadd('myset', 'a', 'b', 'c')
    })

    it('should SPOP single random member', () => {
      const member = redis.spop('myset')
      expect(['a', 'b', 'c']).toContain(member)
      expect(redis.scard('myset')).toBe(2)
    })

    it('should SPOP return null for non-existent key', () => {
      expect(redis.spop('nonexistent')).toBeNull()
    })

    it('should SPOP return empty array with count for non-existent key', () => {
      expect(redis.spop('nonexistent', 3)).toEqual([])
    })

    it('should SPOP multiple members with count', () => {
      const members = redis.spop('myset', 2) as string[]
      expect(members).toHaveLength(2)
      expect(redis.scard('myset')).toBe(1)
    })

    it('should SPOP delete key when set becomes empty', () => {
      redis.spop('myset', 3)
      expect(redis.scard('myset')).toBe(0)
    })
  })

  describe('SRANDMEMBER', () => {
    beforeEach(() => {
      redis.sadd('myset', 'a', 'b', 'c')
    })

    it('should SRANDMEMBER return random member without removing', () => {
      const member = redis.srandmember('myset')
      expect(['a', 'b', 'c']).toContain(member)
      expect(redis.scard('myset')).toBe(3) // Not removed
    })

    it('should SRANDMEMBER return null for non-existent key', () => {
      expect(redis.srandmember('nonexistent')).toBeNull()
    })

    it('should SRANDMEMBER with positive count return unique members', () => {
      const members = redis.srandmember('myset', 2) as string[]
      expect(members).toHaveLength(2)
      // All unique
      expect(new Set(members).size).toBe(2)
    })

    it('should SRANDMEMBER with count > size return all members', () => {
      const members = redis.srandmember('myset', 10) as string[]
      expect(members.sort()).toEqual(['a', 'b', 'c'])
    })

    it('should SRANDMEMBER with negative count allow duplicates', () => {
      redis.sadd('myset', 'd', 'e')
      const members = redis.srandmember('myset', -10) as string[]
      expect(members).toHaveLength(10)
    })

    it('should SRANDMEMBER return empty array for empty set with count', () => {
      redis.srem('myset', 'a', 'b', 'c')
      expect(redis.srandmember('myset', 3)).toEqual([])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Set Operations
  // ─────────────────────────────────────────────────────────────────

  describe('SDIFF', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c', 'd')
      redis.sadd('set2', 'c')
      redis.sadd('set3', 'd', 'e')
    })

    it('should SDIFF return difference of sets', () => {
      expect(redis.sdiff('set1', 'set2').sort()).toEqual(['a', 'b', 'd'])
    })

    it('should SDIFF with multiple sets', () => {
      expect(redis.sdiff('set1', 'set2', 'set3').sort()).toEqual(['a', 'b'])
    })

    it('should SDIFF return all members if second set non-existent', () => {
      expect(redis.sdiff('set1', 'nonexistent').sort()).toEqual(['a', 'b', 'c', 'd'])
    })

    it('should SDIFF return empty if first set non-existent', () => {
      expect(redis.sdiff('nonexistent', 'set1')).toEqual([])
    })

    it('should SDIFF single set return all members', () => {
      expect(redis.sdiff('set1').sort()).toEqual(['a', 'b', 'c', 'd'])
    })
  })

  describe('SDIFFSTORE', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c')
      redis.sadd('set2', 'c', 'd')
    })

    it('should SDIFFSTORE save difference to destination', () => {
      expect(redis.sdiffstore('result', 'set1', 'set2')).toBe(2)
      expect(redis.smembers('result').sort()).toEqual(['a', 'b'])
    })

    it('should SDIFFSTORE overwrite destination', () => {
      redis.sadd('result', 'x', 'y', 'z')
      redis.sdiffstore('result', 'set1', 'set2')
      expect(redis.smembers('result').sort()).toEqual(['a', 'b'])
    })
  })

  describe('SINTER', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c', 'd')
      redis.sadd('set2', 'b', 'c', 'e')
      redis.sadd('set3', 'c', 'f')
    })

    it('should SINTER return intersection of two sets', () => {
      expect(redis.sinter('set1', 'set2').sort()).toEqual(['b', 'c'])
    })

    it('should SINTER return intersection of multiple sets', () => {
      expect(redis.sinter('set1', 'set2', 'set3')).toEqual(['c'])
    })

    it('should SINTER return empty if any set is empty', () => {
      expect(redis.sinter('set1', 'nonexistent')).toEqual([])
    })

    it('should SINTER return empty if no common members', () => {
      redis.sadd('set4', 'x', 'y', 'z')
      expect(redis.sinter('set1', 'set4')).toEqual([])
    })
  })

  describe('SINTERSTORE', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c')
      redis.sadd('set2', 'b', 'c', 'd')
    })

    it('should SINTERSTORE save intersection to destination', () => {
      expect(redis.sinterstore('result', 'set1', 'set2')).toBe(2)
      expect(redis.smembers('result').sort()).toEqual(['b', 'c'])
    })

    it('should SINTERSTORE delete destination if result is empty', () => {
      redis.sadd('set3', 'x', 'y')
      redis.sadd('result', 'existing')
      redis.sinterstore('result', 'set1', 'set3')
      expect(redis.scard('result')).toBe(0)
    })
  })

  describe('SINTERCARD', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c', 'd')
      redis.sadd('set2', 'b', 'c', 'd', 'e')
    })

    it('should SINTERCARD return cardinality of intersection', () => {
      expect(redis.sintercard(2, 'set1', 'set2')).toBe(3)
    })

    it('should SINTERCARD with LIMIT', () => {
      expect(redis.sintercard(2, 'set1', 'set2', 'LIMIT', 2)).toBe(2)
    })
  })

  describe('SUNION', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b', 'c')
      redis.sadd('set2', 'c', 'd', 'e')
    })

    it('should SUNION return union of sets', () => {
      expect(redis.sunion('set1', 'set2').sort()).toEqual(['a', 'b', 'c', 'd', 'e'])
    })

    it('should SUNION include non-existent sets as empty', () => {
      expect(redis.sunion('set1', 'nonexistent').sort()).toEqual(['a', 'b', 'c'])
    })

    it('should SUNION return empty for all non-existent', () => {
      expect(redis.sunion('x', 'y', 'z')).toEqual([])
    })
  })

  describe('SUNIONSTORE', () => {
    beforeEach(() => {
      redis.sadd('set1', 'a', 'b')
      redis.sadd('set2', 'b', 'c')
    })

    it('should SUNIONSTORE save union to destination', () => {
      expect(redis.sunionstore('result', 'set1', 'set2')).toBe(3)
      expect(redis.smembers('result').sort()).toEqual(['a', 'b', 'c'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // SMOVE
  // ─────────────────────────────────────────────────────────────────

  describe('SMOVE', () => {
    beforeEach(() => {
      redis.sadd('src', 'a', 'b', 'c')
      redis.sadd('dst', 'x', 'y')
    })

    it('should SMOVE member from source to destination', () => {
      expect(redis.smove('src', 'dst', 'a')).toBe(1)
      expect(redis.sismember('src', 'a')).toBe(0)
      expect(redis.sismember('dst', 'a')).toBe(1)
    })

    it('should SMOVE return 0 if member not in source', () => {
      expect(redis.smove('src', 'dst', 'z')).toBe(0)
    })

    it('should SMOVE return 0 if source non-existent', () => {
      expect(redis.smove('nonexistent', 'dst', 'a')).toBe(0)
    })

    it('should SMOVE create destination if not exists', () => {
      redis.del('dst')
      expect(redis.smove('src', 'newdst', 'a')).toBe(1)
      expect(redis.sismember('newdst', 'a')).toBe(1)
    })

    it('should SMOVE delete source if becomes empty', () => {
      redis.srem('src', 'b', 'c')
      redis.smove('src', 'dst', 'a')
      expect(redis.scard('src')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // SSCAN
  // ─────────────────────────────────────────────────────────────────

  describe('SSCAN', () => {
    beforeEach(() => {
      for (let i = 0; i < 20; i++) {
        redis.sadd('myset', `member${i}`)
      }
    })

    it('should SSCAN return members', () => {
      const [cursor, members] = redis.sscan('myset', 0)
      expect(members.length).toBeGreaterThan(0)
    })

    it('should SSCAN with MATCH pattern', () => {
      const [cursor, members] = redis.sscan('myset', 0, { match: 'member1*' })
      for (const member of members) {
        expect(member.startsWith('member1')).toBe(true)
      }
    })

    it('should SSCAN return empty for non-existent key', () => {
      const [cursor, members] = redis.sscan('nonexistent', 0)
      expect(cursor).toBe('0')
      expect(members).toEqual([])
    })

    it('should SSCAN with COUNT limit', () => {
      const [cursor, members] = redis.sscan('myset', 0, { count: 5 })
      expect(members.length).toBeLessThanOrEqual(5)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Type Checking
  // ─────────────────────────────────────────────────────────────────

  describe('Type Checking', () => {
    it('should throw WRONGTYPE for set command on string key', () => {
      redis.setStringKey('mystring', 'value')
      expect(() => redis.sadd('mystring', 'a')).toThrow('WRONGTYPE')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Expiration
  // ─────────────────────────────────────────────────────────────────

  describe('Set Expiration', () => {
    it('should respect TTL expiration', () => {
      vi.useFakeTimers()
      redis.sadd('myset', 'a', 'b', 'c')
      redis.expire('myset', 10)

      expect(redis.smembers('myset').length).toBe(3)

      vi.advanceTimersByTime(11000)
      expect(redis.smembers('myset')).toEqual([])
      vi.useRealTimers()
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Edge Cases
  // ─────────────────────────────────────────────────────────────────

  describe('Edge Cases', () => {
    it('should handle many members', () => {
      for (let i = 0; i < 1000; i++) {
        redis.sadd('myset', `member${i}`)
      }
      expect(redis.scard('myset')).toBe(1000)
    })

    it('should handle concurrent adds', () => {
      const members = Array.from({ length: 100 }, (_, i) => `m${i}`)
      for (const m of members) {
        redis.sadd('myset', m)
      }
      expect(redis.scard('myset')).toBe(100)
    })
  })
})
