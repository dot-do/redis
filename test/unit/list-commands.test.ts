/**
 * List Commands Tests
 *
 * Tests for Redis list command implementations
 * Covers LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// ─────────────────────────────────────────────────────────────────
// Mock Redis List Store
// ─────────────────────────────────────────────────────────────────

interface ListEntry {
  elements: string[]
  type: 'list'
  expiresAt: number | null
}

interface KeyEntry {
  elements?: string[]
  value?: string
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisListStore {
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

  private assertType(key: string, expectedType: 'list'): void {
    const entry = this.store.get(key)
    if (entry && entry.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value')
    }
  }

  private getOrCreateList(key: string): string[] {
    this.cleanExpired(key)
    let entry = this.store.get(key)

    if (!entry) {
      entry = { elements: [], type: 'list', expiresAt: null }
      this.store.set(key, entry)
    } else {
      this.assertType(key, 'list')
    }

    return entry.elements!
  }

  private getList(key: string): string[] | null {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return null
    this.assertType(key, 'list')
    return entry.elements!
  }

  // ─────────────────────────────────────────────────────────────────
  // LPUSH/RPUSH
  // ─────────────────────────────────────────────────────────────────

  lpush(key: string, ...elements: string[]): number {
    const list = this.getOrCreateList(key)
    // LPUSH adds each element to the head in order
    // So LPUSH key a b c results in [c, b, a] - last element ends up at head
    for (const element of elements) {
      list.unshift(element)
    }
    return list.length
  }

  rpush(key: string, ...elements: string[]): number {
    const list = this.getOrCreateList(key)
    list.push(...elements)
    return list.length
  }

  lpushx(key: string, ...elements: string[]): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'list')

    // LPUSHX adds each element to the head in order
    for (const element of elements) {
      entry.elements!.unshift(element)
    }
    return entry.elements!.length
  }

  rpushx(key: string, ...elements: string[]): number {
    this.cleanExpired(key)
    const entry = this.store.get(key)
    if (!entry) return 0
    this.assertType(key, 'list')

    entry.elements!.push(...elements)
    return entry.elements!.length
  }

  // ─────────────────────────────────────────────────────────────────
  // LPOP/RPOP
  // ─────────────────────────────────────────────────────────────────

  lpop(key: string, count?: number): string | string[] | null {
    const list = this.getList(key)
    if (!list || list.length === 0) return null

    if (count === undefined) {
      const element = list.shift()
      if (list.length === 0) this.store.delete(key)
      return element ?? null
    }

    const elements = list.splice(0, count)
    if (list.length === 0) this.store.delete(key)
    return elements
  }

  rpop(key: string, count?: number): string | string[] | null {
    const list = this.getList(key)
    if (!list || list.length === 0) return null

    if (count === undefined) {
      const element = list.pop()
      if (list.length === 0) this.store.delete(key)
      return element ?? null
    }

    const elements = list.splice(-count).reverse()
    if (list.length === 0) this.store.delete(key)
    return elements
  }

  // ─────────────────────────────────────────────────────────────────
  // LRANGE/LLEN/LINDEX
  // ─────────────────────────────────────────────────────────────────

  lrange(key: string, start: number, stop: number): string[] {
    const list = this.getList(key)
    if (!list) return []

    const len = list.length

    // Handle negative indices
    let startIdx = start < 0 ? len + start : start
    let stopIdx = stop < 0 ? len + stop : stop

    // Clamp to valid range
    startIdx = Math.max(0, startIdx)
    stopIdx = Math.min(len - 1, stopIdx)

    if (startIdx > stopIdx) return []
    return list.slice(startIdx, stopIdx + 1)
  }

  llen(key: string): number {
    const list = this.getList(key)
    return list ? list.length : 0
  }

  lindex(key: string, index: number): string | null {
    const list = this.getList(key)
    if (!list) return null

    const len = list.length
    const idx = index < 0 ? len + index : index

    if (idx < 0 || idx >= len) return null
    return list[idx]
  }

  // ─────────────────────────────────────────────────────────────────
  // LSET/LINSERT/LREM/LTRIM
  // ─────────────────────────────────────────────────────────────────

  lset(key: string, index: number, element: string): 'OK' {
    const list = this.getList(key)
    if (!list) {
      throw new Error('ERR no such key')
    }

    const len = list.length
    const idx = index < 0 ? len + index : index

    if (idx < 0 || idx >= len) {
      throw new Error('ERR index out of range')
    }

    list[idx] = element
    return 'OK'
  }

  linsert(key: string, position: 'BEFORE' | 'AFTER', pivot: string, element: string): number {
    const list = this.getList(key)
    if (!list) return 0

    const pivotIndex = list.indexOf(pivot)
    if (pivotIndex === -1) return -1

    const insertIndex = position === 'BEFORE' ? pivotIndex : pivotIndex + 1
    list.splice(insertIndex, 0, element)
    return list.length
  }

  lrem(key: string, count: number, element: string): number {
    const list = this.getList(key)
    if (!list) return 0

    let removed = 0
    const absCount = Math.abs(count)

    if (count > 0) {
      // Remove from head
      for (let i = 0; i < list.length && removed < absCount; ) {
        if (list[i] === element) {
          list.splice(i, 1)
          removed++
        } else {
          i++
        }
      }
    } else if (count < 0) {
      // Remove from tail
      for (let i = list.length - 1; i >= 0 && removed < absCount; ) {
        if (list[i] === element) {
          list.splice(i, 1)
          removed++
        }
        i--
      }
    } else {
      // Remove all
      for (let i = 0; i < list.length; ) {
        if (list[i] === element) {
          list.splice(i, 1)
          removed++
        } else {
          i++
        }
      }
    }

    if (list.length === 0) this.store.delete(key)
    return removed
  }

  ltrim(key: string, start: number, stop: number): 'OK' {
    const list = this.getList(key)
    if (!list) return 'OK'

    const len = list.length
    let startIdx = start < 0 ? len + start : start
    let stopIdx = stop < 0 ? len + stop : stop

    startIdx = Math.max(0, startIdx)
    stopIdx = Math.min(len - 1, stopIdx)

    if (startIdx > stopIdx) {
      this.store.delete(key)
    } else {
      const entry = this.store.get(key)!
      entry.elements = list.slice(startIdx, stopIdx + 1)
      if (entry.elements.length === 0) this.store.delete(key)
    }

    return 'OK'
  }

  // ─────────────────────────────────────────────────────────────────
  // LPOS/LMOVE
  // ─────────────────────────────────────────────────────────────────

  lpos(key: string, element: string, options: { rank?: number; count?: number; maxlen?: number } = {}): number | number[] | null {
    const list = this.getList(key)
    // When count is specified, return empty array for non-existent key
    if (!list) return options.count !== undefined ? [] : null

    const rank = options.rank ?? 1
    const count = options.count
    const maxlen = options.maxlen ?? 0

    const positions: number[] = []
    let matchesFound = 0
    let matchesNeeded = Math.abs(rank)

    const startIdx = rank > 0 ? 0 : list.length - 1
    const endIdx = rank > 0 ? list.length : -1
    const step = rank > 0 ? 1 : -1
    const searchLen = maxlen > 0 ? Math.min(maxlen, list.length) : list.length

    for (let i = startIdx, searched = 0; i !== endIdx && searched < searchLen; i += step, searched++) {
      if (list[i] === element) {
        matchesFound++
        if (matchesFound >= matchesNeeded) {
          if (count !== undefined) {
            positions.push(i)
            // COUNT 0 means return all matches, only break if count > 0 and we've found enough
            if (count > 0 && positions.length >= count) break
          } else {
            return i
          }
        }
      }
    }

    if (count !== undefined) {
      return positions
    }
    return null
  }

  lmove(source: string, destination: string, whereFrom: 'LEFT' | 'RIGHT', whereTo: 'LEFT' | 'RIGHT'): string | null {
    const srcList = this.getList(source)
    if (!srcList || srcList.length === 0) return null

    // Pop from source
    const element = whereFrom === 'LEFT' ? srcList.shift()! : srcList.pop()!

    // Push to destination
    const destList = this.getOrCreateList(destination)
    if (whereTo === 'LEFT') {
      destList.unshift(element)
    } else {
      destList.push(element)
    }

    // Clean up empty source list
    if (srcList.length === 0) this.store.delete(source)

    return element
  }

  rpoplpush(source: string, destination: string): string | null {
    return this.lmove(source, destination, 'RIGHT', 'LEFT')
  }

  // Helper to delete a key
  del(key: string): number {
    if (this.store.delete(key)) {
      return 1
    }
    return 0
  }

  // Helper to set expiration
  expire(key: string, seconds: number): 0 | 1 {
    const entry = this.store.get(key)
    if (!entry) return 0
    entry.expiresAt = Date.now() + seconds * 1000
    return 1
  }

  // Helper to clear store
  clear(): void {
    this.store.clear()
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('List Commands', () => {
  let redis: MockRedisListStore

  beforeEach(() => {
    redis = new MockRedisListStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // LPUSH/RPUSH
  // ─────────────────────────────────────────────────────────────────

  describe('LPUSH', () => {
    it('should LPUSH single element to new list', () => {
      expect(redis.lpush('mylist', 'a')).toBe(1)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a'])
    })

    it('should LPUSH multiple elements', () => {
      expect(redis.lpush('mylist', 'a', 'b', 'c')).toBe(3)
      // Elements are pushed in order, so 'c' is at head
      expect(redis.lrange('mylist', 0, -1)).toEqual(['c', 'b', 'a'])
    })

    it('should LPUSH to existing list', () => {
      redis.rpush('mylist', 'x')
      expect(redis.lpush('mylist', 'a', 'b')).toBe(3)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'a', 'x'])
    })

    it('should handle empty string element', () => {
      redis.lpush('mylist', '')
      expect(redis.lindex('mylist', 0)).toBe('')
    })
  })

  describe('RPUSH', () => {
    it('should RPUSH single element to new list', () => {
      expect(redis.rpush('mylist', 'a')).toBe(1)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a'])
    })

    it('should RPUSH multiple elements', () => {
      expect(redis.rpush('mylist', 'a', 'b', 'c')).toBe(3)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c'])
    })

    it('should RPUSH to existing list', () => {
      redis.lpush('mylist', 'x')
      expect(redis.rpush('mylist', 'a', 'b')).toBe(3)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['x', 'a', 'b'])
    })
  })

  describe('LPUSHX', () => {
    it('should LPUSHX to existing list', () => {
      redis.rpush('mylist', 'a')
      expect(redis.lpushx('mylist', 'b')).toBe(2)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'a'])
    })

    it('should LPUSHX return 0 for non-existent key', () => {
      expect(redis.lpushx('mylist', 'a')).toBe(0)
      expect(redis.llen('mylist')).toBe(0)
    })

    it('should LPUSHX multiple elements', () => {
      redis.rpush('mylist', 'x')
      expect(redis.lpushx('mylist', 'a', 'b', 'c')).toBe(4)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['c', 'b', 'a', 'x'])
    })
  })

  describe('RPUSHX', () => {
    it('should RPUSHX to existing list', () => {
      redis.lpush('mylist', 'a')
      expect(redis.rpushx('mylist', 'b')).toBe(2)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b'])
    })

    it('should RPUSHX return 0 for non-existent key', () => {
      expect(redis.rpushx('mylist', 'a')).toBe(0)
      expect(redis.llen('mylist')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // LPOP/RPOP
  // ─────────────────────────────────────────────────────────────────

  describe('LPOP', () => {
    it('should LPOP single element', () => {
      redis.rpush('mylist', 'a', 'b', 'c')
      expect(redis.lpop('mylist')).toBe('a')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'c'])
    })

    it('should LPOP return null for empty list', () => {
      expect(redis.lpop('nonexistent')).toBeNull()
    })

    it('should LPOP with count', () => {
      redis.rpush('mylist', 'a', 'b', 'c', 'd')
      expect(redis.lpop('mylist', 2)).toEqual(['a', 'b'])
      expect(redis.lrange('mylist', 0, -1)).toEqual(['c', 'd'])
    })

    it('should LPOP with count larger than list', () => {
      redis.rpush('mylist', 'a', 'b')
      expect(redis.lpop('mylist', 5)).toEqual(['a', 'b'])
      expect(redis.llen('mylist')).toBe(0)
    })

    it('should delete key when list becomes empty', () => {
      redis.rpush('mylist', 'a')
      redis.lpop('mylist')
      expect(redis.llen('mylist')).toBe(0)
    })
  })

  describe('RPOP', () => {
    it('should RPOP single element', () => {
      redis.rpush('mylist', 'a', 'b', 'c')
      expect(redis.rpop('mylist')).toBe('c')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b'])
    })

    it('should RPOP return null for empty list', () => {
      expect(redis.rpop('nonexistent')).toBeNull()
    })

    it('should RPOP with count', () => {
      redis.rpush('mylist', 'a', 'b', 'c', 'd')
      expect(redis.rpop('mylist', 2)).toEqual(['d', 'c'])
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b'])
    })

    it('should RPOP with count larger than list', () => {
      redis.rpush('mylist', 'a', 'b')
      expect(redis.rpop('mylist', 5)).toEqual(['b', 'a'])
      expect(redis.llen('mylist')).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // LRANGE/LLEN/LINDEX
  // ─────────────────────────────────────────────────────────────────

  describe('LRANGE', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c', 'd', 'e')
    })

    it('should LRANGE entire list', () => {
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c', 'd', 'e'])
    })

    it('should LRANGE subset with positive indices', () => {
      expect(redis.lrange('mylist', 1, 3)).toEqual(['b', 'c', 'd'])
    })

    it('should LRANGE with negative indices', () => {
      expect(redis.lrange('mylist', -3, -1)).toEqual(['c', 'd', 'e'])
    })

    it('should LRANGE with mixed indices', () => {
      expect(redis.lrange('mylist', 0, -2)).toEqual(['a', 'b', 'c', 'd'])
    })

    it('should LRANGE return empty for out of range', () => {
      expect(redis.lrange('mylist', 10, 20)).toEqual([])
    })

    it('should LRANGE return empty for non-existent key', () => {
      expect(redis.lrange('nonexistent', 0, -1)).toEqual([])
    })

    it('should LRANGE handle start > stop', () => {
      expect(redis.lrange('mylist', 4, 2)).toEqual([])
    })

    it('should LRANGE clamp out of bounds', () => {
      expect(redis.lrange('mylist', -100, 100)).toEqual(['a', 'b', 'c', 'd', 'e'])
    })
  })

  describe('LLEN', () => {
    it('should LLEN return length of list', () => {
      redis.rpush('mylist', 'a', 'b', 'c')
      expect(redis.llen('mylist')).toBe(3)
    })

    it('should LLEN return 0 for non-existent key', () => {
      expect(redis.llen('nonexistent')).toBe(0)
    })

    it('should LLEN update after push/pop', () => {
      redis.rpush('mylist', 'a', 'b')
      expect(redis.llen('mylist')).toBe(2)
      redis.lpush('mylist', 'c')
      expect(redis.llen('mylist')).toBe(3)
      redis.rpop('mylist')
      expect(redis.llen('mylist')).toBe(2)
    })
  })

  describe('LINDEX', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c', 'd', 'e')
    })

    it('should LINDEX with positive index', () => {
      expect(redis.lindex('mylist', 0)).toBe('a')
      expect(redis.lindex('mylist', 2)).toBe('c')
      expect(redis.lindex('mylist', 4)).toBe('e')
    })

    it('should LINDEX with negative index', () => {
      expect(redis.lindex('mylist', -1)).toBe('e')
      expect(redis.lindex('mylist', -3)).toBe('c')
      expect(redis.lindex('mylist', -5)).toBe('a')
    })

    it('should LINDEX return null for out of range', () => {
      expect(redis.lindex('mylist', 10)).toBeNull()
      expect(redis.lindex('mylist', -10)).toBeNull()
    })

    it('should LINDEX return null for non-existent key', () => {
      expect(redis.lindex('nonexistent', 0)).toBeNull()
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // LSET/LINSERT/LREM/LTRIM
  // ─────────────────────────────────────────────────────────────────

  describe('LSET', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c')
    })

    it('should LSET element at positive index', () => {
      expect(redis.lset('mylist', 1, 'B')).toBe('OK')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'B', 'c'])
    })

    it('should LSET element at negative index', () => {
      expect(redis.lset('mylist', -1, 'C')).toBe('OK')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'C'])
    })

    it('should LSET throw for non-existent key', () => {
      expect(() => redis.lset('nonexistent', 0, 'x')).toThrow('ERR no such key')
    })

    it('should LSET throw for out of range index', () => {
      expect(() => redis.lset('mylist', 10, 'x')).toThrow('ERR index out of range')
    })
  })

  describe('LINSERT', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c')
    })

    it('should LINSERT BEFORE pivot', () => {
      expect(redis.linsert('mylist', 'BEFORE', 'b', 'X')).toBe(4)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'X', 'b', 'c'])
    })

    it('should LINSERT AFTER pivot', () => {
      expect(redis.linsert('mylist', 'AFTER', 'b', 'X')).toBe(4)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'X', 'c'])
    })

    it('should LINSERT return -1 if pivot not found', () => {
      expect(redis.linsert('mylist', 'BEFORE', 'z', 'X')).toBe(-1)
    })

    it('should LINSERT return 0 for non-existent key', () => {
      expect(redis.linsert('nonexistent', 'BEFORE', 'a', 'X')).toBe(0)
    })

    it('should LINSERT at head', () => {
      expect(redis.linsert('mylist', 'BEFORE', 'a', 'X')).toBe(4)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['X', 'a', 'b', 'c'])
    })

    it('should LINSERT at tail', () => {
      expect(redis.linsert('mylist', 'AFTER', 'c', 'X')).toBe(4)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c', 'X'])
    })
  })

  describe('LREM', () => {
    it('should LREM count > 0 (from head)', () => {
      redis.rpush('mylist', 'a', 'b', 'a', 'c', 'a')
      expect(redis.lrem('mylist', 2, 'a')).toBe(2)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'c', 'a'])
    })

    it('should LREM count < 0 (from tail)', () => {
      redis.rpush('mylist', 'a', 'b', 'a', 'c', 'a')
      expect(redis.lrem('mylist', -2, 'a')).toBe(2)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c'])
    })

    it('should LREM count = 0 (all occurrences)', () => {
      redis.rpush('mylist', 'a', 'b', 'a', 'c', 'a')
      expect(redis.lrem('mylist', 0, 'a')).toBe(3)
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'c'])
    })

    it('should LREM return 0 for element not in list', () => {
      redis.rpush('mylist', 'a', 'b', 'c')
      expect(redis.lrem('mylist', 0, 'x')).toBe(0)
    })

    it('should LREM return 0 for non-existent key', () => {
      expect(redis.lrem('nonexistent', 0, 'a')).toBe(0)
    })

    it('should LREM delete key when list becomes empty', () => {
      redis.rpush('mylist', 'a', 'a', 'a')
      redis.lrem('mylist', 0, 'a')
      expect(redis.llen('mylist')).toBe(0)
    })
  })

  describe('LTRIM', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c', 'd', 'e')
    })

    it('should LTRIM keep specified range', () => {
      expect(redis.ltrim('mylist', 1, 3)).toBe('OK')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['b', 'c', 'd'])
    })

    it('should LTRIM with negative indices', () => {
      expect(redis.ltrim('mylist', -3, -1)).toBe('OK')
      expect(redis.lrange('mylist', 0, -1)).toEqual(['c', 'd', 'e'])
    })

    it('should LTRIM to empty list', () => {
      expect(redis.ltrim('mylist', 10, 20)).toBe('OK')
      expect(redis.llen('mylist')).toBe(0)
    })

    it('should LTRIM return OK for non-existent key', () => {
      expect(redis.ltrim('nonexistent', 0, -1)).toBe('OK')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // LPOS/LMOVE
  // ─────────────────────────────────────────────────────────────────

  describe('LPOS', () => {
    beforeEach(() => {
      redis.rpush('mylist', 'a', 'b', 'c', 'b', 'd', 'b')
    })

    it('should LPOS find first occurrence', () => {
      expect(redis.lpos('mylist', 'b')).toBe(1)
    })

    it('should LPOS with RANK 2 (second occurrence)', () => {
      expect(redis.lpos('mylist', 'b', { rank: 2 })).toBe(3)
    })

    it('should LPOS with RANK 3', () => {
      expect(redis.lpos('mylist', 'b', { rank: 3 })).toBe(5)
    })

    it('should LPOS with negative RANK (from tail)', () => {
      expect(redis.lpos('mylist', 'b', { rank: -1 })).toBe(5)
    })

    it('should LPOS with COUNT', () => {
      expect(redis.lpos('mylist', 'b', { count: 2 })).toEqual([1, 3])
    })

    it('should LPOS with COUNT 0 (all occurrences)', () => {
      expect(redis.lpos('mylist', 'b', { count: 0 })).toEqual([1, 3, 5])
    })

    it('should LPOS with MAXLEN', () => {
      expect(redis.lpos('mylist', 'b', { maxlen: 2 })).toBe(1)
      expect(redis.lpos('mylist', 'b', { maxlen: 1 })).toBeNull()
    })

    it('should LPOS return null for element not found', () => {
      expect(redis.lpos('mylist', 'x')).toBeNull()
    })

    it('should LPOS return null for non-existent key', () => {
      expect(redis.lpos('nonexistent', 'a')).toBeNull()
    })
  })

  describe('LMOVE', () => {
    beforeEach(() => {
      redis.rpush('src', 'a', 'b', 'c')
      redis.rpush('dst', 'x', 'y')
    })

    it('should LMOVE LEFT to LEFT', () => {
      expect(redis.lmove('src', 'dst', 'LEFT', 'LEFT')).toBe('a')
      expect(redis.lrange('src', 0, -1)).toEqual(['b', 'c'])
      expect(redis.lrange('dst', 0, -1)).toEqual(['a', 'x', 'y'])
    })

    it('should LMOVE LEFT to RIGHT', () => {
      expect(redis.lmove('src', 'dst', 'LEFT', 'RIGHT')).toBe('a')
      expect(redis.lrange('src', 0, -1)).toEqual(['b', 'c'])
      expect(redis.lrange('dst', 0, -1)).toEqual(['x', 'y', 'a'])
    })

    it('should LMOVE RIGHT to LEFT', () => {
      expect(redis.lmove('src', 'dst', 'RIGHT', 'LEFT')).toBe('c')
      expect(redis.lrange('src', 0, -1)).toEqual(['a', 'b'])
      expect(redis.lrange('dst', 0, -1)).toEqual(['c', 'x', 'y'])
    })

    it('should LMOVE RIGHT to RIGHT', () => {
      expect(redis.lmove('src', 'dst', 'RIGHT', 'RIGHT')).toBe('c')
      expect(redis.lrange('src', 0, -1)).toEqual(['a', 'b'])
      expect(redis.lrange('dst', 0, -1)).toEqual(['x', 'y', 'c'])
    })

    it('should LMOVE return null for empty source', () => {
      expect(redis.lmove('nonexistent', 'dst', 'LEFT', 'LEFT')).toBeNull()
    })

    it('should LMOVE create destination if not exists', () => {
      redis.del('dst')
      expect(redis.lmove('src', 'newdst', 'LEFT', 'RIGHT')).toBe('a')
      expect(redis.lrange('newdst', 0, -1)).toEqual(['a'])
    })

    it('should LMOVE to same list (rotate)', () => {
      expect(redis.lmove('src', 'src', 'RIGHT', 'LEFT')).toBe('c')
      expect(redis.lrange('src', 0, -1)).toEqual(['c', 'a', 'b'])
    })
  })

  describe('RPOPLPUSH', () => {
    it('should RPOPLPUSH element', () => {
      redis.rpush('src', 'a', 'b', 'c')
      redis.rpush('dst', 'x')
      expect(redis.rpoplpush('src', 'dst')).toBe('c')
      expect(redis.lrange('src', 0, -1)).toEqual(['a', 'b'])
      expect(redis.lrange('dst', 0, -1)).toEqual(['c', 'x'])
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Type Checking
  // ─────────────────────────────────────────────────────────────────

  describe('Type Checking', () => {
    it('should throw WRONGTYPE for LPUSH on non-list key', () => {
      const store = redis as any
      store.store = new Map([['stringkey', { value: 'test', type: 'string', expiresAt: null }]])

      expect(() => redis.lpush('stringkey', 'a')).toThrow('WRONGTYPE')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Expiration
  // ─────────────────────────────────────────────────────────────────

  describe('List Expiration', () => {
    it('should respect TTL expiration', () => {
      vi.useFakeTimers()
      redis.rpush('mylist', 'a', 'b', 'c')
      redis.expire('mylist', 10)

      expect(redis.lrange('mylist', 0, -1)).toEqual(['a', 'b', 'c'])

      vi.advanceTimersByTime(11000)
      expect(redis.lrange('mylist', 0, -1)).toEqual([])
      vi.useRealTimers()
    })
  })
})
