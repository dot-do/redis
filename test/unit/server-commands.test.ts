/**
 * Server Commands Tests
 *
 * Tests for Redis server command implementations
 * Covers PING, ECHO, INFO, TIME
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// ─────────────────────────────────────────────────────────────────
// Mock Redis Server Store
// ─────────────────────────────────────────────────────────────────

interface KeyEntry {
  value: string
  type: 'string' | 'hash' | 'list' | 'set' | 'zset'
  expiresAt: number | null
}

class MockRedisServerStore {
  private store: Map<string, KeyEntry> = new Map()

  // ─────────────────────────────────────────────────────────────────
  // PING
  // ─────────────────────────────────────────────────────────────────

  ping(message?: string): string {
    if (message !== undefined) {
      return message
    }
    return 'PONG'
  }

  // ─────────────────────────────────────────────────────────────────
  // ECHO
  // ─────────────────────────────────────────────────────────────────

  echo(message: string): string {
    return message
  }

  // ─────────────────────────────────────────────────────────────────
  // INFO Command
  // ─────────────────────────────────────────────────────────────────

  info(section?: string): string {
    const keyCount = this.dbsize()

    const sections: Record<string, string> = {
      server: [
        '# Server',
        'redis_version:redois 0.1.0',
        'os:cloudflare-workers',
        'arch_bits:64',
      ].join('\r\n'),
      clients: [
        '# Clients',
        'connected_clients:1',
      ].join('\r\n'),
      memory: [
        '# Memory',
        'used_memory:0',
        'used_memory_human:0B',
      ].join('\r\n'),
      keyspace: [
        '# Keyspace',
        `db0:keys=${keyCount},expires=0`,
      ].join('\r\n'),
    }

    // If no section specified, return all sections
    if (!section) {
      return Object.values(sections).join('\r\n\r\n')
    }

    const sectionLower = section.toLowerCase()
    if (sections[sectionLower]) {
      return sections[sectionLower]
    }

    // Unknown section - return empty
    return ''
  }

  // ─────────────────────────────────────────────────────────────────
  // TIME Command
  // ─────────────────────────────────────────────────────────────────

  time(): [string, string] {
    const now = Date.now()
    const seconds = Math.floor(now / 1000)
    const microseconds = (now % 1000) * 1000
    return [String(seconds), String(microseconds)]
  }

  // ─────────────────────────────────────────────────────────────────
  // Helper methods
  // ─────────────────────────────────────────────────────────────────

  set(key: string, value: string): 'OK' {
    this.store.set(key, {
      value,
      type: 'string',
      expiresAt: null,
    })
    return 'OK'
  }

  dbsize(): number {
    return this.store.size
  }

  clear(): void {
    this.store.clear()
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('Server Commands', () => {
  let redis: MockRedisServerStore

  beforeEach(() => {
    redis = new MockRedisServerStore()
  })

  // ─────────────────────────────────────────────────────────────────
  // PING Command
  // ─────────────────────────────────────────────────────────────────

  describe('PING', () => {
    it('should return PONG when called without arguments', () => {
      expect(redis.ping()).toBe('PONG')
    })

    it('should echo the message when called with an argument', () => {
      expect(redis.ping('Hello')).toBe('Hello')
    })

    it('should handle empty string as message', () => {
      expect(redis.ping('')).toBe('')
    })

    it('should handle special characters in message', () => {
      expect(redis.ping('Hello\nWorld')).toBe('Hello\nWorld')
    })

    it('should handle unicode characters in message', () => {
      expect(redis.ping('Hello World')).toBe('Hello World')
    })

    it('should handle spaces in message', () => {
      expect(redis.ping('Hello World')).toBe('Hello World')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // ECHO Command
  // ─────────────────────────────────────────────────────────────────

  describe('ECHO', () => {
    it('should return the message', () => {
      expect(redis.echo('Hello World')).toBe('Hello World')
    })

    it('should handle empty string', () => {
      expect(redis.echo('')).toBe('')
    })

    it('should handle special characters', () => {
      expect(redis.echo('Hello\nWorld\ttab')).toBe('Hello\nWorld\ttab')
    })

    it('should handle unicode characters', () => {
      expect(redis.echo('Hello World')).toBe('Hello World')
    })

    it('should handle numeric strings', () => {
      expect(redis.echo('12345')).toBe('12345')
    })

    it('should handle binary-like strings', () => {
      expect(redis.echo('\x00\x01\x02\x03')).toBe('\x00\x01\x02\x03')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // INFO Command
  // ─────────────────────────────────────────────────────────────────

  describe('INFO Command', () => {
    it('should return all sections when called without arguments', () => {
      const info = redis.info()

      expect(info).toContain('# Server')
      expect(info).toContain('redis_version:redois 0.1.0')
      expect(info).toContain('# Memory')
      expect(info).toContain('# Keyspace')
    })

    it('should return server section when specified', () => {
      const info = redis.info('server')

      expect(info).toContain('# Server')
      expect(info).toContain('redis_version:redois 0.1.0')
      expect(info).not.toContain('# Memory')
    })

    it('should return memory section when specified', () => {
      const info = redis.info('memory')

      expect(info).toContain('# Memory')
      expect(info).toContain('used_memory')
      expect(info).not.toContain('# Server')
    })

    it('should return keyspace section with correct key count', () => {
      redis.set('key1', 'value1')
      redis.set('key2', 'value2')
      redis.set('key3', 'value3')

      const info = redis.info('keyspace')

      expect(info).toContain('# Keyspace')
      expect(info).toContain('db0:keys=3')
    })

    it('should be case-insensitive for section name', () => {
      const info1 = redis.info('SERVER')
      const info2 = redis.info('Server')
      const info3 = redis.info('server')

      expect(info1).toBe(info2)
      expect(info2).toBe(info3)
    })

    it('should return empty string for unknown section', () => {
      const info = redis.info('unknownsection')

      expect(info).toBe('')
    })

    it('should show zero keys when database is empty', () => {
      const info = redis.info('keyspace')

      expect(info).toContain('db0:keys=0')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // TIME Command
  // ─────────────────────────────────────────────────────────────────

  describe('TIME Command', () => {
    it('should return an array with two elements', () => {
      const time = redis.time()

      expect(Array.isArray(time)).toBe(true)
      expect(time).toHaveLength(2)
    })

    it('should return seconds as first element', () => {
      const now = Date.now()
      const time = redis.time()
      const seconds = parseInt(time[0], 10)

      // Should be within 1 second of current time
      expect(seconds).toBeGreaterThanOrEqual(Math.floor(now / 1000) - 1)
      expect(seconds).toBeLessThanOrEqual(Math.floor(now / 1000) + 1)
    })

    it('should return microseconds as second element', () => {
      const time = redis.time()
      const microseconds = parseInt(time[1], 10)

      // Microseconds should be between 0 and 999999
      expect(microseconds).toBeGreaterThanOrEqual(0)
      expect(microseconds).toBeLessThan(1000000)
    })

    it('should return string values', () => {
      const time = redis.time()

      expect(typeof time[0]).toBe('string')
      expect(typeof time[1]).toBe('string')
    })

    it('should return consistent time with fake timers', () => {
      vi.useFakeTimers()
      const fakeTime = 1704326400000 // 2024-01-04 00:00:00 UTC in milliseconds
      vi.setSystemTime(fakeTime)

      const time = redis.time()

      expect(time[0]).toBe('1704326400')
      expect(time[1]).toBe('0')

      vi.useRealTimers()
    })

    it('should return correct microseconds with fake timers', () => {
      vi.useFakeTimers()
      const fakeTime = 1704326400123 // 2024-01-04 00:00:00.123 UTC
      vi.setSystemTime(fakeTime)

      const time = redis.time()

      expect(time[0]).toBe('1704326400')
      expect(time[1]).toBe('123000') // 123 ms * 1000 = 123000 microseconds

      vi.useRealTimers()
    })
  })
})
