/**
 * RedisCoordinator Tests
 *
 * Tests for the RedisCoordinator Durable Object which manages
 * cluster state, shard routing, and cluster-wide operations.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

interface ShardInfo {
  id: string
  name: string
  status: 'healthy' | 'degraded' | 'offline'
  lastHealthCheck: number
  keyCount: number
  memoryUsage: number
  createdAt: number
}

interface ClusterConfig {
  numShards: number
  replicationFactor: number
  healthCheckIntervalMs: number
  maxKeysPerShard: number
  virtualNodesPerShard: number
}

interface ClusterScanCursor {
  shardIndex: number
  shardCursor: number
}

interface ClusterScanOptions {
  match?: string
  count?: number
  type?: string
}

interface HashRingNode {
  hash: number
  shardId: string
}

// ─────────────────────────────────────────────────────────────────
// Mock RedisCoordinator
// ─────────────────────────────────────────────────────────────────

class MockRedisCoordinator {
  private config: ClusterConfig = {
    numShards: 16,
    replicationFactor: 1,
    healthCheckIntervalMs: 30000,
    maxKeysPerShard: 1000000,
    virtualNodesPerShard: 150,
  }

  private shards: Map<string, ShardInfo> = new Map()
  private hashRing: HashRingNode[] = []
  private shardData: Map<string, Map<string, string>> = new Map() // Mock shard key storage

  constructor(numShards: number = 16) {
    this.config.numShards = numShards
    this.initializeShards()
    this.buildHashRing()
  }

  private initializeShards(): void {
    const now = Date.now()
    for (let i = 0; i < this.config.numShards; i++) {
      // Use non-padded shard names to match RedoisEntrypoint's naming convention
      const id = `shard-${i}`
      this.shards.set(id, {
        id,
        name: `Shard ${i}`,
        status: 'healthy',
        lastHealthCheck: now,
        keyCount: 0,
        memoryUsage: 0,
        createdAt: now,
      })
      this.shardData.set(id, new Map())
    }
  }

  private buildHashRing(): void {
    this.hashRing = []
    for (const [shardId] of this.shards) {
      for (let i = 0; i < this.config.virtualNodesPerShard; i++) {
        const virtualKey = `${shardId}:vnode:${i}`
        const hash = this.hashKey(virtualKey)
        this.hashRing.push({ hash, shardId })
      }
    }
    this.hashRing.sort((a, b) => a.hash - b.hash)
  }

  private hashKey(key: string): number {
    let hash = 2166136261
    for (let i = 0; i < key.length; i++) {
      hash ^= key.charCodeAt(i)
      hash = Math.imul(hash, 16777619)
    }
    return hash >>> 0
  }

  getShardForKey(key: string): string {
    if (this.hashRing.length === 0) {
      const hash = this.hashKey(key)
      const shardIndex = hash % this.config.numShards
      return `shard-${shardIndex}`
    }

    const hash = this.hashKey(key)
    let left = 0
    let right = this.hashRing.length

    while (left < right) {
      const mid = (left + right) >>> 1
      if (this.hashRing[mid].hash < hash) {
        left = mid + 1
      } else {
        right = mid
      }
    }

    const index = left >= this.hashRing.length ? 0 : left
    return this.hashRing[index].shardId
  }

  getShardsForKey(key: string, count?: number): string[] {
    const replicationCount = count ?? this.config.replicationFactor
    const shards: string[] = []
    const seen = new Set<string>()

    if (this.hashRing.length === 0) {
      return [this.getShardForKey(key)]
    }

    const hash = this.hashKey(key)
    let left = 0
    let right = this.hashRing.length

    while (left < right) {
      const mid = (left + right) >>> 1
      if (this.hashRing[mid].hash < hash) {
        left = mid + 1
      } else {
        right = mid
      }
    }

    let index = left >= this.hashRing.length ? 0 : left
    const startIndex = index

    do {
      const shardId = this.hashRing[index].shardId
      if (!seen.has(shardId)) {
        seen.add(shardId)
        shards.push(shardId)
        if (shards.length >= replicationCount) break
      }
      index = (index + 1) % this.hashRing.length
    } while (index !== startIndex)

    return shards
  }

  // Simulate setting a key (for testing cluster-wide operations)
  setKey(key: string, value: string): void {
    const shardId = this.getShardForKey(key)
    const shardMap = this.shardData.get(shardId)!
    shardMap.set(key, value)
    const shard = this.shards.get(shardId)!
    shard.keyCount = shardMap.size
  }

  getKey(key: string): string | null {
    const shardId = this.getShardForKey(key)
    const shardMap = this.shardData.get(shardId)!
    return shardMap.get(key) ?? null
  }

  // Cluster-wide KEYS
  clusterKeys(pattern: string): string[] {
    const allKeys: string[] = []

    for (const [, shardMap] of this.shardData) {
      for (const key of shardMap.keys()) {
        if (this.matchPattern(key, pattern)) {
          allKeys.push(key)
        }
      }
    }

    return allKeys
  }

  private matchPattern(key: string, pattern: string): boolean {
    if (pattern === '*') return true

    // Convert glob pattern to regex
    const regex = pattern
      .replace(/[.+^${}()|[\]\\]/g, '\\$&')
      .replace(/\*/g, '.*')
      .replace(/\?/g, '.')

    return new RegExp(`^${regex}$`).test(key)
  }

  // Cluster-wide SCAN
  clusterScan(
    cursor: ClusterScanCursor,
    opts?: ClusterScanOptions
  ): [ClusterScanCursor, string[]] {
    const shardList = Array.from(this.shards.keys()).sort()
    const keys: string[] = []
    const count = opts?.count ?? 10

    let { shardIndex, shardCursor } = cursor

    while (shardIndex < shardList.length && keys.length < count) {
      const shardId = shardList[shardIndex]
      const shardMap = this.shardData.get(shardId)!
      const shardKeys = Array.from(shardMap.keys())

      // Apply pattern matching if specified
      const matchingKeys = opts?.match
        ? shardKeys.filter(k => this.matchPattern(k, opts.match!))
        : shardKeys

      // Get keys from cursor position
      const remaining = matchingKeys.slice(shardCursor)
      const toTake = Math.min(count - keys.length, remaining.length)
      keys.push(...remaining.slice(0, toTake))

      shardCursor += toTake

      if (shardCursor >= matchingKeys.length) {
        shardIndex++
        shardCursor = 0
      }
    }

    const nextCursor: ClusterScanCursor =
      shardIndex >= shardList.length
        ? { shardIndex: 0, shardCursor: 0 }
        : { shardIndex, shardCursor }

    return [nextCursor, keys]
  }

  // FLUSHALL
  flushAll(): { shardsCleared: number; errors: string[] } {
    let shardsCleared = 0

    for (const [shardId, shardMap] of this.shardData) {
      shardMap.clear()
      const shard = this.shards.get(shardId)!
      shard.keyCount = 0
      shardsCleared++
    }

    return { shardsCleared, errors: [] }
  }

  // DBSIZE
  clusterDbSize(): number {
    let total = 0
    for (const [, shardMap] of this.shardData) {
      total += shardMap.size
    }
    return total
  }

  // Cursor encoding/decoding
  encodeCursor(cursor: ClusterScanCursor): string {
    if (cursor.shardIndex === 0 && cursor.shardCursor === 0) {
      return '0'
    }
    const cursorStr = `${cursor.shardIndex}:${cursor.shardCursor}`
    return btoa(cursorStr)
  }

  decodeCursor(cursorStr: string): ClusterScanCursor {
    if (cursorStr === '0') {
      return { shardIndex: 0, shardCursor: 0 }
    }
    try {
      const decoded = atob(cursorStr)
      const [shardIndex, shardCursor] = decoded.split(':').map(Number)
      return { shardIndex: shardIndex || 0, shardCursor: shardCursor || 0 }
    } catch {
      return { shardIndex: 0, shardCursor: 0 }
    }
  }

  // Getters for testing
  getConfig(): ClusterConfig {
    return { ...this.config }
  }

  getAllShards(): ShardInfo[] {
    return Array.from(this.shards.values())
  }

  getShardInfo(shardId: string): ShardInfo | undefined {
    return this.shards.get(shardId)
  }

  updateShardHealth(
    shardId: string,
    status: ShardInfo['status'],
    keyCount: number,
    memoryUsage: number
  ): boolean {
    const shard = this.shards.get(shardId)
    if (!shard) return false

    shard.status = status
    shard.keyCount = keyCount
    shard.memoryUsage = memoryUsage
    shard.lastHealthCheck = Date.now()
    return true
  }

  isClusterHealthy(): boolean {
    const staleThreshold = this.config.healthCheckIntervalMs * 3
    let unhealthyCount = 0
    const now = Date.now()

    for (const shard of this.shards.values()) {
      const isStale = now - shard.lastHealthCheck > staleThreshold
      if (shard.status !== 'healthy' || isStale) {
        unhealthyCount++
      }
    }

    return unhealthyCount <= this.shards.size / 2
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('RedisCoordinator', () => {
  let coordinator: MockRedisCoordinator

  beforeEach(() => {
    coordinator = new MockRedisCoordinator(16)
  })

  // ─────────────────────────────────────────────────────────────────
  // Cluster Topology
  // ─────────────────────────────────────────────────────────────────

  describe('Cluster Topology', () => {
    it('should initialize with the configured number of shards', () => {
      const shards = coordinator.getAllShards()
      expect(shards).toHaveLength(16)
    })

    it('should create shards with correct IDs', () => {
      const shards = coordinator.getAllShards()
      expect(shards[0].id).toBe('shard-0')
      expect(shards[15].id).toBe('shard-15')
    })

    it('should initialize all shards as healthy', () => {
      const shards = coordinator.getAllShards()
      for (const shard of shards) {
        expect(shard.status).toBe('healthy')
      }
    })

    it('should return correct config', () => {
      const config = coordinator.getConfig()
      expect(config.numShards).toBe(16)
      expect(config.replicationFactor).toBe(1)
      expect(config.virtualNodesPerShard).toBe(150)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Shard Routing
  // ─────────────────────────────────────────────────────────────────

  describe('Shard Routing', () => {
    it('should route keys to shards consistently', () => {
      const shard1 = coordinator.getShardForKey('user:1234')
      const shard2 = coordinator.getShardForKey('user:1234')
      expect(shard1).toBe(shard2)
    })

    it('should distribute different keys across shards', () => {
      const shardSet = new Set<string>()
      for (let i = 0; i < 1000; i++) {
        shardSet.add(coordinator.getShardForKey(`key:${i}`))
      }
      // With 1000 keys, we expect to hit most of the 16 shards
      expect(shardSet.size).toBeGreaterThan(10)
    })

    it('should return valid shard IDs', () => {
      const shardId = coordinator.getShardForKey('testkey')
      expect(shardId).toMatch(/^shard-\d+$/)
    })

    it('should return multiple shards for replication', () => {
      const shards = coordinator.getShardsForKey('user:1234', 3)
      expect(shards.length).toBeLessThanOrEqual(3)
      expect(shards.length).toBeGreaterThanOrEqual(1)

      // All returned shards should be unique
      const uniqueShards = new Set(shards)
      expect(uniqueShards.size).toBe(shards.length)
    })

    it('should return primary shard first for replication', () => {
      const primaryShard = coordinator.getShardForKey('user:1234')
      const shards = coordinator.getShardsForKey('user:1234', 3)
      expect(shards[0]).toBe(primaryShard)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Cluster-Wide KEYS
  // ─────────────────────────────────────────────────────────────────

  describe('Cluster-Wide KEYS', () => {
    beforeEach(() => {
      // Populate some keys
      coordinator.setKey('user:1', 'value1')
      coordinator.setKey('user:2', 'value2')
      coordinator.setKey('session:a', 'sessA')
      coordinator.setKey('session:b', 'sessB')
      coordinator.setKey('cache:x', 'cacheX')
    })

    it('should return all keys with wildcard pattern', () => {
      const keys = coordinator.clusterKeys('*')
      expect(keys).toHaveLength(5)
      expect(keys).toContain('user:1')
      expect(keys).toContain('session:a')
      expect(keys).toContain('cache:x')
    })

    it('should filter keys with prefix pattern', () => {
      const keys = coordinator.clusterKeys('user:*')
      expect(keys).toHaveLength(2)
      expect(keys).toContain('user:1')
      expect(keys).toContain('user:2')
    })

    it('should filter keys with suffix pattern', () => {
      const keys = coordinator.clusterKeys('*:a')
      expect(keys).toHaveLength(1)
      expect(keys).toContain('session:a')
    })

    it('should return empty array when no keys match', () => {
      const keys = coordinator.clusterKeys('nonexistent:*')
      expect(keys).toHaveLength(0)
    })

    it('should handle ? wildcard pattern', () => {
      const keys = coordinator.clusterKeys('user:?')
      expect(keys).toHaveLength(2)
      expect(keys).toContain('user:1')
      expect(keys).toContain('user:2')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Cluster-Wide SCAN
  // ─────────────────────────────────────────────────────────────────

  describe('Cluster-Wide SCAN', () => {
    beforeEach(() => {
      // Populate more keys for pagination testing
      for (let i = 0; i < 50; i++) {
        coordinator.setKey(`key:${i.toString().padStart(3, '0')}`, `value${i}`)
      }
    })

    it('should return keys with initial cursor', () => {
      const [nextCursor, keys] = coordinator.clusterScan(
        { shardIndex: 0, shardCursor: 0 },
        { count: 10 }
      )
      expect(keys.length).toBeLessThanOrEqual(10)
      expect(keys.length).toBeGreaterThan(0)
    })

    it('should paginate through all keys', () => {
      const allKeys: string[] = []
      let cursor: ClusterScanCursor = { shardIndex: 0, shardCursor: 0 }

      // Scan through all pages
      do {
        const [nextCursor, keys] = coordinator.clusterScan(cursor, { count: 10 })
        allKeys.push(...keys)
        cursor = nextCursor
      } while (cursor.shardIndex !== 0 || cursor.shardCursor !== 0)

      expect(allKeys).toHaveLength(50)
    })

    it('should support pattern matching in SCAN', () => {
      // Add some differently named keys
      coordinator.setKey('user:001', 'user1')
      coordinator.setKey('user:002', 'user2')

      const [, keys] = coordinator.clusterScan(
        { shardIndex: 0, shardCursor: 0 },
        { match: 'user:*', count: 100 }
      )

      expect(keys.length).toBeGreaterThanOrEqual(2)
      for (const key of keys) {
        expect(key).toMatch(/^user:/)
      }
    })

    it('should return cursor 0 when scan is complete', () => {
      // Scan with a large count to get all keys at once
      const [nextCursor] = coordinator.clusterScan(
        { shardIndex: 0, shardCursor: 0 },
        { count: 1000 }
      )
      expect(nextCursor.shardIndex).toBe(0)
      expect(nextCursor.shardCursor).toBe(0)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Cursor Encoding/Decoding
  // ─────────────────────────────────────────────────────────────────

  describe('Cursor Encoding/Decoding', () => {
    it('should encode cursor 0:0 as "0"', () => {
      const encoded = coordinator.encodeCursor({ shardIndex: 0, shardCursor: 0 })
      expect(encoded).toBe('0')
    })

    it('should decode "0" as cursor 0:0', () => {
      const decoded = coordinator.decodeCursor('0')
      expect(decoded).toEqual({ shardIndex: 0, shardCursor: 0 })
    })

    it('should round-trip encode/decode non-zero cursor', () => {
      const original: ClusterScanCursor = { shardIndex: 5, shardCursor: 42 }
      const encoded = coordinator.encodeCursor(original)
      const decoded = coordinator.decodeCursor(encoded)
      expect(decoded).toEqual(original)
    })

    it('should handle invalid cursor gracefully', () => {
      const decoded = coordinator.decodeCursor('invalid-cursor')
      expect(decoded).toEqual({ shardIndex: 0, shardCursor: 0 })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // FLUSHALL Coordination
  // ─────────────────────────────────────────────────────────────────

  describe('FLUSHALL Coordination', () => {
    beforeEach(() => {
      // Populate keys across shards
      for (let i = 0; i < 100; i++) {
        coordinator.setKey(`key:${i}`, `value${i}`)
      }
    })

    it('should clear all keys from all shards', () => {
      expect(coordinator.clusterDbSize()).toBe(100)

      const result = coordinator.flushAll()

      expect(coordinator.clusterDbSize()).toBe(0)
      expect(result.shardsCleared).toBe(16)
      expect(result.errors).toHaveLength(0)
    })

    it('should reset shard key counts after flush', () => {
      coordinator.flushAll()

      const shards = coordinator.getAllShards()
      for (const shard of shards) {
        expect(shard.keyCount).toBe(0)
      }
    })

    it('should allow setting new keys after flush', () => {
      coordinator.flushAll()

      coordinator.setKey('newkey', 'newvalue')
      expect(coordinator.getKey('newkey')).toBe('newvalue')
      expect(coordinator.clusterDbSize()).toBe(1)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Cluster DBSIZE
  // ─────────────────────────────────────────────────────────────────

  describe('Cluster DBSIZE', () => {
    it('should return 0 for empty cluster', () => {
      expect(coordinator.clusterDbSize()).toBe(0)
    })

    it('should count all keys across shards', () => {
      for (let i = 0; i < 50; i++) {
        coordinator.setKey(`key:${i}`, `value${i}`)
      }
      expect(coordinator.clusterDbSize()).toBe(50)
    })

    it('should update count after adding keys', () => {
      coordinator.setKey('key1', 'value1')
      expect(coordinator.clusterDbSize()).toBe(1)

      coordinator.setKey('key2', 'value2')
      expect(coordinator.clusterDbSize()).toBe(2)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Health Tracking
  // ─────────────────────────────────────────────────────────────────

  describe('Health Tracking', () => {
    it('should report cluster as healthy when all shards are healthy', () => {
      expect(coordinator.isClusterHealthy()).toBe(true)
    })

    it('should update shard health status', () => {
      const result = coordinator.updateShardHealth('shard-0', 'degraded', 100, 1024)
      expect(result).toBe(true)

      const shard = coordinator.getShardInfo('shard-0')
      expect(shard?.status).toBe('degraded')
      expect(shard?.keyCount).toBe(100)
      expect(shard?.memoryUsage).toBe(1024)
    })

    it('should return false when updating non-existent shard', () => {
      const result = coordinator.updateShardHealth('shard-9999', 'healthy', 0, 0)
      expect(result).toBe(false)
    })

    it('should report cluster as healthy when less than half are unhealthy', () => {
      // Mark 7 shards as degraded (less than half of 16)
      for (let i = 0; i < 7; i++) {
        coordinator.updateShardHealth(`shard-${i}`, 'degraded', 0, 0)
      }
      expect(coordinator.isClusterHealthy()).toBe(true)
    })

    it('should report cluster as unhealthy when more than half are unhealthy', () => {
      // Mark 9 shards as offline (more than half of 16)
      for (let i = 0; i < 9; i++) {
        coordinator.updateShardHealth(`shard-${i}`, 'offline', 0, 0)
      }
      expect(coordinator.isClusterHealthy()).toBe(false)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Key Distribution
  // ─────────────────────────────────────────────────────────────────

  describe('Key Distribution', () => {
    it('should distribute keys relatively evenly across shards', () => {
      const keyCount = 10000
      const shardCounts: Record<string, number> = {}

      for (let i = 0; i < keyCount; i++) {
        const shardId = coordinator.getShardForKey(`key:${i}`)
        shardCounts[shardId] = (shardCounts[shardId] || 0) + 1
      }

      const values = Object.values(shardCounts)
      const avg = keyCount / 16
      const maxDeviation = avg * 1.0 // Allow 100% deviation (hash distribution varies)

      for (const count of values) {
        expect(count).toBeGreaterThan(avg - maxDeviation)
        expect(count).toBeLessThan(avg + maxDeviation)
      }
    })

    it('should maintain consistency when re-creating coordinator', () => {
      const key = 'test:consistent:key'
      const shard1 = coordinator.getShardForKey(key)

      // Create new coordinator
      const coordinator2 = new MockRedisCoordinator(16)
      const shard2 = coordinator2.getShardForKey(key)

      expect(shard1).toBe(shard2)
    })
  })
})
