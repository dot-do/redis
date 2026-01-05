/**
 * RedisCoordinator Durable Object
 *
 * Manages cluster state, key-to-shard routing via consistent hashing,
 * and cluster health tracking.
 *
 * EXPERIMENTAL: This coordinator is not yet integrated with RedoisEntrypoint.
 * The entrypoint currently uses its own simpler sharding logic (256 shards,
 * naming format `shard-{0-255}`). This coordinator provides more advanced
 * features like consistent hashing, replication, and health tracking that
 * may be integrated in a future version.
 *
 * Shard Naming Convention:
 * - Coordinator: `shard-{hash % numShards}` (e.g., shard-0, shard-1, ..., shard-15)
 * - This matches the entrypoint's `shard-{hash % 256}` format for future compatibility
 */

import { DurableObject } from 'cloudflare:workers'
import type { DurableObjectStub } from '@cloudflare/workers-types'
import type { Env } from '../types'

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

interface ClusterStats {
  totalKeys: number
  totalMemory: number
  healthyShards: number
  degradedShards: number
  offlineShards: number
  lastUpdated: number
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
// Default Configuration
// ─────────────────────────────────────────────────────────────────

const DEFAULT_CONFIG: ClusterConfig = {
  numShards: 16,
  replicationFactor: 1,
  healthCheckIntervalMs: 30000,
  maxKeysPerShard: 1000000,
  virtualNodesPerShard: 150, // For better distribution
}

// ─────────────────────────────────────────────────────────────────
// RedisCoordinator Durable Object
// ─────────────────────────────────────────────────────────────────

export class RedisCoordinator extends DurableObject<Env> {
  /** Cluster configuration */
  private config: ClusterConfig = DEFAULT_CONFIG

  /** Shard information cache */
  private shards: Map<string, ShardInfo> = new Map()

  /** Hash ring for consistent hashing */
  private hashRing: HashRingNode[] = []

  /** Whether initialization is complete */
  private initialized = false

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
  }

  // ─────────────────────────────────────────────────────────────────
  // Initialization
  // ─────────────────────────────────────────────────────────────────

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return

    await this.ctx.blockConcurrencyWhile(async () => {
      // Initialize SQL tables
      this.initializeTables()

      // Load configuration
      await this.loadConfig()

      // Load shard info
      await this.loadShards()

      // Build hash ring
      this.buildHashRing()

      this.initialized = true
    })
  }

  private initializeTables(): void {
    const sql = this.ctx.storage.sql

    // Configuration table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `)

    // Shards table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS shards (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'healthy',
        last_health_check INTEGER NOT NULL,
        key_count INTEGER NOT NULL DEFAULT 0,
        memory_usage INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL
      )
    `)

    // Cluster stats table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cluster_stats (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        total_keys INTEGER NOT NULL DEFAULT 0,
        total_memory INTEGER NOT NULL DEFAULT 0,
        healthy_shards INTEGER NOT NULL DEFAULT 0,
        degraded_shards INTEGER NOT NULL DEFAULT 0,
        offline_shards INTEGER NOT NULL DEFAULT 0,
        last_updated INTEGER NOT NULL
      )
    `)
  }

  private async loadConfig(): Promise<void> {
    const sql = this.ctx.storage.sql

    const rows = sql.exec('SELECT key, value FROM config').toArray()
    for (const row of rows) {
      const key = row.key as keyof ClusterConfig
      const value = JSON.parse(row.value as string)
      if (key in this.config) {
        (this.config as unknown as Record<string, unknown>)[key] = value
      }
    }
  }

  private async loadShards(): Promise<void> {
    const sql = this.ctx.storage.sql

    const rows = sql.exec('SELECT * FROM shards').toArray()
    for (const row of rows) {
      this.shards.set(row.id as string, {
        id: row.id as string,
        name: row.name as string,
        status: row.status as ShardInfo['status'],
        lastHealthCheck: row.last_health_check as number,
        keyCount: row.key_count as number,
        memoryUsage: row.memory_usage as number,
        createdAt: row.created_at as number,
      })
    }

    // Initialize shards if none exist
    if (this.shards.size === 0) {
      await this.initializeShards()
    }
  }

  private async initializeShards(): Promise<void> {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    for (let i = 0; i < this.config.numShards; i++) {
      // Use non-padded shard names to match RedoisEntrypoint's naming convention
      const id = `shard-${i}`
      const shard: ShardInfo = {
        id,
        name: `Shard ${i}`,
        status: 'healthy',
        lastHealthCheck: now,
        keyCount: 0,
        memoryUsage: 0,
        createdAt: now,
      }

      sql.exec(
        `INSERT INTO shards (id, name, status, last_health_check, key_count, memory_usage, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        shard.id,
        shard.name,
        shard.status,
        shard.lastHealthCheck,
        shard.keyCount,
        shard.memoryUsage,
        shard.createdAt
      )

      this.shards.set(id, shard)
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Consistent Hashing
  // ─────────────────────────────────────────────────────────────────

  /**
   * Build the hash ring with virtual nodes for better distribution
   */
  private buildHashRing(): void {
    this.hashRing = []

    for (const [shardId] of this.shards) {
      // Add virtual nodes for each shard
      for (let i = 0; i < this.config.virtualNodesPerShard; i++) {
        const virtualKey = `${shardId}:vnode:${i}`
        const hash = this.hashKey(virtualKey)
        this.hashRing.push({ hash, shardId })
      }
    }

    // Sort by hash for binary search
    this.hashRing.sort((a, b) => a.hash - b.hash)
  }

  /**
   * Hash a key using a consistent hash function (xxHash-like)
   * Returns a 32-bit unsigned integer
   */
  private hashKey(key: string): number {
    // Simple but effective hash function (FNV-1a variant)
    let hash = 2166136261
    for (let i = 0; i < key.length; i++) {
      hash ^= key.charCodeAt(i)
      hash = Math.imul(hash, 16777619)
    }
    return hash >>> 0 // Convert to unsigned 32-bit
  }

  /**
   * Get the shard ID for a given key using consistent hashing
   */
  getShardForKey(key: string): string {
    if (this.hashRing.length === 0) {
      // Fallback to simple modulo hashing (matches RedoisEntrypoint naming)
      const hash = this.hashKey(key)
      const shardIndex = hash % this.config.numShards
      return `shard-${shardIndex}`
    }

    const hash = this.hashKey(key)

    // Binary search for the first node with hash >= key hash
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

    // Wrap around to the first node if we're past the end
    const index = left >= this.hashRing.length ? 0 : left

    return this.hashRing[index].shardId
  }

  /**
   * Get multiple shards for replication
   */
  getShardsForKey(key: string, count?: number): string[] {
    const replicationCount = count ?? this.config.replicationFactor
    const shards: string[] = []
    const seen = new Set<string>()

    if (this.hashRing.length === 0) {
      const primary = this.getShardForKey(key)
      return [primary]
    }

    const hash = this.hashKey(key)

    // Binary search for starting position
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

    // Walk the ring collecting unique shards
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

  // ─────────────────────────────────────────────────────────────────
  // HTTP Handler
  // ─────────────────────────────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized()

    const url = new URL(request.url)
    const path = url.pathname

    switch (path) {
      case '/shard':
        return this.handleGetShard(url)
      case '/shards':
        return this.handleGetAllShards()
      case '/config':
        return request.method === 'POST'
          ? this.handleUpdateConfig(request)
          : this.handleGetConfig()
      case '/stats':
        return this.handleGetStats()
      case '/health':
        return request.method === 'POST'
          ? this.handleHealthUpdate(request)
          : this.handleHealthCheck()
      case '/rebalance':
        return this.handleRebalance()
      case '/keys':
        return this.handleClusterKeys(url)
      case '/scan':
        return this.handleClusterScan(url)
      case '/flushall':
        return request.method === 'POST'
          ? this.handleFlushAll()
          : new Response('Method Not Allowed', { status: 405 })
      case '/dbsize':
        return this.handleDbSize()
      default:
        return new Response('Not Found', { status: 404 })
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // API Handlers
  // ─────────────────────────────────────────────────────────────────

  private handleGetShard(url: URL): Response {
    const key = url.searchParams.get('key')
    if (!key) {
      return new Response(JSON.stringify({ error: 'key parameter required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const withReplicas = url.searchParams.get('replicas') === 'true'

    if (withReplicas) {
      const shards = this.getShardsForKey(key)
      return new Response(JSON.stringify({ key, shards }), {
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const shardId = this.getShardForKey(key)
    return new Response(JSON.stringify({ key, shardId }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleGetAllShards(): Response {
    const shards: ShardInfo[] = []
    for (const shard of this.shards.values()) {
      shards.push(shard)
    }
    return new Response(JSON.stringify({ shards }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleGetConfig(): Response {
    return new Response(JSON.stringify(this.config), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleUpdateConfig(request: Request): Promise<Response> {
    const updates = await request.json() as Partial<ClusterConfig>
    const sql = this.ctx.storage.sql
    const now = Date.now()

    for (const [key, value] of Object.entries(updates)) {
      if (key in this.config) {
        (this.config as unknown as Record<string, unknown>)[key] = value

        sql.exec(
          `INSERT OR REPLACE INTO config (key, value, updated_at) VALUES (?, ?, ?)`,
          key,
          JSON.stringify(value),
          now
        )
      }
    }

    // Rebuild hash ring if numShards or virtualNodesPerShard changed
    if ('numShards' in updates || 'virtualNodesPerShard' in updates) {
      // Reinitialize shards if count changed
      if ('numShards' in updates) {
        sql.exec('DELETE FROM shards')
        this.shards.clear()
        await this.initializeShards()
      }
      this.buildHashRing()
    }

    return new Response(JSON.stringify({ success: true, config: this.config }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleGetStats(): Response {
    const stats: ClusterStats = {
      totalKeys: 0,
      totalMemory: 0,
      healthyShards: 0,
      degradedShards: 0,
      offlineShards: 0,
      lastUpdated: Date.now(),
    }

    for (const shard of this.shards.values()) {
      stats.totalKeys += shard.keyCount
      stats.totalMemory += shard.memoryUsage

      switch (shard.status) {
        case 'healthy':
          stats.healthyShards++
          break
        case 'degraded':
          stats.degradedShards++
          break
        case 'offline':
          stats.offlineShards++
          break
      }
    }

    return new Response(JSON.stringify(stats), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleHealthCheck(): Response {
    const now = Date.now()
    const staleThreshold = this.config.healthCheckIntervalMs * 3

    const health = {
      status: 'healthy' as 'healthy' | 'degraded' | 'unhealthy',
      shards: [] as Array<{ id: string; status: string; age: number }>,
    }

    let unhealthyCount = 0

    for (const shard of this.shards.values()) {
      const age = now - shard.lastHealthCheck
      const isStale = age > staleThreshold

      health.shards.push({
        id: shard.id,
        status: isStale ? 'stale' : shard.status,
        age,
      })

      if (shard.status !== 'healthy' || isStale) {
        unhealthyCount++
      }
    }

    if (unhealthyCount > this.shards.size / 2) {
      health.status = 'unhealthy'
    } else if (unhealthyCount > 0) {
      health.status = 'degraded'
    }

    return new Response(JSON.stringify(health), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private async handleHealthUpdate(request: Request): Promise<Response> {
    const update = await request.json() as {
      shardId: string
      status?: ShardInfo['status']
      keyCount?: number
      memoryUsage?: number
    }

    const shard = this.shards.get(update.shardId)
    if (!shard) {
      return new Response(JSON.stringify({ error: 'Shard not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const now = Date.now()
    const sql = this.ctx.storage.sql

    // Update shard info
    if (update.status !== undefined) shard.status = update.status
    if (update.keyCount !== undefined) shard.keyCount = update.keyCount
    if (update.memoryUsage !== undefined) shard.memoryUsage = update.memoryUsage
    shard.lastHealthCheck = now

    sql.exec(
      `UPDATE shards SET status = ?, key_count = ?, memory_usage = ?, last_health_check = ? WHERE id = ?`,
      shard.status,
      shard.keyCount,
      shard.memoryUsage,
      shard.lastHealthCheck,
      shard.id
    )

    return new Response(JSON.stringify({ success: true, shard }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleRebalance(): Response {
    // Trigger a rebalance operation
    // In a real implementation, this would redistribute keys across shards
    const keyDistribution: Record<string, number> = {}

    for (const shard of this.shards.values()) {
      keyDistribution[shard.id] = shard.keyCount
    }

    const total = Object.values(keyDistribution).reduce((a, b) => a + b, 0)
    const average = total / this.shards.size
    const imbalance = Object.values(keyDistribution).reduce(
      (acc, count) => acc + Math.abs(count - average),
      0
    ) / total

    return new Response(JSON.stringify({
      message: 'Rebalance analysis complete',
      totalKeys: total,
      averagePerShard: Math.round(average),
      imbalanceRatio: imbalance.toFixed(4),
      distribution: keyDistribution,
      needsRebalance: imbalance > 0.1, // More than 10% imbalanced
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  /**
   * Handle cluster-wide KEYS command
   * Aggregates keys from all shards matching the pattern
   */
  private async handleClusterKeys(url: URL): Promise<Response> {
    const pattern = url.searchParams.get('pattern') ?? '*'

    try {
      const allKeys = await this.clusterKeys(pattern)
      return new Response(JSON.stringify({ keys: allKeys }), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      return new Response(JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Handle cluster-wide SCAN command
   * Iterates across all shards with cursor-based pagination
   */
  private async handleClusterScan(url: URL): Promise<Response> {
    const cursorParam = url.searchParams.get('cursor') ?? '0'
    const match = url.searchParams.get('match') ?? undefined
    const countParam = url.searchParams.get('count')
    const type = url.searchParams.get('type') ?? undefined

    const count = countParam ? parseInt(countParam, 10) : 10
    const cursor = this.decodeCursor(cursorParam)

    try {
      const [nextCursor, keys] = await this.clusterScan(cursor, { match, count, type })
      return new Response(JSON.stringify({
        cursor: this.encodeCursor(nextCursor),
        keys,
      }), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      return new Response(JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Handle FLUSHALL command across all shards
   */
  private async handleFlushAll(): Promise<Response> {
    try {
      const result = await this.flushAll()
      return new Response(JSON.stringify({
        success: true,
        shardsCleared: result.shardsCleared,
        errors: result.errors,
      }), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      return new Response(JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Handle cluster-wide DBSIZE command
   */
  private async handleDbSize(): Promise<Response> {
    try {
      const totalKeys = await this.clusterDbSize()
      return new Response(JSON.stringify({ totalKeys }), {
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      return new Response(JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error'
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Cursor Encoding/Decoding
  // ─────────────────────────────────────────────────────────────────

  /**
   * Encode a cluster scan cursor to a string
   * Format: base64(shardIndex:shardCursor)
   */
  private encodeCursor(cursor: ClusterScanCursor): string {
    if (cursor.shardIndex === 0 && cursor.shardCursor === 0) {
      return '0'
    }
    const cursorStr = `${cursor.shardIndex}:${cursor.shardCursor}`
    return btoa(cursorStr)
  }

  /**
   * Decode a cursor string back to ClusterScanCursor
   */
  private decodeCursor(cursorStr: string): ClusterScanCursor {
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

  // ─────────────────────────────────────────────────────────────────
  // Cluster-Wide Operations
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get a shard stub by shard ID
   */
  private getShardStub(shardId: string): DurableObjectStub {
    const id = this.env.REDIS_SHARDS.idFromName(shardId)
    return this.env.REDIS_SHARDS.get(id)
  }

  /**
   * Get all shard stubs for cluster-wide operations
   */
  private getAllShardStubs(): Array<{ id: string; stub: DurableObjectStub }> {
    const stubs: Array<{ id: string; stub: DurableObjectStub }> = []
    for (const shard of this.shards.values()) {
      stubs.push({
        id: shard.id,
        stub: this.getShardStub(shard.id),
      })
    }
    return stubs
  }

  /**
   * Cluster-wide KEYS implementation
   * Queries all shards in parallel and aggregates results
   */
  async clusterKeys(pattern: string): Promise<string[]> {
    await this.ensureInitialized()

    const shardStubs = this.getAllShardStubs()
    const allKeys: string[] = []

    // Query all shards in parallel
    const results = await Promise.allSettled(
      shardStubs.map(async ({ stub }) => {
        const response = await stub.fetch(
          `http://shard/keys?pattern=${encodeURIComponent(pattern)}`
        )
        if (!response.ok) {
          throw new Error(`Shard returned ${response.status}`)
        }
        const data = await response.json() as { keys: string[] }
        return data.keys
      })
    )

    // Aggregate successful results
    for (const result of results) {
      if (result.status === 'fulfilled') {
        allKeys.push(...result.value)
      }
    }

    return allKeys
  }

  /**
   * Cluster-wide SCAN implementation
   * Iterates through shards with cursor-based pagination
   */
  async clusterScan(
    cursor: ClusterScanCursor,
    opts?: ClusterScanOptions
  ): Promise<[ClusterScanCursor, string[]]> {
    await this.ensureInitialized()

    const shardList = Array.from(this.shards.keys()).sort()
    const keys: string[] = []
    const count = opts?.count ?? 10

    let { shardIndex, shardCursor } = cursor

    // Scan shards starting from the cursor position
    while (shardIndex < shardList.length && keys.length < count) {
      const shardId = shardList[shardIndex]
      const stub = this.getShardStub(shardId)

      // Build query params
      const params = new URLSearchParams()
      params.set('cursor', String(shardCursor))
      params.set('count', String(count - keys.length))
      if (opts?.match) params.set('match', opts.match)
      if (opts?.type) params.set('type', opts.type)

      try {
        const response = await stub.fetch(
          `http://shard/scan?${params.toString()}`
        )

        if (response.ok) {
          const data = await response.json() as { cursor: number; keys: string[] }
          keys.push(...data.keys)
          shardCursor = data.cursor

          // If shard cursor is 0, move to next shard
          if (shardCursor === 0) {
            shardIndex++
          }
        } else {
          // Skip failed shard, move to next
          shardIndex++
          shardCursor = 0
        }
      } catch {
        // Skip failed shard, move to next
        shardIndex++
        shardCursor = 0
      }
    }

    // Determine next cursor
    const nextCursor: ClusterScanCursor =
      shardIndex >= shardList.length
        ? { shardIndex: 0, shardCursor: 0 } // Scan complete
        : { shardIndex, shardCursor }

    return [nextCursor, keys]
  }

  /**
   * FLUSHALL implementation
   * Clears all keys from all shards
   */
  async flushAll(): Promise<{ shardsCleared: number; errors: string[] }> {
    await this.ensureInitialized()

    const shardStubs = this.getAllShardStubs()
    let shardsCleared = 0
    const errors: string[] = []

    // Flush all shards in parallel
    const results = await Promise.allSettled(
      shardStubs.map(async ({ id, stub }) => {
        const response = await stub.fetch('http://shard/flushdb', { method: 'POST' })
        if (!response.ok) {
          throw new Error(`Shard ${id} returned ${response.status}`)
        }
        return id
      })
    )

    // Count successes and collect errors
    for (const result of results) {
      if (result.status === 'fulfilled') {
        shardsCleared++
        // Update shard key count to 0
        const shard = this.shards.get(result.value)
        if (shard) {
          shard.keyCount = 0
          const sql = this.ctx.storage.sql
          sql.exec(
            `UPDATE shards SET key_count = 0, last_health_check = ? WHERE id = ?`,
            Date.now(),
            shard.id
          )
        }
      } else {
        errors.push(result.reason?.message || 'Unknown error')
      }
    }

    return { shardsCleared, errors }
  }

  /**
   * Cluster-wide DBSIZE implementation
   * Returns total keys across all shards
   */
  async clusterDbSize(): Promise<number> {
    await this.ensureInitialized()

    const shardStubs = this.getAllShardStubs()
    let totalKeys = 0

    // Query all shards in parallel
    const results = await Promise.allSettled(
      shardStubs.map(async ({ stub }) => {
        const response = await stub.fetch('http://shard/dbsize')
        if (!response.ok) {
          throw new Error(`Shard returned ${response.status}`)
        }
        const data = await response.json() as { count: number }
        return data.count
      })
    )

    // Sum up all key counts
    for (const result of results) {
      if (result.status === 'fulfilled') {
        totalKeys += result.value
      }
    }

    return totalKeys
  }

  // ─────────────────────────────────────────────────────────────────
  // RPC Methods (for direct DO-to-DO communication)
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get shard ID for a key (RPC method)
   */
  async getShardId(key: string): Promise<string> {
    await this.ensureInitialized()
    return this.getShardForKey(key)
  }

  /**
   * Get multiple shard IDs for replication (RPC method)
   */
  async getShardIds(key: string, count?: number): Promise<string[]> {
    await this.ensureInitialized()
    return this.getShardsForKey(key, count)
  }

  /**
   * Get cluster configuration (RPC method)
   */
  async getConfig(): Promise<ClusterConfig> {
    await this.ensureInitialized()
    return { ...this.config }
  }

  /**
   * Update shard health info (RPC method)
   */
  async updateShardHealth(
    shardId: string,
    status: ShardInfo['status'],
    keyCount: number,
    memoryUsage: number
  ): Promise<boolean> {
    await this.ensureInitialized()

    const shard = this.shards.get(shardId)
    if (!shard) return false

    shard.status = status
    shard.keyCount = keyCount
    shard.memoryUsage = memoryUsage
    shard.lastHealthCheck = Date.now()

    const sql = this.ctx.storage.sql
    sql.exec(
      `UPDATE shards SET status = ?, key_count = ?, memory_usage = ?, last_health_check = ? WHERE id = ?`,
      shard.status,
      shard.keyCount,
      shard.memoryUsage,
      shard.lastHealthCheck,
      shard.id
    )

    return true
  }

  /**
   * Get all shard info (RPC method)
   */
  async getAllShards(): Promise<ShardInfo[]> {
    await this.ensureInitialized()
    return Array.from(this.shards.values())
  }

  /**
   * Check if cluster is healthy (RPC method)
   */
  async isClusterHealthy(): Promise<boolean> {
    await this.ensureInitialized()

    const now = Date.now()
    const staleThreshold = this.config.healthCheckIntervalMs * 3
    let unhealthyCount = 0

    for (const shard of this.shards.values()) {
      const isStale = now - shard.lastHealthCheck > staleThreshold
      if (shard.status !== 'healthy' || isStale) {
        unhealthyCount++
      }
    }

    // Cluster is healthy if more than half the shards are healthy
    return unhealthyCount <= this.shards.size / 2
  }
}
