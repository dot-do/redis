/**
 * RedisShard Durable Object
 *
 * Core data storage Durable Object implementing Redis-compatible commands
 * using Cloudflare's SQLite storage with TTL support via Alarms API.
 */

import { DurableObject } from 'cloudflare:workers'
import type { Env, SetOptions, ScanOptions, ZAddOptions, ZRangeOptions, RedisType } from '../types'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

interface KeyRow {
  key: string
  type: RedisType
  expires_at: number | null
  created_at: number
  updated_at: number
}

interface StringRow {
  key: string
  value: ArrayBuffer | Uint8Array
}

interface HashRow {
  key: string
  field: string
  value: ArrayBuffer | Uint8Array
}

interface ListRow {
  key: string
  position: number
  value: ArrayBuffer | Uint8Array
}

interface SetRow {
  key: string
  member: ArrayBuffer | Uint8Array
}

interface ZSetRow {
  key: string
  member: ArrayBuffer | Uint8Array
  score: number
}

// ─────────────────────────────────────────────────────────────────
// RedisShard Durable Object
// ─────────────────────────────────────────────────────────────────

export class RedisShard extends DurableObject<Env> {
  private initialized = false
  private nextExpirationCheck: number | null = null

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // Initialize schema on first use
    this.ctx.blockConcurrencyWhile(async () => {
      this.initializeSchema()
      this.initialized = true
      // Schedule initial expiration check
      await this.scheduleExpiration()
    })
  }

  // ─────────────────────────────────────────────────────────────────
  // Schema Initialization
  // ─────────────────────────────────────────────────────────────────

  private initializeSchema(): void {
    const sql = this.ctx.storage.sql

    // Create keys metadata table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS keys (
        key TEXT PRIMARY KEY,
        type TEXT NOT NULL CHECK (type IN ('string', 'hash', 'list', 'set', 'zset', 'stream')),
        expires_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `)

    // Create strings table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS strings (
        key TEXT PRIMARY KEY REFERENCES keys(key) ON DELETE CASCADE,
        value BLOB NOT NULL
      )
    `)

    // Create hashes table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS hashes (
        key TEXT NOT NULL REFERENCES keys(key) ON DELETE CASCADE,
        field TEXT NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (key, field)
      )
    `)

    // Create lists table with position for ordering
    sql.exec(`
      CREATE TABLE IF NOT EXISTS lists (
        key TEXT NOT NULL REFERENCES keys(key) ON DELETE CASCADE,
        position REAL NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (key, position)
      )
    `)

    // Create sets table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS sets (
        key TEXT NOT NULL REFERENCES keys(key) ON DELETE CASCADE,
        member BLOB NOT NULL,
        PRIMARY KEY (key, member)
      )
    `)

    // Create sorted sets table
    sql.exec(`
      CREATE TABLE IF NOT EXISTS zsets (
        key TEXT NOT NULL REFERENCES keys(key) ON DELETE CASCADE,
        member BLOB NOT NULL,
        score REAL NOT NULL,
        PRIMARY KEY (key, member)
      )
    `)

    // Create indexes for efficient queries
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_keys_expires ON keys(expires_at) WHERE expires_at IS NOT NULL`)
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(key, score)`)
    sql.exec(`CREATE INDEX IF NOT EXISTS idx_lists_position ON lists(key, position)`)
  }

  // ─────────────────────────────────────────────────────────────────
  // TTL and Expiration
  // ─────────────────────────────────────────────────────────────────

  /**
   * Alarm handler - deletes expired keys
   */
  async alarm(): Promise<void> {
    const now = Date.now()
    const sql = this.ctx.storage.sql

    // Find and delete expired keys
    const expired = sql.exec(
      `SELECT key FROM keys WHERE expires_at IS NOT NULL AND expires_at <= ?`,
      now
    ).toArray() as { key: string }[]

    for (const row of expired) {
      await this.del(row.key)
    }

    // Schedule next expiration check
    this.nextExpirationCheck = null
    await this.scheduleExpiration()
  }

  /**
   * Schedule an alarm for the next key expiration
   */
  private async scheduleExpiration(): Promise<void> {
    const sql = this.ctx.storage.sql

    // Find the next key to expire
    const next = sql.exec(
      `SELECT MIN(expires_at) as next_expiry FROM keys WHERE expires_at IS NOT NULL`
    ).toArray() as { next_expiry: number | null }[]

    const nextExpiry = next[0]?.next_expiry

    if (nextExpiry !== null && nextExpiry !== undefined) {
      // Only reschedule if this is earlier than current alarm
      if (this.nextExpirationCheck === null || nextExpiry < this.nextExpirationCheck) {
        this.nextExpirationCheck = nextExpiry
        await this.ctx.storage.setAlarm(nextExpiry)
      }
    }
  }

  /**
   * Set expiration on a key
   */
  private async setKeyExpiration(key: string, expiresAt: number | null): Promise<void> {
    const sql = this.ctx.storage.sql

    sql.exec(
      `UPDATE keys SET expires_at = ?, updated_at = ? WHERE key = ?`,
      expiresAt,
      Date.now(),
      key
    )

    if (expiresAt !== null) {
      await this.scheduleExpiration()
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Key Management Helpers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Check if a key exists and is not expired
   */
  private keyExists(key: string): boolean {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const result = sql.exec(
      `SELECT 1 FROM keys WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)`,
      key,
      now
    ).toArray()

    return result.length > 0
  }

  /**
   * Get key metadata
   */
  private getKeyMeta(key: string): KeyRow | null {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const result = sql.exec(
      `SELECT key, type, expires_at, created_at, updated_at FROM keys
       WHERE key = ? AND (expires_at IS NULL OR expires_at > ?)`,
      key,
      now
    ).toArray() as unknown as KeyRow[]

    return result[0] || null
  }


  /**
   * Convert value to buffer for storage
   */
  private toBuffer(value: string | ArrayBuffer): Uint8Array {
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value)
    }
    return new TextEncoder().encode(value)
  }

  /**
   * Convert buffer to string
   */
  private fromBuffer(buffer: ArrayBuffer | Uint8Array | unknown): string {
    if (buffer instanceof Uint8Array) {
      return new TextDecoder().decode(buffer)
    }
    if (buffer instanceof ArrayBuffer) {
      return new TextDecoder().decode(new Uint8Array(buffer))
    }
    // Handle case where buffer might be returned as a different type from SQL
    if (typeof buffer === 'string') {
      return buffer
    }
    return new TextDecoder().decode(new Uint8Array(buffer as ArrayBuffer))
  }

  /**
   * Compare two buffers for equality
   */
  private bufferEquals(a: ArrayBuffer | Uint8Array, b: ArrayBuffer | Uint8Array): boolean {
    const arrA = a instanceof Uint8Array ? a : new Uint8Array(a)
    const arrB = b instanceof Uint8Array ? b : new Uint8Array(b)
    if (arrA.length !== arrB.length) return false
    for (let i = 0; i < arrA.length; i++) {
      if (arrA[i] !== arrB[i]) return false
    }
    return true
  }

  // ─────────────────────────────────────────────────────────────────
  // Synchronous Helper Methods (for use inside transactionSync)
  // ─────────────────────────────────────────────────────────────────

  /**
   * Synchronous version of GET for use inside transactionSync
   */
  private getSync(key: string): string | null {
    const sql = this.ctx.storage.sql
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'string') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const result = sql.exec(
      `SELECT value FROM strings WHERE key = ?`,
      key
    ).toArray() as unknown as StringRow[]

    if (result.length === 0) return null
    return this.fromBuffer(result[0].value)
  }

  /**
   * Synchronous version of DEL for a single key, for use inside transactionSync
   */
  private delSync(key: string): number {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0

    this.deleteKeyData(key, meta.type)
    this.ctx.storage.sql.exec(`DELETE FROM keys WHERE key = ?`, key)
    return 1
  }

  /**
   * Synchronous version of LLEN for use inside transactionSync
   */
  private llenSync(key: string): number {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT COUNT(*) as count FROM lists WHERE key = ?`,
      key
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * Synchronous version of LPOP (single element) for use inside transactionSync
   */
  private lpopSync(key: string): string | null {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT rowid, value FROM lists WHERE key = ? ORDER BY position ASC LIMIT 1`,
      key
    ).toArray() as { rowid: number; value: ArrayBuffer }[]

    if (result.length === 0) return null

    const value = this.fromBuffer(result[0].value)
    sql.exec(`DELETE FROM lists WHERE rowid = ?`, result[0].rowid)

    // Clean up empty list
    const remaining = sql.exec(
      `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
      key
    ).toArray()

    if (remaining.length === 0) {
      sql.exec(`DELETE FROM keys WHERE key = ?`, key)
    } else {
      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
    }

    return value
  }

  /**
   * Synchronous version of RPOP (single element) for use inside transactionSync
   */
  private rpopSync(key: string): string | null {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT rowid, value FROM lists WHERE key = ? ORDER BY position DESC LIMIT 1`,
      key
    ).toArray() as { rowid: number; value: ArrayBuffer }[]

    if (result.length === 0) return null

    const value = this.fromBuffer(result[0].value)
    sql.exec(`DELETE FROM lists WHERE rowid = ?`, result[0].rowid)

    // Clean up empty list
    const remaining = sql.exec(
      `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
      key
    ).toArray()

    if (remaining.length === 0) {
      sql.exec(`DELETE FROM keys WHERE key = ?`, key)
    } else {
      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
    }

    return value
  }

  /**
   * Synchronous version of LPUSH for use inside transactionSync
   * Returns the new length of the list
   */
  private lpushSync(key: string, ...elements: string[]): number {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const meta = this.getKeyMeta(key)
    if (meta && meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    // Ensure key exists
    sql.exec(
      `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
       VALUES (?, 'list', NULL, ?, ?)
       ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
      key,
      now,
      now
    )

    const range = this.getListPositionRange(key)
    let position = range ? range.min - 1 : 0

    // Insert elements in order so last element ends up at head (Redis LPUSH behavior)
    for (let i = 0; i < elements.length; i++) {
      sql.exec(
        `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
        key,
        position--,
        this.toBuffer(elements[i])
      )
    }

    return this.llenSync(key)
  }

  /**
   * Synchronous version of RPUSH for use inside transactionSync
   * Returns the new length of the list
   */
  private rpushSync(key: string, ...elements: string[]): number {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const meta = this.getKeyMeta(key)
    if (meta && meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    // Ensure key exists
    sql.exec(
      `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
       VALUES (?, 'list', NULL, ?, ?)
       ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
      key,
      now,
      now
    )

    const range = this.getListPositionRange(key)
    let position = range ? range.max + 1 : 0

    for (const element of elements) {
      sql.exec(
        `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
        key,
        position++,
        this.toBuffer(element)
      )
    }

    return this.llenSync(key)
  }

  // ─────────────────────────────────────────────────────────────────
  // String Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * GET key
   * Returns the string value of key, or null if key does not exist
   */
  async get(key: string): Promise<string | null> {
    const sql = this.ctx.storage.sql

    // Check key exists and type is string
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'string') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const result = sql.exec(
      `SELECT value FROM strings WHERE key = ?`,
      key
    ).toArray() as unknown as StringRow[]

    if (result.length === 0) return null
    return this.fromBuffer(result[0].value)
  }

  /**
   * SET key value [EX seconds] [PX milliseconds] [EXAT unix-time-seconds] [PXAT unix-time-milliseconds] [NX|XX] [KEEPTTL] [GET]
   */
  async set(key: string, value: string, opts?: SetOptions): Promise<string | null> {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    return this.ctx.storage.transactionSync(() => {
      const existingMeta = this.getKeyMeta(key)
      let oldValue: string | null = null

      // Handle NX/XX conditions
      if (opts?.nx && existingMeta) return null
      if (opts?.xx && !existingMeta) return null

      // Get old value if GET option is set
      if (opts?.get && existingMeta?.type === 'string') {
        const existing = sql.exec(
          `SELECT value FROM strings WHERE key = ?`,
          key
        ).toArray() as unknown as StringRow[]
        if (existing.length > 0) {
          oldValue = this.fromBuffer(existing[0].value)
        }
      }

      // Calculate expiration
      let expiresAt: number | null = null
      if (opts?.keepttl && existingMeta) {
        expiresAt = existingMeta.expires_at
      } else if (opts?.ex) {
        expiresAt = now + opts.ex * 1000
      } else if (opts?.px) {
        expiresAt = now + opts.px
      } else if (opts?.exat) {
        expiresAt = opts.exat * 1000
      } else if (opts?.pxat) {
        expiresAt = opts.pxat
      }

      // Delete existing key if different type
      if (existingMeta && existingMeta.type !== 'string') {
        this.deleteKeyData(key, existingMeta.type)
      }

      // Upsert key metadata
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'string', ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           type = 'string',
           expires_at = excluded.expires_at,
           updated_at = excluded.updated_at`,
        key,
        expiresAt,
        now,
        now
      )

      // Upsert string value
      sql.exec(
        `INSERT INTO strings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        this.toBuffer(value)
      )

      return opts?.get ? oldValue : 'OK'
    }) as string | null
  }

  /**
   * INCR key
   */
  async incr(key: string): Promise<number> {
    return this.incrby(key, 1)
  }

  /**
   * DECR key
   */
  async decr(key: string): Promise<number> {
    return this.incrby(key, -1)
  }

  /**
   * INCRBY key increment
   */
  async incrby(key: string, increment: number): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)

      if (meta && meta.type !== 'string') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = 0
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM strings WHERE key = ?`,
          key
        ).toArray() as unknown as StringRow[]

        if (existing.length > 0) {
          const str = this.fromBuffer(existing[0].value)
          const parsed = parseInt(str, 10)
          if (isNaN(parsed)) {
            throw new Error('ERR value is not an integer or out of range')
          }
          currentValue = parsed
        }
      }

      const newValue = currentValue + increment

      // Upsert key and value
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'string', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO strings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        this.toBuffer(newValue.toString())
      )

      return newValue
    }) as number
  }

  /**
   * DECRBY key decrement
   */
  async decrby(key: string, decrement: number): Promise<number> {
    return this.incrby(key, -decrement)
  }

  /**
   * INCRBYFLOAT key increment
   */
  async incrbyfloat(key: string, increment: number): Promise<string> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)

      if (meta && meta.type !== 'string') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = 0
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM strings WHERE key = ?`,
          key
        ).toArray() as unknown as StringRow[]

        if (existing.length > 0) {
          const str = this.fromBuffer(existing[0].value)
          const parsed = parseFloat(str)
          if (isNaN(parsed)) {
            throw new Error('ERR value is not a valid float')
          }
          currentValue = parsed
        }
      }

      const newValue = currentValue + increment
      const stringValue = newValue.toString()

      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'string', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO strings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        this.toBuffer(stringValue)
      )

      return stringValue
    }) as string
  }

  /**
   * MGET key [key ...]
   */
  async mget(...keys: string[]): Promise<(string | null)[]> {
    const results: (string | null)[] = []

    for (const key of keys) {
      try {
        results.push(await this.get(key))
      } catch {
        results.push(null)
      }
    }

    return results
  }

  /**
   * MSET key value [key value ...]
   */
  async mset(...pairs: string[]): Promise<'OK'> {
    if (pairs.length % 2 !== 0) {
      throw new Error('ERR wrong number of arguments for MSET')
    }

    this.ctx.storage.transactionSync(() => {
      for (let i = 0; i < pairs.length; i += 2) {
        const key = pairs[i]
        const value = pairs[i + 1]
        // Inline set without GET option
        const sql = this.ctx.storage.sql
        const now = Date.now()

        const existingMeta = this.getKeyMeta(key)
        if (existingMeta && existingMeta.type !== 'string') {
          this.deleteKeyData(key, existingMeta.type)
        }

        sql.exec(
          `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
           VALUES (?, 'string', NULL, ?, ?)
           ON CONFLICT(key) DO UPDATE SET
             type = 'string',
             expires_at = NULL,
             updated_at = excluded.updated_at`,
          key,
          now,
          now
        )

        sql.exec(
          `INSERT INTO strings (key, value) VALUES (?, ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
          key,
          this.toBuffer(value)
        )
      }
    })

    return 'OK'
  }

  /**
   * MSETNX key value [key value ...]
   */
  async msetnx(...pairs: string[]): Promise<number> {
    if (pairs.length % 2 !== 0) {
      throw new Error('ERR wrong number of arguments for MSETNX')
    }

    return this.ctx.storage.transactionSync(() => {
      // Check if any key exists
      for (let i = 0; i < pairs.length; i += 2) {
        if (this.keyExists(pairs[i])) {
          return 0
        }
      }

      // Set all keys
      const sql = this.ctx.storage.sql
      const now = Date.now()

      for (let i = 0; i < pairs.length; i += 2) {
        const key = pairs[i]
        const value = pairs[i + 1]

        sql.exec(
          `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
           VALUES (?, 'string', NULL, ?, ?)`,
          key,
          now,
          now
        )

        sql.exec(
          `INSERT INTO strings (key, value) VALUES (?, ?)`,
          key,
          this.toBuffer(value)
        )
      }

      return 1
    }) as number
  }

  /**
   * APPEND key value
   */
  async append(key: string, value: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)

      if (meta && meta.type !== 'string') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = ''
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM strings WHERE key = ?`,
          key
        ).toArray() as unknown as StringRow[]

        if (existing.length > 0) {
          currentValue = this.fromBuffer(existing[0].value)
        }
      }

      const newValue = currentValue + value

      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'string', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO strings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        this.toBuffer(newValue)
      )

      return newValue.length
    }) as number
  }

  /**
   * STRLEN key
   */
  async strlen(key: string): Promise<number> {
    const value = await this.get(key)
    return value?.length ?? 0
  }

  /**
   * GETRANGE key start end
   */
  async getrange(key: string, start: number, end: number): Promise<string> {
    const value = await this.get(key)
    if (!value) return ''

    // Handle negative indices
    const len = value.length
    let s = start < 0 ? Math.max(0, len + start) : start
    let e = end < 0 ? len + end : end

    // Redis is inclusive on both ends
    if (s > e || s >= len) return ''
    e = Math.min(e, len - 1)

    return value.substring(s, e + 1)
  }

  /**
   * SETRANGE key offset value
   */
  async setrange(key: string, offset: number, value: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)

      if (meta && meta.type !== 'string') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = ''
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM strings WHERE key = ?`,
          key
        ).toArray() as unknown as StringRow[]

        if (existing.length > 0) {
          currentValue = this.fromBuffer(existing[0].value)
        }
      }

      // Pad with null bytes if offset is beyond current length
      if (offset > currentValue.length) {
        currentValue = currentValue.padEnd(offset, '\x00')
      }

      // Replace at offset
      const newValue =
        currentValue.substring(0, offset) +
        value +
        currentValue.substring(offset + value.length)

      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'string', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO strings (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,
        this.toBuffer(newValue)
      )

      return newValue.length
    }) as number
  }

  /**
   * SETNX key value
   */
  async setnx(key: string, value: string): Promise<number> {
    const result = await this.set(key, value, { nx: true })
    return result === 'OK' ? 1 : 0
  }

  /**
   * SETEX key seconds value
   */
  async setex(key: string, seconds: number, value: string): Promise<'OK'> {
    await this.set(key, value, { ex: seconds })
    return 'OK'
  }

  /**
   * PSETEX key milliseconds value
   */
  async psetex(key: string, milliseconds: number, value: string): Promise<'OK'> {
    await this.set(key, value, { px: milliseconds })
    return 'OK'
  }

  /**
   * GETSET key value (deprecated in favor of SET with GET option)
   */
  async getset(key: string, value: string): Promise<string | null> {
    return this.set(key, value, { get: true })
  }

  /**
   * GETDEL key
   */
  async getdel(key: string): Promise<string | null> {
    return this.ctx.storage.transactionSync(() => {
      const value = this.getSync(key)
      if (value !== null) {
        this.delSync(key)
      }
      return value
    }) as string | null
  }

  /**
   * GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]
   */
  async getex(key: string, opts?: { ex?: number; px?: number; exat?: number; pxat?: number; persist?: boolean }): Promise<string | null> {
    const now = Date.now()

    const value = await this.get(key)
    if (value === null) return null

    // Update expiration
    let expiresAt: number | null = null
    if (opts?.persist) {
      expiresAt = null
    } else if (opts?.ex) {
      expiresAt = now + opts.ex * 1000
    } else if (opts?.px) {
      expiresAt = now + opts.px
    } else if (opts?.exat) {
      expiresAt = opts.exat * 1000
    } else if (opts?.pxat) {
      expiresAt = opts.pxat
    }

    if (opts) {
      await this.setKeyExpiration(key, expiresAt)
    }

    return value
  }

  // ─────────────────────────────────────────────────────────────────
  // Hash Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * HGET key field
   */
  async hget(key: string, field: string): Promise<string | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT value FROM hashes WHERE key = ? AND field = ?`,
      key,
      field
    ).toArray() as unknown as HashRow[]

    if (result.length === 0) return null
    return this.fromBuffer(result[0].value)
  }

  /**
   * HSET key field value [field value ...]
   * Returns number of fields added (not updated)
   */
  async hset(key: string, ...fieldValues: string[]): Promise<number> {
    if (fieldValues.length % 2 !== 0) {
      throw new Error('ERR wrong number of arguments for HSET')
    }

    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'hash') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'hash', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      let added = 0
      for (let i = 0; i < fieldValues.length; i += 2) {
        const field = fieldValues[i]
        const value = fieldValues[i + 1]

        // Check if field exists
        const existing = sql.exec(
          `SELECT 1 FROM hashes WHERE key = ? AND field = ?`,
          key,
          field
        ).toArray()

        if (existing.length === 0) {
          added++
        }

        sql.exec(
          `INSERT INTO hashes (key, field, value) VALUES (?, ?, ?)
           ON CONFLICT(key, field) DO UPDATE SET value = excluded.value`,
          key,
          field,
          this.toBuffer(value)
        )
      }

      return added
    }) as number
  }

  /**
   * HSETNX key field value
   */
  async hsetnx(key: string, field: string, value: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'hash') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Check if field already exists
      if (meta) {
        const existing = sql.exec(
          `SELECT 1 FROM hashes WHERE key = ? AND field = ?`,
          key,
          field
        ).toArray()

        if (existing.length > 0) {
          return 0
        }
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'hash', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO hashes (key, field, value) VALUES (?, ?, ?)`,
        key,
        field,
        this.toBuffer(value)
      )

      return 1
    }) as number
  }

  /**
   * HMGET key field [field ...]
   */
  async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return fields.map(() => null)
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const results: (string | null)[] = []

    for (const field of fields) {
      const result = sql.exec(
        `SELECT value FROM hashes WHERE key = ? AND field = ?`,
        key,
        field
      ).toArray() as unknown as HashRow[]

      if (result.length === 0) {
        results.push(null)
      } else {
        results.push(this.fromBuffer(result[0].value))
      }
    }

    return results
  }

  /**
   * HGETALL key
   */
  async hgetall(key: string): Promise<Record<string, string>> {
    const meta = this.getKeyMeta(key)
    if (!meta) return {}
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT field, value FROM hashes WHERE key = ?`,
      key
    ).toArray() as unknown as HashRow[]

    const hash: Record<string, string> = {}
    for (const row of result) {
      hash[row.field] = this.fromBuffer(row.value)
    }
    return hash
  }

  /**
   * HDEL key field [field ...]
   */
  async hdel(key: string, ...fields: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'hash') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      let deleted = 0

      for (const field of fields) {
        const existing = sql.exec(
          `SELECT 1 FROM hashes WHERE key = ? AND field = ?`,
          key,
          field
        ).toArray()

        if (existing.length > 0) {
          sql.exec(
            `DELETE FROM hashes WHERE key = ? AND field = ?`,
            key,
            field
          )
          deleted++
        }
      }

      // Clean up key if hash is empty
      const remaining = sql.exec(
        `SELECT 1 FROM hashes WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(
          `UPDATE keys SET updated_at = ? WHERE key = ?`,
          Date.now(),
          key
        )
      }

      return deleted
    }) as number
  }

  /**
   * HEXISTS key field
   */
  async hexists(key: string, field: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT 1 FROM hashes WHERE key = ? AND field = ?`,
      key,
      field
    ).toArray()

    return result.length > 0 ? 1 : 0
  }

  /**
   * HKEYS key
   */
  async hkeys(key: string): Promise<string[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return []
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT field FROM hashes WHERE key = ?`,
      key
    ).toArray() as { field: string }[]

    return result.map((r) => r.field)
  }

  /**
   * HVALS key
   */
  async hvals(key: string): Promise<string[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return []
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT value FROM hashes WHERE key = ?`,
      key
    ).toArray() as unknown as HashRow[]

    return result.map((r) => this.fromBuffer(r.value))
  }

  /**
   * HLEN key
   */
  async hlen(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'hash') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT COUNT(*) as count FROM hashes WHERE key = ?`,
      key
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * HINCRBY key field increment
   */
  async hincrby(key: string, field: string, increment: number): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'hash') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = 0
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM hashes WHERE key = ? AND field = ?`,
          key,
          field
        ).toArray() as unknown as HashRow[]

        if (existing.length > 0) {
          const str = this.fromBuffer(existing[0].value)
          const parsed = parseInt(str, 10)
          if (isNaN(parsed)) {
            throw new Error('ERR hash value is not an integer')
          }
          currentValue = parsed
        }
      }

      const newValue = currentValue + increment

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'hash', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO hashes (key, field, value) VALUES (?, ?, ?)
         ON CONFLICT(key, field) DO UPDATE SET value = excluded.value`,
        key,
        field,
        this.toBuffer(newValue.toString())
      )

      return newValue
    }) as number
  }

  /**
   * HINCRBYFLOAT key field increment
   */
  async hincrbyfloat(key: string, field: string, increment: number): Promise<string> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'hash') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      let currentValue = 0
      if (meta) {
        const existing = sql.exec(
          `SELECT value FROM hashes WHERE key = ? AND field = ?`,
          key,
          field
        ).toArray() as unknown as HashRow[]

        if (existing.length > 0) {
          const str = this.fromBuffer(existing[0].value)
          const parsed = parseFloat(str)
          if (isNaN(parsed)) {
            throw new Error('ERR hash value is not a float')
          }
          currentValue = parsed
        }
      }

      const newValue = currentValue + increment
      const stringValue = newValue.toString()

      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'hash', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO hashes (key, field, value) VALUES (?, ?, ?)
         ON CONFLICT(key, field) DO UPDATE SET value = excluded.value`,
        key,
        field,
        this.toBuffer(stringValue)
      )

      return stringValue
    }) as string
  }

  // ─────────────────────────────────────────────────────────────────
  // List Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get min and max positions for a list
   */
  private getListPositionRange(key: string): { min: number; max: number } | null {
    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT MIN(position) as min, MAX(position) as max FROM lists WHERE key = ?`,
      key
    ).toArray() as { min: number | null; max: number | null }[]

    if (result[0].min === null) return null
    return { min: result[0].min!, max: result[0].max! }
  }

  /**
   * LPUSH key element [element ...]
   */
  async lpush(key: string, ...elements: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'list', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      const range = this.getListPositionRange(key)
      let position = range ? range.min - 1 : 0

      // Insert elements in order so last element ends up at head (Redis LPUSH behavior)
      // LPUSH key a b c results in list [c, b, a] - last element is at head
      for (let i = 0; i < elements.length; i++) {
        sql.exec(
          `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
          key,
          position--,
          this.toBuffer(elements[i])
        )
      }

      return this.llenSync(key)
    }) as number
  }

  /**
   * RPUSH key element [element ...]
   */
  async rpush(key: string, ...elements: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'list', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      const range = this.getListPositionRange(key)
      let position = range ? range.max + 1 : 0

      for (const element of elements) {
        sql.exec(
          `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
          key,
          position++,
          this.toBuffer(element)
        )
      }

      return this.llenSync(key)
    }) as number
  }

  /**
   * LPUSHX key element [element ...]
   * Only pushes if key already exists
   */
  async lpushx(key: string, ...elements: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const range = this.getListPositionRange(key)
      let position = range ? range.min - 1 : 0

      // Insert elements in order so last element ends up at head (Redis LPUSHX behavior)
      for (let i = 0; i < elements.length; i++) {
        sql.exec(
          `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
          key,
          position--,
          this.toBuffer(elements[i])
        )
      }

      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      return this.llenSync(key)
    }) as number
  }

  /**
   * RPUSHX key element [element ...]
   * Only pushes if key already exists
   */
  async rpushx(key: string, ...elements: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const range = this.getListPositionRange(key)
      let position = range ? range.max + 1 : 0

      for (const element of elements) {
        sql.exec(
          `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
          key,
          position++,
          this.toBuffer(element)
        )
      }

      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      return this.llenSync(key)
    }) as number
  }

  /**
   * LPOP key [count]
   */
  async lpop(key: string, count?: number): Promise<string | string[] | null> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return count !== undefined ? [] : null
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const n = count ?? 1

      const result = sql.exec(
        `SELECT rowid, value FROM lists WHERE key = ? ORDER BY position ASC LIMIT ?`,
        key,
        n
      ).toArray() as { rowid: number; value: ArrayBuffer }[]

      if (result.length === 0) return count !== undefined ? [] : null

      const values: string[] = []
      for (const row of result) {
        values.push(this.fromBuffer(row.value))
        sql.exec(`DELETE FROM lists WHERE rowid = ?`, row.rowid)
      }

      // Clean up empty list
      const remaining = sql.exec(
        `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return count !== undefined ? values : values[0]
    }) as string | string[] | null
  }

  /**
   * RPOP key [count]
   */
  async rpop(key: string, count?: number): Promise<string | string[] | null> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return count !== undefined ? [] : null
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const n = count ?? 1

      const result = sql.exec(
        `SELECT rowid, value FROM lists WHERE key = ? ORDER BY position DESC LIMIT ?`,
        key,
        n
      ).toArray() as { rowid: number; value: ArrayBuffer }[]

      if (result.length === 0) return count !== undefined ? [] : null

      const values: string[] = []
      for (const row of result) {
        values.push(this.fromBuffer(row.value))
        sql.exec(`DELETE FROM lists WHERE rowid = ?`, row.rowid)
      }

      // Clean up empty list
      const remaining = sql.exec(
        `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return count !== undefined ? values : values[0]
    }) as string | string[] | null
  }

  /**
   * LRANGE key start stop
   */
  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return []
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const len = await this.llen(key)

    // Handle negative indices
    let s = start < 0 ? Math.max(0, len + start) : start
    let e = stop < 0 ? len + stop : stop

    if (s > e || s >= len) return []
    e = Math.min(e, len - 1)

    const result = sql.exec(
      `SELECT value FROM lists WHERE key = ? ORDER BY position ASC LIMIT ? OFFSET ?`,
      key,
      e - s + 1,
      s
    ).toArray() as unknown as ListRow[]

    return result.map((r) => this.fromBuffer(r.value))
  }

  /**
   * LLEN key
   */
  async llen(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT COUNT(*) as count FROM lists WHERE key = ?`,
      key
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * LINDEX key index
   */
  async lindex(key: string, index: number): Promise<string | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const len = await this.llen(key)

    // Handle negative index
    const idx = index < 0 ? len + index : index
    if (idx < 0 || idx >= len) return null

    const result = sql.exec(
      `SELECT value FROM lists WHERE key = ? ORDER BY position ASC LIMIT 1 OFFSET ?`,
      key,
      idx
    ).toArray() as unknown as ListRow[]

    if (result.length === 0) return null
    return this.fromBuffer(result[0].value)
  }

  /**
   * LSET key index element
   */
  async lset(key: string, index: number, element: string): Promise<'OK'> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) {
        throw new Error('ERR no such key')
      }
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const len = this.llenSync(key)

      // Handle negative index
      const idx = index < 0 ? len + index : index
      if (idx < 0 || idx >= len) {
        throw new Error('ERR index out of range')
      }

      // Get the rowid at this index
      const rows = sql.exec(
        `SELECT rowid FROM lists WHERE key = ? ORDER BY position ASC LIMIT 1 OFFSET ?`,
        key,
        idx
      ).toArray() as { rowid: number }[]

      if (rows.length === 0) {
        throw new Error('ERR index out of range')
      }

      sql.exec(
        `UPDATE lists SET value = ? WHERE rowid = ?`,
        this.toBuffer(element),
        rows[0].rowid
      )

      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)

      return 'OK'
    }) as 'OK'
  }

  /**
   * LREM key count element
   */
  async lrem(key: string, count: number, element: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const elementBuffer = this.toBuffer(element)

      // Find matching elements
      const order = count >= 0 ? 'ASC' : 'DESC'
      const limit = count === 0 ? 1000000 : Math.abs(count)

      const matches = sql.exec(
        `SELECT rowid FROM lists WHERE key = ? AND value = ? ORDER BY position ${order} LIMIT ?`,
        key,
        elementBuffer,
        limit
      ).toArray() as { rowid: number }[]

      let removed = 0
      for (const row of matches) {
        sql.exec(`DELETE FROM lists WHERE rowid = ?`, row.rowid)
        removed++
      }

      // Clean up empty list
      const remaining = sql.exec(
        `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return removed
    }) as number
  }

  /**
   * LTRIM key start stop
   */
  async ltrim(key: string, start: number, stop: number): Promise<'OK'> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 'OK'
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const len = this.llenSync(key)

      // Handle negative indices
      let s = start < 0 ? Math.max(0, len + start) : start
      let e = stop < 0 ? len + stop : stop

      // If range is invalid, delete everything
      if (s > e || s >= len) {
        sql.exec(`DELETE FROM lists WHERE key = ?`, key)
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
        return 'OK'
      }

      e = Math.min(e, len - 1)

      // Get rowids to keep
      const toKeep = sql.exec(
        `SELECT rowid FROM lists WHERE key = ? ORDER BY position ASC LIMIT ? OFFSET ?`,
        key,
        e - s + 1,
        s
      ).toArray() as { rowid: number }[]

      const keepIds = new Set(toKeep.map((r) => r.rowid))

      // Delete all others
      const all = sql.exec(
        `SELECT rowid FROM lists WHERE key = ?`,
        key
      ).toArray() as { rowid: number }[]

      for (const row of all) {
        if (!keepIds.has(row.rowid)) {
          sql.exec(`DELETE FROM lists WHERE rowid = ?`, row.rowid)
        }
      }

      // Clean up empty list
      const remaining = sql.exec(
        `SELECT 1 FROM lists WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return 'OK'
    }) as 'OK'
  }

  /**
   * LINSERT key BEFORE|AFTER pivot element
   */
  async linsert(key: string, position: 'BEFORE' | 'AFTER', pivot: string, element: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'list') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const pivotBuffer = this.toBuffer(pivot)

      // Find pivot position
      const pivotRow = sql.exec(
        `SELECT position FROM lists WHERE key = ? AND value = ? ORDER BY position ASC LIMIT 1`,
        key,
        pivotBuffer
      ).toArray() as { position: number }[]

      if (pivotRow.length === 0) return -1

      const pivotPos = pivotRow[0].position

      // Calculate new position
      let newPos: number
      if (position === 'BEFORE') {
        // Get the previous element's position
        const prev = sql.exec(
          `SELECT position FROM lists WHERE key = ? AND position < ? ORDER BY position DESC LIMIT 1`,
          key,
          pivotPos
        ).toArray() as { position: number }[]

        newPos = prev.length > 0 ? (prev[0].position + pivotPos) / 2 : pivotPos - 1
      } else {
        // Get the next element's position
        const next = sql.exec(
          `SELECT position FROM lists WHERE key = ? AND position > ? ORDER BY position ASC LIMIT 1`,
          key,
          pivotPos
        ).toArray() as { position: number }[]

        newPos = next.length > 0 ? (pivotPos + next[0].position) / 2 : pivotPos + 1
      }

      sql.exec(
        `INSERT INTO lists (key, position, value) VALUES (?, ?, ?)`,
        key,
        newPos,
        this.toBuffer(element)
      )

      sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)

      return this.llen(key) as unknown as number
    }) as number
  }

  /**
   * LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
   * Returns the index of matching elements in the list
   */
  async lpos(key: string, element: string, options?: { rank?: number; count?: number; maxlen?: number }): Promise<number | number[] | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return options?.count !== undefined ? [] : null
    if (meta.type !== 'list') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const elementBuffer = this.toBuffer(element)
    const rank = options?.rank ?? 1
    const count = options?.count
    const maxlen = options?.maxlen ?? 0 // 0 = unlimited

    // Get all list elements in order
    let query = `SELECT value, ROW_NUMBER() OVER (ORDER BY position ASC) - 1 as idx
                 FROM lists WHERE key = ?`
    if (maxlen > 0) {
      if (rank > 0) {
        query += ` ORDER BY position ASC LIMIT ?`
      } else {
        query += ` ORDER BY position DESC LIMIT ?`
      }
    } else {
      query += ` ORDER BY position ASC`
    }

    const rows = maxlen > 0
      ? (sql.exec(query, key, maxlen).toArray() as { value: ArrayBuffer; idx: number }[])
      : (sql.exec(query, key).toArray() as { value: ArrayBuffer; idx: number }[])

    // Find all matching indices
    const matches: number[] = []
    const searchForward = rank > 0
    const targetRank = Math.abs(rank)
    let currentRank = 0

    // If searching backward, reverse the list
    const searchList = searchForward ? rows : [...rows].reverse()

    for (const row of searchList) {
      if (this.bufferEquals(row.value, elementBuffer)) {
        currentRank++
        if (currentRank >= targetRank) {
          // For reverse search, we need to recalculate the actual index
          const actualIdx = searchForward ? row.idx : (rows.length - 1 - row.idx)
          matches.push(actualIdx)

          // If count is 0, return all matches; otherwise stop at count
          if (count !== undefined && count !== 0 && matches.length >= count) {
            break
          }
        }
      }
    }

    // If count is specified, return array
    if (count !== undefined) {
      return matches
    }

    // Otherwise return first match or null
    return matches.length > 0 ? matches[0] : null
  }

  /**
   * LMOVE source destination LEFT|RIGHT LEFT|RIGHT
   * Atomically moves element from source to destination
   */
  async lmove(source: string, destination: string, whereFrom: 'LEFT' | 'RIGHT', whereTo: 'LEFT' | 'RIGHT'): Promise<string | null> {
    return this.ctx.storage.transactionSync(() => {
      // Pop from source
      const element = whereFrom === 'LEFT'
        ? this.lpopSync(source)
        : this.rpopSync(source)

      if (element === null) {
        return null
      }

      // Push to destination
      if (whereTo === 'LEFT') {
        this.lpushSync(destination, element)
      } else {
        this.rpushSync(destination, element)
      }

      return element
    }) as string | null
  }

  // ─────────────────────────────────────────────────────────────────
  // Set Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * SADD key member [member ...]
   */
  async sadd(key: string, ...members: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'set') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'set', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      let added = 0
      for (const member of members) {
        const memberBuffer = this.toBuffer(member)
        const existing = sql.exec(
          `SELECT 1 FROM sets WHERE key = ? AND member = ?`,
          key,
          memberBuffer
        ).toArray()

        if (existing.length === 0) {
          sql.exec(
            `INSERT INTO sets (key, member) VALUES (?, ?)`,
            key,
            memberBuffer
          )
          added++
        }
      }

      return added
    }) as number
  }

  /**
   * SREM key member [member ...]
   */
  async srem(key: string, ...members: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'set') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      let removed = 0

      for (const member of members) {
        const memberBuffer = this.toBuffer(member)
        const existing = sql.exec(
          `SELECT 1 FROM sets WHERE key = ? AND member = ?`,
          key,
          memberBuffer
        ).toArray()

        if (existing.length > 0) {
          sql.exec(
            `DELETE FROM sets WHERE key = ? AND member = ?`,
            key,
            memberBuffer
          )
          removed++
        }
      }

      // Clean up empty set
      const remaining = sql.exec(
        `SELECT 1 FROM sets WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return removed
    }) as number
  }

  /**
   * SMEMBERS key
   */
  async smembers(key: string): Promise<string[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return []
    if (meta.type !== 'set') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT member FROM sets WHERE key = ?`,
      key
    ).toArray() as unknown as SetRow[]

    return result.map((r) => this.fromBuffer(r.member))
  }

  /**
   * SISMEMBER key member
   */
  async sismember(key: string, member: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'set') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT 1 FROM sets WHERE key = ? AND member = ?`,
      key,
      this.toBuffer(member)
    ).toArray()

    return result.length > 0 ? 1 : 0
  }

  /**
   * SMISMEMBER key member [member ...]
   */
  async smismember(key: string, ...members: string[]): Promise<number[]> {
    const meta = this.getKeyMeta(key)
    if (!meta) return members.map(() => 0)
    if (meta.type !== 'set') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const results: number[] = []

    for (const member of members) {
      const result = sql.exec(
        `SELECT 1 FROM sets WHERE key = ? AND member = ?`,
        key,
        this.toBuffer(member)
      ).toArray()
      results.push(result.length > 0 ? 1 : 0)
    }

    return results
  }

  /**
   * SCARD key
   */
  async scard(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'set') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT COUNT(*) as count FROM sets WHERE key = ?`,
      key
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * SPOP key [count]
   */
  async spop(key: string, count?: number): Promise<string | string[] | null> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return count !== undefined ? [] : null
      if (meta.type !== 'set') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const n = count ?? 1

      // Get random members using ORDER BY RANDOM()
      const result = sql.exec(
        `SELECT rowid, member FROM sets WHERE key = ? ORDER BY RANDOM() LIMIT ?`,
        key,
        n
      ).toArray() as { rowid: number; member: ArrayBuffer }[]

      if (result.length === 0) return count !== undefined ? [] : null

      const values: string[] = []
      for (const row of result) {
        values.push(this.fromBuffer(row.member))
        sql.exec(`DELETE FROM sets WHERE rowid = ?`, row.rowid)
      }

      // Clean up empty set
      const remaining = sql.exec(
        `SELECT 1 FROM sets WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return count !== undefined ? values : values[0]
    }) as string | string[] | null
  }

  /**
   * SRANDMEMBER key [count]
   */
  async srandmember(key: string, count?: number): Promise<string | string[] | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return count !== undefined ? [] : null
    if (meta.type !== 'set') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql

    if (count === undefined) {
      const result = sql.exec(
        `SELECT member FROM sets WHERE key = ? ORDER BY RANDOM() LIMIT 1`,
        key
      ).toArray() as unknown as SetRow[]
      return result.length > 0 ? this.fromBuffer(result[0].member) : null
    }

    const allowDuplicates = count < 0
    const n = Math.abs(count)

    if (allowDuplicates) {
      // Can return duplicates
      const result: string[] = []
      for (let i = 0; i < n; i++) {
        const row = sql.exec(
          `SELECT member FROM sets WHERE key = ? ORDER BY RANDOM() LIMIT 1`,
          key
        ).toArray() as unknown as SetRow[]
        if (row.length > 0) {
          result.push(this.fromBuffer(row[0].member))
        }
      }
      return result
    } else {
      // No duplicates
      const result = sql.exec(
        `SELECT member FROM sets WHERE key = ? ORDER BY RANDOM() LIMIT ?`,
        key,
        n
      ).toArray() as unknown as SetRow[]
      return result.map((r) => this.fromBuffer(r.member))
    }
  }

  /**
   * SDIFF key [key ...]
   */
  async sdiff(...keys: string[]): Promise<string[]> {
    if (keys.length === 0) return []

    const firstSet = new Set(await this.smembers(keys[0]))

    for (let i = 1; i < keys.length; i++) {
      const otherMembers = await this.smembers(keys[i])
      for (const member of otherMembers) {
        firstSet.delete(member)
      }
    }

    return Array.from(firstSet)
  }

  /**
   * SINTER key [key ...]
   */
  async sinter(...keys: string[]): Promise<string[]> {
    if (keys.length === 0) return []

    const firstMembers = await this.smembers(keys[0])
    let result = new Set(firstMembers)

    for (let i = 1; i < keys.length; i++) {
      const otherMembers = new Set(await this.smembers(keys[i]))
      result = new Set([...result].filter((x) => otherMembers.has(x)))
    }

    return Array.from(result)
  }

  /**
   * SUNION key [key ...]
   */
  async sunion(...keys: string[]): Promise<string[]> {
    const result = new Set<string>()

    for (const key of keys) {
      const members = await this.smembers(key)
      for (const member of members) {
        result.add(member)
      }
    }

    return Array.from(result)
  }

  /**
   * SDIFFSTORE destination key [key ...]
   */
  async sdiffstore(destination: string, ...keys: string[]): Promise<number> {
    const diff = await this.sdiff(...keys)
    await this.del(destination)
    if (diff.length > 0) {
      return this.sadd(destination, ...diff)
    }
    return 0
  }

  /**
   * SINTERSTORE destination key [key ...]
   */
  async sinterstore(destination: string, ...keys: string[]): Promise<number> {
    const inter = await this.sinter(...keys)
    await this.del(destination)
    if (inter.length > 0) {
      return this.sadd(destination, ...inter)
    }
    return 0
  }

  /**
   * SUNIONSTORE destination key [key ...]
   */
  async sunionstore(destination: string, ...keys: string[]): Promise<number> {
    const union = await this.sunion(...keys)
    await this.del(destination)
    if (union.length > 0) {
      return this.sadd(destination, ...union)
    }
    return 0
  }

  /**
   * SMOVE source destination member
   */
  async smove(source: string, destination: string, member: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sourceMeta = this.getKeyMeta(source)
      if (!sourceMeta) return 0
      if (sourceMeta.type !== 'set') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const memberBuffer = this.toBuffer(member)

      // Check if member exists in source
      const existing = sql.exec(
        `SELECT 1 FROM sets WHERE key = ? AND member = ?`,
        source,
        memberBuffer
      ).toArray()

      if (existing.length === 0) return 0

      // Remove from source
      sql.exec(
        `DELETE FROM sets WHERE key = ? AND member = ?`,
        source,
        memberBuffer
      )

      // Clean up empty source
      const sourceRemaining = sql.exec(
        `SELECT 1 FROM sets WHERE key = ? LIMIT 1`,
        source
      ).toArray()

      if (sourceRemaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, source)
      }

      // Add to destination
      const destMeta = this.getKeyMeta(destination)
      if (destMeta && destMeta.type !== 'set') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const now = Date.now()
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'set', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        destination,
        now,
        now
      )

      sql.exec(
        `INSERT OR IGNORE INTO sets (key, member) VALUES (?, ?)`,
        destination,
        memberBuffer
      )

      return 1
    }) as number
  }

  // ─────────────────────────────────────────────────────────────────
  // Sorted Set Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
   */
  async zadd(key: string, opts: ZAddOptions | number, ...scoreMembersOrScore: (number | string)[]): Promise<number> {
    // Handle both ZADD key score member and ZADD key opts score member syntaxes
    let options: ZAddOptions = {}
    let scoreMembers: (number | string)[]

    if (typeof opts === 'number') {
      scoreMembers = [opts, ...scoreMembersOrScore]
    } else {
      options = opts
      scoreMembers = scoreMembersOrScore as (number | string)[]
    }

    if (scoreMembers.length % 2 !== 0) {
      throw new Error('ERR syntax error')
    }

    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'zset') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'zset', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      let added = 0
      let changed = 0

      for (let i = 0; i < scoreMembers.length; i += 2) {
        const score = scoreMembers[i] as number
        const member = scoreMembers[i + 1] as string
        const memberBuffer = this.toBuffer(member)

        // Check if member exists
        const existing = sql.exec(
          `SELECT score FROM zsets WHERE key = ? AND member = ?`,
          key,
          memberBuffer
        ).toArray() as { score: number }[]

        if (existing.length > 0) {
          // Member exists
          if (options.nx) continue // NX: skip if exists

          const oldScore = existing[0].score
          let newScore = score

          // Apply GT/LT conditions
          if (options.gt && score <= oldScore) continue
          if (options.lt && score >= oldScore) continue

          // Update score
          sql.exec(
            `UPDATE zsets SET score = ? WHERE key = ? AND member = ?`,
            newScore,
            key,
            memberBuffer
          )

          if (oldScore !== newScore) {
            changed++
          }
        } else {
          // Member doesn't exist
          if (options.xx) continue // XX: skip if not exists

          sql.exec(
            `INSERT INTO zsets (key, member, score) VALUES (?, ?, ?)`,
            key,
            memberBuffer,
            score
          )
          added++
          changed++
        }
      }

      return options.ch ? changed : added
    }) as number
  }

  /**
   * ZREM key member [member ...]
   */
  async zrem(key: string, ...members: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return 0
      if (meta.type !== 'zset') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      let removed = 0

      for (const member of members) {
        const memberBuffer = this.toBuffer(member)
        const existing = sql.exec(
          `SELECT 1 FROM zsets WHERE key = ? AND member = ?`,
          key,
          memberBuffer
        ).toArray()

        if (existing.length > 0) {
          sql.exec(
            `DELETE FROM zsets WHERE key = ? AND member = ?`,
            key,
            memberBuffer
          )
          removed++
        }
      }

      // Clean up empty zset
      const remaining = sql.exec(
        `SELECT 1 FROM zsets WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return removed
    }) as number
  }

  /**
   * ZSCORE key member
   */
  async zscore(key: string, member: string): Promise<number | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT score FROM zsets WHERE key = ? AND member = ?`,
      key,
      this.toBuffer(member)
    ).toArray() as { score: number }[]

    return result.length > 0 ? result[0].score : null
  }

  /**
   * ZRANK key member
   */
  async zrank(key: string, member: string): Promise<number | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const memberBuffer = this.toBuffer(member)

    // Check if member exists
    const exists = sql.exec(
      `SELECT score FROM zsets WHERE key = ? AND member = ?`,
      key,
      memberBuffer
    ).toArray() as { score: number }[]

    if (exists.length === 0) return null

    // Count members with lower score or same score but lower member
    const score = exists[0].score
    const rank = sql.exec(
      `SELECT COUNT(*) as rank FROM zsets
       WHERE key = ? AND (score < ? OR (score = ? AND member < ?))`,
      key,
      score,
      score,
      memberBuffer
    ).toArray() as { rank: number }[]

    return rank[0].rank
  }

  /**
   * ZREVRANK key member
   */
  async zrevrank(key: string, member: string): Promise<number | null> {
    const meta = this.getKeyMeta(key)
    if (!meta) return null
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const memberBuffer = this.toBuffer(member)

    // Check if member exists
    const exists = sql.exec(
      `SELECT score FROM zsets WHERE key = ? AND member = ?`,
      key,
      memberBuffer
    ).toArray() as { score: number }[]

    if (exists.length === 0) return null

    // Count members with higher score or same score but higher member
    const score = exists[0].score
    const rank = sql.exec(
      `SELECT COUNT(*) as rank FROM zsets
       WHERE key = ? AND (score > ? OR (score = ? AND member > ?))`,
      key,
      score,
      score,
      memberBuffer
    ).toArray() as { rank: number }[]

    return rank[0].rank
  }

  /**
   * ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
   */
  async zrange(key: string, start: number | string, stop: number | string, opts?: ZRangeOptions): Promise<string[] | Array<{ member: string; score: number }>> {
    const meta = this.getKeyMeta(key)
    if (!meta) return []
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const order = opts?.rev ? 'DESC' : 'ASC'

    let query: string
    const params: unknown[] = [key]

    if (opts?.byScore) {
      // Score-based range
      const min = start === '-inf' ? -Infinity : Number(start)
      const max = stop === '+inf' ? Infinity : Number(stop)

      query = `SELECT member, score FROM zsets WHERE key = ? AND score >= ? AND score <= ? ORDER BY score ${order}, member ${order}`
      params.push(min, max)
    } else if (opts?.byLex) {
      // Lexicographic range (requires same score)
      // For simplicity, treat as member-based ordering
      const minMember = String(start).replace(/^\[/, '').replace(/^\(/, '')
      const maxMember = String(stop).replace(/^\[/, '').replace(/^\(/, '')
      const minInclusive = !String(start).startsWith('(')
      const maxInclusive = !String(stop).startsWith('(')

      query = `SELECT member, score FROM zsets WHERE key = ? AND member ${minInclusive ? '>=' : '>'} ? AND member ${maxInclusive ? '<=' : '<'} ? ORDER BY member ${order}`
      params.push(this.toBuffer(minMember), this.toBuffer(maxMember))
    } else {
      // Index-based range
      const count = await this.zcard(key)
      let s = Number(start) < 0 ? Math.max(0, count + Number(start)) : Number(start)
      let e = Number(stop) < 0 ? count + Number(stop) : Number(stop)

      if (opts?.rev) {
        // Reverse indices
        const tmpS = count - 1 - e
        const tmpE = count - 1 - s
        s = Math.max(0, tmpS)
        e = tmpE
      }

      if (s > e || s >= count) return []
      e = Math.min(e, count - 1)

      query = `SELECT member, score FROM zsets WHERE key = ? ORDER BY score ${order}, member ${order} LIMIT ? OFFSET ?`
      params.push(e - s + 1, s)
    }

    // Apply LIMIT for BYSCORE/BYLEX
    if ((opts?.byScore || opts?.byLex) && opts?.limit) {
      query = query.replace(
        /ORDER BY/,
        `ORDER BY`
      )
      query += ` LIMIT ? OFFSET ?`
      params.push(opts.limit.count, opts.limit.offset)
    }

    const result = sql.exec(query, ...params).toArray() as unknown as ZSetRow[]

    if (opts?.withScores) {
      return result.map((r) => ({
        member: this.fromBuffer(r.member),
        score: r.score,
      }))
    }

    return result.map((r) => this.fromBuffer(r.member))
  }

  /**
   * ZREVRANGE key start stop [WITHSCORES]
   */
  async zrevrange(key: string, start: number, stop: number, opts?: { withScores?: boolean }): Promise<string[] | Array<{ member: string; score: number }>> {
    return this.zrange(key, start, stop, { ...opts, rev: true })
  }

  /**
   * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
   */
  async zrangebyscore(
    key: string,
    min: number | string,
    max: number | string,
    opts?: { withScores?: boolean; limit?: { offset: number; count: number } }
  ): Promise<string[] | Array<{ member: string; score: number }>> {
    return this.zrange(key, min, max, { ...opts, byScore: true })
  }

  /**
   * ZCARD key
   */
  async zcard(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const result = sql.exec(
      `SELECT COUNT(*) as count FROM zsets WHERE key = ?`,
      key
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * ZCOUNT key min max
   */
  async zcount(key: string, min: number | string, max: number | string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.type !== 'zset') {
      throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
    }

    const sql = this.ctx.storage.sql
    const minScore = min === '-inf' ? -Infinity : Number(min)
    const maxScore = max === '+inf' ? Infinity : Number(max)

    const result = sql.exec(
      `SELECT COUNT(*) as count FROM zsets WHERE key = ? AND score >= ? AND score <= ?`,
      key,
      minScore,
      maxScore
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * ZINCRBY key increment member
   */
  async zincrby(key: string, increment: number, member: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      const now = Date.now()

      const meta = this.getKeyMeta(key)
      if (meta && meta.type !== 'zset') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const memberBuffer = this.toBuffer(member)

      // Get current score
      let currentScore = 0
      if (meta) {
        const existing = sql.exec(
          `SELECT score FROM zsets WHERE key = ? AND member = ?`,
          key,
          memberBuffer
        ).toArray() as { score: number }[]

        if (existing.length > 0) {
          currentScore = existing[0].score
        }
      }

      const newScore = currentScore + increment

      // Ensure key exists
      sql.exec(
        `INSERT INTO keys (key, type, expires_at, created_at, updated_at)
         VALUES (?, 'zset', NULL, ?, ?)
         ON CONFLICT(key) DO UPDATE SET updated_at = excluded.updated_at`,
        key,
        now,
        now
      )

      sql.exec(
        `INSERT INTO zsets (key, member, score) VALUES (?, ?, ?)
         ON CONFLICT(key, member) DO UPDATE SET score = excluded.score`,
        key,
        memberBuffer,
        newScore
      )

      return newScore
    }) as number
  }

  /**
   * ZPOPMIN key [count]
   */
  async zpopmin(key: string, count?: number): Promise<Array<{ member: string; score: number }>> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return []
      if (meta.type !== 'zset') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const n = count ?? 1

      const result = sql.exec(
        `SELECT rowid, member, score FROM zsets WHERE key = ? ORDER BY score ASC, member ASC LIMIT ?`,
        key,
        n
      ).toArray() as { rowid: number; member: ArrayBuffer; score: number }[]

      const popped: Array<{ member: string; score: number }> = []
      for (const row of result) {
        popped.push({
          member: this.fromBuffer(row.member),
          score: row.score,
        })
        sql.exec(`DELETE FROM zsets WHERE rowid = ?`, row.rowid)
      }

      // Clean up empty zset
      const remaining = sql.exec(
        `SELECT 1 FROM zsets WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return popped
    }) as Array<{ member: string; score: number }>
  }

  /**
   * ZPOPMAX key [count]
   */
  async zpopmax(key: string, count?: number): Promise<Array<{ member: string; score: number }>> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) return []
      if (meta.type !== 'zset') {
        throw new Error(`WRONGTYPE Operation against a key holding the wrong kind of value`)
      }

      const sql = this.ctx.storage.sql
      const n = count ?? 1

      const result = sql.exec(
        `SELECT rowid, member, score FROM zsets WHERE key = ? ORDER BY score DESC, member DESC LIMIT ?`,
        key,
        n
      ).toArray() as { rowid: number; member: ArrayBuffer; score: number }[]

      const popped: Array<{ member: string; score: number }> = []
      for (const row of result) {
        popped.push({
          member: this.fromBuffer(row.member),
          score: row.score,
        })
        sql.exec(`DELETE FROM zsets WHERE rowid = ?`, row.rowid)
      }

      // Clean up empty zset
      const remaining = sql.exec(
        `SELECT 1 FROM zsets WHERE key = ? LIMIT 1`,
        key
      ).toArray()

      if (remaining.length === 0) {
        sql.exec(`DELETE FROM keys WHERE key = ?`, key)
      } else {
        sql.exec(`UPDATE keys SET updated_at = ? WHERE key = ?`, Date.now(), key)
      }

      return popped
    }) as Array<{ member: string; score: number }>
  }

  // ─────────────────────────────────────────────────────────────────
  // Key Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * DEL key [key ...]
   */
  async del(...keys: string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      let deleted = 0

      for (const key of keys) {
        const meta = this.getKeyMeta(key)
        if (!meta) continue

        this.deleteKeyData(key, meta.type)
        this.ctx.storage.sql.exec(`DELETE FROM keys WHERE key = ?`, key)
        deleted++
      }

      return deleted
    }) as number
  }

  /**
   * Delete data for a key based on its type
   */
  private deleteKeyData(key: string, type: RedisType): void {
    const sql = this.ctx.storage.sql

    switch (type) {
      case 'string':
        sql.exec(`DELETE FROM strings WHERE key = ?`, key)
        break
      case 'hash':
        sql.exec(`DELETE FROM hashes WHERE key = ?`, key)
        break
      case 'list':
        sql.exec(`DELETE FROM lists WHERE key = ?`, key)
        break
      case 'set':
        sql.exec(`DELETE FROM sets WHERE key = ?`, key)
        break
      case 'zset':
        sql.exec(`DELETE FROM zsets WHERE key = ?`, key)
        break
    }
  }

  /**
   * UNLINK key [key ...]
   * Same as DEL in this implementation
   */
  async unlink(...keys: string[]): Promise<number> {
    return this.del(...keys)
  }

  /**
   * EXISTS key [key ...]
   */
  async exists(...keys: string[]): Promise<number> {
    let count = 0
    for (const key of keys) {
      if (this.keyExists(key)) {
        count++
      }
    }
    return count
  }

  /**
   * EXPIRE key seconds
   */
  async expire(key: string, seconds: number): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0

    await this.setKeyExpiration(key, Date.now() + seconds * 1000)
    return 1
  }

  /**
   * PEXPIRE key milliseconds
   */
  async pexpire(key: string, milliseconds: number): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0

    await this.setKeyExpiration(key, Date.now() + milliseconds)
    return 1
  }

  /**
   * EXPIREAT key unix-time-seconds
   */
  async expireat(key: string, timestamp: number): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0

    await this.setKeyExpiration(key, timestamp * 1000)
    return 1
  }

  /**
   * PEXPIREAT key unix-time-milliseconds
   */
  async pexpireat(key: string, timestamp: number): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0

    await this.setKeyExpiration(key, timestamp)
    return 1
  }

  /**
   * TTL key
   * Returns time to live in seconds, or -1 if no TTL, or -2 if key doesn't exist
   */
  async ttl(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return -2
    if (meta.expires_at === null) return -1

    const remaining = Math.ceil((meta.expires_at - Date.now()) / 1000)
    return Math.max(remaining, 0)
  }

  /**
   * PTTL key
   * Returns time to live in milliseconds, or -1 if no TTL, or -2 if key doesn't exist
   */
  async pttl(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return -2
    if (meta.expires_at === null) return -1

    const remaining = meta.expires_at - Date.now()
    return Math.max(remaining, 0)
  }

  /**
   * PERSIST key
   * Remove TTL from key
   */
  async persist(key: string): Promise<number> {
    const meta = this.getKeyMeta(key)
    if (!meta) return 0
    if (meta.expires_at === null) return 0

    await this.setKeyExpiration(key, null)
    return 1
  }

  /**
   * TYPE key
   */
  async type(key: string): Promise<RedisType> {
    const meta = this.getKeyMeta(key)
    return meta?.type ?? 'none'
  }

  /**
   * RENAME key newkey
   */
  async rename(key: string, newkey: string): Promise<'OK'> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) {
        throw new Error('ERR no such key')
      }

      const sql = this.ctx.storage.sql
      const now = Date.now()

      // Delete new key if it exists
      const newMeta = this.getKeyMeta(newkey)
      if (newMeta) {
        this.deleteKeyData(newkey, newMeta.type)
        sql.exec(`DELETE FROM keys WHERE key = ?`, newkey)
      }

      // Update key name in metadata
      sql.exec(
        `UPDATE keys SET key = ?, updated_at = ? WHERE key = ?`,
        newkey,
        now,
        key
      )

      // Update key name in data table
      switch (meta.type) {
        case 'string':
          sql.exec(`UPDATE strings SET key = ? WHERE key = ?`, newkey, key)
          break
        case 'hash':
          sql.exec(`UPDATE hashes SET key = ? WHERE key = ?`, newkey, key)
          break
        case 'list':
          sql.exec(`UPDATE lists SET key = ? WHERE key = ?`, newkey, key)
          break
        case 'set':
          sql.exec(`UPDATE sets SET key = ? WHERE key = ?`, newkey, key)
          break
        case 'zset':
          sql.exec(`UPDATE zsets SET key = ? WHERE key = ?`, newkey, key)
          break
      }

      return 'OK'
    }) as 'OK'
  }

  /**
   * RENAMENX key newkey
   */
  async renamenx(key: string, newkey: string): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      const meta = this.getKeyMeta(key)
      if (!meta) {
        throw new Error('ERR no such key')
      }

      // Check if new key exists
      if (this.keyExists(newkey)) {
        return 0
      }

      this.rename(key, newkey)
      return 1
    }) as number
  }

  /**
   * KEYS pattern
   */
  async keys(pattern: string): Promise<string[]> {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    // Get all non-expired keys
    const allKeys = sql.exec(
      `SELECT key FROM keys WHERE expires_at IS NULL OR expires_at > ?`,
      now
    ).toArray() as { key: string }[]

    if (pattern === '*') {
      return allKeys.map((r) => r.key)
    }

    // Convert Redis glob pattern to regex
    const regex = this.globToRegex(pattern)

    return allKeys
      .map((r) => r.key)
      .filter((k) => regex.test(k))
  }

  /**
   * SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
   */
  async scan(cursor: number, opts?: ScanOptions): Promise<[number, string[]]> {
    const sql = this.ctx.storage.sql
    const now = Date.now()
    const count = opts?.count ?? 10

    // Build query
    let query = `SELECT key, type FROM keys WHERE (expires_at IS NULL OR expires_at > ?)`
    const params: unknown[] = [now]

    if (opts?.type) {
      query += ` AND type = ?`
      params.push(opts.type)
    }

    query += ` ORDER BY key LIMIT ? OFFSET ?`
    params.push(count + 1, cursor) // Fetch one extra to check if more exist

    const result = sql.exec(query, ...params).toArray() as { key: string; type: string }[]

    let keys = result.map((r) => r.key)
    let nextCursor = 0

    // If we got more than count, there are more results
    if (keys.length > count) {
      keys = keys.slice(0, count)
      nextCursor = cursor + count
    }

    // Apply pattern matching
    if (opts?.match && opts.match !== '*') {
      const regex = this.globToRegex(opts.match)
      keys = keys.filter((k) => regex.test(k))
    }

    return [nextCursor, keys]
  }

  /**
   * DBSIZE
   */
  async dbsize(): Promise<number> {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const result = sql.exec(
      `SELECT COUNT(*) as count FROM keys WHERE expires_at IS NULL OR expires_at > ?`,
      now
    ).toArray() as { count: number }[]

    return result[0]?.count ?? 0
  }

  /**
   * FLUSHDB
   */
  async flushdb(): Promise<'OK'> {
    this.ctx.storage.transactionSync(() => {
      const sql = this.ctx.storage.sql
      sql.exec(`DELETE FROM strings`)
      sql.exec(`DELETE FROM hashes`)
      sql.exec(`DELETE FROM lists`)
      sql.exec(`DELETE FROM sets`)
      sql.exec(`DELETE FROM zsets`)
      sql.exec(`DELETE FROM keys`)
    })
    return 'OK'
  }

  /**
   * RANDOMKEY
   */
  async randomkey(): Promise<string | null> {
    const sql = this.ctx.storage.sql
    const now = Date.now()

    const result = sql.exec(
      `SELECT key FROM keys WHERE expires_at IS NULL OR expires_at > ? ORDER BY RANDOM() LIMIT 1`,
      now
    ).toArray() as { key: string }[]

    return result[0]?.key ?? null
  }

  /**
   * Convert Redis glob pattern to JavaScript RegExp
   */
  private globToRegex(pattern: string): RegExp {
    let regex = '^'
    let i = 0

    while (i < pattern.length) {
      const char = pattern[i]

      switch (char) {
        case '*':
          regex += '.*'
          break
        case '?':
          regex += '.'
          break
        case '[':
          const end = pattern.indexOf(']', i)
          if (end === -1) {
            regex += '\\['
          } else {
            const charClass = pattern.slice(i, end + 1)
            regex += charClass
            i = end
          }
          break
        case '\\':
          if (i + 1 < pattern.length) {
            regex += '\\' + pattern[i + 1]
            i++
          } else {
            regex += '\\\\'
          }
          break
        case '.':
        case '+':
        case '^':
        case '$':
        case '(':
        case ')':
        case '{':
        case '}':
        case '|':
          regex += '\\' + char
          break
        default:
          regex += char
      }
      i++
    }

    regex += '$'
    return new RegExp(regex)
  }

  // ─────────────────────────────────────────────────────────────────
  // Server Commands
  // ─────────────────────────────────────────────────────────────────

  /**
   * PING [message]
   * Returns PONG if no argument is provided, otherwise returns the message
   */
  ping(message?: string): string {
    if (message !== undefined) {
      return message
    }
    return 'PONG'
  }

  /**
   * ECHO message
   * Returns the message
   */
  echo(message: string): string {
    return message
  }

  /**
   * INFO [section]
   * Returns information and statistics about the server
   */
  async info(section?: string): Promise<string> {
    const keyCount = await this.dbsize()

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

  /**
   * TIME
   * Returns the current server time as a two-element array:
   * [unix timestamp in seconds, microseconds]
   */
  time(): [string, string] {
    const now = Date.now()
    const seconds = Math.floor(now / 1000)
    const microseconds = (now % 1000) * 1000
    return [String(seconds), String(microseconds)]
  }

  // ─────────────────────────────────────────────────────────────────
  // HTTP Handler
  // ─────────────────────────────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    if (!this.initialized) {
      return new Response(JSON.stringify({ error: 'Database initializing' }), {
        status: 503,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const url = new URL(request.url)
    const path = url.pathname

    try {
      // Health check
      if (path === '/health') {
        return new Response(JSON.stringify({ status: 'healthy' }), {
          headers: { 'Content-Type': 'application/json' },
        })
      }

      // Stats endpoint
      if (path === '/stats') {
        const keyCount = await this.dbsize()
        return new Response(JSON.stringify({ keys: keyCount }), {
          headers: { 'Content-Type': 'application/json' },
        })
      }

      // Command execution endpoint
      if (path === '/command' && request.method === 'POST') {
        const body = await request.json() as { command: string; args: unknown[] }
        const { command, args } = body

        const result = await this.executeCommand(command, args)
        return new Response(JSON.stringify({ result }), {
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error'
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  /**
   * Execute a Redis command by name
   */
  private async executeCommand(command: string, args: unknown[]): Promise<unknown> {
    const cmd = command.toLowerCase()

    switch (cmd) {
      // String commands
      case 'get':
        return this.get(args[0] as string)
      case 'set':
        return this.set(args[0] as string, args[1] as string, args[2] as SetOptions)
      case 'incr':
        return this.incr(args[0] as string)
      case 'decr':
        return this.decr(args[0] as string)
      case 'incrby':
        return this.incrby(args[0] as string, args[1] as number)
      case 'decrby':
        return this.decrby(args[0] as string, args[1] as number)
      case 'incrbyfloat':
        return this.incrbyfloat(args[0] as string, args[1] as number)
      case 'mget':
        return this.mget(...(args as string[]))
      case 'mset':
        return this.mset(...(args as string[]))
      case 'msetnx':
        return this.msetnx(...(args as string[]))
      case 'append':
        return this.append(args[0] as string, args[1] as string)
      case 'strlen':
        return this.strlen(args[0] as string)
      case 'getrange':
        return this.getrange(args[0] as string, args[1] as number, args[2] as number)
      case 'setrange':
        return this.setrange(args[0] as string, args[1] as number, args[2] as string)
      case 'setnx':
        return this.setnx(args[0] as string, args[1] as string)
      case 'setex':
        return this.setex(args[0] as string, args[1] as number, args[2] as string)
      case 'psetex':
        return this.psetex(args[0] as string, args[1] as number, args[2] as string)
      case 'getset':
        return this.getset(args[0] as string, args[1] as string)
      case 'getdel':
        return this.getdel(args[0] as string)
      case 'getex':
        return this.getex(args[0] as string, args[1] as { ex?: number; px?: number; exat?: number; pxat?: number; persist?: boolean })

      // Hash commands
      case 'hget':
        return this.hget(args[0] as string, args[1] as string)
      case 'hset':
        return this.hset(args[0] as string, ...(args.slice(1) as string[]))
      case 'hsetnx':
        return this.hsetnx(args[0] as string, args[1] as string, args[2] as string)
      case 'hmget':
        return this.hmget(args[0] as string, ...(args.slice(1) as string[]))
      case 'hgetall':
        return this.hgetall(args[0] as string)
      case 'hdel':
        return this.hdel(args[0] as string, ...(args.slice(1) as string[]))
      case 'hexists':
        return this.hexists(args[0] as string, args[1] as string)
      case 'hkeys':
        return this.hkeys(args[0] as string)
      case 'hvals':
        return this.hvals(args[0] as string)
      case 'hlen':
        return this.hlen(args[0] as string)
      case 'hincrby':
        return this.hincrby(args[0] as string, args[1] as string, args[2] as number)
      case 'hincrbyfloat':
        return this.hincrbyfloat(args[0] as string, args[1] as string, args[2] as number)

      // List commands
      case 'lpush':
        return this.lpush(args[0] as string, ...(args.slice(1) as string[]))
      case 'rpush':
        return this.rpush(args[0] as string, ...(args.slice(1) as string[]))
      case 'lpop':
        return this.lpop(args[0] as string, args[1] as number | undefined)
      case 'rpop':
        return this.rpop(args[0] as string, args[1] as number | undefined)
      case 'lrange':
        return this.lrange(args[0] as string, args[1] as number, args[2] as number)
      case 'llen':
        return this.llen(args[0] as string)
      case 'lindex':
        return this.lindex(args[0] as string, args[1] as number)
      case 'lset':
        return this.lset(args[0] as string, args[1] as number, args[2] as string)
      case 'lrem':
        return this.lrem(args[0] as string, args[1] as number, args[2] as string)
      case 'ltrim':
        return this.ltrim(args[0] as string, args[1] as number, args[2] as number)
      case 'linsert':
        return this.linsert(args[0] as string, args[1] as 'BEFORE' | 'AFTER', args[2] as string, args[3] as string)
      case 'lpushx':
        return this.lpushx(args[0] as string, ...(args.slice(1) as string[]))
      case 'rpushx':
        return this.rpushx(args[0] as string, ...(args.slice(1) as string[]))
      case 'lpos':
        return this.lpos(args[0] as string, args[1] as string, args[2] as { rank?: number; count?: number; maxlen?: number } | undefined)
      case 'lmove':
        return this.lmove(args[0] as string, args[1] as string, args[2] as 'LEFT' | 'RIGHT', args[3] as 'LEFT' | 'RIGHT')

      // Set commands
      case 'sadd':
        return this.sadd(args[0] as string, ...(args.slice(1) as string[]))
      case 'srem':
        return this.srem(args[0] as string, ...(args.slice(1) as string[]))
      case 'smembers':
        return this.smembers(args[0] as string)
      case 'sismember':
        return this.sismember(args[0] as string, args[1] as string)
      case 'smismember':
        return this.smismember(args[0] as string, ...(args.slice(1) as string[]))
      case 'scard':
        return this.scard(args[0] as string)
      case 'spop':
        return this.spop(args[0] as string, args[1] as number | undefined)
      case 'srandmember':
        return this.srandmember(args[0] as string, args[1] as number | undefined)
      case 'sdiff':
        return this.sdiff(...(args as string[]))
      case 'sinter':
        return this.sinter(...(args as string[]))
      case 'sunion':
        return this.sunion(...(args as string[]))
      case 'sdiffstore':
        return this.sdiffstore(args[0] as string, ...(args.slice(1) as string[]))
      case 'sinterstore':
        return this.sinterstore(args[0] as string, ...(args.slice(1) as string[]))
      case 'sunionstore':
        return this.sunionstore(args[0] as string, ...(args.slice(1) as string[]))
      case 'smove':
        return this.smove(args[0] as string, args[1] as string, args[2] as string)

      // Sorted set commands
      case 'zadd':
        return this.zadd(args[0] as string, args[1] as ZAddOptions | number, ...(args.slice(2) as (number | string)[]))
      case 'zrem':
        return this.zrem(args[0] as string, ...(args.slice(1) as string[]))
      case 'zscore':
        return this.zscore(args[0] as string, args[1] as string)
      case 'zrank':
        return this.zrank(args[0] as string, args[1] as string)
      case 'zrevrank':
        return this.zrevrank(args[0] as string, args[1] as string)
      case 'zrange':
        return this.zrange(args[0] as string, args[1] as number, args[2] as number, args[3] as ZRangeOptions)
      case 'zrevrange':
        return this.zrevrange(args[0] as string, args[1] as number, args[2] as number, args[3] as { withScores?: boolean })
      case 'zrangebyscore':
        return this.zrangebyscore(args[0] as string, args[1] as number | string, args[2] as number | string, args[3] as { withScores?: boolean; limit?: { offset: number; count: number } })
      case 'zcard':
        return this.zcard(args[0] as string)
      case 'zcount':
        return this.zcount(args[0] as string, args[1] as number | string, args[2] as number | string)
      case 'zincrby':
        return this.zincrby(args[0] as string, args[1] as number, args[2] as string)
      case 'zpopmin':
        return this.zpopmin(args[0] as string, args[1] as number | undefined)
      case 'zpopmax':
        return this.zpopmax(args[0] as string, args[1] as number | undefined)

      // Key commands
      case 'del':
        return this.del(...(args as string[]))
      case 'unlink':
        return this.unlink(...(args as string[]))
      case 'exists':
        return this.exists(...(args as string[]))
      case 'expire':
        return this.expire(args[0] as string, args[1] as number)
      case 'pexpire':
        return this.pexpire(args[0] as string, args[1] as number)
      case 'expireat':
        return this.expireat(args[0] as string, args[1] as number)
      case 'pexpireat':
        return this.pexpireat(args[0] as string, args[1] as number)
      case 'ttl':
        return this.ttl(args[0] as string)
      case 'pttl':
        return this.pttl(args[0] as string)
      case 'persist':
        return this.persist(args[0] as string)
      case 'type':
        return this.type(args[0] as string)
      case 'rename':
        return this.rename(args[0] as string, args[1] as string)
      case 'renamenx':
        return this.renamenx(args[0] as string, args[1] as string)
      case 'keys':
        return this.keys(args[0] as string)
      case 'scan':
        return this.scan(args[0] as number, args[1] as ScanOptions)
      case 'dbsize':
        return this.dbsize()
      case 'flushdb':
        return this.flushdb()
      case 'randomkey':
        return this.randomkey()

      // Server commands
      case 'ping':
        return this.ping(args[0] as string | undefined)
      case 'echo':
        return this.echo(args[0] as string)
      case 'info':
        return this.info(args[0] as string | undefined)
      case 'time':
        return this.time()

      default:
        throw new Error(`ERR unknown command '${command}'`)
    }
  }
}
