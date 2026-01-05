/**
 * Upstash REST API Tests
 *
 * Tests for the Upstash-compatible REST API handler
 * Covers single commands, pipelines, authentication, and error handling
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { handleUpstashRest, type UpstashContext } from '../../src/rest/upstash'
import type { Env } from '../../src/types'

// ─────────────────────────────────────────────────────────────────
// Mock Environment and Command Executor
// ─────────────────────────────────────────────────────────────────

function createMockEnv(authToken?: string): Env {
  return {
    REDIS_SHARDS: {} as any,
    REDIS_PUBSUB: {} as any,
    REDIS_COORDINATOR: {} as any,
    AUTH_TOKEN: authToken,
  }
}

interface MockStore {
  [key: string]: string
}

function createMockExecutor(store: MockStore = {}) {
  return async (cmd: string, args: string[]): Promise<unknown> => {
    switch (cmd) {
      case 'GET':
        return store[args[0]] ?? null
      case 'SET':
        store[args[0]] = args[1]
        return 'OK'
      case 'DEL':
        const deleted = args.filter(key => key in store).length
        args.forEach(key => delete store[key])
        return deleted
      case 'INCR':
        const current = parseInt(store[args[0]] ?? '0', 10)
        store[args[0]] = String(current + 1)
        return current + 1
      case 'MGET':
        return args.map(key => store[key] ?? null)
      case 'MSET':
        for (let i = 0; i < args.length; i += 2) {
          store[args[i]] = args[i + 1]
        }
        return 'OK'
      case 'PING':
        return args[0] || 'PONG'
      case 'ECHO':
        return args[0]
      case 'EXISTS':
        return args.filter(key => key in store).length
      case 'LPUSH':
        // Simple mock - just store as joined string
        const key = args[0]
        const values = args.slice(1)
        store[key] = values.join(',')
        return values.length
      case 'LRANGE':
        const listKey = args[0]
        if (!store[listKey]) return []
        return store[listKey].split(',')
      case 'HSET':
        const hashKey = args[0]
        store[hashKey] = JSON.stringify({ [args[1]]: args[2] })
        return 1
      case 'HGET':
        const hkey = args[0]
        const field = args[1]
        if (!store[hkey]) return null
        const hash = JSON.parse(store[hkey])
        return hash[field] ?? null
      case 'INVALID':
        throw new Error('ERR unknown command')
      default:
        throw new Error(`ERR unknown command '${cmd}'`)
    }
  }
}

function createContext(store: MockStore = {}, authToken?: string): UpstashContext {
  return {
    env: createMockEnv(authToken),
    executeCommand: createMockExecutor(store),
  }
}

// ─────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────

async function postJson(
  url: string,
  body: unknown,
  headers: Record<string, string> = {}
): Promise<Response> {
  const request = new Request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    body: JSON.stringify(body),
  })
  const parsedUrl = new URL(url)
  return handleUpstashRest(request, parsedUrl, createContext({}, headers['Authorization']?.slice(7)))
}

async function postJsonWithContext(
  url: string,
  body: unknown,
  context: UpstashContext,
  headers: Record<string, string> = {}
): Promise<Response> {
  const request = new Request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    body: JSON.stringify(body),
  })
  const parsedUrl = new URL(url)
  return handleUpstashRest(request, parsedUrl, context)
}

async function getRequest(
  url: string,
  headers: Record<string, string> = {},
  context?: UpstashContext
): Promise<Response> {
  const request = new Request(url, {
    method: 'GET',
    headers,
  })
  const parsedUrl = new URL(url)
  return handleUpstashRest(request, parsedUrl, context ?? createContext())
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('Upstash REST API', () => {
  // ─────────────────────────────────────────────────────────────────
  // Single Command Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Single Commands', () => {
    describe('POST / with JSON body ["COMMAND", "args"]', () => {
      it('should execute GET command', async () => {
        const store: MockStore = { mykey: 'myvalue' }
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['GET', 'mykey']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'myvalue' })
      })

      it('should execute SET command', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['SET', 'mykey', 'myvalue']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
        expect(store['mykey']).toBe('myvalue')
      })

      it('should execute SET with EX option', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['SET', 'mykey', 'myvalue', 'EX', '100']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
      })

      it('should execute INCR command', async () => {
        const store: MockStore = { counter: '10' }
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['INCR', 'counter']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 11 })
      })

      it('should execute MGET command', async () => {
        const store: MockStore = { key1: 'value1', key2: 'value2' }
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['MGET', 'key1', 'key2', 'key3']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: ['value1', 'value2', null] })
      })

      it('should execute PING command', async () => {
        const context = createContext()

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['PING']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'PONG' })
      })

      it('should execute PING with custom message', async () => {
        const context = createContext()

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['PING', 'hello']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'hello' })
      })

      it('should return null for non-existent key', async () => {
        const context = createContext({})

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['GET', 'nonexistent']),
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: null })
      })

      it('should handle command case insensitivity', async () => {
        const store: MockStore = { mykey: 'myvalue' }
        const context = createContext(store)

        const request = new Request('https://example.com/', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(['get', 'mykey']),  // lowercase
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'myvalue' })
      })
    })

    describe('GET /COMMAND/arg1/arg2 (URL-based)', () => {
      it('should execute GET command via URL', async () => {
        const store: MockStore = { mykey: 'myvalue' }
        const context = createContext(store)

        const request = new Request('https://example.com/GET/mykey', {
          method: 'GET',
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'myvalue' })
      })

      it('should execute SET command via URL', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        const request = new Request('https://example.com/SET/mykey/myvalue', {
          method: 'GET',
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
        expect(store['mykey']).toBe('myvalue')
      })

      it('should handle URL-encoded arguments', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        const request = new Request('https://example.com/SET/my%20key/my%20value', {
          method: 'GET',
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
        expect(store['my key']).toBe('my value')
      })

      it('should handle query parameters as additional args', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        // SET foo bar EX 100
        const request = new Request('https://example.com/SET/foo/bar?EX=100', {
          method: 'GET',
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
      })
    })

    describe('POST /COMMAND/arg with value in body', () => {
      it('should use body as value for SET', async () => {
        const store: MockStore = {}
        const context = createContext(store)

        const request = new Request('https://example.com/SET/mykey', {
          method: 'POST',
          body: 'my value from body',
        })

        const response = await handleUpstashRest(request, new URL(request.url), context)
        expect(response.status).toBe(200)

        const json = await response.json()
        expect(json).toEqual({ result: 'OK' })
        expect(store['mykey']).toBe('my value from body')
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Pipeline Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Pipeline Commands', () => {
    it('should execute pipeline with multiple commands', async () => {
      const store: MockStore = {}
      const context = createContext(store)

      const request = new Request('https://example.com/pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([
          ['SET', 'key1', 'value1'],
          ['SET', 'key2', 'value2'],
          ['GET', 'key1'],
          ['GET', 'key2'],
        ]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)

      const json = await response.json()
      expect(json).toEqual({
        results: [
          { result: 'OK' },
          { result: 'OK' },
          { result: 'value1' },
          { result: 'value2' },
        ],
      })
    })

    it('should return empty results for empty pipeline', async () => {
      const context = createContext()

      const request = new Request('https://example.com/pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)

      const json = await response.json()
      expect(json).toEqual({ results: [] })
    })

    it('should handle errors in pipeline gracefully', async () => {
      const store: MockStore = { key1: 'value1' }
      const context = createContext(store)

      const request = new Request('https://example.com/pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([
          ['GET', 'key1'],
          ['INVALID'],
          ['GET', 'key1'],
        ]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)

      const json = await response.json()
      expect(json.results).toHaveLength(3)
      expect(json.results[0]).toEqual({ result: 'value1' })
      expect(json.results[1].result).toContain('ERR')
      expect(json.results[2]).toEqual({ result: 'value1' })
    })

    it('should execute mixed read/write pipeline', async () => {
      const store: MockStore = { counter: '0' }
      const context = createContext(store)

      const request = new Request('https://example.com/pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([
          ['INCR', 'counter'],
          ['INCR', 'counter'],
          ['INCR', 'counter'],
          ['GET', 'counter'],
        ]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)

      const json = await response.json()
      expect(json).toEqual({
        results: [
          { result: 1 },
          { result: 2 },
          { result: 3 },
          { result: '3' },
        ],
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Authentication Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Authentication', () => {
    it('should allow requests without auth when no token configured', async () => {
      const store: MockStore = { mykey: 'myvalue' }
      const context = createContext(store)  // No auth token

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)
    })

    it('should reject requests without auth when token configured', async () => {
      const store: MockStore = {}
      const context: UpstashContext = {
        env: createMockEnv('secret-token'),
        executeCommand: createMockExecutor(store),
      }

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(401)

      const json = await response.json()
      expect(json).toEqual({ error: 'Unauthorized' })
    })

    it('should accept valid Bearer token', async () => {
      const store: MockStore = { mykey: 'myvalue' }
      const context: UpstashContext = {
        env: createMockEnv('secret-token'),
        executeCommand: createMockExecutor(store),
      }

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer secret-token',
        },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)

      const json = await response.json()
      expect(json).toEqual({ result: 'myvalue' })
    })

    it('should accept token without Bearer prefix', async () => {
      const store: MockStore = { mykey: 'myvalue' }
      const context: UpstashContext = {
        env: createMockEnv('secret-token'),
        executeCommand: createMockExecutor(store),
      }

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'secret-token',
        },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)
    })

    it('should reject invalid token', async () => {
      const store: MockStore = {}
      const context: UpstashContext = {
        env: createMockEnv('secret-token'),
        executeCommand: createMockExecutor(store),
      }

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer wrong-token',
        },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(401)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Error Handling Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Error Handling', () => {
    it('should return 405 for unsupported methods', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'DELETE',
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(405)

      const json = await response.json()
      expect(json).toEqual({ error: 'Method not allowed' })
    })

    it('should return 400 for invalid JSON', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not valid json',
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(400)

      const json = await response.json()
      expect(json.error).toBeDefined()
    })

    it('should return 400 for empty command', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(400)

      const json = await response.json()
      expect(json).toEqual({ error: 'Empty command' })
    })

    it('should return 400 for non-array body', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command: 'GET', key: 'foo' }),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(400)

      const json = await response.json()
      expect(json.error).toContain('array')
    })

    it('should return 400 for unknown command', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['UNKNOWNCOMMAND', 'arg1']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(400)

      const json = await response.json()
      expect(json.error).toContain('unknown command')
    })

    it('should return 400 for pipeline with non-array command', async () => {
      const context = createContext()

      const request = new Request('https://example.com/pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify([
          ['GET', 'key1'],
          'not-an-array',
        ]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(400)

      const json = await response.json()
      expect(json.error).toContain('array')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // CORS Tests
  // ─────────────────────────────────────────────────────────────────

  describe('CORS', () => {
    it('should handle OPTIONS preflight request', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'OPTIONS',
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(204)
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
      expect(response.headers.get('Access-Control-Allow-Methods')).toContain('POST')
    })

    it('should include CORS headers in success response', async () => {
      const store: MockStore = { mykey: 'myvalue' }
      const context = createContext(store)

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['GET', 'mykey']),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })

    it('should include CORS headers in error response', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'DELETE',
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // HEAD Request Tests
  // ─────────────────────────────────────────────────────────────────

  describe('HEAD Requests', () => {
    it('should return 200 for HEAD request (health check)', async () => {
      const context = createContext()

      const request = new Request('https://example.com/', {
        method: 'HEAD',
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Data Type Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Data Types', () => {
    it('should handle numeric arguments', async () => {
      const store: MockStore = {}
      const context = createContext(store)

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['SET', 'count', 42]),  // number
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)
      expect(store['count']).toBe('42')  // Converted to string
    })

    it('should handle boolean arguments', async () => {
      const store: MockStore = {}
      const context = createContext(store)

      const request = new Request('https://example.com/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(['SET', 'flag', true]),
      })

      const response = await handleUpstashRest(request, new URL(request.url), context)
      expect(response.status).toBe(200)
      expect(store['flag']).toBe('true')
    })
  })
})
