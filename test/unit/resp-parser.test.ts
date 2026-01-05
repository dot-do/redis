/**
 * RESP Protocol Parser Tests
 *
 * Tests for Redis Serialization Protocol (RESP) parsing
 * Covers RESP2 and RESP3 data types
 */

import { describe, it, expect, beforeEach } from 'vitest'

// ─────────────────────────────────────────────────────────────────
// RESP Parser Implementation (for testing)
// ─────────────────────────────────────────────────────────────────

// RESP type prefixes
const RESP_SIMPLE_STRING = '+'
const RESP_ERROR = '-'
const RESP_INTEGER = ':'
const RESP_BULK_STRING = '$'
const RESP_ARRAY = '*'
// RESP3 types
const RESP_NULL = '_'
const RESP_BOOLEAN = '#'
const RESP_DOUBLE = ','
const RESP_MAP = '%'
const RESP_SET = '~'
const RESP_VERBATIM_STRING = '='
const RESP_BIG_NUMBER = '('
const RESP_PUSH = '>'

export type RespValue =
  | string
  | number
  | null
  | boolean
  | Error
  | RespValue[]
  | Map<RespValue, RespValue>
  | Set<RespValue>

export class RespParser {
  private buffer: string = ''
  private position: number = 0

  /**
   * Parse a RESP message
   */
  parse(data: string): RespValue {
    this.buffer = data
    this.position = 0
    return this.parseValue()
  }

  /**
   * Encode a value to RESP format
   */
  encode(value: RespValue): string {
    if (value === null) {
      return '_\r\n'
    }
    if (typeof value === 'boolean') {
      return `#${value ? 't' : 'f'}\r\n`
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return `:${value}\r\n`
      }
      return `,${value}\r\n`
    }
    if (typeof value === 'string') {
      // Use bulk string for safety (handles any characters)
      return `$${value.length}\r\n${value}\r\n`
    }
    if (value instanceof Error) {
      return `-${value.message}\r\n`
    }
    if (Array.isArray(value)) {
      const elements = value.map((v) => this.encode(v)).join('')
      return `*${value.length}\r\n${elements}`
    }
    if (value instanceof Map) {
      const entries: string[] = []
      for (const [k, v] of value) {
        entries.push(this.encode(k))
        entries.push(this.encode(v))
      }
      return `%${value.size}\r\n${entries.join('')}`
    }
    if (value instanceof Set) {
      const elements = Array.from(value).map((v) => this.encode(v)).join('')
      return `~${value.size}\r\n${elements}`
    }
    throw new Error(`Cannot encode value: ${value}`)
  }

  private parseValue(): RespValue {
    const type = this.readChar()

    switch (type) {
      case RESP_SIMPLE_STRING:
        return this.parseSimpleString()
      case RESP_ERROR:
        return this.parseError()
      case RESP_INTEGER:
        return this.parseInteger()
      case RESP_BULK_STRING:
        return this.parseBulkString()
      case RESP_ARRAY:
        return this.parseArray()
      case RESP_NULL:
        return this.parseNull()
      case RESP_BOOLEAN:
        return this.parseBoolean()
      case RESP_DOUBLE:
        return this.parseDouble()
      case RESP_MAP:
        return this.parseMap()
      case RESP_SET:
        return this.parseSet()
      case RESP_VERBATIM_STRING:
        return this.parseVerbatimString()
      case RESP_BIG_NUMBER:
        return this.parseBigNumber()
      case RESP_PUSH:
        return this.parsePush()
      default:
        throw new Error(`Unknown RESP type: ${type}`)
    }
  }

  private parseSimpleString(): string {
    return this.readLine()
  }

  private parseError(): Error {
    const message = this.readLine()
    return new Error(message)
  }

  private parseInteger(): number {
    const line = this.readLine()
    return parseInt(line, 10)
  }

  private parseBulkString(): string | null {
    const length = parseInt(this.readLine(), 10)
    if (length === -1) {
      return null
    }
    const str = this.buffer.slice(this.position, this.position + length)
    this.position += length
    this.readLine() // consume trailing \r\n
    return str
  }

  private parseArray(): RespValue[] | null {
    const length = parseInt(this.readLine(), 10)
    if (length === -1) {
      return null
    }
    const arr: RespValue[] = []
    for (let i = 0; i < length; i++) {
      arr.push(this.parseValue())
    }
    return arr
  }

  private parseNull(): null {
    this.readLine() // consume trailing \r\n
    return null
  }

  private parseBoolean(): boolean {
    const char = this.readChar()
    this.readLine() // consume trailing \r\n
    return char === 't'
  }

  private parseDouble(): number {
    const line = this.readLine()
    if (line === 'inf') return Infinity
    if (line === '-inf') return -Infinity
    if (line === 'nan') return NaN
    return parseFloat(line)
  }

  private parseMap(): Map<RespValue, RespValue> {
    const length = parseInt(this.readLine(), 10)
    const map = new Map<RespValue, RespValue>()
    for (let i = 0; i < length; i++) {
      const key = this.parseValue()
      const value = this.parseValue()
      map.set(key, value)
    }
    return map
  }

  private parseSet(): Set<RespValue> {
    const length = parseInt(this.readLine(), 10)
    const set = new Set<RespValue>()
    for (let i = 0; i < length; i++) {
      set.add(this.parseValue())
    }
    return set
  }

  private parseVerbatimString(): string {
    const length = parseInt(this.readLine(), 10)
    // Format: encoding:content (e.g., "txt:Hello")
    const content = this.buffer.slice(this.position, this.position + length)
    this.position += length
    this.readLine() // consume trailing \r\n
    // Skip the encoding prefix (first 4 chars including colon)
    return content.slice(4)
  }

  private parseBigNumber(): bigint {
    const line = this.readLine()
    return BigInt(line)
  }

  private parsePush(): RespValue[] {
    // Push is like an array but for pub/sub
    const length = parseInt(this.readLine(), 10)
    const arr: RespValue[] = []
    for (let i = 0; i < length; i++) {
      arr.push(this.parseValue())
    }
    return arr
  }

  private readChar(): string {
    return this.buffer[this.position++]
  }

  private readLine(): string {
    const start = this.position
    while (this.position < this.buffer.length) {
      if (this.buffer[this.position] === '\r' && this.buffer[this.position + 1] === '\n') {
        const line = this.buffer.slice(start, this.position)
        this.position += 2 // Skip \r\n
        return line
      }
      this.position++
    }
    return this.buffer.slice(start)
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('RespParser', () => {
  let parser: RespParser

  beforeEach(() => {
    parser = new RespParser()
  })

  // ─────────────────────────────────────────────────────────────────
  // RESP2 Types
  // ─────────────────────────────────────────────────────────────────

  describe('RESP2 Types', () => {
    describe('Simple String', () => {
      it('should parse simple string', () => {
        const result = parser.parse('+OK\r\n')
        expect(result).toBe('OK')
      })

      it('should parse empty simple string', () => {
        const result = parser.parse('+\r\n')
        expect(result).toBe('')
      })

      it('should parse simple string with spaces', () => {
        const result = parser.parse('+Hello World\r\n')
        expect(result).toBe('Hello World')
      })

      it('should parse PONG response', () => {
        const result = parser.parse('+PONG\r\n')
        expect(result).toBe('PONG')
      })
    })

    describe('Error', () => {
      it('should parse error', () => {
        const result = parser.parse('-ERR unknown command\r\n')
        expect(result).toBeInstanceOf(Error)
        expect((result as Error).message).toBe('ERR unknown command')
      })

      it('should parse WRONGTYPE error', () => {
        const result = parser.parse('-WRONGTYPE Operation against a key holding the wrong kind of value\r\n')
        expect(result).toBeInstanceOf(Error)
        expect((result as Error).message).toContain('WRONGTYPE')
      })

      it('should parse empty error', () => {
        const result = parser.parse('-\r\n')
        expect(result).toBeInstanceOf(Error)
        expect((result as Error).message).toBe('')
      })
    })

    describe('Integer', () => {
      it('should parse positive integer', () => {
        const result = parser.parse(':42\r\n')
        expect(result).toBe(42)
      })

      it('should parse zero', () => {
        const result = parser.parse(':0\r\n')
        expect(result).toBe(0)
      })

      it('should parse negative integer', () => {
        const result = parser.parse(':-1\r\n')
        expect(result).toBe(-1)
      })

      it('should parse large integer', () => {
        const result = parser.parse(':9999999999\r\n')
        expect(result).toBe(9999999999)
      })
    })

    describe('Bulk String', () => {
      it('should parse bulk string', () => {
        const result = parser.parse('$5\r\nhello\r\n')
        expect(result).toBe('hello')
      })

      it('should parse empty bulk string', () => {
        const result = parser.parse('$0\r\n\r\n')
        expect(result).toBe('')
      })

      it('should parse null bulk string', () => {
        const result = parser.parse('$-1\r\n')
        expect(result).toBeNull()
      })

      it('should parse bulk string with newlines', () => {
        // "hello\r\nworld" is 12 characters: h-e-l-l-o-\r-\n-w-o-r-l-d
        const result = parser.parse('$12\r\nhello\r\nworld\r\n')
        expect(result).toBe('hello\r\nworld')
      })

      it('should parse bulk string with special characters', () => {
        const result = parser.parse('$12\r\nhello\tworld!\r\n')
        expect(result).toBe('hello\tworld!')
      })

      it('should parse binary-safe bulk string', () => {
        const result = parser.parse('$5\r\n\x00\x01\x02\x03\x04\r\n')
        expect(result).toBe('\x00\x01\x02\x03\x04')
      })
    })

    describe('Array', () => {
      it('should parse empty array', () => {
        const result = parser.parse('*0\r\n')
        expect(result).toEqual([])
      })

      it('should parse array of bulk strings', () => {
        const result = parser.parse('*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
        expect(result).toEqual(['foo', 'bar'])
      })

      it('should parse array of integers', () => {
        const result = parser.parse('*3\r\n:1\r\n:2\r\n:3\r\n')
        expect(result).toEqual([1, 2, 3])
      })

      it('should parse mixed type array', () => {
        const result = parser.parse('*4\r\n+OK\r\n:42\r\n$5\r\nhello\r\n$-1\r\n')
        expect(result).toEqual(['OK', 42, 'hello', null])
      })

      it('should parse null array', () => {
        const result = parser.parse('*-1\r\n')
        expect(result).toBeNull()
      })

      it('should parse array with errors', () => {
        const result = parser.parse('*2\r\n+OK\r\n-ERR error\r\n') as RespValue[]
        expect(result).toHaveLength(2)
        expect(result[0]).toBe('OK')
        expect(result[1]).toBeInstanceOf(Error)
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // RESP3 Types
  // ─────────────────────────────────────────────────────────────────

  describe('RESP3 Types', () => {
    describe('Null', () => {
      it('should parse null', () => {
        const result = parser.parse('_\r\n')
        expect(result).toBeNull()
      })
    })

    describe('Boolean', () => {
      it('should parse true', () => {
        const result = parser.parse('#t\r\n')
        expect(result).toBe(true)
      })

      it('should parse false', () => {
        const result = parser.parse('#f\r\n')
        expect(result).toBe(false)
      })
    })

    describe('Double', () => {
      it('should parse positive double', () => {
        const result = parser.parse(',3.14159\r\n')
        expect(result).toBeCloseTo(3.14159)
      })

      it('should parse negative double', () => {
        const result = parser.parse(',-2.5\r\n')
        expect(result).toBeCloseTo(-2.5)
      })

      it('should parse zero', () => {
        const result = parser.parse(',0\r\n')
        expect(result).toBe(0)
      })

      it('should parse infinity', () => {
        const result = parser.parse(',inf\r\n')
        expect(result).toBe(Infinity)
      })

      it('should parse negative infinity', () => {
        const result = parser.parse(',-inf\r\n')
        expect(result).toBe(-Infinity)
      })

      it('should parse NaN', () => {
        const result = parser.parse(',nan\r\n')
        expect(result).toBeNaN()
      })

      it('should parse scientific notation', () => {
        const result = parser.parse(',1.5e10\r\n')
        expect(result).toBe(1.5e10)
      })
    })

    describe('Map', () => {
      it('should parse empty map', () => {
        const result = parser.parse('%0\r\n')
        expect(result).toBeInstanceOf(Map)
        expect((result as Map<RespValue, RespValue>).size).toBe(0)
      })

      it('should parse map with string keys', () => {
        const result = parser.parse('%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n')
        expect(result).toBeInstanceOf(Map)
        const map = result as Map<RespValue, RespValue>
        expect(map.get('foo')).toBe(1)
        expect(map.get('bar')).toBe(2)
      })

      it('should parse map with mixed value types', () => {
        const result = parser.parse('%2\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n:30\r\n')
        expect(result).toBeInstanceOf(Map)
        const map = result as Map<RespValue, RespValue>
        expect(map.get('name')).toBe('Alice')
        expect(map.get('age')).toBe(30)
      })
    })

    describe('Set', () => {
      it('should parse empty set', () => {
        const result = parser.parse('~0\r\n')
        expect(result).toBeInstanceOf(Set)
        expect((result as Set<RespValue>).size).toBe(0)
      })

      it('should parse set of strings', () => {
        const result = parser.parse('~3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n')
        expect(result).toBeInstanceOf(Set)
        const set = result as Set<RespValue>
        expect(set.has('one')).toBe(true)
        expect(set.has('two')).toBe(true)
        expect(set.has('three')).toBe(true)
      })

      it('should parse set of integers', () => {
        const result = parser.parse('~3\r\n:1\r\n:2\r\n:3\r\n')
        expect(result).toBeInstanceOf(Set)
        const set = result as Set<RespValue>
        expect(set.has(1)).toBe(true)
        expect(set.has(2)).toBe(true)
        expect(set.has(3)).toBe(true)
      })
    })

    describe('Verbatim String', () => {
      it('should parse verbatim string', () => {
        const result = parser.parse('=10\r\ntxt:Hello!\r\n')
        expect(result).toBe('Hello!')
      })

      it('should parse markdown verbatim string', () => {
        const result = parser.parse('=12\r\nmkd:# Header\r\n')
        expect(result).toBe('# Header')
      })
    })

    describe('Big Number', () => {
      it('should parse big number', () => {
        const result = parser.parse('(12345678901234567890\r\n')
        expect(result).toBe(BigInt('12345678901234567890'))
      })

      it('should parse negative big number', () => {
        const result = parser.parse('(-12345678901234567890\r\n')
        expect(result).toBe(BigInt('-12345678901234567890'))
      })
    })

    describe('Push', () => {
      it('should parse push message', () => {
        const result = parser.parse('>3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$4\r\ndata\r\n')
        expect(result).toEqual(['message', 'channel', 'data'])
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Edge Cases
  // ─────────────────────────────────────────────────────────────────

  describe('Edge Cases', () => {
    describe('Empty Values', () => {
      it('should handle empty simple string', () => {
        const result = parser.parse('+\r\n')
        expect(result).toBe('')
      })

      it('should handle empty bulk string', () => {
        const result = parser.parse('$0\r\n\r\n')
        expect(result).toBe('')
      })

      it('should handle empty array', () => {
        const result = parser.parse('*0\r\n')
        expect(result).toEqual([])
      })

      it('should handle empty map', () => {
        const result = parser.parse('%0\r\n')
        expect(result).toBeInstanceOf(Map)
        expect((result as Map<RespValue, RespValue>).size).toBe(0)
      })

      it('should handle empty set', () => {
        const result = parser.parse('~0\r\n')
        expect(result).toBeInstanceOf(Set)
        expect((result as Set<RespValue>).size).toBe(0)
      })
    })

    describe('Nested Arrays', () => {
      it('should parse nested array', () => {
        const result = parser.parse('*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n')
        expect(result).toEqual([[1, 2], [3, 4]])
      })

      it('should parse deeply nested array', () => {
        const result = parser.parse('*1\r\n*1\r\n*1\r\n:42\r\n')
        expect(result).toEqual([[[42]]])
      })

      it('should parse array with null elements', () => {
        const result = parser.parse('*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n')
        expect(result).toEqual(['foo', null, 'bar'])
      })

      it('should parse array with nested null array', () => {
        const result = parser.parse('*2\r\n*-1\r\n*2\r\n:1\r\n:2\r\n')
        expect(result).toEqual([null, [1, 2]])
      })
    })

    describe('Complex Nested Structures', () => {
      it('should parse array containing map', () => {
        const result = parser.parse('*2\r\n%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n:42\r\n')
        const arr = result as RespValue[]
        expect(arr).toHaveLength(2)
        expect(arr[0]).toBeInstanceOf(Map)
        expect((arr[0] as Map<RespValue, RespValue>).get('key')).toBe('value')
        expect(arr[1]).toBe(42)
      })

      it('should parse map containing array', () => {
        const result = parser.parse('%1\r\n$4\r\nlist\r\n*3\r\n:1\r\n:2\r\n:3\r\n')
        const map = result as Map<RespValue, RespValue>
        expect(map.get('list')).toEqual([1, 2, 3])
      })

      it('should parse set containing various types', () => {
        const result = parser.parse('~3\r\n$3\r\nfoo\r\n:42\r\n#t\r\n')
        const set = result as Set<RespValue>
        expect(set.has('foo')).toBe(true)
        expect(set.has(42)).toBe(true)
        expect(set.has(true)).toBe(true)
      })
    })

    describe('Unicode', () => {
      it('should handle unicode in bulk string', () => {
        const str = 'Hello, World!'
        const encoded = Buffer.from(str, 'utf-8')
        const result = parser.parse(`$${encoded.length}\r\n${str}\r\n`)
        expect(result).toBe(str)
      })

      it('should handle emoji in bulk string', () => {
        const str = 'Hello World'
        const result = parser.parse(`$${str.length}\r\n${str}\r\n`)
        expect(result).toBe(str)
      })
    })

    describe('Error Handling', () => {
      it('should throw on unknown type', () => {
        expect(() => parser.parse('X\r\n')).toThrow('Unknown RESP type: X')
      })
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Encoding
  // ─────────────────────────────────────────────────────────────────

  describe('Encoding', () => {
    it('should encode null', () => {
      const encoded = parser.encode(null)
      expect(encoded).toBe('_\r\n')
    })

    it('should encode boolean true', () => {
      const encoded = parser.encode(true)
      expect(encoded).toBe('#t\r\n')
    })

    it('should encode boolean false', () => {
      const encoded = parser.encode(false)
      expect(encoded).toBe('#f\r\n')
    })

    it('should encode integer', () => {
      const encoded = parser.encode(42)
      expect(encoded).toBe(':42\r\n')
    })

    it('should encode double', () => {
      const encoded = parser.encode(3.14)
      expect(encoded).toBe(',3.14\r\n')
    })

    it('should encode string', () => {
      const encoded = parser.encode('hello')
      expect(encoded).toBe('$5\r\nhello\r\n')
    })

    it('should encode error', () => {
      const encoded = parser.encode(new Error('ERR test'))
      expect(encoded).toBe('-ERR test\r\n')
    })

    it('should encode array', () => {
      const encoded = parser.encode(['foo', 42])
      expect(encoded).toBe('*2\r\n$3\r\nfoo\r\n:42\r\n')
    })

    it('should encode map', () => {
      const map = new Map<RespValue, RespValue>([['key', 'value']])
      const encoded = parser.encode(map)
      expect(encoded).toBe('%1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
    })

    it('should encode set', () => {
      const set = new Set<RespValue>([1, 2])
      const encoded = parser.encode(set)
      expect(encoded).toBe('~2\r\n:1\r\n:2\r\n')
    })

    it('should round-trip encode/decode', () => {
      const values: RespValue[] = [
        null,
        true,
        false,
        42,
        'hello',
        ['foo', 'bar'],
        [1, 2, 3],
      ]

      for (const value of values) {
        const encoded = parser.encode(value)
        const decoded = parser.parse(encoded)
        expect(decoded).toEqual(value)
      }
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Command Encoding
  // ─────────────────────────────────────────────────────────────────

  describe('Command Encoding', () => {
    it('should encode SET command', () => {
      const command = ['SET', 'mykey', 'myvalue']
      const encoded = parser.encode(command)
      expect(encoded).toBe('*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n')
    })

    it('should encode GET command', () => {
      const command = ['GET', 'mykey']
      const encoded = parser.encode(command)
      expect(encoded).toBe('*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n')
    })

    it('should encode MSET command', () => {
      const command = ['MSET', 'key1', 'val1', 'key2', 'val2']
      const encoded = parser.encode(command)
      expect(encoded).toBe('*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n')
    })

    it('should encode ZADD command with scores', () => {
      const command = ['ZADD', 'myset', '1', 'one', '2', 'two']
      const encoded = parser.encode(command)
      expect(encoded).toBe('*6\r\n$4\r\nZADD\r\n$5\r\nmyset\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n')
    })
  })
})
