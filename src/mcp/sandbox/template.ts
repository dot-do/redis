/**
 * Sandbox Template Generator
 *
 * Generates sandboxed code wrappers for safe user code execution
 * with Redis API access
 */

// ─────────────────────────────────────────────────────────────────
// Dangerous Patterns for Validation
// ─────────────────────────────────────────────────────────────────

const DANGEROUS_PATTERNS = [
  // Network access
  /\bfetch\s*\(/gi,
  /\bXMLHttpRequest\b/gi,
  /\bWebSocket\b/gi,
  /\bEventSource\b/gi,
  /\bnavigator\b/gi,

  // Global object manipulation
  /\bglobalThis\b/gi,
  /\bwindow\b/gi,
  /\bself\b/gi,
  /\bparent\b/gi,
  /\btop\b/gi,
  /\bframes\b/gi,

  // Code execution
  /\beval\s*\(/gi,
  /\bFunction\s*\(/gi,
  /new\s+Function\b/gi,
  /\bimport\s*\(/gi,
  /\brequire\s*\(/gi,

  // Timer abuse (allow but with limits)
  /\bsetInterval\s*\(/gi,

  // Prototype pollution
  /__proto__/gi,
  /\bconstructor\s*\[/gi,
  /Object\s*\.\s*prototype/gi,
  /Array\s*\.\s*prototype/gi,

  // Process/environment access
  /\bprocess\b/gi,
  /\bDeno\b/gi,
  /\bBun\b/gi,

  // File system
  /\brequire\s*\(\s*['"]fs['"]\s*\)/gi,
  /\brequire\s*\(\s*['"]path['"]\s*\)/gi,
  /\brequire\s*\(\s*['"]child_process['"]\s*\)/gi,

  // Worker creation (could escape sandbox)
  /new\s+Worker\b/gi,
  /new\s+SharedWorker\b/gi,
  /\bServiceWorker\b/gi,

  // Dangerous object access
  /\bReflect\s*\./gi,
  /\bProxy\s*\(/gi,

  // Bypass techniques
  /\(\s*\d+\s*,\s*eval\s*\)/gi,     // (0,eval) or (1,eval) indirect eval
  /\(\s*\d+\s*,\s*Function\s*\)/gi, // indirect Function
  /\\u0065val/gi,                    // unicode eval
  /\\u0046unction/gi,                // unicode Function
]

const DANGEROUS_KEYWORDS = [
  'eval',
  'Function',
  '__proto__',
  'constructor',
  'prototype',
  'globalThis',
  'window',
  'self',
  'process',
  'require',
  'import',
  'module',
  'exports',
]

// ─────────────────────────────────────────────────────────────────
// Validation Result
// ─────────────────────────────────────────────────────────────────

export interface ValidationResult {
  valid: boolean
  errors: string[]
  warnings: string[]
}

// ─────────────────────────────────────────────────────────────────
// Code Validation
// ─────────────────────────────────────────────────────────────────

/**
 * Validate user code for dangerous patterns
 */
export function validateUserCode(code: string): ValidationResult {
  const errors: string[] = []
  const warnings: string[] = []

  // Check for dangerous patterns
  for (const pattern of DANGEROUS_PATTERNS) {
    const matches = code.match(pattern)
    if (matches) {
      errors.push(`Forbidden pattern detected: ${matches[0]}`)
    }
  }

  // Check for attempts to access forbidden globals
  const assignmentPattern = /(?:const|let|var)\s+(\w+)\s*=/g
  const declaredVars = new Set<string>()
  let match
  while ((match = assignmentPattern.exec(code)) !== null) {
    declaredVars.add(match[1])
  }

  // Check for use of dangerous keywords that aren't declared locally
  for (const keyword of DANGEROUS_KEYWORDS) {
    const keywordPattern = new RegExp(`\\b${keyword}\\b`, 'g')
    if (keywordPattern.test(code) && !declaredVars.has(keyword)) {
      // Only error if it's not part of a string literal
      const withoutStrings = code.replace(/(["'`])(?:(?!\1)[^\\]|\\.)*\1/g, '')
      if (new RegExp(`\\b${keyword}\\b`).test(withoutStrings)) {
        if (['eval', 'Function', '__proto__', 'prototype'].includes(keyword)) {
          errors.push(`Forbidden keyword used: ${keyword}`)
        } else {
          warnings.push(`Potentially dangerous keyword: ${keyword}`)
        }
      }
    }
  }

  // Check for infinite loop patterns
  const whileTrue = /while\s*\(\s*true\s*\)/g
  const forNoCondition = /for\s*\(\s*;\s*;\s*\)/g
  if (whileTrue.test(code) || forNoCondition.test(code)) {
    warnings.push('Potential infinite loop detected. Make sure to include a break condition.')
  }

  // Check for very large string literals (could be used for DoS)
  const stringLiterals = code.match(/(["'`])(?:(?!\1)[^\\]|\\.)*\1/g) || []
  for (const literal of stringLiterals) {
    if (literal.length > 100000) {
      errors.push('String literal exceeds maximum size (100KB)')
    }
  }

  // Check code length
  if (code.length > 1000000) {
    errors.push('Code exceeds maximum length (1MB)')
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  }
}

// ─────────────────────────────────────────────────────────────────
// Sandbox Template Generation
// ─────────────────────────────────────────────────────────────────

/**
 * Generate sandboxed code wrapper for user code execution
 */
export function generateSandboxCode(userCode: string): string {
  // Escape the user code for embedding in a string
  const escapedCode = userCode
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '\\`')
    .replace(/\$/g, '\\$')

  return `
// ═══════════════════════════════════════════════════════════════
// Redois Sandbox Environment
// ═══════════════════════════════════════════════════════════════

// Capture console output
const __logs = [];
const __originalConsole = console;
const console = {
  log: (...args) => __logs.push({ level: 'log', args: args.map(a => String(a)) }),
  info: (...args) => __logs.push({ level: 'info', args: args.map(a => String(a)) }),
  warn: (...args) => __logs.push({ level: 'warn', args: args.map(a => String(a)) }),
  error: (...args) => __logs.push({ level: 'error', args: args.map(a => String(a)) }),
  debug: (...args) => __logs.push({ level: 'debug', args: args.map(a => String(a)) }),
  trace: (...args) => __logs.push({ level: 'trace', args: args.map(a => String(a)) }),
  time: (label) => __logs.push({ level: 'time', args: ['Timer started: ' + label] }),
  timeEnd: (label) => __logs.push({ level: 'timeEnd', args: ['Timer ended: ' + label] }),
  assert: (condition, ...args) => {
    if (!condition) __logs.push({ level: 'assert', args: ['Assertion failed:', ...args.map(a => String(a))] });
  },
  clear: () => {},
  count: () => {},
  countReset: () => {},
  group: () => {},
  groupCollapsed: () => {},
  groupEnd: () => {},
  table: (data) => __logs.push({ level: 'table', args: [JSON.stringify(data, null, 2)] }),
  dir: (obj) => __logs.push({ level: 'dir', args: [JSON.stringify(obj, null, 2)] }),
  dirxml: (obj) => __logs.push({ level: 'dirxml', args: [JSON.stringify(obj, null, 2)] }),
};

// Redis proxy object (will be injected by the worker)
const redis = globalThis.__redisProxy;

// Blocked globals
const fetch = undefined;
const XMLHttpRequest = undefined;
const WebSocket = undefined;
const EventSource = undefined;
const Worker = undefined;
const SharedWorker = undefined;
const ServiceWorker = undefined;
const navigator = undefined;
const location = undefined;
const document = undefined;
const window = undefined;
const parent = undefined;
const top = undefined;
const frames = undefined;
const opener = undefined;

// Safe timeout (limited iterations)
let __timeoutCount = 0;
const __maxTimeouts = 100;
const setTimeout = (fn, ms) => {
  if (__timeoutCount++ > __maxTimeouts) {
    throw new Error('Maximum setTimeout calls exceeded');
  }
  return globalThis.setTimeout(fn, Math.min(ms, 5000));
};

const clearTimeout = globalThis.clearTimeout;

// Disabled setInterval
const setInterval = () => {
  throw new Error('setInterval is not allowed in sandbox');
};
const clearInterval = () => {};

// Execution wrapper
async function __execute() {
  const __startTime = Date.now();
  let __result;

  try {
    // User code execution
    __result = await (async () => {
      ${escapedCode}
    })();
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
      logs: __logs,
      executionTime: Date.now() - __startTime,
    };
  }

  return {
    success: true,
    result: __result,
    logs: __logs,
    executionTime: Date.now() - __startTime,
  };
}

// Export for worker
export default {
  async fetch(request) {
    const result = await __execute();
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
    });
  },
};
`
}

// ─────────────────────────────────────────────────────────────────
// Redis API Template
// ─────────────────────────────────────────────────────────────────

/**
 * Generate Redis proxy stub for documentation/type hints
 */
export function generateRedisApiStub(): string {
  return `
/**
 * Redis API available in sandbox
 *
 * @example
 * // String operations
 * await redis.get('key')
 * await redis.set('key', 'value')
 * await redis.set('key', 'value', { ex: 60 }) // expires in 60 seconds
 * await redis.del('key1', 'key2')
 * await redis.exists('key1', 'key2')
 * await redis.incr('counter')
 * await redis.incrby('counter', 10)
 * await redis.decr('counter')
 * await redis.decrby('counter', 10)
 * await redis.append('key', 'suffix')
 * await redis.strlen('key')
 *
 * // Key operations
 * await redis.keys('user:*')
 * await redis.scan(0, { match: 'user:*', count: 100 })
 * await redis.expire('key', 60)
 * await redis.ttl('key')
 * await redis.type('key')
 * await redis.rename('oldkey', 'newkey')
 *
 * // Hash operations
 * await redis.hget('hash', 'field')
 * await redis.hset('hash', 'field', 'value')
 * await redis.hgetall('hash')
 * await redis.hdel('hash', 'field1', 'field2')
 *
 * // List operations
 * await redis.lpush('list', 'value1', 'value2')
 * await redis.rpush('list', 'value1', 'value2')
 * await redis.lpop('list')
 * await redis.rpop('list')
 * await redis.lrange('list', 0, -1)
 *
 * // Set operations
 * await redis.sadd('set', 'member1', 'member2')
 * await redis.smembers('set')
 * await redis.srem('set', 'member1')
 * await redis.sismember('set', 'member')
 *
 * // Sorted set operations
 * await redis.zadd('zset', 1.0, 'member')
 * await redis.zrange('zset', 0, -1)
 * await redis.zrange('zset', 0, -1, { withScores: true })
 * await redis.zrem('zset', 'member')
 * await redis.zscore('zset', 'member')
 *
 * // Server operations
 * await redis.dbsize()
 * await redis.flushdb()
 * await redis.info()
 * await redis.ping()
 */
`
}

// ─────────────────────────────────────────────────────────────────
// Code Transformation Utilities
// ─────────────────────────────────────────────────────────────────

/**
 * Wrap synchronous code to handle async Redis calls
 */
export function wrapAsyncCode(code: string): string {
  // Check if code already has async/await
  if (/\basync\b/.test(code) || /\bawait\b/.test(code)) {
    return code
  }

  // Check if code uses Redis calls
  if (/redis\s*\./.test(code)) {
    // Add await to Redis calls if not already async
    return code.replace(
      /(?<!await\s+)redis\s*\.\s*(\w+)\s*\(/g,
      'await redis.$1('
    )
  }

  return code
}

/**
 * Sanitize code for safe embedding
 */
export function sanitizeCode(code: string): string {
  // Remove potential comment-based attacks
  let sanitized = code

  // Remove null bytes
  sanitized = sanitized.replace(/\0/g, '')

  // Normalize line endings
  sanitized = sanitized.replace(/\r\n/g, '\n').replace(/\r/g, '\n')

  // Remove trailing whitespace
  sanitized = sanitized.replace(/[ \t]+$/gm, '')

  return sanitized
}

/**
 * Prepare user code for execution
 */
export function prepareUserCode(code: string): { code: string; validation: ValidationResult } {
  // Sanitize first
  const sanitized = sanitizeCode(code)

  // Validate
  const validation = validateUserCode(sanitized)

  if (!validation.valid) {
    return { code: '', validation }
  }

  // Wrap async if needed
  const wrapped = wrapAsyncCode(sanitized)

  // Generate sandbox
  const sandboxed = generateSandboxCode(wrapped)

  return { code: sandboxed, validation }
}
