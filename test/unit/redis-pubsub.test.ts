/**
 * RedisPubSub Durable Object Tests
 *
 * Tests for Redis pub/sub functionality with Hibernatable WebSockets.
 * Covers SUBSCRIBE, PSUBSCRIBE, PUBLISH, UNSUBSCRIBE, and PUNSUBSCRIBE commands.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

interface WebSocketAttachment {
  channels: Set<string>
  patterns: Set<string>
  clientId: string
  connectedAt: number
}

interface PubSubMessage {
  type: 'message' | 'pmessage' | 'subscribe' | 'psubscribe' | 'unsubscribe' | 'punsubscribe'
  channel: string
  message?: string
  pattern?: string
  count: number
}

// ─────────────────────────────────────────────────────────────────
// Mock WebSocket
// ─────────────────────────────────────────────────────────────────

class MockWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3

  readyState: number = MockWebSocket.OPEN
  messages: string[] = []
  private attachment: WebSocketAttachment | null = null

  send(message: string): void {
    if (this.readyState !== MockWebSocket.OPEN) {
      throw new Error('WebSocket is not open')
    }
    this.messages.push(message)
  }

  close(_code?: number, _reason?: string): void {
    this.readyState = MockWebSocket.CLOSED
  }

  serializeAttachment(attachment: WebSocketAttachment): void {
    this.attachment = {
      ...attachment,
      channels: new Set(attachment.channels),
      patterns: new Set(attachment.patterns),
    }
  }

  deserializeAttachment(): WebSocketAttachment | null {
    if (!this.attachment) return null
    return {
      ...this.attachment,
      channels: new Set(this.attachment.channels),
      patterns: new Set(this.attachment.patterns),
    }
  }

  getMessages(): PubSubMessage[] {
    return this.messages.map((m) => JSON.parse(m))
  }

  clearMessages(): void {
    this.messages = []
  }
}

// ─────────────────────────────────────────────────────────────────
// Mock RedisPubSub (simplified for unit testing)
// ─────────────────────────────────────────────────────────────────

class MockRedisPubSub {
  private channelSubscribers: Map<string, Set<MockWebSocket>> = new Map()
  private patternSubscribers: Map<string, Set<MockWebSocket>> = new Map()
  private connections: Set<MockWebSocket> = new Set()
  private stats = {
    messagesPublished: 0,
    messagesDelivered: 0,
    subscriptionCount: 0,
    patternCount: 0,
  }

  // ─────────────────────────────────────────────────────────────────
  // Connection Management
  // ─────────────────────────────────────────────────────────────────

  addConnection(ws: MockWebSocket): void {
    const clientId = crypto.randomUUID()
    const attachment: WebSocketAttachment = {
      channels: new Set(),
      patterns: new Set(),
      clientId,
      connectedAt: Date.now(),
    }
    ws.serializeAttachment(attachment)
    this.connections.add(ws)
  }

  removeConnection(ws: MockWebSocket): void {
    this.cleanupWebSocket(ws)
  }

  // ─────────────────────────────────────────────────────────────────
  // Pub/Sub Core Methods
  // ─────────────────────────────────────────────────────────────────

  publish(channel: string, message: string): number {
    let count = 0
    this.stats.messagesPublished++

    // Direct channel subscribers
    const channelSubs = this.channelSubscribers.get(channel)
    if (channelSubs) {
      for (const ws of channelSubs) {
        if (this.isWebSocketOpen(ws)) {
          this.sendToWebSocket(ws, {
            type: 'message',
            channel,
            message,
            count: this.getSubscriptionCount(ws),
          })
          count++
          this.stats.messagesDelivered++
        }
      }
    }

    // Pattern subscribers
    for (const [pattern, patternSubs] of this.patternSubscribers) {
      if (this.matchPattern(pattern, channel)) {
        for (const ws of patternSubs) {
          if (this.isWebSocketOpen(ws)) {
            this.sendToWebSocket(ws, {
              type: 'pmessage',
              pattern,
              channel,
              message,
              count: this.getSubscriptionCount(ws),
            })
            count++
            this.stats.messagesDelivered++
          }
        }
      }
    }

    return count
  }

  subscribe(ws: MockWebSocket, channels: string[]): void {
    const attachment = this.getAttachment(ws)

    for (const channel of channels) {
      if (!this.channelSubscribers.has(channel)) {
        this.channelSubscribers.set(channel, new Set())
      }
      this.channelSubscribers.get(channel)!.add(ws)

      attachment.channels.add(channel)
      this.stats.subscriptionCount++

      // Send confirmation with current count from local attachment
      this.sendToWebSocket(ws, {
        type: 'subscribe',
        channel,
        count: attachment.channels.size + attachment.patterns.size,
      })
    }

    ws.serializeAttachment(attachment)
  }

  psubscribe(ws: MockWebSocket, patterns: string[]): void {
    const attachment = this.getAttachment(ws)

    for (const pattern of patterns) {
      if (!this.patternSubscribers.has(pattern)) {
        this.patternSubscribers.set(pattern, new Set())
      }
      this.patternSubscribers.get(pattern)!.add(ws)

      attachment.patterns.add(pattern)
      this.stats.patternCount++

      // Send confirmation with current count from local attachment
      this.sendToWebSocket(ws, {
        type: 'psubscribe',
        channel: pattern,
        count: attachment.channels.size + attachment.patterns.size,
      })
    }

    ws.serializeAttachment(attachment)
  }

  unsubscribe(ws: MockWebSocket, channels?: string[]): void {
    const attachment = this.getAttachment(ws)
    const channelsToRemove = channels ?? Array.from(attachment.channels)

    for (const channel of channelsToRemove) {
      const subs = this.channelSubscribers.get(channel)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.channelSubscribers.delete(channel)
        }
      }

      attachment.channels.delete(channel)
      this.stats.subscriptionCount = Math.max(0, this.stats.subscriptionCount - 1)

      // Send confirmation with current count from local attachment
      this.sendToWebSocket(ws, {
        type: 'unsubscribe',
        channel,
        count: attachment.channels.size + attachment.patterns.size,
      })
    }

    ws.serializeAttachment(attachment)
  }

  punsubscribe(ws: MockWebSocket, patterns?: string[]): void {
    const attachment = this.getAttachment(ws)
    const patternsToRemove = patterns ?? Array.from(attachment.patterns)

    for (const pattern of patternsToRemove) {
      const subs = this.patternSubscribers.get(pattern)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.patternSubscribers.delete(pattern)
        }
      }

      attachment.patterns.delete(pattern)
      this.stats.patternCount = Math.max(0, this.stats.patternCount - 1)

      // Send confirmation with current count from local attachment
      this.sendToWebSocket(ws, {
        type: 'punsubscribe',
        channel: pattern,
        count: attachment.channels.size + attachment.patterns.size,
      })
    }

    ws.serializeAttachment(attachment)
  }

  // ─────────────────────────────────────────────────────────────────
  // Stats & Info
  // ─────────────────────────────────────────────────────────────────

  getStats(): typeof this.stats & {
    activeConnections: number
    activeChannels: number
    activePatterns: number
  } {
    return {
      ...this.stats,
      activeConnections: this.connections.size,
      activeChannels: this.channelSubscribers.size,
      activePatterns: this.patternSubscribers.size,
    }
  }

  getChannelSubscribers(channel: string): number {
    return this.channelSubscribers.get(channel)?.size ?? 0
  }

  getPatternSubscribers(pattern: string): number {
    return this.patternSubscribers.get(pattern)?.size ?? 0
  }

  getActiveChannels(): string[] {
    return Array.from(this.channelSubscribers.keys())
  }

  getActivePatterns(): string[] {
    return Array.from(this.patternSubscribers.keys())
  }

  // ─────────────────────────────────────────────────────────────────
  // Private Helpers
  // ─────────────────────────────────────────────────────────────────

  private cleanupWebSocket(ws: MockWebSocket): void {
    this.connections.delete(ws)
    const attachment = this.getAttachment(ws)

    for (const channel of attachment.channels) {
      const subs = this.channelSubscribers.get(channel)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.channelSubscribers.delete(channel)
        }
      }
    }

    for (const pattern of attachment.patterns) {
      const subs = this.patternSubscribers.get(pattern)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.patternSubscribers.delete(pattern)
        }
      }
    }
  }

  private getAttachment(ws: MockWebSocket): WebSocketAttachment {
    const attachment = ws.deserializeAttachment()
    if (attachment) {
      return attachment
    }
    return {
      channels: new Set(),
      patterns: new Set(),
      clientId: crypto.randomUUID(),
      connectedAt: Date.now(),
    }
  }

  private getSubscriptionCount(ws: MockWebSocket): number {
    const attachment = this.getAttachment(ws)
    return attachment.channels.size + attachment.patterns.size
  }

  private isWebSocketOpen(ws: MockWebSocket): boolean {
    return ws.readyState === MockWebSocket.OPEN
  }

  private sendToWebSocket(ws: MockWebSocket, message: PubSubMessage): void {
    if (this.isWebSocketOpen(ws)) {
      ws.send(JSON.stringify(message))
    }
  }

  /**
   * Match a Redis-style glob pattern against a channel name
   * Supports: * (any characters), ? (single character), [abc] (character class)
   */
  matchPattern(pattern: string, channel: string): boolean {
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

    try {
      return new RegExp(regex).test(channel)
    } catch {
      return pattern === channel
    }
  }
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

describe('RedisPubSub Durable Object', () => {
  let pubsub: MockRedisPubSub

  beforeEach(() => {
    pubsub = new MockRedisPubSub()
  })

  // ─────────────────────────────────────────────────────────────────
  // SUBSCRIBE Tests
  // ─────────────────────────────────────────────────────────────────

  describe('SUBSCRIBE', () => {
    it('should subscribe to a single channel', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.subscribe(ws, ['news'])

      expect(pubsub.getChannelSubscribers('news')).toBe(1)
      expect(pubsub.getActiveChannels()).toContain('news')

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'subscribe',
        channel: 'news',
        count: 1,
      })
    })

    it('should subscribe to multiple channels', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.subscribe(ws, ['news', 'sports', 'weather'])

      expect(pubsub.getChannelSubscribers('news')).toBe(1)
      expect(pubsub.getChannelSubscribers('sports')).toBe(1)
      expect(pubsub.getChannelSubscribers('weather')).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(3)
      expect(messages[0].channel).toBe('news')
      expect(messages[1].channel).toBe('sports')
      expect(messages[2].channel).toBe('weather')
      expect(messages[2].count).toBe(3)
    })

    it('should allow multiple clients to subscribe to same channel', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()
      pubsub.addConnection(ws1)
      pubsub.addConnection(ws2)

      pubsub.subscribe(ws1, ['news'])
      pubsub.subscribe(ws2, ['news'])

      expect(pubsub.getChannelSubscribers('news')).toBe(2)
    })

    it('should handle duplicate subscriptions gracefully', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)

      pubsub.subscribe(ws, ['news'])
      pubsub.subscribe(ws, ['news'])

      // Channel should only have the WebSocket once
      expect(pubsub.getChannelSubscribers('news')).toBe(1)
    })

    it('should track subscription count correctly', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.subscribe(ws, ['channel1'])
      expect(ws.getMessages()[0].count).toBe(1)

      ws.clearMessages()
      pubsub.subscribe(ws, ['channel2'])
      expect(ws.getMessages()[0].count).toBe(2)

      ws.clearMessages()
      pubsub.subscribe(ws, ['channel3', 'channel4'])
      expect(ws.getMessages()[1].count).toBe(4)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // PSUBSCRIBE Tests
  // ─────────────────────────────────────────────────────────────────

  describe('PSUBSCRIBE', () => {
    it('should subscribe to a pattern', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.psubscribe(ws, ['news.*'])

      expect(pubsub.getPatternSubscribers('news.*')).toBe(1)
      expect(pubsub.getActivePatterns()).toContain('news.*')

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'psubscribe',
        channel: 'news.*',
        count: 1,
      })
    })

    it('should subscribe to multiple patterns', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.psubscribe(ws, ['news.*', 'sports.*', 'weather.*'])

      expect(pubsub.getPatternSubscribers('news.*')).toBe(1)
      expect(pubsub.getPatternSubscribers('sports.*')).toBe(1)
      expect(pubsub.getPatternSubscribers('weather.*')).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(3)
      expect(messages[2].count).toBe(3)
    })

    it('should allow mixed channel and pattern subscriptions', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.subscribe(ws, ['news', 'sports'])
      pubsub.psubscribe(ws, ['weather.*'])

      expect(pubsub.getChannelSubscribers('news')).toBe(1)
      expect(pubsub.getChannelSubscribers('sports')).toBe(1)
      expect(pubsub.getPatternSubscribers('weather.*')).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(3)
      expect(messages[2].count).toBe(3)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // UNSUBSCRIBE Tests
  // ─────────────────────────────────────────────────────────────────

  describe('UNSUBSCRIBE', () => {
    it('should unsubscribe from a specific channel', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news', 'sports'])
      ws.clearMessages()

      pubsub.unsubscribe(ws, ['news'])

      expect(pubsub.getChannelSubscribers('news')).toBe(0)
      expect(pubsub.getChannelSubscribers('sports')).toBe(1)
      expect(pubsub.getActiveChannels()).not.toContain('news')
      expect(pubsub.getActiveChannels()).toContain('sports')

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'unsubscribe',
        channel: 'news',
        count: 1,
      })
    })

    it('should unsubscribe from all channels when no channels specified', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news', 'sports', 'weather'])
      ws.clearMessages()

      pubsub.unsubscribe(ws)

      expect(pubsub.getActiveChannels()).toHaveLength(0)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(3)
      expect(messages[messages.length - 1].count).toBe(0)
    })

    it('should handle unsubscribe from non-subscribed channel', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news'])
      ws.clearMessages()

      pubsub.unsubscribe(ws, ['sports'])

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0].type).toBe('unsubscribe')
    })

    it('should remove channel from map when last subscriber leaves', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()
      pubsub.addConnection(ws1)
      pubsub.addConnection(ws2)

      pubsub.subscribe(ws1, ['news'])
      pubsub.subscribe(ws2, ['news'])

      expect(pubsub.getActiveChannels()).toContain('news')

      pubsub.unsubscribe(ws1, ['news'])
      expect(pubsub.getActiveChannels()).toContain('news')

      pubsub.unsubscribe(ws2, ['news'])
      expect(pubsub.getActiveChannels()).not.toContain('news')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // PUNSUBSCRIBE Tests
  // ─────────────────────────────────────────────────────────────────

  describe('PUNSUBSCRIBE', () => {
    it('should unsubscribe from a specific pattern', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.psubscribe(ws, ['news.*', 'sports.*'])
      ws.clearMessages()

      pubsub.punsubscribe(ws, ['news.*'])

      expect(pubsub.getPatternSubscribers('news.*')).toBe(0)
      expect(pubsub.getPatternSubscribers('sports.*')).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'punsubscribe',
        channel: 'news.*',
        count: 1,
      })
    })

    it('should unsubscribe from all patterns when no patterns specified', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.psubscribe(ws, ['news.*', 'sports.*', 'weather.*'])
      ws.clearMessages()

      pubsub.punsubscribe(ws)

      expect(pubsub.getActivePatterns()).toHaveLength(0)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(3)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // PUBLISH Tests
  // ─────────────────────────────────────────────────────────────────

  describe('PUBLISH', () => {
    it('should publish to channel subscribers', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news'])
      ws.clearMessages()

      const count = pubsub.publish('news', 'Hello World')

      expect(count).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'message',
        channel: 'news',
        message: 'Hello World',
        count: 1,
      })
    })

    it('should publish to multiple subscribers', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()
      const ws3 = new MockWebSocket()
      pubsub.addConnection(ws1)
      pubsub.addConnection(ws2)
      pubsub.addConnection(ws3)

      pubsub.subscribe(ws1, ['news'])
      pubsub.subscribe(ws2, ['news'])
      pubsub.subscribe(ws3, ['sports'])

      ws1.clearMessages()
      ws2.clearMessages()
      ws3.clearMessages()

      const count = pubsub.publish('news', 'Breaking News')

      expect(count).toBe(2)
      expect(ws1.getMessages()).toHaveLength(1)
      expect(ws2.getMessages()).toHaveLength(1)
      expect(ws3.getMessages()).toHaveLength(0)
    })

    it('should return 0 when no subscribers', () => {
      const count = pubsub.publish('nonexistent', 'Hello')
      expect(count).toBe(0)
    })

    it('should not publish to closed WebSockets', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news'])
      ws.clearMessages()

      ws.readyState = MockWebSocket.CLOSED

      const count = pubsub.publish('news', 'Hello')

      expect(count).toBe(0)
      expect(ws.messages).toHaveLength(0)
    })

    it('should publish to pattern subscribers', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.psubscribe(ws, ['news.*'])
      ws.clearMessages()

      const count = pubsub.publish('news.breaking', 'Alert!')

      expect(count).toBe(1)

      const messages = ws.getMessages()
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        type: 'pmessage',
        pattern: 'news.*',
        channel: 'news.breaking',
        message: 'Alert!',
        count: 1,
      })
    })

    it('should publish to both channel and pattern subscribers', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()
      pubsub.addConnection(ws1)
      pubsub.addConnection(ws2)

      pubsub.subscribe(ws1, ['news.breaking'])
      pubsub.psubscribe(ws2, ['news.*'])

      ws1.clearMessages()
      ws2.clearMessages()

      const count = pubsub.publish('news.breaking', 'Alert!')

      expect(count).toBe(2)
      expect(ws1.getMessages()[0].type).toBe('message')
      expect(ws2.getMessages()[0].type).toBe('pmessage')
    })

    it('should handle JSON messages', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['data'])
      ws.clearMessages()

      const jsonMessage = JSON.stringify({ event: 'update', data: { id: 1, name: 'test' } })
      pubsub.publish('data', jsonMessage)

      const messages = ws.getMessages()
      expect(messages[0].message).toBe(jsonMessage)
      expect(JSON.parse(messages[0].message!)).toEqual({ event: 'update', data: { id: 1, name: 'test' } })
    })

    it('should handle empty message', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['channel'])
      ws.clearMessages()

      const count = pubsub.publish('channel', '')

      expect(count).toBe(1)
      expect(ws.getMessages()[0].message).toBe('')
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Pattern Matching Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Pattern Matching', () => {
    it('should match * wildcard (any characters)', () => {
      expect(pubsub.matchPattern('news.*', 'news.breaking')).toBe(true)
      expect(pubsub.matchPattern('news.*', 'news.')).toBe(true)
      expect(pubsub.matchPattern('news.*', 'news.us.politics')).toBe(true)
      expect(pubsub.matchPattern('*', 'anything')).toBe(true)
      expect(pubsub.matchPattern('news.*', 'sports.breaking')).toBe(false)
    })

    it('should match ? wildcard (single character)', () => {
      expect(pubsub.matchPattern('news.?', 'news.a')).toBe(true)
      expect(pubsub.matchPattern('news.?', 'news.1')).toBe(true)
      expect(pubsub.matchPattern('news.?', 'news.ab')).toBe(false)
      expect(pubsub.matchPattern('news.?', 'news.')).toBe(false)
    })

    it('should match [abc] character class', () => {
      expect(pubsub.matchPattern('channel.[abc]', 'channel.a')).toBe(true)
      expect(pubsub.matchPattern('channel.[abc]', 'channel.b')).toBe(true)
      expect(pubsub.matchPattern('channel.[abc]', 'channel.c')).toBe(true)
      expect(pubsub.matchPattern('channel.[abc]', 'channel.d')).toBe(false)
    })

    it('should match [a-z] character range', () => {
      expect(pubsub.matchPattern('channel.[a-z]', 'channel.a')).toBe(true)
      expect(pubsub.matchPattern('channel.[a-z]', 'channel.z')).toBe(true)
      expect(pubsub.matchPattern('channel.[a-z]', 'channel.m')).toBe(true)
      expect(pubsub.matchPattern('channel.[a-z]', 'channel.A')).toBe(false)
      expect(pubsub.matchPattern('channel.[a-z]', 'channel.1')).toBe(false)
    })

    it('should match combined patterns', () => {
      expect(pubsub.matchPattern('user.*.profile', 'user.123.profile')).toBe(true)
      expect(pubsub.matchPattern('user.*.profile', 'user.abc.profile')).toBe(true)
      expect(pubsub.matchPattern('user.*.profile', 'user.123.settings')).toBe(false)

      expect(pubsub.matchPattern('log.[0-9][0-9][0-9]', 'log.123')).toBe(true)
      expect(pubsub.matchPattern('log.[0-9][0-9][0-9]', 'log.999')).toBe(true)
      expect(pubsub.matchPattern('log.[0-9][0-9][0-9]', 'log.12')).toBe(false)
    })

    it('should escape regex special characters', () => {
      expect(pubsub.matchPattern('test.com', 'test.com')).toBe(true)
      expect(pubsub.matchPattern('test.com', 'testXcom')).toBe(false)
      expect(pubsub.matchPattern('price$', 'price$')).toBe(true)
      expect(pubsub.matchPattern('(test)', '(test)')).toBe(true)
    })

    it('should handle h?llo pattern', () => {
      expect(pubsub.matchPattern('h?llo', 'hello')).toBe(true)
      expect(pubsub.matchPattern('h?llo', 'hallo')).toBe(true)
      expect(pubsub.matchPattern('h?llo', 'hxllo')).toBe(true)
    })

    it('should handle h*llo pattern', () => {
      expect(pubsub.matchPattern('h*llo', 'hllo')).toBe(true)
      expect(pubsub.matchPattern('h*llo', 'hello')).toBe(true)
      expect(pubsub.matchPattern('h*llo', 'heeeello')).toBe(true)
    })

    it('should handle h[ae]llo pattern', () => {
      expect(pubsub.matchPattern('h[ae]llo', 'hello')).toBe(true)
      expect(pubsub.matchPattern('h[ae]llo', 'hallo')).toBe(true)
      expect(pubsub.matchPattern('h[ae]llo', 'hillo')).toBe(false)
    })

    it('should handle negated character class h[^e]llo', () => {
      expect(pubsub.matchPattern('h[^e]llo', 'hallo')).toBe(true)
      expect(pubsub.matchPattern('h[^e]llo', 'hbllo')).toBe(true)
      expect(pubsub.matchPattern('h[^e]llo', 'hello')).toBe(false)
    })

    it('should handle h[a-b]llo pattern', () => {
      expect(pubsub.matchPattern('h[a-b]llo', 'hallo')).toBe(true)
      expect(pubsub.matchPattern('h[a-b]llo', 'hbllo')).toBe(true)
      expect(pubsub.matchPattern('h[a-b]llo', 'hcllo')).toBe(false)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Connection Management Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Connection Management', () => {
    it('should clean up subscriptions when connection is removed', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news', 'sports'])
      pubsub.psubscribe(ws, ['weather.*'])

      expect(pubsub.getActiveChannels()).toContain('news')
      expect(pubsub.getActiveChannels()).toContain('sports')
      expect(pubsub.getActivePatterns()).toContain('weather.*')

      pubsub.removeConnection(ws)

      expect(pubsub.getActiveChannels()).not.toContain('news')
      expect(pubsub.getActiveChannels()).not.toContain('sports')
      expect(pubsub.getActivePatterns()).not.toContain('weather.*')
    })

    it('should not affect other connections when one is removed', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()
      pubsub.addConnection(ws1)
      pubsub.addConnection(ws2)

      pubsub.subscribe(ws1, ['news'])
      pubsub.subscribe(ws2, ['news', 'sports'])

      pubsub.removeConnection(ws1)

      expect(pubsub.getChannelSubscribers('news')).toBe(1)
      expect(pubsub.getChannelSubscribers('sports')).toBe(1)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Stats Tests
  // ─────────────────────────────────────────────────────────────────

  describe('Stats Tracking', () => {
    it('should track message publish count', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news'])

      pubsub.publish('news', 'message1')
      pubsub.publish('news', 'message2')
      pubsub.publish('sports', 'message3') // No subscribers

      const stats = pubsub.getStats()
      expect(stats.messagesPublished).toBe(3)
      expect(stats.messagesDelivered).toBe(2)
    })

    it('should track active connections', () => {
      const ws1 = new MockWebSocket()
      const ws2 = new MockWebSocket()

      expect(pubsub.getStats().activeConnections).toBe(0)

      pubsub.addConnection(ws1)
      expect(pubsub.getStats().activeConnections).toBe(1)

      pubsub.addConnection(ws2)
      expect(pubsub.getStats().activeConnections).toBe(2)

      pubsub.removeConnection(ws1)
      expect(pubsub.getStats().activeConnections).toBe(1)
    })

    it('should track active channels and patterns', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)

      expect(pubsub.getStats().activeChannels).toBe(0)
      expect(pubsub.getStats().activePatterns).toBe(0)

      pubsub.subscribe(ws, ['news', 'sports'])
      expect(pubsub.getStats().activeChannels).toBe(2)

      pubsub.psubscribe(ws, ['weather.*'])
      expect(pubsub.getStats().activePatterns).toBe(1)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // Edge Cases
  // ─────────────────────────────────────────────────────────────────

  describe('Edge Cases', () => {
    it('should handle channel names with special characters', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)

      pubsub.subscribe(ws, ['channel:with:colons', 'channel.with.dots', 'channel/with/slashes'])
      ws.clearMessages()

      pubsub.publish('channel:with:colons', 'test')
      expect(ws.getMessages()).toHaveLength(1)
    })

    it('should handle empty channel list for subscribe', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      ws.clearMessages()

      pubsub.subscribe(ws, [])

      expect(ws.getMessages()).toHaveLength(0)
    })

    it('should handle concurrent subscriptions and publishes', () => {
      const subscribers: MockWebSocket[] = []

      // Create 10 subscribers
      for (let i = 0; i < 10; i++) {
        const ws = new MockWebSocket()
        pubsub.addConnection(ws)
        pubsub.subscribe(ws, ['broadcast'])
        ws.clearMessages()
        subscribers.push(ws)
      }

      // Publish to all
      const count = pubsub.publish('broadcast', 'Hello everyone!')

      expect(count).toBe(10)
      subscribers.forEach((ws) => {
        expect(ws.getMessages()).toHaveLength(1)
        expect(ws.getMessages()[0].message).toBe('Hello everyone!')
      })
    })

    it('should handle pattern with no matches', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.psubscribe(ws, ['nonexistent.*'])
      ws.clearMessages()

      const count = pubsub.publish('other.channel', 'test')

      expect(count).toBe(0)
      expect(ws.getMessages()).toHaveLength(0)
    })

    it('should handle same client receiving message from both channel and pattern', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news.breaking'])
      pubsub.psubscribe(ws, ['news.*'])
      ws.clearMessages()

      const count = pubsub.publish('news.breaking', 'Alert!')

      // Should receive both a 'message' and 'pmessage'
      expect(count).toBe(2)
      expect(ws.getMessages()).toHaveLength(2)
      expect(ws.getMessages()[0].type).toBe('message')
      expect(ws.getMessages()[1].type).toBe('pmessage')
    })

    it('should handle unicode in channel names and messages', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['channel-with-emoji'])
      ws.clearMessages()

      pubsub.publish('channel-with-emoji', 'Hello World!')

      expect(ws.getMessages()[0].message).toBe('Hello World!')
    })

    it('should handle very long channel names', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      const longChannelName = 'a'.repeat(1000)

      pubsub.subscribe(ws, [longChannelName])
      ws.clearMessages()

      const count = pubsub.publish(longChannelName, 'test')

      expect(count).toBe(1)
    })
  })

  // ─────────────────────────────────────────────────────────────────
  // WebSocket Attachment Persistence (Hibernation)
  // ─────────────────────────────────────────────────────────────────

  describe('WebSocket Attachment Persistence', () => {
    it('should persist subscription state in attachment', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news', 'sports'])
      pubsub.psubscribe(ws, ['weather.*'])

      const attachment = ws.deserializeAttachment()
      expect(attachment).not.toBeNull()
      expect(attachment!.channels.has('news')).toBe(true)
      expect(attachment!.channels.has('sports')).toBe(true)
      expect(attachment!.patterns.has('weather.*')).toBe(true)
    })

    it('should update attachment on unsubscribe', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)
      pubsub.subscribe(ws, ['news', 'sports'])
      pubsub.unsubscribe(ws, ['news'])

      const attachment = ws.deserializeAttachment()
      expect(attachment!.channels.has('news')).toBe(false)
      expect(attachment!.channels.has('sports')).toBe(true)
    })

    it('should have clientId in attachment', () => {
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)

      const attachment = ws.deserializeAttachment()
      expect(attachment).not.toBeNull()
      expect(attachment!.clientId).toBeDefined()
      expect(typeof attachment!.clientId).toBe('string')
      expect(attachment!.clientId.length).toBeGreaterThan(0)
    })

    it('should have connection timestamp in attachment', () => {
      const now = Date.now()
      const ws = new MockWebSocket()
      pubsub.addConnection(ws)

      const attachment = ws.deserializeAttachment()
      expect(attachment!.connectedAt).toBeGreaterThanOrEqual(now)
      expect(attachment!.connectedAt).toBeLessThanOrEqual(Date.now())
    })
  })
})
