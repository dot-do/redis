/**
 * RedisPubSub Durable Object
 *
 * Implements Redis pub/sub functionality using Cloudflare's Hibernatable WebSockets.
 * Supports SUBSCRIBE, PSUBSCRIBE, PUBLISH, UNSUBSCRIBE, and PUNSUBSCRIBE commands.
 */

import { DurableObject } from 'cloudflare:workers'
import type { Env } from '../types'

// ─────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────

interface WebSocketAttachment {
  /** Subscribed channels */
  channels: Set<string>
  /** Subscribed patterns */
  patterns: Set<string>
  /** Client identifier */
  clientId: string
  /** Connection timestamp */
  connectedAt: number
}

interface PubSubMessage {
  type: 'message' | 'pmessage' | 'subscribe' | 'psubscribe' | 'unsubscribe' | 'punsubscribe'
  channel: string
  message?: string
  pattern?: string
  count: number
}

interface IncomingMessage {
  action: 'subscribe' | 'psubscribe' | 'unsubscribe' | 'punsubscribe' | 'ping'
  channels?: string[]
  patterns?: string[]
}

// ─────────────────────────────────────────────────────────────────
// RedisPubSub Durable Object
// ─────────────────────────────────────────────────────────────────

export class RedisPubSub extends DurableObject<Env> {
  /** Channel to WebSocket set mapping */
  private channelSubscribers: Map<string, Set<WebSocket>> = new Map()

  /** Pattern to WebSocket set mapping */
  private patternSubscribers: Map<string, Set<WebSocket>> = new Map()

  /** All connected WebSockets */
  private connections: Set<WebSocket> = new Set()

  /** Stats tracking */
  private stats = {
    messagesPublished: 0,
    messagesDelivered: 0,
    subscriptionCount: 0,
    patternCount: 0,
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // Restore WebSocket state on wake from hibernation
    this.ctx.blockConcurrencyWhile(async () => {
      const websockets = this.ctx.getWebSockets()
      for (const ws of websockets) {
        this.restoreWebSocket(ws)
      }
    })
  }

  // ─────────────────────────────────────────────────────────────────
  // HTTP Handler
  // ─────────────────────────────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname

    // WebSocket upgrade
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocketUpgrade(request)
    }

    // REST API
    switch (path) {
      case '/publish':
        return this.handlePublishRequest(request)
      case '/stats':
        return this.handleStatsRequest()
      case '/channels':
        return this.handleChannelsRequest()
      default:
        return new Response('Not Found', { status: 404 })
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // WebSocket Upgrade
  // ─────────────────────────────────────────────────────────────────

  private handleWebSocketUpgrade(_request: Request): Response {
    const pair = new WebSocketPair()
    const [client, server] = Object.values(pair)

    const clientId = crypto.randomUUID()
    const attachment: WebSocketAttachment = {
      channels: new Set(),
      patterns: new Set(),
      clientId,
      connectedAt: Date.now(),
    }

    // Accept with hibernation support
    this.ctx.acceptWebSocket(server, [clientId])

    // Store attachment for hibernation
    server.serializeAttachment(attachment)

    // Track connection
    this.connections.add(server)

    // Send welcome message
    this.sendToWebSocket(server, {
      type: 'subscribe',
      channel: '__welcome__',
      message: JSON.stringify({ clientId, timestamp: Date.now() }),
      count: 0,
    })

    return new Response(null, {
      status: 101,
      webSocket: client,
    })
  }

  // ─────────────────────────────────────────────────────────────────
  // REST API Handlers
  // ─────────────────────────────────────────────────────────────────

  private async handlePublishRequest(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 })
    }

    const body = await request.json() as { channel: string; message: string }
    const { channel, message } = body

    if (!channel || message === undefined) {
      return new Response(JSON.stringify({ error: 'channel and message required' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      })
    }

    const count = await this.publish(channel, message)
    return new Response(JSON.stringify({ published: count }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleStatsRequest(): Response {
    return new Response(JSON.stringify({
      ...this.stats,
      activeConnections: this.connections.size,
      activeChannels: this.channelSubscribers.size,
      activePatterns: this.patternSubscribers.size,
    }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  private handleChannelsRequest(): Response {
    const channels: Record<string, number> = {}
    for (const [channel, subscribers] of this.channelSubscribers) {
      channels[channel] = subscribers.size
    }

    const patterns: Record<string, number> = {}
    for (const [pattern, subscribers] of this.patternSubscribers) {
      patterns[pattern] = subscribers.size
    }

    return new Response(JSON.stringify({ channels, patterns }), {
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // ─────────────────────────────────────────────────────────────────
  // Pub/Sub Core Methods
  // ─────────────────────────────────────────────────────────────────

  /**
   * Publish a message to a channel
   * Returns the number of subscribers that received the message
   */
  async publish(channel: string, message: string): Promise<number> {
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

  /**
   * Subscribe a WebSocket to one or more channels
   */
  subscribe(ws: WebSocket, channels: string[]): void {
    const attachment = this.getAttachment(ws)

    for (const channel of channels) {
      // Add to channel subscribers
      if (!this.channelSubscribers.has(channel)) {
        this.channelSubscribers.set(channel, new Set())
      }
      this.channelSubscribers.get(channel)!.add(ws)

      // Track in attachment
      attachment.channels.add(channel)
      this.stats.subscriptionCount++

      // Send confirmation
      this.sendToWebSocket(ws, {
        type: 'subscribe',
        channel,
        count: this.getSubscriptionCount(ws),
      })
    }

    // Update attachment
    ws.serializeAttachment(attachment)
  }

  /**
   * Subscribe a WebSocket to one or more patterns
   */
  psubscribe(ws: WebSocket, patterns: string[]): void {
    const attachment = this.getAttachment(ws)

    for (const pattern of patterns) {
      // Add to pattern subscribers
      if (!this.patternSubscribers.has(pattern)) {
        this.patternSubscribers.set(pattern, new Set())
      }
      this.patternSubscribers.get(pattern)!.add(ws)

      // Track in attachment
      attachment.patterns.add(pattern)
      this.stats.patternCount++

      // Send confirmation
      this.sendToWebSocket(ws, {
        type: 'psubscribe',
        channel: pattern,
        count: this.getSubscriptionCount(ws),
      })
    }

    // Update attachment
    ws.serializeAttachment(attachment)
  }

  /**
   * Unsubscribe a WebSocket from one or more channels
   */
  unsubscribe(ws: WebSocket, channels?: string[]): void {
    const attachment = this.getAttachment(ws)

    // If no channels specified, unsubscribe from all
    const channelsToRemove = channels ?? Array.from(attachment.channels)

    for (const channel of channelsToRemove) {
      // Remove from channel subscribers
      const subs = this.channelSubscribers.get(channel)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.channelSubscribers.delete(channel)
        }
      }

      // Remove from attachment
      attachment.channels.delete(channel)
      this.stats.subscriptionCount = Math.max(0, this.stats.subscriptionCount - 1)

      // Send confirmation
      this.sendToWebSocket(ws, {
        type: 'unsubscribe',
        channel,
        count: this.getSubscriptionCount(ws),
      })
    }

    // Update attachment
    ws.serializeAttachment(attachment)
  }

  /**
   * Unsubscribe a WebSocket from one or more patterns
   */
  punsubscribe(ws: WebSocket, patterns?: string[]): void {
    const attachment = this.getAttachment(ws)

    // If no patterns specified, unsubscribe from all
    const patternsToRemove = patterns ?? Array.from(attachment.patterns)

    for (const pattern of patternsToRemove) {
      // Remove from pattern subscribers
      const subs = this.patternSubscribers.get(pattern)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.patternSubscribers.delete(pattern)
        }
      }

      // Remove from attachment
      attachment.patterns.delete(pattern)
      this.stats.patternCount = Math.max(0, this.stats.patternCount - 1)

      // Send confirmation
      this.sendToWebSocket(ws, {
        type: 'punsubscribe',
        channel: pattern,
        count: this.getSubscriptionCount(ws),
      })
    }

    // Update attachment
    ws.serializeAttachment(attachment)
  }

  // ─────────────────────────────────────────────────────────────────
  // Hibernatable WebSocket Handlers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Called when a WebSocket receives a message
   * This wakes the DO from hibernation if needed
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    try {
      const data = typeof message === 'string'
        ? JSON.parse(message)
        : JSON.parse(new TextDecoder().decode(message))

      await this.handleIncomingMessage(ws, data as IncomingMessage)
    } catch (error) {
      this.sendToWebSocket(ws, {
        type: 'message',
        channel: '__error__',
        message: error instanceof Error ? error.message : 'Unknown error',
        count: 0,
      })
    }
  }

  /**
   * Called when a WebSocket is closed
   * Cleans up all subscriptions for this connection
   */
  async webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    this.cleanupWebSocket(ws)
  }

  /**
   * Called when a WebSocket encounters an error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error)
    this.cleanupWebSocket(ws)
  }

  // ─────────────────────────────────────────────────────────────────
  // Private Helpers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Handle incoming WebSocket messages
   */
  private async handleIncomingMessage(ws: WebSocket, data: IncomingMessage): Promise<void> {
    switch (data.action) {
      case 'subscribe':
        if (data.channels && data.channels.length > 0) {
          this.subscribe(ws, data.channels)
        }
        break

      case 'psubscribe':
        if (data.patterns && data.patterns.length > 0) {
          this.psubscribe(ws, data.patterns)
        }
        break

      case 'unsubscribe':
        this.unsubscribe(ws, data.channels)
        break

      case 'punsubscribe':
        this.punsubscribe(ws, data.patterns)
        break

      case 'ping':
        this.sendToWebSocket(ws, {
          type: 'message',
          channel: '__pong__',
          message: Date.now().toString(),
          count: this.getSubscriptionCount(ws),
        })
        break

      default:
        console.warn('Unknown action:', data)
    }
  }

  /**
   * Restore WebSocket state after hibernation wake
   */
  private restoreWebSocket(ws: WebSocket): void {
    const attachment = this.getAttachment(ws)

    // Track connection
    this.connections.add(ws)

    // Restore channel subscriptions
    for (const channel of attachment.channels) {
      if (!this.channelSubscribers.has(channel)) {
        this.channelSubscribers.set(channel, new Set())
      }
      this.channelSubscribers.get(channel)!.add(ws)
    }

    // Restore pattern subscriptions
    for (const pattern of attachment.patterns) {
      if (!this.patternSubscribers.has(pattern)) {
        this.patternSubscribers.set(pattern, new Set())
      }
      this.patternSubscribers.get(pattern)!.add(ws)
    }
  }

  /**
   * Clean up a WebSocket when it disconnects
   */
  private cleanupWebSocket(ws: WebSocket): void {
    // Remove from tracking
    this.connections.delete(ws)

    // Get attachment for cleanup
    const attachment = this.getAttachment(ws)

    // Remove from all channel subscriptions
    for (const channel of attachment.channels) {
      const subs = this.channelSubscribers.get(channel)
      if (subs) {
        subs.delete(ws)
        if (subs.size === 0) {
          this.channelSubscribers.delete(channel)
        }
      }
    }

    // Remove from all pattern subscriptions
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

  /**
   * Get attachment from WebSocket, with defaults
   */
  private getAttachment(ws: WebSocket): WebSocketAttachment {
    try {
      const attachment = ws.deserializeAttachment() as WebSocketAttachment | null
      if (attachment) {
        // Convert arrays back to Sets if needed (serialization converts Sets to arrays)
        return {
          channels: attachment.channels instanceof Set
            ? attachment.channels
            : new Set(attachment.channels as unknown as string[]),
          patterns: attachment.patterns instanceof Set
            ? attachment.patterns
            : new Set(attachment.patterns as unknown as string[]),
          clientId: attachment.clientId,
          connectedAt: attachment.connectedAt,
        }
      }
    } catch {
      // Ignore deserialization errors
    }

    return {
      channels: new Set(),
      patterns: new Set(),
      clientId: crypto.randomUUID(),
      connectedAt: Date.now(),
    }
  }

  /**
   * Get total subscription count for a WebSocket
   */
  private getSubscriptionCount(ws: WebSocket): number {
    const attachment = this.getAttachment(ws)
    return attachment.channels.size + attachment.patterns.size
  }

  /**
   * Check if a WebSocket is still open
   */
  private isWebSocketOpen(ws: WebSocket): boolean {
    return ws.readyState === WebSocket.OPEN
  }

  /**
   * Send a message to a WebSocket
   */
  private sendToWebSocket(ws: WebSocket, message: PubSubMessage): void {
    if (this.isWebSocketOpen(ws)) {
      ws.send(JSON.stringify(message))
    }
  }

  /**
   * Match a Redis-style glob pattern against a channel name
   * Supports: * (any characters), ? (single character), [abc] (character class)
   */
  private matchPattern(pattern: string, channel: string): boolean {
    // Convert Redis glob pattern to regex
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
          // Character class
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
          // Escape next character
          if (i + 1 < pattern.length) {
            regex += '\\' + pattern[i + 1]
            i++
          } else {
            regex += '\\\\'
          }
          break
        // Escape regex special characters
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
      // If regex is invalid, fall back to exact match
      return pattern === channel
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // RPC Methods (for direct DO-to-DO communication)
  // ─────────────────────────────────────────────────────────────────

  /**
   * Get the number of subscribers for a channel
   */
  async getChannelSubscribers(channel: string): Promise<number> {
    return this.channelSubscribers.get(channel)?.size ?? 0
  }

  /**
   * Get the number of subscribers for a pattern
   */
  async getPatternSubscribers(pattern: string): Promise<number> {
    return this.patternSubscribers.get(pattern)?.size ?? 0
  }

  /**
   * Get all active channels
   */
  async getActiveChannels(): Promise<string[]> {
    return Array.from(this.channelSubscribers.keys())
  }

  /**
   * Get all active patterns
   */
  async getActivePatterns(): Promise<string[]> {
    return Array.from(this.patternSubscribers.keys())
  }

  /**
   * Get number of active connections
   */
  async getConnectionCount(): Promise<number> {
    return this.connections.size
  }
}
