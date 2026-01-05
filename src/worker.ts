/**
 * Redois Worker Entry Point
 *
 * Main Cloudflare Worker that handles all incoming requests.
 */

import { RedoisEntrypoint } from './entrypoint'
import { RedisShard } from './durable-objects/redis-shard'
import { RedisPubSub } from './durable-objects/redis-pubsub'
import { RedisCoordinator } from './durable-objects/redis-coordinator'
import type { Env } from './types'

// Export Durable Object classes
export { RedisShard, RedisPubSub, RedisCoordinator }

// Export default fetch handler
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const entrypoint = new RedoisEntrypoint(ctx, env)
    return entrypoint.fetch(request)
  },
}
