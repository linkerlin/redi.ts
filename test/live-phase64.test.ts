/**
 * Phase 64 live: RLocalCachedMap expiration event policy. The default
 * keyevent pattern clears the near cache when the Redis key expires;
 * DONT_SUBSCRIBE leaves it untouched.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p64:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    r.on('error', () => undefined);
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
const waitFor = async (check: () => boolean, timeoutMs = 5000): Promise<boolean> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return true;
    await sleep(20);
  }
  return check();
};

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live phase 64: local cached map expiration policy', () => {
  it('clears on key expiration unless DONT_SUBSCRIBE', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const eventName = `${NS}e`;
    const noneName = `${NS}n`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(eventName, noneName);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase64] notify-keyspace-events not configurable — skipping');
      }
      if (!canConfig) return;

      const subscribed = r.getLocalCachedMap<string, string>(eventName, {
        expirationEventPolicy: 'SUBSCRIBE_WITH_KEYEVENT_PATTERN',
      });
      await subscribed.put('a', '1');
      expect(await subscribed.get('a')).toBe('1');
      expect(subscribed.getCachedMap().size).toBe(1);
      await r.redis.pexpire(eventName, 100);
      expect(await waitFor(() => subscribed.getCachedMap().size === 0)).toBe(true);
      expect(await subscribed.get('a')).toBeNull();

      const none = r.getLocalCachedMap<string, string>(noneName, {
        expirationEventPolicy: 'DONT_SUBSCRIBE',
      });
      await none.put('a', '2');
      expect(await none.get('a')).toBe('2');
      expect(none.getCachedMap().size).toBe(1);
      await r.redis.pexpire(noneName, 100);
      await sleep(400);
      expect(none.getCachedMap().size).toBe(1);
    } finally {
      await r.getKeys().delete(eventName, noneName).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
