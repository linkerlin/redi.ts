/**
 * Phase 65 live: lock keyspace listeners — RLock expired (lease) and deleted
 * (unlock) events via RedissonObject's expired/del subscriptions.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p65:';
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

describe('live phase 65: lock keyspace listeners', () => {
  it('delivers expired and deleted events', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}l`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(name);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase65] notify-keyspace-events not configurable — skipping');
      }
      if (!canConfig) return;

      const lock = r.getLock(name);
      const events: string[] = [];
      const id = await lock.addListener({
        onExpired: () => events.push('expired'),
        onDeleted: () => events.push('deleted'),
      });
      expect(await lock.tryLock(0, 200, 'p65')).toBe(true);
      expect(await waitFor(() => events.includes('expired'))).toBe(true);

      expect(await lock.tryLock(0, 5000, 'p65')).toBe(true);
      await lock.unlock('p65');
      expect(await waitFor(() => events.includes('deleted'))).toBe(true);

      const before = events.length;
      await lock.removeListener(id);
      expect(await lock.tryLock(0, 5000, 'p65')).toBe(true);
      await lock.unlock('p65');
      await sleep(300);
      expect(events.length).toBe(before);
    } finally {
      await r.getKeys().delete(name).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
