/**
 * Phase 52 live: RSet countIntersection (SINTERCARD + LIMIT) and keyspace
 * listeners (sadd/srem/spop/store). Config mutation is serialized with the
 * other keyspace tests via the shared Redis lock.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p52:';
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

describe('live phase 52: RSet countIntersection + listeners', () => {
  it('counts intersections and delivers set keyspace events', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1n = `${NS}s1`;
    const s2n = `${NS}s2`;
    const s3n = `${NS}s3`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(s1n, s2n, s3n);
      const s1 = r.getSet<string>(s1n);
      const s2 = r.getSet<string>(s2n);
      const s3 = r.getSet<string>(s3n);
      await s1.add('a', 'b', 'c');
      await s2.add('b', 'c', 'd');
      await s3.add('c', 'e');
      expect(await s1.countIntersection(s2n)).toBe(2);
      expect(await s1.countIntersection(s2n, s3n)).toBe(1);
      expect(await s1.countIntersection(1, s2n)).toBe(1);
      expect(await s1.countIntersection()).toBe(3);
      expect(await s1.countIntersection(`${NS}missing`)).toBe(0);

      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase52] notify-keyspace-events not configurable — skipping listener assertions');
      }
      if (canConfig) {
        const events: string[] = [];
        const id = await s1.addListener({
          onAdd: () => events.push('add'),
          onRemove: () => events.push('remove'),
          onRandomRemove: () => events.push('random'),
          onStore: () => events.push('store'),
        });
        expect(id).toBeGreaterThan(0);
        await s1.add('x');
        await s1.remove('x');
        await s1.add('y', 'z');
        await s1.removeRandom(1);
        await s1.intersection(s2n);
        expect(await waitFor(() => ['add', 'remove', 'random', 'store'].every((e) => events.includes(e)))).toBe(true);

        const before = events.length;
        await s1.removeListener(id);
        await s1.add('after');
        await sleep(400);
        expect(events.length).toBe(before);
      }
    } finally {
      await r.getKeys().delete(s1n, s2n, s3n).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
