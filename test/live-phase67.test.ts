/**
 * Phase 67 live: RBucket set/expired/del and RScoredSortedSet
 * zadd/zrem/zincr/store keyspace listeners.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p67:';
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

describe('live phase 67: bucket/zset listeners', () => {
  it('delivers set/expired/del and zadd/zrem/zincr/store events', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const bn = `${NS}b`;
    const zn = `${NS}z`;
    const z2 = `${NS}z2`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(bn, zn, z2);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase67] notify-keyspace-events not configurable — skipping');
      }
      if (!canConfig) return;

      const bucket = r.getBucket<string>(bn);
      const bucketEvents: string[] = [];
      const bid = await bucket.addListener({
        onSet: () => bucketEvents.push('set'),
        onExpired: () => bucketEvents.push('expired'),
        onDeleted: () => bucketEvents.push('del'),
      });
      await bucket.set('v');
      expect(await waitFor(() => bucketEvents.includes('set'))).toBe(true);
      await r.redis.pexpire(bn, 100);
      expect(await waitFor(() => bucketEvents.includes('expired'))).toBe(true);
      await bucket.set('v2');
      await bucket.delete();
      expect(await waitFor(() => bucketEvents.includes('del'))).toBe(true);
      await bucket.removeListener(bid);

      const z = r.getScoredSortedSet<string>(zn);
      const zEvents: string[] = [];
      const zid = await z.addListener({
        onAdd: () => zEvents.push('add'),
        onRemove: () => zEvents.push('remove'),
        onIncrement: () => zEvents.push('incr'),
        onStore: () => zEvents.push('store'),
      });
      await z.add(1, 'a');
      await z.addScore('a', 2);
      await z.remove('a');
      await r.getScoredSortedSet<string>(z2).add(5, 'b');
      await z.union(z2);
      expect(await waitFor(() => ['add', 'remove', 'incr', 'store'].every((e) => zEvents.includes(e)))).toBe(true);
      await z.removeListener(zid);
    } finally {
      await r.getKeys().delete(bn, zn, z2).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
