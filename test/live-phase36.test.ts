/**
 * Phase 36 live: probabilistic deep API — TDigest (min/max/cdf/rank/byRank/
 * merge/info/reset), CuckooFilter (init/addIfAbsent/existsMany/count/info) and
 * TopK (dropped results/incrementBy/contains/count/listWithCount/info).
 *
 * Commands are capability-gated (Redis 8 modules); skips with a log when a
 * command is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p36:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

async function hasCmd(redis: Redis, cmd: string): Promise<boolean> {
  try {
    const info: any = await (redis as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(info) && info.length > 0 && info[0] !== null && info[0] !== undefined;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 36: probabilistic deep APIs', () => {
  it('TDigest statistics and merge', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}td`;
    const name2 = `${NS}td2`;
    try {
      if (!(await hasCmd(r.redis, 'TDIGEST.CREATE'))) {
        console.log('[phase36] TDIGEST unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name, name2);
      const td = r.getTDigest(name);
      await td.create(100);
      await td.add(...Array.from({ length: 100 }, (_, i) => i + 1));
      expect(await td.getMin()).toBe(1);
      expect(await td.getMax()).toBe(100);
      const q = await td.quantile(0.5);
      expect(q[0]).toBeGreaterThan(49);
      expect(q[0]).toBeLessThan(52);
      const cdf = await td.cumulativeProbability(50);
      expect(cdf[0]).toBeGreaterThan(0.4);
      expect(cdf[0]).toBeLessThan(0.6);
      expect(await td.trimmedMean(0, 1)).toBeCloseTo(50.5, 0);

      const ranks = await td.rank(50, 100);
      expect(ranks[0]).toBeGreaterThanOrEqual(48);
      expect(ranks[0]).toBeLessThanOrEqual(50);
      expect(ranks[1]).toBe(99);
      expect((await td.revRank(100))[0]).toBe(0);
      const by = await td.byRank(0, 99);
      expect(by[0]).toBe(1);
      expect(by[1]).toBe(100);
      expect((await td.byRevRank(0))[0]).toBe(100);

      const info = await td.getInfo();
      expect(Number(info.Compression)).toBe(100);
      expect(Number(info['Merged weight'])).toBe(100);

      const td2 = r.getTDigest(name2);
      await td2.create();
      await td2.add(101, 102, 103);
      await td.mergeWith(name2);
      expect(await td.getMax()).toBe(103);
      expect(await td.getMin()).toBe(1);

      await td.reset();
      expect(Number.isNaN(await td.getMin())).toBe(true);
    } finally {
      await r.getKeys().delete(name, name2);
      await r.shutdown();
    }
  }, 120000);

  it('CuckooFilter existence/count/info', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}cf`;
    try {
      if (!(await hasCmd(r.redis, 'CF.RESERVE'))) {
        console.log('[phase36] CF unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name);
      const cf = r.getCuckooFilter(name);
      await cf.init(1000);
      expect(await cf.add('a')).toBe(true);
      expect(await cf.add('b')).toBe(true);
      expect(await cf.addIfAbsent('b')).toBe(false);
      expect(await cf.addIfAbsent('c')).toBe(true);
      expect(await cf.exists('a')).toBe(true);
      expect(await cf.existsMany(['a', 'zz', 'c'])).toEqual(['a', 'c']);
      expect(await cf.count('a')).toBe(1);
      const info = await cf.getInfo();
      expect(Number(info['Number of items inserted'])).toBe(3);
      expect(await cf.remove('a')).toBe(true);
      expect(await cf.exists('a')).toBe(false);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);

  it('TopK dropped items and listWithCount', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}tk`;
    try {
      if (!(await hasCmd(r.redis, 'TOPK.RESERVE'))) {
        console.log('[phase36] TOPK unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name);
      const tk = r.getTopK(name);
      await tk.init(3);
      const dropped = await tk.add('a', 'b', 'c', 'd');
      expect(dropped.length).toBe(4);
      expect(dropped.filter((x) => x !== null).length).toBeGreaterThanOrEqual(1);

      const evicted = await tk.incrementBy('e', 5);
      expect(evicted === null || typeof evicted === 'string').toBe(true);
      expect(await tk.contains('e')).toBe(true);
      expect(await tk.count('e')).toBe(5);
      const list = await tk.list();
      expect(list.length).toBeLessThanOrEqual(3);
      const withCount = await tk.listWithCount();
      expect(withCount.get('e')).toBe(5);
      expect(withCount.size).toBeLessThanOrEqual(3);
      const info = await tk.getInfo();
      expect(Number(info.k)).toBe(3);

      await r.getKeys().delete(name);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
