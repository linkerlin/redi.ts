/**
 * Phase 38 live: lazy SSCAN/ZSCAN iterators (encoded-name MATCH) and
 * RLocalCachedMap.preloadCache(count).
 *
 * Needs real Redis; skips when Redis is unavailable (no Java probe needed).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p38:';
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

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 38: iterators + preload', () => {
  it('SSCAN/ZSCAN iterators and preloadCache(count)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const sn = `${NS}set`;
    const zn = `${NS}zset`;
    const ln = `${NS}lcm`;
    try {
      await r.getKeys().delete(sn, zn, ln);
      const s = r.getSet<string>(sn);
      await s.add('a', 'ab', 'b');
      const all: string[] = [];
      for await (const v of s.iterator()) all.push(v);
      expect(all.sort()).toEqual(['a', 'ab', 'b']);
      const aPrefix: string[] = [];
      for await (const v of s.iterator('"a*')) aPrefix.push(v);
      expect(aPrefix.sort()).toEqual(['a', 'ab']);

      const z = r.getScoredSortedSet<string>(zn);
      await z.add(1, 'a');
      await z.add(2, 'b');
      const values: string[] = [];
      for await (const v of z.stream()) values.push(v);
      expect(values.sort()).toEqual(['a', 'b']);
      const entries: { value: string; score: number }[] = [];
      for await (const e of z.entriesIterator()) entries.push(e);
      expect(entries.sort((x, y) => x.score - y.score)).toEqual([
        { value: 'a', score: 1 },
        { value: 'b', score: 2 },
      ]);
      const zPrefixed: string[] = [];
      for await (const v of z.iterator('"b*')) zPrefixed.push(v);
      expect(zPrefixed).toEqual(['b']);

      const lcm = r.getLocalCachedMap<string, string>(ln, { maxSize: 10 });
      await lcm.put('a', '1');
      await lcm.put('b', '2');
      await lcm.put('c', '3');
      await lcm.clearLocalCache();
      await lcm.preloadCache(2);
      expect(lcm.getCachedMap().size).toBe(2);
      await lcm.clearLocalCache();
      await lcm.preloadCache();
      expect(lcm.getCachedMap().size).toBe(3);
    } finally {
      await r.getKeys().delete(sn, zn, ln);
      await r.shutdown();
    }
  }, 120000);
});
