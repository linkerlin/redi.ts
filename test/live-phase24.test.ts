/**
 * Phase 24 live interop: RScoredSortedSet remainder — addAll family with
 * NX/XX/GT/LT flags (scores read back by Java), first/last entry, random and
 * randomEntries (ZRANDMEMBER), non-blocking + blocking entry pops
 * (ZPOPMIN/MAX, BZPOPMIN/MAX) and BZMPOP across keys.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p24:';
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
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

describe('live interop phase 24: scored-sorted-set remainder', () => {
  it('addAll family + first/last/random, Java reads the scores', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const zn = `${NS}z`;
    try {
      await r.getKeys().delete(zn);
      // warm the resident JVM before the assertions
      await probe(`zset-score ${zn} warm`);
      const z = r.getScoredSortedSet<string>(zn);
      expect(await z.addAll(new Map([['a', 1], ['b', 2]]))).toBe(2);
      expect(await z.addAll(new Map([['a', 5], ['c', 3]]))).toBe(2);

      // NX skips existing; XX skips missing
      expect(await z.addAllIfAbsent(new Map([['a', 9], ['d', 4]]))).toBe(1);
      expect(await z.addAllIfExist(new Map([['a', 6], ['ghost', 1]]))).toBe(1);

      // GT/LT update only in the right direction; both add missing members
      expect(await z.addAllIfGreater(new Map([['a', 7], ['e', 1]]))).toBe(2);
      expect(await z.addAllIfGreater(new Map([['a', 6], ['e', 0]]))).toBe(0);
      expect(await z.addAllIfLess(new Map([['b', 3], ['f', 0]]))).toBe(1);
      expect(await z.addAllIfLess(new Map([['b', 1], ['f', 9]]))).toBe(1); // b lowered; f unchanged

      for (const [m, s] of [['a', 7], ['b', 1], ['c', 3], ['d', 4], ['e', 1], ['f', 0]] as const) {
        expect(await probe(`zset-score ${zn} ${m}`)).toBe(`ZSCORE:${s}.0`);
      }
      expect(await z.firstEntry()).toEqual({ value: 'f', score: 0 });
      expect(await z.lastEntry()).toEqual({ value: 'a', score: 7 });

      const one = await z.random();
      expect(['a', 'b', 'c', 'd', 'e', 'f']).toContain(one);
      const some = await z.random(3);
      expect(some).toHaveLength(3);
      expect(new Set(some).size).toBe(3);
      const entries = await z.randomEntries(3);
      expect(entries.size).toBe(3);
      for (const [m, s] of entries) expect(await probe(`zset-score ${zn} ${m}`)).toBe(`ZSCORE:${s}.0`);

      expect(await z.pollFirstEntry()).toEqual({ value: 'f', score: 0 });
      expect(await z.pollLastEntry()).toEqual({ value: 'a', score: 7 });
      expect(await z.pollFirstEntry()).toEqual({ value: 'b', score: 1 }); // lexicographic tie with e
    } finally {
      await r.getKeys().delete(zn);
      await r.shutdown();
    }
  }, 120000);

  it('blocking BZPOPMIN/BZPOPMAX and BZMPOP wake on new members', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const bn = `${NS}b`;
    const an = `${NS}a`;
    const o1 = `${NS}o1`;
    try {
      await r.getKeys().delete(bn, an, o1);
      const z = r.getScoredSortedSet<string>(bn);
      const pending = z.pollFirstWithTimeout(3000);
      await sleep(200);
      await z.add(7, 'wake');
      expect(await pending).toEqual({ value: 'wake', score: 7 });
      expect(await z.pollFirstWithTimeout(0)).toBeNull();

      const lp = z.pollLastWithTimeout(3000);
      await sleep(200);
      await z.add(9, 'late');
      expect(await lp).toEqual({ value: 'late', score: 9 });

      // BZMPOP: source key is reported; own key wins when populated
      const a1 = r.getScoredSortedSet<string>(an);
      const o = r.getScoredSortedSet<string>(o1);
      const anyPending = a1.pollFirstFromAny(3000, o1);
      await sleep(200);
      await o.add(4, 'other');
      expect(await anyPending).toEqual({ key: o1, value: 'other', score: 4 });
      await a1.add(1, 'own');
      await o.add(2, 'other2');
      expect(await a1.pollFirstFromAny(0, o1)).toEqual({ key: an, value: 'own', score: 1 });
      expect(await a1.pollLastFromAny(0, o1)).toEqual({ key: o1, value: 'other2', score: 2 });
      expect(await a1.pollLastFromAny(0, o1)).toBeNull();
    } finally {
      await r.getKeys().delete(bn, an, o1);
      await r.shutdown();
    }
  }, 120000);
});
