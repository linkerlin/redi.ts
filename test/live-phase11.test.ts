/**
 * Phase 11 live interop: RScoredSortedSet set operations (union/intersection/
 * diff store + read + count). Store arg order mirrors Redisson exactly:
 * STORE computes over the OTHER sets into SELF; reads include SELF.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p11:';
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


async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();

describe('live interop phase 11: zset union/intersection/diff', () => {
  it('store ops land in Java reads (incl. WEIGHTS position)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const za = `${NS}za`;
    const zb = `${NS}zb`;
    const zc = `${NS}zc`;
    const zf = `${NS}zf`;
    const zg = `${NS}zg`;
    const zh = `${NS}zh`;
    const zi = `${NS}zi`;
    const zd = `${NS}zd`;
    try {
      await cleanExact(r, za, zb, zc, zf, zg, zh, zi, zd);
      const a = r.getScoredSortedSet<string>(za);
      await a.add(1, 'a');
      await a.add(2, 'b');
      const b = r.getScoredSortedSet<string>(zb);
      await b.add(10, 'b');
      await b.add(3, 'c');
      // union of OTHERS into self (self is not an input — Java semantics)
      expect(await r.getScoredSortedSet<string>(zc).union(za, zb)).toBe(3);
      expect(await probe(`zset-score ${zc} a`)).toBe('ZSCORE:1.0');
      expect(await probe(`zset-score ${zc} b`)).toBe('ZSCORE:12.0');
      expect(await probe(`zset-score ${zc} c`)).toBe('ZSCORE:3.0');
      // weighted: x:1*2 + 10*3 = 32, y:1*3 = 3
      await r.getScoredSortedSet<string>(zf).add(1, 'x');
      await r.getScoredSortedSet<string>(zg).add(10, 'x');
      await r.getScoredSortedSet<string>(zg).add(1, 'y');
      expect(await r.getScoredSortedSet<string>(zh).unionWith({ names: [zf, zg], weights: [2, 3] })).toBe(2);
      expect(await probe(`zset-score ${zh} x`)).toBe('ZSCORE:32.0');
      expect(await probe(`zset-score ${zh} y`)).toBe('ZSCORE:3.0');
      // intersection of others into self
      expect(await r.getScoredSortedSet<string>(zi).intersection(za, zb)).toBe(1);
      expect(await probe(`zset-score ${zi} b`)).toBe('ZSCORE:12.0');
      expect(await probe(`zset-score ${zi} a`)).toBe('ZSCORE:null');
      // difference of others into self
      expect(await r.getScoredSortedSet<string>(zd).diff(za, zb)).toBe(1);
      expect(await probe(`zset-score ${zd} a`)).toBe('ZSCORE:1.0');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('read variants + count (TS-side, Java cross-checks members)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const za = `${NS}ra`;
    const zb = `${NS}rb`;
    try {
      await cleanExact(r, za, zb);
      const a = r.getScoredSortedSet<string>(za);
      await a.add(1, 'a');
      await a.add(2, 'b');
      const b = r.getScoredSortedSet<string>(zb);
      await b.add(10, 'b');
      await b.add(3, 'c');
      expect(await a.readUnion(zb)).toEqual(['a', 'c', 'b']);
      expect(await a.readUnionEntries(zb)).toEqual([
        { value: 'a', score: 1 },
        { value: 'c', score: 3 },
        { value: 'b', score: 12 },
      ]);
      expect(await a.readIntersection(zb)).toEqual(['b']);
      expect(await a.readIntersectionEntries(zb)).toEqual([{ value: 'b', score: 12 }]);
      expect(await a.countIntersection(zb)).toBe(1);
      expect(await a.countIntersectionLimit(1, zb)).toBe(1);
      expect(await a.readDiff(zb)).toEqual(['a']);
      expect(await a.readDiffEntries(zb)).toEqual([{ value: 'a', score: 1 }]);
      // Java decodes the same TS-written members through its own readUnion
      expect(await probe(`zset-readunion ${za} ${zb}`)).toBe('READUNION:[a, c, b]');
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
