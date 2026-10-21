/**
 * Phase 66 live: probabilistic advanced argument objects —
 * TDigestMergeArgs, CuckooFilterInitArgs/CuckooFilterAddArgs and TopKInitArgs,
 * plus TopK batch increment/contains/count.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import {
  Redisson, TDigestMergeArgs, CuckooFilterInitArgs, CuckooFilterAddArgs, TopKInitArgs,
} from '../src';

const NS = 'TsJavaT:p66:';
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

async function hasCmd(redis: Redis, cmd: string): Promise<boolean> {
  try {
    const info: any = await (redis as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(info) && info.length > 0 && info[0] !== null && info[0] !== undefined;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();

describe('live phase 66: probabilistic argument objects', () => {
  it('supports TDigest merge, Cuckoo init/add and TopK init/batches', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const d1 = `${NS}td1`;
    const d2 = `${NS}td2`;
    const cf = `${NS}cf`;
    const tk = `${NS}tk`;
    try {
      await r.getKeys().delete(d1, d2, cf, tk);

      if (await hasCmd(r.redis, 'TDIGEST.MERGE')) {
        const a = r.getTDigest(d1);
        const b = r.getTDigest(d2);
        await a.create(50);
        await b.create(50);
        await a.add(1, 2, 3);
        await b.add(10, 20);
        await a.mergeWith({ keys: [d2], compression: 100, override: true });
        expect(await a.getMin()).toBe(10);
        expect(await a.getMax()).toBe(20);
        const q = await a.quantile(0.5);
        expect(q[0]).toBeGreaterThanOrEqual(10);
        expect(q[0]).toBeLessThanOrEqual(20);
      } else {
        console.log('[phase66] TDIGEST unsupported — skipping merge args');
      }

      if (await hasCmd(r.redis, 'CF.RESERVE')) {
        const filter = r.getCuckooFilter(cf);
        await filter.init({ capacity: 100, bucketSize: 2, maxIterations: 20, expansion: 2 });
        const info = await filter.getInfo();
        expect(Number(info['Number of buckets'] ?? info.buckets ?? 0)).toBeGreaterThan(0);
        expect(await filter.add(CuckooFilterAddArgs.items(['a', 'b', 'c']))).toEqual(['a', 'b', 'c']);
        expect(await filter.addIfAbsent(CuckooFilterAddArgs.items(['a', 'd']))).toEqual(['d']);
        expect(await filter.exists('a')).toBe(true);
        expect(await filter.count('a')).toBeGreaterThanOrEqual(1);
      } else {
        console.log('[phase66] CuckooFilter unsupported — skipping args');
      }

      if (await hasCmd(r.redis, 'TOPK.RESERVE')) {
        const top = r.getTopK(tk);
        await top.init({ topK: 2, width: 8 });
        expect((await top.add('a', 'b', 'c')).length).toBe(3);
        const evicted = await top.incrementBy(new Map([['a', 5], ['d', 3]]));
        expect(evicted.length).toBe(2);
        expect(await top.contains(['a', 'zzz'])).toEqual([true, false]);
        const counts = await top.count(['a', 'zzz']);
        expect(counts[0]).toBeGreaterThanOrEqual(1);
        expect(counts[1]).toBe(0);
        expect(await top.list()).toContain('a');
      } else {
        console.log('[phase66] TopK unsupported — skipping args');
      }
    } finally {
      await r.getKeys().delete(d1, d2, cf, tk);
      await r.shutdown();
    }
  }, 120000);
});
