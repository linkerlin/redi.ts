/**
 * Phase 32 live interop: RKeys lazy key streams, removeListener(handler)
 * callbacks for topics, and per-stream group/consumer multi-read.
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p32:';
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

describe('live interop phase 32: API polish', () => {
  it('key stream + removeListener(handler) stops deliveries', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const t = `${NS}topic`;
    try {
      await r.getKeys().delete(`${NS}a`, `${NS}b`, `${NS}c`);
      await r.redis.mset(`${NS}a`, '1', `${NS}b`, '1', `${NS}c`, '1');
      const keys: string[] = [];
      for await (const k of r.getKeys().getKeysStream(`${NS}*`)) keys.push(k);
      expect(keys.sort()).toEqual([`${NS}a`, `${NS}b`, `${NS}c`]);

      const topic = r.getTopic<string>(t);
      const seen: string[] = [];
      const cb = (m: string) => seen.push(m);
      await topic.addListener(cb);
      expect(await topic.countListeners()).toBe(1);
      await topic.removeListener(cb);
      expect(await topic.countListeners()).toBe(0);
      await topic.publish('after-remove');
      await sleep(300);
      expect(seen).toEqual([]);
      await r.getKeys().delete(`${NS}a`, `${NS}b`, `${NS}c`);
    } finally {
      await r.getKeys().delete(t);
      await r.shutdown();
    }
  }, 120000);

  it('readGroupMany accepts per-stream group/consumer specs', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}s1`;
    const s2 = `${NS}s2`;
    try {
      await r.getKeys().delete(s1, s2);
      const a = r.getStream(s1);
      const b = r.getStream(s2);
      await a.add({ m: 'x' });
      await b.add({ m: 'y' });
      await a.createGroup('g1', '0');
      await b.createGroup('g2', '0');
      const read = await a.readGroupMany({
        [s1]: { group: 'g1', consumer: 'c1', id: '>' },
        [s2]: { group: 'g2', consumer: 'c2', id: '>' },
      }, 10);
      expect(read.get(s1)?.map((e) => e.fields.m)).toEqual(['x']);
      expect(read.get(s2)?.map((e) => e.fields.m)).toEqual(['y']);
      const again = await a.readGroupMany({
        [s1]: { group: 'g1', consumer: 'c1', id: '>' },
        [s2]: { group: 'g2', consumer: 'c2', id: '>' },
      }, 10);
      expect(again.size).toBe(0);
    } finally {
      await r.getKeys().delete(s1, s2);
      await r.shutdown();
    }
  }, 120000);
});
