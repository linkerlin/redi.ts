/**
 * Phase 61 live: IncrementArgs Lua-CAS emulation (INCREX is not available on
 * vanilla Redis) — bounds/saturate, BigInt exactness, TTL/PERSIST/ENX and
 * concurrent atomic increments.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p61:';
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

const hasRedis = await redisUp();

describe('live phase 61: IncrementArgs', () => {
  it('applies bounds, saturation, exact BigInt and TTL policies', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ln = `${NS}l`;
    const dn = `${NS}d`;
    try {
      await r.getKeys().delete(ln, dn);
      const al = r.getAtomicLong(ln);
      await al.set(8);
      expect(await al.incrementAndGet({ by: 5, lowerBound: 0, upperBound: 10, saturate: true })).toBe(10);
      expect(await al.incrementAndGet({ by: 5, upperBound: 10, saturate: true })).toBe(10);
      await al.set(9);
      expect(await al.incrementAndGet({ by: 5, upperBound: 10 })).toBe(9);
      expect(await al.get()).toBe(9);

      const big = 9007199254740993n;
      await al.set(big);
      expect(await al.incrementAndGet({ by: 1 })).toBe(big + 1n);

      await al.set(0);
      await Promise.all(Array.from({ length: 20 }, () => al.incrementAndGet({ by: 1 })));
      expect(await al.get()).toBe(20);

      await al.incrementAndGet({ timeToLiveMs: 5000 });
      expect(await r.redis.pttl(ln)).toBeGreaterThan(0);
      const ttlBefore = await r.redis.pttl(ln);
      await al.incrementAndGet({ timeToLiveMs: 9000, expireIfNotSet: true });
      const ttlAfter = await r.redis.pttl(ln);
      expect(ttlAfter).toBeGreaterThan(0);
      expect(ttlAfter).toBeLessThanOrEqual(ttlBefore);
      await al.incrementAndGet({ persist: true });
      expect(await r.redis.pttl(ln)).toBe(-1);
      await al.incrementAndGet({ expireAt: new Date(Date.now() + 6000) });
      expect(await r.redis.pttl(ln)).toBeGreaterThan(0);

      const ad = r.getAtomicDouble(dn);
      await ad.set(1.5);
      expect(await ad.incrementAndGet({ by: 0.25, lowerBound: 0, upperBound: 1.6, saturate: true })).toBe(1.6);
      await ad.set(1.5);
      expect(await ad.incrementAndGet({ by: 1, upperBound: 2 })).toBe(1.5);
      expect(await ad.incrementAndGet({ by: 0.5, lowerBound: 0, upperBound: 2 })).toBe(2);
    } finally {
      await r.getKeys().delete(ln, dn);
      await r.shutdown();
    }
  }, 120000);
});
