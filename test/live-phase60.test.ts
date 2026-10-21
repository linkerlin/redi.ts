/**
 * Phase 60 live: RAtomicLong/AtomicDouble compareAndDelete with comparison
 * conditions (< <= > >= == ~=), plus the existing equality overload.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, CompareAndDeleteArgs } from '../src';

const NS = 'TsJavaT:p60:';
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

describe('live phase 60: conditional compareAndDelete', () => {
  it('deletes only when the comparison matches', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ln = `${NS}l`;
    const dn = `${NS}d`;
    try {
      await r.getKeys().delete(ln, dn);
      const al = r.getAtomicLong(ln);
      await al.set(5);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.lessOrEqual(5))).toBe(true);
      expect(await al.get()).toBe(0);

      await al.set(5);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.less(5))).toBe(false);
      expect(await al.get()).toBe(5);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.notEqual(5))).toBe(false);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.equal(5))).toBe(true);
      expect(await al.get()).toBe(0);

      const ad = r.getAtomicDouble(dn);
      await ad.set(1.5);
      expect(await ad.compareAndDelete(CompareAndDeleteArgs.greaterOrEqual(1.5))).toBe(true);
      expect(await ad.get()).toBe(0);
      await ad.set(1.5);
      expect(await ad.compareAndDelete(CompareAndDeleteArgs.greater(2))).toBe(false);
      expect(await ad.get()).toBe(1.5);
      expect(await ad.compareAndDelete(1.5)).toBe(true);
      expect(await ad.get()).toBe(0);
    } finally {
      await r.getKeys().delete(ln, dn);
      await r.shutdown();
    }
  }, 120000);
});
