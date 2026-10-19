/**
 * Phase 18 live interop: 64-bit Long precision (bucket + atomic CAS),
 * AtomicDouble getAndAdd and atomic queue drainTo.
 *
 * Needs real Redis + Java probe; skips silently when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, JsonCodec } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p18:';
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

describe('live interop phase 18: 64-bit precision + atomic drainTo', () => {
  it('Long.MAX_VALUE / Long.MIN_VALUE buckets round-trip with BigInt codec', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const big = new JsonCodec({ bigIntForUnsafeLong: true });
    const name = `${NS}long:max`;
    try {
      await cleanExact(r, name);
      await r.getBucket<bigint>(name, big).set(9223372036854775807n);
      expect(await probe(`bucket-get-long ${name}`)).toBe('BUCKET_LONG:9223372036854775807');
      expect(await r.getBucket<bigint>(name, big).get()).toBe(9223372036854775807n);

      expect(await probe(`bucket-set-long ${name} -9223372036854775808`)).toBe('BUCKET_LONG_SET');
      expect(await r.getBucket<bigint>(name, big).get()).toBe(-9223372036854775808n);
    } finally {
      await cleanExact(r, name);
      await r.shutdown();
    }
  }, 180000);

  it('AtomicLong CAS compares decimal strings, not IEEE-754 doubles', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}atomic:long`;
    try {
      await cleanExact(r, name);
      const a = r.getAtomicLong(name);
      await a.set(9223372036854775806n);
      // Number(9223372036854775807) rounds to ...76000; must NOT match.
      expect(await a.compareAndSet(9223372036854775807, 1)).toBe(false);
      expect(await a.compareAndSet(9223372036854775806n, 9223372036854775807n)).toBe(true);
      expect(await probe(`atomic-get ${name}`)).toBe('ATOMIC:9223372036854775807');

      expect(await probe(`atomic-cas-long ${name} 9223372036854775807 9223372036854775806`)).toBe('ATOMIC_CAS:true');
      expect(await a.get()).toBe(9223372036854775806n);

      expect(await probe(`atomic-set-long ${name} 9223372036854775807`)).toBe('ATOMIC_SET_OK');
      expect(await a.get()).toBe(9223372036854775807n);
    } finally {
      await cleanExact(r, name);
      await r.shutdown();
    }
  }, 180000);

  it('AtomicDouble getAndAdd returns the exact stored old value', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}atomic:double`;
    try {
      await cleanExact(r, name);
      const d = r.getAtomicDouble(name);
      await d.set(0.1);
      expect(await d.getAndAdd(0.2)).toBe(0.1);
      expect(await d.get()).toBeCloseTo(0.3);
      // A second call must also report the exact previously stored value.
      expect(await d.getAndAdd(0.1)).toBeCloseTo(0.3);
    } finally {
      await cleanExact(r, name);
      await r.shutdown();
    }
  }, 120000);

  it('drainTo moves values atomically and target reads match Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}queue:src`;
    const dest = `${NS}queue:dest`;
    try {
      await cleanExact(r, src, dest);
      const q = r.getBlockingQueue<number>(src);
      await q.offer(1);
      await q.offer(2);
      await q.offer(3);
      const target = r.getQueue<number>(dest);
      expect(await q.drainTo(target)).toBe(3);
      expect(await q.size()).toBe(0);
      expect(await target.readAll()).toEqual([1, 2, 3]);
      expect(await probe(`deque-read ${dest}`)).toBe('DEQUE:[1, 2, 3]');
    } finally {
      await cleanExact(r, src, dest);
      await r.shutdown();
    }
  }, 120000);
});
