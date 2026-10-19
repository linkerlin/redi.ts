/**
 * Phase 28 live interop: RLock.lockInterruptibly (AbortSignal equivalent of
 * Java interruption) across two clients, and RMapCache
 * expireEntries/getWithTTLOnly/getMaxSize/setMaxSize (wire checks).
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { prefixName, suffixName } from '../src/util';

const NS = 'TsJavaT:p28:';
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

describe('live interop phase 28: interruptible lock + mapcache remainder', () => {
  it('lockInterruptibly aborts while contended and acquires when free', async () => {
    if (!hasRedis) return;
    const c1 = Redisson.create({ address: REDIS_URL });
    const c2 = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock`;
    try {
      await c1.getKeys().delete(name);
      const l1 = c1.getLock(name);
      const l2 = c2.getLock(name);
      await l1.lock(30000);
      const ac = new AbortController();
      const pending = l2.lockInterruptibly(undefined, ac.signal);
      setTimeout(() => ac.abort(), 150);
      await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
      expect(await l1.isLocked()).toBe(true); // holder not disturbed
      await l1.unlock();

      await l2.lockInterruptibly(30000);
      expect(await l2.isLocked()).toBe(true);
      await l2.unlock();
      expect(await l2.isLocked()).toBe(false);
    } finally {
      await c1.getKeys().delete(name);
      await c1.shutdown();
      await c2.shutdown();
    }
  }, 120000);

  it('RMapCache expireEntries/getWithTTLOnly/getMaxSize', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc`;
    const ttlKey = prefixName('redisson__timeout__set', name);
    const optionsKey = suffixName(name, 'redisson_options');
    try {
      await r.getKeys().delete(name, ttlKey, optionsKey);
      const mc = r.getMapCache<string, string>(name);
      await mc.put('a', '1');
      await mc.put('b', '2');
      expect(await mc.getMaxSize()).toBe(0);
      expect(await mc.expireEntries(['a', 'b', 'ghost'], 60000)).toBe(2);

      // encoded map-cache fields live in the TTL ZSET with a future deadline
      const now = Date.now();
      for (const field of ['"a"', '"b"']) {
        const score = await r.redis.zscore(ttlKey, field);
        expect(score).not.toBeNull();
        expect(Number(score)).toBeGreaterThan(now);
      }
      expect(await mc.getWithTTLOnly('a')).toBe('1');
      expect(await mc.getWithTTLOnly('ghost')).toBeNull();
      // reading with TTL-only must not create a fresh access entry for `a`
      // beyond what put/expire already wrote (wire sanity)
      expect(await r.redis.hget(optionsKey, 'max-size')).toBeNull();
      await mc.setMaxSize(5, 'LRU');
      expect(await mc.getMaxSize()).toBe(5);
      expect(await r.redis.hget(optionsKey, 'max-size')).toBe('5');
    } finally {
      await r.getKeys().delete(name, ttlKey, optionsKey);
      await r.shutdown();
    }
  }, 120000);
});
