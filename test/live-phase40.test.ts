/**
 * Phase 40 live: RJsonBuckets batch MSET/MGET and RMapCacheNative conditional
 * batch writes + batch field expiry (Date/Instant), with Java map reads.
 *
 * Needs real Redis (JSON module for the first case); capability-gated.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p40:';
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

describe('live interop phase 40: json buckets + native map-cache batch', () => {
  it('RJsonBuckets MSET/MGET round-trip', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}ja`;
    const b = `${NS}jb`;
    try {
      if (!(await hasCmd(r.redis, 'JSON.SET'))) {
        console.log('[phase40] JSON unsupported — skipping');
        return;
      }
      await r.getKeys().delete(a, b);
      const buckets = r.getJsonBuckets();
      await buckets.set({ [a]: { x: 1 }, [b]: { y: 2 } });
      const got = await buckets.get(a, b, `${NS}missing`);
      expect(got.size).toBe(2);
      expect(got.get(a)).toEqual({ x: 1 });
      expect(got.get(b)).toEqual({ y: 2 });
      expect(await r.getJsonBucket<any>(a).get()).toEqual({ x: 1 });
      // path-scoped batch read
      const pathGot = await buckets.getWithPath('$.x', a);
      expect(pathGot.get(a)).toEqual([1]);
    } finally {
      await r.getKeys().delete(a, b);
      await r.shutdown();
    }
  }, 120000);

  it('RMapCacheNative conditional batch + Date expiry, Java reads fields', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn`;
    try {
      await r.getKeys().delete(name);
      await probe(`map-get ${name} warm`);
      const mc = r.getMapCacheNative<string, string>(name);
      await mc.put('a', '1');
      expect(await mc.putIfAllKeysExist({ b: '2' })).toBe(false);
      expect(await mc.get('b')).toBeNull();

      expect(await mc.putIfAllKeysAbsent({ b: '2', c: '3' }, 5000)).toBe(true);
      expect(await mc.remainTTL('b')).toBeGreaterThan(0);
      expect(await mc.putIfAllKeysAbsent({ b: '9' })).toBe(false);
      expect(await mc.get('b')).toBe('2');

      expect(await mc.putIfAllKeysExist({ a: '9', b: '8' }, new Date(Date.now() + 6000))).toBe(true);
      expect(await mc.remainTTL('a')).toBeGreaterThan(0);
      expect(await probe(`map-get ${name} a`)).toBe('MAP_GET:9');
      expect(await probe(`map-get ${name} b`)).toBe('MAP_GET:8');

      expect(await mc.expireEntries(['a', 'b', 'ghost'], new Date(Date.now() + 9000))).toBe(2);
      expect(await mc.remainTTL('a')).toBeGreaterThan(7000);
      expect(await mc.expireEntriesIfGreater(['a'], 20000)).toBe(1);
      expect(await mc.remainTTL('a')).toBeGreaterThan(15000);
      expect(await mc.expireEntriesIfLess(['a'], 1000)).toBe(1);
      expect(await mc.remainTTL('a')).toBeLessThan(2000);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
