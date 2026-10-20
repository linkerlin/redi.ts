/**
 * Phase 42 live: RMapCacheNative fast writes — fastPut, fastPutIfAbsent,
 * putIfExist, fastPutIfExist with relative/absolute field TTLs, plus a Java
 * map read to confirm the wire shape.
 *
 * Needs real Redis (HEXPIRE family for TTL assertions); skips when absent.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p42:';
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

describe('live interop phase 42: native map-cache fast writes', () => {
  it('fast paths honour existence guards and field TTLs; Java reads back', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn`;
    try {
      await r.getKeys().delete(name);
      await probe(`map-get ${name} warm`);
      const mc = r.getMapCacheNative<string, string>(name);

      expect(await mc.fastPut('k', 'v1', 5000)).toBe(true);
      expect(await mc.get('k')).toBe('v1');
      expect(await mc.fastPut('k', 'v1b')).toBe(false);
      expect(await probe(`map-get ${name} k`)).toBe('MAP_GET:v1b');
      const fieldTtl = await hasCmd(r.redis, 'HPEXPIRE');
      // HSET on an existing field clears its TTL (same as Java's plain fastPut path)
      if (fieldTtl) expect(await mc.remainTTL('k')).toBe(-1);

      expect(await mc.fastPutIfAbsent('k', 'v2')).toBe(false);
      expect(await mc.get('k')).toBe('v1b');
      expect(await mc.fastPutIfAbsent('new', 'n1', new Date(Date.now() + 6000))).toBe(true);
      if (fieldTtl) expect(await mc.remainTTL('new')).toBeGreaterThan(0);

      expect(await mc.putIfExist('k', 'v3', 7000)).toBe('v1b');
      if (fieldTtl) expect(await mc.remainTTL('k')).toBeGreaterThan(0);
      expect(await mc.get('k')).toBe('v3');
      expect(await mc.putIfExist('ghost', 'x')).toBeNull();
      expect(await mc.get('ghost')).toBeNull();
      expect(await mc.fastPutIfExist('k', 'v4', 7000)).toBe(true);
      expect(await mc.get('k')).toBe('v4');
      expect(await mc.fastPutIfExist('ghost2', 'x')).toBe(false);
      expect(await mc.get('ghost2')).toBeNull();
      if (fieldTtl) expect(await mc.remainTTL('k')).toBeGreaterThan(0);

      expect(await probe(`map-get ${name} k`)).toBe('MAP_GET:v4');
      expect(await probe(`map-get ${name} new`)).toBe('MAP_GET:n1');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
