/**
 * Phase 58 live: RMapCacheNative batch field TTLs — HPTTL FIELDS n and
 * HPERSIST FIELDS n, including -2 missing and no-TTL (-1) values.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p58:';
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

describe('live phase 58: native map-cache batch TTLs', () => {
  it('reads and clears field TTLs in one batch', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mn`;
    try {
      await r.getKeys().delete(name);
      const mn = r.getMapCacheNative<string, string>(name);
      await mn.put('a', '1', 5000);
      await mn.put('b', '2');

      const ttls = await mn.remainTimeToLive(['a', 'b', 'missing']);
      expect(ttls.size).toBe(3);
      expect(ttls.get('a')).toBeGreaterThan(0);
      expect(ttls.get('b')).toBe(-1);
      expect(ttls.get('missing')).toBe(-2);

      const cleared = await mn.clearExpire(['a', 'b', 'missing']);
      expect(cleared.get('a')).toBe(true);
      expect(cleared.get('b')).toBe(false);
      expect(cleared.get('missing')).toBe(false);
      expect(await mn.remainTimeToLive('a')).toBe(-1);

      expect((await mn.remainTimeToLive([])).size).toBe(0);
      expect((await mn.clearExpire([])).size).toBe(0);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
