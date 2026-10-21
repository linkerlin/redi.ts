/**
 * Phase 51 live: RMapCacheNative structured PutArgs (HSETEX KEEPTTL/PX/PXAT,
 * FXX/FNX) and compute/computeIfAbsent with field TTLs, plus Java map reads.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p51:';
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

describe('live phase 51: native map-cache PutArgs + compute', () => {
  it('structured batches keep field TTLs and compute serializes', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mn`;
    try {
      if (!(await hasCmd(r.redis, 'HSETEX'))) {
        console.log('[phase51] HSETEX unsupported — skipping structured batch assertions');
        return;
      }
      await r.getKeys().delete(name);
      const mn = r.getMapCacheNative<string, number>(name);

      await mn.putAll({ a: 1, b: 2 }, 5000);
      expect(await mn.get('a')).toBe(1);
      expect(await mn.remainTTL('a')).toBeGreaterThan(0);
      expect(await probe(`map-get ${name} a`)).toBe('MAP_GET:1');

      await mn.putAll({ entries: { a: 10 }, keepTTL: true });
      expect(await mn.get('a')).toBe(10);
      expect(await mn.remainTTL('a')).toBeGreaterThan(0);

      await mn.putAll({ entries: { c: 3 }, expireAt: new Date(Date.now() + 6000) });
      expect(await mn.remainTTL('c')).toBeGreaterThan(0);
      await mn.putAll({ entries: { d: 4 } });
      expect(await mn.remainTTL('d')).toBe(-1);

      expect(await mn.putIfAllKeysExist({ entries: { a: 11, b: 22 }, timeToLiveMs: 5000 })).toBe(true);
      expect(await mn.get('a')).toBe(11);
      expect(await mn.remainTTL('a')).toBeGreaterThan(0);
      expect(await mn.putIfAllKeysAbsent({ entries: { a: 1 } })).toBe(false);
      expect(await mn.putIfAllKeysAbsent({ entries: { e: 5, f: 6 }, timeToLiveMs: 5000 })).toBe(true);
      expect(await mn.remainTTL('e')).toBeGreaterThan(0);
      expect(await mn.putIfAllKeysExist({ entries: { a: 1, ghost: 2 } })).toBe(false);
      expect(await mn.get('a')).toBe(11);

      expect(await mn.compute('n', 5000, (_k, old) => (old ?? 0) + 1)).toBe(1);
      expect(await mn.remainTTL('n')).toBeGreaterThan(0);
      expect(await mn.compute('n', 5000, (_k, old) => (old ?? 0) + 1)).toBe(2);
      expect(await mn.compute('n', 5000, () => null)).toBeNull();
      expect(await mn.get('n')).toBeNull();

      const [v1, v2] = await Promise.all([
        mn.computeIfAbsent('m', 5000, async () => 7),
        mn.computeIfAbsent('m', 5000, async () => 8),
      ]);
      expect(v1).toBe(7);
      expect(v2).toBe(7);
      expect(await mn.remainTTL('m')).toBeGreaterThan(0);
      expect(await mn.computeIfAbsent('m', 5000, () => 9)).toBe(7);
      expect(await probe(`map-get ${name} m`)).toBe('MAP_GET:7');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
