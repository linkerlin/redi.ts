/**
 * Phase 59 live: RMap / RMapCacheNative field listeners via keyspace events
 * (hset/hdel/hincrbyfloat/hexpired/hpersist). Redis 8.6 has no
 * `__subkeyevent`, so the field is null (Java's fallback path).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p59:';
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

const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
const waitFor = async (check: () => boolean, timeoutMs = 5000): Promise<boolean> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return true;
    await sleep(20);
  }
  return check();
};

const hasRedis = await redisUp();

describe('live phase 59: map field listeners', () => {
  it('delivers put/incr/remove and native expired/clear-expire events', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const mapName = `${NS}map`;
    const nativeName = `${NS}native`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(mapName, nativeName);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase59] notify-keyspace-events not configurable — skipping');
      }
      if (!canConfig) return;

      const m = r.getMap<string, string>(mapName);
      const events: string[] = [];
      const mid = await m.addListener({
        onPut: (_key, field) => events.push(`put:${String(field)}`),
        onIncrement: (_key, field) => events.push(`incr:${String(field)}`),
        onRemove: (_key, field) => events.push(`remove:${String(field)}`),
      });
      await m.put('a', '1');
      await r.redis.hincrbyfloat(mapName, 'n', 1);
      await m.remove('a');
      expect(await waitFor(() => events.includes('put:null') && events.includes('incr:null') && events.includes('remove:null'))).toBe(true);
      const before = events.length;
      await m.removeListener(mid);
      await m.put('b', '2');
      await sleep(300);
      expect(events.length).toBe(before);

      const mn = r.getMapCacheNative<string, string>(nativeName);
      const nativeEvents: string[] = [];
      const nid = await mn.addListener({
        onPut: () => nativeEvents.push('put'),
        onExpired: () => nativeEvents.push('expired'),
        onClearExpire: () => nativeEvents.push('clear'),
      });
      await mn.put('e', 'v', 100);
      expect(await waitFor(() => nativeEvents.includes('expired'))).toBe(true);
      await mn.put('c', 'v', 5000);
      expect(await mn.clearExpire('c')).toBe(true);
      expect(await waitFor(() => nativeEvents.includes('clear'))).toBe(true);
      expect(nativeEvents).toContain('put');
      await mn.removeListener(nid);
    } finally {
      await r.getKeys().delete(mapName, nativeName).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
