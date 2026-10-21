/**
 * Phase 63 live: RLocalCachedMap reconnection strategies — CLEAR drops the
 * near cache after the invalidation subscription reconnects; LOAD refreshes
 * locally cached fields from Redis (equivalent to Java's log replay).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p63:';
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
const waitFor = async (check: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<boolean> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(20);
  }
  return check();
};

const hasRedis = await redisUp();

describe('live phase 63: local cached map reconnection strategies', () => {
  it('clears or reloads the near cache on reconnect', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const r2 = Redisson.create({ address: REDIS_URL });
    const clearName = `${NS}clear`;
    const loadName = `${NS}load`;
    try {
      await r.getKeys().delete(clearName, loadName);

      const clearMap = r.getLocalCachedMap<string, string>(clearName, { reconnectionStrategy: 'CLEAR' });
      await clearMap.put('a', '1');
      expect(await clearMap.get('a')).toBe('1');
      expect(clearMap.getCachedMap().size).toBe(1);
      const clearSub: any = (clearMap as any).sub;
      expect(clearSub).toBeTruthy();
      clearSub.disconnect();
      await sleep(80);
      await clearSub.connect();
      expect(await waitFor(() => clearMap.getCachedMap().size === 0)).toBe(true);
      expect(await clearMap.get('a')).toBe('1');

      const loadMap = r.getLocalCachedMap<string, string>(loadName, { reconnectionStrategy: 'LOAD' });
      await loadMap.put('a', '1');
      expect(await loadMap.get('a')).toBe('1');
      expect(loadMap.getCachedMap().size).toBe(1);
      const loadSub: any = (loadMap as any).sub;
      loadSub.disconnect();
      await sleep(80);
      // Write while the invalidation channel is down (missed invalidation).
      await r2.getMap<string, string>(loadName).fastPut('a', '2');
      await loadSub.connect();
      expect(await waitFor(async () => (await loadMap.get('a')) === '2')).toBe(true);
      expect(loadMap.getCachedMap().get('a')).toBe('2');
    } finally {
      await r.getKeys().delete(clearName, loadName).catch(() => undefined);
      await r2.shutdown();
      await r.shutdown();
    }
  }, 120000);
});
