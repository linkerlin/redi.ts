/**
 * Phase 62 live: RSetCache zadd/zrem listeners and RLock newCondition
 * (UnsupportedOperationException in Java 4.6.1).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p62:';
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

describe('live phase 62: SetCache listeners + newCondition', () => {
  it('delivers zadd/zrem and throws for lock conditions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sc`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(name);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase62] notify-keyspace-events not configurable — skipping listener assertions');
      }
      if (canConfig) {
        const sc = r.getSetCache<string>(name);
        const events: string[] = [];
        const id = await sc.addListener({
          onAdd: () => events.push('add'),
          onRemove: () => events.push('remove'),
        });
        await sc.add('a');
        await sc.remove('a');
        expect(await waitFor(() => events.includes('add') && events.includes('remove'))).toBe(true);
        const before = events.length;
        await sc.removeListener(id);
        await sc.add('b');
        await sleep(300);
        expect(events.length).toBe(before);
      }

      expect(() => r.getLock(`${NS}l`).newCondition()).toThrow(/Unsupported/);
      expect(() => r.getFairLock(`${NS}f`).newCondition()).toThrow(/Unsupported/);
      const rw = r.getReadWriteLock(`${NS}rw`);
      expect(() => rw.readLock.newCondition()).toThrow(/Unsupported/);
      expect(() => rw.writeLock.newCondition()).toThrow(/Unsupported/);
    } finally {
      await r.getKeys().delete(name).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
