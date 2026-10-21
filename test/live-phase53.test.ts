/**
 * Phase 53 live: RAtomicLong/AtomicDouble addListener — incrby and
 * incrbyfloat keyspace events, removal silence. Config changes use the shared
 * keyspace test lock.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p53:';
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

describe('live phase 53: atomic listeners', () => {
  it('delivers incrby/incrbyfloat events and stops after removal', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ln = `${NS}l`;
    const dn = `${NS}d`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      await r.getKeys().delete(ln, dn);
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        console.log('[phase53] notify-keyspace-events not configurable — skipping');
      }
      if (!canConfig) return;

      const al = r.getAtomicLong(ln);
      const ad = r.getAtomicDouble(dn);
      const longEvents: string[] = [];
      const doubleEvents: string[] = [];
      const lid = await al.addListener({ onChange: (key) => longEvents.push(key) });
      const did = await ad.addListener({ onChange: (key) => doubleEvents.push(key) });
      await al.addAndGet(5);
      await ad.addAndGet(0.5);
      expect(await waitFor(() => longEvents.includes(ln) && doubleEvents.includes(dn))).toBe(true);

      const longBefore = longEvents.length;
      const doubleBefore = doubleEvents.length;
      await al.removeListener(lid);
      await ad.removeListener(did);
      await al.incrementAndGet();
      await ad.incrementAndGet();
      await sleep(400);
      expect(longEvents.length).toBe(longBefore);
      expect(doubleEvents.length).toBe(doubleBefore);
    } finally {
      await r.getKeys().delete(ln, dn).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
