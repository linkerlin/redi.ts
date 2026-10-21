/**
 * Phase 47 live: RKeys global keyspace listeners (new/set/expired/del).
 * Requires notify-keyspace-events with K+E (+n for new); the test serializes
 * config changes with live-phase31, saves/restores the previous config and
 * skips when CONFIG SET is refused.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p47:';
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

describe('live phase 47: RKeys keyspace listeners', () => {
  it('delivers new/set/expired/del and stops after removeListener', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}one`;
    const k2 = `${NS}two`;
    const k3 = `${NS}three`;
    let prev = '';
    let canConfig = false;
    let canNew = true;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      try {
        await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
        canConfig = true;
      } catch {
        try {
          await r.redis.config('SET', 'notify-keyspace-events', 'KEA');
          canConfig = true;
          canNew = false;
          console.log('[phase47] `n` flag unavailable — new-event assertion skipped');
        } catch {
          console.log('[phase47] notify-keyspace-events not configurable — skipping');
        }
      }
      if (!canConfig) return;

      await r.getKeys().delete(k1, k2, k3);
      const events: string[] = [];
      const record = (kind: string) => (key: string) => {
        if (key.startsWith(NS)) events.push(`${kind}:${key}`);
      };
      const id = await r.getKeys().addListener({
        onNew: record('new'),
        onSet: record('set'),
        onExpired: record('expired'),
        onDeleted: record('deleted'),
      });
      expect(id).toBeGreaterThan(0);

      await r.getBucket(k1).set('v');
      await r.getBucket(k1).delete();
      await r.getBucket(k2).set('v');
      await r.getBucket(k2).expire(200);
      expect(await waitFor(() => events.includes(`set:${k1}`) && events.includes(`deleted:${k1}`))).toBe(true);
      expect(await waitFor(() => events.includes(`expired:${k2}`))).toBe(true);
      if (canNew) expect(await waitFor(() => events.includes(`new:${k1}`) && events.includes(`new:${k2}`))).toBe(true);

      // Removal works through a fresh getKeys() facade (shared hub).
      await r.getKeys().removeListener(id);
      await r.getBucket(k3).set('v');
      await sleep(400);
      expect(events.some((e) => e.endsWith(`:${k3}`))).toBe(false);
    } finally {
      await r.getKeys().delete(k1, k2, k3).catch(() => undefined);
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 120000);
});
