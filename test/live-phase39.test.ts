/**
 * Phase 39 live: RReliableTopic 4.6.1 semantics — one subscriber group per
 * client instance (all listeners share it), publish() returns the group count,
 * heartbeat ZSET cleanup and automatic XTRIM after acknowledgment.
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { suffixName } from '../src/util';

const NS = 'TsJavaT:p39:';
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
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

async function waitFor(check: () => Promise<boolean> | boolean, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(100);
  }
  return check();
}

describe('live interop phase 39: reliable topic semantics', () => {
  it('one group per instance, publish count, trim and lifecycle', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rt`;
    const timeoutKey = suffixName(name, 'timeout');
    let r2: ReturnType<typeof Redisson.create> | null = null;
    try {
      await r.getKeys().delete(name, timeoutKey);
      const rt = r.getReliableTopic<string>(name);
      expect(await rt.publish('before')).toBe(0);
      const seenA: string[] = [];
      const seenB: string[] = [];
      const a = await rt.addListener((v) => { seenA.push(v); });
      const b = await rt.addListener((v) => { seenB.push(v); });
      expect(await rt.countListeners()).toBe(2);
      const groups1: any = await r.redis.xinfo('GROUPS', name);
      expect(groups1.length).toBe(1); // both listeners share one group (Redisson instance)
      expect(await rt.publish('hello')).toBe(1);
      expect(await waitFor(() => seenA.includes('hello') && seenB.includes('hello'), 5000)).toBe(true);

      // after ack + maintenance, the stream is trimmed to the group's position
      expect(await waitFor(async () => (await r.redis.xlen(name)) === 0, 8000)).toBe(true);

      r2 = Redisson.create({ address: REDIS_URL });
      const rt2 = r2.getReliableTopic<string>(name);
      const seen2: string[] = [];
      await rt2.addListener((v) => { seen2.push(v); });
      expect(await rt.publish('two')).toBe(2);
      expect(await waitFor(() => seenA.includes('two') && seen2.includes('two'), 5000)).toBe(true);

      await rt.removeListener(a); // another listener remains → group stays
      expect(((await r.redis.xinfo('GROUPS', name)) as any[]).length).toBe(2);
      await rt.removeAllListeners();
      expect(((await r.redis.xinfo('GROUPS', name)) as any[]).length).toBe(1);
      await rt2.removeAllListeners();
      expect(await rt.publish('none')).toBe(0);
      expect(await r.redis.zcard(timeoutKey)).toBe(0);
      void b;
    } finally {
      try { await r2?.shutdown(); } catch { /* noop */ }
      await r.getKeys().delete(name, timeoutKey);
      await r.shutdown();
    }
  }, 120000);
});
