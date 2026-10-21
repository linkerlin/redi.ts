/**
 * Phase 54 live: RDelayedQueue 4.6.1 surface — negative delay validation,
 * unsupported add/single-arg offer/indexOf, no-arg remove(), and atomic
 * pollLastAndOfferFirstTo from staging to a destination queue.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { prefixName } from '../src/util';

const NS = 'TsJavaT:p54:';
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
const hasRedis = await redisUp();

describe('live phase 54: delayed queue surface', () => {
  it('validates delays, moves staged members and removes from the destination', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}dq`;
    const dest = `${NS}dest`;
    const stage = prefixName('redisson_delay_queue', name);
    const timeouts = prefixName('redisson_delay_queue_timeout', name);
    try {
      await r.getKeys().delete(name, dest, stage, timeouts);
      const dq = r.getDelayedQueue<string>(name);

      await expect(dq.offer('x')).rejects.toThrow(/offer/);
      await expect(dq.offer('x', -5)).rejects.toThrow(/negative/);
      await expect(dq.add('x')).rejects.toThrow(/offer/);
      await expect(dq.indexOf('x')).rejects.toThrow(/Unsupported/);
      await expect(dq.remove()).rejects.toThrow(/NoSuchElement/);

      await dq.offer('move-me', 60000);
      expect(await dq.pendingSize()).toBe(1);
      expect(await dq.size()).toBe(1);
      expect(await dq.pollLastAndOfferFirstTo(dest)).toBe('move-me');
      expect(await dq.pendingSize()).toBe(0);
      expect(await dq.size()).toBe(0);
      expect(await r.getQueue<string>(dest).readAll()).toEqual(['move-me']);

      await dq.offer('ready', 150);
      await sleep(400);
      expect(await dq.poll()).toBe('ready');
      await expect(dq.remove()).rejects.toThrow(/NoSuchElement/);
    } finally {
      await r.getKeys().delete(name, dest, stage, timeouts);
      await r.shutdown();
    }
  }, 120000);
});
