/**
 * Phase 21 live interop: background delayed-queue migrator (QueueTransferTask
 * port). Delivery is proven end-to-end: after TS start() the queue migrates
 * expired members on its own — the destination LIST is read by Java through a
 * plain RList (`list-read`), which creates no DelayedQueue wrapper and
 * therefore no Java transfer task that could migrate as a side effect.
 *
 * Also covers: re-arming on a new earlier head, stop() halting background
 * delivery (explicit poll() still catches up), destroy() leaving data intact.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName } from '../src/util';

const NS = 'TsJavaT:p21:';
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

async function waitFor(check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(100);
  }
  return check();
}

describe('live interop phase 21: delayed-queue background migrator', () => {
  it('start() delivers without poll(), re-arms on a new head, stops cleanly, destroys without data loss', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}dq`;
    const staging = prefixName('redisson_delay_queue', name);
    const timeout = prefixName('redisson_delay_queue_timeout', name);
    const dq = r.getDelayedQueue<string>(name);
    try {
      await r.getKeys().delete(name, staging, timeout);
      // Warm the resident probe before the timing-sensitive section so JVM
      // boot (or a Maven recompile) cannot skew the delivery windows.
      await probe(`list-read ${name}`);

      await dq.start();
      await dq.offer('later', 3000);
      await dq.offer('sooner', 1000); // new head → channel re-arms the timer

      // If channel-driven re-arming failed, nothing pushes until the 3s
      // 'later' head is due — seeing 'sooner' by 2.5s discriminates.
      expect(await waitFor(async () => (await dq.targetSize()) >= 1, 2500)).toBe(true);
      expect(await r.redis.lrange(name, 0, -1)).toEqual(['"sooner"']);
      expect(await dq.size()).toBe(1); // 'later' still staged
      expect(await dq.pendingSize()).toBe(1);

      expect(await waitFor(async () => (await dq.targetSize()) >= 2, 4000)).toBe(true);
      expect(await r.redis.lrange(name, 0, -1)).toEqual(['"sooner"', '"later"']);
      expect(await dq.size()).toBe(0);
      expect(await dq.pendingSize()).toBe(0);
      // Java reads the fully migrated destination through a plain LIST read.
      expect(await probe(`list-read ${name}`)).toBe('LIST:[sooner, later]');

      // Drain so later stages start from an empty destination.
      expect(await dq.poll()).toBe('sooner');
      expect(await dq.poll()).toBe('later');
      expect(await dq.targetSize()).toBe(0);

      // stop(): background delivery halts ...
      await dq.stop();
      await dq.offer('manual', 400);
      await sleep(1500);
      expect(await dq.targetSize()).toBe(0); // nothing migrated in the background
      expect(await dq.size()).toBe(1); // 'manual' still staged
      // ... but an explicit poll() still catches up.
      expect(await dq.poll()).toBe('manual');
      expect(await dq.targetSize()).toBe(0);
      expect(await dq.size()).toBe(0);

      // destroy(): detaches the task without deleting staged data.
      await dq.offer('held', 60000);
      await dq.destroy();
      expect(await dq.size()).toBe(1);
      expect(await r.redis.zcard(timeout)).toBe(1);
      expect(await dq.targetSize()).toBe(0);
      await dq.start(); // terminal after destroy: resolves, does nothing
      await sleep(300);
      expect(await dq.size()).toBe(1);
    } finally {
      await dq.stop();
      await r.getKeys().delete(name, staging, timeout);
      await r.shutdown();
    }
  }, 180000);
});
