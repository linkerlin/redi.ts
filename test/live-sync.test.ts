/**
 * Phase 2 live interop: mutual exclusion (RLock), shared semaphore permits,
 * shared rate-limiter window. Needs real Redis + probe (see live-interop.test.ts).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe';

const NS = 'TsJavaT:sync:';
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

describe('live interop phase 2: locks/semaphore/ratelimiter', () => {
  it('TS-held lock blocks Java, release lets Java in', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock:a`;
    try {
      await r.getKeys().delete(name);
      const l = r.getLock(name);
      await l.lock(30000, 1);
      expect(await probe(`lock-try ${name}`)).toBe('CONTENDED');
      await l.unlock(1);
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('Java-held lock blocks TS, release lets TS in', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock:b`;
    try {
      await r.getKeys().delete(name);
      // hold in background: probe prints HELD, sleeps 4s, prints RELEASED
      const hold = probeBg(`lock-hold ${name} 4000`);
      // wait until HELD is observable
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = await r.getLock(name).isLocked();
      }
      expect(held).toBe(true);
      expect(await r.getLock(name).tryLock(0, 5000, 99)).toBe(false);
      await hold; // released
      expect(await r.getLock(name).tryLock(0, 5000, 99)).toBe(true);
      await r.getLock(name).unlock(99);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('semaphore permits are shared across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sem:shared`;
    try {
      await r.getKeys().delete(name);
      const sem = r.getSemaphore(name);
      expect(await sem.trySetPermits(1)).toBe(true);
      expect(await sem.tryAcquire(1)).toBe(true); // TS drains the only permit
      expect(await probe(`sem-try ${name}`)).toBe('false'); // Java sees 0
      await sem.release(1);
      expect(await probe(`sem-try ${name}`)).toBe('true'); // Java gets it
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('rate limiter window is shared across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ratelimit:shared`;
    try {
      await r.getKeys().delete(`{${name}}:value`, `{${name}}:permits`, name);
      const rl = r.getRateLimiter(name);
      await rl.setRate('OVERALL', 3, 60000);
      expect(await rl.tryAcquire(1)).toBe(true); // 2 left
      expect(await probe(`rate-try ${name} 1`)).toBe('true'); // 1 left
      expect(await probe(`rate-try ${name} 1`)).toBe('true'); // 0 left
      expect(await rl.tryAcquire(1)).toBe(false); // exhausted on both sides
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('spinlock HASH-level exclusion both directions (no latch involved)', async () => {
    // NOTE: Java SpinLock adds a per-request unlock-latch key that TS does
    // not write/read; the HASH itself is fully shared, so mutual exclusion
    // holds both ways. Only latch-based waiter wakeup is Java-only.
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}spin:shared`;
    try {
      await r.getKeys().delete(name);
      const sp = r.getSpinLock(name);
      await sp.lock(30000, 11);
      expect(await probe(`lock-try ${name}`)).toBe('CONTENDED');
      await sp.unlock(11);
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED');

      const hold = probeBg(`lock-hold ${name} 4000`);
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = await r.getSpinLock(name).isLocked();
      }
      expect(held).toBe(true);
      expect(await r.getSpinLock(name).tryLock(0, 5000, 12)).toBe(false);
      await hold;
      expect(await r.getSpinLock(name).tryLock(0, 5000, 12)).toBe(true);
      await r.getSpinLock(name).unlock(12);
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
