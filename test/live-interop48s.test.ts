/**
 * Live interop vs **Java Redisson 4.8.0**, round 19 — the last distinct
 * blocking code paths (earlier rounds always used the timeout variants):
 *
 *  - RBlockingQueue.take(): TS blocks FOREVER (dedicated connection, no
 *    timeout parameter exists on this path); a Java offer wakes it. A
 *    30s safety race fails loudly instead of hanging the suite.
 *  - RSemaphore.acquire(): TS blocks on the last permit; a Java RELEASE
 *    (not a TS one) wakes it — the `redisson_sc` wake loop for the plain
 *    semaphore, previously proven only for PES (round 12).
 *
 * Probe command `sem48-release`.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48s:';

const hasRedis = await (async () => {
  const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
  try {
    await r.connect();
    return true;
  } catch {
    return false;
  } finally {
    r.disconnect();
  }
})();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48s] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

/** Fails loudly after `ms` instead of hanging a forever-block on a missed wake. */
function withSafety<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`forever-${what} never woke within ${ms}ms`)), ms).unref?.() ?? undefined),
  ]) as Promise<T>;
}

describe('live interop48s: forever-take + semaphore wake vs Java 4.8.0', () => {
  it('RBlockingQueue.take(): TS blocks forever, a Java offer wakes it', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q = `${NS}q`;
    try {
      await r.getKeys().delete(q);
      const take = withSafety(r.getBlockingQueue<string>(q).take(), 30000, 'take');
      await new Promise((res) => setTimeout(res, 200)); // let it reach BLPOP

      expect(await probe48.probe(`q48-offer ${q} wake-up`)).toBe('offered=true');
      expect(await take).toBe('wake-up');
      expect(await r.getQueue<string>(q).readAll()).toEqual([]);
    } finally {
      await r.getKeys().delete(q);
      await r.shutdown();
    }
  }, 300000);

  it('RSemaphore.acquire(): TS blocks on the last permit, a Java release wakes it', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sem`;
    try {
      await r.getKeys().delete(name);
      const sem = r.getSemaphore(name);
      expect(await sem.trySetPermits(1)).toBe(true);

      // Java holds the only permit; TS blocks acquiring
      expect(await probe48.probe(`sem48-acquire ${name}`)).toBe('acquired=true');
      const acquire = withSafety(sem.acquire(1), 30000, 'semaphore acquire');
      await new Promise((res) => setTimeout(res, 200));
      expect(await sem.availablePermits()).toBe(0);

      // Java's release frees the permit and publishes the wake
      await probe48.probe(`sem48-release ${name}`);
      await acquire; // resolves — no assertion value, resolution IS the proof
      expect(await sem.availablePermits()).toBe(0); // TS now holds it
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
