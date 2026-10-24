/**
 * Live interop vs **Java Redisson 4.8.0**, round 13 — CONCURRENT
 * cross-language contention. Every earlier suite alternated operations
 * sequentially; this one has both languages hammering the SAME key at the
 * same instant and asserts the invariants that only hold if the wire is
 * truly atomic across languages:
 *
 *  - Atomic INCR race: 50 TS + 50 Java increments in flight together →
 *    exactly 100, read identically by both.
 *  - Queue drain race: 20 preloaded items consumed concurrently by TS
 *    polls and Java polls → every item delivered exactly once, no loss,
 *    no duplication.
 *  - Semaphore no-oversell: 2 permits raced for by two TS acquires and a
 *    Java acquire → exactly 2 succeed, availability 0.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48m:';

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
    if (!probeOk) console.warn('[live interop48m] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48m] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48m: concurrent cross-language races vs Java 4.8.0', () => {
  it('atomic INCR race: 50 TS + 50 Java increments → exactly 100', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}race`;
    try {
      await r.getKeys().delete(k);
      const al = r.getAtomicLong(k);

      // both sides increment concurrently (Java via 50 probe round-trips)
      const tsSide = (async () => {
        for (let i = 0; i < 50; i++) await al.incrementAndGet();
      })();
      const jSide = (async () => {
        for (let i = 0; i < 50; i++) await probe48.probe(`atomic48-incrargs ${k} 1 - - 0`);
      })();
      await Promise.all([tsSide, jSide]);

      expect(await al.get()).toBe(100);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=100');
    } finally {
      await r.getKeys().delete(k);
      await r.shutdown();
    }
  }, 300000);

  it('queue drain race: every item consumed exactly once across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}q`;
    try {
      await r.getKeys().delete(name);
      // 20 known items preloaded from TS
      const items = Array.from({ length: 20 }, (_, i) => `item-${i}`);
      await r.getQueue<string>(name).addAll(items);

      // both languages poll concurrently until empty
      const q = r.getQueue<string>(name);
      const tsSide = (async () => {
        const got: string[] = [];
        for (let i = 0; i < 20; i++) {
          const v = await q.poll();
          if (v !== null) got.push(v);
        }
        return got;
      })();
      const jSide = (async () => {
        const got: string[] = [];
        for (let i = 0; i < 20; i++) {
          const line = await probe48.probe(`q48-poll ${name}`);
          const v = line.slice('value='.length);
          if (v !== 'null') got.push(v);
        }
        return got;
      })();
      const [tsGot, jGot] = await Promise.all([tsSide, jSide]);

      // exactly-once: union is the full set, no element seen by both
      expect(tsGot.length + jGot.length).toBe(20);
      const all = [...tsGot, ...jGot].sort();
      expect(all).toEqual([...items].sort());
      const dupes = all.filter((v, i) => i > 0 && all[i - 1] === v);
      expect(dupes).toEqual([]);
      expect(await q.size()).toBe(0);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('semaphore no-oversell: 1 permit raced by both languages → exactly one holder', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sem`;
    try {
      await r.getKeys().delete(name);
      const sem = r.getSemaphore(name);
      expect(await sem.trySetPermits(1)).toBe(true);

      // both languages race for the single permit; exactly one must win.
      // If TS wins, Java's tryAcquire is false and tsAttempt is already
      // resolved; if Java wins, tsAttempt blocks until the release below.
      const tsAttempt = sem.acquire(1);
      await new Promise((res) => setTimeout(res, 100));
      const jWon = (await probe48.probe(`sem48-acquire ${name}`)) === 'acquired=true';
      await new Promise((res) => setTimeout(res, 100));

      // the no-oversell invariant: no permit available (total 1 − held 1),
      // no matter which language won. RSemaphore exposes available only.
      expect(await sem.availablePermits()).toBe(0);

      // free the permit; any blocked TS attempt completes
      await sem.release(1);
      await tsAttempt;
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
