/**
 * Live interop vs **Java Redisson 4.8.0**, round 10 — cross-language
 * coordination: RCountDownLatch countdown and RReadWriteLock exclusion.
 *
 *  - Latch: TS `await` polls the shared counter while Java `countDown()`s
 *    it to zero — the one-shot barrier opening across languages; plus the
 *    reverse setup (Java-configured latch released by TS).
 *  - RW lock: a TS-held READ lock lets Java read-acquire (shared) but
 *    blocks its write-acquire; a Java-held WRITE lock blocks both TS
 *    ends; exclusivity flips correctly after each release.
 *
 * Probe commands `latch48-*` / `rw48-*`.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48j:';

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
    if (!probeOk) console.warn('[live interop48j] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48j] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48j: latch countdown + RW-lock exclusion vs Java 4.8.0', () => {
  it('RCountDownLatch: Java counts down a TS awaiter to zero', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}latch`;
    try {
      await r.getKeys().delete(name);

      // TS arms the latch; Java sees the count
      const latch = r.getCountDownLatch(name);
      expect(await latch.trySetCount(2)).toBe(true);
      expect(await probe48.probe(`latch48-count ${name}`)).toBe('count=2');

      // TS awaits (polls the shared counter); Java counts down twice
      const opened = latch.await(10000);
      await new Promise((res) => setTimeout(res, 150));
      expect(await latch.await(0)).toBe(false); // still armed for zero-wait
      await probe48.probe(`latch48-countdown ${name}`);
      await probe48.probe(`latch48-countdown ${name}`);
      expect(await opened).toBe(true);
      expect(await probe48.probe(`latch48-count ${name}`)).toBe('count=0');

      // reverse setup: Java arms, TS releases
      const jname = `${NS}latch-j`;
      expect(await probe48.probe(`latch48-set ${jname} 1`)).toBe('set=true');
      await r.getCountDownLatch(jname).countDown();
      expect(await probe48.probe(`latch48-count ${jname}`)).toBe('count=0');
      await r.getKeys().delete(jname);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RReadWriteLock: TS-held read shares with Java reads, blocks Java writes', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw`;
    try {
      await r.getKeys().delete(name);
      const rw = r.getReadWriteLock(name);

      // TS holds a READ lock: Java read-acquires (shared), write is denied
      await rw.readLock.lock(30000);
      expect(await probe48.probe(`rw48-writetry ${name}`)).toBe('got=false');
      expect(await probe48.probe(`rw48-readtry ${name}`)).toBe('got=true');
      await probe48.probe(`rw48-readunlock ${name}`);

      // TS releases; Java takes the WRITE lock exclusively
      await rw.readLock.unlock();
      expect(await probe48.probe(`rw48-writetry ${name}`)).toBe('got=true');

      // Java-held WRITE blocks both TS ends
      expect(await rw.writeLock.tryLock(0)).toBe(false);
      expect(await rw.readLock.tryLock(0)).toBe(false);
      await probe48.probe(`rw48-writeunlock ${name}`);

      // free again: TS write-acquires
      expect(await rw.writeLock.tryLock(0)).toBe(true);
      await rw.writeLock.unlock();
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
