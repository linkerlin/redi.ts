/**
 * Live interop vs **Java Redisson 4.8.0**, round 33 — delayed delivery and
 * composite/fenced locking, three surfaces whose last cross-language
 * evidence predates the 4.8 jar rounds:
 *
 *  - RDelayedQueue: Java `offer(value, delay, MS)` STAGES the member
 *    (staging LIST + timeout ZSET) — TS `size()`/`pendingSize()`/
 *    `contains()` unpack Java's struct-packed staging bytes; TS
 *    `poll()` runs the sync catch-up migration (no background task
 *    running) and consumes it. Reverse: TS `offer` + started migrator
 *    delivers into the destination where Java's queue poll reads it.
 *  - RMultiLock: all-or-nothing over three locks across languages —
 *    Java-held triple rejects TS MultiLock and single lock; TS-held
 *    triple parks a Java background `tryLock(6s, MS)` until TS
 *    `unlock()` wakes it.
 *  - RRedLock quorum: TS holding 1 of 3 still lets Java's RedLock win
 *    (2 ≥ quorum); a TS RedLock that can only reach 1 of its targets
 *    fails AND rolls back the one it took.
 *  - RFencedLock: one cross-language monotonic token sequence —
 *    contended `tryLockAndGetToken(waitMs, MS)` returns null while the
 *    shared counter keeps counting; TS `delete()` resets it to 0 and
 *    the sequence restarts. DISCOVERY: 4.8's no-arg
 *    tryLockAndGetToken() is bytecode-identical to lockAndGetToken()
 *    (both route to waitTime=-1/leaseTime=-1) and BLOCKS on contention
 *    until an unlock wake — TS's no-arg variant is the single
 *    non-blocking attempt its interface contract promises.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, RMultiLock, RRedLock } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ag:';

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
    console.warn(`[live interop48ag] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ag: delayed queue / multi-lock / red-lock / fenced lock vs Java 4.8.0', () => {
  it('RDelayedQueue: Java stages struct-packed members TS unpacks; TS migrator delivers where Java polls', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}dq`;
    const dq = r.getDelayedQueue<string>(name);
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name]);
      await dq.delete();

      // Java offers with a 400ms delay and detaches its own transfer task —
      // the member stays STAGED; only a TS migration can move it
      expect(await probe48.probe(`dq48-offer ${name} 400 java-dq-item`)).toBe('offered=ok');

      // TS reads Java's staged wire: staging LLEN, timeout ZSET, and the
      // struct-packed ('Bc0Lc0') member whose payload must decode identically
      expect(await dq.size()).toBe(1);
      expect(await dq.pendingSize()).toBe(1);
      expect(await dq.contains('java-dq-item')).toBe(true);

      // after the deadline, TS's poll() runs the sync catch-up migration
      await sleep(700);
      expect(await dq.poll()).toBe('java-dq-item');
      expect(await dq.readAll()).toEqual([]);
      expect(await dq.size()).toBe(0);

      // TS offers; its started migrator moves it into the destination queue
      await dq.offer('ts-dq-item', 400);
      expect(await dq.size()).toBe(1);
      await dq.start();
      await sleep(1000);
      expect(await dq.targetSize()).toBe(1);
      expect(await probe48.probe(`dq48-read ${name}`)).toBe('value=ts-dq-item');
      expect(await dq.size()).toBe(0);
    } finally {
      await dq.stop();
      await dq.destroy();
      await dq.delete();
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RMultiLock all-or-nothing across languages; RRedLock quorum with cross-side rollback', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l1 = `${NS}ml1`;
    const l2 = `${NS}ml2`;
    const l3 = `${NS}ml3`;
    const l4 = `${NS}ml4`;
    const l5 = `${NS}ml5`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l1, l2, l3, l4, l5]);
      const tsMl = new RMultiLock([r.getLock(l1), r.getLock(l2), r.getLock(l3)]);

      // Java's MultiLock takes the fresh triple; TS MultiLock and single lock
      // are both rejected
      expect(await probe48.probe(`mlk48-trylock ${l1} ${l2} ${l3}`)).toBe('locked=true');
      expect(await tsMl.tryLock()).toBe(false);
      expect(await r.getLock(l1).tryLock(0)).toBe(false);

      // Java releases → TS MultiLock acquires all three
      expect(await probe48.probe(`mlk48-unlock ${l1} ${l2} ${l3}`)).toBe('unlocked=ok');
      expect(await tsMl.tryLock()).toBe(true);

      // cross-language wake: Java's background MultiLock parks inside
      // tryLock(6s, MS) until TS unlock releases the triple
      expect(await probe48.probe(`mlk48-bgtrylock ${l1} ${l2} ${l3} 6000`)).toBe('armed');
      await sleep(300);
      await tsMl.unlock();
      expect(await probe48.probe(`mlk48-bgresult 9000`)).toBe('locked=true');

      // the bg THREAD holds the triple now (a foreign thread's unlock
      // throws) — TS rejected until the bg thread releases itself
      expect(await r.getLock(l1).tryLock(0)).toBe(false);
      expect(await probe48.probe(`mlk48-bgrelease 9000`)).toBe('released=ok');

      // RedLock quorum: TS holds 1 of 3 — Java still wins (2 ≥ quorum 2)
      expect(await r.getLock(l5).tryLock(0)).toBe(true);
      expect(await probe48.probe(`rlk48-trylock ${l5} ${l2} ${l3}`)).toBe('locked=true');

      // TS RedLock that can only reach l4 (1 < 2) fails AND rolls back l4 —
      // no leaked partial acquisition
      const tsRl = new RRedLock([r.getLock(l4), r.getLock(l2), r.getLock(l3)]);
      expect(await tsRl.tryLock()).toBe(false);
      expect(await r.getLock(l4).tryLock(0)).toBe(true);
      await r.getLock(l4).unlock();

      // Java releases → TS RedLock over the freed pair succeeds
      expect(await probe48.probe(`rlk48-unlock ${l5} ${l2} ${l3}`)).toBe('unlocked=ok');
      const tsRl2 = new RRedLock([r.getLock(l2), r.getLock(l3)]);
      expect(await tsRl2.tryLock()).toBe(true);
      await tsRl2.unlock();
      await r.getLock(l5).unlock();
    } finally {
      await delAll(raw, [l1, l2, l3, l4, l5]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RFencedLock: one cross-language monotonic token sequence; delete() restarts it', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const f = `${NS}fnc`;
    const raw = new Redis(REDIS_URL);
    try {
      // clean slate: TS delete() clears the RLock HASH + the token counter
      // (false on a fresh name — nothing to delete)
      await r.getFencedLock(f).delete();
      expect(await probe48.probe(`fnc48-gettoken ${f}`)).toBe('token=0');

      // TS acquires → token 1; Java is contended (null) but reads the SAME counter
      expect(await r.getFencedLock(f).tryLockAndGetToken()).toBe(1);
      expect(await probe48.probe(`fnc48-trylockget ${f} 300`)).toBe('token=null');
      expect(await probe48.probe(`fnc48-gettoken ${f}`)).toBe('token=1');

      // TS releases → Java takes the NEXT token (sequence continues cross-language)
      await r.getFencedLock(f).unlock();
      expect(await probe48.probe(`fnc48-trylockget ${f} 300`)).toBe('token=2');

      // Java releases → TS continues
      expect(await probe48.probe(`fnc48-unlock ${f}`)).toBe('unlocked=ok');
      expect(await r.getFencedLock(f).tryLockAndGetToken()).toBe(3);
      await r.getFencedLock(f).unlock();

      // Java delete() (RObject method — the 4.8 RFencedLock interface does not
      // expose it) wipes HASH + counter; the sequence restarts at 1
      expect(await probe48.probe(`fnc48-delete ${f}`)).toBe('deleted=true');
      expect(await probe48.probe(`fnc48-gettoken ${f}`)).toBe('token=0');
      expect(await r.getFencedLock(f).tryLockAndGetToken()).toBe(1);
      await r.getFencedLock(f).unlock();
    } finally {
      await delAll(raw, [f]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
