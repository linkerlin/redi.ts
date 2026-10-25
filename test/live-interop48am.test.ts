/**
 * Live interop vs **Java Redisson 4.8.0**, round 39 — three behaviors the
 * earlier rounds did not cover:
 *
 *  - RMapCache IDLE expiry (`redisson__idle__set:{name}` wire, distinct
 *    from the TTL ZSET): TS `put(k, v, ttlMs, idleMs)` → Java reads and
 *    vice versa; past the idle window (with TTL far longer) the entry is
 *    gone on BOTH sides (gone-after, law 10).
 *  - RHyperLogLog set algebra: `countWith` across TS/Java-built registers
 *    and `mergeWith` combining them into a third — cardinality 4 agrees
 *    in both languages.
 *  - RFairLock STRICT FIFO across mixed-language waiters: Java holds,
 *    TS waiter A, Java bg waiter B, TS waiter C queue up in that order;
 *    serial unlocks hand the lock down the queue — A before B before C.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48am:';

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
    console.warn(`[live interop48am] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48am: mapcache idle expiry / hll merge / fair-lock mixed FIFO vs Java 4.8.0', () => {
  it('RMapCache idle expiry: entries vanish on both sides past the idle window', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const m1 = `${NS}mc1`; // TS-built idle entry
    const m2 = `${NS}mc2`; // Java-built idle entry
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [m1, m2]);
      const mc1 = r.getMapCache<string, string>(m1);
      const mc2 = r.getMapCache<string, string>(m2);

      // TS puts ttl=60s idle=3s → Java reads it now (idle clock alive)
      expect(await mc1.put('k', 'ts-idle', 60000, 3000)).toBeNull();
      expect(await probe48.probe(`mc48-get ${m1} k`)).toBe('value=ts-idle');

      // Java puts the same shape → TS reads
      expect(await probe48.probe(`mci48-put ${m2} j java-idle 60000 3000`)).toBe('put=ok');
      expect(await mc2.get('j')).toBe('java-idle');

      // past the idle window (TTL far from due) both entries are gone on
      // BOTH sides — the idle ZSET drove the expiry, not the TTL
      await sleep(6500);
      expect(await probe48.probe(`mc48-get ${m1} k`)).toBe('value=null');
      expect(await mc1.get('k')).toBeNull();
      expect(await probe48.probe(`mc48-get ${m2} j`)).toBe('value=null');
      expect(await mc2.get('j')).toBeNull();
    } finally {
      await delAll(raw, [m1, m2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RHyperLogLog: countWith across languages and mergeWith into a shared dest', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}hll1`; // TS-built {1,2}
    const s2 = `${NS}hll2`; // Java-built {3,4}
    const dst = `${NS}hlld`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s1, s2, dst]);

      expect(await r.getHyperLogLog<number>(s1).add(1, 2)).toBe(true);
      expect(await probe48.probe(`hll48-add ${s2} 3`)).toBe('added=ok');
      expect(await probe48.probe(`hll48-add ${s2} 4`)).toBe('added=ok');

      // TS countWith sees the Java-built register too: |{1,2} ∪ {3,4}| = 4
      expect(await r.getHyperLogLog<number>(s1).countWith(s2)).toBe(4);

      // Java countWith over both = 4; Java mergeWith combines them into dst
      expect(await probe48.probe(`hll48-countwith ${s1} ${s2}`)).toBe('count=4');
      expect(await probe48.probe(`hll48-merge ${dst} ${s1} ${s2}`)).toBe('merged=ok');

      // merged register counts 4 on both sides
      expect(await r.getHyperLogLog<number>(dst).count()).toBe(4);
      expect(await probe48.probe(`hll48-count ${dst}`)).toBe('count=4');
    } finally {
      await delAll(raw, [s1, s2, dst]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RFairLock: strict FIFO handoff through mixed TS/Java waiters', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}fl`;
    const raw = new Redis(REDIS_URL);
    const order: string[] = [];
    try {
      await delAll(raw, [l]);
      const fl = r.getFairLock(l);

      // Java holds
      expect(await probe48.probe(`flock48-lock ${l}`)).toBe('locked=ok');

      // TS waiter A queues first (own threadId — the default 1 would make
      // A and C the SAME owner and C would re-enter instantly)
      const a = (async () => {
        await fl.lock(60000, 101);
        order.push('A');
      })();

      // Java bg waiter B queues second
      expect(await probe48.probe(`flock48-bglock ${l} 30000 60000`)).toBe('started');
      await sleep(400);

      // TS waiter C queues third
      const c = (async () => {
        await fl.lock(60000, 103);
        order.push('C');
      })();
      await sleep(400);

      // holder releases → A must acquire FIRST (queue head)
      expect(await probe48.probe(`flock48-unlock ${l}`)).toBe('unlocked=ok');
      await a;
      expect(order).toEqual(['A']);

      // A releases → B (the Java waiter) acquires
      await fl.unlock(101);
      expect(await probe48.probe(`flock48-bgwaitacquired 9000`)).toBe('acquired=true');

      // B releases → C acquires last
      expect(await probe48.probe(`flock48-bggo`)).toBe('go=ok');
      await c;
      expect(order).toEqual(['A', 'C']);
      await probe48.probe(`flock48-bgjoin 9000`);
      await fl.unlock(103);
    } finally {
      try { await probe48.probe(`lock48-forceunlock ${l}`); } catch { /* noop */ }
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
