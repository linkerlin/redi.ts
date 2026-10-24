/**
 * Live interop vs **Java Redisson 4.8.0**, round 3 — RAtomicLong's 4.8 args
 * family and RSearch (FT.*), surfaces phase 60/61 (atomic args, TS-only) and
 * 37/44/49/57 (search, TS-only) had covered without a Java side:
 *
 *  - `incrementAndGet(LongIncrementArgs)` — TS is a Lua-CAS emulation of the
 *    native `INCREX` (redis 8.8+); Java 4.8.0 calls INCREX natively. The
 *    shared-state walk (reject-without-saturate / clamp-with-saturate, both
 *    directions) proves the emulation agrees with Java byte-for-byte.
 *  - `compareAndDelete(CompareAndDeleteArgs)` — six comparison ops, both
 *    sides implemented as the same Lua; cross-language delete decisions.
 *  - RSearch — TS-built index (TEXT/NUMERIC/TAG over plain HASH docs)
 *    queried by Java; Java-built index queried by TS.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, CompareAndDeleteArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48c:';

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

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await raw.call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
    if (!probeOk) console.warn('[live interop48c] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48c] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

/** FT indexing is async — poll until the query sees the expected total. */
async function pollTotal(
  search: ReturnType<Redisson['getSearch']>, idx: string, query: string, want: number,
): Promise<number> {
  let total = 0;
  for (let i = 0; i < 100; i++) {
    total = (await search.search(idx, query, { limit: [0, 10] })).total;
    if (total === want) return total;
    await new Promise((res) => setTimeout(res, 100));
  }
  return total;
}

describe('live interop48c: RAtomicLong args + RSearch vs Java 4.8.0', () => {
  it('RAtomicLong incrementAndGet(LongIncrementArgs): TS Lua-CAS ≡ Java native INCREX', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'INCREX'))) {
      console.log('[live interop48c] INCREX unavailable (needs redis >=8.8 — Java side requires it) — skipping');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}al`;
    try {
      const al = r.getAtomicLong(k);
      await al.set(10);

      // TS reject (no saturate): out of bounds leaves the value, returns current
      expect(await al.incrementAndGet({ by: 5, upperBound: 12 })).toBe(10);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=10');
      // TS saturate: clamps to the bound
      expect(await al.incrementAndGet({ by: 5, upperBound: 12, saturate: true })).toBe(12);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=12');

      // Java reject (native INCREX without SATURATE): 12-15 < LBOUND 0 → unchanged
      expect(await probe48.probe(`atomic48-incrargs ${k} -15 0 - 0`)).toBe('value=12');
      expect(await al.get()).toBe(12);
      // Java saturate: clamps down to 0
      expect(await probe48.probe(`atomic48-incrargs ${k} -20 0 - 1`)).toBe('value=0');
      expect(await al.get()).toBe(0);

      // plain incrementAndGet crosses languages freely
      expect(await al.incrementAndGet()).toBe(1);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=1');
    } finally {
      await r.getKeys().delete(k);
      await r.shutdown();
    }
  }, 300000);

  it('RAtomicLong compareAndDelete(CompareAndDeleteArgs): same verdicts from both sides', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}cad`;
    try {
      const al = r.getAtomicLong(k);

      await al.set(5);
      expect(await probe48.probe(`atomic48-cadd ${k} > 10`)).toBe('deleted=false');
      expect(await al.get()).toBe(5);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.equal(5))).toBe(true);
      expect(await al.get()).toBe(0);

      await al.set(7);
      expect(await probe48.probe(`atomic48-cadd ${k} ~= 7`)).toBe('deleted=false');
      expect(await al.compareAndDelete(CompareAndDeleteArgs.lessOrEqual(7))).toBe(true);

      await al.set(9);
      expect(await probe48.probe(`atomic48-cadd ${k} >= 9`)).toBe('deleted=true');
      await al.set(4);
      expect(await al.compareAndDelete(CompareAndDeleteArgs.greater(3))).toBe(true);
      await al.set(2);
      expect(await probe48.probe(`atomic48-cadd ${k} < 2`)).toBe('deleted=false');
      expect(await al.compareAndDelete(CompareAndDeleteArgs.less(3))).toBe(true);
    } finally {
      await r.getKeys().delete(k);
      await r.shutdown();
    }
  }, 300000);

  it('RSearch: TS-built index queried by Java and vice versa', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'FT.CREATE'))) {
      console.log('[live interop48c] FT.* unavailable — skipping RSearch');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const prefix = `${NS}doc:`;
    const jidx = `${NS}jidx`;
    const jprefix = `${NS}jdoc:`;
    try {
      // --- direction 1: TS builds the index + docs; Java queries ---
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      // plain HASH fields (an RMap would JSON-encode field names, unindexed)
      await r.redis.hset(`${prefix}a`, 'title', 'apple', 'price', '10', 'cat', 'fruit');
      await r.redis.hset(`${prefix}b`, 'title', 'banana', 'price', '5', 'cat', 'fruit');
      await r.getSearch().createIndex(idx, ['title', 'TEXT', 'price', 'NUMERIC', 'cat', 'TAG'], { prefixes: [prefix] });
      expect(await pollTotal(r.getSearch(), idx, '*', 2)).toBe(2);

      const jAll = await probe48.probeLines(`search48-count ${idx} *`);
      expect(jAll[0]).toBe('total=2');
      expect(jAll.slice(1).sort()).toEqual([`doc=${prefix}a`, `doc=${prefix}b`]);
      const jRange = await probe48.probeLines(`search48-count ${idx} @price:[4 6]`);
      expect(jRange).toEqual(['total=1', `doc=${prefix}b`]);
      const jTag = await probe48.probeLines(`search48-count ${idx} @cat:{fruit}`);
      expect(jTag[0]).toBe('total=2'); // probe() returns the LAST line; total leads

      // --- direction 2: Java builds the index + docs; TS queries ---
      try { await probe48.probe(`search48-dropdocs ${jidx}`); } catch { /* absent */ }
      await probe48.probe(`search48-create ${jidx} ${jprefix}`);
      await probe48.probe(`search48-hset ${jprefix}ja title cherry price 7 cat berry`);
      await probe48.probe(`search48-hset ${jprefix}jb title date price 9 cat berry`);
      expect(await pollTotal(r.getSearch(), jidx, '*', 2)).toBe(2);
      const tsAll = await r.getSearch().search(jidx, '*', { limit: [0, 10] });
      expect(tsAll.total).toBe(2);
      expect(tsAll.docs.map((d: any) => d.id).sort()).toEqual([`${jprefix}ja`, `${jprefix}jb`]);
      const tsRange = await r.getSearch().search(jidx, '@price:[6 8]', { limit: [0, 10] });
      expect(tsRange.total).toBe(1);
      expect(tsRange.docs[0].id).toBe(`${jprefix}ja`);
      expect(tsRange.docs[0].title).toBe('cherry');
    } finally {
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      try { await probe48.probe(`search48-dropdocs ${jidx}`); } catch { /* probe gone */ }
      await r.getKeys().delete(`${prefix}a`, `${prefix}b`);
      await r.shutdown();
    }
  }, 300000);
});
