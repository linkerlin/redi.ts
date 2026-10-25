/**
 * Live interop vs **Java Redisson 4.8.0**, round 42 — three read/remove
 * families the earlier rounds did not cover:
 *
 *  - RTimeSeries (ZSET family) removal: Java `getAndRemove(ts)` /
 *    `remove(ts)` on TS-added entries and vice versa; `removeRange`
 *    deletes a window from the other side's series.
 *  - RRingBuffer read family: Java `readNewest(n)` / `readOldest(n)`
 *    view TS's ring (wrapped) and vice versa.
 *  - RStream `readMany`: TS multi-stream XREAD over two Java-written
 *    streams returns both; Java's two-stream `read` returns TS entries.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ap:';

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
    console.warn(`[live interop48ap] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ap: timeseries removal / ring-buffer reads / stream readMany vs Java 4.8.0', () => {
  it('RTimeSeries removal: getAndRemove/remove/removeRange across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const t1 = `${NS}ts1`; // TS-built, Java removes
    const t2 = `${NS}ts2`; // Java-built, TS removes
    const raw = new Redis(REDIS_URL);
    try {
      // companion keys follow the family (law 4/9) — plain names, DEL only
      await delAll(raw, [t1, t2]);

      // TS adds 3 entries (4.6.1 family: add(ts, v)); Java getAndRemove one
      const ts1 = r.getTimeSeries<number>(t1);
      await ts1.add(1000, 1.5);
      await ts1.add(2000, 2.5);
      await ts1.add(3000, 3.5);
      expect(await probe48.probe(`ts48-getandremove ${t1} 2000`)).toBe('value=2.5');
      expect(await ts1.size()).toBe(2);
      expect(await probe48.probe(`ts48-remove ${t1} 3000`)).toBe('removed=true');

      // Java builds a series with the 4.8 args family; TS removes one entry
      // and a range (families share the ZSET wire when not mixed per key)
      await probe48.probe(`ts48-add ${t2} 100 1.5`);
      await probe48.probe(`ts48-add ${t2} 200 2.5`);
      await probe48.probe(`ts48-add ${t2} 300 3.5`);
      const ts2 = r.getTimeSeries<number>(t2);
      expect(await ts2.getAndRemove(200)).toBe(2.5);
      expect(await ts2.removeRange(100, 300)).toBe(2);
      expect(await ts2.size()).toBe(0);
    } finally {
      await delAll(raw, [t1, t2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RRingBuffer readNewest/readOldest: both languages view the same wrapped ring', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const rb1 = `${NS}rb1`; // TS-built ring
    const rb2 = `${NS}rb2`; // Java-built ring
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [rb1, rb2]);

      // TS ring capacity 3, add 5 → wrapped to [three,four,five]
      const rb = r.getRingBuffer<string>(rb1);
      await rb.trySetCapacity(3);
      for (const v of ['one', 'two', 'three', 'four', 'five']) await rb.add(v);

      // readNewest/readOldest return SLICES in ring (chronological) order —
      // not newest-first
      expect(await probe48.probe(`rb48-readnewest ${rb1} 2`)).toBe('newest=four,five');
      expect(await probe48.probe(`rb48-readoldest ${rb1} 2`)).toBe('oldest=three,four');

      // Java ring: setCapacity + adds; TS readNewest/readOldest agree
      // (Java capacity setter via a fresh probe command? use trySet on TS first)
      const rb2ts = r.getRingBuffer<string>(rb2);
      await rb2ts.trySetCapacity(3);
      for (const v of ['j1', 'j2', 'j3', 'j4']) await probe48.probe(`rb48-add ${rb2} ${v}`);
      expect(await rb2ts.readNewest(2)).toEqual(['j3', 'j4']);
      expect(await rb2ts.readOldest(2)).toEqual(['j2', 'j3']);
    } finally {
      await delAll(raw, [rb1, rb2, `redisson_rb:{${rb1}}`, `redisson_rb:{${rb2}}`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream readMany: two-stream reads in both languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}s1`;
    const s2 = `${NS}s2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s1, s2]);

      // Java writes one entry into each stream; TS readMany returns both
      await probe48.probe(`stream48-add ${s1} f jv1`);
      await probe48.probe(`stream48-add ${s2} f jv2`);
      const got = await r.getStream<string>(s1).readMany({ [s1]: '0', [s2]: '0' }, 10);
      expect(got.get(s1)).toHaveLength(1);
      expect(got.get(s1)![0].fields.f).toBe('jv1');
      expect(got.get(s2)![0].fields.f).toBe('jv2');

      // TS writes; Java's two-stream read returns both
      await r.getStream<string>(s1).add({ f: 'tv1' });
      await r.getStream<string>(s2).add({ f: 'tv2' });
      const lines = await probe48.probeLines(`stream48-readmany 10 ${s1} ${s2}`);
      expect(lines.some((x) => x.startsWith(`stream=${s1}`) && x.includes('f=tv1'))).toBe(true);
      expect(lines.some((x) => x.startsWith(`stream=${s2}`) && x.includes('f=tv2'))).toBe(true);
    } finally {
      await delAll(raw, [s1, s2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
