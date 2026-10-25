/**
 * Live interop vs **Java Redisson 4.8.0**, round 35 — three surfaces the
 * earlier rounds did not cover:
 *
 *  - RGeo store family: TS `storeSearchTo` (GEORADIUS … STORE / GEOSEARCHSTORE)
 *    writes a plain member set into the destination that Java reads back,
 *    and `storeSortedSearchTo` (… STOREDIST) scores the destination ZSET by
 *    distance; Java `storeSearchTo`/`storeSortedSearchTo` build destinations
 *    TS reads as scored entries.
 *  - Java-side watchdog: Java `lock()` (no lease → its own 30s/3 renewal)
 *    stays held across a full renewal cycle — TS observes the TTL bounce
 *    back near 30s (unrenewed floor ~17.5s) and acquires only after the
 *    Java `unlock()`.
 *  - RLock re-entry count: Java re-enters twice (HASH field count 2); TS
 *    is rejected while either count remains and acquires only after BOTH
 *    Java unlocks — the count, not the hash presence, gates the lock.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, GeoSearchArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ai:';

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
    console.warn(`[live interop48ai] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

async function forceUnlockQuiet(name: string): Promise<void> {
  try {
    await probe48.probe(`lock48-forceunlock ${name}`);
  } catch {
    // nothing held — nothing to release
  }
}

describe('live interop48ai: geo store family / Java watchdog / re-entry count vs Java 4.8.0', () => {
  it('RGeo storeSearchTo/storeSortedSearchTo: destinations shared across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}geo-src`;
    const dst1 = `${NS}geo-d1`; // plain store
    const dst2 = `${NS}geo-d2`; // sorted store
    const js1 = `${NS}geo-j1`;
    const js2 = `${NS}geo-j2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [src, dst1, dst2, js1, js2]);
      // three members: two near the origin, one far away
      await r.getGeo<string>(src).add(13.361389, 38.115556, 'palermo');
      await r.getGeo<string>(src).add(15.087269, 37.502669, 'catania');
      await r.getGeo<string>(src).add(13.0, 38.1, 'near-west');

      // TS plain store → members land with their geohash-integer scores
      // (GEORADIUS STORE keeps the geohash bits, ~3.48e15 — NOT 0); Java
      // reads the identical set and scores
      expect(await r.getGeo<string>(src).storeSearchTo(dst1, GeoSearchArgs.from(13.36, 38.11).radius(200000, 'm'))).toBe(3);
      const d1 = await probe48.probeLines(`zset48-dump ${dst1}`);
      expect(d1.map((x) => x.slice('entry='.length).split('@')[0]).sort())
        .toEqual(['catania', 'near-west', 'palermo']);
      for (const line of d1) {
        expect(Number(line.split('@')[1])).toBeGreaterThan(3.4e15);
      }

      // TS sorted store → ZSET scored by distance from the origin (the
      // probe dump prints value@score sorted ALPHABETICALLY — compare per
      // member, not by line order)
      expect(await r.getGeo<string>(src).storeSortedSearchTo(dst2, GeoSearchArgs.from(13.36, 38.11).radius(200000, 'm'))).toBe(3);
      const d2 = new Map(await probe48.probeLines(`zset48-dump ${dst2}`)
        .then((ls) => ls.map((x) => {
          const [v, sc] = x.slice('entry='.length).split('@');
          return [v, Number(sc)] as [string, number];
        })));
      expect(d2.size).toBe(3);
      expect(d2.get('palermo')!).toBeLessThan(d2.get('near-west')!);
      expect(d2.get('near-west')!).toBeLessThan(d2.get('catania')!);

      // Java plain store → TS reads the members as a set
      expect(await probe48.probe(`geo48-store ${src} ${js1} 13.36 38.11 200000`)).toBe('stored=3');
      // plain-store destination is a ZSET (geo members) — read scored
      expect((await r.getScoredSortedSet<string>(js1).range(0, -1)).sort()).toEqual(['catania', 'near-west', 'palermo']);

      // Java sorted store → TS reads scored entries, ascending distance
      expect(await probe48.probe(`geo48-storesorted ${src} ${js2} 13.36 38.11 200000`)).toBe('stored=3');
      const j2 = await r.getScoredSortedSet<string>(js2).entryRange(0, -1);
      expect(j2.map((e) => e.value)).toEqual(['palermo', 'near-west', 'catania']);
      expect(j2[0].score).toBeLessThan(j2[1].score);
      expect(j2[1].score).toBeLessThan(j2[2].score);
    } finally {
      await delAll(raw, [src, dst1, dst2, js1, js2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('Java-side watchdog: no-lease lock() renews across a full cycle; TS waits it out', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}lk-jwd`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);

      // Java no-arg lock(): watchdog (30s, renewed every 30s/3) — TS sees
      // held + TTL near 30s
      expect(await probe48.probe(`lock48-watchdog ${l}`)).toBe('locked=true');
      await sleep(300);
      expect(await r.getLock(l).isLocked()).toBe(true);

      // after one renewal cycle the TTL must be back near 30s — an
      // unrenewed lease would have decayed to ~17.5s here
      await sleep(12000);
      const ttl = await raw.pttl(l);
      expect(ttl).toBeGreaterThan(22000);
      expect(await r.getLock(l).isLocked()).toBe(true);

      // Java unlock releases the hash; TS acquires immediately
      expect(await probe48.probe(`lock48-unlock ${l}`)).toBe('unlocked=ok');
      expect(await r.getLock(l).tryLock(0)).toBe(true);
      await r.getLock(l).unlock();
    } finally {
      await forceUnlockQuiet(l);
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RLock re-entry count: two Java re-entries gate TS until both unlocks', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}lk-re`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);

      // Java locks twice on the same thread — HASH field count = 2
      expect(await probe48.probe(`lock48-watchdog ${l}`)).toBe('locked=true');
      expect(await probe48.probe(`lock48-watchdog ${l}`)).toBe('locked=true');
      expect(await r.getLock(l).tryLock(0)).toBe(false);

      // first unlock decrements to 1 — still held
      expect(await probe48.probe(`lock48-unlock ${l}`)).toBe('unlocked=ok');
      expect(await r.getLock(l).tryLock(0)).toBe(false);

      // second unlock removes the hash — TS acquires
      expect(await probe48.probe(`lock48-unlock ${l}`)).toBe('unlocked=ok');
      expect(await r.getLock(l).tryLock(0)).toBe(true);
      await r.getLock(l).unlock();
    } finally {
      await forceUnlockQuiet(l);
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
