/**
 * Live interop vs **Java Redisson 4.8.0**, round 23 — four smaller families
 * that 4.6.1 probe covered but the 4.8.0 jar had zero evidence for:
 *
 *  - RBitSet: raw SETBIT domain — Java bits readable from TS and vice versa;
 *    64-bit segments (getLong/setLong) and BITOP OR agree cross-language.
 *  - RHyperLogLog: PFADD on codec-encoded elements; the small-range count
 *    of a shared register matches on both sides.
 *  - RGeo: codec members in the geohash ZSET — dist/pos/search agree in
 *    both directions.
 *  - RIdGenerator: block allocation is [old, old+alloc) on BOTH sides
 *    (Java Lua returns the OLD counter value as the block start). Every
 *    Java `getIdGenerator()` call is a fresh instance that burns one whole
 *    block, so the cross-language sequence is deterministic: J:1000,
 *    J:1010, TS:1020+…, J:1030 (alloc=10). Exact numbers asserted.
 *
 * Probe commands bits48-*, hll48-*, geo48-*, idgen48-*.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { GeoSearchArgs } from '../src';
import { suffixName } from '../src/util';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48w:';

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
    console.warn(`[live interop48w] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48w: bitset / hll / geo / idgen vs Java 4.8.0', () => {
  it('RBitSet: bits and 64-bit segments agree both ways; BITOP OR cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b1 = `${NS}b1`;
    const b2 = `${NS}b2`;
    const dest = `${NS}b3`;
    try {
      await r.getKeys().delete(b1, b2, dest);

      // Java writes single bits; TS reads them
      await probe48.probe(`bits48-set ${b1} 0 true`);
      await probe48.probe(`bits48-set ${b1} 3 true`);
      const bs1 = r.getBitSet(b1);
      expect(await bs1.get(0)).toBe(true);
      expect(await bs1.get(1)).toBe(false);
      expect(await bs1.cardinality()).toBe(2);

      // TS writes; Java reads cardinality + a bit
      await bs1.set(7, true);
      await bs1.set(9, true);
      expect(await probe48.probe(`bits48-card ${b1}`)).toBe('card=4');
      expect(await probe48.probe(`bits48-get ${b1} 7`)).toBe('bit=true');

      // 64-bit segment at bit 64: TS sets, Java reads (and reverse). BITFIELD
      // windows are MSB-first: value 5 (0b101) → LSB at bit 127, next 1 at 125.
      expect(await bs1.setLong(64, 5)).toBe(0);
      expect(await probe48.probe(`bits48-long-get ${b1} 64`)).toBe('value=5');
      await probe48.probe(`bits48-long-set ${b2} 64 6`);
      expect(await r.getBitSet(b2).getLong(64)).toBe(6);

      // BITOP OR: Java ORs b1|b2 into dest, TS reads the union
      await probe48.probe(`bits48-or ${dest} ${b1} ${b2}`);
      expect([...(await r.getBitSet(dest).asBitSet())].sort((a, z) => a - z)).toEqual([0, 3, 7, 9, 125, 126, 127]);
    } finally {
      await r.getKeys().delete(b1, b2, dest);
      await r.shutdown();
    }
  }, 300000);

  it('RHyperLogLog: shared register counts identically on both sides', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}hll`;
    try {
      await r.getKeys().delete(name);

      for (const v of [1, 2, 3, 4, 5]) {
        await probe48.probe(`hll48-add ${name} ${v}`);
      }
      const hll = r.getHyperLogLog<number>(name);
      expect(await hll.count()).toBe(5); // small-range correction is exact here

      await hll.add(6);
      await hll.add(7);
      expect(await probe48.probe(`hll48-count ${name}`)).toBe('count=7');
      expect(await hll.count()).toBe(7);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RGeo: dist/pos/search agree both directions on codec members', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}geo`;
    try {
      await r.getKeys().delete(name);

      // Java adds Palermo + Catania; TS measures and searches
      await probe48.probe(`geo48-add ${name} 13.361389 38.115556 Palermo`);
      await probe48.probe(`geo48-add ${name} 15.087269 37.502669 Catania`);
      const geo = r.getGeo<string>(name);
      const d = await geo.dist('Palermo', 'Catania');
      expect(d).not.toBeNull();
      expect(d!).toBeGreaterThan(160000);
      expect(d!).toBeLessThan(172000); // ≈166.3km real distance

      const pos = (await geo.pos('Palermo'))[0];
      expect(pos?.lon).toBeCloseTo(13.3614, 2);
      expect(pos?.lat).toBeCloseTo(38.1156, 2);

      const near = await geo.search(13.361389, 38.115556, 200000, 'm');
      expect(near.map((e) => e.member).sort()).toEqual(['Catania', 'Palermo']);

      // TS adds Taormina; Java's radius search sees both TS and Java members
      await geo.add(15.2862, 37.8511, 'Taormina');
      const lines = await probe48.probeLines(`geo48-search ${name} 15.087269 37.502669 100000`);
      expect(lines.map((l) => l.replace('member=', '')).sort()).toEqual(['Catania', 'Taormina']);
      // Java-computed distance agrees with TS's within rounding
      const jd = await probe48.probe(`geo48-dist ${name} Palermo Taormina`);
      const meters = Number(jd.replace('dist=', ''));
      expect(Math.abs(meters - (await geo.dist('Palermo', 'Taormina'))!)).toBeLessThan(2);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RIdGenerator: block allocation [old, old+alloc) deterministic across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}idg`;
    const allocKey = suffixName(name, 'allocation'); // suffixName braces the name
    try {
      await r.getKeys().delete(name, allocKey);

      expect(await probe48.probe(`idgen48-init ${name} 1000 10`)).toBe('init=true');

      // Java probe call #1: fresh instance claims [1000..1009], returns 1000
      expect(await probe48.probe(`idgen48-next ${name}`)).toBe('next=1000');
      // probe call #2: another fresh instance claims [1010..1019]
      expect(await probe48.probe(`idgen48-next ${name}`)).toBe('next=1010');

      // TS instance (kept alive) claims [1020..1029] and serves from memory
      const idg = r.getIdGenerator(name);
      expect(await idg.nextId()).toBe(1020);
      expect(await idg.nextId()).toBe(1021);

      // Java's third call claims [1030..1039] — no duplicates anywhere
      expect(await probe48.probe(`idgen48-next ${name}`)).toBe('next=1030');
    } finally {
      await r.getKeys().delete(name, allocKey);
      await r.shutdown();
    }
  }, 300000);
});
