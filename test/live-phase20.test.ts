/**
 * Phase 20 live interop: RMap HSCAN views + HRANDFIELD/HSTRLEN,
 * RScoredSortedSet score ops + reversed ranges, RBitSet BITFIELD/BITOP,
 * RGeo GeoSearchArgs family + GEOSEARCHSTORE (bidirectional).
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, BitFieldArgs, GeoSearchArgs } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p20:';
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
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

/** Parses `m=1.5;n=2` payloads into a Map. */
function javaEntries(out: string, prefix: string): Map<string, number> {
  const body = out.slice(prefix.length);
  const m = new Map<string, number>();
  if (!body) return m;
  for (const kv of body.split(';')) {
    const i = kv.indexOf('=');
    m.set(kv.slice(0, i), Number(kv.slice(i + 1)));
  }
  return m;
}

describe('live interop phase 20: map/zset/bitset/geo deep API', () => {
  it('RMap HSCAN iterators + HRANDFIELD/HSTRLEN work both ways', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}map`;
    const name2 = `${NS}map2`;
    try {
      await r.getKeys().delete(name, name2);
      const m = r.getMap<string, string>(name);
      await m.putAll({ a: '1', b: '2', c: '3' });

      // TS-written hash is readable by Java valueSize / randomKeys / entrySet HSCAN view
      expect(await probe(`map-value-size ${name} a`)).toBe('MAP_VSIZE:3');
      const rk = await probe(`map-random-keys ${name} 2`);
      expect(rk.startsWith('MAP_RKEYS:')).toBe(true);
      const jkeys = rk.slice('MAP_RKEYS:'.length).replace(/[[\]]/g, '').split(',').map((s) => s.trim()).filter(Boolean);
      expect(jkeys.length).toBe(2);
      expect(jkeys.every((k) => ['a', 'b', 'c'].includes(k))).toBe(true);
      expect(await probe(`map-random-entries ${name} 3`)).toBe('MAP_RENTRIES:a=1;b=2;c=3');
      expect(await probe(`map-scan-entries ${name} * 10`)).toBe('MAP_SCAN_ENTRIES:a=1;b=2;c=3');
      expect(await probe(`map-scan-keys ${name} * 10`)).toBe('MAP_SCAN_KEYS:[a, b, c]');

      // Java-written entries are readable by TS iterators / HRANDFIELD
      expect(await probe(`map-put ${name2} x 10`)).toBe('MAP_PUT');
      expect(await probe(`map-put ${name2} y 20`)).toBe('MAP_PUT');
      const m2 = r.getMap<string, string>(name2);
      const pairs: [string, string][] = [];
      for await (const e of m2.entriesIterator('*', 1)) pairs.push(e);
      expect(pairs.sort()).toEqual([
        ['x', '10'],
        ['y', '20'],
      ]);
      const jvalues: string[] = [];
      for await (const v of m2.valuesIterator('*', 1)) jvalues.push(v);
      expect(jvalues.sort()).toEqual(['10', '20']);
      expect((await m2.randomKeys(2)).sort()).toEqual(['x', 'y']);
      expect([...((await m2.randomEntries(2)).keys())].sort()).toEqual(['x', 'y']);
      // Java encodes String "10" as `"10"` → 4 bytes
      expect(await m2.valueSize('x')).toBe(4);
      expect(await probe(`map-value-size ${name2} x`)).toBe('MAP_VSIZE:4');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('RScoredSortedSet score ops + reversed ranges interop', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}zs`;
    const name2 = `${NS}zs2`;
    try {
      await r.getKeys().delete(name, name2);
      const z = r.getScoredSortedSet<string>(name);
      await z.add(1, 'a');
      await z.add(2, 'b');
      await z.add(3, 'c');

      // Java reads the TS-written set through the new range family
      expect(await probe(`zset-first-last ${name}`)).toBe('ZSET_FIRST_LAST:1.0|3.0');
      expect(await probe(`zset-rev ${name} 0 -1`)).toBe('ZSET_REV:[c, b, a]');
      expect(await probe(`zset-rev-score ${name} 2 true 3 true`)).toBe('ZSET_REV_SCORE:[c, b]');
      expect(await probe(`zset-entry-rev ${name} 0 -1`)).toBe('ZSET_ENTRY_REV:c=3.0;b=2.0;a=1.0');
      expect(await probe(`zset-entry-range ${name} 1 true 3 true`)).toBe('ZSET_ENTRY_RANGE:a=1.0;b=2.0;c=3.0');
      expect(await probe(`zset-count ${name} 2 true 3 true`)).toBe('ZSET_COUNT:2');
      expect(await probe(`zset-rank ${name} c`)).toBe('ZSET_RANKS:2|0');
      expect(await probe(`zset-rank ${name} missing`)).toBe('ZSET_RANKS:null|null');
      expect(await probe(`zset-addscore ${name} c -1`)).toBe('ZSET_ADDSCORE:2.0');
      expect(await z.getScore('c')).toBe(2);
      expect(await z.firstScore()).toBe(1);
      expect(await z.lastScore()).toBe(2);

      // Java writes a fresh set; TS reads it back with the same API family
      expect(await probe(`zset-addscore ${name2} m1 5`)).toBe('ZSET_ADDSCORE:5.0');
      expect(await probe(`zset-addscore ${name2} m2 10`)).toBe('ZSET_ADDSCORE:10.0');
      expect(await probe(`zset-addscore ${name2} m3 15`)).toBe('ZSET_ADDSCORE:15.0');
      const z2 = r.getScoredSortedSet<string>(name2);
      expect(await z2.firstScore()).toBe(5);
      expect(await z2.lastScore()).toBe(15);
      expect(await z2.valueRangeReversed(0, -1)).toEqual(['m3', 'm2', 'm1']);
      expect(await z2.valueRangeReversed(5, true, 10, true)).toEqual(['m2', 'm1']);
      expect((await z2.entryRangeReversed(5, true, 10, true)).map((e) => e.value)).toEqual(['m2', 'm1']);
      expect((await z2.entryRange(5, true, 15, true)).map((e) => e.score)).toEqual([5, 10, 15]);
      expect(await z2.rank('m2')).toBe(1);
      expect(await z2.revRank('m2')).toBe(1);
      expect(await z2.rank('missing')).toBe(null);
      expect(await z2.count(5, true, 15, false)).toBe(2);
      expect(await z2.addScoreAndGetRank('m3', 5)).toBe(2);
      expect(await z2.addScoreAndGetRevRank('m3', -5)).toBe(0);

      // TS deletions are visible to Java
      expect(await z2.removeRangeByScore(5, true, 5, true)).toBe(1);
      expect(await probe(`zset-score ${name2} m1`)).toBe('ZSCORE:null');
      expect(await z2.removeRangeByRank(0, 0)).toBe(1);
      expect(await probe(`zset-score ${name2} m2`)).toBe('ZSCORE:null');
      expect(await z2.range(0, -1)).toEqual(['m3']);
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('RBitSet BITFIELD typed ops + BITOP family interop', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bits`;
    try {
      await r.getKeys().delete(name);
      const b = r.getBitSet(name);

      // Java writes typed values (BITFIELD u8/i16/i64), TS reads
      expect(await probe(`bitset-typed-set ${name} u 8 0 200`)).toBe('BITS_TSET:0');
      expect(await b.getUnsigned(8, 0)).toBe(200);
      expect(await probe(`bitset-typed-set ${name} i 16 8 -1234`)).toBe('BITS_TSET:0');
      expect(await b.getSigned(16, 8)).toBe(-1234);
      expect(await probe(`bitset-long-set ${name} 64 1099511627776`)).toBe('BITS_LSET:0');
      expect(await b.getLong(64)).toBe(1099511627776);

      // TS writes typed values, Java reads
      expect(await b.setUnsigned(8, 0, 7)).toBe(200);
      expect(await probe(`bitset-typed-get ${name} u 8 0`)).toBe('BITS_TGET:7');
      expect(await b.incrementAndGetSigned(16, 8, 1234)).toBe(0);
      expect(await probe(`bitset-typed-get ${name} i 16 8`)).toBe('BITS_TGET:0');
      expect(await b.getByte(128)).toBe(0);
      expect(await b.incrementAndGetByte(128, -5)).toBe(-5);
      expect(await probe(`bitset-typed-get ${name} i 8 128`)).toBe('BITS_TGET:-5');

      // setRange/clear ranges are `to`-exclusive on both sides (Java loop i < to)
      await b.clear();
      await b.setRange(0, 4);
      expect(await b.cardinality()).toBe(4);
      expect(await probe(`bitset-length ${name}`)).toBe('BITS_LEN:4');
      expect(await probe(`bitset-cardinality ${name}`)).toBe('BITS_CARD:4');
      expect(await probe(`bitset-setrange ${name} 8 12 true`)).toBe('BITS_SETRANGE');
      expect(await b.get(8)).toBe(true);
      expect(await b.get(11)).toBe(true);
      expect(await b.get(12)).toBe(false);
      expect(await b.cardinality()).toBe(8);
      // size() = STRLEN × 8, length() = highest set bit + 1 (bit 11 is the highest set)
      expect(await b.size()).toBe(16);
      expect(await b.length()).toBe(12);
      expect(await probe(`bitset-size ${name}`)).toBe('BITS_SIZE:16');
      expect(await probe(`bitset-length ${name}`)).toBe('BITS_LEN:12');
      expect(await probe(`bitset-clearrange ${name} 8 12`)).toBe('BITS_CLEARRANGE');
      expect(await b.cardinality()).toBe(4);

      // raw bytes written by TS are visible to Java
      await b.fromByteArray(Buffer.from([0b11110000]));
      expect(await probe(`bitset-cardinality ${name}`)).toBe('BITS_CARD:4');
      expect(await probe(`bitset-typed-get ${name} u 8 0`)).toBe('BITS_TGET:240');

      // BITFIELD builder: Java and TS produce identical replies incl. OVERFLOW FAIL nulls
      const name3 = `${NS}bits3`;
      await r.getKeys().delete(name3);
      const b3 = r.getBitSet(name3);
      expect(await probe(`bitset-bitfield ${name3} SET:u8:0:5,GET:u8:0`)).toBe('BITS_BITFIELD:[0, 5]');
      expect(await b3.getUnsigned(8, 0)).toBe(5);
      const res = await b3.bitField(
        new BitFieldArgs().getUnsigned(8, 0).setUnsigned(8, 0, 9).overflow('FAIL').incrementUnsignedBy(8, 0, 250),
      );
      // BITFIELD replies: GET→5, SET→old value 5, OVERFLOW is a modifier (no reply), INCRBY under FAIL→null
      expect(res).toEqual([5, 5, null]);
      expect(await probe(`bitset-typed-get ${name3} u 8 0`)).toBe('BITS_TGET:9');
      expect(await probe(`bitset-bitfield ${name3} GET:u8:0,OVERFLOW:sat,INCRBY:u8:0:250`)).toBe('BITS_BITFIELD:[9, 255]');
      expect(await b3.getUnsigned(8, 0)).toBe(255);

      // BITOP family: classic ops + Redis 8 DIFF/DIFF1/ANDOR/ONE, same result on both sides
      const major = Number(/redis_version:(\d+)/.exec(await r.redis.info('server'))?.[1] ?? '0');
      const ba = r.getBitSet(`${NS}bitopA`);
      const bb = r.getBitSet(`${NS}bitopB`);
      const reset = async () => {
        await r.getKeys().delete(ba.name, bb.name);
        await ba.fromByteArray(Buffer.from([0b11110000]));
        await bb.fromByteArray(Buffer.from([0b10101010]));
      };
      await reset();
      expect(await ba.and(bb.name)).toBe(1);
      expect(await ba.cardinality()).toBe(2);
      expect(await probe(`bitset-cardinality ${ba.name}`)).toBe('BITS_CARD:2');
      await reset();
      expect(await ba.or(bb.name)).toBe(1);
      expect(await ba.cardinality()).toBe(6);
      expect(await probe(`bitset-op or ${ba.name} ${bb.name}`)).toBe('BITS_OP:1');
      expect(await probe(`bitset-cardinality ${ba.name}`)).toBe('BITS_CARD:6');
      await reset();
      expect(await ba.xor(bb.name)).toBe(1);
      expect(await ba.cardinality()).toBe(4);
      await reset();
      expect(await ba.not()).toBe(1);
      expect(await ba.cardinality()).toBe(4); // ~11110000 = 00001111
      if (major >= 8) {
        await reset();
        expect(await ba.diff(bb.name)).toBe(1); // A & ~B
        expect(await ba.cardinality()).toBe(2);
        expect(await probe(`bitset-op diff ${ba.name} ${bb.name}`)).toBe('BITS_OP:1');
        expect(await probe(`bitset-cardinality ${ba.name}`)).toBe('BITS_CARD:2');
        await reset();
        expect(await ba.diffInverse(bb.name)).toBe(1); // B & ~A
        expect(await ba.cardinality()).toBe(2);
        await reset();
        expect(await ba.andOr(bb.name)).toBe(1); // A & B
        expect(await ba.cardinality()).toBe(2);
        await reset();
        expect(await ba.setExclusive(bb.name)).toBe(1); // A ^ B
        expect(await ba.cardinality()).toBe(4);
        expect(await probe(`bitset-op one ${ba.name} ${bb.name}`)).toBe('BITS_OP:1');
        expect(await probe(`bitset-cardinality ${ba.name}`)).toBe('BITS_CARD:4');
      }
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('RGeo GeoSearchArgs family + GEOSEARCHSTORE interop', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}geo`;
    const dest = `${NS}geodest`;
    const dest2 = `${NS}geostore`;
    const dest3 = `${NS}geosorted`;
    try {
      await r.getKeys().delete(name, dest, dest2, dest3);
      const g = r.getGeo<string>(name);
      await g.add(13.361389, 38.115556, 'Palermo');
      await g.add(15.087269, 37.502669, 'Catania');

      // TS-written members are found by Java through all three search shapes
      expect(await probe(`geo-search-radius ${name} 15 37 200 km`)).toBe('GEO_SEARCH:[Catania, Palermo]');
      expect(await probe(`geo-search-member ${name} Palermo 200 km`)).toBe('GEO_SEARCH_MEMBER:[Catania, Palermo]');
      expect(await probe(`geo-search-box ${name} 15 37 200 200 km`)).toBe('GEO_SEARCH_BOX:[Catania]');
      expect(await probe(`geo-search-box ${name} 15 37 400 400 km`)).toBe('GEO_SEARCH_BOX:[Catania, Palermo]');
      expect(await probe(`geo-search-radius ${name} 15 37 200 km 1 asc`)).toBe('GEO_SEARCH:[Catania]');
      expect(await probe(`geo-search-member ${name} Palermo 100 km`)).toBe('GEO_SEARCH_MEMBER:[Palermo]');

      // distances agree (documented example: Palermo 190.4424 km, Catania 56.4413 km from (15, 37))
      const jdist = javaEntries(await probe(`geo-search-dist ${name} 15 37 200 km`), 'GEO_SEARCH_DIST:');
      expect(jdist.get('Palermo')).toBeCloseTo(190.4424, 3);
      expect(jdist.get('Catania')).toBeCloseTo(56.4413, 3);
      const tdist = await g.searchWithDistance(GeoSearchArgs.from(15, 37).radius(200, 'km'));
      expect(tdist.get('Palermo')).toBeCloseTo(190.4424, 3);
      expect(tdist.get('Catania')).toBeCloseTo(56.4413, 3);
      // box path goes through GEOSEARCH; same distances
      const jdistBox = javaEntries(await probe(`geo-search-dist-box ${name} 15 37 400 400 km`), 'GEO_SEARCH_DIST_BOX:');
      const tdistBox = await g.searchWithDistance(GeoSearchArgs.from(15, 37).box(400, 400, 'km'));
      expect(tdistBox.get('Palermo')).toBeCloseTo(jdistBox.get('Palermo')!, 3);
      expect(tdistBox.get('Catania')).toBeCloseTo(jdistBox.get('Catania')!, 3);

      // positions agree with Java's GeoPosition decoder
      const jpos = (await probe(`geo-search-pos ${name} 15 37 200 km`)).slice('GEO_SEARCH_POS:'.length);
      const jposMap = new Map(jpos.split(';').map((kv) => {
        const i = kv.indexOf('=');
        const [lon, lat] = kv.slice(i + 2, -1).split(',');
        return [kv.slice(0, i), [Number(lon), Number(lat)] as [number, number]];
      }));
      const tpos = await g.searchWithPosition(GeoSearchArgs.from(15, 37).radius(200, 'km'));
      expect(tpos.get('Palermo')!.lon).toBeCloseTo(jposMap.get('Palermo')![0], 5);
      expect(tpos.get('Palermo')!.lat).toBeCloseTo(jposMap.get('Palermo')![1], 5);

      // TS GEOSEARCHSTORE result is a readable geo set for Java
      expect(await g.storeSearchTo(dest, GeoSearchArgs.from(15, 37).box(400, 400, 'km'))).toBe(2);
      expect((await probe(`geo-pos ${dest} Palermo`)).startsWith('GEO_POS:[')).toBe(true);
      // Java GEOSEARCHSTORE result is readable by TS
      expect(await probe(`geo-store-search ${name} ${dest2} 15 37 400 400 km`)).toBe('GEO_STORE:2');
      expect(await (await r.redis.zscore(dest2, '"Palermo"')) !== null).toBe(true);
      expect(await r.getGeo<string>(dest2).pos('Catania')).not.toBe(null);
      // legacy STOREDIST path stores distances as scores
      expect(await g.storeSortedSearchTo(dest3, GeoSearchArgs.from(15, 37).radius(200, 'km'))).toBe(2);
      const scored = Number(await r.redis.zscore(dest3, '"Catania"'));
      expect(scored).toBeCloseTo(56.4413, 3);
      expect(await probe(`geo-store-sorted ${name} ${dest3} 15 37 200 km`)).toBe('GEO_STORE_SORTED:2');

      // addIfExists family both ways
      expect(await g.addIfExists(13.5, 38.5, 'Ghost')).toBe(false);
      expect(await g.addIfExists(13.5, 38.5, 'Palermo')).toBe(true);
      expect((await g.pos('Palermo'))[0]!.lon).toBeCloseTo(13.5, 3);
      expect(await probe(`geo-add-if-exists ${name} 15.5 37.5 Catania`)).toBe('GEO_AIE:true');
      expect(await probe(`geo-add-if-exists ${name} 15.5 37.5 Ghost`)).toBe('GEO_AIE:false');
      // GEOADD XX only counts added elements, so the reply is 0 even when the member moves;
      // check both sides' replies *and* the actual movement.
      expect(await probe(`geo-add-if-exists-xx ${name} 15.7 37.7 Catania`)).toBe('GEO_AIE_XX:0');
      expect((await g.pos('Catania'))[0]!.lon).toBeCloseTo(15.7, 3);
      expect(await g.addIfExists([{ lon: 15.8, lat: 37.8, member: 'Catania' }])).toBe(0);
      expect((await g.pos('Catania'))[0]!.lat).toBeCloseTo(37.8, 3);
      expect(await probe(`geo-add-if-exists-xx ${name} 15.7 37.7 Ghost2`)).toBe('GEO_AIE_XX:0');
      expect(await g.addIfExists([{ lon: 15.7, lat: 37.7, member: 'Ghost3' }])).toBe(0);
    } finally {
      await r.shutdown();
    }
  }, 180000);
});
