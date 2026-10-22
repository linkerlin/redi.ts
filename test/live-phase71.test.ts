/**
 * Phase 71 live: Redisson 4.8 OSS additions — RTimeSeriesNative /
 * RTimeSeriesNatives (RedisTimeSeries module TS.* commands) and RMaps (mass
 * map writes; HIMPORT when the server has it, pipelined DEL+HSET fallback
 * writing plain codec-encoded HASHes). TS.* groups are gated per command via
 * COMMAND INFO (the module ships with Redis Stack, not OSS 8.x). No Java-side
 * probe command fits the new surface, so Java parity rests on the verbatim
 * command shapes asserted here plus the raw-reply wire checks.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import {
  Redisson, TSCreateArgs, TSAddArgs, TSRangeArgs, TSMultiGetArgs, TSMultiRangeArgs,
  TSGroupedRangeArgs, MapsImportArgs,
} from '../src';

const NS = 'TsJavaT:p71:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    r.on('error', () => undefined);
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await (raw as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

describe('live phase 71: TimeSeries native (TS.*) + RMaps', () => {
  it('RTimeSeriesNative create/add/get/range with labels and retention', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'TS.ADD'))) {
      console.warn('[live phase71] TS.* module unavailable — RTimeSeriesNative checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const ts = r.getTimeSeriesNative(`${NS}n1`);
    await raw.unlink(`${NS}n1`);
    await ts.create(TSCreateArgs.defaults().retention(600000).duplicatePolicy('LAST')
      .label('area', '32').label('sensor', 'n1'));
    expect(await ts.createIfAbsent(TSCreateArgs.defaults())).toBe(false); // already exists
    // timestamps must sit inside the retention window — newer TimeSeries
    // modules trim out-of-retention samples on write, so fixed 1970-era
    // values would vanish on up-to-date servers
    const t0 = Date.now() - 60000;
    expect(await ts.add(t0, 1)).toBe(t0);
    expect(await ts.add(TSAddArgs.sample(t0 + 1000, 2.5))).toBe(t0 + 1000);
    const serverTs = await ts.addCurrent(3.5);
    expect(serverTs).toBeGreaterThan(t0 + 1000);
    const last = await ts.get();
    expect(last!.values).toEqual([3.5]);
    expect(last!.timestamp).toBe(serverTs);
    const samples = await ts.range(TSRangeArgs.range(t0, t0 + 1000));
    expect(samples.map((s) => s.values[0])).toEqual([1, 2.5]);
    expect((await ts.rangeReversed(0, t0 + 1000)).map((s) => s.timestamp)).toEqual([t0 + 1000, t0]);
    expect((await ts.first())!.timestamp).toBe(t0);
    expect(await ts.size()).toBe(3);
    expect(await ts.firstTimestamp()).toBe(t0);
    expect(await ts.lastTimestamp()).toBe(serverTs);
    expect(await ts.getLabels()).toEqual({ area: '32', sensor: 'n1' });
    const info = await ts.getInfo();
    expect(info.totalSamples).toBe(3);
    expect(info.retentionTimeMs).toBe(600000);
    expect(info.labels).toEqual({ area: '32', sensor: 'n1' });
    expect(info.chunkType).toBe('COMPRESSED');
    expect(await ts.removeRange(t0, t0)).toBe(1);
    expect(await ts.size()).toBe(2);
    // raw wire: TS.RANGE replies [ts, [value]] pairs
    const rawReply: any = await raw.call('TS.RANGE', `${NS}n1`, '-', '+');
    expect(rawReply.length).toBe(2);
    expect(Number(rawReply[1][0])).toBe(serverTs);
    expect(Number(rawReply[1][1][0])).toBeCloseTo(3.5, 10);
    await r.getKeys().unlink(`${NS}n1`);
    raw.disconnect();
    await r.shutdown();
  });

  it('RTimeSeriesNatives batch add/get across series', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'TS.MADD'))) {
      console.warn('[live phase71] TS.MADD unavailable — RTimeSeriesNatives batch checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const multi = r.getTimeSeriesNatives();
    const k1 = `${NS}s1`;
    const k2 = `${NS}s2`;
    await raw.unlink(k1, k2);
    await r.getTimeSeriesNative(k1).create(TSCreateArgs.defaults().label('area', '32').label('sensor', '1'));
    await r.getTimeSeriesNative(k2).create(TSCreateArgs.defaults().label('area', '32').label('sensor', '2'));
    const added = await multi.addAll({
      [k1]: { 1000: 1, 2000: 2 },
      [k2]: { 1000: 10 },
    });
    expect(added.get(k1)).toEqual([1000, 2000]);
    expect(added.get(k2)).toEqual([1000]);

    const all = await multi.getAll(TSMultiGetArgs.filter('area=32').withLabels());
    expect(all.size).toBe(2);
    expect(all.get(k1)!.labels).toEqual({ area: '32', sensor: '1' });
    expect(all.get(k1)!.sample!.values).toEqual([2]);
    expect(all.get(k2)!.sample!.values).toEqual([10]);
    const selected = await multi.getAll(TSMultiGetArgs.filter('area=32').selectedLabels('sensor'));
    expect(selected.get(k2)!.labels).toEqual({ sensor: '2' });

    const ranged = await multi.range(TSMultiRangeArgs.filter('area=32').withLabels().from(0).count(10));
    expect(ranged.get(k1)!.samples.map((s) => s.values[0])).toEqual([1, 2]);
    expect(ranged.get(k2)!.samples.map((s) => s.values[0])).toEqual([10]);
    const rev = await multi.rangeReversed(TSMultiRangeArgs.filter('area=32'));
    expect(rev.get(k1)!.samples.map((s) => s.timestamp)).toEqual([2000, 1000]);

    expect((await multi.queryIndex('area=32')).sort()).toEqual([k1, k2].sort());
    expect((await multi.queryIndex('area=32', 'sensor=1'))).toEqual([k1]);

    if (await canCmd(raw, 'TS.NRANGE')) {
      const grouped = await multi.groupedRange(TSGroupedRangeArgs.keys(k1, k2).range(0, 3000));
      // one row per timestamp; k2 missing at 2000 → NaN there
      expect(grouped.map((s) => s.timestamp)).toEqual([1000, 2000]);
      expect(grouped[0].values).toEqual([1, 10]);
      expect(grouped[1].values[0]).toBe(2);
      expect(Number.isNaN(grouped[1].values[1])).toBe(true);
    } else {
      console.warn('[live phase71] TS.NRANGE unavailable — groupedRange check skipped');
    }
    if (await canCmd(raw, 'TS.QUERYLABELS')) {
      expect((await multi.labelNames('area=32')).sort()).toEqual(['area', 'sensor']);
      expect(await multi.labelValues('sensor', 'area=32')).toEqual(['1', '2']);
    } else {
      console.warn('[live phase71] TS.QUERYLABELS unavailable — labelNames/Values check skipped');
    }
    await r.getKeys().unlink(k1, k2);
    raw.disconnect();
    await r.shutdown();
  });

  it('RMaps set/get/delete across multiple maps', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const maps = r.getMaps<string, number>();
    const m1 = `${NS}map1`;
    const m2 = `${NS}map2`;
    const m3 = `${NS}map3`;
    await raw.unlink(m1, m2, m3);
    await maps.set({ [m1]: { a: 1, b: 2 }, [m2]: { a: 10 }, [m3]: { z: 'other-group' } });
    // read back through RMap (same plain-name HASH + JSON field/value wire)
    const rm1 = r.getMap<string, number>(m1);
    const rm2 = r.getMap<string, number>(m2);
    const rm3 = r.getMap<string, any>(m3);
    expect(await rm1.get('a')).toBe(1);
    expect(await rm1.get('b')).toBe(2);
    expect(await rm2.get('a')).toBe(10);
    expect(await rm3.get('z')).toBe('other-group');
    // Java-readable wire: raw fields/values are codec-encoded exactly as Java
    // RMap writes them (no probe command fits the new surface — the raw check
    // is the Java-parity evidence: JsonJacksonCodec HASH fields/values).
    expect(await raw.hget(m1, '"a"')).toBe('1');
    expect(await raw.hget(m3, '"z"')).toBe('"other-group"');
    // set replaces the whole object
    await maps.set({ [m1]: { only: 7 } });
    expect(await rm1.get('a')).toBeNull();
    expect(await rm1.get('only')).toBe(7);
    // batchSize portions + Map input
    await maps.set(new Map([[m2, new Map([['c', 30]])]]), 1);
    expect(await rm2.get('c')).toBe(30);
    // import session: buffered rows, flush, count
    const imp = maps.createImport(MapsImportArgs.fields('f1', 'f2').batchSize(2));
    await imp.add(m2, 1, 'one');
    await imp.add(m3, 2, 'two');
    expect(await r.getMap<string, any>(m2).get('f1')).toBe(1); // auto-flushed at 2
    await imp.add(m1, 3, 'three');
    expect(await r.getMap<string, any>(m1).get('f1')).toBeNull(); // still buffered
    await imp.flush();
    expect(await r.getMap<string, any>(m1).get('f2')).toBe('three');
    expect(imp.getImportedCount()).toBe(3);
    // delete via the map objects (plain DEL semantics)
    expect(await rm1.delete()).toBe(true);
    expect(await raw.exists(m1)).toBe(0);
    await r.getKeys().unlink(m1, m2, m3);
    raw.disconnect();
    await r.shutdown();
  });
});
