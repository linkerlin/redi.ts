/**
 * Phase 73 live: Redis 8.8 AR array family — RArray / RCircularBuffer full
 * ports (src/arrays.ts) and the RBatch getCountMin/getTimeSeriesNative
 * facades. The wire command names carry no dots (ARSET, ARGET, ARRING ...,
 * per the Java RedisCommands constants), so the capability gate probes
 * 'ARSET' — NOT 'AR.SET'. The CMS and TS batch checks are gated separately.
 * Local Redis is externally managed and may be down: every case no-ops with
 * a logged skip (never silent, never red) when the server or the command is
 * missing.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import {
  Redisson, ArrayGrepArgs, CountMinInitArgs, TSCreateArgs,
} from '../src';

const NS = 'TsJavaT:p73:';
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

/** Exact companion keys (iron law 9: no SCAN patterns for companion keys). */
function acbSettings(name: string): string {
  return `redisson_acb:{${name}}`;
}

describe('live phase 73: AR.* RArray + RCircularBuffer + batch facades', () => {
  it('RArray set/get/multi-get/set-map/range/scan/count/delete/info/lastItems', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'ARSET'))) {
      console.warn('[live phase73] ARSET unavailable (Redis < 8.8?) — RArray checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const a1 = `${NS}a1`;
    await raw.unlink(a1);
    const arr = r.getArray<string>(a1);
    expect(await arr.set(0, 'zero')).toBe(1);
    expect(await arr.set(5, 'five')).toBe(1);
    expect(await arr.get(0)).toBe('zero');
    expect(await arr.get(5)).toBe('five');
    expect(await arr.get(3)).toBeNull();
    // multi-set + multi-get
    expect(await arr.set(1, 'one', 'two')).toBe(2);
    expect(await arr.get(1, 2, 3)).toEqual(['one', 'two', null]);
    expect(await arr.isSet(3)).toBe(false);
    expect(await arr.isSet(1)).toBe(true);
    // ARMSET via Map
    const a4 = `${NS}a4`;
    await raw.unlink(a4);
    const am = r.getArray<string>(a4);
    expect(await am.set(new Map([[0, 'x'], [3, 'y']]))).toBe(2);
    expect(await am.get(0, 3)).toEqual(['x', 'y']);
    // counts / length / range
    expect(await arr.count()).toBe(4);
    expect(await arr.count(0, 2)).toBe(3);
    expect(await arr.countMatches(0, 2, 'two')).toBe(1);
    expect(await arr.length()).toBe(6);
    expect(await arr.range(0, 2)).toEqual(['zero', 'one', 'two']);
    expect(await arr.range(4, 5)).toEqual([null, 'five']);
    // scan entries (+ LIMIT)
    const scanned = await arr.scan(0, 5);
    expect(scanned.map((e) => [e.index, e.value])).toEqual([[0, 'zero'], [1, 'one'], [2, 'two'], [5, 'five']]);
    expect((await arr.scan(0, 5, 2)).length).toBe(2);
    // delete / deleteRange / deleteRanges
    expect(await arr.delete(1)).toBe(1);
    expect(await arr.count()).toBe(3);
    expect(await arr.deleteRange(0, 0)).toBe(1);
    expect(await arr.deleteRanges(2, 2, 5, 5)).toBe(2);
    expect(await arr.count()).toBe(0);
    // info
    expect(await arr.getInfo()).toMatchObject({ count: 0 });
    expect(await arr.getFullInfo()).toMatchObject({ count: 0 });
    // raw wire: ARGET returns the codec-encoded JSON (Java-parity evidence)
    expect(await raw.call('ARGET', a4, 0)).toBe('"x"');
    await r.getKeys().unlink(a1, a4);
    raw.disconnect();
    await r.shutdown();
  });

  it('RArray insert/ring/next/seek/lastItems/grep/aggregates/iterator', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'ARINSERT'))) {
      console.warn('[live phase73] ARINSERT unavailable (Redis < 8.8?) — insert cursor checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const a2 = `${NS}a2`;
    await raw.unlink(a2);
    const cur = r.getArray<number>(a2);
    expect(await cur.insert(10, 20)).toBe(1); // 10@0, 20@1 → last index 1
    expect(await cur.next()).toBe(2);
    expect(await cur.seek(10)).toBe(true);
    expect(await cur.insert(11)).toBe(10); // 11@10
    expect(await cur.length()).toBe(11);
    expect(await cur.count()).toBe(3);
    expect(await cur.lastItems(2)).toEqual([20, 11]);
    expect(await cur.lastItemsReversed(2)).toEqual([11, 20]);
    // ring
    const a5 = `${NS}a5`;
    await raw.unlink(a5);
    const ring = r.getArray<number>(a5);
    expect(await ring.ring(4, 1, 2, 3, 4, 5)).toBe(0); // 5 wraps onto slot 0
    expect(await ring.count()).toBe(4);
    expect(await ring.get(0)).toBe(5);
    // grep family
    const a3 = `${NS}a3`;
    await raw.unlink(a3);
    const gr = r.getArray<string>(a3);
    await gr.set(0, 'apple', 'banana', 'apricot');
    await gr.set(5, 'cherry');
    expect((await gr.grep(ArrayGrepArgs.match('ap'))).sort((x, y) => x - y)).toEqual([0, 2]);
    expect(await gr.grep(ArrayGrepArgs.glob('a*'))).toEqual([0, 2]);
    expect(await gr.grep(ArrayGrepArgs.exact('cherry').noCase())).toEqual([5]);
    expect(await gr.grep(0, 2, ArrayGrepArgs.match('a'))).toEqual([0, 2]);
    expect(await gr.grepEntries(ArrayGrepArgs.exact('cherry'))).toEqual([{ index: 5, value: 'cherry' }]);
    expect(await gr.grepEntries(ArrayGrepArgs.match('ap'))).toEqual([
      { index: 0, value: 'apple' }, { index: 2, value: 'apricot' },
    ]);
    // aggregates (AROP SUM/MIN/MAX/AND/OR/XOR)
    const n1 = `${NS}n1`;
    await raw.unlink(n1);
    const nums = r.getArray<number>(n1);
    await nums.set(0, 1, 2, 3);
    expect(await nums.sum(0, 2)).toBeCloseTo(6, 10);
    expect(await nums.min(0, 2)).toBe(1);
    expect(await nums.max(0, 2)).toBe(3);
    expect(await nums.bitAnd(0, 2)).toBe(1 & 2 & 3);
    expect(await nums.bitOr(0, 2)).toBe(1 | 2 | 3);
    expect(await nums.bitXor(0, 2)).toBe(1 ^ 2 ^ 3);
    // iterator walks scan pages ascending
    const seen: Array<{ index: number; value: number }> = [];
    for await (const e of cur.iterator(2)) seen.push({ index: e.index, value: e.value });
    expect(seen).toEqual([{ index: 0, value: 10 }, { index: 1, value: 20 }, { index: 10, value: 11 }]);
    expect(await cur.getInfo()).toMatchObject({ count: 3, length: 11 });
    const full = await cur.getFullInfo();
    expect(full.count).toBe(3);
    expect(full.sliceSize).toBeGreaterThan(0);
    await r.getKeys().unlink(a2, a3, a5, n1);
    raw.disconnect();
    await r.shutdown();
  });

  it('RCircularBuffer capacity/add/addAll/readAll/peeks/aggregates/clear', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'ARRING'))) {
      console.warn('[live phase73] ARRING unavailable (Redis < 8.8?) — RCircularBuffer checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const cb1 = `${NS}cb1`;
    const cb1s = acbSettings(cb1);
    await raw.unlink(cb1, cb1s);
    const cb = r.getCircularBuffer<number>(cb1);
    expect(await cb.trySetCapacity(3)).toBe(true);
    expect(await cb.trySetCapacity(5)).toBe(false); // second trySet legally false
    expect(await cb.capacity()).toBe(3);
    // plain long on the wire (LongCodec on the Java side)
    expect(await raw.get(cb1s)).toBe('3');
    await cb.add(1);
    await cb.add(2);
    await cb.add(3);
    expect(await cb.size()).toBe(3);
    expect(await cb.isFull()).toBe(true);
    expect(await cb.remainingCapacity()).toBe(0);
    await cb.add(4); // overwrites the oldest slot
    expect(await cb.readAll()).toEqual([2, 3, 4]);
    expect(await cb.peekLast()).toBe(4);
    expect(await cb.contains(3)).toBe(true);
    expect(await cb.contains(9)).toBe(false);
    expect(await cb.count(4)).toBe(1);
    expect(await cb.lastItems(2, false)).toEqual([3, 4]);
    expect(await cb.lastItems(2, true)).toEqual([4, 3]);
    // addAll + set(size, values) + aggregates
    const cb3 = `${NS}cb3`;
    const cb3s = acbSettings(cb3);
    await raw.unlink(cb3, cb3s);
    const fresh = r.getCircularBuffer<number>(cb3);
    expect(await fresh.set(4, 10, 20, 30)).toBe(2); // fresh cursor 0 → last write at 2
    expect(await fresh.capacity()).toBe(4);
    expect(await fresh.size()).toBe(3);
    expect(await fresh.get(0, 1, 2)).toEqual([10, 20, 30]);
    expect(await fresh.range(0, 1)).toEqual([10, 20]);
    expect(await fresh.sum()).toBe(60);
    expect(await fresh.sum(0, 1)).toBe(30);
    expect(await fresh.min()).toBe(10);
    expect(await fresh.max()).toBe(30);
    expect(await fresh.average()).toBeCloseTo(20, 10);
    expect(await fresh.bitAnd()).toBe(10 & 20 & 30);
    expect(await fresh.bitOr()).toBe(10 | 20 | 30);
    expect(await fresh.bitXor()).toBe(10 ^ 20 ^ 30);
    expect(await fresh.bitAnd(0, 0)).toBe(10);
    // clear keeps capacity; empty aggregates go null
    await fresh.clear();
    expect(await fresh.isEmpty()).toBe(true);
    expect(await fresh.capacity()).toBe(4);
    expect(await fresh.sum()).toBeNull();
    expect(await fresh.average()).toBeNull();
    expect(await fresh.bitAnd()).toBeNull();
    // setCapacity override
    await fresh.setCapacity(7);
    expect(await fresh.capacity()).toBe(7);
    // delete removes array + settings key
    expect(await cb.delete()).toBe(true);
    expect(await raw.exists(cb1, cb1s)).toBe(0);
    await r.getKeys().unlink(cb3, cb3s);
    raw.disconnect();
    await r.shutdown();
  });

  it('RBatch getCountMin facade round-trip (init+add+execute+count)', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'CMS.INITBYDIM'))) {
      console.warn('[live phase73] CMS.* module unavailable — batched CountMin checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const k = `${NS}cms`;
    await raw.unlink(k);
    const batch = r.createBatch();
    const cms = batch.getCountMin<number>(k);
    cms.init(CountMinInitArgs.byDimensions(2000, 5));
    const added = cms.add('phase73', 2);
    const counted = cms.count('phase73');
    const infoP = cms.getInfo();
    await batch.execute();
    expect(await added).toBe(2);
    expect(await counted).toBe(2);
    expect(Number((await infoP).width)).toBe(2000);
    expect(Number((await infoP).depth)).toBe(5);
    await r.getKeys().unlink(k);
    raw.disconnect();
    await r.shutdown();
  });

  it('RBatch getTimeSeriesNative facade round-trip', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'TS.ADD'))) {
      console.warn('[live phase73] TS.* module unavailable — batched TimeSeriesNative checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const k = `${NS}tsb`;
    await raw.unlink(k);
    const batch = r.createBatch();
    const ts = batch.getTimeSeriesNative(k);
    ts.create(TSCreateArgs.defaults().retention(600000));
    const t0 = Date.now() - 60000;
    const addP = ts.add(t0, 1.5);
    const curP = ts.addCurrent(2.5);
    const getP = ts.get();
    const rangeP = ts.range(0, Date.now() + 60000);
    const revP = ts.rangeReversed(0, Date.now() + 60000);
    const infoP = ts.getInfo();
    await batch.execute();
    expect(await addP).toBe(t0);
    expect(await curP).toBeGreaterThan(t0);
    expect((await getP)!.values).toEqual([2.5]);
    expect((await rangeP).map((s) => s.values[0])).toEqual([1.5, 2.5]);
    expect((await revP).map((s) => s.values[0])).toEqual([2.5, 1.5]);
    expect((await infoP).totalSamples).toBe(2);
    await r.getKeys().unlink(k);
    raw.disconnect();
    await r.shutdown();
  });
});
