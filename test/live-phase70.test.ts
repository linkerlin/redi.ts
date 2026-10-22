/**
 * Phase 70 live: Redisson 4.8 RTimeSeries expansion — args-based add family
 * (add/addIfAbsent/addOrReplace/addIfLess/addIfGreater/addAndSum/addAndGet with
 * retention), plural accessors (getAll family, removeAll, getAndRemoveAll),
 * readTail, info, ByLabel ranges, labels, aggregate. These are TS-only
 * assertions: no Java-side probe commands exist for the new methods, so wire
 * parity relies on the verbatim 4.8 Lua plus the raw-member shape checks here.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, TimeSeriesAddArgs, TimeSeriesAggregationArgs, TimeSeriesReadArgs } from '../src';

const NS = 'TsJavaT:p70:';
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

describe('live phase 70: TimeSeries 4.8 expansion', () => {
  it('add(args) writes the 4.8 member shape and trims by retention', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts1`;
    const ts = r.getTimeSeries<number>(name);
    await ts.unlink();
    expect(await ts.add(TimeSeriesAddArgs.entry(1000, 1))).toBe(true);
    expect(await ts.add(TimeSeriesAddArgs.entry(2000, 2))).toBe(true);
    expect(await ts.add(TimeSeriesAddArgs.entry(5000, 5).retention(2000))).toBe(true);
    expect(await ts.getAll(1000)).toEqual([]); // below cutoff 5000-2000=3000
    expect(await ts.getAll(2000)).toEqual([]); // cutoff bound is exclusive
    expect(await ts.getAll(5000)).toEqual([5]);
    // an entry outside its own retention window is not written at all
    expect(await ts.add(TimeSeriesAddArgs.entry(1500, 9).retention(2000))).toBe(false);
    expect(await ts.getAll(1500)).toEqual([]);
    // wire: 4.8 members start with the marker byte 4, ids come from the seq HASH
    const members = await raw.zrangeBuffer(name, 0, -1);
    expect(members.length).toBeGreaterThan(0);
    for (const m of members) expect(m[0]).toBe(4);
    const seqId = await raw.hget(`redisson__ts_seq:{${name}}`, 'id');
    expect(seqId).not.toBeNull();
    expect(Number(seqId)).toBeGreaterThanOrEqual(3);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('addIfAbsent occupied / expired-slot semantics', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts2`);
    await ts.unlink();
    expect(await ts.addIfAbsent(TimeSeriesAddArgs.entry(100, 1))).toBe(true);
    expect(await ts.addIfAbsent(TimeSeriesAddArgs.entry(100, 2))).toBe(false); // occupied
    expect(await ts.getAll(100)).toEqual([1]);
    // a slot held only by an expired entry is free again (the stale one is dropped)
    expect(await ts.addIfAbsent(TimeSeriesAddArgs.entry(200, 2).ttl(300))).toBe(true);
    await new Promise((res) => setTimeout(res, 1000)); // > 3x the entry ttl
    expect(await ts.getAll(200)).toEqual([]);
    expect(await ts.addIfAbsent(TimeSeriesAddArgs.entry(200, 3))).toBe(true);
    expect(await ts.getAll(200)).toEqual([3]);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('addOrReplace replaces the whole timestamp', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts3`);
    await ts.unlink();
    expect(await ts.addOrReplace(TimeSeriesAddArgs.entry(1000, 10))).toBe(true); // created
    expect(await ts.addOrReplace(TimeSeriesAddArgs.entry(1000, 20))).toBe(false); // replaced
    expect(await ts.getValue(1000)).toBe(20);
    await ts.add(TimeSeriesAddArgs.entry(1000, 30)); // plain add stacks a second one
    expect(await ts.addOrReplace(TimeSeriesAddArgs.entry(1000, 40))).toBe(false);
    expect(await ts.getAll(1000)).toEqual([40]); // exactly one entry left
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('addIfLess / addIfGreater / addAndSum folds and addAndGet running total', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts4`);
    await ts.unlink();
    expect(await ts.addIfGreater(TimeSeriesAddArgs.entry(100, 5))).toBe(true);
    expect(await ts.addIfGreater(TimeSeriesAddArgs.entry(100, 3))).toBe(false);
    expect(await ts.addIfLess(TimeSeriesAddArgs.entry(100, 6))).toBe(false);
    expect(await ts.addIfLess(TimeSeriesAddArgs.entry(100, 4.5))).toBe(true);
    expect(await ts.getValue(100)).toBe(4.5);

    expect(await ts.addAndSum(TimeSeriesAddArgs.entry(200, 5))).toBe(true);
    expect(await ts.addAndSum(TimeSeriesAddArgs.entry(200, 7))).toBe(false);
    expect(await ts.getValue(200)).toBe(12);
    expect(await ts.getAll(200)).toEqual([12]);

    expect(await ts.addAndGet(TimeSeriesAddArgs.entry(300, 5))).toBe(5);
    expect(await ts.addAndGet(TimeSeriesAddArgs.entry(400, 7))).toBe(12);
    expect(await ts.getValue(400)).toBe(12); // the total as of that timestamp
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('getAll family, removeAll counting, getAndRemoveAll', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts5`);
    await ts.unlink();
    for (const v of [10, 11, 12]) await ts.add(TimeSeriesAddArgs.entry(5000, v));
    expect(await ts.getAll(5000)).toEqual([10, 11, 12]);
    const entries = await ts.getAllEntries(5000);
    expect(entries.map((e) => e.value)).toEqual([10, 11, 12]);
    expect(entries.every((e) => e.timestamp === 5000)).toBe(true);
    expect(await ts.removeAll(5000)).toBe(3);
    expect(await ts.removeAll(5000)).toBe(0);

    await ts.add(TimeSeriesAddArgs.entry(6000, 60));
    await ts.add(TimeSeriesAddArgs.entry(6000, 61));
    expect(await ts.getAndRemoveAll(6000)).toEqual([60, 61]);
    expect(await ts.getAll(6000)).toEqual([]);
    await ts.add(TimeSeriesAddArgs.entry(7000, 70));
    const removed = await ts.getAndRemoveAllEntries(7000);
    expect(removed.map((e) => e.value)).toEqual([70]);
    expect(await ts.getAll(7000)).toEqual([]);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('readTail real walk: strict cursor, count floor', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts6`);
    await ts.unlink();
    await ts.add(TimeSeriesAddArgs.entry(1000, 10));
    await ts.add(TimeSeriesAddArgs.entry(3000, 31));
    await ts.add(TimeSeriesAddArgs.entry(3000, 32));
    await ts.add(TimeSeriesAddArgs.entry(4000, 40));
    const batch = await ts.readTail(TimeSeriesReadArgs.after(2000).count(2));
    expect(batch.map((e) => e.value)).toEqual([31, 32]); // whole of timestamp 3000
    expect((await ts.readTail(TimeSeriesReadArgs.after(3000))).map((e) => e.value)).toEqual([40]);
    expect((await ts.readTail(TimeSeriesReadArgs.after(1000).count(1))).map((e) => e.value)).toEqual([31, 32]);
    expect(await ts.readTail(TimeSeriesReadArgs.after(1000).count(-1))).toEqual([]);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('info fields', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts7`);
    await ts.unlink();
    const empty = await ts.info();
    expect(empty.size).toBe(0);
    expect(empty.firstTimestamp).toBeNull();
    expect(empty.lastTimestamp).toBeNull();
    expect(empty.entriesIssued).toBe(0);
    await ts.add(TimeSeriesAddArgs.entry(1000, 1, 'a'));
    await ts.add(TimeSeriesAddArgs.entry(2000, 2, 'b'));
    const info = await ts.info();
    expect(info.size).toBe(2);
    expect(info.totalEntries).toBe(2);
    expect(info.firstTimestamp).toBe(1000);
    expect(info.lastTimestamp).toBe(2000);
    expect(info.entriesIssued).toBeGreaterThanOrEqual(2);
    expect(info.timeToLive).toBe(-1); // no expiry on the collection itself
    expect(info.memoryUsage).toBeGreaterThan(0);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('ByLabel ranges with two labels, labels(), removeRangeByLabel', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts8`);
    await ts.unlink();
    await ts.add(TimeSeriesAddArgs.entry(1000, 1, 'alpha'));
    await ts.add(TimeSeriesAddArgs.entry(2000, 2, 'beta'));
    await ts.add(TimeSeriesAddArgs.entry(3000, 3, 'alpha'));
    await ts.add(TimeSeriesAddArgs.entry(4000, 4)); // no label
    expect((await ts.labels()).sort()).toEqual(['alpha', 'beta']);
    expect(await ts.rangeByLabel(0, 5000, 'alpha')).toEqual([1, 3]);
    expect(await ts.rangeReversedByLabel(0, 5000, 'alpha')).toEqual([3, 1]);
    expect((await ts.entryRangeByLabel(0, 5000, 'beta')).map((e) => e.label)).toEqual(['beta']);
    expect((await ts.entryRangeReversedByLabel(0, 5000, 'alpha')).map((e) => e.value)).toEqual([3, 1]);
    expect(await ts.rangeByLabel(0, 5000, null)).toEqual([4]); // entries without a label
    expect(await ts.rangeByLabel(0, 5000, 'alpha', 1)).toEqual([1]); // limit
    expect(await ts.removeRangeByLabel(0, 5000, 'alpha')).toBe(2);
    expect(await ts.removeRangeByLabel(0, 5000, 'alpha')).toBe(0);
    expect(await ts.size()).toBe(2);
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });

  it('aggregate buckets on the real Lua path', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const ts = r.getTimeSeries<number>(`${NS}ts9`);
    await ts.unlink();
    await ts.add(TimeSeriesAddArgs.entry(1000, 3));
    await ts.add(TimeSeriesAddArgs.entry(2000, 5));
    await ts.add(TimeSeriesAddArgs.entry(3000, 4));
    const buckets = await ts.aggregate(
      TimeSeriesAggregationArgs.between(0, 4000).bucket(2000).count().sum().avg().min().max().first().last()
        .stdDevPopulation().variancePopulation(),
    );
    expect(buckets.length).toBe(2);
    expect(buckets[0].timestamp).toBe(0);
    expect(buckets[0].values).toEqual({ COUNT: 1, SUM: 3, AVG: 3, MIN: 3, MAX: 3, FIRST: 3, LAST: 3, STD_DEV_POPULATION: 0, VARIANCE_POPULATION: 0 });
    expect(buckets[1].timestamp).toBe(2000);
    expect(buckets[1].values).toMatchObject({ COUNT: 2, SUM: 9, AVG: 4.5, MIN: 4, MAX: 5, FIRST: 5, LAST: 4 });
    expect(buckets[1].values.VARIANCE_POPULATION!).toBeCloseTo(0.25, 10);
    expect(buckets[1].values.STD_DEV_POPULATION!).toBeCloseTo(0.5, 10);
    // label filter + value filter
    await ts.add(TimeSeriesAddArgs.entry(2500, 50, 'spike'));
    const labeled = await ts.aggregate(TimeSeriesAggregationArgs.between(0, 4000).bucket(4000).label('spike').count().sum());
    expect(labeled.length).toBe(1);
    expect(labeled[0].values).toEqual({ COUNT: 1, SUM: 50 });
    const filtered = await ts.aggregate(TimeSeriesAggregationArgs.between(0, 4000).bucket(4000).filterByValue(0, 10).count().sum());
    expect(filtered[0].values).toEqual({ COUNT: 3, SUM: 12 });
    await ts.unlink();
    raw.disconnect();
    await r.shutdown();
  });
});
