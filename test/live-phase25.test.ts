/**
 * Phase 25 live interop: RTimeSeries read/delete family — addAll, entry
 * reads (getEntry/getValue, first/last/firstTimestamp), range/rangeReversed
 * with limits, Java round-trips (ts-range / ts-add-label), getAndRemove,
 * pollFirst/pollLast, remove/removeRange and TTL expiry filtering.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName } from '../src/util';

const NS = 'TsJavaT:p25:';
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
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

async function clean(r: ReturnType<typeof Redisson.create>, name: string): Promise<void> {
  await r.getKeys().delete(name, prefixName('redisson__ts_ttl', name), prefixName('redisson__ts_seq', name));
}

describe('live interop phase 25: time-series family', () => {
  it('addAll/read/range/poll + Java label round-trip', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts`;
    try {
      await clean(r, name);
      // warm the resident JVM before timing-free assertions
      await probe(`ts-range ${name} 0 1`);
      const ts = r.getTimeSeries<string>(name);
      await ts.addAll([
        { timestamp: 1000, value: 'v1' },
        { timestamp: 2000, value: 'v2', label: 'l2' },
        { timestamp: 3000, value: 'v3' },
      ]);
      expect(await ts.size()).toBe(3);
      expect(await probe(`ts-range ${name} 0 9999999999999`)).toBe('TS_VALUES:[v1, v2, v3]');

      expect(await ts.first()).toBe('v1');
      expect(await ts.last()).toBe('v3');
      expect(await ts.firstTimestamp()).toBe(1000);
      expect(await ts.lastTimestamp()).toBe(3000);
      expect((await ts.firstEntries(2)).map((e) => e.value)).toEqual(['v1', 'v2']);
      expect((await ts.lastEntries(2)).map((e) => e.value)).toEqual(['v2', 'v3']);
      expect((await ts.range(1000, 2000)).map((e) => e.value)).toEqual(['v1', 'v2']);
      expect((await ts.range(1000, 3000, 2)).map((e) => e.value)).toEqual(['v1', 'v2']);
      expect((await ts.rangeReversed(1000, 3000)).map((e) => e.value)).toEqual(['v3', 'v2', 'v1']);
      expect((await ts.entryRange(1000, 3000, 2)).map((e) => e.value)).toEqual(['v1', 'v2']);
      expect((await ts.entryRangeReversed(1000, 3000, 2)).map((e) => e.value)).toEqual(['v3', 'v2']);

      expect(await ts.getEntry(2000)).toEqual({ timestamp: 2000, value: 'v2', label: 'l2' });
      expect(await ts.getValue(2000)).toBe('v2');

      // Java-written labeled entry is decoded by TS (n=4/4.6.1 both handled)
      expect(await probe(`ts-add-label ${name} 4000 jv jlbl`)).toBe('TS_LBL_ADDED');
      const je = await ts.getEntry(4000);
      expect(je?.value).toBe('jv');
      expect(je?.label).toBe('jlbl');
      expect((await ts.lastEntry())?.value).toBe('jv');

      expect(await ts.getAndRemove(2000)).toBe('v2');
      expect(await ts.getEntry(2000)).toBeNull();
      expect(await ts.pollFirst()).toBe('v1');
      expect(await ts.pollLast()).toBe('jv');
      expect((await ts.pollFirstEntries(2)).map((e) => e.value)).toEqual(['v3']);
      expect(await ts.size()).toBe(0);
      expect(await ts.pollFirst()).toBeNull();
      expect(await ts.pollLastEntry()).toBeNull();
    } finally {
      await clean(r, name);
      await r.shutdown();
    }
  }, 120000);

  it('remove/removeRange + TTL expiry filtering', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const rm = `${NS}rm`;
    const ttl = `${NS}ttl`;
    try {
      await clean(r, rm);
      await clean(r, ttl);
      const a = r.getTimeSeries<string>(rm);
      await a.addAll([
        { timestamp: 1000, value: 'a' },
        { timestamp: 2000, value: 'b' },
        { timestamp: 3000, value: 'c' },
      ]);
      expect(await a.remove(2000)).toBe(true);
      expect(await a.remove(2000)).toBe(false);
      expect(await a.removeRange(1000, 3000)).toBe(2);
      expect(await a.size()).toBe(0);

      const t = r.getTimeSeries<string>(ttl);
      await t.add(1000, 'short', 5000, 'lbl');
      await t.add(2000, 'long');
      expect(await t.size()).toBe(2);
      expect(await t.firstEntry()).toEqual({ timestamp: 1000, value: 'short', label: 'lbl' });
      expect((await t.firstEntries(5)).map((e) => e.value)).toEqual(['short', 'long']);
      await sleep(5500);
      expect(await t.size()).toBe(1);
      expect(await t.first()).toBe('long');
      expect(await t.firstTimestamp()).toBe(2000);
      expect((await t.range(0, 9999)).map((e) => e.value)).toEqual(['long']);
      expect(await t.removeRange(0, 9999)).toBe(1); // only the live entry counts
      expect(await t.size()).toBe(0);
    } finally {
      await clean(r, rm);
      await clean(r, ttl);
      await r.shutdown();
    }
  }, 120000);
});
