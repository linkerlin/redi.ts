/**
 * Phase 27 live interop: RMap materialized views (keySet/values/entrySet with
 * HSCAN semantics, readAllEntrySet) + Java replace/remove conditional
 * overloads, and RTimeSeries stream/iterator paging.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName } from '../src/util';

const NS = 'TsJavaT:p27:';
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

describe('live interop phase 27: map views + time-series stream', () => {
  it('map views match Java HSCAN and conditional replace/remove', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}map`;
    try {
      await r.getKeys().delete(name);
      await probe(`map-get ${name} warm`);
      const m = r.getMap<string, string>(name);
      await m.putAll({ a: '1', b: '2', ab: '3' });

      expect((await m.keySet()).sort()).toEqual(['a', 'ab', 'b']);
      expect((await m.values()).sort()).toEqual(['1', '2', '3']);
      expect((await m.entrySet()).length).toBe(3);
      expect((await m.readAllEntrySet()).length).toBe(3);
      // MATCH sees encoded field names (quoted by JsonCodec), same as Java HSCAN
      expect((await m.keySet('"a*')).sort()).toEqual(['a', 'ab']);
      expect((await m.entrySet('"a*')).sort()).toEqual([['a', '1'], ['ab', '3']]);

      const jk = (await probe(`map-scan-keys ${name} * 10`))
        .slice('MAP_SCAN_KEYS:'.length)
        .replace(/[[\]]/g, '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .sort();
      expect(jk).toEqual(['a', 'ab', 'b']);
      const je = (await probe(`map-scan-entries ${name} * 10`)).slice('MAP_SCAN_ENTRIES:'.length);
      const jm = Object.fromEntries(je.split(';').filter(Boolean).map((kv) => kv.split('=')));
      expect(jm).toEqual({ a: '1', ab: '3', b: '2' });

      expect(await m.replace('a', '9')).toBe('1');
      expect(await m.replace('a', '9', '10')).toBe(true);
      expect(await m.replace('a', '99', '11')).toBe(false); // stale expectation
      expect(await m.replace('a', '10', '11')).toBe(true);
      expect(await probe(`map-get ${name} a`)).toBe('MAP_GET:11');
      expect(await m.remove('a', '10')).toBe(false);
      expect(await m.remove('a', '11')).toBe(true);
      expect(await m.get('a')).toBeNull();
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);

  it('RTimeSeries stream/iterator yield values in order (Java reads the same)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts`;
    try {
      await r.getKeys().delete(name, prefixName('redisson__ts_ttl', name), prefixName('redisson__ts_seq', name));
      await probe(`ts-range ${name} 0 1`);
      const ts = r.getTimeSeries<string>(name);
      await ts.addAll([
        { timestamp: 1000, value: 'v1' },
        { timestamp: 2000, value: 'v2', label: 'l2' },
        { timestamp: 3000, value: 'v3' },
      ]);
      const streamed: string[] = [];
      for await (const v of ts.stream(2)) streamed.push(v);
      expect(streamed).toEqual(['v1', 'v2', 'v3']);
      const iterated: string[] = [];
      for await (const v of ts.iterator(1)) iterated.push(v);
      expect(iterated).toEqual(['v1', 'v2', 'v3']);
      expect(await probe(`ts-range ${name} 0 9999999999999`)).toBe('TS_VALUES:[v1, v2, v3]');
    } finally {
      await r.getKeys().delete(name, prefixName('redisson__ts_ttl', name), prefixName('redisson__ts_seq', name));
      await r.shutdown();
    }
  }, 120000);
});
