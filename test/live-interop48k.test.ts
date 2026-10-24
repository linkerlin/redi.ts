/**
 * Live interop vs **Java Redisson 4.8.0**, round 11 — RBucket object CAS,
 * multi-series TS.MRANGE with FILTER and RVectorSet sharing:
 *
 *  - `RBucket.compareAndSet` on OBJECT payloads (only the long variant had
 *    Java coverage): expect/update JSON maps cross languages, stale
 *    expects rejected on both sides. Probe `bkt48-*`.
 *  - `RTimeSeriesNatives.range(TSMultiRangeArgs.filter(...))` (phase71
 *    TS-only): TS-created labeled native series found by Java's MRANGE
 *    filter and vice versa. Probe `tsn48-create`/`tsn48-mrange`.
 *  - RVectorSet (deep 4.8 surface, only 4.6.1 basics had coverage):
 *    vectors added in one language are similarity-searched and
 *    attribute-read in the other. Probe `vec48-*`.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, TSCreateArgs, TSMultiRangeArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48k:';

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
    if (!probeOk) console.warn('[live interop48k] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48k] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48k: bucket CAS + native mrange + vector set vs Java 4.8.0', () => {
  it('RBucket.compareAndSet: object payloads CAS in both languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bkt`;
    try {
      await r.getKeys().delete(name);

      // TS writes an object; Java reads it and CASes it away
      const b = r.getBucket<any>(name);
      await b.set({ v: 1 });
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json={"v":1}');
      expect(await probe48.probe(`bkt48-cas ${name} {"v":1} {"v":2}`)).toBe('cas=true');
      expect(await b.get()).toEqual({ v: 2 });

      // TS CASes Java's value; Java's stale expect is rejected
      expect(await b.compareAndSet({ v: 2 }, { v: 3 })).toBe(true);
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json={"v":3}');
      expect(await probe48.probe(`bkt48-cas ${name} {"v":1} {"v":9}`)).toBe('cas=false');
      expect(await b.get()).toEqual({ v: 3 });
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RTimeSeriesNatives: MRANGE FILTER finds labeled series across languages', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'TS.ADD'))) {
      console.log('[live interop48k] TS.* unavailable — skipping native mrange');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}tsa`;
    const b = `${NS}tsb`;
    const jc = `${NS}tsj`;
    try {
      await r.getKeys().delete(a, b, jc);
      const t0 = Date.now() - 10000;

      // TS creates two labeled native series; only one matches the filter
      await r.getTimeSeriesNative(a).create(TSCreateArgs.defaults().labels({ kind: 'ts-side' }));
      await r.getTimeSeriesNative(b).create(TSCreateArgs.defaults().labels({ kind: 'other' }));
      await r.getTimeSeriesNative(a).add(t0 + 100, 1.5);
      await r.getTimeSeriesNative(b).add(t0 + 100, 9.5);

      const jRange = await probe48.probeLines(`tsn48-mrange kind=ts-side`);
      expect(jRange).toEqual([`series=${a} ${t0 + 100}=1.5`]); // b filtered out

      // Java creates a labeled series; TS's MRANGE filter finds it
      await probe48.probe(`tsn48-create ${jc} src=java`);
      await probe48.probe(`tsn48-add ${jc} ${t0 + 200} 4.5`);
      const tsRange = await r.getTimeSeriesNatives().range(TSMultiRangeArgs.filter('src=java'));
      expect(tsRange.size).toBe(1);
      expect([...tsRange.keys()]).toEqual([jc]);
      expect(tsRange.get(jc)!.samples).toEqual([{ timestamp: t0 + 200, values: [4.5] }]);
    } finally {
      await r.getKeys().delete(a, b, jc);
      await r.shutdown();
    }
  }, 300000);

  it('RVectorSet: vectors and attributes shared across languages', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'VADD'))) {
      console.log('[live interop48k] VADD unavailable — skipping vector set');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vec`;
    try {
      await r.getKeys().delete(name);
      const vs = r.getVectorSet(name);

      // TS adds two close vectors; Java similarity-searches from TS's element
      expect(await vs.add('e1', [1, 0, 0])).toBe(true);
      expect(await vs.add('e2', [0.9, 0.1, 0])).toBe(true);
      const jSim = await probe48.probeLines(`vec48-sim ${name} e1 2`);
      expect(jSim).toContain('sim=e1');
      expect(jSim).toContain('sim=e2');

      // Java adds with JSON attributes; TS searches + reads the attributes
      expect(await probe48.probe(`vec48-add ${name} e3 0,1,0 {"color":"red"}`)).toBe('added=true');
      expect(await vs.sim('e3', 10)).toContain('e3');
      expect(await vs.getAttributes('e3')).toEqual({ color: 'red' });

      // TS sets attributes; Java reads them back
      expect(await vs.setAttributes('e3', { color: 'blue' })).toBe(true);
      expect(await probe48.probe(`vec48-attr ${name} e3`)).toContain('blue');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
