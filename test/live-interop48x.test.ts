/**
 * Live interop vs **Java Redisson 4.8.0**, round 24 — the last three
 * families without 4.8.0-jar evidence:
 *
 *  - RLongAdder: the flush chain is the subtlest wire in the library —
 *    each client owns `{name}:{id}:counter`, every add publishes the
 *    delta on `{name}:adder-topic`, every subscriber merges it locally;
 *    Java's `sum()` Lua reads ALL counter keys, TS's `sum()` GETDELs its
 *    own (already including merged deltas). Cohort pattern: Java adds
 *    100 → TS adds 20 → TS sum (merge) = 120 → signal → Java sum = 120.
 *  - RMultimap: HASH field→id uses a Highway-derived id — the id
 *    derivation must match cross-language or getAll misses the
 *    collection. Both SET and LIST variants, both directions.
 *  - RMap random entries: HRANDFIELD subsets from both languages stay
 *    inside the shared key space.
 *
 * Probe commands adder48-*, mm48-*, lmm48-*, map48-random-entries.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { suffixName } from '../src/util';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48x:';

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
    console.warn(`[live interop48x] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

/** The adder's per-client keys (`{name}:<id>:counter|semaphore`) are random
 *  per instance and leak across runs — sweep them by pattern (no braces in
 *  the glob; the stored keys merely CONTAIN the braced name). */
async function sweepAdder(raw: Redis, name: string): Promise<void> {
  const stale = await raw.keys(`*${name}*`);
  if (stale.length > 0) await raw.del(...stale);
}

describe('live interop48x: long-adder cohort / multimap / map random vs Java 4.8.0', () => {
  it('RLongAdder: Java cohort add → TS merge-sum → Java sum, both = 120', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}adder`;
    const signal = `${name}:ts-done`;
    const raw = new Redis(REDIS_URL);
    try {
      await sweepAdder(raw, name);

      // Java adds 100 and parks until the signal key appears (REPL blocks;
      // fire without await — the cohort command is the only one in flight)
      const cohortP = probe48.probe(`adder48-cohort ${name} 100 ${signal}`);

      // Wait for Java's READY marker (its counter + publish are live)
      let ready = false;
      for (let i = 0; i < 100 && !ready; i++) {
        ready = (await raw.get(`${signal}:ready`)) !== null;
        if (!ready) await sleep(100);
      }
      expect(ready).toBe(true);

      // TS adds 20 (published) and sums: its counter already holds Java's
      // merged 100 → GETDEL returns 120
      const adder = r.getLongAdder(name);
      await adder.add(20);
      expect(await adder.sum()).toBe(120);

      // Signal → Java's cohort sums: its subscriber merged TS's 20 into
      // its own 100, and TS's counter is gone (GETDEL'd) → 120, no double
      raw.set(signal, '1');
      expect(await cohortP).toBe('sum=120');
    } finally {
      raw.disconnect();
      await r.shutdown();
      const sweep = new Redis(REDIS_URL);
      await sweepAdder(sweep, name);
      sweep.disconnect();
    }
  }, 300000);

  it('RMultimap: Highway collection ids match — SET and LIST both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const setMm = `${NS}mm`;
    const listMm = `${NS}lmm`;
    try {
      await r.getKeys().delete(setMm, listMm);

      // Java fills the SET multimap; TS reads the collection by id
      await probe48.probe(`mm48-put ${setMm} k1 a`);
      await probe48.probe(`mm48-put ${setMm} k1 b`);
      await probe48.probe(`mm48-put ${setMm} k2 c`);
      const sm = r.getSetMultimap<string, string>(setMm);
      expect((await sm.get('k1')).sort()).toEqual(['a', 'b']);
      expect(await sm.get('k2')).toEqual(['c']);
      expect((await sm.keySet()).sort()).toEqual(['k1', 'k2']);
      expect(await probe48.probe(`mm48-keyset ${setMm}`)).toBe('keys=[k1, k2]');

      // TS fills k3; Java's getAll must derive the same collection id
      await sm.put('k3', 'x');
      const lines = await probe48.probeLines(`mm48-getall ${setMm} k3`);
      expect(lines).toEqual(['value=x']);

      // LIST variant: order preserved across languages
      const lm = r.getListMultimap<string, string>(listMm);
      await probe48.probe(`lmm48-put ${listMm} k a`);
      await probe48.probe(`lmm48-put ${listMm} k b`);
      await probe48.probe(`lmm48-put ${listMm} k c`);
      expect(await lm.get('k')).toEqual(['a', 'b', 'c']);
      await lm.put('k2', 'ts-first');
      expect(await probe48.probeLines(`lmm48-getall ${listMm} k2`)).toEqual(['value=ts-first']);
    } finally {
      // multimap cleanup MUST be delete()/clear(): child collections hang off
      // HASH-derived ids invisible to any pattern (AGENTS law 9)
      await r.getSetMultimap<string, string>(setMm).delete();
      await r.getListMultimap<string, string>(listMm).delete();
      await r.shutdown();
    }
  }, 300000);

  it('RMap randomEntries: HRANDFIELD subsets from both languages stay in the key space', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}map`;
    try {
      await r.getKeys().delete(name);
      const map = r.getMap<string, string>(name);
      await map.putAll({ a: '1', b: '2', c: '3', d: '4', e: '5' });

      const full = new Set(['a=1', 'b=2', 'c=3', 'd=4', 'e=5']);
      const lines = await probe48.probeLines(`map48-random-entries ${name} 100`);
      expect(lines.map((l) => l.replace('entry=', '')).sort()).toEqual([...full].sort());

      const randomTs = await map.randomEntries(3);
      expect(randomTs.size).toBe(3);
      for (const [k, v] of randomTs) {
        expect(full.has(`${k}=${v}`)).toBe(true);
      }
      const randomJ = await probe48.probeLines(`map48-random-entries ${name} 3`);
      expect(randomJ).toHaveLength(3);
      for (const l of randomJ) {
        expect(full.has(l.replace('entry=', ''))).toBe(true);
      }
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
