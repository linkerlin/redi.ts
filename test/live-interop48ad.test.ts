/**
 * Live interop vs **Java Redisson 4.8.0**, round 30 — TTL-deep map cache
 * surfaces and the small operators the earlier rounds skipped:
 *
 *  - RMapCache deep ops: TS `computeIfAbsent(key, ttlMs, fn)` (per-key
 *    lock + TTL) read by Java, Java `computeIfAbsent(K, Duration, fn)`
 *    read by TS; per-entry TTL cross-checked both ways via
 *    `remainTimeToLive(K)` (Java's RMapCache variant is PER-KEY, unlike
 *    the no-arg hash-level RExpirable one); `getWithTTLOnly` does not
 *    refresh; `expireEntry` shortens from either side.
 *  - RMapCacheNative (7.4+ HSETEX/HPEXPIRE): zero 4.8 evidence until now
 *    — Java `put(K, V, Duration)` ↔ TS `put(key, value, ttlMs)`, values
 *    and HPTTL read back on both sides.
 *  - Grab-bag: RBitSet AND/XOR merges across languages (Java writes the
 *    result, TS reads bits), TS Bucket `getAndSet`/`getAndDelete` with
 *    Java readback, RList index ops (`set`/single `get`/`trim`) both ways.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ad:';

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
    console.warn(`[live interop48ad] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ad: mapcache deep ops / native / grab-bag vs Java 4.8.0', () => {
  it('RMapCache: computeIfAbsent + per-key TTL shortening from both sides', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc`;
    const raw = new Redis(REDIS_URL);
    try {
      // law 9: MapCache companions are prefixName'd (braced)
      await delAll(raw, [name, `redisson__timeout__set:{${name}}`]);
      const mc = r.getMapCache<string, string>(name);

      // TS computeIfAbsent with a 60s TTL → Java reads the value
      expect(await mc.computeIfAbsent('ck', 60000, () => 'ts-computed')).toBe('ts-computed');
      expect(await probe48.probe(`mc48-get ${name} ck`)).toBe('value=ts-computed');
      const tsTtl = await mc.remainTimeToLive('ck');
      expect(tsTtl).toBeGreaterThan(30000);
      expect(tsTtl).toBeLessThanOrEqual(60000);

      // Java computeIfAbsent (Duration) → TS reads the value
      expect(await probe48.probe(`mc48-compute ${name} jk 60000 java-computed`)).toBe('value=java-computed');
      expect(await mc.get('jk')).toBe('java-computed');

      // Java put(ttl) → TS per-key remainTimeToLive bounded; getWithTTLOnly
      // returns the value without touching the deadline
      await probe48.probe(`mc48-putttl ${name} pk java-put 60000`);
      const putTtl = await mc.remainTimeToLive('pk');
      expect(putTtl).toBeGreaterThan(30000);
      expect(putTtl).toBeLessThanOrEqual(60000);
      expect(await mc.getWithTTLOnly('pk')).toBe('java-put');

      // TS expireEntry shortens to 15s → Java's per-key rttl agrees
      expect(await mc.expireEntry('pk', 15000)).toBe(true);
      const javaView = Number((await probe48.probe(`mc48-rttl ${name} pk`)).slice('ttl='.length));
      expect(javaView).toBeGreaterThan(0);
      expect(javaView).toBeLessThanOrEqual(15500); // clock rounding slack

      // Java expireEntry(Duration) shortens the TS-computed key → TS agrees
      expect(await probe48.probe(`mc48-expireentry ${name} jk 15000`)).toBe('ok=true');
      const jkTtl = await mc.remainTimeToLive('jk');
      expect(jkTtl).toBeGreaterThan(0);
      expect(jkTtl).toBeLessThanOrEqual(15500); // clock rounding slack
    } finally {
      await delAll(raw, [name, `redisson__timeout__set:{${name}}`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RMapCacheNative: HSETEX/HPEXPIRE values and TTLs both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name]);

      // Java put(Duration) → TS get + per-field HPTTL read
      await probe48.probe(`mcn48-put ${name} jf java-native 60000`);
      expect(await r.getMapCacheNative<string, string>(name).get('jf')).toBe('java-native');
      const jttl = await r.getMapCacheNative<string, string>(name).remainTimeToLive('jf');
      expect(jttl).toBeGreaterThan(30000);
      expect(jttl).toBeLessThanOrEqual(60000);

      // TS put(key, value, ttlMs) → Java get + per-field HPTTL read
      await r.getMapCacheNative<string, string>(name).put('tf', 'ts-native', 60000);
      expect(await probe48.probe(`mcn48-get ${name} tf`)).toBe('value=ts-native');
      const tsttl = Number((await probe48.probe(`mcn48-rttl ${name} tf`)).slice('ttl='.length));
      expect(tsttl).toBeGreaterThan(30000);
      expect(tsttl).toBeLessThanOrEqual(60000);
    } finally {
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('grab-bag: BitSet AND/XOR, Bucket getAnd*, RList index ops', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b1 = `${NS}bits-and`;
    const bx = `${NS}bits-xor`;
    const b2 = `${NS}bits-a`;
    const b3 = `${NS}bits-b`;
    const bk = `${NS}bkt`;
    const list = `${NS}list`;
    const raw = new Redis(REDIS_URL);
    const all = [b1, bx, b2, b3, bk, list];
    try {
      await delAll(raw, all);

      // TS writes operand bitsets {1,3} and {3,5}. Java BITOP includes
      // SELF as an input (`BITOP AND b1 b1 b3` with fresh b1 would give 0),
      // so b1 is seeded via OR first; AND then yields {3} (reply = byte
      // length, not popcount)
      const bs2 = r.getBitSet(b2);
      const bs3 = r.getBitSet(b3);
      await bs2.set([1, 3]);
      await bs3.set([3, 5]);
      expect(await probe48.probe(`bits48-or ${b1} ${b2}`)).toBe('len=1');
      expect(await probe48.probe(`bits48-and ${b1} ${b3}`)).toBe('card=1');
      expect(await r.getBitSet(b1).get(3)).toBe(true);
      expect(await r.getBitSet(b1).get(1)).toBe(false);

      // Java XORs into a fresh key → {1,5} (XOR with the zeroed self is
      // identity); the BITOP reply is the result's BYTE LENGTH
      expect(await probe48.probe(`bits48-xor ${bx} ${b2} ${b3}`)).toBe('card=1');
      expect(await r.getBitSet(bx).get(1)).toBe(true);
      expect(await r.getBitSet(bx).get(5)).toBe(true);

      // TS Bucket getAndSet/getAndDelete; Java reads the intermediate state
      const bucket = r.getBucket<string>(bk);
      await bucket.set('v1');
      expect(await bucket.getAndSet('v2')).toBe('v1');
      expect(await probe48.probe(`bkt48-get ${bk}`)).toBe('json="v2"');
      expect(await bucket.getAndDelete()).toBe('v2');
      expect(await bucket.get()).toBeNull();

      // RList index ops: TS RPUSHes, Java overwrites index 1, TS reads;
      // TS overwrites index 2, Java reads; TS trims, Java reads the rest
      const l = r.getList<string>(list);
      await l.add('i0', 'i1', 'i2');
      await probe48.probe(`list48-set ${list} 1 java-set`);
      expect(await l.get(1)).toBe('java-set');
      await l.set(2, 'ts-set');
      expect(await probe48.probe(`list48-get ${list} 2`)).toBe('value=ts-set');
      await l.trim(1, 2);
      expect(await probe48.probeLines(`list48-range ${list}`)).toEqual(['item=java-set', 'item=ts-set']);
    } finally {
      await delAll(raw, all);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
