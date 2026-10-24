/**
 * Live interop vs **Java Redisson 4.8.0**, round 16 — numeric precision
 * boundaries, the last untested codec risk area:
 *
 *  - Unsafe longs (|v| > 2^53): the wire carries the exact decimal in the
 *    `["java.lang.Long",v]` wrapper — Java reads TS-written values
 *    EXACTLY, and TS reads Java-written values exactly through the
 *    opt-in `JsonCodec({ bigIntForUnsafeLong: true })` (the default codec
 *    keeps the documented lossy number contract; encoding is always
 *    lossless).
 *  - int32 boundary: inside stays a bare literal, outside gets the Long
 *    wrapper — both directions exact.
 *  - RAtomicLong stores raw decimal strings: unsafe values are exact in
 *    BOTH directions with the default API (BigInt on the TS side).
 *  - IEEE-754 doubles survive the round trip bit-for-bit (Java's minimal
 *    Double.toString matches TS's serialization).
 *
 * Probe commands `bkt48-setlong` / `atomic48-setlong`.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, JsonCodec } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48p:';

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
    console.warn(`[live interop48p] Java 4.8 probe unavailable — precision checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48p: numeric precision boundaries vs Java 4.8.0', () => {
  it('unsafe longs (2^53+1): exact both ways through the Long wrapper', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}b`;
    try {
      await r.getKeys().delete(name);

      // TS writes 2^53+1 as BigInt → ["java.lang.Long",9007199254740993];
      // Java parses it to the exact long (ASCII echo is lossless)
      await r.getBucket<bigint>(name).set(9007199254740993n);
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json=9007199254740993');

      // Java writes the same magnitude (default typing wraps it); TS reads
      // it exactly through the opt-in BigInt codec
      await probe48.probe(`bkt48-setlong ${name} 9007199254740993`);
      const exact = r.getBucket<any>(name, new JsonCodec({ bigIntForUnsafeLong: true }));
      expect(await exact.get()).toBe(9007199254740993n);

      // negative unsafe long, same machinery
      await r.getBucket<bigint>(name).set(-9007199254740995n);
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json=-9007199254740995');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('int32 boundary: bare literal inside, Long wrapper outside, exact both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    await raw.connect();
    const name = `${NS}b32`;
    try {
      await r.getKeys().delete(name);

      // inside int32: bare literal on the wire, exact cross-language
      await r.getBucket<number>(name).set(2147483647);
      expect(await raw.get(name)).toBe('2147483647');
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json=2147483647');

      // just outside: the Long wrapper appears, still exact
      await r.getBucket<number>(name).set(2147483648);
      expect(await raw.get(name)).toBe('["java.lang.Long",2147483648]');
      expect(await probe48.probe(`bkt48-get ${name}`)).toBe('json=2147483648');

      // Java's own int32-boundary write read exactly by TS
      await probe48.probe(`bkt48-setlong ${name} -2147483649`);
      expect(await r.getBucket<any>(name, new JsonCodec({ bigIntForUnsafeLong: true })).get()).toBe(-2147483649);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
      raw.disconnect();
    }
  }, 300000);

  it('RAtomicLong/RAtomicDouble: unsafe values and doubles exact via raw storage', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const al = `${NS}al`;
    const ad = `${NS}ad`;
    try {
      await r.getKeys().delete(al, ad);

      // atomic long: raw decimal string storage — exact in both directions
      // with the default API (TS parses unsafe as BigInt)
      await r.getAtomicLong(al).set(9007199254740993n);
      expect(await probe48.probe(`atomic48-get ${al}`)).toBe('value=9007199254740993');
      await probe48.probe(`atomic48-setlong ${al} -9007199254740995`);
      expect(await r.getAtomicLong(al).get()).toBe(-9007199254740995n);

      // double: IEEE-754 bit-for-bit round trip (Java's minimal
      // Double.toString agrees with TS's serialization)
      await r.getAtomicDouble(ad).set(3.141592653589793);
      expect(await probe48.probe(`atomicd48-get ${ad}`)).toBe('value=3.141592653589793');
    } finally {
      await r.getKeys().delete(al, ad);
      await r.shutdown();
    }
  }, 300000);
});
