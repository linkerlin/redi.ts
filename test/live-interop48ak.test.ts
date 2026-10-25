/**
 * Live interop vs **Java Redisson 4.8.0**, round 37 — three paths the
 * earlier rounds did not cover:
 *
 *  - RLock BLOCKING `lock()` (not tryLock) woken across languages: a TS
 *    `lock()` parked behind a Java holder completes when Java unlocks; a
 *    Java background blocking `lock()` parked behind a TS holder completes
 *    when TS unlocks.
 *  - RBinaryStream raw bytes: RBinaryStream extends `RBucket<byte[]>`, so
 *    plain get/set carries bytes — TS `set(Buffer)` → Java `get()` reads
 *    identical bytes/length; Java `set(byte[])` → TS `get()`/`getRange()`
 *    read them back.
 *  - RStream `trim(MAXLEN n)`: Java trims a TS-built stream (removed count
 *    + size agree); TS trims a Java-built stream the same way.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ak:';

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
    console.warn(`[live interop48ak] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

async function forceUnlockQuiet(name: string): Promise<void> {
  try {
    await probe48.probe(`lock48-forceunlock ${name}`);
  } catch {
    // nothing held — nothing to release
  }
}

describe('live interop48ak: blocking lock wake / binary stream bytes / stream trim vs Java 4.8.0', () => {
  it('RLock blocking lock() completes when the other language unlocks', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}lk-blk`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);

      // Java holds; TS lock() parks (no tryLock) and completes on Java unlock
      expect(await probe48.probe(`lock48-watchdog ${l}`)).toBe('locked=true');
      const p = r.getLock(l).lock(60000);
      await sleep(300);
      expect(await probe48.probe(`lock48-unlock ${l}`)).toBe('unlocked=ok');
      await p; // resolves once the wake fires and the acquire succeeds
      expect(await r.getLock(l).isLocked()).toBe(true);
      await r.getLock(l).unlock();

      // TS holds; a Java background blocking lock() parks until TS unlocks
      expect(await r.getLock(l).lock(60000)).toBeUndefined();
      expect(await probe48.probe(`lock48-bglock ${l}`)).toBe('armed');
      await sleep(300);
      await r.getLock(l).unlock();
      expect(await probe48.probe(`lock48-bgresult 9000`)).toBe('locked=ok');
    } finally {
      await forceUnlockQuiet(l);
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBinaryStream: raw bytes cross languages unchanged', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}bs`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k]);
      const bs = r.getBinaryStream(k);

      // TS writes raw bytes (incl. non-UTF8-safe latin1) → Java reads identically
      const payload = Buffer.from('ts-bytes-\x80\xff-payload', 'latin1');
      await bs.set(payload);
      // bytes survive the round trip (len covers the non-UTF8-safe ones);
      // the probe's line protocol prints high bytes lossily, so compare
      // length + the printable segments
      const jview = await probe48.probe(`bs48-get ${k}`);
      expect(jview.startsWith(`len=${payload.length} text=ts-bytes-`)).toBe(true);
      expect(jview.endsWith('-payload')).toBe(true);

      // Java overwrites with its own bytes → TS reads them back whole + by range
      expect(await probe48.probe(`bs48-set ${k} java-bytes-ok`)).toBe('set=ok');
      const got = await bs.get();
      expect(got?.toString('latin1')).toBe('java-bytes-ok');
      expect((await bs.getRange(0, 3))?.toString('latin1')).toBe('java');
      expect(await bs.size()).toBe(13);
    } finally {
      await delAll(raw, [k]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream trim(MAXLEN): both languages trim each other’s stream identically', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}st1`; // TS-built, Java trims
    const s2 = `${NS}st2`; // Java-built, TS trims
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s1, s2]);

      // TS adds 5 entries; Java trims to MAXLEN 2 (exact — noLimit terminal)
      for (let i = 1; i <= 5; i++) await r.getStream<string>(s1).add({ n: `v${i}` });
      expect(await probe48.probe(`stream48-size ${s1}`)).toBe('size=5');
      expect(await probe48.probe(`stream48-trim ${s1} 2`)).toBe('removed=3');
      expect(await r.getStream<string>(s1).size()).toBe(2);

      // Java adds 5; TS trims to 2 — removed count + final size agree
      for (let i = 1; i <= 5; i++) await probe48.probe(`stream48-add ${s2} n j${i}`);
      expect(await r.getStream<string>(s2).size()).toBe(5);
      expect(await r.getStream<string>(s2).trim(2, false)).toBe(3);
      expect(await probe48.probe(`stream48-size ${s2}`)).toBe('size=2');
    } finally {
      await delAll(raw, [s1, s2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
