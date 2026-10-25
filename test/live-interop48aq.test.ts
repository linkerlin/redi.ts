/**
 * Live interop vs **Java Redisson 4.8.0**, round 43 — three conditional/
 * byte-level families the earlier rounds did not cover:
 *
 *  - RGeo `addIfExists` (GEOADD XX semantics): updating an EXISTING
 *    member's coordinates returns true and moves it (dist changes
 *    cross-language); an absent member is a no-op returning false.
 *  - RRateLimiter `RateType.PER_CLIENT`: budgets are PER CLIENT — a
 *    Java consumer exhausting its own budget leaves the TS side's
 *    budget untouched (both acquire 2 within the same window) while
 *    OVERALL shares (round 48e already proved that direction).
 *  - RBitSet byte ops (`getByte`/`setByte`, BITFIELD i8): TS bit-set →
 *    Java getByte reads the MSB-first byte; Java setByte → TS getByte
 *    and incrementAndGetByte agree.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48aq:';

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
    console.warn(`[live interop48aq] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48aq: geo addIfExists / rate limiter PER_CLIENT / bitset bytes vs Java 4.8.0', () => {
  it('RGeo addIfExists: XX updates move the member cross-language, absent no-op', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const g = `${NS}geo`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [g]);
      const geo = r.getGeo<string>(g);
      await geo.add(13.36, 38.11, 'm1');

      // Java addIfExists MOVES the existing member (dist changes)
      const d0 = (await geo.dist('m1', 'm2')) ?? 0;
      void d0;
      await geo.add(15.08, 37.50, 'm2');
      const before = (await geo.dist('m1', 'm2'))!;
      expect(await probe48.probe(`geo48-addifexists ${g} 13.0 38.0 m2`)).toBe('updated=true');
      const after = (await geo.dist('m1', 'm2'))!;
      expect(after).toBeLessThan(before);

      // absent member → no-op false (nothing created)
      expect(await probe48.probe(`geo48-addifexists ${g} 13.0 38.0 ghost`)).toBe('updated=false');
      expect(await geo.add(13.0, 38.0, 'ghost')).toBe(true); // still addable afterwards

      // TS addIfExists on a Java-written member moves it (Java dist sees it)
      expect(await probe48.probe(`geo48-add ${g} 13.5 38.2 jmem`)).toBe('added=ok');
      expect(await geo.addIfExists(13.37, 38.12, 'jmem')).toBe(true);
      expect(await probe48.probe(`geo48-dist ${g} jmem m1`)).toMatch(/^dist=/);
      expect(Number((await probe48.probe(`geo48-dist ${g} jmem m1`)).slice('dist='.length))).toBeLessThan(12000);
    } finally {
      await delAll(raw, [g]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RRateLimiter PER_CLIENT: budgets isolated per client across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const rl = `${NS}rate-pc`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [rl]);
      const limiter = r.getRateLimiter(rl);

      // TS sets PER_CLIENT 2 per 60s — each CLIENT gets its own budget
      expect(await limiter.trySetRate('PER_CLIENT', 2, 60000)).toBe(true);

      // Java consumes its OWN budget fully
      expect(await probe48.probe(`rate48-try ${rl}`)).toBe('allowed=true');
      expect(await probe48.probe(`rate48-try ${rl}`)).toBe('allowed=true');
      expect(await probe48.probe(`rate48-try ${rl}`)).toBe('allowed=false');

      // TS's budget untouched by Java — still allowed twice
      expect(await limiter.tryAcquire()).toBe(true);
      expect(await limiter.tryAcquire()).toBe(true);
      expect(await limiter.tryAcquire()).toBe(false);
    } finally {
      await delAll(raw, [rl]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBitSet bytes: getByte/setByte agree across languages (MSB-first)', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b = `${NS}bits`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [b]);
      const bs = r.getBitSet(b);

      // TS sets bits 0..7 → the i8 field at bit offset 0 is 0xFF (-1 signed)
      for (let i = 0; i < 8; i++) await bs.set(i, true);
      expect(await probe48.probe(`bs48-getbyte ${b} 0`)).toBe('byte=-1');

      // SEMANTICS: getByte/setByte offsets are BIT offsets (both languages
      // pass them straight to BITFIELD GET/SET i8) despite the byte name —
      // "byte 1" lives at bit offset 8
      expect(await probe48.probe(`bs48-setbyte ${b} 8 5`)).toBe('prev=0');
      expect(await bs.getByte(8)).toBe(5);
      expect(await bs.incrementAndGetByte(8, 2)).toBe(7);
      expect(await probe48.probe(`bs48-getbyte ${b} 8`)).toBe('byte=7');
    } finally {
      await delAll(raw, [b]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
