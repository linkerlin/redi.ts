/**
 * Live interop vs **Java Redisson 4.8.0**, round 34 — blocking-wake paths
 * and lock TTL semantics the earlier rounds did not cover:
 *
 *  - RBlockingDeque: all four end × direction blocking wakes — TS parks
 *    on the LEFT/RIGHT end (BLPOP/BRPOP) and a Java offer at the
 *    OPPOSITE end wakes it (single element pops from either end); a
 *    Java background pollFirst/pollLast(timeout, MS) is woken by a TS
 *    offerFirst/offerLast.
 *  - RLock lease expiry handover: a TS 500ms lease is rejected by Java
 *    inside the window and releases to Java after expiry; the reverse
 *    (Java tryLock(0, 500, MS) → TS rejected → TS wins after expiry).
 *  - RLock watchdog renewal: TS `lock()` (no lease) renews past one
 *    watchdog cycle (30s/3) — Java sees isLocked=true and the TTL stays
 *    above the unrenewed floor; TS unlock lets Java take it.
 *  - RStream: a TS blocking `readGroupWith({ blockMs })` parks on
 *    XREADGROUP BLOCK in a dedicated connection until a Java XADD
 *    arrives (the non-blocking group-read paths were round 22).
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ah:';

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
    console.warn(`[live interop48ah] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
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

describe('live interop48ah: deque ends / lock lease + watchdog / stream blocking read vs Java 4.8.0', () => {
  it('RBlockingDeque: all four end x direction blocking wakes across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bd`;
    const bd = r.getBlockingDeque<string>(name);
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name]);

      // TS parks on the LEFT end; a Java offer at the RIGHT end wakes it
      // (the deque is empty, so the single element pops from either end)
      const pL = bd.pollFirstWithTimeout(8000);
      await sleep(300);
      expect(await probe48.probe(`bd48-offerlast ${name} java-l`)).toBe('offered=true');
      expect(await pL).toBe('java-l');

      // Java background parks on the RIGHT end; TS offerFirst wakes it
      expect(await probe48.probe(`bd48-bgpoll ${name} last 8000`)).toBe('armed');
      await sleep(300);
      await bd.addFirst('ts-l');
      expect(await probe48.probe(`bd48-bgresult 9000`)).toBe('value=ts-l');

      // TS parks on the RIGHT end; Java offerFirst wakes it
      const pR = bd.pollLastWithTimeout(8000);
      await sleep(300);
      expect(await probe48.probe(`bd48-offerfirst ${name} java-r`)).toBe('offered=true');
      expect(await pR).toBe('java-r');

      // Java background parks on the LEFT end; TS offerLast wakes it
      expect(await probe48.probe(`bd48-bgpoll ${name} first 8000`)).toBe('armed');
      await sleep(300);
      await bd.addLast('ts-r');
      expect(await probe48.probe(`bd48-bgresult 9000`)).toBe('value=ts-r');
    } finally {
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RLock lease expiry: 500ms leases hand over across languages after expiry', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}lk-lease`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);

      // TS takes a 500ms lease (no watchdog): Java is rejected inside the
      // window and takes over after the hash expires
      expect(await r.getLock(l).tryLock(0, 500)).toBe(true);
      expect(await probe48.probe(`lock48-trylock ${l}`)).toBe('trylock=false');
      await sleep(1200);
      expect(await probe48.probe(`lock48-trylock ${l}`)).toBe('trylock=true');

      // force-release (cross-holder legal), Java takes a 500ms lease:
      // TS rejected inside the window, wins after expiry
      expect(await probe48.probe(`lock48-forceunlock ${l}`)).toBe('forced=true');
      expect(await probe48.probe(`lock48-trylocklease ${l} 500`)).toBe('locked=true');
      expect(await r.getLock(l).tryLock(0)).toBe(false);
      await sleep(1200);
      expect(await r.getLock(l).tryLock(0)).toBe(true);
      await r.getLock(l).unlock();
    } finally {
      await forceUnlockQuiet(l);
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RLock watchdog: TS lock() renews past one cycle; release lets Java acquire', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}lk-wd`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);

      // TS lock() with watchdog (30s, renewed every 30s/3)
      expect(await r.getLock(l).lock()).toBeUndefined();
      await sleep(300);
      expect(await probe48.probe(`lock48-islocked ${l}`)).toBe('locked=true');

      // after a full renewal cycle (30s/3) the TTL must be back near 30s —
      // an unrenewed lease decays to ~17.5s here; the 22000 threshold keeps
      // clear margins on both sides under full-suite parallel load
      await sleep(12000);
      const ttl = await raw.pttl(l);
      expect(ttl).toBeGreaterThan(22000);
      expect(await probe48.probe(`lock48-islocked ${l}`)).toBe('locked=true');

      // TS unlock releases the hash; Java acquires immediately
      await r.getLock(l).unlock();
      expect(await probe48.probe(`lock48-trylock ${l}`)).toBe('trylock=true');
    } finally {
      await forceUnlockQuiet(l);
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream: TS blocking readGroupWith parks until a Java XADD arrives', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}st`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s]);

      // Java creates the group (mkstream), TS parks in a dedicated
      // connection on XREADGROUP BLOCK
      expect(await probe48.probe(`stream48-create-group ${s} g34`)).toBe('group=ok');
      const p = r.getStream<string>(s).readGroupWith('g34', 'c34', { blockMs: 8000 });
      await sleep(300);

      // Java XADD wakes the parked reader; the codec-encoded field decodes
      expect(await probe48.probe(`stream48-add ${s} f1 java-wake`)).toMatch(/^id=/);
      const entries = await p;
      expect(entries).toHaveLength(1);
      expect(entries[0].fields.f1).toBe('java-wake');

      // the entry is in Java's PEL for consumer c34 (delivered, unacked)
      const pend = await probe48.probeLines(`stream48-pending ${s} g34`);
      expect(pend.some((x) => x.includes('c34'))).toBe(true);
    } finally {
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
