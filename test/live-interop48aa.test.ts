/**
 * Live interop vs **Java Redisson 4.8.0**, round 27 — blocked waits woken
 * cross-language and the two pub/sub / multimap shapes still without 4.8
 * evidence:
 *
 *  - RCountDownLatch: a TS `await()` parked on the wake channel resolves
 *    when Java counts down to zero; a Java background await() (daemon
 *    thread) resolves when TS counts down. Java's no-arg await() is VOID
 *    in 4.8 — the probe uses the bounded 120s variant.
 *  - RSemaphore: TS `acquire()` parked at 0 permits is woken by Java
 *    `release()` (same `redisson_sc:{name}` channel); a Java background
 *    acquire() by a TS release.
 *  - RPatternTopic: PSUBSCRIBE deliveries carry the concrete channel +
 *    codec payload both directions (Java's PatternMessageListener is
 *    3-arg: pattern, channel, message).
 *  - RSetMultimapCache TTL: the RMultimapCache interface has NO put-with-
 *    TTL in 4.8 — per-key expiry is `expireKey` only, both sides. Values
 *    visible cross-language, gone after expiry.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48aa:';

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
    console.warn(`[live interop48aa] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

function withSafety<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`${what} never woke within ${ms}ms`)), ms).unref?.() ?? undefined),
  ]) as Promise<T>;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48aa: latch/sem wake / pattern topic / multimap-cache TTL vs Java 4.8.0', () => {
  it('RCountDownLatch: parked await() woken cross-language in both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}latch-ts`;
    const jname = `${NS}latch-java`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name, jname]);

      // TS awaits a count of 2; Java counts down twice → await resolves true
      const latch = r.getCountDownLatch(name);
      expect(await latch.trySetCount(2)).toBe(true);
      const awaitP = latch.await();
      await sleep(300);
      await probe48.probe(`latch48-countdown ${name}`);
      await probe48.probe(`latch48-countdown ${name}`);
      expect(await withSafety(awaitP, 15000, 'ts latch await')).toBe(true);

      // Java background await(); TS counts down twice → ok=true
      expect(await probe48.probe(`latch48-set ${jname} 2`)).toBe('set=true');
      expect(await probe48.probe(`latch48-bgwait ${jname}`)).toBe('armed');
      const jl = r.getCountDownLatch(jname);
      await jl.countDown();
      await jl.countDown();
      expect(await probe48.probe(`latch48-bgresult 8000`)).toBe('ok=true');
    } finally {
      await delAll(raw, [name, jname]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RSemaphore: acquire() parked at 0 permits woken cross-language both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sem-ts`;
    const jname = `${NS}sem-java`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name, jname]);

      // TS acquire() parks (0 permits); Java release() wakes it
      const sem = r.getSemaphore(name);
      expect(await sem.trySetPermits(0)).toBe(true);
      const acqP = sem.acquire();
      await sleep(300);
      expect(await probe48.probe(`sem48-release ${name}`)).toBe('released=ok');
      await withSafety(acqP, 15000, 'ts semaphore acquire');
      expect(await sem.availablePermits()).toBe(0);

      // Java background acquire(); TS release() wakes it
      expect(await probe48.probe(`sem48-tryset ${jname} 0`)).toBe('set=true');
      expect(await probe48.probe(`sem48-bgacquire ${jname}`)).toBe('armed');
      await r.getSemaphore(jname).release(1);
      expect(await probe48.probe(`sem48-bgresult 8000`)).toBe('acquired');
    } finally {
      await delAll(raw, [name, jname]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RPatternTopic: PSUBSCRIBE deliveries carry channel + payload both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const pat = `${NS}pat.*`;
    const ch = `${NS}pat.one`;
    try {
      // Java publishes on the concrete channel → TS pattern listener gets
      // both the channel and the decoded payload
      const ptopic = r.getPatternTopic<string>(pat); // ONE instance
      let resolveGot: (v: string) => void = () => undefined;
      const gotP = new Promise<string>((res) => { resolveGot = res; });
      const id = await ptopic.addListener((channel, msg) => resolveGot(`${channel}|${msg}`));
      expect(await probe48.probe(`topic48-pub ${ch} java-pat-msg`)).toBe('published=ok');
      expect(await withSafety(gotP, 15000, 'pattern message')).toBe(`${ch}|java-pat-msg`);
      await ptopic.removeListener(id);

      // Java PSUBSCRIBEs → TS publishes on the concrete channel
      expect(await probe48.probe(`pattern48-listen ${pat}`)).toBe('listening=ok');
      expect(await ptopic.publish(ch, 'ts-pat-msg')).toBe(1);
      expect(await probe48.probe(`pattern48-poll 8000`)).toBe(`msg=${ch}|ts-pat-msg`);
    } finally {
      await r.getPatternTopic<string>(pat).removeAllListeners();
      await r.shutdown();
    }
  }, 300000);

  it('RSetMultimapCache: expireKey TTL visible cross-language, gone after', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mmc-java`;
    const tname = `${NS}mmc-ts`;
    const k = 'mk';
    try {
      const mmc = r.getSetMultimapCache<string, string>(name);
      // (law 9: child collections are invisible to patterns — delete())
      await mmc.delete();

      // Java puts two values then expires the key in 6s; TS reads both
      expect(await probe48.probe(`mmc48-put ${name} ${k} java-mm1`)).toBe('put=true');
      expect(await probe48.probe(`mmc48-put ${name} ${k} java-mm2`)).toBe('put=true');
      expect(await probe48.probe(`mmc48-expirekey ${name} ${k} 6000`)).toBe('ok=true');
      expect((await mmc.get(k)).sort()).toEqual(['java-mm1', 'java-mm2']);
      await sleep(8000); // gone-after (law 10)
      expect(await mmc.get(k)).toEqual([]);

      // TS puts two + expireKey; Java reads both, gone after
      const tmc = r.getSetMultimapCache<string, string>(tname);
      await tmc.delete();
      await tmc.put(k, 'ts-mm1');
      await tmc.put(k, 'ts-mm2');
      expect(await tmc.expireKey(k, 6000)).toBe(true);
      expect(await probe48.probeLines(`mmc48-getall ${tname} ${k}`)).toEqual(['value=ts-mm1', 'value=ts-mm2']);
      await sleep(8000); // gone-after (law 10)
      expect(await probe48.probeLines(`mmc48-getall ${tname} ${k}`)).toEqual([]);
    } finally {
      await r.getSetMultimapCache<string, string>(name).delete();
      await r.getSetMultimapCache<string, string>(tname).delete();
      await r.shutdown();
    }
  }, 300000);
});
