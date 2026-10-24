/**
 * Live interop vs **Java Redisson 4.8.0**, round 31 — TTL-management
 * primitives and blocking sorted-set polls, the last unproven surfaces
 * outside RFunction:
 *
 *  - RExpirable primitives: TS `expire(ms)` → Java `remainTimeToLive()`
 *    bounded; Java `expire(ms, MS)` → TS bound; TS `expireAt(epochMs)` →
 *    Java bound; Java persist is `clearExpire()` (RExpirable has no
 *    persist() in 4.8 — it lives on RObject) → TS reads -1; TS
 *    `clearExpire()` → Java reads -1.
 *  - RScoredSortedSet blocking polls: TS `pollFirstWithTimeout` parked on
 *    BZPOPMIN woken by Java `add` (entry carries value + score); a Java
 *    background `pollFirst(60, SECONDS)` (daemon thread) woken by a TS add.
 *  - RMapCacheNative conditionals: Java `putIfAbsent(K, V, Duration)` on
 *    a fresh field → created; TS putIfAbsent sees the existing value;
 *    TS putIfAbsent with TTL → Java reads value + HPTTL.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ae:';

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
    console.warn(`[live interop48ae] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ae: expirable primitives / zset blocking / native conditionals vs Java 4.8.0', () => {
  it('RExpirable: expire/expireAt/persist round-trips across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}exp`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k]);
      await r.getBucket<string>(k).set('v');

      // TS expires in 30s → Java's remainTimeToLive is bounded
      expect(await r.getBucket<string>(k).expire(30000)).toBe(true);
      const jttl1 = Number((await probe48.probe(`exp48-rttl ${k}`)).slice('ttl='.length));
      expect(jttl1).toBeGreaterThan(15000);
      expect(jttl1).toBeLessThanOrEqual(30500);

      // Java expires in 10s → TS sees the shortened TTL
      expect(await probe48.probe(`exp48-expire ${k} 10000`)).toBe('ok=true');
      const tsttl = await r.getBucket<string>(k).remainTimeToLive();
      expect(tsttl).toBeGreaterThan(0);
      expect(tsttl).toBeLessThanOrEqual(10500);

      // TS expireAt(now+20s) → Java bounded ~20s
      expect(await r.getBucket<string>(k).expireAt(Date.now() + 20000)).toBe(true);
      const jttl2 = Number((await probe48.probe(`exp48-rttl ${k}`)).slice('ttl='.length));
      expect(jttl2).toBeGreaterThan(10000);
      expect(jttl2).toBeLessThanOrEqual(20500);

      // Java clears the expiry → TS reads -1 (no expiry)
      expect(await probe48.probe(`exp48-persist ${k}`)).toBe('ok=true');
      expect(await r.getBucket<string>(k).remainTimeToLive()).toBe(-1);

      // TS re-arms and clears → Java reads -1
      await r.getBucket<string>(k).expire(30000);
      expect(await r.getBucket<string>(k).clearExpire()).toBe(true);
      expect(await probe48.probe(`exp48-rttl ${k}`)).toBe('ttl=-1');
    } finally {
      await delAll(raw, [k]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RScoredSortedSet: blocking pollFirst woken cross-language both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const z1 = `${NS}zq-ts`;
    const z2 = `${NS}zq-java`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [z1, z2]);

      // TS parks on BZPOPMIN; Java's add wakes it with value + score
      const pollP = r.getScoredSortedSet<string>(z1).pollFirstWithTimeout(15000);
      await sleep(300);
      expect(await probe48.probe(`zset48-add ${z1} 1.5 java-wake`)).toBe('added=true');
      const entry = await pollP;
      expect(entry?.value).toBe('java-wake');
      expect(entry?.score).toBe(1.5);

      // Java background pollFirst(60s) parks; TS's add wakes it
      expect(await probe48.probe(`zset48-bgpoll ${z2}`)).toBe('armed');
      expect(await r.getScoredSortedSet<string>(z2).add(2.5, 'ts-wake')).toBe(true);
      expect(await probe48.probe(`zset48-bgresult 8000`)).toBe('value=ts-wake');
    } finally {
      await delAll(raw, [z1, z2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RMapCacheNative: conditional writes cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name]);
      const mcn = r.getMapCacheNative<string, string>(name);

      // Java putIfAbsent(Duration) creates a fresh field
      expect(await probe48.probe(`mcn48-putifabsent ${name} jf java-c 60000`)).toBe('prev=null');
      expect(await mcn.get('jf')).toBe('java-c');

      // TS putIfAbsent sees the existing value (rejected)
      expect(await mcn.putIfAbsent('jf', 'x', 0)).toBe('java-c');

      // TS putIfAbsent with TTL creates → Java reads value + HPTTL
      expect(await mcn.putIfAbsent('tf', 'ts-c', 60000)).toBeNull();
      expect(await probe48.probe(`mcn48-get ${name} tf`)).toBe('value=ts-c');
      const tsttl = Number((await probe48.probe(`mcn48-rttl ${name} tf`)).slice('ttl='.length));
      expect(tsttl).toBeGreaterThan(30000);
      expect(tsttl).toBeLessThanOrEqual(60000);
    } finally {
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
