/**
 * Live interop vs **Java Redisson 4.8.0**, round 47 — audit-ledger batch 1:
 *
 *  - RBucket: setIfAbsent (SETNX / SET PX NX), getAndClearExpire
 *    (GETEX PERSIST), findCommon/findCommonLength (LCS MINMATCHLEN 30 / LEN),
 *    getDigest (Redis ≥8 DIGEST).
 *  - RSemaphore: releaseIfExists (exists-guarded incr + publish NEW counter).
 *    Round-47 wire fix: TS release/publish used to carry the DELTA while Java
 *    publishes the new counter value — cross-language waiters comparing the
 *    published total would starve. Both payloads asserted via a raw subscriber.
 *  - RPermitExpirableSemaphore: tryRelease (removes an EXPIRED permit but
 *    returns false), tryRelease(List) is ALL-OR-NOTHING (any missing/expired
 *    id aborts with 0 removals), getPermits (purge + available + acquired).
 *  - RMapCache: updateEntryExpiration, expireEntryIfNotSet(IfNotSet family),
 *    getAllWithTTLOnly.
 *  - RStream trimNonStrict (XTRIM ~ no-ops on small streams — both languages
 *    must agree on the same removed=0), RList fastSet/fastRemove (sentinel
 *    Lua)/subList views.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48au:';

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
    console.warn(`[live interop48au] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48au: audit-ledger batch (bucket/sem/pes/mapcache/stream/list) vs Java 4.8.0', () => {
  it('RBucket setIfAbsent/getAndClearExpire/findCommon/getDigest agree cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b1 = `${NS}b1`;
    const b2 = `${NS}b2`;
    const bf = `${NS}bf`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [b1, b2, bf]);
      const bucket = r.getBucket<string>(b1);

      // setIfAbsent: Java writes first, TS sees it live → false; fresh key → true
      expect(await probe48.probe(`rb48-sia ${b1} java-value`)).toBe('ok=true');
      expect(await bucket.setIfAbsent('ts-value')).toBe(false);
      expect(await bucket.get()).toBe('java-value');
      expect(await bucket.setIfAbsent('ts-value', 60000)).toBe(false);
      expect(await r.getBucket<string>(bf).setIfAbsent('fresh')).toBe(true);
      expect(await probe48.probe(`rb48-sia ${bf} other`)).toBe('ok=false');

      // getAndClearExpire: TS writes a TTL'd key, Java GETEX PERSIST clears it
      await bucket.set('payload', 60000);
      expect(await raw.pttl(b1)).toBeGreaterThan(50000);
      expect(await probe48.probe(`rb48-getclear ${b1}`)).toBe('value=payload');
      expect(await raw.pttl(b1)).toBe(-1);
      expect(await bucket.get()).toBe('payload');

      // findCommon / findCommonLength — Redis LCS is the longest common
      // SUBSEQUENCE: from `-OneSuffix` / `-TwoSuffix` it skips One/Two and
      // bridges CCCC-Suffix. Both languages must decode the identical string.
      const sub = 'value-with-common-prefix-AAAABBBBCCCC-Suffix';
      await r.getBucket<string>(b1).set(`${sub.replace('-Suffix', '')}-OneSuffix`);
      await r.getBucket<string>(b2).set(`${sub.replace('-Suffix', '')}-TwoSuffix`);
      expect(await probe48.probe(`rb48-lcs ${b1} ${b2}`)).toBe(`common=${sub}`);
      expect(await bucket.findCommon(b2)).toBe(sub);
      // LEN runs over the raw stored bytes (JSON-quoted → +2)
      const lenTs = await bucket.findCommonLength(b2);
      expect(await probe48.probe(`rb48-lcslen ${b1} ${b2}`)).toBe(`len=${lenTs}`);
      expect(lenTs).toBe(sub.length + 2);

      // getDigest: identical hex digests for the same content, both directions
      expect(await probe48.probe(`rb48-digest ${b1}`)).toBe(`digest=${await bucket.getDigest()}`);
      const b3 = `${NS}b3`;
      await raw.del(b3);
      expect(await probe48.probe(`rb48-sia ${b3} digest-me`)).toBe('ok=true');
      const digests = [
        await r.getBucket<string>(b3).getDigest(),
        await probe48.probe(`rb48-digest ${b3}`),
      ];
      expect(digests[0]).toMatch(/^[0-9a-f]{16}$/);
      expect(digests[1]).toBe(`digest=${digests[0]}`);
    } finally {
      await delAll(raw, [b1, b2, bf, `${NS}b3`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RSemaphore releaseIfExists + the published payload is the NEW counter (wire fix)', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}sem`;
    const channel = `redisson_sc:{${s}}`;
    const raw = new Redis(REDIS_URL);
    const messages: string[] = [];
    const sub = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s]);
      await sub.subscribe(channel);
      sub.on('message', (_ch, msg) => messages.push(msg));

      // counter 2 (Java) → TS releaseIfExists(1) → 3
      expect(await probe48.probe(`sem48-tryset ${s} 2`)).toBe('set=true');
      expect(await r.getSemaphore(s).releaseIfExists(1)).toBe(true);
      expect(await probe48.probe(`sem48-available ${s}`)).toBe('available=3');

      // 0 permits → false (Java short-circuit); missing key → false
      expect(await r.getSemaphore(s).releaseIfExists(0)).toBe(false);
      expect(await r.getSemaphore(`${NS}ghost`).releaseIfExists(1)).toBe(false);

      // Java releaseIfExists increments onto the surplus too (no upper bound)
      expect(await probe48.probe(`sem48-relifexists ${s} 3`)).toBe('ok=true');
      expect(await r.getSemaphore(s).availablePermits()).toBe(6);

      // Publish payloads — the round-47 wire fix: BOTH languages publish the
      // NEW counter value (Java always did; TS used to publish the delta).
      // tryset 2 → TS releaseIfExists 3 → Java relifexists 6 → releases 7, 8.
      await probe48.probe(`sem48-release ${s}`);
      await r.getSemaphore(s).release(1);
      await new Promise((res) => setTimeout(res, 300));
      expect(messages).toEqual(['2', '3', '6', '7', '8']);
    } finally {
      sub.disconnect();
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('PES tryRelease (expired → false but purged), tryRelease(List) all-or-nothing, getPermits', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const p = `${NS}pes`;
    const p2 = `${NS}pes2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [p, `${p}:timeout`, p2, `${p2}:timeout`]);
      const pes = r.getPermitExpirableSemaphore(p);

      // Java sets 3, TS acquires one alive permit → TS tryRelease true → 3 again
      expect(await probe48.probe(`pes48-tryset ${p} 3`)).toBe('set=true');
      const [alive] = (await pes.tryAcquire(60000))!;
      expect(await pes.tryRelease(alive)).toBe(true);
      expect(await probe48.probe(`pes48-available ${p}`)).toBe('available=3');

      // TS acquires with a short lease, lets it EXPIRE; Java tryRelease removes
      // the dead permit (counter restored) but reports false
      const [dead] = (await pes.tryAcquire(500))!;
      await new Promise((res) => setTimeout(res, 900));
      expect(await probe48.probe(`pes48-tryrelease ${p} ${dead}`)).toBe('ok=false');
      expect(await probe48.probe(`pes48-available ${p}`)).toBe('available=3');
      // getPermits: TS and Java agree (available + acquired after purge)
      expect(await pes.getPermits()).toBe(3);
      expect(await probe48.probe(`pes48-getpermits ${p}`)).toBe('permits=3');

      // tryRelease(List) all-or-nothing: [alive, expired] aborts with 0 removals
      const [y] = (await pes.tryAcquire(60000))!;
      expect(await pes.tryRelease([y, dead])).toBe(0);
      expect(await pes.availablePermits()).toBe(2); // y still held, dead purged earlier
      expect(await pes.releaseAll([y])).toBe(1);
      expect(await pes.availablePermits()).toBe(3);

      // Java tryReleaseIds on two live Java-acquired ids → released=2
      expect(await probe48.probe(`pes48-tryset ${p2} 2`)).toBe('set=true');
      const j1 = await probe48.probe(`pes48-tryacquire ${p2} 60000`);
      const j2 = await probe48.probe(`pes48-tryacquire ${p2} 60000`);
      const idOf = (line: string) => line.slice('permit='.length);
      expect(await probe48.probe(`pes48-tryreleaseids ${p2} ${idOf(j1!)},${idOf(j2!)}`)).toBe('released=2');
      expect(await probe48.probe(`pes48-available ${p2}`)).toBe('available=2');
    } finally {
      await delAll(raw, [p, `${p}:timeout`, p2, `${p2}:timeout`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RMapCache updateEntryExpiration / expireEntryIfNotSet / getAllWithTTLOnly across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const m = `${NS}mc`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [m, `${m}:ttl`, `${m}:idle`, `${m}:access`]);
      const mc = r.getMapCache<string, string>(m);

      // TS writes a TTL'd entry → Java updateEntryExpiration lengthens it
      await mc.put('k1', 'v1', 60000);
      expect(await probe48.probe(`mc48-updatettl ${m} k1 90000 0`)).toBe('ok=true');
      const remain = await mc.remainTimeToLive('k1');
      expect(remain).toBeGreaterThanOrEqual(85000);
      expect(remain).toBeLessThanOrEqual(91000);

      // no-TTL entry → Java expireEntryIfNotSet true once, false on repeat;
      // TS agrees it now HAS an expiry
      await mc.put('k2', 'v2');
      expect(await probe48.probe(`mc48-entryns ${m} k2 60000 0`)).toBe('ok=true');
      expect(await probe48.probe(`mc48-entryns ${m} k2 60000 0`)).toBe('ok=false');
      expect(await mc.expireEntryIfNotSet('k2', 60000)).toBe(false);

      // TS side: no-TTL entry wins once, then refuses; missing entry → false
      await mc.put('k3', 'v3');
      expect(await mc.expireEntryIfNotSet('k3', 60000)).toBe(true);
      expect(await mc.expireEntryIfNotSet('k3', 60000)).toBe(false);
      expect(await mc.expireEntryIfNotSet('ghost', 60000)).toBe(false);

      // getAllWithTTLOnly: present keys only, values without expiry touches
      expect(await probe48.probeLines(`mc48-getallttl ${m} k1,ghost`)).toEqual(['k1=v1']);
      const all = await mc.getAllWithTTLOnly(['k1', 'k2', 'ghost']);
      expect(all.size).toBe(2);
      expect(all.get('k1')).toBe('v1');
      expect(all.get('k2')).toBe('v2');
      // updateEntryExpiration TS wrapper: ttl ≤ 0 clears the ttl score
      expect(await mc.updateEntryExpiration('k3', 30000)).toBe(true);
      const k3ttl = Number((await probe48.probe(`mc48-rttl ${m} k3`)).split('=')[1]);
      expect(k3ttl).toBeGreaterThanOrEqual(25000);
    } finally {
      await delAll(raw, [m, `${m}:ttl`, `${m}:idle`, `${m}:access`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream trimNonStrict (~ no-op parity) + RList fastSet/fastRemove/subList views', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const st = `${NS}st`;
    const li = `${NS}li`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [st, li]);
      const stream = r.getStream<string, string>(st);

      // XTRIM ~ is a no-op below the node granularity on BOTH languages
      for (let i = 0; i < 3; i++) await stream.add({ f: `v${i}` });
      expect(await probe48.probeLines(`stream48-trimns ${st} 1`)).toEqual(['removed=0']);
      expect(await probe48.probe(`stream48-size ${st}`)).toBe('size=3');
      expect(await stream.trimNonStrict(1)).toBe(0);
      expect(await stream.size()).toBe(3);

      // RList: TS seeds, Java sentinel-removes; no DELETED_BY_REDISSON leaks
      const list = r.getList<string>(li);
      for (const v of ['a', 'b', 'c', 'd', 'e']) await list.add(v);
      await probe48.probe(`list48-fastset ${li} 1 X`);
      expect(await list.get(1)).toBe('X');
      await probe48.probe(`list48-fastremove ${li} 0`);
      expect(await list.readAll()).toEqual(['X', 'c', 'd', 'e']);

      // Java sublist view vs TS sublist view of the same bounds
      expect(await probe48.probe(`list48-sublist ${li} 1 3`)).toBe('sub=c,d');
      expect((await list.subList(1, 3).readAll())).toEqual(['c', 'd']);

      // TS fastSet/fastRemove; Java observes
      await list.fastSet(2, 'Y');
      expect(await probe48.probe(`list48-get ${li} 2`)).toBe('value=Y');
      await list.fastRemove(3);
      expect(await probe48.probeLines(`list48-range ${li} 0 -1`)).toEqual(['item=X', 'item=c', 'item=Y']);
      expect(await list.subList(1, 3).size()).toBe(2);
    } finally {
      await delAll(raw, [st, li]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
