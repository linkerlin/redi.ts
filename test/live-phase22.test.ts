/**
 * Phase 22 live interop: §3 leftovers.
 *
 * - RFencedLock.tryLockAndGetToken token sequence shared with Java 4.6.1
 *   (`fenced-token` probe uses the same method).
 * - RBlockingQueue/Deque BLPOP/BLMPOP/LMOVE/BLMOVE on a real server
 *   (dedicated connection + real blocking wake-ups).
 * - RRateLimiter keepAlive renewed by both TS and the Java 4.6.1 script.
 * - RMultimapCache periodic eviction sweep (raw HASH check, no lazy read).
 * - RBatch facades: deque/atomic double/bitset/geo/stream/mapcachenative.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, JsonCodec } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName, suffixName } from '../src/util';

const NS = 'TsJavaT:p22:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

async function waitFor(check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(100);
  }
  return check();
}

describe('live interop phase 22: §3 leftovers', () => {
  it('fenced tryLockAndGetToken shares the token sequence with Java 4.6.1', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fenced`;
    const tokenKey = prefixName('redisson_lock_token', name);
    try {
      await r.getKeys().delete(name, tokenKey);
      const fl = r.getFencedLock(name);
      const t1 = await fl.tryLockAndGetToken();
      expect(t1).not.toBeNull();
      await fl.unlock();
      expect(await fl.getToken()).toBe(t1);

      // Java acquires the same lock with tryLockAndGetToken(); the counter is shared.
      const j = await probe(`fenced-token ${name}`);
      expect(j).toMatch(/^TOKEN:\d+$/);
      const jt = Number(j.slice('TOKEN:'.length));
      expect(jt).toBe(t1! + 1);

      const t3 = await fl.tryLockAndGetToken(1000);
      expect(t3).toBe(jt + 1);
      await fl.unlock();
    } finally {
      await r.getKeys().delete(name, tokenKey);
      await r.shutdown();
    }
  }, 120000);

  it('pollFromAny/BLMPOP wake across queues and BLMOVE moves blocked elements', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const qn = `${NS}q`;
    const on = `${NS}q2`;
    const dn = `${NS}d`;
    const d2n = `${NS}d2`;
    try {
      await r.getKeys().delete(qn, on, dn, d2n);
      const q = r.getBlockingQueue<string>(qn);
      const other = r.getBlockingQueue<string>(on);

      // BLPOP priority: own queue first when both are non-empty.
      await q.offer('own');
      await other.offer('other');
      expect(await q.pollFromAny(0, on)).toBe('own');
      expect(await q.pollFromAnyWithName(0, on)).toEqual({ key: on, value: 'other' });

      // blocking wake: offer lands on the *other* queue after the call starts.
      const pending = q.pollFromAny(3000, on);
      await sleep(200);
      await other.offer('late');
      expect(await pending).toBe('late');

      // BLMPOP with COUNT: both values come from the same source queue.
      await other.offer('a');
      await other.offer('b');
      const multi = await q.pollFirstFromAny(1000, 2, on);
      expect(multi.get(on)).toEqual(['a', 'b']);
      await other.offer('c');
      await other.offer('d');
      expect((await q.pollLastFromAny(1000, 2, on)).get(on)).toEqual(['d', 'c']);

      // LMOVE / BLMOVE
      const d = r.getBlockingDeque<string>(dn);
      const d2 = r.getBlockingDeque<string>(d2n);
      await d.add('h');
      await d.add('t');
      expect(await d.move(d2n, 'RIGHT', 'LEFT')).toBe('t');
      expect(await d2.peekFirst()).toBe('t');
      expect(await d.pollFirstWithTimeout(0)).toBe('h'); // drain so the next move truly blocks
      // blocking move wakes when the source gets an element
      const moved = d.moveWithTimeout(d2n, 'LEFT', 'RIGHT', 3000);
      await sleep(200);
      await d.add('wake');
      expect(await moved).toBe('wake');
      // pollLastAndOfferFirstTo = BLMOVE RIGHT→LEFT
      await d.add('p');
      await d.add('q');
      expect(await d.pollLastAndOfferFirstTo(d2n, 1000)).toBe('q');
      expect(await d2.peekFirst()).toBe('q');
    } finally {
      await r.getKeys().delete(qn, on, dn, d2n);
      await r.shutdown();
    }
  }, 180000);

  it('rate limiter keepAlive TTL is renewed by TS and by Java 4.6.1 acquire', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rl`;
    const valueKey = suffixName(name, 'value');
    const permitsKey = suffixName(name, 'permits');
    try {
      await r.getKeys().delete(name, valueKey, permitsKey);
      const rl = r.getRateLimiter(name);
      await rl.setRate('OVERALL', 10, 1000, 5000);
      expect(await rl.tryAcquire()).toBe(true);
      for (const k of [name, valueKey, permitsKey]) {
        const ttl = await r.redis.pttl(k);
        expect(ttl).toBeGreaterThan(0);
        expect(ttl).toBeLessThanOrEqual(5000);
      }

      await sleep(1500);
      const decayed = await r.redis.pttl(valueKey);
      expect(decayed).toBeLessThan(4200);
      // Java's 4.6.1 acquire script reads keepAliveTime from the same HASH and renews.
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
      expect(await r.redis.pttl(valueKey)).toBeGreaterThan(4300);
      expect(await r.redis.pttl(permitsKey)).toBeGreaterThan(4300);

      // A TS acquire renews again (same script shape).
      await sleep(500);
      expect(await rl.tryAcquire()).toBe(true);
      expect(await r.redis.pttl(valueKey)).toBeGreaterThan(4500);
    } finally {
      await r.getKeys().delete(name, valueKey, permitsKey);
      await r.shutdown();
    }
  }, 120000);

  it('multimap cache periodic eviction removes children without a lazy read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mm`;
    try {
      await r.getKeys().delete(name);
      const mm = r.getSetMultimapCache<string, string>(name);
      await mm.put('k1', 'v1');
      await mm.put('k2', 'v2');
      const field = new JsonCodec().encode('k1');
      const childId = await r.redis.hget(name, field);
      expect(childId).not.toBeNull();
      await mm.expireKey('k1', 150);
      mm.enablePeriodicEviction(50);
      try {
        expect(await waitFor(async () => (await r.redis.hlen(name)) === 1, 3000)).toBe(true);
      } finally {
        mm.disablePeriodicEviction();
      }
      expect(await r.redis.hget(name, field)).toBeNull();
      expect(await r.redis.exists(suffixName(name, childId!))).toBe(0);
      expect(await r.redis.zcard(suffixName(name, 'redisson_set_multimap_ttl'))).toBe(0);
      expect(await mm.get('k2')).toEqual(['v2']);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);

  it('batch facades write the same wire as direct structures', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const dn = `${NS}bd`;
    const adn = `${NS}bad`;
    const bn = `${NS}bbs`;
    const gn = `${NS}bg`;
    const sn = `${NS}bs`;
    const mn = `${NS}bmc`;
    try {
      await r.getKeys().delete(dn, adn, bn, gn, sn, mn);
      const batch = r.createBatch();
      const d = batch.getDeque<string>(dn);
      d.offerFirst('a');
      d.offerLast('b');
      const df = d.pollFirst();
      const dl = d.pollLast();
      const dsz = d.size();

      const ad = batch.getAtomicDouble(adn);
      ad.set(0.1);
      const old = ad.getAndAdd(0.2);
      const cur = ad.get();

      const bs = batch.getBitSet(bn);
      const bprev = bs.set(5);
      const bnow = bs.get(5);
      const bcard = bs.cardinality();
      const bsize = bs.size();

      const geo = batch.getGeo<string>(gn);
      const ga = geo.add(13.361389, 38.115556, 'palermo');
      const gb = geo.add(15.087269, 37.502669, 'catania');
      const dist = geo.dist('palermo', 'catania', 'km');
      const pos = geo.pos('palermo');

      const st = batch.getStream(sn);
      const sid = st.add({ a: 1 });
      const ssz = st.size();

      const mc = batch.getMapCacheNative<string, number>(mn);
      const mput = mc.put('k', 7, 5000);
      const mget = mc.get('k');
      const mttl = mc.remainTTL('k');
      await batch.execute();

      // XDEL needs the generated id, so it runs in a second batch.
      const b2 = r.createBatch();
      const sdel = b2.getStream(sn).remove(await sid);
      await b2.execute();

      expect(await df).toBe('a');
      expect(await dl).toBe('b');
      expect(await dsz).toBe(0);
      expect(await old).toBeCloseTo(0.1, 10);
      expect(await cur).toBeCloseTo(0.3, 10);
      expect(await bprev).toBe(false);
      expect(await bnow).toBe(true);
      expect(await bcard).toBe(1);
      expect(await bsize).toBe(8);
      expect(await ga).toBe(true);
      expect(await gb).toBe(true);
      expect(await dist).toBeGreaterThan(160);
      expect(await dist).toBeLessThan(170);
      expect((await pos)!.lon).toBeCloseTo(13.361389, 3);
      expect(await sid).toMatch(/^\d+-\d+$/);
      expect(await ssz).toBe(1);
      expect(await sdel).toBe(1);
      expect(await mput).toBeNull();
      expect(await mget).toBe(7);
      expect(await mttl).toBeGreaterThan(0);
      expect(await mttl).toBeLessThanOrEqual(5000);

      // Wire proof: the geoset is a sorted set with decoded members, and the
      // native TTL lives on the hash field itself.
      expect(await r.redis.zscore(gn, '"palermo"')).not.toBeNull();
      const rawTtl = await (r.redis as any).hpttl(mn, 'FIELDS', 1, '"k"');
      expect(Number(Array.isArray(rawTtl) ? rawTtl[0] : rawTtl)).toBeGreaterThan(0);
    } finally {
      await r.getKeys().delete(dn, adn, bn, gn, sn, mn);
      await r.shutdown();
    }
  }, 180000);
});
