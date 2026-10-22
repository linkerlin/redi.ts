/**
 * Phase 8 live interop: LocalCachedMap data layer + TS↔TS invalidation,
 * Deque/List/Set positional ops, AtomicDouble precision, Bucket/Map CAS,
 * PES multi-permit + lease update, Rate updateRate, RW downgrade,
 * RLock watchdog renewal, zset poll + queue drainTo + stream deleteEntries.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p8:';
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


async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 8: lcm/deque-list-set/atomicdouble-bucket-cas/pes-multi/rate-update/downgrade/watchdog/zset-drain', () => {
  it('localcachedmap data layer is RMap wire + TS-TS invalidation', async () => {
    if (!hasRedis) return;
    const a = Redisson.create({ address: REDIS_URL });
    const b = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lcm:shared`;
    try {
      await cleanExact(a, name, `${name}:inval`);
      const la = a.getLocalCachedMap<string, string>(name);
      const lb = b.getLocalCachedMap<string, string>(name);
      await la.put('k', 'v1');
      expect(await lb.get('k')).toBe('v1'); // miss → fetch
      expect(await lb.get('k')).toBe('v1'); // near-cache hit
      await la.put('k', 'v2'); // broadcast invalidate/update
      await new Promise((x) => setTimeout(x, 1500));
      expect(await lb.get('k')).toBe('v2'); // must NOT be stale v1
      // data layer is plain RMap wire: Java reads it directly
      expect(await probe(`map-get ${name} k`)).toBe('MAP_GET:v2');
      // Java writes RMap; TS LCM (after local clear) fetches it
      await a.getMap<string, string>(name).put('jk', 'jv');
      await lb.clearLocalCache();
      expect(await lb.get('jk')).toBe('jv');
      await la.destroy();
      await lb.destroy();
    } finally {
      await a.shutdown();
      await b.shutdown();
    }
  }, 120000);

  it('deque/list/set positional ops visible in Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const dq = `${NS}dq:pos`;
      await cleanExact(r, dq);
      const d = r.getDeque<string>(dq);
      await d.addLast('last');
      await d.addFirst('first');
      await d.addLast('end');
      expect(await probe(`deque-read ${dq}`)).toBe('DEQUE:[first, last, end]');
      expect(await d.pollFirst()).toBe('first');
      expect(await probe(`deque-read ${dq}`)).toBe('DEQUE:[last, end]');
      expect(await d.pollLast()).toBe('end');
      expect(await probe(`deque-read ${dq}`)).toBe('DEQUE:[last]');

      const li = `${NS}li:pos`;
      await cleanExact(r, li);
      const l = r.getList<string>(li);
      await l.add('a', 'b', 'c');
      await l.addAt(1, 'x');
      await l.set(0, 'A');
      expect(await l.indexOf('x')).toBe(1);
      await l.trim(0, 1);
      expect(await probe(`list-read ${li}`)).toBe('LIST:[A, x]');

      const st = `${NS}set:ops`;
      const dst = `${NS}set:dst`;
      await cleanExact(r, st, dst);
      const s = r.getSet<string>(st);
      await s.add('m1', 'm2');
      expect(await s.move(dst, 'm1')).toBe(true);
      expect(await probe(`set-read ${st}`)).toBe('SET:[m2]');
      expect(await probe(`set-read ${dst}`)).toBe('SET:[m1]');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('atomicdouble precision + bucket/map CAS cross-read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const ad = `${NS}ad:prec`;
      await cleanExact(r, ad);
      await r.getAtomicDouble(ad).set(0);
      await probe(`atomicdouble-add ${ad} 0.1`);
      await r.getAtomicDouble(ad).addAndGet(0.2);
      const back = await probe(`atomicdouble-get ${ad}`);
      expect(Math.abs(Number(back.slice('AD_GET:'.length)) - 0.3)).toBeLessThan(1e-9);

      const bk = `${NS}bk:cas`;
      await cleanExact(r, bk);
      const b = r.getBucket<number>(bk);
      await b.set(1);
      expect(await b.compareAndSet(1, 2)).toBe(true);
      expect(await probe(`bucket-get ${bk}`)).toBe('BUCKET_GET:2');
      expect(await b.compareAndDelete(2)).toBe(true);
      expect(await probe(`bucket-get ${bk}`)).toBe('BUCKET_NULL');

      const mp = `${NS}map:cas`;
      await cleanExact(r, mp);
      const m = r.getMap<string, any>(mp);
      expect(await m.putIfAbsent('k', 'v')).toBeNull();
      expect(await m.addAndGet('n', 41)).toBe(41);
      expect(await m.replaceWithCheck('k', 'v', 'v2')).toBe(true);
      expect(await probe(`map-get ${mp} k`)).toBe('MAP_GET:v2');
      expect(await probe(`map-get ${mp} n`)).toBe('MAP_GET:41');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('pes multi-permit + lease update shared', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes:multi`;
    try {
      await cleanExact(r, name, `{${name}}:timeout`);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(3)).toBe(true);
      const ids = await pes.tryAcquire(30000, 2);
      expect(ids).not.toBeNull();
      expect(await probe(`pes-available ${name}`)).toBe('PES_AVAILABLE:1');
      expect(await pes.releaseAll(ids!)).toBe(2);
      expect(await probe(`pes-available ${name}`)).toBe('PES_AVAILABLE:3');

      const single = await pes.tryAcquire(5000);
      expect(single).not.toBeNull();
      expect(await pes.updateLeaseTime(single![0], 60000)).toBe(true);
      expect(await pes.getLeaseTime(single![0])).toBeGreaterThan(30000);
      expect(await pes.release(single![0])).toBe(true);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('rate updateRate + availablePermits cross-read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ratelimit:upd`;
    try {
      await cleanExact(r, name, `{${name}}:value`, `{${name}}:permits`);
      const rl = r.getRateLimiter(name);
      await rl.setRate('OVERALL', 2, 60000);
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
      expect(await rl.availablePermits()).toBe(1);
      expect(await rl.updateRate('OVERALL', 5, 60000)).toBe(true);
      expect(await rl.availablePermits()).toBe(5);
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('rw downgrade write-to-read same holder', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw:downgrade`;
    try {
      await cleanExact(r, name);
      const rw = r.getReadWriteLock(name);
      expect(await rw.getWriteLock().tryLock(0, 30000, 5)).toBe(true);
      // same holder takes the read lock without releasing write (downgrade)
      expect(await rw.getReadLock().tryLock(0, 30000, 5)).toBe(true);
      expect(await probe(`rw-write-try ${name}`)).toBe('CONTENDED');
      await rw.getReadLock().unlock(5);
      // write still held → still contended
      expect(await probe(`rw-write-try ${name}`)).toBe('CONTENDED');
      await rw.getWriteLock().unlock(5);
      expect(await probe(`rw-write-try ${name}`)).toBe('ACQUIRED');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('lock watchdog renews past the lease', async () => {
    if (!hasRedis) return;
    // Short watchdog keeps the proof fast: lease 4s, renew every 4/3s — a
    // renewal must happen or the lock expires before the 6s check.
    const r = Redisson.create({ address: REDIS_URL, lockWatchdogTimeout: 4000 });
    const name = `${NS}lock:watchdog`;
    try {
      await cleanExact(r, name);
      const l = r.getLock(name);
      await l.lock(undefined, 9); // watchdog mode
      await new Promise((x) => setTimeout(x, 6000)); // past one full lease
      expect(await l.isLocked()).toBe(true);
      expect(await probe(`lock-try ${name}`)).toBe('CONTENDED');
      await l.unlock(9);
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('zset poll + queue drainTo + stream deleteEntries', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const zs = `${NS}zs:poll`;
      await cleanExact(r, zs);
      const z = r.getScoredSortedSet<string>(zs);
      await z.add(1, 'low');
      await z.add(9, 'high');
      expect(await z.pollFirst()).toEqual(['low']);
      expect(await probe(`zset-score ${zs} low`)).toBe('ZSCORE:null');
      expect(await probe(`zset-score ${zs} high`)).toBe('ZSCORE:9.0');

      const q1 = `${NS}q:drain-src`;
      const q2 = `${NS}q:drain-dst`;
      await cleanExact(r, q1, q2);
      const src = r.getBlockingQueue<string>(q1);
      const dst = r.getQueue<string>(q2);
      await src.offer('d1');
      await src.offer('d2');
      expect(await src.drainTo(dst)).toBe(2);
      expect(await probe(`bq-take ${q2} 5000 ${q2}:take-result`)).toBe('GOT:d1');

      const s = `${NS}stream:del`;
      await cleanPattern(r, `${s}*`);
      const st = r.getStream(s);
      const id1 = await st.add({ n: 'x' });
      await st.add({ n: 'y' });
      expect(await st.deleteEntries(id1)).toBe(1);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:1');
    } finally {
      await r.shutdown();
    }
  }, 150000);
});

async function cleanPattern(r: Redisson, pattern: string): Promise<void> {
  const keys = await r.getKeys().getKeys(pattern, 500);
  if (keys.length > 0) await r.getKeys().delete(...keys);
}
