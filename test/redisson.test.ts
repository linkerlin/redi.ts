import { describe, it, expect } from 'vitest';
import RedisMock from 'ioredis-mock';
import { Redisson, StringCodec, JsonCodec } from '../src';
import { LOCK_ACQUIRE_SCRIPT, LOCK_UNLOCK_SCRIPT, constantBackOff, exponentialBackOff } from '../src/locks';
import { RRateLimiter } from '../src/probabilistic';
import { prefixName, suffixName } from '../src/util';

const make = () => Redisson.create({ client: new RedisMock() as any });

describe('Redisson core (mock)', () => {
  it('bucket', async () => {
    const r = make();
    const b = r.getBucket<{ a: number }>('b');
    await b.set({ a: 1 });
    expect(await b.get()).toEqual({ a: 1 });
    expect(await b.trySet({ a: 2 })).toBe(false);
    expect(await b.compareAndSet({ a: 1 }, { a: 9 })).toBe(true);
    expect(await b.get()).toEqual({ a: 9 });
    await r.shutdown();
  });
  it('string codec is raw', async () => {
    const r = make();
    await r.getBucket<string>('s', new StringCodec()).set('hi');
    expect(await r.redis.get('s')).toBe('hi');
  });
  it('json codec writes @class wrapper readable by all', async () => {
    const r = make();
    await r.getBucket<any>('w').set({ n: 1 });
    const raw = await r.redis.get('w');
    expect(raw).toContain('LinkedHashMap');
    expect(await r.getBucket<any>('w').get()).toEqual({ n: 1 });
  });
  it('atomic long + double', async () => {
    const r = make();
    const a = r.getAtomicLong('n');
    expect(await a.incrementAndGet()).toBe(1);
    expect(await a.addAndGet(5)).toBe(6);
    expect(await a.compareAndSet(6, 10)).toBe(true);
    expect(await a.get()).toBe(10);
    const d = r.getAtomicDouble('dd');
    await d.set(1.5);
    expect(await d.addAndGet(0.5)).toBeCloseTo(2.0);
  });
  it('map with JSON keys', async () => {
    const r = make();
    const m = r.getMap<string, number>('m');
    await m.put('x', 1);
    expect(await m.get('x')).toBe(1);
    expect(await m.size()).toBe(1);
    // raw field is JSON-encoded "x"
    expect(await r.redis.hget('m', '"x"')).toBe('1');
    expect(await m.remove('x')).toBe(1);
  });
  it('set/list/queue/zset/lex/hll/geo/bitset', async () => {
    const r = make();
    const s = r.getSet<number>('s1');
    await s.add(1, 2);
    expect(await s.contains(1)).toBe(true);
    expect(await s.size()).toBe(2);
    const l = r.getList<number>('l1');
    await l.add(1, 2, 3);
    expect(await l.get(0)).toBe(1);
    expect(await l.size()).toBe(3);
    const q = r.getQueue<number>('q1');
    await q.offer(7);
    expect(await q.poll()).toBe(7);
    const z = r.getScoredSortedSet<number>('z1');
    await z.add(1, 10);
    await z.add(2, 20);
    expect(await z.range(0, -1)).toEqual([10, 20]);
    expect(await z.rank(20)).toBe(1);
    const lx = r.getLexSortedSet('lx1');
    await lx.add('a');
    expect(await lx.contains('a')).toBe(true);
    // HLL/Geo/BitSet need real Redis (ioredis-mock lacks PFADD/GEOADD); skip on mock
    try {
      const h = r.getHyperLogLog('h1');
      await h.add('a', 'b');
      expect(await h.count()).toBeGreaterThan(0);
    } catch (e: any) {
      expect(String(e?.message ?? e)).toMatch(/Unsupported command|unknown command/i);
    }
    try {
      const bs = r.getBitSet('bs1');
      await bs.set(7, true);
      expect(await bs.get(7)).toBe(true);
      expect(await bs.cardinality()).toBe(1);
    } catch (e: any) {
      expect(String(e?.message ?? e)).toMatch(/Unsupported command|unknown command/i);
    }
  });
  it('lock uses redisson layout and is reentrant', async () => {
    const r = make();
    const l = r.getLock('lk');
    await l.lock(5000);
    expect(await r.redis.hget('lk', `${r.id}:1`)).toBe('1');
    expect(await l.tryLock(0, 5000)).toBe(true);
    expect(await r.redis.hget('lk', `${r.id}:1`)).toBe('2');
    await l.unlock();
    await l.unlock();
    expect(await l.isLocked()).toBe(false);
    await expect(l.unlock()).rejects.toThrow();
    await l.lock(5000, 1);
    expect(await l.tryLock(0, 5000, 2)).toBe(false);
    await l.unlock(1);
  });
  it('lock lua + channel naming match Redisson', () => {
    expect(LOCK_ACQUIRE_SCRIPT).toContain("hexists");
    expect(LOCK_UNLOCK_SCRIPT).toContain("publish");
    expect(prefixName('redisson_lock__channel', 'lk')).toBe('redisson_lock__channel:{lk}');
  });
  it('backoff policies mirror LockOptions defaults', () => {
    expect(constantBackOff().create().next()).toBe(64);
    expect(constantBackOff(10).create().next()).toBe(10);
    expect(() => constantBackOff(0)).toThrow();
    const p = exponentialBackOff().create();
    expect(p.next()).toBe(1); // initial first, jitter applies from 2nd sleep
    for (let i = 0; i < 30; i++) p.next();
    expect(p.next()).toBe(128); // sticky cap
    expect(() => exponentialBackOff(0)).toThrow();
  });
  it('rate limiter PER_CLIENT keys never degenerate', async () => {
    const r = make();
    // direct construction without a client id gets a per-process tag
    // (never the degenerate trailing-colon suffix)
    const rl = new RRateLimiter(r.redis as any, 'pcl1');
    await rl.setRate('PER_CLIENT', 2, 60000);
    expect(await rl.tryAcquire(1)).toBe(true);
    // NOTE: no braces in the MATCH pattern (glob alternation trap, AGENTS.md:9)
    const keys = await r.redis.keys('*pcl1*');
    expect(keys.some((k) => k.endsWith(':value:'))).toBe(false);
    expect(keys.some((k) => /\{pcl1\}:value:[0-9a-f]+$/.test(k))).toBe(true);
    await r.shutdown();
  });
  it('semaphore / latch / idgen / keys / script', async () => {
    const r = make();
    const sem = r.getSemaphore('sem1');
    expect(await sem.trySetPermits(2)).toBe(true);
    expect(await sem.tryAcquire(1)).toBe(true);
    expect(await sem.availablePermits()).toBe(1);
    await sem.release(1);
    const latch = r.getCountDownLatch('cdl1');
    expect(await latch.trySetCount(1)).toBe(true);
    expect(await latch.getCount()).toBe(1);
    await latch.countDown();
    expect(await latch.getCount()).toBe(0);
    const g = r.getIdGenerator('id1');
    const a = await g.nextId();
    const b = await g.nextId();
    expect(b).toBe(a + 1);
    const keys = r.getKeys();
    expect(await keys.countExists('sem1')).toBe(1);
    const sc = r.getScript();
    expect(await sc.eval('READ_ONLY', 'return 1', 'INTEGER')).toBe(1);
  });
  it('mapcache / setcache / multimap / localcachedmap', async () => {
    const r = make();
    const mc = r.getMapCache<string, number>('mc1');
    await mc.put('k', 1, 60000);
    expect(await mc.get('k')).toBe(1);
    expect(await mc.size()).toBe(1);
    const sc = r.getSetCache<number>('sc1');
    expect(await sc.add(5, 60000)).toBe(true);
    expect(await sc.contains(5)).toBe(true);
    const mm = r.getSetMultimap<string, number>('mm1');
    await mm.put('k', 1);
    await mm.put('k', 2);
    expect(await mm.get('k')).toEqual(expect.arrayContaining([1, 2]));
    const lcm = r.getLocalCachedMap<string, number>('lcm1');
    await lcm.put('k', 42);
    expect(await lcm.get('k')).toBe(42);
    // data layer is plain RMap wire
    expect(await r.getMap<string, number>('lcm1').get('k')).toBe(42);
  });
  it('stream / topic / ratelimiter / bloom / timeseries / batch', async () => {
    const r = make();
    const st = r.getStream('st1');
    await st.add({ a: 1 });
    expect(await st.size()).toBe(1);
    const t = r.getTopic<number>('t1');
    expect(await t.publish(1)).toBeGreaterThanOrEqual(0);
    const rl = r.getRateLimiter('rl1');
    await rl.setRate('OVERALL', 10, 1000);
    expect(await rl.tryAcquire(1)).toBe(true);
    const bf = r.getBloomFilter('bf1');
    await bf.tryInit(1000, 0.03);
    expect(await bf.add('hello')).toBe(true);
    expect(await bf.contains('hello')).toBe(true);
    const ts = r.getTimeSeries<number>('ts1');
    await ts.add(Date.now(), 5);
    expect(await ts.size()).toBe(1);
    const batch = r.createBatch();
    expect(batch).toBeDefined();
    // factories exist for full surface
    expect(r.getFairLock('f')).toBeDefined();
    expect(r.getReadWriteLock('rw')).toBeDefined();
    expect(r.getFencedLock('fl')).toBeDefined();
    expect(r.getSpinLock('sp')).toBeDefined();
    expect(r.getNonReentrantLock('nr')).toBeDefined();
    expect(r.getPermitExpirableSemaphore('pes')).toBeDefined();
    expect(r.getReliableTopic('rt')).toBeDefined();
    expect(r.getShardedTopic('sht')).toBeDefined();
    expect(r.getDelayedQueue('dq')).toBeDefined();
    expect(r.getRingBuffer('rb')).toBeDefined();
    expect(r.getGeo('g')).toBeDefined();
    expect(r.getBitSet('b2')).toBeDefined();
    expect(r.getSearch()).toBeDefined();
    expect(r.getVectorSet('vs')).toBeDefined();
    expect(r.getPriorityQueue('pq')).toBeDefined();
    expect(() => r.getSortedSet('x')).toThrow();
    expect(() => r.createTransaction()).toThrow();
  });
  it('batch typed facades queue writes, reads resolve on execute', async () => {
    const r = make();
    const batch = r.createBatch();
    const b = batch.getBucket<{ n: number }>('bb');
    b.set({ n: 7 });
    const bg = b.get();
    const m = batch.getMap<string, number>('bm');
    const fp = m.fastPut('k', 1);
    const mg = m.get('k');
    const fr = m.fastRemove('k');
    const l = batch.getList<number>('bl');
    const la = l.add(1, 2);
    const lg = l.get(0);
    const lz = l.size();
    const s = batch.getSet<number>('bs');
    const sa = s.add(5);
    const sc = s.contains(5);
    const q = batch.getQueue<number>('bq');
    const qo = q.offer(9);
    const qp = q.poll();
    const a = batch.getAtomicLong('ba');
    a.set(10);
    const ai = a.incrementAndGet();
    const ag = a.get();
    const z = batch.getScoredSortedSet<number>('bz');
    const za = z.add(2, 20);
    const zg = z.getScore(20);
    const zr = z.rank(20);
    await batch.execute();
    // reads resolve with decoded values (RFuture semantics: never await pre-execute)
    expect(await bg).toEqual({ n: 7 });
    expect(await fp).toBe(true);
    expect(await mg).toBe(1);
    expect(await fr).toBe(1);
    expect(await la).toBe(2);
    expect(await lg).toBe(1);
    expect(await lz).toBe(2);
    expect(await sa).toBe(1);
    expect(await sc).toBe(true);
    expect(await qo).toBe(true);
    expect(await qp).toBe(9);
    expect(await ai).toBe(11);
    expect(await ag).toBe(11);
    expect(await za).toBe(true);
    expect(await zg).toBe(2);
    expect(await zr).toBe(0);
    // direct reads see the same state (wire-identical to non-batch writes)
    expect(await r.getBucket<{ n: number }>('bb').get()).toEqual({ n: 7 });
    expect(await r.getAtomicLong('ba').get()).toBe(11);
    await r.shutdown();
  });
  it('batch discard rejects pending reads', async () => {
    const r = make();
    const batch = r.createBatch();
    const g = batch.getBucket<number>('bd').get();
    await batch.discard();
    await expect(g).rejects.toThrow(/discarded/);
    // pipe is usable again after discard
    batch.getBucket<number>('bd2').set(1);
    await batch.execute();
    expect(await r.getBucket<number>('bd2').get()).toBe(1);
    await r.shutdown();
  });
});
