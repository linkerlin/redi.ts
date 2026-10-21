import { describe, it, expect } from 'vitest';
import RedisMock from 'ioredis-mock';
import { Redisson, StringCodec, JsonCodec, CompareAndDeleteArgs } from '../src';
import { LOCK_ACQUIRE_SCRIPT, LOCK_UNLOCK_SCRIPT, constantBackOff, exponentialBackOff } from '../src/locks';
import { RRateLimiter } from '../src/probabilistic';
import { prefixName, suffixName, hash128toBase64, unpackMapCacheValue } from '../src/util';

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
  it('map HSCAN iterators + HRANDFIELD/HSTRLEN', async () => {
    const r = make();
    const m = r.getMap<string, number>('m2');
    await m.putAll({ a: 1, b: 2, c: 3 });
    expect(await m.valueSize('a')).toBe(1);
    const keys = await m.randomKeys(2);
    expect(keys.length).toBe(2);
    expect(keys.every((k) => ['a', 'b', 'c'].includes(k))).toBe(true);
    const entries = await m.randomEntries(3);
    expect(entries.size).toBe(3);
    expect([...entries.values()].sort()).toEqual([1, 2, 3]);
    const scanned: string[] = [];
    for await (const k of m.keysIterator('*', 1)) scanned.push(k);
    expect(scanned.sort()).toEqual(['a', 'b', 'c']);
    const values: number[] = [];
    for await (const v of m.valuesIterator('*', 1)) values.push(v);
    expect(values.sort()).toEqual([1, 2, 3]);
    const pairs: [string, number][] = [];
    for await (const e of m.entriesIterator('*', 1)) pairs.push(e);
    expect(pairs.length).toBe(3);
    expect(pairs.map(([k]) => k).sort()).toEqual(['a', 'b', 'c']);
  });
  it('scored set score ops + reversed ranges', async () => {
    const r = make();
    const z = r.getScoredSortedSet<string>('z2');
    await z.add(1, 'a');
    await z.add(2, 'b');
    await z.add(3, 'c');
    expect(await z.firstScore()).toBe(1);
    expect(await z.lastScore()).toBe(3);
    expect(await z.addScore('a', 1)).toBe(2);
    expect(await z.rank('zz')).toBe(null);
    expect(await z.revRank('zz')).toBe(null);
    expect(await z.addScoreAndGetRank('c', 2)).toBe(2);
    expect(await z.addScoreAndGetRevRank('c', -2)).toBe(0);
    expect(await z.firstScore()).toBe(2);
    expect(await z.lastScore()).toBe(3);
    expect(await z.addAndGetRevRank(0, 'a')).toBe(2);
    expect(await z.firstScore()).toBe(0);
    const z2 = r.getScoredSortedSet<string>('z3');
    await z2.add(1, 'a');
    await z2.add(2, 'b');
    await z2.add(3, 'c');
    expect(await z2.valueRangeReversed(0, -1)).toEqual(['c', 'b', 'a']);
    expect(await z2.valueRangeReversed(2, true, 3, true)).toEqual(['c', 'b']);
    expect((await z2.entryRangeReversed(0, -1)).map((e) => e.value)).toEqual(['c', 'b', 'a']);
    expect((await z2.entryRangeReversed(2, true, 3, true)).map((e) => e.value)).toEqual(['c', 'b']);
    expect((await z2.entryRange(0, -1)).map((e) => e.value)).toEqual(['a', 'b', 'c']);
    expect((await z2.entryRange(1, true, 2, true)).map((e) => e.value)).toEqual(['a', 'b']);
    expect((await z2.entryRange(2, false, 3, true)).map((e) => e.value)).toEqual(['c']);
    expect((await z2.entryRange(1, true, 3, true)).map((e) => e.score)).toEqual([1, 2, 3]);
    expect(await z2.count(2, true, 3, true)).toBe(2);
    expect(await z2.count(1, false, 3, true)).toBe(2);
    expect(await z2.removeRangeByScore(2, true, 2, true)).toBe(1);
    expect(await z2.removeRangeByRank(0, 0)).toBe(1);
    expect(await z2.range(0, -1)).toEqual(['c']);
    expect(await z2.lastScore()).toBe(3);
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
      // length() = highest set bit + 1; byte 0x41 has its highest bit at index 7 (mock-safe ASCII)
      await bs.set(1, true);
      expect(await bs.length()).toBe(8);
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
  it('set store ops write into self, read variants do not', async () => {
    const r = make();
    const a = r.getSet<string>('sa');
    await a.add('a', 'b');
    const b = r.getSet<string>('sb');
    await b.add('b', 'c');
    // read-only first (inputs untouched)
    expect(new Set(await a.readUnion('sb'))).toEqual(new Set(['a', 'b', 'c']));
    expect(await a.readDiff('sb')).toEqual(['a']);
    expect(await a.readIntersection('sb')).toEqual(['b']);
    expect(await a.size()).toBe(2);
    // store variants follow Redisson 4.6.1: the dest is overwritten with the
    // result over others only — SELF is NOT an input (command shape is
    // SOPSTORE dest others... with bare keys and NO numkeys).
    // Poison regression: a real key literally named "3" (the bogus numkeys
    // value the old shape sent for 2 inputs) must not leak into any result.
    await r.getSet<string>('3').add('poison');
    const d = r.getSet<string>('sd');
    await d.add('zz'); // would survive if SELF were an input
    expect(await d.union('sa', 'sb')).toBe(3);
    expect(new Set(await d.readAll())).toEqual(new Set(['a', 'b', 'c']));
    const di = r.getSet<string>('sdi');
    await di.add('a'); // would shrink the result if SELF were an input
    expect(await di.intersection('sa', 'sb')).toBe(1);
    expect(await di.readAll()).toEqual(['b']);
    const dd = r.getSet<string>('sdd');
    await dd.add('a', 'b', 'c', 'x'); // 'x' would survive if SELF were an input
    expect(await dd.diff('sa', 'sb')).toBe(1);
    expect(await dd.readAll()).toEqual(['a']);
    await r.shutdown();
  });
});

describe('phase 18: 64-bit precision + atomic drainTo', () => {
  it('atomic long set/get/CAS keep Long.MAX_VALUE exact', async () => {
    const r = make();
    const a = r.getAtomicLong('p18:al');
    await a.set(9223372036854775807n);
    expect(await a.get()).toBe(9223372036854775807n);
    expect(await a.compareAndSet(9223372036854775807n, 9223372036854775806n)).toBe(true);
    expect(await a.compareAndSet(9223372036854775807, 1)).toBe(false);
    expect(await a.get()).toBe(9223372036854775806n);
    const b = r.getAtomicLong('p18:al-missing');
    expect(await b.compareAndSet(0, 5)).toBe(true);
    expect(await b.get()).toBe(5);
    expect(await b.getAndDelete()).toBe(5);
    await r.shutdown();
  });

  it('atomic long getAndAdd returns the exact old value', async () => {
    const r = make();
    const a = r.getAtomicLong('p18:al-add');
    await a.set(9007199254740991n);
    expect(await a.getAndAdd(1n)).toBe(9007199254740991);
    await r.redis.set(a.name, '9007199254740993');
    expect(await a.getAndAdd(1n)).toBe(9007199254740993n);
    await r.shutdown();
  });

  it('atomic double getAndAdd returns the stored old value (no reverse calc)', async () => {
    const r = make();
    const d = r.getAtomicDouble('p18:ad');
    await d.set(0.1);
    expect(await d.getAndAdd(0.2)).toBe(0.1);
    expect(await d.get()).toBeCloseTo(0.3);
    await r.shutdown();
  });

  it('blocking queue drainTo extracts atomically with maxElements', async () => {
    const r = make();
    const q = r.getBlockingQueue<number>('p18:q');
    await q.offer(1);
    await q.offer(2);
    await q.offer(3);
    await q.offer(4);
    await q.offer(5);
    const dest = r.getQueue<number>('p18:q-dest');
    expect(await q.drainTo(dest, 3)).toBe(3);
    expect(await q.readAll()).toEqual([4, 5]);
    expect(await dest.readAll()).toEqual([1, 2, 3]);
    expect(await q.drainTo(dest)).toBe(2);
    expect(await dest.readAll()).toEqual([1, 2, 3, 4, 5]);
    await r.shutdown();
  });

  it('drainTo does not lose concurrent tail appends', async () => {
    const r = make();
    const q = r.getBlockingQueue<number>('p18:q-race');
    await q.offer(1);
    await q.offer(2);
    const dest = r.getQueue<number>('p18:q-race-dest');
    const draining = q.drainTo(dest);
    const appending = (async () => {
      for (let i = 3; i <= 50; i++) await q.offer(i);
    })();
    await Promise.all([draining, appending]);
    const combined = [...(await dest.readAll()), ...(await q.readAll())].sort((a, b) => a - b);
    expect(combined).toEqual(Array.from({ length: 50 }, (_, i) => i + 1));
    await r.shutdown();
  });
});

describe('phase 17: bounded semaphore + blocking/topic lifecycle', () => {
  it('bounded queue maintains available permits across offer/poll/remove/clear', async () => {
    const r = make();
    const q = r.getBoundedBlockingQueue<string>('p17:bq');
    expect(await q.trySetCapacity(2)).toBe(true);
    expect(await q.remainingCapacity()).toBe(2);
    expect(await q.getCapacity()).toBe(2);
    expect(await q.offer('a')).toBe(true);
    expect(await q.offer('b')).toBe(true);
    expect(await q.remainingCapacity()).toBe(0);
    expect(await q.offer('c')).toBe(false);
    expect(await q.poll()).toBe('a');
    expect(await q.remainingCapacity()).toBe(1);
    expect(await q.offer('c')).toBe(true);
    expect(await q.remove('b')).toBe(true);
    expect(await q.remainingCapacity()).toBe(1);
    expect(await q.readAll()).toEqual(['c']);
    await q.clear();
    expect(await q.remainingCapacity()).toBe(2);
    expect(await q.poll()).toBeNull();

    const q2 = r.getBoundedBlockingQueue<string>('p17:drain');
    expect(await q2.trySetCapacity(3)).toBe(true);
    await q2.offer('d1');
    await q2.offer('d2');
    const dest = r.getQueue<string>('p17:dest');
    expect(await q2.drainTo(dest)).toBe(2);
    expect(await q2.remainingCapacity()).toBe(3);
    expect(await dest.readAll()).toEqual(['d1', 'd2']);
    await r.shutdown();
  });

  it('bounded put blocks until a consumer releases capacity', async () => {
    const r = make();
    const q = r.getBoundedBlockingQueue<string>('p17:put');
    expect(await q.trySetCapacity(1)).toBe(true);
    expect(await q.offer('a')).toBe(true);
    const blocked = q.put('b');
    await new Promise((x) => setTimeout(x, 60));
    expect(await q.size()).toBe(1);
    expect(await q.poll()).toBe('a');
    await Promise.race([
      blocked,
      new Promise((_, reject) => setTimeout(() => reject(new Error('put did not wake')), 3000)),
    ]);
    expect(await q.readAll()).toEqual(['b']);
    await r.shutdown();
  }, 10000);

  it('blocking poll uses sub-second timeout and 0 is non-blocking', async () => {
    const r = make();
    const q = r.getBlockingQueue<string>('p17:bqpoll');
    const t0 = Date.now();
    expect(await q.pollWithTimeout(0)).toBeNull();
    expect(Date.now() - t0).toBeLessThan(200);
    const t1 = Date.now();
    expect(await q.pollWithTimeout(100)).toBeNull();
    const elapsed = Date.now() - t1;
    expect(elapsed).toBeGreaterThanOrEqual(50);
    expect(elapsed).toBeLessThan(1500);
    await r.shutdown();
  }, 10000);

  it('take wakes when an element arrives', async () => {
    const r = make();
    const q = r.getBoundedBlockingQueue<string>('p17:take');
    expect(await q.trySetCapacity(1)).toBe(true);
    const pending = q.take();
    await new Promise((x) => setTimeout(x, 50));
    expect(await q.offer('x')).toBe(true);
    await expect(Promise.race([
      pending,
      new Promise((_, reject) => setTimeout(() => reject(new Error('take did not wake')), 3000)),
    ])).resolves.toBe('x');
    expect(await q.remainingCapacity()).toBe(1);
    await r.shutdown();
  }, 10000);

  it('topic listeners can be removed and stop receiving', async () => {
    const r = make();
    const t = r.getTopic<number>('p17:t');
    const got: number[] = [];
    const id1 = await t.addListener((v) => got.push(v));
    const id2 = await t.addListener((v) => got.push(v));
    await t.publish(1);
    await new Promise((x) => setTimeout(x, 50));
    expect(got.filter((v) => v === 1)).toHaveLength(2);
    await t.removeListener(id1);
    await t.publish(2);
    await new Promise((x) => setTimeout(x, 50));
    expect(got.filter((v) => v === 2)).toHaveLength(1);
    await t.removeAllListeners();
    await t.publish(3);
    await new Promise((x) => setTimeout(x, 50));
    expect(got).not.toContain(3);
    await r.shutdown();
  }, 10000);

  it('sharded topic removeListener releases its subscription', async () => {
    const r = make();
    const t = r.getShardedTopic<number>('p17:st');
    const got: number[] = [];
    const id = await t.addListener((v) => got.push(v));
    await t.publish(1);
    await new Promise((x) => setTimeout(x, 50));
    expect(got).toContain(1);
    await t.removeListener(id);
    await t.publish(2);
    await new Promise((x) => setTimeout(x, 50));
    expect(got).not.toContain(2);
    await r.shutdown();
  }, 10000);
});

describe('phase 16 lifecycle: companion keys + delayed queue + local cache', () => {
  it('fair lock delete/unlink clear queue and timeout ZSET', async () => {
    const r = make();
    const l = r.getFairLock('p16:fair');
    const queue = prefixName('redisson_lock_queue', l.name);
    const timeout = prefixName('redisson_lock_timeout', l.name);
    const field = `${r.id}:1`;
    await r.redis.hset(l.name, field, '1');
    await r.redis.rpush(queue, field);
    await r.redis.zadd(timeout, '1', field);
    expect(await l.delete()).toBe(true);
    expect(await r.redis.exists(l.name, queue, timeout)).toBe(0);

    // unlink path must clean the same companion set
    await r.redis.hset(l.name, field, '1');
    await r.redis.rpush(queue, field);
    await r.redis.zadd(timeout, '1', field);
    expect(await l.unlink()).toBe(true);
    expect(await r.redis.exists(l.name, queue, timeout)).toBe(0);
    await r.shutdown();
  });

  it('fenced lock delete removes the fencing token', async () => {
    const r = make();
    const fl = r.getFencedLock('p16:fenced');
    const token = prefixName('redisson_lock_token', fl.name);
    await r.redis.hset(fl.name, `${r.id}:1`, '1');
    await r.redis.set(token, '7');
    expect(await fl.delete()).toBe(true);
    expect(await r.redis.exists(fl.name, token)).toBe(0);
    await r.shutdown();
  });

  it('PES delete removes the acquired-permit timeout ZSET', async () => {
    const r = make();
    const pes = r.getPermitExpirableSemaphore('p16:pes');
    const timeout = suffixName(pes.name, 'timeout');
    expect(await pes.trySetPermits(1)).toBe(true);
    expect(await pes.tryAcquire(60000)).not.toBeNull();
    expect(await r.redis.exists(pes.name, timeout)).toBe(2);
    await pes.delete();
    expect(await r.redis.exists(pes.name, timeout)).toBe(0);
    await r.shutdown();
  });

  it('set cache delete/clear removes idle and touch metadata', async () => {
    const r = make();
    const sc = r.getSetCache<string>('p16:sc');
    const idle = prefixName('redisson__idle__set', sc.name);
    const touch = suffixName(sc.name, 'redisson_touch');
    expect(await sc.add('v', 60000, 60000)).toBe(true);
    expect(await r.redis.exists(sc.name, idle, touch)).toBe(3);
    expect(await sc.delete()).toBe(true);
    expect(await r.redis.exists(sc.name, idle, touch)).toBe(0);

    await sc.add('v2', 60000, 60000);
    await sc.clear();
    expect(await r.redis.exists(sc.name, idle, touch)).toBe(0);
    await r.shutdown();
  });

  it('ring buffer delete removes capacity; clear keeps Java semantics', async () => {
    const r = make();
    const rb = r.getRingBuffer<number>('p16:rb');
    const cap = prefixName('redisson_rb', rb.name);
    expect(await rb.trySetCapacity(2)).toBe(true);
    await rb.add(1);
    await rb.clear();
    // Java clear() only deletes the LIST; capacity remains usable.
    expect(await rb.capacity()).toBe(2);
    await rb.add(2);
    expect(await rb.delete()).toBe(true);
    expect(await r.redis.exists(rb.name, cap)).toBe(0);
    expect(await rb.trySetCapacity(3)).toBe(true);
    await r.shutdown();
  });

  it('bounded blocking queue delete releases the capacity key', async () => {
    const r = make();
    const bq = r.getBoundedBlockingQueue<string>('p16:bq');
    const cap = prefixName('redisson_bqs', bq.name);
    expect(await bq.trySetCapacity(1)).toBe(true);
    expect(await bq.offer('a')).toBe(true);
    expect(await bq.offer('b')).toBe(false);
    expect(await bq.delete()).toBe(true);
    expect(await r.redis.exists(bq.name, cap)).toBe(0);
    // The old TS bug left redisson_bqs behind, so this stayed false forever.
    expect(await bq.trySetCapacity(2)).toBe(true);
    await r.shutdown();
  });

  it('multimap delete/clear remove child collections and TTL ZSET', async () => {
    const r = make();
    const mm = r.getSetMultimapCache<string, string>('p16:mm');
    await mm.put('k', 'v');
    const id = await r.redis.hget(mm.name, mm.encodeKey('k'));
    expect(id).not.toBeNull();
    const child = suffixName(mm.name, id!);
    const ttl = suffixName(mm.name, 'redisson_set_multimap_ttl');
    expect(await mm.expireKey('k', 60000)).toBe(true);
    expect(await r.redis.exists(mm.name, child, ttl)).toBe(3);
    expect(await mm.delete()).toBe(true);
    expect(await r.redis.exists(mm.name, child, ttl)).toBe(0);

    // clear() goes through the same path (HASH must be read first).
    await mm.put('k2', 'v2');
    const id2 = await r.redis.hget(mm.name, mm.encodeKey('k2'));
    const child2 = suffixName(mm.name, id2!);
    await mm.clear();
    expect(await r.redis.exists(mm.name, child2)).toBe(0);
    await r.shutdown();
  });

  it('native multimap delete removes deterministic child collections', async () => {
    const r = make();
    const s = r.getSetMultimapCacheNative<string, string>('p16:mmns');
    await s.put('k', 'v');
    const sid = await r.redis.hget(s.name, s.encodeKey('k'));
    const sc = suffixName(s.name, sid!);
    expect(await r.redis.exists(s.name, sc)).toBe(2);
    expect(await s.delete()).toBe(true);
    expect(await r.redis.exists(s.name, sc)).toBe(0);

    const l = r.getListMultimapCacheNative<string, string>('p16:mmnl');
    await l.put('k', 'v');
    const lid = await r.redis.hget(l.name, l.encodeKey('k'));
    const lc = suffixName(l.name, lid!);
    expect(await r.redis.exists(l.name, lc)).toBe(2);
    expect(await l.unlink()).toBe(true);
    expect(await r.redis.exists(l.name, lc)).toBe(0);
    await r.shutdown();
  });

  it('unlink clears companions for mapcache/bloom/timeseries/ratelimiter/topic', async () => {
    const r = make();
    const mcName = 'p16:mc';
    const mc = r.getMapCache<string, string>(mcName);
    await mc.put('k', 'v', 60000, 60000);
    const mcTtl = prefixName('redisson__timeout__set', mcName);
    const mcIdle = prefixName('redisson__idle__set', mcName);
    expect(await r.redis.exists(mcName, mcTtl, mcIdle)).toBe(3);
    await mc.unlink();
    expect(await r.redis.exists(mcName, mcTtl, mcIdle)).toBe(0);

    const bfName = 'p16:bf';
    const bf = r.getBloomFilter(bfName);
    expect(await bf.tryInit(1000, 0.03)).toBe(true);
    await bf.add('x');
    const cfg = suffixName(bfName, 'config');
    expect(await r.redis.exists(bfName, cfg)).toBe(2);
    await bf.unlink();
    expect(await r.redis.exists(bfName, cfg)).toBe(0);

    const tsName = 'p16:ts';
    const ts = r.getTimeSeries<number>(tsName);
    await ts.add(Date.now(), 1);
    const tsTtl = prefixName('redisson__ts_ttl', tsName);
    const tsSeq = prefixName('redisson__ts_seq', tsName);
    expect(await r.redis.exists(tsName, tsTtl, tsSeq)).toBe(3);
    await ts.unlink();
    expect(await r.redis.exists(tsName, tsTtl, tsSeq)).toBe(0);

    const rlName = 'p16:rl';
    const rl = r.getRateLimiter(rlName);
    await rl.setRate('OVERALL', 2, 60000);
    expect(await rl.tryAcquire(1)).toBe(true);
    const rlValue = suffixName(rlName, 'value');
    const rlPermits = suffixName(rlName, 'permits');
    await r.redis.zadd(rlPermits, '0', 'seeded');
    expect(await r.redis.exists(rlName, rlValue, rlPermits)).toBe(3);
    await rl.unlink();
    expect(await r.redis.exists(rlName, rlValue, rlPermits)).toBe(0);

    const rtName = 'p16:rt';
    const rtTimeout = suffixName(rtName, 'timeout');
    await r.redis.set(rtName, 'stream-placeholder');
    await r.redis.zadd(rtTimeout, '0', 'loop');
    await r.getReliableTopic(rtName).unlink();
    expect(await r.redis.exists(rtName, rtTimeout)).toBe(0);
    await r.shutdown();
  });

  it('delayed queue size reads staging; delete preserves the target queue', async () => {
    const r = make();
    const dq = r.getDelayedQueue<string>('p16:dq');
    const staging = prefixName('redisson_delay_queue', dq.name);
    const timeout = prefixName('redisson_delay_queue_timeout', dq.name);
    await r.redis.rpush(staging, 'packed-member');
    await r.redis.zadd(timeout, '0', 'packed-member');
    await r.redis.rpush(dq.name, '"target"');
    expect(await dq.size()).toBe(1);
    expect(await dq.targetSize()).toBe(1);
    expect(await dq.delete()).toBe(true);
    expect(await r.redis.exists(staging, timeout)).toBe(0);
    expect(await r.redis.lrange(dq.name, 0, -1)).toEqual(['"target"']);
    await r.shutdown();
  });

  it('local cached map honours timeToLiveMs and maxIdleMs', async () => {
    const r = make();
    const field = '"k"';
    const ttl = r.getLocalCachedMap<string, string>('p16:lcm-ttl', { timeToLiveMs: 50 });
    try {
      await ttl.put('k', 'near');
      await r.redis.hset(ttl.name, field, '"backend"');
      await new Promise((x) => setTimeout(x, 80));
      expect(await ttl.get('k')).toBe('backend');
    } finally {
      await ttl.destroy();
    }

    const idle = r.getLocalCachedMap<string, string>('p16:lcm-idle', { maxIdleMs: 50 });
    try {
      await idle.put('k', 'near');
      await r.redis.hset(idle.name, field, '"backend"');
      expect(await idle.get('k')).toBe('near'); // fresh within idle window
      await new Promise((x) => setTimeout(x, 80));
      expect(await idle.get('k')).toBe('backend');
    } finally {
      await idle.destroy();
      await r.shutdown();
    }
  });
});

describe('phase 19: key synchronizers + RW views + mapcache depth', () => {
  it('key synchronizers derive Java companion names across structures', async () => {
    const r = make();
    const h = hash128toBase64('"k1"');
    const m = r.getMap<string, string>('p19:sync');
    expect(m.getLock('k1').name).toBe(suffixName('p19:sync', `${h}:lock`));
    expect(m.getFairLock('k1').name).toBe(suffixName('p19:sync', `${h}:fairlock`));
    expect(m.getReadWriteLock('k1').readLock.name).toBe(suffixName('p19:sync', `${h}:rw_lock`));
    expect(m.getReadWriteLock('k1').writeLock.name).toBe(suffixName('p19:sync', `${h}:rw_lock`));
    expect(m.getSemaphore('k1').name).toBe(suffixName('p19:sync', `${h}:semaphore`));
    expect(m.getCountDownLatch('k1').name).toBe(suffixName('p19:sync', `${h}:countdownlatch`));
    expect(m.getPermitExpirableSemaphore('k1').name).toBe(suffixName('p19:sync', `${h}:permitexpirablesemaphore`));

    // the same companion is reachable from mapcache / multimap with the same name
    const mc = r.getMapCache<string, string>('p19:sync');
    expect(mc.getLock('k1').name).toBe(m.getLock('k1').name);
    const mm = r.getSetMultimap<string, string>('p19:sync');
    expect(mm.getLock('k1').name).toBe(m.getLock('k1').name);

    // set values use the value encoder; hash-tagged names stay tagged
    const s = r.getSet<string>('p19:sync-set');
    expect(s.getLock('v').name).toBe(suffixName('p19:sync-set', `${hash128toBase64('"v"')}:lock`));
    const tagged = r.getSet<string>('{p19:sync-tagged}');
    expect(tagged.getLock('v').name.startsWith('{p19:sync-tagged}:')).toBe(true);
    await r.shutdown();
  });

  it('map key locks are mutually exclusive across structure instances', async () => {
    const r = make();
    const a = r.getMap<string, string>('p19:ke');
    const b = r.getMap<string, string>('p19:ke');
    expect((a.getLock('k') as any).clientId).toBe(r.id);
    const l1 = a.getLock('k');
    expect(await l1.tryLock(0, 5000, 7)).toBe(true);
    expect(await b.getLock('k').tryLock(0, 5000, 8)).toBe(false);
    await l1.unlock(7);
    const l2 = b.getLock('k');
    expect(await l2.tryLock(0, 5000, 8)).toBe(true);
    expect(await l2.isHeldByThread(8)).toBe(true);
    await l2.unlock(8);
    await r.shutdown();
  });

  it('rw lock views expose isLocked/isHeldByThread/getHoldCount/forceUnlock', async () => {
    const r = make();
    const rw = r.getReadWriteLock('p19:rwv');
    const rl = rw.readLock;
    await rl.lock(5000, 1);
    expect(await rl.isLocked()).toBe(true);
    expect(await rl.isHeldByThread(1)).toBe(true);
    expect(await rl.isHeldByThread(2)).toBe(false);
    expect(await rl.getHoldCount(1)).toBe(1);
    await rl.lock(5000, 1);
    expect(await rl.getHoldCount(1)).toBe(2);
    await rl.unlock(1);
    await rl.unlock(1);
    expect(await rl.isLocked()).toBe(false);
    expect(await rl.forceUnlock()).toBe(false);

    const wl = rw.writeLock;
    await wl.lock(5000, 1);
    expect(await wl.isLocked()).toBe(true);
    expect(await wl.isHeldByThread(1)).toBe(true);
    expect(await wl.getHoldCount(1)).toBe(1);
    // Java: read view sees mode=='write' with hlen==2 as NOT locked
    expect(await rl.isLocked()).toBe(false);
    expect(await wl.forceUnlock()).toBe(true);
    expect(await wl.isLocked()).toBe(false);
    expect(await wl.isHeldByThread(1)).toBe(false);
    await r.shutdown();
  });

  it('multi/red locks accept fair locks and rw views (LockLike)', async () => {
    const r = make();
    const ml = r.newMultiLock(
      r.getLock('p19:ml:a'),
      r.getFairLock('p19:ml:b'),
      r.getReadWriteLock('p19:ml:c').writeLock,
    );
    expect(await ml.tryLock(1, 5000)).toBe(true);
    expect(await r.getLock('p19:ml:a').tryLock(0, 5000, 2)).toBe(false);
    await ml.unlock(1);
    const probe = r.getLock('p19:ml:a');
    expect(await probe.tryLock(0, 5000, 2)).toBe(true);
    await probe.unlock(2);

    const rl = r.newRedLock(
      r.getLock('p19:rl:a'),
      r.getLock('p19:rl:b'),
      r.getLock('p19:rl:c'),
    );
    expect(await rl.tryLock(5000, 1)).toBe(true);
    await rl.unlock(1);
    await r.shutdown();
  });

  it('mapcache putAll/expireEntry/remainTimeToLive follow Java expiry semantics', async () => {
    const r = make();
    const mc = r.getMapCache<string, string>('p19:mcd');
    const ttlSet = prefixName('redisson__timeout__set', 'p19:mcd');
    const idleSet = prefixName('redisson__idle__set', 'p19:mcd');
    await mc.putAll(new Map([['a', '1'], ['b', '2']]), 60000, 0);
    expect(await mc.get('a')).toBe('1');
    const ttl = await mc.remainTimeToLive('a');
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(60000);
    expect(await mc.remainTimeToLive('nope')).toBe(-2);

    await mc.put('c', '3');
    expect(await mc.remainTimeToLive('c')).toBe(-1);
    expect(await mc.expireEntry('c', 30000, 5000)).toBe(true);
    expect(await mc.remainTimeToLive('c')).toBeGreaterThan(0);
    // packed maxIdle delta rewritten, idle score present
    const raw = await (r.redis as any).hgetBuffer('p19:mcd', mc.encodeKey('c'));
    const u = unpackMapCacheValue(Buffer.from(raw));
    expect(u.ok).toBe(true);
    expect(u.maxIdle).toBe(5000);
    expect(await r.redis.zscore(idleSet, mc.encodeKey('c'))).not.toBeNull();
    // clearing both scores packs maxIdle 0 again
    expect(await mc.expireEntry('c', 0, 0)).toBe(true);
    expect(await mc.remainTimeToLive('c')).toBe(-1);
    expect(await r.redis.zscore(idleSet, mc.encodeKey('c'))).toBeNull();
    expect(await r.redis.zscore(ttlSet, mc.encodeKey('c'))).toBeNull();

    // putAll with ttl=0 clears the previous expiry
    await mc.putAll({ a: '10' });
    expect(await mc.get('a')).toBe('10');
    expect(await mc.remainTimeToLive('a')).toBe(-1);

    // already-expired entries refuse re-expiry (Java: no resurrection)
    await mc.put('d', '4');
    await r.redis.zadd(ttlSet, '1', mc.encodeKey('d'));
    expect(await mc.expireEntry('d', 30000)).toBe(false);
    expect(await mc.expireEntry('missing', 30000)).toBe(false);
    await r.shutdown();
  });

  it('mapcache listeners receive created/updated/removed events and gate has-listeners', async () => {
    const r = make();
    const mc = r.getMapCache<string, string>('p19:mcl');
    const events: string[] = [];
    const id = await mc.addListener({
      onCreated: (e) => events.push(`c:${e.key}:${e.value}`),
      onUpdated: (e) => events.push(`u:${e.key}:${e.value}:${e.oldValue}`),
      onRemoved: (e) => events.push(`r:${e.key}:${e.oldValue}`),
    });
    expect(id).toBeGreaterThan(0);
    const optionsKey = suffixName('p19:mcl', 'redisson_options');
    expect(await r.redis.hget(optionsKey, 'has-listeners')).toBe('1');

    await mc.put('k', 'v1');
    await mc.put('k', 'v2');
    await mc.remove('k');
    await new Promise((x) => setTimeout(x, 150));
    expect(events).toContain('c:k:v1');
    expect(events).toContain('u:k:v2:v1');
    expect(events).toContain('r:k:v2');

    await mc.removeListener(id);
    await mc.put('k2', 'v3');
    await new Promise((x) => setTimeout(x, 150));
    expect(events).not.toContain('c:k2:v3');
    await mc.removeAllListeners();
    await r.shutdown();
  });

  it('mapcache native conditional expiry degrades gracefully without HEXPIRE', async () => {
    const r = make();
    const mn = r.getMapCacheNative<string, string>('p19:mn');
    await mn.put('k', 'v');
    expect(await mn.get('k')).toBe('v');
    // ioredis-mock lacks the HEXPIRE family: methods must answer false, not throw
    expect(await mn.expireEntry('k', 1000)).toBe(false);
    expect(await mn.expireEntryIfNotSet('k', 1000)).toBe(false);
    expect(await mn.expireEntryIfGreater('k', 1000)).toBe(false);
    expect(await mn.expireEntryIfLess('k', 1000)).toBe(false);
    expect(await mn.clearExpire('k')).toBe(false);
    // putIfAbsent still guards atomically; only the field TTL is unavailable
    expect(await mn.putIfAbsent('k', 'other', 5000)).toBe('v');
    expect(await mn.get('k')).toBe('v');
    expect(await mn.putIfAbsent('fresh', 'n1', 5000)).toBe(null);
    expect(await mn.get('fresh')).toBe('n1');
    expect(await mn.putIfAbsent('fresh', 'n2')).toBe('n1');
    await r.shutdown();
  });
});

describe('phase 21: delayed-queue background migrator (mock)', () => {
  it('start/stop/destroy are idempotent no-ops on mock and never drop data', async () => {
    const r = make();
    const dq = r.getDelayedQueue<string>('p21:dq');

    // mock offers land straight in the destination (no Lua struct support)
    await dq.offer('v1', 5000);
    expect(await dq.targetSize()).toBe(1);

    await dq.start();
    await dq.start(); // idempotent
    await new Promise((x) => setTimeout(x, 50));
    expect(await dq.targetSize()).toBe(1); // no duplicate, no loss

    await dq.stop();
    await dq.stop(); // idempotent
    await dq.offer('v2', 5000);
    expect(await dq.poll()).toBe('v1'); // manual catch-up still works
    expect(await dq.poll()).toBe('v2');

    // destroy(): detaches the transfer task only — data survives untouched
    await dq.offer('held', 60000);
    await dq.destroy();
    await dq.start(); // terminal after destroy: resolves, does nothing
    expect(await dq.targetSize()).toBe(1);
    expect(await dq.poll()).toBe('held');

    await r.shutdown();
  });
});

describe('phase 22: §3 leftovers (mock)', () => {
  const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
  const waitFor = async (check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (await check()) return true;
      await sleep(50);
    }
    return check();
  };

  it('fenced tryLockAndGetToken: non-blocking, waiting and contention', async () => {
    const r = make();
    const fl = r.getFencedLock('p22:fl');
    const t1 = await fl.tryLockAndGetToken();
    expect(t1).not.toBeNull();
    expect(await fl.getToken()).toBe(t1);
    await fl.unlock();

    // contended by another thread: non-blocking → null; waiting → null on timeout
    const held = await fl.lockAndGetToken(30000, 2);
    expect(held).not.toBeNull();
    expect(await fl.tryLockAndGetToken(0, 30000, 1)).toBeNull();
    const waited = await fl.tryLockAndGetToken(120, 30000, 1);
    expect(waited).toBeNull();
    await fl.unlock(2);

    // free again: waiting form succeeds immediately
    const t2 = await fl.tryLockAndGetToken(500);
    expect(t2).not.toBeNull();
    expect(t2!).toBeGreaterThan(t1!);
    await fl.unlock();
    await r.shutdown();
  });

  it('blocking queue pollFromAny family + deque move/BLMOVE', async () => {
    const r = make();
    const q = r.getBlockingQueue<string>('p22:q');
    const other = r.getBlockingQueue<string>('p22:q2');
    await q.offer('a');
    await q.offer('b');
    expect(await q.pollFromAny(0, 'p22:q2')).toBe('a'); // this queue has priority
    await other.offer('x');
    // own queue still has 'b' → argument order wins over the other queue
    expect(await q.pollFromAnyWithName(0, 'p22:q2')).toEqual({ key: 'p22:q', value: 'b' });
    expect(await q.pollFromAnyWithName(0, 'p22:q2')).toEqual({ key: 'p22:q2', value: 'x' });
    expect(await q.pollFromAny(0, 'p22:q2')).toBeNull(); // both empty, non-blocking

    await q.offer('1');
    await q.offer('2');
    expect((await q.pollFirstFromAny(0, 2)).get('p22:q')).toEqual(['1', '2']);
    await q.offer('3');
    await q.offer('4');
    expect((await q.pollLastFromAny(0, 2)).get('p22:q')).toEqual(['4', '3']);
    // empty + non-blocking → empty map
    expect((await q.pollFirstFromAny(0, 1)).size).toBe(0);

    const d = r.getBlockingDeque<string>('p22:d');
    const d2 = r.getBlockingDeque<string>('p22:d2');
    await d.add('h');
    await d.add('t'); // d=[h, t]
    expect(await d.move('p22:d2', 'RIGHT', 'LEFT')).toBe('t'); // d=[h], d2=[t]
    expect(await d2.peekFirst()).toBe('t');
    // BLMOVE back: d2=[], d=[h, t] (non-blocking timeout=0 path)
    expect(await d2.moveWithTimeout('p22:d', 'LEFT', 'RIGHT', 0)).toBe('t');
    expect(await d2.size()).toBe(0);
    expect(await d.pollLastAndOfferFirstTo('p22:d2', 0)).toBe('t'); // d=[h], d2=[t]
    expect(await d.takeLastAndOfferFirstTo('p22:d2')).toBe('h'); // d=[], d2=[t, h]
    expect(await d.size()).toBe(0);
    // empty source with a deadline: polling fallback returns null in time
    const t0 = Date.now();
    expect(await d.moveWithTimeout('p22:d2', 'LEFT', 'RIGHT', 100)).toBeNull();
    expect(Date.now() - t0).toBeGreaterThanOrEqual(80);
    await r.shutdown();
  });

  it('rate limiter keepAlive is renewed on every successful acquire', async () => {
    const r = make();
    const rl = r.getRateLimiter('p22:rl');
    await rl.setRate('OVERALL', 10, 1000, 3000);
    expect(await rl.tryAcquire()).toBe(true);
    const valueKey = suffixName('p22:rl', 'value');
    const permitsKey = suffixName('p22:rl', 'permits');
    for (const k of ['p22:rl', valueKey]) {
      const ttl = await r.redis.pttl(k);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(3000);
    }
    // a later successful acquire pushes the TTL back to ~keepAlive
    await sleep(60);
    expect(await rl.tryAcquire()).toBe(true);
    expect(await r.redis.pttl(valueKey)).toBeGreaterThan(2900);
    // permits ZSET exists only on real Redis (mock's Lua has no struct.pack)
    if ((await r.redis.exists(permitsKey)) === 1) {
      expect(await r.redis.pttl(permitsKey)).toBeGreaterThan(2900);
    }
    await r.shutdown();
  });

  it('local cached map: LRU/LFU eviction + getCachedMap snapshot', async () => {
    const r = make();
    const lru = r.getLocalCachedMap<string, string>('p22:lcm-lru', { maxSize: 2, evictionPolicy: 'LRU' });
    await lru.put('a', '1');
    await lru.put('b', '2');
    await sleep(15);
    expect(await lru.get('a')).toBe('1'); // a becomes most recently used
    await lru.put('c', '3'); // evicts b, not a
    expect([...lru.getCachedMap().keys()].sort()).toEqual(['a', 'c']);
    await r.redis.hset('p22:lcm-lru', '"b"', '"fresh-b"');
    expect(await lru.get('b')).toBe('fresh-b'); // b was evicted → fresh read
    // ... and caching b now evicts a (older than c), proving LRU order
    expect([...lru.getCachedMap().keys()].sort()).toEqual(['b', 'c']);

    const lfu = r.getLocalCachedMap<string, string>('p22:lcm-lfu', { maxSize: 2, evictionPolicy: 'LFU' });
    await lfu.put('a', '1');
    await lfu.put('b', '2');
    await lfu.get('a');
    await lfu.get('a'); // a: 2 accesses, b: 0
    await lfu.put('c', '3'); // evicts b (fewest accesses)
    expect([...lfu.getCachedMap().keys()].sort()).toEqual(['a', 'c']);
    await r.redis.hset('p22:lcm-lfu', '"b"', '"fresh-b"');
    expect(await lfu.get('b')).toBe('fresh-b');
    // caching b evicts c (0 hits), never the hot a (2 hits)
    expect([...lfu.getCachedMap().keys()].sort()).toEqual(['a', 'b']);
    await r.redis.hset('p22:lcm-lfu', '"a"', '"stale-a"');
    expect(await lfu.get('a')).toBe('1'); // a survived, still cached
    await r.shutdown();
  });

  it('multimap cache periodic eviction sweeps expired children without a read', async () => {
    const r = make();
    const mm = r.getSetMultimapCache<string, string>('p22:mm');
    await mm.put('k1', 'v1');
    await mm.put('k2', 'v2');
    expect(await r.redis.hlen('p22:mm')).toBe(2);
    await mm.expireKey('k1', 100);
    mm.enablePeriodicEviction(50);
    try {
      expect(await waitFor(async () => (await r.redis.hlen('p22:mm')) === 1, 2000)).toBe(true);
      expect(await r.redis.zcard(suffixName('p22:mm', 'redisson_set_multimap_ttl'))).toBe(0);
    } finally {
      mm.disablePeriodicEviction();
    }
    await r.shutdown();
  });

  it('batch facades: deque/atomic double/bitset/stream/mapcachenative', async () => {
    const r = make();
    const batch = r.createBatch();
    const d = batch.getDeque<number>('p22:bdq');
    d.offerFirst(1);
    d.offerLast(2);
    const df = d.pollFirst();
    const dl = d.pollLast();
    const dsz = d.size();
    const ad = batch.getAtomicDouble('p22:bad');
    ad.set(1.5);
    const aplus = ad.addAndGet(0.25);
    const aget = ad.get();
    const bs = batch.getBitSet('p22:bbs');
    const bset = bs.set(3);
    const bget = bs.get(3);
    const bcard = bs.cardinality();
    const bsize = bs.size();
    const st = batch.getStream('p22:bst');
    const sid = st.add({ a: 1 });
    const ssz = st.size();
    const mc = batch.getMapCacheNative<string, number>('p22:bmc');
    const mput = mc.put('k', 5);
    const mget = mc.get('k');
    const mrm = mc.remove('k');
    await batch.execute();
    expect(await df).toBe(1);
    expect(await dl).toBe(2);
    expect(await dsz).toBe(0);
    expect(await aplus).toBeCloseTo(1.75, 10);
    expect(await aget).toBeCloseTo(1.75, 10);
    expect(await bset).toBe(false); // previous bit value
    expect(await bget).toBe(true);
    expect(await bcard).toBe(1);
    expect(await bsize).toBe(8);
    expect(String(await sid)).toMatch(/^\d+-\d+$/);
    expect(await ssz).toBe(1);
    expect(await mput).toBeNull();
    expect(await mget).toBe(5);
    expect(await mrm).toBe(5);
    await r.shutdown();
  });
});

describe('phase 23: RKeys ops + atomic scalars (mock)', () => {
  it('expire overload/expireAt/clearExpire/remainTimeToLive/getSlot/unlinkByPattern', async () => {
    const r = make();
    await r.redis.set('p23:k1', 'v');
    await r.redis.set('p23:k2', 'v');
    expect(await r.getKeys().expire('p23:k1', 60000)).toBe(true);
    expect(await r.getKeys().remainTimeToLive('p23:k1')).toBeGreaterThan(0);
    expect(await r.getKeys().clearExpire('p23:k1')).toBe(true);
    expect(await r.getKeys().remainTimeToLive('p23:k1')).toBe(-1);
    expect(await r.getKeys().remainTimeToLive('p23:missing')).toBe(-2);

    // Java expire(Duration, String...) multi-key form (overload by first arg)
    expect(await r.getKeys().expire(60000, 'p23:k1', 'p23:k2')).toBe(2);
    const ts = Date.now() + 60000;
    expect(await r.getKeys().expireAt('p23:k1', ts)).toBe(true);
    expect(await r.getKeys().expireAt(ts, 'p23:k1', 'p23:k2')).toBe(2);

    // cluster slot: CRC16-XMODEM vector + hash-tag equality
    expect(await r.getKeys().getSlot('123456789')).toBe(12739);
    expect(await r.getKeys().getSlot('{user1000}.a')).toBe(await r.getKeys().getSlot('{user1000}.b'));
    expect(await r.getKeys().getSlot('foo{}{bar}')).toBe(await r.getKeys().getSlot('foo{}{bar}'));

    await r.redis.set('p23:pat:a', '1');
    await r.redis.set('p23:pat:b', '1');
    expect(await r.getKeys().unlinkByPattern('p23:pat:*')).toBe(2);
    await r.shutdown();
  });

  it('atomic Long/Double remainder: getAndInc/Dec, setIfLess/Greater, getAndDelete, compareAndDelete', async () => {
    const r = make();
    const l = r.getAtomicLong('p23:l');
    await l.set(10);
    expect(await l.getAndIncrement()).toBe(10);
    expect(await l.getAndDecrement()).toBe(11);
    expect(await l.get()).toBe(10);
    expect(await l.setIfLess(11, 5)).toBe(true); // 10 < 11 → set 5
    expect(await l.setIfLess(4, 99)).toBe(false); // 5 >= 4
    expect(await l.get()).toBe(5);
    expect(await l.setIfGreater(4, 7)).toBe(true); // 5 > 4 → set 7
    expect(await l.setIfGreater(8, 1)).toBe(false); // 7 <= 8
    expect(await l.getAndDelete()).toBe(7);
    expect(await l.get()).toBe(0);

    const d = r.getAtomicDouble('p23:d');
    await d.set(2.5);
    expect(await d.incrementAndGet()).toBe(3.5);
    expect(await d.decrementAndGet()).toBe(2.5);
    expect(await d.getAndIncrement()).toBe(2.5);
    expect(await d.getAndDecrement()).toBe(3.5);
    expect(await d.get()).toBe(2.5);
    expect(await d.setIfLess(3, 1.5)).toBe(true);
    expect(await d.setIfGreater(2, 3.3)).toBe(false);
    expect(await d.compareAndDelete(2)).toBe(false);
    expect(await d.compareAndDelete(1.5)).toBe(true);
    expect(await d.getAndDelete()).toBeNull();
    await r.shutdown();
  });

  it('zset addIfAbsent/addIfExists honor the Java ZADD flags (mock has no GT/LT)', async () => {
    const r = make();
    const z = r.getScoredSortedSet<string>('p23:z');
    expect(await z.addIfAbsent(1, 'a')).toBe(true);
    expect(await z.addIfAbsent(2, 'a')).toBe(false);
    expect(await z.getScore('a')).toBe(1);
    expect(await z.addIfExists(2, 'a')).toBe(true);
    expect(await z.getScore('a')).toBe(2);
    expect(await z.addIfExists(9, 'ghost')).toBe(false);
    expect(await z.contains('ghost')).toBe(false);
    await r.shutdown();
  });

  it('buckets conditional writes require MSETEX and fail loudly without it', async () => {
    const r = make();
    const b = r.getBuckets();
    await expect(b.setIfAllKeysAbsent({ 'p23:b:a': 1 })).rejects.toThrow(/MSETEX/);
    await expect(b.setIfAllKeysExist({ 'p23:b:a': 1 })).rejects.toThrow(/MSETEX/);
    await r.shutdown();
  });
});

describe('phase 25: RTimeSeries read/delete family (mock)', () => {
  it('addAll, entry reads, ranges and poll/remove', async () => {
    const r = make();
    const ts = r.getTimeSeries<number>('p25:ts');
    await ts.addAll([
      { timestamp: 1000, value: 10 },
      { timestamp: 2000, value: 20, label: 'l2' },
      { timestamp: 3000, value: 30 },
    ]);
    expect(await ts.size()).toBe(3);
    const e2 = await ts.getEntry(2000);
    expect(e2?.value).toBe(20);
    expect(e2?.label).toBe('l2');
    expect(await ts.getValue(2000)).toBe(20);
    expect(await ts.first()).toBe(10);
    expect(await ts.last()).toBe(30);
    expect(await ts.firstTimestamp()).toBe(1000);
    expect(await ts.lastTimestamp()).toBe(3000);
    expect((await ts.firstEntries(2)).map((e) => e.value)).toEqual([10, 20]);
    expect((await ts.lastEntries(2)).map((e) => e.value)).toEqual([20, 30]);
    expect((await ts.range(1000, 2000)).map((e) => e.value)).toEqual([10, 20]);
    expect((await ts.rangeReversed(1000, 3000)).map((e) => e.value)).toEqual([30, 20, 10]);
    expect((await ts.entryRange(1000, 3000, 2)).map((e) => e.value)).toEqual([10, 20]);
    expect((await ts.entryRangeReversed(1000, 3000, 2)).map((e) => e.value)).toEqual([30, 20]);

    expect(await ts.getAndRemove(2000)).toBe(20);
    expect(await ts.getEntry(2000)).toBeNull();
    expect(await ts.pollFirst()).toBe(10);
    expect(await ts.pollLast()).toBe(30);
    expect(await ts.size()).toBe(0);
    expect(await ts.pollFirst()).toBeNull();
    expect(await ts.pollLastEntries(2)).toEqual([]);
    await r.shutdown();
  });

  it('remove/removeRange delete only live entries', async () => {
    const r = make();
    const ts = r.getTimeSeries<string>('p25:ts2');
    await ts.addAll([
      { timestamp: 1000, value: 'a' },
      { timestamp: 2000, value: 'b' },
      { timestamp: 3000, value: 'c' },
    ]);
    expect(await ts.remove(2000)).toBe(true);
    expect(await ts.remove(2000)).toBe(false);
    expect(await ts.removeRange(1000, 3000)).toBe(2);
    expect(await ts.size()).toBe(0);
    await r.shutdown();
  });
});

describe('phase 24: RScoredSortedSet remainder (mock)', () => {
  it('addAll family + firstEntry/lastEntry', async () => {
    const r = make();
    const z = r.getScoredSortedSet<string>('p24:z');
    expect(await z.addAll(new Map([['a', 1], ['b', 2]]))).toBe(2);
    expect(await z.addAll(new Map([['a', 5], ['c', 3]]))).toBe(2); // both changed/added
    expect(await z.getScore('a')).toBe(5);
    expect(await z.getScore('c')).toBe(3);

    expect(await z.addAllIfAbsent(new Map([['a', 9], ['d', 4]]))).toBe(1); // only d
    expect(await z.getScore('a')).toBe(5);
    expect(await z.getScore('d')).toBe(4);

    expect(await z.addAllIfExist(new Map([['a', 6], ['ghost', 1]]))).toBe(1); // only a
    expect(await z.getScore('a')).toBe(6);
    expect(await z.contains('ghost')).toBe(false);

    expect(await z.firstEntry()).toEqual({ value: 'b', score: 2 });
    expect(await z.lastEntry()).toEqual({ value: 'a', score: 6 });
    await r.shutdown();
  });

  it('pollFirst/LastEntry and blocking poll with timeout (polling fallback)', async () => {
    const r = make();
    const z = r.getScoredSortedSet<string>('p24:poll');
    await z.addAll(new Map([['a', 1], ['b', 2]]));
    expect(await z.pollFirstEntry()).toEqual({ value: 'a', score: 1 });
    expect(await z.pollLastEntry()).toEqual({ value: 'b', score: 2 });
    expect(await z.pollFirstEntry()).toBeNull();

    // non-blocking with data
    await z.add(3, 'c');
    expect(await z.pollFirstWithTimeout(0)).toEqual({ value: 'c', score: 3 });
    // deadline on an empty set → null after ~timeout (mock has no BZPOPMIN)
    const t0 = Date.now();
    expect(await z.pollFirstWithTimeout(100)).toBeNull();
    expect(Date.now() - t0).toBeGreaterThanOrEqual(80);

    // FromAny: own key wins when both are populated; otherwise source key reported
    await z.add(1, 'own');
    const other = r.getScoredSortedSet<string>('p24:other');
    await other.add(2, 'other');
    expect(await z.pollFirstFromAny(0, 'p24:other')).toEqual({ key: 'p24:poll', value: 'own', score: 1 });
    expect(await z.pollFirstFromAny(0, 'p24:other')).toEqual({ key: 'p24:other', value: 'other', score: 2 });
    expect((await z.pollLastFromAny(0, 'p24:other'))).toBeNull();
    await r.shutdown();
  });

  it('random/randomEntries work through the mock fallback and BYSCORE ranges stay correct', async () => {
    const r = make();
    const z = r.getScoredSortedSet<string>('p24:rnd');
    await z.addAll(new Map([['a', 1], ['b', 2], ['c', 3]]));
    const one = await z.random();
    expect(['a', 'b', 'c']).toContain(one);
    expect((await z.random(10)).sort()).toEqual(['a', 'b', 'c']);
    const entries = await z.randomEntries(2);
    expect(entries.size).toBe(2);
    for (const [m, s] of entries) expect({ a: 1, b: 2, c: 3 }[m]).toBe(s);

    expect(await z.rangeByScore(1, 2)).toEqual(['a', 'b']);
    expect(await z.rangeByScore(1, 3, 1, 1)).toEqual(['b']);
    expect(await z.valueRangeReversed(1, true, 3, true)).toEqual(['c', 'b', 'a']);
    expect(await z.entryRange(1.5, false, 3, true)).toEqual([
      { value: 'b', score: 2 },
      { value: 'c', score: 3 },
    ]);
    expect(await z.entryRangeReversed(1.5, false, 3, true)).toEqual([
      { value: 'c', score: 3 },
      { value: 'b', score: 2 },
    ]);
    await r.shutdown();
  });
});

describe('phase 26: topic management + set counts (mock)', () => {
  it('RTopic/RPatternTopic/RShardedTopic management surfaces', async () => {
    const r = make();
    const t = r.getTopic<string>('p26:t');
    expect(await t.getChannelNames()).toEqual(['p26:t']);
    expect(await t.countListeners()).toBe(0);
    const id = await t.addListener(() => undefined);
    expect(await t.countListeners()).toBe(1);
    await t.removeListener(id);
    expect(await t.countListeners()).toBe(0);

    const p = r.getPatternTopic<string>('p26:*');
    expect(await p.getPatternNames()).toEqual(['p26:*']);
    expect(await p.getActiveTopics()).toEqual([]); // mock has no PUBSUB CHANNELS

    const sh = r.getShardedTopic<string>('p26:s');
    expect(await sh.getChannelNames()).toEqual(['p26:s']);
    const sid = await sh.addListener(() => undefined);
    expect(await sh.countListeners()).toBe(sid === 0 ? 0 : 1);
    await sh.removeAllListeners();
    expect(await sh.countListeners()).toBe(0);
    await r.shutdown();
  });

  it('RSet addAllCounted/removeAllCounted/containsEach', async () => {
    const r = make();
    const s = r.getSet<number>('p26:set');
    expect(await s.addAllCounted([1, 2, 3])).toBe(3);
    expect(await s.addAllCounted([2, 3, 4])).toBe(1);
    expect(await s.containsEach([1, 5, 4])).toEqual([1, 4]);
    expect(await s.containsAll(1, 2, 3, 9)).toBe(3);
    expect(await s.removeAllCounted([1, 2])).toBe(2);
    expect((await s.readAll()).sort()).toEqual([3, 4]);
    await r.shutdown();
  });
});

describe('phase 27: map views + time-series stream (mock)', () => {
  it('RMap keySet/values/entrySet views, readAllEntrySet, replace/remove overloads', async () => {
    const r = make();
    const m = r.getMap<string, number>('p27:m');
    await m.putAll({ a: 1, b: 2, ab: 3 });
    expect((await m.keySet()).sort()).toEqual(['a', 'ab', 'b']);
    // MATCH applies to the ENCODED field names (HSCAN semantics, as in Java)
    expect((await m.keySet('"a*')).sort()).toEqual(['a', 'ab']);
    expect((await m.values()).sort()).toEqual([1, 2, 3]);
    expect((await m.entrySet('"a*')).sort()).toEqual([['a', 1], ['ab', 3]]);
    expect((await m.readAllEntrySet()).length).toBe(3);

    expect(await m.replace('a', 9)).toBe(1);
    expect(await m.replace('a', 9, 10)).toBe(true);
    expect(await m.replace('a', 9, 11)).toBe(false); // stale old value
    expect(await m.get('a')).toBe(10);
    expect(await m.remove('a', 11)).toBe(false);
    expect(await m.remove('a', 10)).toBe(true);
    expect(await m.get('a')).toBeNull();
    await r.shutdown();
  });

  it('RTimeSeries stream/iterator page values in ascending order', async () => {
    const r = make();
    const ts = r.getTimeSeries<number>('p27:ts');
    await ts.addAll([1000, 2000, 3000, 4000, 5000].map((t, i) => ({ timestamp: t, value: (i + 1) * 10 })));
    const streamed: number[] = [];
    for await (const v of ts.stream(2)) streamed.push(v);
    expect(streamed).toEqual([10, 20, 30, 40, 50]);
    const iterated: number[] = [];
    for await (const v of ts.iterator(3)) iterated.push(v);
    expect(iterated).toEqual([10, 20, 30, 40, 50]);
    await r.shutdown();
  });
});

describe('phase 28: interruptible lock + mapcache remainder (mock)', () => {
  it('lockInterruptibly rejects with AbortError before acquiring', async () => {
    const r = make();
    const l = r.getLock('p28:lock');
    const pre = new AbortController();
    pre.abort();
    await expect(l.lockInterruptibly(undefined, pre.signal)).rejects.toMatchObject({ name: 'AbortError' });
    expect(await l.isLocked()).toBe(false);

    await l.lockInterruptibly(30000);
    expect(await l.isLocked()).toBe(true);
    await l.unlock();

    // contended: holder on thread 2, waiter aborts while blocked
    await l.lock(30000, 2);
    const ac = new AbortController();
    const pending = l.lockInterruptibly(undefined, ac.signal, 1);
    setTimeout(() => ac.abort(), 120);
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect(await l.isLocked()).toBe(true); // holder untouched
    await l.unlock(2);
    expect(await l.isLocked()).toBe(false);
    await r.shutdown();
  });

  it('RMapCache expireEntries/getWithTTLOnly/getMaxSize', async () => {
    const r = make();
    const mc = r.getMapCache<string, string>('p28:mc');
    await mc.put('a', '1');
    await mc.put('b', '2');
    expect(await mc.getMaxSize()).toBe(0);
    expect(await mc.expireEntries(['a', 'b', 'ghost'], 60000)).toBe(2);
    expect(await mc.getWithTTLOnly('a')).toBe('1');
    expect(await mc.getWithTTLOnly('ghost')).toBeNull();
    await mc.setMaxSize(5, 'LRU');
    expect(await mc.getMaxSize()).toBe(5);
    await r.shutdown();
  });
});

describe('phase 29: rwlock_timeout protocol (mock)', () => {
  it('per-acquisition timeout keys + hash TTL recomputed while readers remain', async () => {
    const r = make();
    const name = 'p29:rw';
    const rl = r.getReadWriteLock(name).getReadLock();
    const tagged = '{p29:rw}';
    await rl.lock(30000, 1);
    const f1 = (await r.redis.hkeys(name)).find((f) => f !== 'mode')!;
    expect(await r.redis.exists(`${tagged}:${f1}:rwlock_timeout:1`)).toBe(1);

    await rl.lock(30000, 1); // reentrant acquisition → second timeout key
    const afterSecond = (await r.redis.keys(`${tagged}:${f1}:rwlock_timeout:*`)).sort();
    expect(afterSecond.length).toBe(2);
    await rl.unlock(1);
    const afterRelease = await r.redis.keys(`${tagged}:${f1}:rwlock_timeout:*`);
    expect(afterRelease).toEqual([`${tagged}:${f1}:rwlock_timeout:1`]);
    expect(await r.redis.hexists(name, f1)).toBe(1);

    // a second reader keeps the lock alive after the first one fully releases
    await rl.lock(30000, 2);
    const f2 = (await r.redis.hkeys(name)).find((f) => f !== 'mode' && f !== f1)!;
    expect((await r.redis.keys(`${tagged}:${f2}:rwlock_timeout:*`)).length).toBe(1);
    await rl.unlock(1); // first holder gone, second still holds
    expect(await r.redis.exists(name)).toBe(1);
    expect(await r.redis.hget(name, 'mode')).toBe('read');
    expect(await r.redis.pttl(name)).toBeGreaterThan(0);
    expect((await r.redis.keys(`${tagged}:${f2}:rwlock_timeout:*`)).length).toBe(1);

    await rl.unlock(2);
    expect(await r.redis.exists(name)).toBe(0);
    expect((await r.redis.keys(`${tagged}:${f2}:rwlock_timeout:*`)).length).toBe(0);
    await r.shutdown();
  });
});

describe('phase 30: mapcache compute + status listeners (mock)', () => {
  it('compute/computeIfAbsent serialize per key and honour TTL/removal', async () => {
    const r = make();
    const mc = r.getMapCache<string, number>('p30:mc');
    expect(await mc.compute('k', 60000, (_k, old) => (old ?? 0) + 1)).toBe(1);
    expect(await mc.compute('k', 60000, (_k, old) => (old ?? 0) + 1)).toBe(2);
    expect(await mc.compute('k', 60000, () => null)).toBeNull();
    expect(await mc.get('k')).toBeNull();

    await Promise.all([1, 2, 3, 4, 5].map(() => mc.compute('c', 60000, (_k, old) => (old ?? 0) + 1)));
    expect(await mc.get('c')).toBe(5);

    expect(await mc.computeIfAbsent('a', 60000, () => 7)).toBe(7);
    expect(await mc.computeIfAbsent('a', 60000, () => 9)).toBe(7);
    expect(await mc.computeIfAbsent('b', 60000, () => null)).toBeNull();
    expect(await mc.get('b')).toBeNull();
    await r.shutdown();
  });

  it('RTopic status listeners fire on add/remove/removeAll', async () => {
    const r = make();
    const t = r.getTopic<string>('p30:t');
    const events: string[] = [];
    const status = {
      onSubscribe: (ch: string) => events.push(`sub:${ch}`),
      onUnsubscribe: (ch: string) => events.push(`unsub:${ch}`),
    };
    const sid = await t.addStatusListener(status);
    expect(events).toEqual(['sub:p30:t']);
    expect(await t.countListeners()).toBe(1);
    await t.removeListener(sid);
    expect(events).toEqual(['sub:p30:t', 'unsub:p30:t']);
    expect(await t.countListeners()).toBe(0);

    await t.addStatusListener(status);
    await t.addListener(() => undefined);
    expect(await t.countListeners()).toBe(2);
    await t.removeAllListeners();
    expect(await t.countListeners()).toBe(0);
    await r.shutdown();
  });
});

describe('phase 31: keyspace listeners + multi-stream group read (mock)', () => {
  it('stream/time-series listener lifecycle and empty readGroupMany', async () => {
    const r = make();
    const st = r.getStream('p31:s');
    const id = await st.addListener({ onAdd: () => undefined, onRemove: () => undefined });
    expect(id).toBeGreaterThan(0);
    await st.removeListener(id);
    await st.addListener({ onAdd: () => undefined });
    await st.removeAllListeners();
    expect((await st.readGroupMany('g', 'c', {})).size).toBe(0);

    const ts = r.getTimeSeries<number>('p31:ts');
    const tid = await ts.addListener({ onAdd: () => undefined, onRemove: () => undefined });
    expect(tid).toBeGreaterThan(0);
    await ts.removeListener(tid);
    await ts.addListener({ onRemove: () => undefined });
    await ts.removeAllListeners();
    await r.shutdown();
  });
});

describe('phase 33: bitset batch + delayed queue reads (mock)', () => {
  it('RBitSet batch/range/asBitSet', async () => {
    const r = make();
    const b = r.getBitSet('p33:bits');
    await b.set([1, 3, 8]);
    expect(await b.get(1)).toBe(true);
    expect(await b.get(1, 2, 3)).toEqual([true, false, true]);
    expect([...(await b.asBitSet())].sort((x, y) => x - y)).toEqual([1, 3, 8]);
    await b.set(4, 7, true); // setRange: [4, 7)
    expect(await b.get(4, 5, 6, 7)).toEqual([true, true, true, false]);
    await b.set(new Set([2]), true);
    expect(await b.get(2)).toBe(true);
    await b.clear();
    expect(await b.size()).toBe(0);
    await r.shutdown();
  });

  it('RDelayedQueue readAll/poll(count)', async () => {
    const r = make();
    const dq = r.getDelayedQueue<string>('p33:dq');
    await dq.offer('a', 5000);
    await dq.offer('b', 5000);
    expect(await dq.readAll()).toEqual(['a', 'b']);
    expect(await dq.poll(2)).toEqual(['a', 'b']);
    expect(await dq.poll()).toBeNull();
    await r.shutdown();
  });
});

describe('phase 32: API polish (mock)', () => {
  it('RKeys lazy key streams', async () => {
    const r = make();
    await r.redis.mset('p32:s:a', '1', 'p32:s:b', '1', 'p32:other', '1');
    const keys: string[] = [];
    for await (const k of r.getKeys().getKeysStream('p32:s:*')) keys.push(k);
    expect(keys.sort()).toEqual(['p32:s:a', 'p32:s:b']);
    const all: string[] = [];
    for await (const k of r.getKeys().getKeysStreamByPattern('p32:*')) all.push(k);
    expect(all.sort()).toEqual(['p32:other', 'p32:s:a', 'p32:s:b']);
    await r.shutdown();
  });

  it('topic removeListener(callback) and per-group readGroupMany overload', async () => {
    const r = make();
    const t = r.getTopic<string>('p32:t');
    const cb = () => undefined;
    await t.addListener(cb);
    expect(await t.countListeners()).toBe(1);
    await t.removeListener(cb);
    expect(await t.countListeners()).toBe(0);
    await t.removeListener(() => undefined); // unknown callback: no-op

    const sh = r.getShardedTopic<string>('p32:sh');
    const scb = () => undefined;
    const sid = await sh.addListener(scb);
    expect(await sh.countListeners()).toBe(sid === 0 ? 0 : 1);
    await sh.removeListener(scb);
    expect(await sh.countListeners()).toBe(0);

    const p = r.getPatternTopic<string>('p32:*');
    const pcb = () => undefined;
    await p.addListener(pcb);
    await p.removeListener(pcb);
    await p.removeAllListeners();

    const st = r.getStream('p32:stream');
    expect((await st.readGroupMany({}, 10)).size).toBe(0);
    expect((await st.readGroupMany('g', 'c', {}, 10)).size).toBe(0);
    await r.shutdown();
  });
});

describe('phase 38: lazy iterators + preload count (mock)', () => {
  it('RSet iterators and RScoredSortedSet value/entry iterators', async () => {
    const r = make();
    const s = r.getSet<string>('p38:s');
    await s.add('a', 'b', 'c');
    const vals: string[] = [];
    for await (const v of s.iterator()) vals.push(v);
    expect(vals.sort()).toEqual(['a', 'b', 'c']);
    const dv: string[] = [];
    for await (const v of s.distributedIterator()) dv.push(v);
    expect(dv.sort()).toEqual(['a', 'b', 'c']);
    const sv: string[] = [];
    for await (const v of s.stream()) sv.push(v);
    expect(sv.sort()).toEqual(['a', 'b', 'c']);

    const z = r.getScoredSortedSet<string>('p38:z');
    await z.add(1, 'a');
    await z.add(2, 'b');
    const zv: string[] = [];
    for await (const v of z.iterator()) zv.push(v);
    expect(zv.sort()).toEqual(['a', 'b']);
    const ze: { value: string; score: number }[] = [];
    for await (const e of z.entriesIterator()) ze.push(e);
    expect(ze.sort((x, y) => x.score - y.score)).toEqual([
      { value: 'a', score: 1 },
      { value: 'b', score: 2 },
    ]);
    await r.shutdown();
  });

  it('RLocalCachedMap preloadCache(count) bounds the near-cache fill', async () => {
    const r = make();
    await r.redis.hset('p38:lcm', '"a"', '"1"', '"b"', '"2"', '"c"', '"3"');
    const lcm = r.getLocalCachedMap<string, string>('p38:lcm', { maxSize: 10 });
    await lcm.preloadCache(2);
    expect(lcm.getCachedMap().size).toBe(2);
    await lcm.preloadCache();
    expect(lcm.getCachedMap().size).toBe(3);
    await r.shutdown();
  });
});

describe('phase 40: native map-cache batch conditionals (mock)', () => {
  it('putIfAllKeysExist/Absent and batch expiry fallback', async () => {
    const r = make();
    const mc = r.getMapCacheNative<string, string>('p40:mcn');
    await mc.put('a', '1');
    expect(await mc.putIfAllKeysExist({ b: '2' })).toBe(false);
    expect(await mc.get('b')).toBeNull();
    expect(await mc.putIfAllKeysAbsent({ b: '2', c: '3' })).toBe(true);
    expect(await mc.get('b')).toBe('2');
    expect(await mc.putIfAllKeysAbsent({ b: '9' })).toBe(false);
    expect(await mc.get('b')).toBe('2');
    expect(await mc.putIfAllKeysExist({ a: '9', b: '8' })).toBe(true);
    expect(await mc.get('a')).toBe('9');
    expect(await mc.putIfAllKeysExist({})).toBe(false);
    // mock has no HPEXPIRE family: batch expiry reports 0 updated fields
    expect(await mc.expireEntries(['a', 'b', 'ghost'], 5000)).toBe(0);
    await r.shutdown();
  });
});

describe('phase 41: queue/deque 4.6.1 surface (mock)', () => {
  it('RQueue poll(count)/indexOf/pollLastAndOfferFirstTo', async () => {
    const r = make();
    const q = r.getQueue<string>('p41:q');
    await q.add('a');
    await q.add('b');
    await q.add('c');
    expect(await q.indexOf('b')).toBe(1);
    expect(await q.indexOf('missing')).toBe(-1);
    expect(await q.pollLastAndOfferFirstTo('p41:dest')).toBe('c');
    expect(await r.getQueue<string>('p41:dest').readAll()).toEqual(['c']);
    expect(await q.poll(2)).toEqual(['a', 'b']);
    expect(await q.poll()).toBeNull();
    await r.shutdown();
  });

  it('RDeque variadic add, *IfExists, poll(count), move', async () => {
    const r = make();
    const d = r.getDeque<string>('p41:d');
    expect(await d.addFirst('a', 'b')).toBe(2); // [b, a]
    expect(await d.addLast('c', 'd')).toBe(4); // [b, a, c, d]
    expect(await d.pollLast(2)).toEqual(['d', 'c']);
    expect(await d.pollFirst(1)).toEqual(['b']);
    expect(await d.readAll()).toEqual(['a']);
    expect(await d.addFirstIfExists('x')).toBe(2);
    expect(await d.addLastIfExists('y')).toBe(3);
    expect(await d.readAll()).toEqual(['x', 'a', 'y']);
    const missing = r.getDeque<string>('p41:missing');
    expect(await missing.addFirstIfExists('z')).toBe(0);
    expect(await missing.addLastIfExists('z')).toBe(0);
    expect(await missing.size()).toBe(0);

    expect(await d.move('p41:d2', 'LEFT', 'RIGHT')).toBe('x');
    expect(await d.readAll()).toEqual(['a', 'y']);
    expect(await r.getDeque<string>('p41:d2').readAll()).toEqual(['x']);
    await r.shutdown();
  });
});

describe('phase 42: native map-cache fast writes (mock)', () => {
  it('fastPut / fastPutIfAbsent / putIfExist / fastPutIfExist', async () => {
    const r = make();
    const mn = r.getMapCacheNative<string, string>('p42:mn');
    expect(await mn.fastPut('k', 'v1')).toBe(true);
    expect(await mn.fastPut('k', 'v1b')).toBe(false);
    expect(await mn.get('k')).toBe('v1b');
    expect(await mn.fastPutIfAbsent('k', 'v2')).toBe(false);
    expect(await mn.get('k')).toBe('v1b');
    expect(await mn.fastPutIfAbsent('new', 'n1')).toBe(true);
    expect(await mn.get('new')).toBe('n1');

    expect(await mn.putIfExist('k', 'v3')).toBe('v1b');
    expect(await mn.get('k')).toBe('v3');
    expect(await mn.putIfExist('ghost', 'x')).toBeNull();
    expect(await mn.get('ghost')).toBeNull();
    expect(await mn.fastPutIfExist('k', 'v4')).toBe(true);
    expect(await mn.fastPutIfExist('ghost2', 'x')).toBe(false);
    expect(await mn.get('k')).toBe('v4');
    expect(await mn.get('ghost2')).toBeNull();
    await r.shutdown();
  });
});

describe('phase 45: blocking queue element subscriptions (mock)', () => {
  const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
  const waitFor = async (check: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<boolean> => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (await check()) return true;
      await sleep(20);
    }
    return check();
  };

  it('delivers in order with async back-pressure and stops on unsubscribe', async () => {
    const r = make();
    const q = r.getBlockingQueue<string>('p45:q');
    const got: string[] = [];
    const id = q.subscribeOnElements(async (v) => {
      await sleep(10);
      got.push(v);
    });
    await q.offer('a');
    await q.offer('b');
    expect(await waitFor(() => got.length === 2)).toBe(true);
    expect(got).toEqual(['a', 'b']);

    const consumer = () => undefined;
    const id2 = q.subscribeOnElements(consumer);
    expect(() => q.subscribeOnElements(consumer)).toThrow();
    q.unsubscribe(id2);
    q.unsubscribe(999999); // unknown ids are ignored
    q.unsubscribe(id);
    await sleep(150);
    await q.offer('c');
    await sleep(300);
    expect(got).not.toContain('c');
    expect(await q.poll()).toBe('c');
    await r.shutdown();
  });

  it('works for blocking deques and bounded queues (capacity restored)', async () => {
    const r = make();
    const d = r.getBlockingDeque<string>('p45:d');
    const dGot: string[] = [];
    const did = d.subscribeOnElements((v) => { dGot.push(v); });
    await d.addLast('x');
    await d.addLast('y');
    expect(await waitFor(() => dGot.length === 2)).toBe(true);
    expect(dGot).toEqual(['x', 'y']);
    d.unsubscribe(did);

    const bq = r.getBoundedBlockingQueue<string>('p45:bq');
    expect(await bq.trySetCapacity(1)).toBe(true);
    const bGot: string[] = [];
    const bid = bq.subscribeOnElements((v) => { bGot.push(v); });
    await bq.offer('z');
    expect(await waitFor(() => bGot.includes('z'))).toBe(true);
    expect(await waitFor(async () => (await bq.remainingCapacity()) === 1)).toBe(true);
    bq.unsubscribe(bid);
    await r.shutdown();
  });
});

describe('phase 51: native map-cache PutArgs + compute (mock)', () => {
  it('structured putAll/conditions and compute semantics', async () => {
    const r = make();
    const mn = r.getMapCacheNative<string, number>('p51:mn');
    await mn.putAll(new Map([['a', 1], ['b', 2]]));
    expect(await mn.get('a')).toBe(1);
    expect(await mn.get('b')).toBe(2);
    await mn.putAll({ entries: { c: 3, d: 4 }, timeToLiveMs: 5000 });
    expect(await mn.get('c')).toBe(3);
    expect(await mn.get('d')).toBe(4);

    expect(await mn.putIfAllKeysExist({ entries: { a: 10, x: 1 }, timeToLiveMs: 5000 })).toBe(false);
    expect(await mn.get('a')).toBe(1);
    expect(await mn.putIfAllKeysExist({ entries: { a: 10, b: 20 } })).toBe(true);
    expect(await mn.get('a')).toBe(10);
    expect(await mn.get('b')).toBe(20);
    expect(await mn.putIfAllKeysAbsent({ entries: { a: 9 } })).toBe(false);
    expect(await mn.putIfAllKeysAbsent({ entries: { e: 5, f: 6 }, keepTTL: true })).toBe(true);
    expect(await mn.get('e')).toBe(5);

    expect(await mn.compute('n', 5000, (_k, old) => (old ?? 0) + 1)).toBe(1);
    expect(await mn.compute('n', 5000, (_k, old) => (old ?? 0) + 1)).toBe(2);
    expect(await mn.compute('n', 5000, () => null)).toBeNull();
    expect(await mn.get('n')).toBeNull();

    let mapped = 0;
    const [v1, v2] = await Promise.all([
      mn.computeIfAbsent('m', 5000, () => { mapped += 1; return 7; }),
      mn.computeIfAbsent('m', 5000, () => { mapped += 1; return 8; }),
    ]);
    expect(mapped).toBe(1);
    expect(v1).toBe(7);
    expect(v2).toBe(7);
    expect(await mn.computeIfAbsent('m', 5000, () => 9)).toBe(7);
    expect(await mn.computeIfAbsent('empty', 5000, () => null)).toBeNull();
    expect(await mn.get('empty')).toBeNull();
    await r.shutdown();
  });
});

describe('phase 47: RKeys keyspace listeners (mock)', () => {
  it('registers/removes through different getKeys facades and rejects flush-only', async () => {
    const r = make();
    const id = await r.getKeys().addListener({ onSet: () => undefined, onDeleted: () => undefined });
    expect(id).toBeGreaterThan(0);
    await r.getKeys().removeListener(id);
    await r.getKeys().removeListener(id); // idempotent
    await r.getKeys().removeListener(999999);
    await expect(r.getKeys().addListener({ onFlush: () => undefined })).rejects.toThrow(/RESP3/);
    await expect(r.getKeys().addListener({} as any)).rejects.toThrow(/Unsupported/);
    await r.shutdown();
  });
});

describe('phase 48: queue last-from-any + limited key scans (mock)', () => {
  it('pollLastFromAnyWithName honours queue priority, source name and negative timeout', async () => {
    const r = make();
    const q = r.getBlockingQueue<string>('p48:q');
    const q2 = r.getBlockingQueue<string>('p48:q2');
    await q.add('a1');
    await q.add('a2');
    expect(await q.pollLastFromAnyWithName(1000, 'p48:q2')).toEqual({ key: 'p48:q', value: 'a2' });
    expect(await q.pollLastFromAnyWithName(-1, 'p48:q2')).toBeNull();
    expect(await q.pollLastFromAnyWithName(200)).toEqual({ key: 'p48:q', value: 'a1' });
    await q2.add('b1');
    await q2.add('b2');
    expect(await q.pollLastFromAnyWithName(1000, 'p48:q2')).toEqual({ key: 'p48:q2', value: 'b2' });
    expect(await q.pollLastFromAnyWithName(50, 'p48:q2')).toEqual({ key: 'p48:q2', value: 'b1' });
    expect(await q2.pollLastFromAnyWithName(100, 'p48:q')).toBeNull();
    await r.shutdown();
  });

  it('getKeysWithLimit and getKeysByPattern', async () => {
    const r = make();
    for (let i = 0; i < 5; i++) await r.getBucket(`p48:key:${i}`).set(i);
    expect((await r.getKeys().getKeysWithLimit('p48:key:*', 2))).toHaveLength(2);
    expect((await r.getKeys().getKeysWithLimit(1))).toHaveLength(1);
    expect(await r.getKeys().getKeysWithLimit(0)).toEqual([]);
    const all = await r.getKeys().getKeysByPattern('p48:key:*');
    expect(all.sort()).toEqual(['p48:key:0', 'p48:key:1', 'p48:key:2', 'p48:key:3', 'p48:key:4']);
    await r.shutdown();
  });
});

describe('phase 52: RSet countIntersection + listeners (mock)', () => {
  it('counts intersections with limit and registers removable listeners', async () => {
    const r = make();
    const s1 = r.getSet<string>('p52:s1');
    const s2 = r.getSet<string>('p52:s2');
    await s1.add('a', 'b', 'c');
    await s2.add('b', 'c', 'd');
    expect(await s1.countIntersection('p52:s2')).toBe(2);
    expect(await s1.countIntersection(1, 'p52:s2')).toBe(1);
    expect(await s1.countIntersection()).toBe(3);
    expect(await s1.countIntersection('p52:missing')).toBe(0);

    const id = await s1.addListener({
      onAdd: () => undefined,
      onRemove: () => undefined,
      onRandomRemove: () => undefined,
      onStore: () => undefined,
    });
    expect(id).toBeGreaterThan(0);
    await s1.removeListener(id);
    await s1.removeListener(id);
    await s1.removeListener(999999);
    await r.shutdown();
  });
});

describe('phase 53: atomic listener registration (mock)', () => {
  it('registers/removes listeners on RAtomicLong and RAtomicDouble', async () => {
    const r = make();
    const al = r.getAtomicLong('p53:al');
    const ad = r.getAtomicDouble('p53:ad');
    const lid = await al.addListener({ onChange: () => undefined });
    const did = await ad.addListener({ onChange: () => undefined });
    expect(lid).toBeGreaterThan(0);
    expect(did).toBeGreaterThan(0);
    await al.removeListener(lid);
    await ad.removeListener(did);
    await al.removeListener(lid);
    await ad.removeListener(999999);
    await r.shutdown();
  });
});

describe('phase 54: delayed queue 4.6.1 surface (mock)', () => {
  it('rejects unsupported add/single-arg offer/indexOf and supports remove()', async () => {
    const r = make();
    const dq = r.getDelayedQueue<string>('p54:dq');
    await expect(dq.offer('x')).rejects.toThrow(/offer/);
    await expect(dq.offer('x', -1)).rejects.toThrow(/negative/);
    await expect(dq.add('x')).rejects.toThrow(/offer/);
    await expect(dq.indexOf('x')).rejects.toThrow(/Unsupported/);
    await expect(dq.remove()).rejects.toThrow(/NoSuchElement/);
    await dq.offer('y', 1000);
    expect(await dq.remove()).toBe('y');
    expect(await dq.remove('missing')).toBe(false);
    await r.shutdown();
  });
});

describe('phase 56: RMap putAll batchSize (mock)', () => {
  it('writes field batches and validates batchSize', async () => {
    const r = make();
    const m = r.getMap<string, string>('p56:m');
    await m.putAll({ a: '1', b: '2', c: '3', d: '4', e: '5' }, 2);
    expect(await m.size()).toBe(5);
    expect(await m.get('c')).toBe('3');
    await m.putAll(new Map([['f', '6']]), 1);
    expect(await m.get('f')).toBe('6');
    await expect(m.putAll({ x: '1' }, 0)).rejects.toThrow(/batchSize/);
    await expect(m.putAll({ x: '1' }, 1.5)).rejects.toThrow(/batchSize/);
    await m.putAll(new Map(), 2);
    expect(await m.size()).toBe(6);
    await r.shutdown();
  });
});

describe('phase 58: native map-cache batch TTL reads (mock)', () => {
  it('returns keyed maps for remainTimeToLive/clearExpire batches', async () => {
    const r = make();
    const mn = r.getMapCacheNative<string, string>('p58:mn');
    await mn.put('a', '1', 5000);
    await mn.put('b', '2');
    const ttls = await mn.remainTimeToLive(['a', 'b', 'missing']);
    expect([...ttls.keys()]).toEqual(['a', 'b', 'missing']);
    expect(ttls.has('a')).toBe(true);
    const cleared = await mn.clearExpire(['a', 'b', 'missing']);
    expect([...cleared.keys()]).toEqual(['a', 'b', 'missing']);
    expect(cleared.get('a')).toBe(false);
    expect(await mn.remainTimeToLive([])).toEqual(new Map());
    expect(await mn.clearExpire([])).toEqual(new Map());
    await r.shutdown();
  });
});

describe('phase 59: map field listeners (mock)', () => {
  it('registers/removes listeners on RMap and RMapCacheNative', async () => {
    const r = make();
    const m = r.getMap<string, string>('p59:m');
    const mn = r.getMapCacheNative<string, string>('p59:mn');
    const mid = await m.addListener({ onPut: () => undefined, onIncrement: () => undefined, onRemove: () => undefined });
    const nid = await mn.addListener({
      onPut: () => undefined,
      onRemove: () => undefined,
      onExpired: () => undefined,
      onClearExpire: () => undefined,
    });
    expect(mid).toBeGreaterThan(0);
    expect(nid).toBeGreaterThan(0);
    await m.removeListener(mid);
    await mn.removeListener(nid);
    await m.removeListener(mid);
    await mn.removeListener(999999);
    await r.shutdown();
  });
});

describe('phase 60: conditional compareAndDelete (mock)', () => {
  it('supports all comparison operators without deleting on mismatch', async () => {
    const r = make();
    const al = r.getAtomicLong('p60:al');
    const ad = r.getAtomicDouble('p60:ad');
    await al.set(5);
    expect(await al.compareAndDelete(CompareAndDeleteArgs.greater(4))).toBe(true);
    expect(await al.get()).toBe(0);

    await al.set(5);
    expect(await al.compareAndDelete(CompareAndDeleteArgs.lessOrEqual(4))).toBe(false);
    expect(await al.get()).toBe(5);
    expect(await al.compareAndDelete(CompareAndDeleteArgs.equal(5))).toBe(true);
    expect(await al.get()).toBe(0);
    expect(await al.compareAndDelete(CompareAndDeleteArgs.greater(0))).toBe(false);
    expect(await al.compareAndDelete(7)).toBe(false);

    await ad.set(1.5);
    expect(await ad.compareAndDelete(CompareAndDeleteArgs.notEqual(2))).toBe(true);
    expect(await ad.get()).toBe(0);
    await ad.set(1.5);
    expect(await ad.compareAndDelete(CompareAndDeleteArgs.lessOrEqual(1.4))).toBe(false);
    expect(await ad.get()).toBe(1.5);
    expect(await ad.compareAndDelete(1.5)).toBe(true);
    await r.shutdown();
  });
});

describe('phase 61: IncrementArgs emulation (mock)', () => {
  it('respects bounds/saturation, BigInt and concurrent CAS retries', async () => {
    const r = make();
    const al = r.getAtomicLong('p61:l');
    const ad = r.getAtomicDouble('p61:d');
    await al.set(8);
    expect(await al.incrementAndGet({ by: 5, lowerBound: 0, upperBound: 10, saturate: true })).toBe(10);
    expect(await al.incrementAndGet({ by: 5, upperBound: 10, saturate: true })).toBe(10);
    await al.set(9);
    expect(await al.incrementAndGet({ by: 5, upperBound: 10 })).toBe(9);
    expect(await al.get()).toBe(9);

    await al.set(9007199254740993n);
    expect(await al.incrementAndGet({ by: 1 })).toBe(9007199254740994n);

    await al.set(0);
    await Promise.all(Array.from({ length: 10 }, () => al.incrementAndGet({ by: 2 })));
    expect(await al.get()).toBe(20);

    await ad.set(1.5);
    expect(await ad.incrementAndGet({ by: 0.25, lowerBound: 0, upperBound: 1.6, saturate: true })).toBe(1.6);
    await ad.set(1.5);
    expect(await ad.incrementAndGet({ by: 1, upperBound: 2 })).toBe(1.5);
    expect(await ad.incrementAndGet({ by: 0.5, lowerBound: 0, upperBound: 2 })).toBe(2);
    await r.shutdown();
  });
});

describe('phase 62: SetCache listeners + newCondition (mock)', () => {
  it('registers SetCache listeners and rejects newCondition everywhere', async () => {
    const r = make();
    const sc = r.getSetCache<string>('p62:sc');
    const id = await sc.addListener({ onAdd: () => undefined, onRemove: () => undefined });
    expect(id).toBeGreaterThan(0);
    await sc.removeListener(id);
    await sc.removeListener(id);
    await sc.removeListener(999999);

    const lock = r.getLock('p62:l');
    const fair = r.getFairLock('p62:f');
    const rw = r.getReadWriteLock('p62:rw');
    const multi = r.getMultiLock(lock);
    const red = r.getRedLock(lock);
    expect(() => lock.newCondition()).toThrow(/Unsupported/);
    expect(() => fair.newCondition()).toThrow(/Unsupported/);
    expect(() => rw.readLock.newCondition()).toThrow(/Unsupported/);
    expect(() => rw.writeLock.newCondition()).toThrow(/Unsupported/);
    expect(() => multi.newCondition()).toThrow(/Unsupported/);
    expect(() => red.newCondition()).toThrow(/Unsupported/);
    await r.shutdown();
  });
});

describe('phase 63: local cached map reconnection options (mock)', () => {
  it('accepts NONE/CLEAR/LOAD and keeps normal get/put behavior', async () => {
    const r = make();
    for (const strategy of ['NONE', 'CLEAR', 'LOAD'] as const) {
      const m = r.getLocalCachedMap<string, string>(`p63:${strategy}`, { reconnectionStrategy: strategy });
      await m.put('a', '1');
      expect(await m.get('a')).toBe('1');
      expect(m.getCachedMap().get('a')).toBe('1');
      await m.clearLocalCache();
      expect(m.getCachedMap().size).toBe(0);
    }
    await r.shutdown();
  });
});

describe('phase 64: local cached map expiration policy (mock)', () => {
  it('accepts both expiration policies without affecting local reads', async () => {
    const r = make();
    const d = r.getLocalCachedMap<string, string>('p64:d');
    const none = r.getLocalCachedMap<string, string>('p64:n', { expirationEventPolicy: 'DONT_SUBSCRIBE' });
    await d.put('a', '1');
    await none.put('a', '2');
    expect(await d.get('a')).toBe('1');
    expect(await none.get('a')).toBe('2');
    expect(d.getCachedMap().get('a')).toBe('1');
    expect(none.getCachedMap().get('a')).toBe('2');
    await r.shutdown();
  });
});

describe('phase 65: lock keyspace listeners (mock)', () => {
  it('registers/removes expired/deleted listeners on lock types', async () => {
    const r = make();
    const lock = r.getLock('p65:l');
    const fair = r.getFairLock('p65:f');
    const rw = r.getReadWriteLock('p65:rw');
    const ids = [
      await lock.addListener({ onExpired: () => undefined, onDeleted: () => undefined }),
      await fair.addListener({ onExpired: () => undefined, onDeleted: () => undefined }),
      await rw.readLock.addListener({ onExpired: () => undefined }),
      await rw.writeLock.addListener({ onDeleted: () => undefined }),
    ];
    expect(ids.every((id) => id > 0)).toBe(true);
    await lock.removeListener(ids[0]);
    await fair.removeListener(ids[1]);
    await rw.readLock.removeListener(ids[2]);
    await rw.writeLock.removeListener(ids[3]);
    await lock.removeListener(ids[0]);
    await r.shutdown();
  });
});
