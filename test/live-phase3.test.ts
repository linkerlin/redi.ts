/**
 * Phase 3 live interop: RWLock, FencedLock token, DelayedQueue, ReliableTopic,
 * TimeSeries, LongAdder, PermitExpirableSemaphore. Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe';

const NS = 'TsJavaT:p3:';
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


async function cleanKeys(r: Redisson, ...names: string[]): Promise<void> {
  if (names.length > 0) await r.getKeys().delete(...names);
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 3: rwlock/fenced/delayed/topic/ts/adder/pes', () => {
  it('rw: TS-held write blocks Java read+write', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw:a`;
    try {
      await cleanKeys(r, name);
      const rw = r.getReadWriteLock(name);
      await rw.getWriteLock().lock(30000, 1);
      expect(await probe(`rw-read-try ${name}`)).toBe('CONTENDED');
      expect(await probe(`rw-write-try ${name}`)).toBe('CONTENDED');
      await rw.getWriteLock().unlock(1);
      expect(await probe(`rw-read-try ${name}`)).toBe('ACQUIRED');
      expect(await probe(`rw-write-try ${name}`)).toBe('ACQUIRED');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('rw: Java-held write blocks TS; Java-held read shares with TS read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw:b`;
    try {
      await cleanKeys(r, name);
      const holdW = probeBg(`rw-write-hold ${name} 4000`);
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = ((await r.redis.exists(name)) as number) > 0;
      }
      expect(held).toBe(true);
      const rw = r.getReadWriteLock(name);
      expect(await rw.getReadLock().tryLock(0, 5000, 21)).toBe(false);
      expect(await rw.getWriteLock().tryLock(0, 5000, 22)).toBe(false);
      await holdW;
      expect(await rw.getReadLock().tryLock(0, 5000, 21)).toBe(true);
      await rw.getReadLock().unlock(21);
      expect(await rw.getWriteLock().tryLock(0, 5000, 22)).toBe(true);
      await rw.getWriteLock().unlock(22);

      // shared read: Java holds read, TS read must also acquire, TS write must not
      const holdR = probeBg(`rw-read-hold ${name} 4000`);
      held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = ((await r.redis.exists(name)) as number) > 0;
      }
      expect(held).toBe(true);
      expect(await rw.getWriteLock().tryLock(0, 5000, 24)).toBe(false);
      expect(await rw.getReadLock().tryLock(0, 5000, 23)).toBe(true);
      await rw.getReadLock().unlock(23);
      await holdR;
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('fenced: token counter is shared and monotonic', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fenced`;
    try {
      await cleanKeys(r, name, `redisson_lock_token:{${name}}`);
      const fl = r.getFencedLock(name);
      const t1 = await fl.lockAndGetToken(30000, 1);
      expect(t1).not.toBeNull();
      await fl.unlock(1);
      const line = await probe(`fenced-token ${name}`);
      expect(line.startsWith('TOKEN:')).toBe(true);
      const t2 = Number(line.slice('TOKEN:'.length));
      expect(t2).toBe((t1 as number) + 1);
      expect(await fl.getToken()).toBe(t2);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('delayed queue migrates both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      // Java → TS
      const destJT = `${NS}dq:jt`;
      await cleanKeys(r, destJT, `redisson_delay_queue_timeout:{${destJT}}`, `redisson_delay_queue:{${destJT}}`);
      expect(await probe(`dq-offer ${destJT} hello-java 1500`)).toBe('OFFERED');
      const dqJT = r.getDelayedQueue<string>(destJT);
      let got: string | null = null;
      for (let i = 0; i < 100 && got === null; i++) {
        got = await dqJT.poll();
        if (got === null) await new Promise((x) => setTimeout(x, 100));
      }
      expect(got).toBe('hello-java');

      // TS → Java
      const destTJ = `${NS}dq:tj`;
      await cleanKeys(r, destTJ, `redisson_delay_queue_timeout:{${destTJ}}`, `redisson_delay_queue:{${destTJ}}`);
      await r.getDelayedQueue<string>(destTJ).offer('hello-ts', 1500);
      expect(await probe(`dq-poll ${destTJ} 12000`)).toBe('GOT:hello-ts');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('reliable topic delivers both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      // TS → Java (result mirrored to a key; one retry on transient JVM loss)
      const tj = `${NS}rt:tj`;
      const tjRes = `${tj}:listen-result`;
      await cleanKeys(r, tjRes);
      let tjVerdict = '';
      for (let attempt = 0; attempt < 2 && tjVerdict !== 'GOT:msg-ts-java'; attempt++) {
        const listenP = probeBg(`rt-listen ${tj} 15000 ${tjRes}`).catch(() => []);
        await new Promise((x) => setTimeout(x, 2500));
        await r.getReliableTopic<string>(tj).publish('msg-ts-java');
        const lines = await listenP;
        const fromKey = await r.getBucket<string>(tjRes).get();
        tjVerdict =
          [...lines].reverse().find((l) => l.startsWith('GOT:') || l === 'TIMEOUT') ?? '';
        if (tjVerdict !== 'GOT:msg-ts-java' && fromKey) tjVerdict = fromKey;
        if (tjVerdict !== 'GOT:msg-ts-java') await cleanKeys(r, tjRes);
      }
      expect(tjVerdict).toBe('GOT:msg-ts-java');

      // Java → TS
      const jt = `${NS}rt:jt`;
      const rt = r.getReliableTopic<string>(jt);
      const got: string[] = [];
      const lid = await rt.addListener((msg) => {
        got.push(String(msg));
      });
      await new Promise((x) => setTimeout(x, 2500));
      expect(await probe(`rt-publish ${jt} msg-java-ts`)).toBe('PUBLISHED');
      for (let i = 0; i < 120 && got.length === 0; i++) {
        await new Promise((x) => setTimeout(x, 100));
      }
      await rt.removeListener(lid);
      expect(got).toContain('msg-java-ts');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('timeseries entries are shared with exact timestamps', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts`;
    try {
      await cleanKeys(r, name, `redisson__ts_ttl:{${name}}`, `redisson__ts_seq:{${name}}`);
      const ts = Date.now();
      expect(await probe(`ts-add ${name} ${ts} jval`)).toBe('TS_ADDED');
      const back = await r.getTimeSeries<string>(name).range(ts - 5000, ts + 5000);
      expect(back.some((e) => e.value === 'jval' && e.timestamp === ts)).toBe(true);

      const ts2 = ts + 5000;
      await r.getTimeSeries<string>(name).add(ts2, 'tsval');
      const line = await probe(`ts-range ${name} 0 9999999999999`);
      expect(line).toContain('tsval');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('longadder sums flush across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}adder`;
    const signal = `${name}:ts-done`;
    const readyKey = `${signal}:ready`;
    const resultKey = `${signal}:result`;
    const ad = r.getLongAdder(name);
    try {
      const pats = await r.getKeys().getKeys(`{${name}}:*`, 100);
      await cleanKeys(r, name, signal, readyKey, resultKey, ...pats);
      // cohort JVM stays alive (local 100 + listener) across both sums
      const cohortP = probeBg(`longadder-cohort ${name} 100 ${signal}`).catch((e) => [String(e)]);
      // wait for READY via key (immune to stdout timing/shell quoting)
      let ready = false;
      for (let i = 0; i < 450 && !ready; i++) {
        await new Promise((x) => setTimeout(x, 200));
        ready = ((await r.redis.exists(readyKey)) as number) > 0;
      }
      expect(ready).toBe(true);
      await ad.add(23);
      // both listeners live: TS flush 23 + Java flush 100 → 123
      expect(await ad.sum()).toBe(123);
      await r.redis.set(signal, '1');
      const out = (await cohortP).join('\n');
      const res = await r.redis.get(resultKey);
      expect(`${res ?? ''}\n${out}`).toContain('ADDER_SUM:123');
    } finally {
      await ad.destroy();
      await r.shutdown();
    }
  }, 240000);

  it('permit-expirable-semaphore pool is shared', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes`;
    try {
      await cleanKeys(r, name, `{${name}}:timeout`);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(1)).toBe(true);
      const ids = await pes.tryAcquire(30000);
      expect(ids).not.toBeNull();
      expect(await probe(`pes-available ${name}`)).toBe('PES_AVAILABLE:0');
      expect(await pes.release(ids![0])).toBe(true);
      expect(await probe(`pes-available ${name}`)).toBe('PES_AVAILABLE:1');
    } finally {
      await r.shutdown();
    }
  }, 180000);
});
