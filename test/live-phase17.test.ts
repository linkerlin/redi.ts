/**
 * Phase 17 live interop: bounded-queue semaphore protocol, dedicated
 * connections for blocking commands (watchdog anti-starvation), and topic
 * subscription lifecycle.
 *
 * Needs real Redis + Java probe; skips silently when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName, sleep } from '../src/util';

const NS = 'TsJavaT:p17:';
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

async function waitUntil(fn: () => boolean | Promise<boolean>, timeoutMs = 10000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return true;
    await sleep(50);
  }
  return false;
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 17: bounded semaphore / blocking isolation / topics', () => {
  it('bounded queue permits are shared with Java (offer/poll/take)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bq:permits`;
    const sem = prefixName('redisson_bqs', name);
    try {
      await cleanExact(r, name, sem, `${name}:take`);
      const q = r.getBoundedBlockingQueue<string>(name);
      expect(await q.trySetCapacity(2)).toBe(true);
      expect(await q.offer('a')).toBe(true);
      expect(await probe(`bq-remaining ${name}`)).toBe('BQ_REMAIN:1');
      expect(await probe(`bq-offer ${name} b`)).toBe('BQ_OFFERED');
      expect(await probe(`bq-remaining ${name}`)).toBe('BQ_REMAIN:0');
      expect(await q.offer('c')).toBe(false);
      expect(await q.poll()).toBe('a');
      expect(await q.remainingCapacity()).toBe(1);
      expect(await probe(`bbq-take ${name} 8000 ${name}:take`)).toMatch(/GOT:b/);
      expect(await q.remainingCapacity()).toBe(2);
    } finally {
      await cleanExact(r, name, sem, `${name}:take`);
      await r.shutdown();
    }
  }, 180000);

  it('TS put wakes when a Java consumer frees capacity', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bq:java-take`;
    const sem = prefixName('redisson_bqs', name);
    try {
      await cleanExact(r, name, sem, `${name}:take`);
      const q = r.getBoundedBlockingQueue<string>(name);
      expect(await q.trySetCapacity(1)).toBe(true);
      expect(await q.offer('a')).toBe(true);
      const takeP = probe(`bbq-take ${name} 30000 ${name}:take`, 90000);
      const putP = q.put('b');
      const outcome = await Promise.race([
        putP.then(() => 'put'),
        sleep(45000).then(() => 'timeout'),
      ]);
      expect(outcome).toBe('put');
      expect(await takeP).toMatch(/GOT:a/);
      expect(await q.poll()).toBe('b');
    } finally {
      await cleanExact(r, name, sem, `${name}:take`);
      await r.shutdown();
    }
  }, 180000);

  it('Java put blocks until a TS poll releases capacity', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bq:java-put`;
    const sem = prefixName('redisson_bqs', name);
    const resultKey = `${name}:put-result`;
    try {
      await cleanExact(r, name, sem, resultKey);
      const q = r.getBoundedBlockingQueue<string>(name);
      expect(await q.trySetCapacity(1)).toBe(true);
      expect(await q.offer('a')).toBe(true);
      const putP = probe(`bbq-put ${name} b ${resultKey}`, 90000);
      const bucket = r.getBucket<string>(resultKey);
      expect(await waitUntil(async () => (await bucket.get()) === 'BQ_PUT_STARTED', 30000)).toBe(true);
      await sleep(300);
      expect(await q.poll()).toBe('a');
      expect(await putP).toContain('BQ_PUT_OK');
      expect(await waitUntil(async () => (await bucket.get()) === 'BQ_PUT_OK', 10000)).toBe(true);
      expect(await q.poll()).toBe('b');
    } finally {
      await cleanExact(r, name, sem, resultKey);
      await r.shutdown();
    }
  }, 180000);

  it('blocking take does not starve the main connection', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}block:queue`;
    const sentinel = `${NS}block:sentinel`;
    try {
      await cleanExact(r, name, sentinel);
      const q = r.getBlockingQueue<string>(name);
      const takeP = q.take();
      await sleep(150);
      const t0 = Date.now();
      await Promise.race([
        r.getBucket<string>(sentinel).set('ok'),
        sleep(3000).then(() => { throw new Error('main connection blocked by BLPOP'); }),
      ]);
      expect(Date.now() - t0).toBeLessThan(2000);
      await q.offer('x');
      await expect(takeP).resolves.toBe('x');
    } finally {
      await cleanExact(r, name, sentinel);
      await r.shutdown();
    }
  }, 60000);

  it('priority blocking take does not starve the main connection', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}block:priority`;
    const sentinel = `${NS}block:priority-sentinel`;
    try {
      await cleanExact(r, name, sentinel);
      const q = r.getPriorityBlockingQueue<string>(name);
      const takeP = q.take();
      await sleep(150);
      await Promise.race([
        r.getBucket<string>(sentinel).set('ok'),
        sleep(3000).then(() => { throw new Error('main connection blocked by BZPOPMIN'); }),
      ]);
      await q.offer('x', 1);
      await expect(takeP).resolves.toBe('x');
    } finally {
      await cleanExact(r, name, sentinel);
      await r.shutdown();
    }
  }, 60000);

  it('blocking stream readGroup does not starve the main connection', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}block:stream`;
    const sentinel = `${NS}block:stream-sentinel`;
    try {
      await cleanExact(r, name, sentinel);
      const s = r.getStream<string>(name);
      await s.add({ init: 'x' });
      await s.createGroup('g17', '$');
      const readP = s.readGroupWith('g17', 'c17', { count: 1, blockMs: 2000 });
      await sleep(150);
      await Promise.race([
        r.getBucket<string>(sentinel).set('ok'),
        sleep(3000).then(() => { throw new Error('main connection blocked by XREADGROUP'); }),
      ]);
      await s.add({ v: 'm17' });
      const entries = await readP;
      expect(entries.length).toBe(1);
      expect(entries[0].fields.v).toBe('m17');
    } finally {
      await cleanExact(r, name, sentinel);
      await r.shutdown();
    }
  }, 60000);

  it('plain/pattern topic removeListener stops delivery', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const topicName = `${NS}topic:plain`;
    const patternName = `${NS}topic:*`;
    try {
      const t = r.getTopic<number>(topicName);
      const a: number[] = [];
      const b: number[] = [];
      const id1 = await t.addListener((v) => a.push(v));
      const id2 = await t.addListener((v) => b.push(v));
      await t.publish(1);
      expect(await waitUntil(() => a.includes(1) && b.includes(1), 5000)).toBe(true);
      await t.removeListener(id1);
      await t.publish(2);
      expect(await waitUntil(() => b.includes(2), 5000)).toBe(true);
      await sleep(200);
      expect(a).not.toContain(2);
      await t.removeAllListeners();
      await t.publish(3);
      await sleep(300);
      expect(b).not.toContain(3);
      void id2;

      const pt = r.getPatternTopic<number>(patternName);
      const got: string[] = [];
      await pt.addListener((ch, v) => got.push(`${ch}:${v}`));
      await r.getTopic<number>(`${NS}topic:pattern`).publish(10);
      expect(await waitUntil(() => got.some((x) => x.endsWith(':10')), 5000)).toBe(true);
      await pt.removeAllListeners();
      await r.getTopic<number>(`${NS}topic:pattern`).publish(11);
      await sleep(300);
      expect(got.some((x) => x.endsWith(':11'))).toBe(false);
    } finally {
      await r.shutdown();
    }
  }, 60000);

  it('sharded topic removeListener/removeAll stops delivery', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}topic:sharded`;
    try {
      const t = r.getShardedTopic<number>(name);
      const a: number[] = [];
      const b: number[] = [];
      const id1 = await t.addListener((v) => a.push(v));
      await t.addListener((v) => b.push(v));
      await t.publish(1);
      expect(await waitUntil(() => a.length > 0 && b.length > 0, 5000)).toBe(true);
      await t.removeListener(id1);
      await t.publish(2);
      expect(await waitUntil(() => b.includes(2), 5000)).toBe(true);
      await sleep(200);
      expect(a).not.toContain(2);
      await t.removeAllListeners();
      await t.publish(3);
      await sleep(300);
      expect(b).not.toContain(3);
    } finally {
      await r.shutdown();
    }
  }, 60000);

  it('reliable topic consume loop uses a dedicated connection', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}topic:reliable`;
    const timeout = `${name}:timeout`;
    const sentinel = `${NS}topic:reliable-sentinel`;
    try {
      await cleanExact(r, name, timeout, sentinel);
      const rt = r.getReliableTopic<string>(name);
      const got: string[] = [];
      const id = await rt.addListener((v) => { got.push(v); });
      await sleep(400);
      const t0 = Date.now();
      await Promise.race([
        r.getBucket<string>(sentinel).set('ok'),
        sleep(3000).then(() => { throw new Error('main connection blocked by XREADGROUP'); }),
      ]);
      expect(Date.now() - t0).toBeLessThan(2000);
      await rt.publish('m17');
      expect(await waitUntil(() => got.includes('m17'), 10000)).toBe(true);
      await rt.removeListener(id);
      await rt.delete();
    } finally {
      await cleanExact(r, name, timeout, sentinel);
      await r.shutdown();
    }
  }, 120000);
});
