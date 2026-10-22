/**
 * Phase 45 live: RBlockingQueue/RBlockingDeque/RBoundedBlockingQueue
 * subscribeOnElements/unsubscribe on real Redis — delivery order, async
 * back-pressure, prompt BLPOP cancellation and bounded-queue capacity release.
 *
 * Needs real Redis; skips when unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p45:';
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

const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
const waitFor = async (check: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<boolean> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(20);
  }
  return check();
};

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live phase 45: blocking queue element subscriptions', () => {
  it('delivers in order, cancels BLPOP promptly and restores bounded capacity', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const qn = `${NS}q`;
    const dn = `${NS}d`;
    const bn = `${NS}bq`;
    const bqKey = `redisson_bqs:{${bn}}`;
    try {
      await r.getKeys().delete(qn, dn, bn, bqKey);
      const q = r.getBlockingQueue<string>(qn);
      const got: string[] = [];
      const id = q.subscribeOnElements(async (v) => {
        await sleep(5);
        got.push(v);
      });
      await q.offer('a');
      await q.offer('b');
      await q.offer('c');
      expect(await waitFor(() => got.length === 3)).toBe(true);
      expect(got).toEqual(['a', 'b', 'c']);

      const consumer = () => undefined;
      const id2 = q.subscribeOnElements(consumer);
      expect(() => q.subscribeOnElements(consumer)).toThrow();
      q.unsubscribe(id2);
      q.unsubscribe(id);

      // The pending BLPOP connection is aborted; a later element stays queued.
      await sleep(150);
      await q.offer('after');
      await sleep(300);
      expect(got).not.toContain('after');
      expect(await q.size()).toBe(1);
      expect(await q.poll()).toBe('after');

      const d = r.getBlockingDeque<string>(dn);
      const dGot: string[] = [];
      const did = d.subscribeOnElements((v) => { dGot.push(v); });
      await d.addLast('right');
      expect(await waitFor(() => dGot.length === 1)).toBe(true);
      expect(dGot).toEqual(['right']);
      d.unsubscribe(did);

      const bq = r.getBoundedBlockingQueue<string>(bn);
      expect(await bq.trySetCapacity(1)).toBe(true);
      const bGot: string[] = [];
      const bid = bq.subscribeOnElements((v) => { bGot.push(v); });
      await bq.offer('x');
      expect(await waitFor(() => bGot.includes('x'))).toBe(true);
      expect(await waitFor(async () => (await bq.remainingCapacity()) === 1)).toBe(true);
      bq.unsubscribe(bid);
    } finally {
      await r.getKeys().delete(qn, dn, bn, bqKey);
      await r.shutdown();
    }
  }, 120000);
});
