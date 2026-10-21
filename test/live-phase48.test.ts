/**
 * Phase 48 live: RBlockingQueue/Deque pollLastFromAnyWithName (BRPOP with
 * source name) and RKeys getKeysWithLimit/getKeysByPattern.
 *
 * Needs real Redis; skips when unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p48:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    r.on('error', () => undefined);
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();

describe('live phase 48: last-from-any + limited scans', () => {
  it('BRPOP returns the source queue and key scans honour limits', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const qn = `${NS}q`;
    const q2n = `${NS}q2`;
    try {
      await r.getKeys().delete(qn, q2n, `${NS}key:0`, `${NS}key:1`, `${NS}key:2`);
      const q = r.getBlockingQueue<string>(qn);
      const q2 = r.getBlockingQueue<string>(q2n);
      await q.add('a1');
      await q.add('a2');
      await q2.add('b1');

      expect(await q.pollLastFromAnyWithName(1000, q2n)).toEqual({ key: qn, value: 'a2' });
      expect(await q.pollLastFromAnyWithName(-1, q2n)).toBeNull();
      expect(await q.pollLastFromAnyWithName(1000, q2n)).toEqual({ key: qn, value: 'a1' });
      expect(await q.pollLastFromAnyWithName(1000, q2n)).toEqual({ key: q2n, value: 'b1' });
      expect(await q.pollLastFromAnyWithName(150, q2n)).toBeNull();

      const d = r.getBlockingDeque<string>(`${NS}d`);
      await r.getKeys().delete(`${NS}d`);
      await d.addLast('d1');
      await d.addLast('d2');
      expect(await d.pollLastFromAnyWithName(1000, qn)).toEqual({ key: `${NS}d`, value: 'd2' });

      for (let i = 0; i < 4; i++) await r.getBucket(`${NS}key:${i}`).set(i);
      expect(await r.getKeys().getKeysWithLimit(`${NS}key:*`, 2)).toHaveLength(2);
      expect(await r.getKeys().getKeysWithLimit(1)).toHaveLength(1);
      expect(await r.getKeys().getKeysWithLimit(0)).toEqual([]);
      const all = await r.getKeys().getKeysByPattern(`${NS}key:*`);
      expect(all.sort()).toEqual([`${NS}key:0`, `${NS}key:1`, `${NS}key:2`, `${NS}key:3`]);
    } finally {
      const keys = [qn, q2n, `${NS}d`, 0, 1, 2, 3].map((k) => (typeof k === 'number' ? `${NS}key:${k}` : k));
      await r.getKeys().delete(...keys);
      await r.shutdown();
    }
  }, 120000);
});
