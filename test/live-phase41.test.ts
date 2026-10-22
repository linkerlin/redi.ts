/**
 * Phase 41 live: RQueue/RDeque 4.6.1 surface — poll(count), indexOf,
 * RPOPLPUSH, variadic LPUSH/RPUSH, LPUSHX/RPUSHX and LMOVE.
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p41:';
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

describe('live interop phase 41: queue/deque surface', () => {
  it('poll counts, indexOf, RPOPLPUSH, variadic writes and LMOVE', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const qn = `${NS}q`;
    const dn = `${NS}d`;
    const d2n = `${NS}d2`;
    try {
      await r.getKeys().delete(qn, dn, d2n);
      const q = r.getQueue<string>(qn);
      await q.add('a');
      await q.add('b');
      await q.add('c');
      expect(await q.indexOf('b')).toBe(1);
      expect(await q.pollLastAndOfferFirstTo(d2n)).toBe('c');
      expect(await r.getDeque<string>(d2n).readAll()).toEqual(['c']);
      expect(await q.poll(2)).toEqual(['a', 'b']);

      const d = r.getDeque<string>(dn);
      expect(await d.addFirst('a', 'b')).toBe(2); // [b, a]
      expect(await d.addLast('c', 'd')).toBe(4); // [b, a, c, d]
      expect(await d.pollLast(2)).toEqual(['d', 'c']);
      expect(await d.pollFirst(1)).toEqual(['b']);
      expect(await d.readAll()).toEqual(['a']);
      expect(await d.addFirstIfExists('x')).toBe(2);
      expect(await d.addLastIfExists('y')).toBe(3);
      expect(await d.readAll()).toEqual(['x', 'a', 'y']);
      const missing = r.getDeque<string>(`${NS}missing`);
      expect(await missing.addFirstIfExists('z')).toBe(0);
      expect(await missing.addLastIfExists('z')).toBe(0);
      expect(await missing.size()).toBe(0);

      await r.getKeys().delete(d2n);
      expect(await d.move(d2n, 'LEFT', 'RIGHT')).toBe('x');
      expect(await d.readAll()).toEqual(['a', 'y']);
      expect(await r.getDeque<string>(d2n).readAll()).toEqual(['x']);
    } finally {
      await r.getKeys().delete(qn, dn, d2n, `${NS}missing`);
      await r.shutdown();
    }
  }, 120000);
});
