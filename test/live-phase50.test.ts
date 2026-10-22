/**
 * Phase 50 live: RVectorSet deep API — VectorAddArgs options, raw vectors,
 * VLINKS WITHSCORES and VectorSimilarArgs (names/scores/attributes).
 *
 * Needs Redis 8 VectorSet; capability-gated on VADD.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p50:';
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

async function hasCmd(redis: Redis, cmd: string): Promise<boolean> {
  try {
    const info: any = await (redis as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(info) && info.length > 0 && info[0] !== null && info[0] !== undefined;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
const waitFor = async (check: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<boolean> => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(20);
  }
  return check();
};

describe('live phase 50: RVectorSet deep API', () => {
  it('supports add args, raw vectors, scored neighbors and similar queries', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vs`;
    const names = [name, `${NS}m`, `${NS}q8`, `${NS}reduce`];
    try {
      if (!(await hasCmd(r.redis, 'VADD'))) {
        console.log('[phase50] VectorSet unsupported — skipping');
        return;
      }
      await r.getKeys().delete(...names);
      const vs = r.getVectorSet(name);
      expect(await vs.add('a', [1, 0])).toBe(true);
      expect(await vs.add({ element: 'b', vector: [0.9, 0.1], attributes: { tag: 'y' } })).toBe(true);
      expect(await vs.add({ element: 'c', vector: [0, 1], attributes: { tag: 'z' }, explorationFactor: 100 })).toBe(true);
      // CAS is a server-side update hint; the reply may be 1 (added) or 0 (updated).
      const cas = await vs.add({ element: 'a', vector: [0.8, 0.2], cas: true, attributes: { tag: 'x' } });
      expect(typeof cas).toBe('boolean');
      expect(await vs.size()).toBe(3);
      expect(await vs.getAttributes('b')).toEqual({ tag: 'y' });
      expect(await vs.setAttributes('b', { tag: 'y2' })).toBe(true);
      expect(await vs.getAttributes('b')).toEqual({ tag: 'y2' });

      const raw = await vs.getRawVector('a');
      expect(Array.isArray(raw)).toBe(true);
      expect(raw.some((x) => Buffer.isBuffer(x))).toBe(true);
      await waitFor(async () => (await vs.getNeighbors('a')).length > 0);
      const neighbors = await vs.getNeighbors('a');
      expect(neighbors.length).toBeGreaterThan(0);
      expect(neighbors.every((x) => typeof x === 'string')).toBe(true);
      await waitFor(async () => (await vs.getNeighborEntries('a')).length > 0);
      const neighborEntries = await vs.getNeighborEntries('a');
      expect(neighborEntries.length).toBeGreaterThan(0);
      expect(typeof neighborEntries[0].score).toBe('number');

      const byElement = await vs.getSimilar({ element: 'a', count: 2 });
      expect(byElement).toContain('a');
      expect(byElement).toHaveLength(2);
      const byVector = await vs.getSimilar({ vector: [1, 0], count: 2, linearScan: true, mainThread: true });
      expect(byVector).toContain('a');
      const filtered = await vs.getSimilar({ element: 'a', count: 3, filter: '.tag == "y2"' });
      expect(filtered).toEqual(['b']);

      const entries = await vs.getSimilarEntries({ vector: [1, 0], count: 2, epsilon: 0.5 });
      expect(entries.map((x) => x.value)).toContain('a');
      expect(entries[0].score).toBeGreaterThanOrEqual(entries[1].score);
      const withAttrs = await vs.getSimilarEntriesWithAttributes({ vector: [1, 0], count: 3 });
      const b = withAttrs.find((x) => x.value === 'b');
      expect(b?.attributes).toContain('y2');

      // Option pass-through on fresh sets.
      expect(await r.getVectorSet(names[1]).add({ element: 'm', vector: [1, 0], maxConnections: 16 })).toBe(true);
      expect(await r.getVectorSet(names[2]).add({ element: 'q', vector: [1, 0], quantization: 'Q8' })).toBe(true);
      expect(await r.getVectorSet(names[3]).add({ element: 'r', vector: [1, 0], reduce: 2 })).toBe(true);
    } finally {
      await r.getKeys().delete(...names);
      await r.shutdown();
    }
  }, 120000);
});
