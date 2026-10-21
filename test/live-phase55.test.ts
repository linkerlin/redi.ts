/**
 * Phase 55 live: RVectorSet iterator()/stream() — lexicographic VRANGE paging
 * with exclusive continuation, including a non-alphabetical insertion order.
 *
 * Needs Redis 8 VectorSet; capability-gated on VADD.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p55:';
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

describe('live phase 55: VectorSet iterator/stream', () => {
  it('pages through members lexicographically with small batches', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vs`;
    try {
      if (!(await hasCmd(r.redis, 'VADD'))) {
        console.log('[phase55] VectorSet unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name);
      const vs = r.getVectorSet(name);
      const members = ['m3', 'm1', 'm5', 'm2', 'm4', 'm0'];
      for (const m of members) expect(await vs.add(m, [1, 0])).toBe(true);
      const iterated: string[] = [];
      for await (const m of vs.iterator(2)) iterated.push(m);
      expect(iterated).toEqual([...members].sort());
      const streamed: string[] = [];
      for await (const m of vs.stream(4)) streamed.push(m);
      expect(streamed).toEqual([...members].sort());

      const empty = r.getVectorSet(`${NS}empty`);
      const none: string[] = [];
      for await (const m of empty.iterator(2)) none.push(m);
      expect(none).toEqual([]);
    } finally {
      await r.getKeys().delete(name, `${NS}empty`);
      await r.shutdown();
    }
  }, 120000);
});
