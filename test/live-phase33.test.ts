/**
 * Phase 33 live: RScript kill/flush, RFunction operational surface
 * (list/stats/dump/restore/kill) and Redis 8 VectorSet deep API
 * (VEMB/VINFO/VLINKS/VRANDMEMBER/VRANGE/VDIM/VGETATTR/VSETATTR).
 *
 * Commands are capability-gated; skips when Redis (or the vector-set module)
 * is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p33:';
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

describe('live interop phase 33: script/function/vector-set surface', () => {
  it('script kill/flush and function list/stats/dump/restore', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      if (!(await hasCmd(r.redis, 'FUNCTION'))) {
        console.log('[phase33] FUNCTION unsupported — skipping');
        return;
      }
      const script = r.getScript();
      const sha = await script.scriptLoad('return 42');
      expect((await script.scriptExists(sha))[0]).toBe(true);
      await script.scriptFlush();
      expect((await script.scriptExists(sha))[0]).toBe(false);
      await expect(script.scriptKill()).rejects.toThrow(/NOTBUSY|No scripts/i);

      const fn = r.getFunction();
      const lib = `#!lua name=p33lib\nredis.register_function('p33answer', function() return 42 end)`;
      await fn.flush();
      await fn.load(lib);
      const list = await fn.list('p33lib');
      expect(list.length).toBe(1);
      expect(list[0].name).toBe('p33lib');
      expect(list[0].functions).toContain('p33answer');
      expect(list[0].engine).toBeDefined();

      const stats = await fn.stats();
      expect(stats).toBeTypeOf('object');
      const dump = await fn.dump();
      expect(dump.length).toBeGreaterThan(0);

      await fn.flush();
      expect(await fn.list()).toEqual([]);
      await fn.restore(dump);
      expect(await fn.callFn('p33answer')).toBe(42);

      await fn.flush();
      await fn.restoreAfterFlush(dump);
      expect(await fn.callFn('p33answer')).toBe(42);

      await fn.delete('p33lib');
      expect(await fn.list('p33lib')).toEqual([]);
    } finally {
      try { await r.getFunction().flush(); } catch { /* noop */ }
      await r.shutdown();
    }
  }, 120000);

  it('VectorSet deep API', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vec`;
    try {
      if (!(await hasCmd(r.redis, 'VADD'))) {
        console.log('[phase33] VectorSet unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name);
      const vs = r.getVectorSet(name);
      expect(await vs.add('a', [1, 0, 0])).toBe(true);
      expect(await vs.add('b', [0, 1, 0])).toBe(true);
      expect(await vs.add('c', [0, 0, 1])).toBe(true);
      expect(await vs.size()).toBe(3);
      expect(await vs.dimensions()).toBe(3);
      expect(await vs.getVector('a')).toEqual([1, 0, 0]);
      expect(await vs.contains('a')).toBe(true);
      expect(await vs.contains('missing')).toBe(false);

      expect(['a', 'b', 'c']).toContain(await vs.random());
      expect((await vs.random(2)).length).toBe(2);
      expect((await vs.range('-', '+', 10)).sort()).toEqual(['a', 'b', 'c']);
      expect(await vs.getNeighbors('a')).toEqual(expect.any(Array));

      const info = await vs.getInfo();
      expect(Number(info.size)).toBe(3);
      expect(Number(info['vector-dim'])).toBe(3);

      expect(await vs.setAttributes('a', { x: 1 })).toBe(true);
      expect(await vs.getAttributes('a')).toEqual({ x: 1 });
      expect(await vs.getAttributes('missing')).toBeNull();
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);

  it('RMapCacheNative absolute (Date) field expiry', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn`;
    try {
      await r.getKeys().delete(name);
      const mc = r.getMapCacheNative<string, string>(name);
      await mc.put('k', 'v', new Date(Date.now() + 5000));
      expect(await mc.get('k')).toBe('v');
      expect(await mc.remainTimeToLive('k')).toBeGreaterThan(0);
      expect(await mc.expireEntry('k', new Date(Date.now() + 8000))).toBe(true);
      expect(await mc.remainTimeToLive('k')).toBeGreaterThan(5000);
      await mc.put('gone', 'x', new Date(Date.now() - 1000));
      expect(await mc.get('gone')).toBeNull();
      await mc.put('k2', 'v2');
      expect(await mc.putIfAbsent('k2', 'other', new Date(Date.now() + 5000))).toBe('v2');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
