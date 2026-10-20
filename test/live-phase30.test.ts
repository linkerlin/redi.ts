/**
 * Phase 30 live interop: RMapCache.compute / computeIfAbsent (per-key lock,
 * Java reads the result through `mc-get`) and RTopic StatusListener callbacks.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p30:';
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
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

describe('live interop phase 30: mapcache compute + status listeners', () => {
  it('compute/computeIfAbsent + Java mc-get, concurrent serialization', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc`;
    try {
      const clean = async () => {
        const keys = await r.getKeys().getKeys(`*${name}*`);
        if (keys.length > 0) await r.getKeys().delete(...keys);
      };
      await clean();
      await probe(`mc-get ${name} warm`);
      const mc = r.getMapCache<string, string>(name);
      expect(await mc.compute('k', 60000, (_k, old) => (old === null ? '1' : String(Number(old) + 1)))).toBe('1');
      expect(await mc.compute('k', 60000, (_k, old) => (old === null ? '1' : String(Number(old) + 1)))).toBe('2');
      expect(await probe(`mc-get ${name} k`)).toBe('MC_GET:2');

      await Promise.all([1, 2, 3, 4, 5].map(() => mc.compute('c', 60000, (_k, old) => (old === null ? '1' : String(Number(old) + 1)))));
      expect(await mc.get('c')).toBe('5');
      expect(await mc.remainTimeToLive('c')).toBeGreaterThan(0);
      expect(await probe(`mc-get ${name} c`)).toBe('MC_GET:5');

      expect(await mc.compute('k', 60000, () => null)).toBeNull();
      expect(await probe(`mc-get ${name} k`)).toBe('MC_NULL');

      expect(await mc.computeIfAbsent('a', 60000, () => '7')).toBe('7');
      expect(await mc.computeIfAbsent('a', 60000, () => '9')).toBe('7');
      expect(await probe(`mc-get ${name} a`)).toBe('MC_GET:7');
      expect(await mc.remainTimeToLive('a')).toBeGreaterThan(0);
    } finally {
      const keys = await r.getKeys().getKeys(`*${name}*`);
      if (keys.length > 0) await r.getKeys().delete(...keys);
      await r.shutdown();
    }
  }, 120000);

  it('status listener fires on subscribe/unsubscribe', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}topic`;
    try {
      const t = r.getTopic<string>(name);
      const events: string[] = [];
      const status = {
        onSubscribe: (ch: string) => events.push(`sub:${ch}`),
        onUnsubscribe: (ch: string) => events.push(`unsub:${ch}`),
      };
      const sid = await t.addStatusListener(status);
      expect(events).toEqual([`sub:${name}`]);
      expect(await t.countListeners()).toBe(1);
      const mid = await t.addListener(() => undefined);
      expect(await t.countListeners()).toBe(2);
      await t.removeListener(sid);
      expect(events).toEqual([`sub:${name}`, `unsub:${name}`]);
      await t.removeListener(mid);
      expect(await t.countListeners()).toBe(0);

      // removeAllListeners clears both kinds
      await t.addStatusListener(status);
      await t.addListener(() => undefined);
      await t.removeAllListeners();
      expect(await t.countListeners()).toBe(0);
      expect(events.filter((e) => e.startsWith('unsub:')).length).toBe(2);
      await sleep(0);
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
