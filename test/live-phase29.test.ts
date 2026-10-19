/**
 * Phase 29 live interop: rwlock_timeout protocol. Java 4.6.1's read-lock
 * unlock script recomputes the HASH TTL from per-acquisition
 * `<tagged>:<field>:rwlock_timeout:<n>` keys; without them a Java release
 * could delete a HASH that still has other readers. This test mixes a
 * Java-held read lock with a TS read lock and verifies the lock survives the
 * TS release, then disappears after Java's own release.
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probeBg } from './helpers/java-probe';

const NS = 'TsJavaT:p29:';
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

describe('live interop phase 29: rwlock_timeout', () => {
  it('TS read release keeps a Java-held read lock alive (and its timeout key)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw`;
    const tagged = `{${name}}`;
    const cleanup = async () => {
      const keys = await r.getKeys().getKeys(`*${name}*`);
      if (keys.length > 0) await r.getKeys().delete(...keys);
    };
    try {
      await cleanup();
      const hold = probeBg(`rw-read-hold ${name} 5000`);
      let ready = false;
      for (let i = 0; i < 100 && !ready; i++) {
        await sleep(100);
        ready = (await r.redis.exists(name)) === 1 && (await r.redis.hget(name, 'mode')) === 'read';
      }
      expect(ready).toBe(true);

      const javaField = (await r.redis.hkeys(name)).find((f) => f !== 'mode');
      expect(javaField).toBeTruthy();
      expect(await r.redis.exists(`${tagged}:${javaField}:rwlock_timeout:1`)).toBe(1);

      const tsRead = r.getReadWriteLock(name).getReadLock();
      await tsRead.lock(30000);
      expect(await r.redis.hlen(name)).toBe(3); // mode + Java reader + TS reader
      const tsField = (await r.redis.hkeys(name)).find((f) => f !== 'mode' && f !== javaField);
      expect(tsField).toBeTruthy();
      // real Redis: TS writes the same integer-suffixed key Java expects
      expect(await r.redis.exists(`${tagged}:${tsField}:rwlock_timeout:1`)).toBe(1);
      expect(await r.redis.pttl(`${tagged}:${tsField}:rwlock_timeout:1`)).toBeGreaterThan(0);
      await tsRead.unlock();
      // Java's reader (and its timeout key) must survive the TS release
      expect(await r.redis.exists(name)).toBe(1);
      expect(await r.redis.hget(name, 'mode')).toBe('read');
      expect(await r.redis.hexists(name, javaField!)).toBe(1);
      expect(await r.redis.exists(`${tagged}:${javaField}:rwlock_timeout:1`)).toBe(1);
      expect(await r.redis.pttl(name)).toBeGreaterThan(0);

      await hold; // Java unlocks after 5s
      expect(await r.redis.exists(name)).toBe(0);
    } finally {
      await cleanup();
      await r.shutdown();
    }
  }, 180000);
});
