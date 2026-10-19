/**
 * Phase 16 live interop: companion-key lifecycle (delete/unlink), DelayedQueue
 * size/delete semantics, and LocalCachedMap local TTL/idle.
 *
 * The cross-language assertions focus on re-initialisation: after TS delete(),
 * Java must be able to set capacity/token again. Orphaned companion keys were
 * the failure mode this phase fixes.
 *
 * Needs real Redis + Java probe; skips silently when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';
import { prefixName, suffixName } from '../src/util';

const NS = 'TsJavaT:p16:';
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

const hasRedis = await redisUp();

describe('live interop phase 16: lifecycle + delayed queue + local cache', () => {
  it('bounded queue capacity can be re-initialised by Java after TS delete', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bq`;
    const sem = prefixName('redisson_bqs', name);
    try {
      await cleanExact(r, name, sem);
      const bq = r.getBoundedBlockingQueue<string>(name);
      expect(await bq.trySetCapacity(1)).toBe(true);
      expect(await bq.offer('v')).toBe(true);
      expect(await probe(`bq-capacity ${name} 2`)).toBe('BQ_CAP:false');
      await bq.delete();
      expect(await r.redis.exists(name, sem)).toBe(0);
      expect(await probe(`bq-capacity ${name} 2`)).toBe('BQ_CAP:true');
    } finally {
      await cleanExact(r, name, sem);
      await r.shutdown();
    }
  }, 180000);

  it('ring buffer capacity can be re-initialised by Java after TS delete', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ring`;
    const cap = prefixName('redisson_rb', name);
    try {
      await cleanExact(r, name, cap);
      const rb = r.getRingBuffer<string>(name);
      expect(await rb.trySetCapacity(2)).toBe(true);
      await rb.add('a');
      expect(await probe(`ring-setcap ${name} 3`)).toBe('RING_CAP:false');
      await rb.delete();
      expect(await r.redis.exists(name, cap)).toBe(0);
      expect(await probe(`ring-setcap ${name} 3`)).toBe('RING_CAP:true');
    } finally {
      await cleanExact(r, name, cap);
      await r.shutdown();
    }
  }, 180000);

  it('delayed queue size is the staging list; delete preserves the target queue', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}dq`;
    const staging = prefixName('redisson_delay_queue', name);
    const timeout = prefixName('redisson_delay_queue_timeout', name);
    try {
      await cleanExact(r, name, staging, timeout);
      const dq = r.getDelayedQueue<string>(name);
      await dq.offer('future', 60000);
      expect(await dq.size()).toBe(1);
      expect(await dq.targetSize()).toBe(0);
      expect(await probe(`dq-size ${name}`)).toBe('DQ_SIZE:1');

      // A caller-owned destination element must survive dq.delete().
      await r.getBlockingQueue<string>(name).offer('keep');
      await dq.delete();
      expect(await r.redis.exists(staging, timeout)).toBe(0);
      expect(await r.getBlockingQueue<string>(name).size()).toBe(1);
      expect(await probe(`dq-size ${name}`)).toBe('DQ_SIZE:0');
    } finally {
      await cleanExact(r, name, staging, timeout);
      await r.shutdown();
    }
  }, 180000);

  it('multimap delete removes child collections and TTL ZSET (Java reads empty)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mm`;
    const ttl = suffixName(name, 'redisson_set_multimap_ttl');
    try {
      await cleanExact(r, name, ttl);
      const mm = r.getSetMultimapCache<string, string>(name);
      await mm.put('k', 'v');
      expect(await mm.expireKey('k', 60000)).toBe(true);
      expect(await probe(`mm-get ${name} k`)).toBe('MM_GET:[v]');

      const id = await r.redis.hget(name, mm.encodeKey('k'));
      expect(id).not.toBeNull();
      const child = suffixName(name, id!);
      await mm.delete();
      expect(await r.redis.exists(name, child, ttl)).toBe(0);
      expect(await probe(`mm-get ${name} k`)).toBe('MM_GET:[]');
    } finally {
      const ids = await r.redis.hvals(name);
      const children = ids.map((id) => suffixName(name, id));
      const stale = await r.redis.keys(`${name}*`);
      await cleanExact(r, name, ttl, ...children, ...stale);
      await r.shutdown();
    }
  }, 180000);

  it('fenced lock token restarts at 1 after TS delete', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fenced`;
    const token = prefixName('redisson_lock_token', name);
    try {
      await cleanExact(r, name, token);
      await r.redis.hset(name, `${r.id}:1`, '1');
      await r.redis.set(token, '7');
      await r.getFencedLock(name).delete();
      expect(await r.redis.exists(name, token)).toBe(0);
      expect(await probe(`fenced-token ${name}`)).toBe('TOKEN:1');
    } finally {
      await cleanExact(r, name, token);
      await r.shutdown();
    }
  }, 180000);

  it('fair lock / PES / set cache leave no companion keys behind', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const fair = `${NS}fair`;
    const pesName = `${NS}pes`;
    const scName = `${NS}sc`;
    const queue = prefixName('redisson_lock_queue', fair);
    const fairTimeout = prefixName('redisson_lock_timeout', fair);
    const pesTimeout = suffixName(pesName, 'timeout');
    const idle = prefixName('redisson__idle__set', scName);
    const touch = suffixName(scName, 'redisson_touch');
    try {
      await cleanExact(r, fair, queue, fairTimeout, pesName, pesTimeout, scName, idle, touch);

      const fl = r.getFairLock(fair);
      await r.redis.hset(fair, `${r.id}:1`, '1');
      await r.redis.rpush(queue, `${r.id}:1`);
      await r.redis.zadd(fairTimeout, '1', `${r.id}:1`);
      await fl.delete();
      expect(await r.redis.exists(fair, queue, fairTimeout)).toBe(0);

      const pes = r.getPermitExpirableSemaphore(pesName);
      expect(await pes.trySetPermits(1)).toBe(true);
      expect(await pes.tryAcquire(60000)).not.toBeNull();
      await pes.delete();
      expect(await r.redis.exists(pesName, pesTimeout)).toBe(0);

      const sc = r.getSetCache<string>(scName);
      expect(await sc.add('v', 60000, 60000)).toBe(true);
      await sc.delete();
      expect(await r.redis.exists(scName, idle, touch)).toBe(0);
    } finally {
      await cleanExact(r, fair, queue, fairTimeout, pesName, pesTimeout, scName, idle, touch);
      await r.shutdown();
    }
  }, 120000);

  it('local cached map expires near-cache entries by TTL and idle', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ttlName = `${NS}lcm:ttl`;
    const idleName = `${NS}lcm:idle`;
    const field = '"k"';
    try {
      await cleanExact(r, ttlName, idleName);
      const ttl = r.getLocalCachedMap<string, string>(ttlName, { timeToLiveMs: 500 });
      const idle = r.getLocalCachedMap<string, string>(idleName, { maxIdleMs: 500 });
      try {
        await ttl.put('k', 'near');
        await r.redis.hset(ttlName, field, '"backend"');
        await new Promise((x) => setTimeout(x, 700));
        expect(await ttl.get('k')).toBe('backend');

        await idle.put('k', 'near');
        await r.redis.hset(idleName, field, '"backend"');
        expect(await idle.get('k')).toBe('near');
        await new Promise((x) => setTimeout(x, 700));
        expect(await idle.get('k')).toBe('backend');
      } finally {
        await ttl.destroy();
        await idle.destroy();
      }
    } finally {
      await cleanExact(r, ttlName, idleName);
      await r.shutdown();
    }
  }, 120000);
});
