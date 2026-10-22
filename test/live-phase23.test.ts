/**
 * Phase 23 live interop: RKeys ops (expire/expireAt/clearExpire/remainTimeToLive/
 * getSlot/move/unlinkByPattern), RBuckets conditional writes (MSETEX NX/XX,
 * Java reads the values back through JsonJacksonCodec), atomic Long/Double
 * remainder, and RScoredSortedSet addIfAbsent/Exists/Less/Greater (ZADD
 * NX/XX CH/LT CH/GT CH — cross-checked through the Java `zset-score` probe).
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p23:';
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

describe('live interop phase 23: keys ops + conditionals + atomics', () => {
  it('RKeys expire family/remainTTL/getSlot (vs Java)/move/unlinkByPattern', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}k1`;
    const k2 = `${NS}k2`;
    const mv = `${NS}mv`;
    let db9: Redis | null = null;
    try {
      await r.getKeys().delete(k1, k2, mv);
      await r.redis.set(k1, '"v"');
      expect(await r.getKeys().remainTimeToLive(k1)).toBe(-1);
      expect(await r.getKeys().expire(k1, 5000)).toBe(true);
      const ttl = await r.getKeys().remainTimeToLive(k1);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(5000);
      expect(await r.getKeys().clearExpire(k1)).toBe(true);
      expect(await r.getKeys().remainTimeToLive(k1)).toBe(-1);
      expect(await r.getKeys().expireAt(k1, Date.now() + 5000)).toBe(true);
      expect(await r.getKeys().remainTimeToLive(k1)).toBeGreaterThan(0);

      await r.redis.set(k2, '"v"');
      expect(await r.getKeys().expire(5000, k1, k2)).toBe(2);
      expect(await r.getKeys().expireAt(Date.now() + 5000, k1, k2)).toBe(2);
      expect(await r.getKeys().clearExpire(k1)).toBe(true);

      // slot: local CRC16 vector + live cluster CLUSTER KEYSLOT cross-check
      // (Java RKeys.getSlot delegates to CLUSTER KEYSLOT, which needs a
      // cluster-enabled server; node 7100 serves that role when running).
      expect(await r.getKeys().getSlot('123456789')).toBe(12739);
      let clusterCli: Redis | null = null;
      try {
        clusterCli = new Redis('redis://127.0.0.1:7100', { lazyConnect: true, connectTimeout: 1500, maxRetriesPerRequest: 1 });
        await clusterCli.connect();
      } catch {
        clusterCli = null;
      }
      try {
        for (const key of ['123456789', `${NS}plain`, `${NS}{tag}a`, `${NS}{tag}b`, 'foo{}{bar}', 'foo{bar}{zap}']) {
          if (clusterCli) {
            expect(Number(await (clusterCli as any).cluster('KEYSLOT', key))).toBe(await r.getKeys().getSlot(key));
          }
        }
        expect(await r.getKeys().getSlot(`${NS}{tag}a`)).toBe(await r.getKeys().getSlot(`${NS}{tag}b`));
        if (!clusterCli) console.log('[phase23] cluster 7100 unreachable — slots checked against the local CRC16 vector only');
      } finally {
        clusterCli?.disconnect();
      }

      // MOVE across logical databases (verified from a db-9 connection)
      await r.redis.set(mv, '"moved"');
      expect(await r.getKeys().move(mv, 9)).toBe(true);
      expect(await r.redis.exists(mv)).toBe(0);
      db9 = new Redis(REDIS_URL, { db: 9, lazyConnect: true, maxRetriesPerRequest: 1 });
      await db9.connect();
      expect(await db9.get(mv)).toBe('"moved"');
      await db9.del(mv);
      expect(await r.getKeys().move(`${NS}nope`, 9)).toBe(false);

      // unlinkByPattern
      await r.redis.set(`${NS}pat:a`, '1');
      await r.redis.set(`${NS}pat:b`, '1');
      await r.redis.set(`${NS}pat:c`, '1');
      expect(await r.getKeys().unlinkByPattern(`${NS}pat:*`)).toBe(3);
      expect(await r.getKeys().getKeys(`${NS}pat:*`)).toEqual([]);
    } finally {
      if (db9) db9.disconnect();
      await r.getKeys().delete(k1, k2, mv);
      await r.shutdown();
    }
  }, 120000);

  it('RBuckets MSETEX NX/XX with TTL; Java reads the same values', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b1 = `${NS}b1`;
    const b2 = `${NS}b2`;
    try {
      await r.getKeys().delete(b1, b2, `${NS}bmiss`);
      // Warm the resident probe (JVM boot + possible Maven recompile) BEFORE
      // writing short-TTL keys, or Java would read them after expiry.
      await probe(`buckets-get ${NS}warm1 ${NS}warm2`);
      const buckets = r.getBuckets();
      expect(await buckets.setIfAllKeysAbsent({ [b1]: 7, [b2]: 8 }, 8000)).toBe(true);
      expect(await r.redis.pttl(b1)).toBeGreaterThan(0);
      expect(await probe(`buckets-get ${b1} ${b2}`)).toBe(`BUCKETS:${b1}=7,${b2}=8`);

      expect(await buckets.setIfAllKeysAbsent({ [b1]: 9, [b2]: 9 })).toBe(false); // both exist
      expect(await buckets.setIfAllKeysAbsent({ [b1]: 9, [b2]: 9, [`${NS}bmiss`]: 9 })).toBe(false);
      expect(await r.redis.get(b1)).toBe('7'); // NX failure writes nothing

      expect(await buckets.setIfAllKeysExist({ [b1]: 11, [b2]: 12 })).toBe(true);
      expect(await r.redis.mget(b1, b2)).toEqual(['11', '12']);
      expect(await buckets.setIfAllKeysExist({ [b1]: 1, [`${NS}bmiss`]: 2 })).toBe(false);
      expect(await r.redis.get(b1)).toBe('11'); // XX failure writes nothing
      // TTL was dropped by the second (TTL-less) write, same as Redis MSETEX
      expect(await r.redis.pttl(b1)).toBe(-1);
    } finally {
      await r.getKeys().delete(b1, b2, `${NS}bmiss`);
      await r.shutdown();
    }
  }, 120000);

  it('atomic Long/Double remainder behaves exactly and survives Java reads', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ln = `${NS}l`;
    const dn = `${NS}d`;
    try {
      await r.getKeys().delete(ln, dn);
      const l = r.getAtomicLong(ln);
      await l.set(10);
      expect(await l.getAndIncrement()).toBe(10);
      expect(await l.getAndDecrement()).toBe(11);
      expect(await l.setIfLess(11, 5)).toBe(true);
      expect(await l.setIfGreater(3, 7)).toBe(true);
      expect(await l.setIfGreater(9, 1)).toBe(false);
      expect(await probe(`atomic-get ${ln}`)).toBe('ATOMIC:7'); // Java reads the same integer
      expect(await l.getAndDelete()).toBe(7);
      expect(await r.redis.exists(ln)).toBe(0);
      expect(await l.getAndDelete()).toBeNull();

      const d = r.getAtomicDouble(dn);
      await d.set(0.1);
      expect(await d.getAndAdd(0.2)).toBeCloseTo(0.1, 12);
      expect(await r.redis.get(dn)).toBe('0.3'); // exact decimal on the wire
      expect(await d.incrementAndGet()).toBeCloseTo(1.3, 12);
      expect(await d.getAndIncrement()).toBeCloseTo(1.3, 12);
      expect(await d.decrementAndGet()).toBeCloseTo(1.3, 12);
      expect(await d.getAndDecrement()).toBeCloseTo(1.3, 12);
      expect(await d.setIfLess(1, 0.5)).toBe(true);
      expect(await d.setIfGreater(1, 0.25)).toBe(false);
      expect(await r.redis.get(dn)).toBe('0.5');
      expect(await d.compareAndDelete(0.25)).toBe(false);
      expect(await d.compareAndDelete(0.5)).toBe(true);
      expect(await d.getAndDelete()).toBeNull();
    } finally {
      await r.getKeys().delete(ln, dn);
      await r.shutdown();
    }
  }, 120000);

  it('zset addIfAbsent/Exists/Less/Greater use Java ZADD flags (score cross-check)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const zn = `${NS}z`;
    try {
      await r.getKeys().delete(zn);
      const z = r.getScoredSortedSet<string>(zn);
      expect(await z.addIfAbsent(1.5, 'a')).toBe(true);
      expect(await z.addIfAbsent(9, 'a')).toBe(false);
      expect(await z.getScore('a')).toBe(1.5);

      expect(await z.addIfGreater(1, 'a')).toBe(false); // lower → unchanged
      expect(await z.getScore('a')).toBe(1.5);
      expect(await z.addIfGreater(2.5, 'a')).toBe(true); // higher → updated
      expect(await z.getScore('a')).toBe(2.5);

      expect(await z.addIfLess(3, 'a')).toBe(false); // higher → unchanged
      expect(await z.addIfLess(2, 'a')).toBe(true); // lower → updated
      expect(await z.getScore('a')).toBe(2);

      expect(await z.addIfGreater(4.25, 'b')).toBe(true); // missing → added by GT
      expect(await z.addIfLess(0.5, 'c')).toBe(true); // missing → added by LT

      expect(await z.addIfExists(9, 'ghost')).toBe(false);
      expect(await z.addIfExists(3, 'a')).toBe(true);
      expect(await probe(`zset-score ${zn} a`)).toBe('ZSCORE:3.0');
      expect(await probe(`zset-score ${zn} b`)).toBe('ZSCORE:4.25');
      expect(await probe(`zset-score ${zn} c`)).toBe('ZSCORE:0.5');
    } finally {
      await r.getKeys().delete(zn);
      await r.shutdown();
    }
  }, 120000);
});
