/**
 * Phase 19 live interop: key-synchronizer companion names, map key locks,
 * RMapCache entry events (both directions) and RMapCacheNative conditional
 * entry expiry (HEXPIRE/HPERSIST family).
 *
 * Needs real Redis + Java probe; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { keySynchronizerName, suffixName } from '../src/util';
import { probe, probeBg, probeLines } from './helpers/java-probe';

const NS = 'TsJavaT:p19:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

const sleep = (ms: number) => new Promise((x) => setTimeout(x, ms));

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

describe('live interop phase 19: key synchronizers + mapcache depth', () => {
  it('TS and Java derive identical companion names for all six synchronizers', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sync`;
    try {
      const out = await probe(`map-lock-name ${name} k1`);
      expect(out.startsWith('SYNC:')).toBe(true);
      const parts = out.slice('SYNC:'.length).split('|');
      const expected = ['lock', 'fairlock', 'rw_lock', 'semaphore', 'countdownlatch', 'permitexpirablesemaphore']
        .map((suffix) => keySynchronizerName(name, '"k1"', suffix));
      expect(parts).toEqual(expected);
      // TS view objects resolve to the very same names
      const map = r.getMap<string, string>(name);
      expect(map.getLock('k1').name).toBe(parts[0]);
      expect(map.getFairLock('k1').name).toBe(parts[1]);
      expect(map.getReadWriteLock('k1').readLock.name).toBe(parts[2]);
      expect(map.getReadWriteLock('k1').writeLock.name).toBe(parts[2]);
      expect(map.getSemaphore('k1').name).toBe(parts[3]);
      expect(map.getCountDownLatch('k1').name).toBe(parts[4]);
      expect(map.getPermitExpirableSemaphore('k1').name).toBe(parts[5]);
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('map key lock excludes Java and Java key lock excludes TS', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mlock`;
    const map = r.getMap<string, string>(name);
    try {
      await r.getKeys().delete(name, `${name}:redisson_options`);
      const l = map.getLock('k');
      expect(await l.tryLock(0, 20000, 1)).toBe(true);
      expect(await probe(`map-lock-try ${name} k`)).toBe('CONTENDED');
      await l.unlock(1);
      expect(await probe(`map-lock-try ${name} k`)).toBe('ACQUIRED');

      // Java holds in the background; TS must not acquire until release
      const hold = probeBg(`map-lock-hold ${name} k 4000`);
      let held = false;
      for (let i = 0; i < 120 && !held; i++) {
        await sleep(150);
        held = (await r.redis.exists(l.name)) === 1;
      }
      expect(held).toBe(true);
      expect(await map.getLock('k').tryLock(0, 5000, 2)).toBe(false);
      await hold;
      // after Java's unlock the derived hash must be gone
      expect(await r.redis.exists(l.name)).toBe(0);
      expect(await map.getLock('k').tryLock(0, 5000, 2)).toBe(true);
      await map.getLock('k').unlock(2);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 240000);

  it('TS mapcache listener receives Java-authored events (Java writes, TS reads)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc:j2t`;
    const optionsKey = suffixName(name, 'redisson_options');
    const mc = r.getMapCache<string, string>(name);
    const events: string[] = [];
    try {
      await r.getKeys().delete(name, optionsKey);
      await mc.addListener({
        onCreated: (e) => events.push(`CREATED:${e.key}:${e.value}`),
        onUpdated: (e) => events.push(`UPDATED:${e.key}:${e.value}:${e.oldValue}`),
        onRemoved: (e) => events.push(`REMOVED:${e.key}:${e.oldValue}`),
      });
      // has-listeners gates Java's publish path in the Lua scripts
      expect(await r.redis.hget(optionsKey, 'has-listeners')).toBe('1');

      expect(await probe(`mapcache-put ${name} k v1`)).toBe('MC_PUT');
      expect(await probe(`mapcache-put ${name} k v2`)).toBe('MC_PUT');
      expect(await probe(`mapcache-remove ${name} k`)).toBe('MC_REMOVED');

      for (let i = 0; i < 100 && events.length < 3; i++) await sleep(100);
      expect(events).toContain('CREATED:k:v1');
      expect(events).toContain('UPDATED:k:v2:v1');
      expect(events).toContain('REMOVED:k:v2');
    } finally {
      await mc.removeAllListeners();
      await r.getKeys().delete(name, optionsKey);
      await r.shutdown();
    }
  }, 240000);

  it('Java mapcache listener receives TS-authored events (TS writes, Java reads)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc:t2j`;
    const optionsKey = suffixName(name, 'redisson_options');
    const mc = r.getMapCache<string, string>(name);
    try {
      await r.getKeys().delete(name, optionsKey);
      // Start the Java listener; it registers has-listeners itself and then
      // collects events for 6s before printing them.
      const listen = probeLines(`mapcache-listen ${name} 6000`);
      let ready = false;
      for (let i = 0; i < 160 && !ready; i++) {
        await sleep(150);
        ready = (await r.redis.hget(optionsKey, 'has-listeners')) === '1';
      }
      expect(ready).toBe(true);
      await sleep(500); // let the JVM finish its topic subscriptions

      await mc.put('k', 'v1');
      await mc.put('k', 'v2');
      await mc.remove('k');

      const lines = (await listen).filter((l) => l.startsWith('MCE:'));
      expect(lines).toContain('MCE:CREATED:k:v1:null');
      expect(lines).toContain('MCE:UPDATED:k:v2:v1');
      expect(lines).toContain('MCE:REMOVED:k:null:v2');
    } finally {
      await r.getKeys().delete(name, optionsKey);
      await r.shutdown();
    }
  }, 240000);

  it('RMapCacheNative conditional expiry interoperates with Java HPEXPIRE/HPERSIST', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcn`;
    const mn = r.getMapCacheNative<string, string>(name);
    const ttlOf = async (key: string) => Number((await probe(`mcnative-ttl ${name} ${key}`)).replace('MCN_TTL:', ''));
    try {
      await r.getKeys().delete(name);
      // Java writes with a Duration TTL; TS must observe it via HPTTL FIELDS
      expect(await probe(`mcnative-put ${name} k v 60000`)).toBe('MCN_SET');
      const t = await mn.remainTimeToLive('k');
      expect(t).toBeGreaterThan(0);
      expect(t).toBeLessThanOrEqual(60000);

      // TS writes a TTL-less field; Java answers -1 (no expire) / -2 (missing)
      await mn.put('c', 'ts3');
      expect(await probe(`mcnative-get ${name} c`)).toBe('MCN_GET:ts3');
      expect(await ttlOf('c')).toBe(-1);
      expect(await ttlOf('missing')).toBe(-2);
      expect(await probe(`mcnative-expire ${name} nope 1000`)).toBe('MCN_EXPIRE:false');

      // TS sets a conditional TTL, Java observes the remaining time
      expect(await mn.expireEntry('c', 30000)).toBe(true);
      const tc = await ttlOf('c');
      expect(tc).toBeGreaterThan(0);
      expect(tc).toBeLessThanOrEqual(30000);
      // NX refuses once a TTL exists; LT shortens it; GT lengthens it again
      // (all local calls — no JVM startup skew between them)
      expect(await mn.expireEntryIfNotSet('c', 5000)).toBe(false);
      expect(await mn.expireEntryIfLess('c', 15000)).toBe(true);
      let remaining = await mn.remainTimeToLive('c');
      expect(remaining).toBeGreaterThan(0);
      expect(remaining).toBeLessThanOrEqual(15000);
      expect(await mn.expireEntryIfGreater('c', 20000)).toBe(true);
      remaining = await mn.remainTimeToLive('c');
      expect(remaining).toBeGreaterThan(15000);
      expect(remaining).toBeLessThanOrEqual(20000);

      // Java HPERSIST clears a TS-set TTL (TTL long enough to survive the
      // ~7-10s probe JVM startup; 4.6.1 answers Boolean null once gone)
      await mn.put('e', 'x');
      expect(await mn.expireEntry('e', 120000)).toBe(true);
      expect(await probe(`mcnative-clearexpire ${name} e`)).toBe('MCN_CLEAR:true');
      expect(await mn.remainTimeToLive('e')).toBe(-1);
      expect(await mn.clearExpire('e')).toBe(false); // TS: already persisted (no TTL)

      // Java applies a TTL to a fresh field; TS honours NX/GT/LT on it
      await mn.put('d', 'x');
      expect(await probe(`mcnative-expire-notset ${name} d 60000`)).toBe('MCN_EXPIRE:true');
      expect(await mn.expireEntryIfNotSet('d', 30000)).toBe(false);
      expect(await probe(`mcnative-expire-gt ${name} d 90000`)).toBe('MCN_EXPIRE:true');
      expect(await probe(`mcnative-expire-lt ${name} d 70000`)).toBe('MCN_EXPIRE:true');

      // HPERSIST from TS clears the Java-set TTL
      expect(await mn.clearExpire('d')).toBe(true);
      expect(await mn.remainTimeToLive('d')).toBe(-1);
      expect(await probe(`mcnative-ttl ${name} d`)).toBe('MCN_TTL:-1');

      // putIfAbsent: Java keeps an existing value (returns it, TTL untouched),
      // TS sets when absent with a TTL Java can observe
      expect(await probe(`mcnative-putifabsent ${name} g g1 60000`)).toBe('MCN_PIA:null');
      expect(await probe(`mcnative-putifabsent ${name} g g2 60000`)).toBe('MCN_PIA:g1');
      expect(await mn.get('g')).toBe('g1');
      expect(await mn.putIfAbsent('g', 'tsg', 60000)).toBe('g1');
      expect(await mn.get('g')).toBe('g1');
      expect(await mn.remainTimeToLive('g')).toBeGreaterThan(5000);
      expect(await mn.putIfAbsent('h', 'tsh', 60000)).toBe(null);
      const th = await ttlOf('h');
      expect(th).toBeGreaterThan(5000);
      expect(th).toBeLessThanOrEqual(60000);
      expect(await probe(`mcnative-putifabsent ${name} h h2 60000`)).toBe('MCN_PIA:tsh');
      expect(await mn.get('h')).toBe('tsh');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
