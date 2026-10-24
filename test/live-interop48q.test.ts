/**
 * Live interop vs **Java Redisson 4.8.0**, round 17 — watchdog renewal,
 * shared bloom initialization and search aliases:
 *
 *  - Lock watchdog: a TS lock() held WITHOUT an explicit lease renews
 *    itself (watchdogTimeout 8s) and stays locked well past the first
 *    lease — observed by Java's isLocked at 12s (1.5× the lease: renewal
 *    is the only way the key still exists). Unlock flips it everywhere.
 *  - Bloom filter shared init: TS tryInit writes the `{name}:config` hash
 *    (hashIterations/bitSize via HighwayHash-128); Java ADDS to the same
 *    filter — it must read TS's config and land on the same bits
 *    (round 9 proved each side's OWN init; this closes the shared-init
 *    loop in both directions).
 *  - RSearch aliases: alias added by one language resolves searches in
 *    the other (FT.ALIASADD wire).
 *
 * Probe commands `search48-alias` / `search48-dropalias`.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48q:';

const hasRedis = await (async () => {
  const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
  try {
    await r.connect();
    return true;
  } catch {
    return false;
  } finally {
    r.disconnect();
  }
})();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await raw.call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48q] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48q: watchdog renewal + shared bloom init + aliases vs Java 4.8.0', () => {
  it('RLock watchdog: TS renewal keeps the lock alive past its lease, Java observes', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    // 8s watchdog → ~2.7s renewal period, 8s lease
    const r = Redisson.create({ address: REDIS_URL, lockWatchdogTimeout: 8000 });
    const name = `${NS}lock`;
    try {
      await r.getKeys().delete(name);
      const lock = r.getLock(name);
      await lock.lock(); // NO explicit lease — watchdog governs

      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=true');
      // 12s = 1.5× the first lease: only renewal keeps the key alive.
      // Margin tolerates a ~4s backward clock jump (law 10).
      await new Promise((res) => setTimeout(res, 12000));
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=true');

      await lock.unlock();
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=false');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RBloomFilter shared init: Java adds bits into a TS-initialized filter and back', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'BF.RESERVE'))) {
      console.log('[live interop48q] BF.* unavailable — skipping shared bloom init');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bfl`; // TS-initialized
    const jname = `${NS}bfl-j`; // Java-initialized
    const cfg = (n: string) => `{${n}}:config`;
    try {
      await r.getKeys().delete(name, jname, cfg(name), cfg(jname));

      // TS writes the config hash; Java reads it and lands on the same bits
      const bf = r.getBloomFilter<string>(name);
      expect(await bf.tryInit(1000, 0.01)).toBe(true);
      expect(await probe48.probe(`bfl48-add ${name} shared-ts-init`)).toBe('added=true');
      expect(await bf.contains('shared-ts-init')).toBe(true);
      expect(await bf.count()).toBe(1);

      // reverse: Java's config, TS adds
      const jbf = r.getBloomFilter<string>(jname);
      await probe48.probe(`bfl48-init ${jname} 1000 0.01`);
      expect(await jbf.add('shared-java-init')).toBe(true);
      expect(await probe48.probe(`bfl48-contains ${jname} shared-java-init`)).toBe('contains=true');
      expect(await jbf.count()).toBe(1);
    } finally {
      await r.getKeys().delete(name, jname, cfg(name), cfg(jname));
      await r.shutdown();
    }
  }, 300000);

  it('RSearch aliases: alias added in one language resolves in the other', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'FT.CREATE'))) {
      console.log('[live interop48q] FT.* unavailable — skipping aliases');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const prefix = `${NS}doc:`;
    const alias = `${NS}alias-x`;
    try {
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      try { await r.getSearch().delAlias(alias); } catch { /* absent */ }
      await r.redis.hset(`${prefix}a`, 'title', 'apple', 'cat', 'fruit');
      await r.getSearch().createIndex(idx, ['title', 'TEXT', 'cat', 'TAG'], { prefixes: [prefix] });

      // TS adds the alias; Java searches THROUGH it
      await r.getSearch().addAlias(alias, idx);
      const jSearch = await probe48.probeLines(`search48-count ${alias} @cat:{fruit}`);
      expect(jSearch[0]).toBe('total=1');

      // Java-managed alias: drop and re-add pointing the other way, then TS searches
      await probe48.probe(`search48-dropalias ${alias}`);
      await probe48.probe(`search48-alias ${alias} ${idx}`);
      expect((await r.getSearch().search(alias, '*', { limit: [0, 10] })).total).toBe(1);
    } finally {
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      try { await probe48.probe(`search48-dropalias ${alias}`); } catch { /* probe gone */ }
      await r.getKeys().delete(`${prefix}a`);
      await r.shutdown();
    }
  }, 300000);
});
