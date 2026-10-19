/**
 * Phase 12 live interop: RBatch typed facades. Lua-backed ops (Map.put/
 * remove/putIfAbsent, AtomicLong.compareAndSet) only run on real Redis
 * (mock pipelines lack eval); everything else is mock-covered.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p12:';
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

describe('live interop phase 12: batch lua ops + cross-language reads', () => {
  it('batch Lua ops resolve old values on real Redis', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bmap`;
    const aname = `${NS}batomic`;
    try {
      // NOTE: every key this test touches must be cleaned — CAS asserts on
      // exact values, so cross-run residue flips results (caught in full15).
      await cleanExact(r, name, aname);
      const batch = r.createBatch();
      const m = batch.getMap<string, number>(name);
      const p1 = m.put('k', 1);
      const p2 = m.put('k', 2);
      const pa = m.putIfAbsent('k', 99);
      const rm = m.remove('k');
      const a = batch.getAtomicLong(aname);
      const cas = a.compareAndSet(0, 5);
      await batch.execute();
      expect(await p1).toBeNull();
      expect(await p2).toBe(1);
      expect(await pa).toBe(2); // present → returns current, no overwrite
      expect(await rm).toBe(2);
      expect(await cas).toBe(true);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('batch writes land in Java reads', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const bk = `${NS}bb`;
    const mp = `${NS}bm`;
    const al = `${NS}ba`;
    const li = `${NS}bl`;
    const st = `${NS}bs`;
    const zs = `${NS}bz`;
    const qu = `${NS}bq`;
    try {
      await cleanExact(r, bk, mp, al, li, st, zs, qu);
      const batch = r.createBatch();
      batch.getBucket<string>(bk).set('batch-v');
      batch.getMap<string, number>(mp).put('mk', 42);
      batch.getAtomicLong(al).addAndGet(7);
      batch.getList<string>(li).add('lx');
      batch.getSet<string>(st).add('sx');
      batch.getScoredSortedSet<string>(zs).add(3, 'zx');
      batch.getQueue<string>(qu).offer('qx');
      await batch.execute();
      expect(await probe(`bucket-get ${bk}`)).toBe('BUCKET_GET:batch-v');
      expect(await probe(`map-get ${mp} mk`)).toBe('MAP_GET:42');
      expect(await probe(`atomic-get ${al}`)).toBe('ATOMIC:7');
      expect(await probe(`list-read ${li}`)).toBe('LIST:[lx]');
      expect(await probe(`set-read ${st}`)).toBe('SET:[sx]');
      expect(await probe(`zset-score ${zs} zx`)).toBe('ZSCORE:3.0');
      expect(await probe(`bq-take ${qu} 5000 ${qu}:take-result`)).toBe('GOT:qx');
    } finally {
      await r.shutdown();
    }
  }, 180000);
});
