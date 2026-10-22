/**
 * Phase 9 live interop: MultiLock/RedLock orchestration, LocalCachedMap
 * strategies, MapCache/SetCache fast paths, ZSet/Map/Atomic/Bucket/Keys/
 * ScriptSHA/Vector extras, CRC16 slot parity (static), mock contention.
 * Needs real Redis + probe (except the static block).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import RedisMock from 'ioredis-mock';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe';
import { prefixName, suffixName } from '../src/util';

const NS = 'TsJavaT:p9:';
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

/** CRC16-CCITT (poly 0x1021), the Redis Cluster slot hash. */
function crc16(s: string): number {
  const b = Buffer.from(s, 'utf8');
  let crc = 0;
  for (const byte of b) {
    crc ^= byte << 8;
    for (let i = 0; i < 8; i++) {
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
    }
  }
  return crc;
}

function slot(key: string): number {
  const m = /\{([^}]*)\}/.exec(key);
  const tag = m && m[1].length > 0 ? m[1] : key;
  return crc16(tag) % 16384;
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('phase 9 static: slot parity + mock contention (no Redis needed)', () => {
  it('every companion key lands in the same slot as its main key', () => {
    const cases: [string, string][] = [
      ['mylock', prefixName('redisson_lock__channel', 'mylock')],
      ['mylock', prefixName('redisson_lock_queue', 'mylock')],
      ['mylock', prefixName('redisson_lock_timeout', 'mylock')],
      ['mylock', prefixName('redisson_rwlock', 'mylock')],
      ['mylock', prefixName('redisson_lock_token', 'mylock')],
      ['mysem', prefixName('redisson_sc', 'mysem')],
      ['myq', prefixName('redisson_delay_queue', 'myq')],
      ['myq', prefixName('redisson_delay_queue_timeout', 'myq')],
      ['myq', prefixName('redisson_delay_queue_channel', 'myq')],
      ['mymc', prefixName('redisson__timeout__set', 'mymc')],
      ['mymc', prefixName('redisson__idle__set', 'mymc')],
      ['mymc', prefixName('redisson__map_cache__last_access__set', 'mymc')],
      ['mymc', suffixName('mymc', 'redisson_options')],
      ['mymc', suffixName('mymc', 'config')],
      ['myrl', suffixName('myrl', 'value')],
      ['myrl', suffixName('myrl', 'permits')],
      ['myadd', suffixName('myadd', 'adder-topic')],
      ['myadd', suffixName('myadd', 'reqid123:counter')],
      ['myadd', suffixName('myadd', 'reqid123:semaphore')],
      ['myts', prefixName('redisson__ts_ttl', 'myts')],
      ['myts', prefixName('redisson__ts_seq', 'myts')],
      ['myrb', prefixName('redisson_rb', 'myrb')],
      ['mybq', prefixName('redisson_bqs', 'mybq')],
      ['mymm', suffixName('mymm', 'someid')],
      ['mymm', suffixName('mymm', 'redisson_set_multimap_ttl')],
    ];
    for (const [main, comp] of cases) {
      expect(slot(comp)).toBe(slot(main));
    }
  });

  it('mock: exactly one winner under parallel contention', async () => {
    const r = Redisson.create({ client: new RedisMock() as any });
    try {
      const results = await Promise.all(
        Array.from({ length: 20 }, (_, i) => r.getLock('contention').tryLock(0, 30000, 100 + i)),
      );
      expect(results.filter(Boolean).length).toBe(1);
      const winner = 100 + results.findIndex(Boolean);
      await r.getLock('contention').unlock(winner);
      expect(await r.getLock('contention').tryLock(0, 30000, 7)).toBe(true);
      await r.getLock('contention').unlock(7);
    } finally {
      await r.shutdown();
    }
  });
});

describe('live interop phase 9: multilock/redlock/lcm-strategies/cache-paths/misc', () => {
  it('multilock all-or-nothing + rollback visible in Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const m1 = `${NS}ml:a`;
    const m2 = `${NS}ml:b`;
    try {
      await cleanExact(r, m1, m2);
      const ml = r.newMultiLock(r.getLock(m1), r.getLock(m2));
      expect(await ml.tryLock(1, 30000)).toBe(true);
      expect(await probe(`lock-try ${m1}`)).toBe('CONTENDED');
      expect(await probe(`lock-try ${m2}`)).toBe('CONTENDED');
      await ml.unlock(1);
      expect(await probe(`lock-try ${m1}`)).toBe('ACQUIRED');
      expect(await probe(`lock-try ${m2}`)).toBe('ACQUIRED');

      // rollback: Java holds m1 → multilock fails → m2 must NOT stay held
      const holdP = probeBg(`lock-hold ${m1} 5000`).catch(() => []);
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = ((await r.redis.exists(m1)) as number) > 0;
      }
      expect(held).toBe(true);
      expect(await r.newMultiLock(r.getLock(m1), r.getLock(m2)).tryLock(2, 30000)).toBe(false);
      expect(await r.getLock(m2).tryLock(0, 5000, 3)).toBe(true); // rolled back
      await r.getLock(m2).unlock(3);
      await holdP;
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('redlock quorum succeeds with one member held', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const names = [`${NS}rl:a`, `${NS}rl:b`, `${NS}rl:c`];
    try {
      await cleanExact(r, ...names);
      const holdP = probeBg(`lock-hold ${names[0]} 6000`).catch(() => []);
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = ((await r.redis.exists(names[0])) as number) > 0;
      }
      expect(held).toBe(true);
      // 2/3 quorum reachable despite Java holding A
      const rl = r.newRedLock(r.getLock(names[0]), r.getLock(names[1]), r.getLock(names[2]));
      expect(await rl.tryLock(30000, 8)).toBe(true);
      expect(await probe(`lock-try ${names[1]}`)).toBe('CONTENDED');
      expect(await probe(`lock-try ${names[2]}`)).toBe('CONTENDED');
      await rl.unlock(8);
      await holdP;
      expect(await probe(`lock-try ${names[1]}`)).toBe('ACQUIRED');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('lcm UPDATE carries values, INVALIDATE refetches', async () => {
    if (!hasRedis) return;
    const a = Redisson.create({ address: REDIS_URL });
    const b = Redisson.create({ address: REDIS_URL });
    try {
      const u = `${NS}lcm:update`;
      await cleanExact(a, u, `${u}:inval`);
      const au = a.getLocalCachedMap<string, string>(u, { syncStrategy: 'UPDATE' });
      const bu = b.getLocalCachedMap<string, string>(u, { syncStrategy: 'UPDATE' });
      // deterministic readiness: both listeners subscribed before any write
      // (fixed sleeps race client boot; missed broadcasts never redeliver)
      for (let i = 0; i < 200; i++) {
        const n: any = await a.redis.pubsub('NUMSUB', `${u}:inval`);
        if (Number(n?.[1] ?? n) >= 2) break;
        await new Promise((x) => setTimeout(x, 100));
      }
      await au.put('k', 'v1');
      expect(await bu.get('k')).toBe('v1');
      await au.put('k', 'v2');
      await new Promise((x) => setTimeout(x, 1200));
      await a.redis.del(u); // kill backend: B must answer from broadcast, not refetch
      expect(await bu.get('k')).toBe('v2');
      await au.destroy();
      await bu.destroy();

      const iv = `${NS}lcm:invalidate`;
      await cleanExact(a, iv, `${iv}:inval`);
      const ai = a.getLocalCachedMap<string, string>(iv); // default INVALIDATE
      const bi = b.getLocalCachedMap<string, string>(iv);
      for (let i = 0; i < 200; i++) {
        const n: any = await a.redis.pubsub('NUMSUB', `${iv}:inval`);
        if (Number(n?.[1] ?? n) >= 2) break;
        await new Promise((x) => setTimeout(x, 100));
      }
      await ai.put('k', 'v1');
      expect(await bi.get('k')).toBe('v1');
      await ai.put('k', 'v2');
      await new Promise((x) => setTimeout(x, 1200));
      await a.redis.del(iv); // kill backend: B invalidated → miss → null
      expect(await bi.get('k')).toBeNull();
      await ai.destroy();
      await bi.destroy();
    } finally {
      await a.shutdown();
      await b.shutdown();
    }
  }, 120000);

  it('mapcache/setcache fast paths cross-read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const mc = `${NS}mc:fast`;
      await cleanExact(r, mc, `redisson__timeout__set:{${mc}}`, `redisson__idle__set:{${mc}}`);
      const mcc = r.getMapCache<string, string>(mc);
      expect(await mcc.putIfAbsent('k', 'v1')).toBeNull();
      expect(await mcc.putIfAbsent('k', 'v2')).toBe('v1');
      expect(await probe(`mc-get ${mc} k`)).toBe('MC_GET:v1');
      expect(await mcc.fastPut('k2', 'w')).toBe(true);
      expect(await probe(`mc-size ${mc}`)).toBe('MC_SIZE:2');
      expect(await mcc.remove('k')).toBe('v1');
      expect(await probe(`mc-get ${mc} k`)).toBe('MC_NULL');

      const sc = `${NS}sc:fast`;
      await cleanExact(r, sc, `redisson__idle__set:{${sc}}`);
      const scc = r.getSetCache<string>(sc);
      expect(await scc.add('a')).toBe(true);
      expect(await probe(`sc-contains ${sc} a`)).toBe('SC_HAS:true');
      expect(await scc.remove('a')).toBe(true);
      expect(await probe(`sc-contains ${sc} a`)).toBe('SC_HAS:false');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('zset/map/atomic/bucket/keys/script-sha/vector extras', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const zs = `${NS}zs:x`;
      await cleanExact(r, zs);
      const z = r.getScoredSortedSet<string>(zs);
      await z.add(1, 'a');
      await z.add(2, 'b');
      await z.add(3, 'c');
      expect(await z.revRank('a')).toBe(2);
      expect(await z.rangeWithScores(0, 1)).toEqual([
        { value: 'a', score: 1 },
        { value: 'b', score: 2 },
      ]);
      expect(await z.rangeByScore(2, 3)).toEqual(['b', 'c']);
      expect(await z.pollLast()).toEqual(['c']);
      expect(await probe(`zset-score ${zs} c`)).toBe('ZSCORE:null');

      const al = `${NS}at:x`;
      await cleanExact(r, al);
      await probe(`atomic-get ${al}`); // ensure probe path warm (returns ATOMIC:0)
      await r.getAtomicLong(al).addAndGet(41);
      expect(await probe(`atomic-get ${al}`)).toBe('ATOMIC:41');

      const bk = `${NS}bk:ttl`;
      await cleanExact(r, bk);
      await r.getBucket<string>(bk).set('temp', 60000);
      expect(await probe(`bucket-get ${bk}`)).toBe('BUCKET_GET:temp');

      const ks = `${NS}keys:copy`;
      const kd = `${NS}keys:copydst`;
      await cleanExact(r, ks, kd);
      await r.getBucket<string>(ks).set('cp');
      expect(await r.getKeys().copy(ks, kd)).toBe(true);
      expect(await probe(`bucket-get ${kd}`)).toBe('BUCKET_GET:cp');

      const sha = await r.getScript().scriptLoad('return(40+2)');
      expect(await r.getScript().evalSha(sha, 'INTEGER')).toBe(42);

      const vs = `${NS}vec:x`;
      await cleanExact(r, vs);
      await r.getVectorSet(vs).add('e1', [1, 0]);
      await r.getVectorSet(vs).add('e2', [0, 1]);
      expect(await r.getVectorSet(vs).remove('e1')).toBe(true);
      expect(await probe(`vector-contains ${vs} e1`)).toBe('VEC_HAS:false');
      expect(await probe(`vector-contains ${vs} e2`)).toBe('VEC_HAS:true');
    } finally {
      await r.shutdown();
    }
  }, 180000);
});
