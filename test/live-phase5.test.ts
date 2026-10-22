/**
 * Phase 5 live interop: PatternTopic, ShardedTopic, BlockingQueue handoff,
 * CountDownLatch open, Geo/BitSet/HLL cross-read, SetCache TTL, MapCache
 * maxIdle refresh, MapCacheNative, reentrant holdcount, Keys/Script,
 * VectorSet sharing. Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe';
import { suffixName } from '../src/util';

const NS = 'TsJavaT:p5:';
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


async function cleanPattern(r: Redisson, pattern: string): Promise<void> {
  const keys = await r.getKeys().getKeys(pattern, 500);
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 5: pattern/sharded/blocking/latch/geo/bits/hll/cache/reenter/keys/vector', () => {
  it('pattern topic delivers both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const pat = `${NS}pat:*`;
      const chan = `${NS}pat:chan`;
      const resKey = `${NS}pat:listen-result`;
      await cleanExact(r, resKey);
      const listenP = probeBg(`pattern-listen ${pat} 15000 ${resKey}`).catch(() => []);
      // wait for Java psubscribe registration
      let npat = 0;
      for (let i = 0; i < 200 && npat === 0; i++) {
        await new Promise((x) => setTimeout(x, 100));
        try {
          npat = Number(await (r.redis as any).pubsub('NUMPAT'));
        } catch {
          npat = 1;
        }
      }
      expect(npat).toBeGreaterThan(0);
      await r.getTopic<string>(chan).publish('p-msg-ts');
      await listenP;
      expect(await r.getBucket<string>(resKey).get()).toBe(`GOT:${chan}=p-msg-ts`);

      // Java → TS
      const got: { ch: string; msg: string }[] = [];
      const pt = r.getPatternTopic<string>(pat);
      const lid = await pt.addListener((ch, msg) => {
        got.push({ ch, msg: String(msg) });
      });
      await new Promise((x) => setTimeout(x, 500));
      await probe(`topic-publish ${chan} p-msg-java`);
      for (let i = 0; i < 100 && got.length === 0; i++) await new Promise((x) => setTimeout(x, 100));
      await pt.removeListener(lid);
      expect(got.length).toBeGreaterThan(0);
      expect(got[0].ch).toBe(chan);
      expect(got[0].msg).toBe('p-msg-java');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('sharded topic delivers both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const tj = `${NS}sht:tj`;
      const resKey = `${tj}:listen-result`;
      await cleanExact(r, resKey);
      const listenP = probeBg(`sharded-listen ${tj} 15000 ${resKey}`).catch(() => []);
      await new Promise((x) => setTimeout(x, 2500)); // resident probe + ssubscribe
      await r.getShardedTopic<string>(tj).publish('s-msg-ts');
      await listenP;
      expect(await r.getBucket<string>(resKey).get()).toBe('GOT:s-msg-ts');

      // Java → TS (exercises smessage path fixed in RShardedTopic)
      const jt = `${NS}sht:jt`;
      const got: string[] = [];
      const st = r.getShardedTopic<string>(jt);
      await st.addListener((m) => {
        got.push(String(m));
      });
      await new Promise((x) => setTimeout(x, 500));
      await probe(`sharded-publish ${jt} s-msg-java`);
      for (let i = 0; i < 120 && got.length === 0; i++) await new Promise((x) => setTimeout(x, 100));
      expect(got).toContain('s-msg-java');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('blocking queue hands off both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const q = `${NS}bqh:shared`;
      await cleanPattern(r, `${q}*`);
      await r.getBlockingQueue<string>(q).offer('h-ts');
      expect(await probe(`bq-take ${q} 10000 ${q}:take-result`)).toBe('GOT:h-ts');
      await probe(`q-offer ${q} h-java`);
      expect(await r.getBlockingQueue<string>(q).pollWithTimeout(8000)).toBe('h-java');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('countdown latch opens across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const a = `${NS}latch:a`;
      await cleanExact(r, a);
      await r.getCountDownLatch(a).trySetCount(1);
      await probe(`latch-countdown ${a}`);
      expect(await r.getCountDownLatch(a).await(5000)).toBe(true);

      const b = `${NS}latch:b`;
      await cleanExact(r, b);
      await probe(`latch-set ${b} 1`);
      await r.getCountDownLatch(b).countDown();
      expect(await probe(`latch-await ${b} 5000 ${b}:await-result`)).toBe('LATCH_OPEN');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('geo/bitset/hll cross-read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const g = `${NS}geo:shared`;
      await cleanExact(r, g);
      await probe(`geo-add ${g} 13.361389 38.115556 Palermo`);
      await probe(`geo-add ${g} 15.087269 37.502669 Catania`);
      const pos = await r.getGeo<string>(g).pos('Palermo');
      expect(pos[0]).not.toBeNull();
      expect(pos[0]!.lon).toBeCloseTo(13.361389, 3);
      expect(pos[0]!.lat).toBeCloseTo(38.115556, 3);
      const d = await r.getGeo<string>(g).dist('Palermo', 'Catania', 'm');
      expect(d).not.toBeNull();
      expect(d!).toBeGreaterThan(160000);
      expect(d!).toBeLessThan(175000);

      const g2 = `${NS}geo:ts`;
      await cleanExact(r, g2);
      await r.getGeo<string>(g2).add(10, 20, 'A');
      await r.getGeo<string>(g2).add(10.01, 20, 'B');
      const jd = await probe(`geo-dist ${g2} A B`);
      const meters = Number(jd.slice('GEO_DIST:'.length));
      expect(meters).toBeGreaterThan(900);
      expect(meters).toBeLessThan(1200);

      const bs = `${NS}bits:shared`;
      await cleanExact(r, bs);
      await r.getBitSet(bs).set(100, true);
      expect(await probe(`bitset-get ${bs} 100`)).toBe('BIT:1');
      expect(await probe(`bitset-set ${bs} 200`)).toBe('BIT_SET');
      expect(await r.getBitSet(bs).get(200)).toBe(true);
      expect(await r.getBitSet(bs).cardinality()).toBe(2);

      const h = `${NS}hll:shared`;
      await cleanExact(r, h);
      await probe(`hll-add ${h} 1,2,3,4,5`);
      expect(await r.getHyperLogLog(h).count()).toBe(5);
      await r.getHyperLogLog(h).add(6, 7);
      expect(await probe(`hll-count ${h}`)).toBe('HLL_COUNT:7');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('setcache TTL, mapcache maxIdle, mapcache-native', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const sc = `${NS}sc5:shared`;
      await cleanExact(r, sc, `redisson__idle__set:{${sc}}`, `{${sc}}:redisson_touch`);
      await probe(`setcache-add-ttl ${sc} scv 5000`);
      expect(await r.getSetCache<string>(sc).contains('scv')).toBe(true);
      await new Promise((x) => setTimeout(x, 5500));
      expect(await r.getSetCache<string>(sc).contains('scv')).toBe(false);
      // idle (TS extension): sliding window enforced + refreshed on read
      const scc = r.getSetCache<string>(sc);
      expect(await scc.add('iv', 0, 3000)).toBe(true);
      expect(await probe(`sc-contains ${sc} iv`)).toBe('SC_HAS:true');
      await new Promise((x) => setTimeout(x, 3500));
      expect(await scc.contains('iv')).toBe(false);
      expect(await scc.size()).toBe(0);

      const mc = `${NS}mc5:shared`;
      await cleanExact(r, mc, `redisson__timeout__set:{${mc}}`, `redisson__idle__set:{${mc}}`);
      // plain expiry: touch once, then hands off longer than maxIdle
      await probe(`mc-put-idle ${mc} k1 v 8000`);
      const mcc = r.getMapCache<string, string>(mc);
      expect(await mcc.get('k1')).toBe('v');
      await new Promise((x) => setTimeout(x, 8500));
      expect(await mcc.get('k1')).toBeNull();
      // refresh: every successful get pushes the deadline out by maxIdle,
      // so the final hands-off must exceed maxIdle since the LAST touch
      await probe(`mc-put-idle ${mc} k2 v 5000`);
      expect(await mcc.get('k2')).toBe('v');
      await new Promise((x) => setTimeout(x, 3000));
      expect(await mcc.get('k2')).toBe('v'); // refreshed
      await new Promise((x) => setTimeout(x, 3000));
      expect(await mcc.get('k2')).toBe('v'); // refreshed again
      await new Promise((x) => setTimeout(x, 6000));
      expect(await mcc.get('k2')).toBeNull(); // 6s > 5s since last touch

      const mcn = `${NS}mcn:shared`;
      await cleanExact(r, mcn);
      await r.getMapCacheNative<string, string>(mcn).put('tk', 'tv', 60000);
      expect(await probe(`mcnative-get ${mcn} tk`)).toBe('MCN_GET:tv');
      expect(await probe(`mcnative-put ${mcn} jk jv 60000`)).toBe('MCN_SET');
      expect(await r.getMapCacheNative<string, string>(mcn).get('jk')).toBe('jv');
      expect(await r.getMapCacheNative<string, string>(mcn).remainTTL('jk')).toBeGreaterThan(0);
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('reentrant holdcount, keys ops, script parity', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const name = `${NS}relock`;
      await cleanExact(r, name);
      const holdP = probeBg(`lock-hold-re ${name} 4000`).catch(() => []);
      let held = false;
      for (let i = 0; i < 100 && !held; i++) {
        await new Promise((x) => setTimeout(x, 100));
        held = ((await r.redis.exists(name)) as number) > 0;
      }
      expect(held).toBe(true);
      const fields = await r.redis.hgetall(name);
      expect(Object.values(fields)).toEqual(['2']); // reentrant count wire
      expect(await r.getLock(name).tryLock(0, 5000, 77)).toBe(false);
      const out = (await holdP).join('\n');
      expect(out).toContain('RE_HOLDCOUNT:2');
      expect(await r.getLock(name).tryLock(0, 5000, 77)).toBe(true);
      await r.getLock(name).unlock(77);

      const k1 = `${NS}keys:a`;
      const k2 = `${NS}keys:b`;
      await cleanExact(r, k1, k2);
      await r.getBucket<string>(k1).set('kv');
      await probe(`keys-rename ${k1} ${k2}`);
      expect(await r.getBucket<string>(k2).get()).toBe('kv');
      expect(await r.getKeys().expire(k2, 1500)).toBe(true);
      await new Promise((x) => setTimeout(x, 2000));
      expect(await r.getBucket<string>(k2).isExists()).toBe(false);

      expect(await probe(`script-eval return(40+2)`)).toBe('SCRIPT_RES:42');
      expect(await r.getScript().eval('READ_WRITE', 'return(40+2)', 'INTEGER')).toBe(42);
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('vector sets share elements', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vec:shared`;
    try {
      await cleanExact(r, name);
      await r.getVectorSet(name).add('tse', [1, 0, 0]);
      expect(await probe(`vector-contains ${name} tse`)).toBe('VEC_HAS:true');
      expect(await probe(`vector-info ${name}`)).toBe('VEC_SIZE:1');
      expect(await probe(`vector-add ${name} je 0,1,0`)).toBe('VEC_ADDED');
      expect(await r.getVectorSet(name).card()).toBe(2);
      const sim = await r.getVectorSet(name).sim('tse', 2);
      expect(new Set(sim)).toEqual(new Set(['tse', 'je']));
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('list multimap preserves order across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lmm:shared`;
    try {
      // NOTE: brace-prefixed collection keys ({name}:<id>) are invisible to
      // bare-name SCAN patterns AND are reused deterministically — stale
      // collections would be re-appended (same as Java). clear() discovers
      // collections THROUGH the HASH, so it must run BEFORE pattern cleanup
      // wipes the mapping (reverse order silently no-ops).
      await r.getListMultimap<string, string>(name).clear();
      await cleanPattern(r, `${name}*`);
      const lm = r.getListMultimap<string, string>(name);
      await lm.put('k', 'v1');
      await lm.put('k', 'v2');
      await lm.put('k', 'v3');
      expect(await lm.get('k')).toEqual(['v1', 'v2', 'v3']);
      // collection key must be suffixName-shaped: {name}:{id}
      const ids = await r.redis.hgetall(name);
      const field = '"k"';
      const collKey = suffixName(name, ids[field]);
      expect(await r.redis.llen(collKey)).toBe(3);
    } finally {
      await r.shutdown();
    }
  }, 60000);
});
