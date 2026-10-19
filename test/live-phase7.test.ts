/**
 * Phase 7 live interop: BinaryStream bytes, ReliableTopic fanout,
 * MapCache maxSize eviction, MultimapCache expiry, advanced zset/map/atomic,
 * Stream trim, Function reload, BoundedBlockingQueue blocking take,
 * DelayedQueue deadline FIFO. Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe';

const NS = 'TsJavaT:p7:';
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

describe('live interop phase 7: bstream/fanout/mc-maxsize/mmcache/zset+/trim/fn-reload/bbq-take/dq-fifo', () => {
  it('binary stream shares raw bytes', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bs:shared`;
    try {
      await cleanExact(r, name);
      await r.getBinaryStream(name).set(Buffer.from('hello-bytes-ts', 'utf8'));
      expect(await probe(`bstream-get ${name}`)).toBe('BS_GET:hello-bytes-ts');
      expect(await probe(`bstream-set ${name} hello-bytes-java`)).toBe('BS_SET');
      expect((await r.getBinaryStream(name).get())?.toString('utf8')).toBe('hello-bytes-java');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('reliable topic fans out to two groups', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      // TS publishes once; Java's two groups must BOTH get it
      const tj = `${NS}rt:fanout-tj`;
      const resKey = `${tj}:fanout-result`;
      await cleanExact(r, resKey);
      const fanoutP = probeBg(`rt-fanout ${tj} 15000 ${resKey}`).catch(() => []);
      await new Promise((x) => setTimeout(x, 2500)); // resident probe + group creation
      await r.getReliableTopic<string>(tj).publish('fan-ts');
      await fanoutP;
      expect(await r.getBucket<string>(resKey).get()).toBe('FANOUT:G1:fan-ts,G2:fan-ts');

      // Java publishes once; TS's two listeners must BOTH get it
      const jt = `${NS}rt:fanout-jt`;
      await cleanPattern(r, `${jt}*`);
      const rt = r.getReliableTopic<string>(jt);
      const g1: string[] = [];
      const g2: string[] = [];
      const l1 = await rt.addListener((m) => {
        g1.push(String(m));
      });
      const l2 = await rt.addListener((m) => {
        g2.push(String(m));
      });
      await new Promise((x) => setTimeout(x, 1500));
      expect(await probe(`rt-publish ${jt} fan-java`)).toBe('PUBLISHED');
      for (let i = 0; i < 150 && (g1.length === 0 || g2.length === 0); i++) {
        await new Promise((x) => setTimeout(x, 100));
      }
      await rt.removeListener(l1);
      await rt.removeListener(l2);
      expect(g1).toEqual(['fan-java']);
      expect(g2).toEqual(['fan-java']);
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('mapcache maxSize evicts eldest, visible in Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc:maxsize`;
    try {
      await cleanExact(r, name, `redisson__timeout__set:{${name}}`, `redisson__idle__set:{${name}}`,
        `redisson__map_cache__last_access__set:{${name}}`, `${name}:redisson_options`);
      const mc = r.getMapCache<string, string>(name);
      await mc.setMaxSize(2);
      await mc.put('e1', 'v1');
      await new Promise((x) => setTimeout(x, 50));
      await mc.put('e2', 'v2');
      await new Promise((x) => setTimeout(x, 50));
      await mc.put('e3', 'v3');
      expect(await probe(`mc-size ${name}`)).toBe('MC_SIZE:2');
      expect(await probe(`mc-get ${name} e1`)).toBe('MC_NULL');
      expect(await probe(`mc-get ${name} e3`)).toBe('MC_GET:v3');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('multimapcache expiry hides entries from Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mmc:shared`;
    try {
      await r.getSetMultimapCache<string, string>(name).clear();
      await cleanPattern(r, `${name}*`);
      const mmc = r.getSetMultimapCache<string, string>(name);
      await mmc.put('k', 'v1');
      expect(await probe(`mm-get ${name} k`)).toBe('MM_GET:[v1]');
      expect(await mmc.expireKey('k', 1500)).toBe(true);
      await new Promise((x) => setTimeout(x, 2000));
      expect(await probe(`mm-get ${name} k`)).toBe('MM_GET:[]');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('zset/map/atomic advanced ops cross-read', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const zs = `${NS}zs:adv`;
      await cleanExact(r, zs);
      const z = r.getScoredSortedSet<string>(zs);
      await z.add(5, 'm5');
      await z.addIfAbsent(7, 'm7');
      expect(await probe(`zset-score ${zs} m5`)).toBe('ZSCORE:5.0');
      expect(await probe(`zset-score ${zs} m7`)).toBe('ZSCORE:7.0');
      expect(await z.addAndGetRank(1, 'm1')).toBe(0);

      const mp = `${NS}map:adv`;
      await cleanExact(r, mp);
      const m = r.getMap<string, any>(mp);
      expect(await m.putIfAbsent('pk', 10)).toBeNull();
      expect(await m.addAndGet('counter', 5)).toBe(5);
      expect(await probe(`map-get ${mp} pk`)).toBe('MAP_GET:10');
      expect(await probe(`map-get ${mp} counter`)).toBe('MAP_GET:5');

      const al = `${NS}atomic:adv`;
      await cleanExact(r, al);
      const a = r.getAtomicLong(al);
      await a.set(100);
      expect(await a.addAndGet(23)).toBe(123);
      expect(await probe(`atomic-get ${al}`)).toBe('ATOMIC:123');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('stream trim caps length for Java readers', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}stream:trim`;
    try {
      await cleanPattern(r, `${s}*`);
      const st = r.getStream(s);
      await st.add({ n: 'a' });
      await st.add({ n: 'b' });
      await st.add({ n: 'c' });
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:3');
      // ~ parity: like Redisson's default non-strict add, small streams are
      // intentionally NOT trimmed on either side — assert same observable size
      await st.add({ n: 'd' }, '*', 2);
      await st.add({ n: 'e' }, '*', 2);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:5');
      // exact trim is honored by both
      expect(await st.trim(2, false)).toBeGreaterThanOrEqual(0);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:2');
      expect(await probe(`stream-add ${s} n f`)).toMatch(/^STREAM_ADDED:/);
      expect(await st.size()).toBe(3);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('function reload replaces implementation for both', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      // NOTE: distinct library/function names from phase-6 (files run in
      // parallel workers; shared names would race REPLACE against each other).
      const lib = `#!lua name=p6lib7\nredis.register_function('p6answer7', function() return 43; end)`;
      await (r.redis as any).call('FUNCTION', 'LOAD', 'REPLACE', lib);
      expect(await r.getFunction().callFn('p6answer7', [], [])).toBe(43);
      expect(await probe(`fn-call p6answer7`)).toBe('FN_RES:43');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('bounded blocking take unblocks on TS offer', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q = `${NS}bbq:take`;
    try {
      await cleanPattern(r, `${q}*`);
      await cleanExact(r, `redisson_bqs:{${q}}`);
      expect(await r.getBoundedBlockingQueue<string>(q).trySetCapacity(1)).toBe(true);
      const takeP = probeBg(`bbq-take ${q} 15000 ${q}:take-result`).catch(() => []);
      await new Promise((x) => setTimeout(x, 2500)); // resident probe + blocking poll
      await r.getBoundedBlockingQueue<string>(q).offer('bb-ts');
      const out = (await takeP).join('\n');
      const key = await r.getBucket<string>(`${q}:take-result`).get();
      expect(`${out}\n${key}`).toContain('GOT:bb-ts');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('delayed queue delivers in deadline order', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const dest = `${NS}dq:fifo`;
    try {
      await cleanExact(r, dest, `redisson_delay_queue_timeout:{${dest}}`, `redisson_delay_queue:{${dest}}`);
      // ONE JVM for both offers: separate JVMs boot ~7s apart, which would
      // dwarf the delays and invert the intended deadline order.
      expect(await probe(`dq-offer2 ${dest} slow-item 2500 fast-item 800`)).toBe('OFFERED2');
      const dq = r.getDelayedQueue<string>(dest);
      const got: string[] = [];
      for (let i = 0; i < 120 && got.length < 2; i++) {
        const v = await dq.poll();
        if (v !== null) got.push(v);
        else await new Promise((x) => setTimeout(x, 100));
      }
      expect(got).toEqual(['fast-item', 'slow-item']);
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
