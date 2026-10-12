/**
 * Phase 6 live interop: MapCache events, Stream pending/ack, DelayedQueue
 * contains/remove of foreign entries, RateLimiter release both directions,
 * TimeSeries labels + TTL, BitSet BITOP, HLL merge, MapCacheNative expiry,
 * Buckets, Redis Functions, ReliableTopic ordering. Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import Redis from 'ioredis';
import { Redisson } from '../src';

const execAsync = promisify(exec);
const NS = 'TsJavaT:p6:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const POM = path.join(ROOT, 'interop', 'pom.xml');
const MVN = process.env.MVN ?? (process.platform === 'win32' ? 'C:\\\\javas\\\\apache-maven-3.9.6-bin\\\\apache-maven-3.9.6\\\\bin\\\\mvn.cmd' : 'mvn');

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

async function probe(args: string, timeoutMs = 120000): Promise<string> {
  const cmd = `"${MVN}" -f "${POM}" -q compile exec:java "-Dexec.args=${args}"`;
  const { stdout } = await execAsync(cmd, { timeout: timeoutMs, cwd: ROOT });
  const lines = stdout.split('\n').map((l) => l.trim()).filter(Boolean);
  return [...lines].reverse().find((l) => !l.startsWith('WARNING') && !l.startsWith('SLF4J')) ?? '';
}

async function cleanPattern(r: Redisson, pattern: string): Promise<void> {
  const keys = await r.getKeys().getKeys(pattern, 500);
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();

describe('live interop phase 6: mc-events/stream-ack/dq-remove/rate-release/ts-label/bitop/hll/mcnative/buckets/fn/order', () => {
  it('mapcache events reach Java listeners', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const name = `${NS}mcev`;
      const resKey = `${name}:events`;
      const readyKey = `${resKey}:ready`;
      await cleanPattern(r, `${name}*`);
      const listenP = execAsync(
        `"${MVN}" -f "${POM}" -q compile exec:java "-Dexec.args=mc-listen ${name} 25000 ${resKey}"`,
        { timeout: 90000, cwd: ROOT },
      ).then((x) => x.stdout, () => '');
      // wait for Java subscriptions (deterministic; fixed sleeps race JVM boot)
      let ready = false;
      for (let i = 0; i < 450 && !ready; i++) {
        await new Promise((x) => setTimeout(x, 200));
        ready = ((await r.redis.exists(readyKey)) as number) > 0;
      }
      expect(ready).toBe(true);
      const mc = r.getMapCache<string, string>(name);
      await mc.put('ek', 'ev1'); // created
      await mc.put('ek', 'ev2'); // updated
      await mc.remove('ek'); // removed
      await mc.put('ex', 'exv', 1200); // expired (no touch)
      await listenP;
      const got = await r.getBucket<string>(resKey).get();
      expect(got).toContain('C:ek=ev1');
      expect(got).toContain('U:ek=ev2');
      expect(got).toContain('R:ek=ev2');
      expect(got).toContain('X:ex=exv');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('stream pending tracks Java reads, TS ack clears', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const s = `${NS}stream:ack`;
      await cleanPattern(r, `${s}*`);
      expect(await probe(`stream-group-create ${s} gack`)).toBe('GROUP_CREATED');
      const st = r.getStream(s);
      await st.add({ n: 'one' });
      await st.add({ n: 'two' });
      const resKey = `${s}:read-result`;
      await cleanExact(r, resKey);
      expect(await probe(`stream-group-read ${s} gack jc ${resKey}`)).toMatch(/STREAM_GOT:/);
      const pend1: any = await (r.redis as any).xpending(s, 'gack');
      const count1 = Array.isArray(pend1) ? Number(pend1[0]) : 0;
      expect(count1).toBe(2);
      const detail: any[] = await (r.redis as any).xpending(s, 'gack', '-', '+', 10);
      const ids = detail.map((e: any) => e[0] as string);
      expect(ids.length).toBe(2);
      expect(await st.ack('gack', ...ids)).toBe(2);
      const pend2: any = await (r.redis as any).xpending(s, 'gack');
      expect(Array.isArray(pend2) ? Number(pend2[0]) : -1).toBe(0);
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('delayed remove deletes foreign packed entries', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const dest = `${NS}dq:rm`;
      await cleanExact(r, dest, `redisson_delay_queue_timeout:{${dest}}`, `redisson_delay_queue:{${dest}}`);
      expect(await probe(`dq-offer ${dest} gone-item 8000`)).toBe('OFFERED');
      const dq = r.getDelayedQueue<string>(dest);
      expect(await dq.contains('gone-item')).toBe(true);
      expect(await dq.remove('gone-item')).toBe(true);
      expect(await dq.contains('gone-item')).toBe(false);
      expect(await probe(`dq-poll ${dest} 3000`)).toBe('EMPTY');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('rate release returns permits on both sides', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ratelimit:rel`;
    try {
      await cleanExact(r, name, `{${name}}:value`, `{${name}}:permits`);
      const rl = r.getRateLimiter(name);
      await rl.setRate('OVERALL', 2, 60000);
      expect(await rl.tryAcquire(2)).toBe(true);
      expect(await rl.tryAcquire(1)).toBe(false);
      await rl.release(2);
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
      expect(await probe(`rate-try ${name} 1`)).toBe('true');
      expect(await probe(`rate-try ${name} 1`)).toBe('false');
      await rl.release(2);
      expect(await rl.tryAcquire(2)).toBe(true);
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('timeseries labels both directions, TTL expires', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts:lbl`;
    try {
      await cleanExact(r, name, `redisson__ts_ttl:{${name}}`, `redisson__ts_seq:{${name}}`);
      const ts = Date.now();
      expect(await probe(`ts-add-label ${name} ${ts} jlv jlbl`)).toBe('TS_LBL_ADDED');
      const back = await r.getTimeSeries<string>(name).range(ts - 5000, ts + 5000);
      const hit = back.find((e) => e.value === 'jlv');
      expect(hit).toBeDefined();
      expect(hit!.timestamp).toBe(ts);
      expect(hit!.label).toBe('jlbl');

      const ts2 = ts + 30000;
      await r.getTimeSeries<string>(name).add(ts2, 'ttl-v', 2000, 'ttl-lbl');
      expect((await r.getTimeSeries<string>(name).range(ts2 - 1000, ts2 + 1000)).length).toBe(1);
      await new Promise((x) => setTimeout(x, 2600));
      expect(await r.getTimeSeries<string>(name).range(ts2 - 1000, ts2 + 1000)).toEqual([]);
      const line = await probe(`ts-range ${name} 0 9999999999999`);
      expect(line).not.toContain('ttl-v');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('bitop and hll merge across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const a = `${NS}bits:a`;
      const b = `${NS}bits:b`;
      await cleanExact(r, a, b);
      const ba = r.getBitSet(a);
      const bb = r.getBitSet(b);
      await ba.set(1, true);
      await ba.set(2, true);
      await bb.set(2, true);
      await bb.set(3, true);
      await ba.and(b);
      expect(await probe(`bitset-get ${a} 2`)).toBe('BIT:1');
      expect(await probe(`bitset-get ${a} 1`)).toBe('BIT:0');
      expect(await probe(`bitset-get ${a} 3`)).toBe('BIT:0');

      const h1 = `${NS}hll:m1`;
      const h2 = `${NS}hll:m2`;
      const hd = `${NS}hll:md`;
      await cleanExact(r, h1, h2, hd);
      await probe(`hll-add ${h1} 1,2,3`);
      await probe(`hll-add ${h2} 4,5`);
      await r.getHyperLogLog(hd).mergeWith(h1, h2);
      expect(await r.getHyperLogLog(hd).count()).toBe(5);
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('mcnative TTL expiry, buckets, functions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const mcn = `${NS}mcn:exp`;
      await cleanExact(r, mcn);
      expect(await probe(`mcnative-put ${mcn} ek ev 5000`)).toBe('MCN_SET');
      expect(await r.getMapCacheNative<string, string>(mcn).get('ek')).toBe('ev');
      await new Promise((x) => setTimeout(x, 5500));
      expect(await r.getMapCacheNative<string, string>(mcn).get('ek')).toBeNull();

      const bk = `${NS}bk:shared`;
      await cleanExact(r, `${bk}:a`, `${bk}:b`);
      await r.getBuckets().set({ [`${bk}:a`]: 'bva', [`${bk}:b`]: 9 });
      expect(await probe(`buckets-get ${bk}:a ${bk}:b`)).toBe(`BUCKETS:${bk}:a=bva,${bk}:b=9`);

      const lib = `#!lua name=p6lib\nredis.register_function('p6answer', function() return 42; end)`;
      await (r.redis as any).call('FUNCTION', 'LOAD', 'REPLACE', lib);
      expect(await r.getFunction().callFn('p6answer', [], [])).toBe(42);
      expect(await probe(`fn-call p6answer`)).toBe('FN_RES:42');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('reliable topic preserves order Java to TS', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const t = `${NS}rt:order`;
      await cleanPattern(r, `${t}*`);
      const rt = r.getReliableTopic<string>(t);
      const got: string[] = [];
      const lid = await rt.addListener((msg) => {
        got.push(String(msg));
      });
      await new Promise((x) => setTimeout(x, 1500));
      expect(await probe(`rt-publish ${t} ord-1`)).toBe('PUBLISHED');
      expect(await probe(`rt-publish ${t} ord-2`)).toBe('PUBLISHED');
      expect(await probe(`rt-publish ${t} ord-3`)).toBe('PUBLISHED');
      for (let i = 0; i < 150 && got.length < 3; i++) await new Promise((x) => setTimeout(x, 100));
      await rt.removeListener(lid);
      expect(got).toEqual(['ord-1', 'ord-2', 'ord-3']);
    } finally {
      await r.shutdown();
    }
  }, 180000);
});
