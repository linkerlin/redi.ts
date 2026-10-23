/**
 * Live interop vs Java Redisson 4.6.1 (needs real Redis at 127.0.0.1:6379
 * and the interop probe: `mvn -f interop/pom.xml -q compile`).
 *
 * Direction A (Java → TS): probe `write-java TsJavaT:` then TS reads.
 * Direction B (TS → Java): TS writes `TsJavaT:t:` then probe `read-ts`.
 *
 * Skips when Redis or mvn/probe is unavailable.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import Redis from 'ioredis';
import { Redisson } from '../src';

const execAsync = promisify(exec);
const NS = 'TsJavaT:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
// repo root = parent of test/ (works regardless of vitest cwd)
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const POM = path.join(ROOT, 'interop', 'pom.xml');

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

const MVN = process.env.MVN ?? (process.platform === 'win32' ? 'C:\\\\javas\\\\apache-maven-3.9.6-bin\\\\apache-maven-3.9.6\\\\bin\\\\mvn.cmd' : 'mvn');

async function mvnProbe(args: string): Promise<{ ok: boolean; out: string }> {
  // .cmd needs a shell on Windows; quote the -D arg to keep the space.
  const cmd = `"${MVN}" -f "${POM}" -q compile exec:java "-Dexec.args=${args}"`;
  // TsProbe reads REDIS_HOST/REDIS_PORT (not REDIS_URL) — map them so the
  // fixture JVM hits the same server the TS client does
  let redisEnv: Record<string, string> = {};
  try {
    const u = new URL(REDIS_URL);
    redisEnv = { REDIS_HOST: u.hostname, REDIS_PORT: u.port || '6379' };
  } catch { /* keep defaults */ }
  try {
    const { stdout, stderr } = await execAsync(cmd, { timeout: 300000, cwd: ROOT, env: { ...process.env, ...redisEnv } });
    return { ok: true, out: `${stdout}\n${stderr}` };
  } catch (e: any) {
    const out = String((e as any)?.stdout ?? '') + String((e as any)?.stderr ?? '') + String((e as any)?.message ?? e);
    console.error('mvn probe failed:', out.slice(0, 2000));
    return { ok: false, out };
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop TS <-> Java Redisson (real Redis)', () => {
  beforeAll(async () => {
    if (!hasRedis) return;
    // Java writes canonical fixtures for TS to read.
    await mvnProbe(`write-java ${NS}`);
  }, 60000); // JVM/Maven cold start under parallel workers can exceed Vitest's 10s default

  it('A: TS reads Java-written fixtures', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const J = `${NS}j:`;
    try {
      expect(await r.getBucket(`${J}bucket:str`).get()).toBe('hello from java');
      expect(await r.getBucket(`${J}bucket:int`).get()).toBe(42);
      expect(await r.getBucket(`${J}bucket:long`).get()).toBe(3000000000);
      expect(await r.getBucket(`${J}bucket:double`).get()).toBeCloseTo(3.14);
      expect(await r.getBucket(`${J}bucket:bool`).get()).toBe(true);
      expect(await r.getBucket(`${J}bucket:list`).get()).toEqual([1, 2, 3]);
      expect(await r.getBucket(`${J}bucket:map`).get()).toEqual({ a: 1, b: 'two' });

      expect(await r.getAtomicLong(`${J}atomiclong`).get()).toBe(100);
      expect(await r.getAtomicDouble(`${J}atomicdouble`).get()).toBeCloseTo(2.71);

      const m = r.getMap<string, any>(`${J}map`);
      expect(await m.get('str')).toBe('value');
      expect(await m.get('int')).toBe(42);
      expect(await m.get('long')).toBe(3000000000);
      expect(await m.get('double')).toBeCloseTo(3.14);
      expect(await m.get('bool')).toBe(true);
      expect(await m.get('list')).toEqual([1, 2, 3]);

      const s = r.getSet<any>(`${J}set`);
      expect(await s.contains('alpha')).toBe(true);
      expect(await s.contains(42)).toBe(true);
      expect(await s.size()).toBe(4);

      expect(await r.getList<any>(`${J}list`).readAll()).toEqual(['one', 2, 3.14, true]);
      expect(await r.getQueue<any>(`${J}queue`).peek()).toBe('q1');
      expect(await r.getDeque<any>(`${J}deque`).readAll()).toEqual(['d1', 'd2']);

      const z = r.getScoredSortedSet<any>(`${J}zset`);
      expect(await z.getScore('mid')).toBeCloseTo(2.0);
      expect(await z.range(0, -1)).toEqual(['low', 'mid', 'high']);

      const lex = r.getLexSortedSet(`${J}lex`);
      expect(await lex.contains('banana')).toBe(true);
      expect(await lex.size()).toBe(3);

      expect(await r.getBlockingQueue<any>(`${J}bqueue`).peek()).toBe('bq1');

      const bf = r.getBloomFilter(`${J}bloom`);
      expect(await bf.contains('maybe1')).toBe(true);
      expect(await bf.contains('definitely-not-here-xyz')).toBe(false);

      expect(await r.getMapCache<string, any>(`${J}mapcache`).get('k1')).toBe('v1');
      expect(await r.getSetCache<any>(`${J}setcache`).contains('s1')).toBe(true);

      expect(await r.getCountDownLatch(`${J}latch`).getCount()).toBe(3);
      expect(await r.getSemaphore(`${J}sem`).availablePermits()).toBe(5);
      expect(await r.getHyperLogLog(`${J}hll`).count()).toBe(5);

      const geo = r.getGeo<string>(`${J}geo`);
      expect(await geo.size()).toBe(2);

      const st = r.getStream(`${J}stream`);
      expect(await st.size()).toBe(1);
      const entries = await st.read(10, '0');
      expect(entries.length).toBe(1);
      expect(Object.values(entries[0].fields)).toContain('value');
    } finally {
      await r.shutdown();
    }
  });

  it('B: Java reads TS-written fixtures', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const T = `${NS}t:`;
    try {
      // clean TS namespace (raw del, no codec)
      const keys = await r.getKeys().getKeys(`${NS}t:*`, 500);
      if (keys.length > 0) await r.getKeys().delete(...keys);

      await r.getBucket(`${T}bucket:str`).set('hello from ts');
      await r.getBucket(`${T}bucket:int`).set(7);
      await r.getBucket(`${T}bucket:long`).set(5000000000);
      await r.getBucket(`${T}bucket:double`).set(1.41);
      await r.getBucket(`${T}bucket:bool`).set(false);
      await r.getBucket<number[]>(`${T}bucket:list`).set([4, 5, 6]);

      await r.getAtomicLong(`${T}atomiclong`).set(200);
      await r.getAtomicDouble(`${T}atomicdouble`).set(6.28);

      const m = r.getMap<string, any>(`${T}map`);
      await m.put('name', 'redi.ts');
      await m.put('n', 1);
      await m.put('flag', true);

      const s = r.getSet<any>(`${T}set`);
      await s.add('x', 99, 'y');

      const l = r.getList<any>(`${T}list`);
      await l.add('a', 'b', 'c');

      const q = r.getQueue<any>(`${T}queue`);
      await q.offer('qa');
      await q.offer('qb');

      const dq = r.getDeque<any>(`${T}deque`);
      await dq.addFirst('d_first');
      await dq.addLast('d_last');

      const lex = r.getLexSortedSet(`${T}lex`);
      await lex.add('tx1');
      await lex.add('tx2');

      await r.getBlockingQueue<any>(`${T}bqueue`).offer('tbq1');

      const z = r.getScoredSortedSet<any>(`${T}zset`);
      await z.add(10, 'a');

      const bf = r.getBloomFilter(`${T}bloom`);
      await bf.tryInit(1000, 0.03);
      await bf.add('tsbloom1');

      await r.getMapCache<string, any>(`${T}mapcache`).put('tk', 'tv');
      await r.getSetCache<any>(`${T}setcache`).add('tsc1');

      await r.getCountDownLatch(`${T}latch`).trySetCount(4);
      await r.getSemaphore(`${T}sem`).trySetPermits(8);

      await r.getHyperLogLog(`${T}hll`).add(7, 8, 9);

      await r.getGeo<string>(`${T}geo`).add(13.361389, 38.115556, 'TPalermo');

      await r.getStream(`${T}stream`).add({ tfield: 'tvalue' });
    } finally {
      await r.shutdown();
    }

    const res = await mvnProbe(`read-ts ${NS}`);
    console.log(res.out);
    expect(res.out).toContain('[PASS] bucket:str');
    const fails = res.out.split('\n').filter((l) => l.startsWith('[FAIL]') || l.startsWith('[ERROR]'));
    expect(fails).toEqual([]);
  }, 300000);
});
