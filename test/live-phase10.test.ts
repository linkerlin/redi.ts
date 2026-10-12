/**
 * Phase 10 live interop: BlockingDeque handoff, batch Set/Atomic visibility,
 * Bucket TTL, MapCache TTL+maxIdle combined, SetCache idle presence,
 * MapCache destroy hygiene, PriorityQueue self-consistency (mock, GO_ONLY).
 * Needs real Redis + probe (except the mock test).
 */
import { describe, it, expect } from 'vitest';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import Redis from 'ioredis';
import RedisMock from 'ioredis-mock';
import { Redisson } from '../src';

const execAsync = promisify(exec);
const NS = 'TsJavaT:p10:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const POM = path.join(ROOT, 'interop', 'pom.xml');
const MVN = process.env.MVN ?? (process.platform === 'win32' ? 'C:\\javas\\apache-maven-3.9.6-bin\\apache-maven-3.9.6\\bin\\mvn.cmd' : 'mvn');

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

async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();

describe('phase 10 static: priority self-consistency on mock', () => {
  it('zset-score priority queue is FIFO by score (GO_ONLY, not Comparator)', async () => {
    const r = Redisson.create({ client: new RedisMock() as any });
    try {
      const pq = r.getPriorityQueue<string>('pq10');
      await pq.offer('c', 3);
      await pq.offer('a', 1);
      await pq.offer('b', 2);
      expect(await pq.size()).toBe(3);
      expect(await pq.peek()).toBe('a');
      expect(await pq.poll()).toBe('a');
      expect(await pq.poll()).toBe('b');
      expect(await pq.poll()).toBe('c');
      expect(await pq.poll()).toBeNull();
    } finally {
      await r.shutdown();
    }
  });
});

describe('live interop phase 10: bdeque/batch/bucket-ttl/mc-combo/sc-idle/destroy', () => {
  it('blocking deque hands off both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q = `${NS}bdeq:shared`;
    try {
      await cleanExact(r, q);
      await probe(`q-offer ${q} from-java`);
      expect(await r.getBlockingDeque<string>(q).takeFirst()).toBe('from-java');
      const bd = r.getBlockingDeque<string>(q);
      await bd.addLast('from-ts');
      expect(await probe(`deque-read ${q}`)).toBe('DEQUE:[from-ts]');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('batch set+atomic writes are visible in Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}batch:set`;
    const al = `${NS}batch:atomic`;
    try {
      await cleanExact(r, s, al);
      const batch = r.createBatch();
      batch.pipeline.sadd(s, '"b1"', '"b2"');
      batch.pipeline.set(al, '10');
      await batch.execute();
      expect(await probe(`set-read ${s}`)).toBe('SET:[b1, b2]');
      expect(await probe(`atomic-get ${al}`)).toBe('ATOMIC:10');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('bucket TTL + mapcache ttl/idle combined visible in Java', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      const bk = `${NS}bk:ttl10`;
      await cleanExact(r, bk);
      await r.getBucket<string>(bk).set('temp10', 60000);
      expect(await probe(`bucket-get ${bk}`)).toBe('BUCKET_GET:temp10');

      const mc = `${NS}mc:combo`;
      await cleanExact(r, mc, `redisson__timeout__set:{${mc}}`, `redisson__idle__set:{${mc}}`);
      // TTL + maxIdle together (both 4.6.1-supported dimensions)
      await r.getMapCache<string, string>(mc).put('ck', 'cv', 8000, 8000);
      expect(await probe(`mc-get ${mc} ck`)).toBe('MC_GET:cv');
      await new Promise((x) => setTimeout(x, 8500));
      expect(await probe(`mc-get ${mc} ck`)).toBe('MC_NULL');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('setcache idle: presence shared, expiry is TS-side', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    // NOTE: 4.6.1 RSetCache has no maxIdle API — idle durations are a TS
    // extension stored in redisson__idle__set. Java sees presence (TTL score
    // is MaxInt64) but cannot enforce TS idle; expiry below is TS-side.
    const sc = `${NS}sc:idle10`;
    try {
      await cleanExact(r, sc, `redisson__idle__set:{${sc}}`);
      expect(await r.getSetCache<string>(sc).add('iv', 0, 3000)).toBe(true);
      expect(await probe(`sc-contains ${sc} iv`)).toBe('SC_HAS:true');
      await new Promise((x) => setTimeout(x, 3500));
      expect(await r.getSetCache<string>(sc).contains('iv')).toBe(false);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('mapcache destroy clears data + companions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc:destroy`;
    try {
      const mc = r.getMapCache<string, string>(name);
      await mc.put('a', '1', 60000, 60000);
      await mc.put('b', '2');
      expect(await probe(`mc-size ${name}`)).toBe('MC_SIZE:2');
      await mc.destroy();
      expect(await probe(`mc-size ${name}`)).toBe('MC_SIZE:0');
      expect(await mc.size()).toBe(0);
      expect(await r.redis.exists(`redisson__timeout__set:{${name}}`)).toBe(0);
      expect(await r.redis.exists(`redisson__idle__set:{${name}}`)).toBe(0);
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
