/**
 * Phase 4 live interop: Topic pub/sub, Stream consumer groups, SetMultimap
 * (HighwayHash internal ids), BoundedBlockingQueue capacity, RingBuffer,
 * IdGenerator interleaving, Batch map put, MapCache per-entry TTL expiry.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import Redis from 'ioredis';
import { Redisson } from '../src';

const execAsync = promisify(exec);
const NS = 'TsJavaT:p4:';
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

async function clean(r: Redisson, pattern: string): Promise<void> {
  const keys = await r.getKeys().getKeys(pattern, 500);
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

/** Direct DEL for exact keys — never route brace/hash-tag names through SCAN
 *  MATCH: `{}` is glob alternation in Redis patterns. */
async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();

describe('live interop phase 4: topic/stream/multimap/bq/ring/idgen/batch/mc-ttl', () => {
  it('topic delivers both directions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    try {
      // TS → Java (verdict mirrored to resultKey as a JSON bucket value)
      const tj = `${NS}topic:tj`;
      const resKey = `${tj}:listen-result`;
      await clean(r, `${tj}*`);
      const listenP = execAsync(
        `"${MVN}" -f "${POM}" -q compile exec:java "-Dexec.args=topic-listen ${tj} 15000 ${resKey}"`,
        { timeout: 90000, cwd: ROOT },
      ).then((x) => x.stdout, () => '');
      // wait until the Java subscriber is actually registered (pubsub numsub)
      let subscribed = false;
      for (let i = 0; i < 200 && !subscribed; i++) {
        await new Promise((x) => setTimeout(x, 100));
        subscribed = await r.getTopic<string>(tj).countSubscribers() > 0;
      }
      expect(subscribed).toBe(true);
      await r.getTopic<string>(tj).publish('t-msg-ts');
      await listenP;
      const v1 = await r.getBucket<string>(resKey).get();
      expect(v1).toBe('GOT:t-msg-ts');

      // Java → TS
      const jt = `${NS}topic:jt`;
      const got: string[] = [];
      const lid = await r.getTopic<string>(jt).addListener((m) => {
        got.push(String(m));
      });
      await new Promise((x) => setTimeout(x, 500));
      await probe(`topic-publish ${jt} t-msg-java`);
      for (let i = 0; i < 100 && got.length === 0; i++) await new Promise((x) => setTimeout(x, 100));
      await r.getTopic<string>(jt).removeListener(lid);
      expect(got).toContain('t-msg-java');
    } finally {
      await r.shutdown();
    }
  }, 150000);

  it('stream consumer group reads cross-language', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const stream = `${NS}stream:shared`;
    try {
      await clean(r, `${stream}*`);
      expect(await probe(`stream-group-create ${stream} gts`)).toBe('GROUP_CREATED');
      // TS produces; Java group reads
      await r.getStream(stream).add({ jread: 'from-ts' });
      const resKey = `${stream}:read-result`;
      await clean(r, resKey);
      expect(await probe(`stream-group-read ${stream} gts java-c1 ${resKey}`)).toMatch(/^STREAM_GOT:.*from-ts/);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('set multimap internal ids are byte-identical', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mm:shared`;
    try {
      // clear() first: brace-prefixed collection keys are invisible to SCAN
      // patterns and deterministic ids get reused (see phase-5 lmm note).
      await r.getSetMultimap<string, string>(name).clear();
      await clean(r, `${name}*`);
      expect(await probe(`multimap-put ${name} shared-key jval1`)).toBe('MM_PUT');
      // TS reads Java's entry — exercises HighwayHash internal id compatibility
      const vals = await r.getSetMultimap<string, string>(name).get('shared-key');
      expect(vals).toContain('jval1');
      // TS writes; Java collection key must land on the same id (byte-level)
      await r.getSetMultimap<string, string>(name).put('shared-key', 'tsval1');
      const ids = await r.redis.hgetall(name);
      const field = '"shared-key"';
      expect(ids[field]).toBeTruthy();
      const { suffixName } = await import('../src/util');
      const coll = suffixName(name, ids[field]);
      const members = await r.redis.smembers(coll);
      expect(members.length).toBe(2);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('bounded blocking queue capacity is shared', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bq:shared`;
    try {
      await clean(r, `${name}*`);
      await cleanExact(r, `redisson_bqs:{${name}}`);
      expect(await probe(`bq-capacity ${name} 2`)).toBe('BQ_CAP:true');
      expect(await r.getBoundedBlockingQueue<string>(name).offer('ts1')).toBe(true);
      expect(await probe(`bq-offer ${name} j1`)).toBe('BQ_OFFERED');
      // full now: TS offer must be refused
      expect(await r.getBoundedBlockingQueue<string>(name).offer('overflow')).toBe(false);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('ring buffer capacity shared, eviction matches', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ring:shared`;
    try {
      await clean(r, `${name}*`);
      await cleanExact(r, `redisson_rb:{${name}}`);
      expect(await probe(`ring-setcap ${name} 3`)).toBe('RING_CAP:true');
      const rb = r.getRingBuffer<string>(name);
      await rb.add('t1');
      await rb.add('t2');
      expect(await probe(`ring-add ${name} j1`)).toBe('RING_ADDED');
      await rb.add('t3'); // overflow: t1 evicted
      expect(await rb.readAll()).toEqual(['t2', 'j1', 't3']);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('id generator ranges interleave without collisions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}idgen:shared`;
    try {
      await clean(r, `${name}*`);
      await cleanExact(r, `{${name}}:allocation`);
      expect(await probe(`idgen-init ${name} 5000 1`)).toBe('IDGEN_INIT:true');
      const g = r.getIdGenerator(name);
      const ts1 = await g.nextId();
      const java1 = Number((await probe(`idgen-next ${name}`)).slice('IDGEN:'.length));
      const ts2 = await g.nextId();
      const ids = [ts1, java1, ts2].sort((a, b) => a - b);
      expect(new Set(ids).size).toBe(3); // unique
      expect(ids[0]).toBe(5000);
      expect(ids[2]).toBe(5002);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('batch map put is visible to both sides', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}batchmap:shared`;
    try {
      await clean(r, `${name}*`);
      expect(await probe(`batch-map-put ${name} bk1 bv1`)).toBe('BATCH_DONE');
      expect(await r.getMap<string, string>(name).get('bk1')).toBe('bv1');
      // TS batch → Java raw
      const batch = r.createBatch();
      batch.pipeline.hset(name, '"bk2"', '"bv2"');
      await batch.execute();
      expect(await r.getMap<string, string>(name).get('bk2')).toBe('bv2');
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('mapcache per-entry TTL expires across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mcttl:shared`;
    try {
      // NOTE: companion sets need exact DELs (brace keys invisible to SCAN);
      // TTL needs generous margin: full-suite parallel workers make the
      // JVM→TS handoff jittery, and visible-before assertions hate slowness.
      await clean(r, `${name}*`);
      await cleanExact(r, `redisson__timeout__set:{${name}}`, `redisson__idle__set:{${name}}`);
      expect(await probe(`mc-put-ttl ${name} tk1 tv1 6000`)).toBe('MC_TTL_SET');
      expect(await r.getMapCache<string, string>(name).get('tk1')).toBe('tv1');
      await new Promise((x) => setTimeout(x, 6500));
      expect(await r.getMapCache<string, string>(name).get('tk1')).toBeNull();
    } finally {
      await r.shutdown();
    }
  }, 120000);
});
