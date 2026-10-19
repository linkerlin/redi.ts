/**
 * Phase 21 live cluster: an ioredis Cluster used as the Redisson client.
 *
 * Every structure keeps all of its keys in one slot thanks to the hash-tag
 * companion rule (prefixName/suffixName wrap a bare name as `{name}`, and
 * `name` hashes exactly like `{name}`), so multi-key Lua stays cluster-safe.
 * A regression surfaces as CROSSSLOT/MOVED errors, never as silent
 * misbehaviour.
 *
 * Gated on a local 3-master test cluster (scripts/cluster-up.sh, 7100-7102);
 * logs a warning and skips when it is unreachable.
 */
import { describe, it, expect } from 'vitest';
import Redis, { Cluster } from 'ioredis';
import { Redisson } from '../src';
import { prefixName, suffixName } from '../src/util';

const CLUSTER_URL = process.env.REDIS_CLUSTER_URL ?? 'redis://127.0.0.1:7100';

async function clusterAvailable(): Promise<boolean> {
  try {
    const u = new URL(CLUSTER_URL);
    const r = new Redis(Number(u.port || 7100), u.hostname || '127.0.0.1', {
      connectTimeout: 2000,
      maxRetriesPerRequest: 1,
      lazyConnect: true,
      retryStrategy: () => null,
    });
    await r.connect();
    const info = String(await r.cluster('info'));
    r.disconnect();
    return info.includes('cluster_state:ok') && !info.includes('cluster_slots_assigned:0');
  } catch {
    return false;
  }
}

function makeCluster(): Cluster {
  const u = new URL(CLUSTER_URL);
  const c = new Cluster([{ host: u.hostname || '127.0.0.1', port: Number(u.port || 7100) }], {
    clusterRetryStrategy: () => null,
    redisOptions: { connectTimeout: 3000, maxRetriesPerRequest: 1 },
  });
  c.on('error', () => undefined); // keep node-level failures from crashing the test
  return c;
}

const hasCluster = await clusterAvailable();
if (!hasCluster) {
  console.warn(`[live-cluster] no cluster at ${CLUSTER_URL} — skipping (start it with scripts/cluster-up.sh)`);
}

const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));
async function waitFor(check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return true;
    await sleep(100);
  }
  return check();
}

describe('live cluster phase 21: ioredis Cluster as client', () => {
  it('bucket/map/atomic/lock work; a second client contends through slot routing', async () => {
    if (!hasCluster) return;
    const r = Redisson.create({ client: makeCluster() });
    const r2 = Redisson.create({ client: makeCluster() });
    const tag = 'TsJavaCluP21:{c21core}';
    const bucketKey = `${tag}:bucket`;
    const mapKey = `${tag}:map`;
    const cntKey = `${tag}:cnt`;
    const lockKey = `${tag}:lock`;
    const lockChannel = prefixName('redisson_lock__channel', lockKey);
    try {
      await r.redis.del(bucketKey, mapKey, cntKey, lockKey, lockChannel);

      const b = r.getBucket<{ n: number }>(bucketKey);
      await b.set({ n: 1 });
      expect(await b.get()).toEqual({ n: 1 });

      const m = r.getMap<string, string>(mapKey);
      await m.put('k', 'v');
      expect(await m.get('k')).toBe('v');
      expect(await m.containsKey('k')).toBe(true);

      const a = r.getAtomicLong(cntKey);
      expect(await a.incrementAndGet()).toBe(1);
      expect(await a.addAndGet(5)).toBe(6);

      const lock = r.getLock(lockKey);
      const other = r2.getLock(lockKey);
      await lock.lock(10000);
      expect(await other.tryLock(100)).toBe(false);
      await lock.unlock();
      expect(await other.tryLock(200, 10000)).toBe(true);
      await other.unlock();
    } finally {
      await r.redis.del(bucketKey, mapKey, cntKey, lockKey, lockChannel);
      await r.shutdown();
      await r2.shutdown();
    }
  }, 120000);

  it('map cache: TTL entries and listener events cross the cluster pub/sub bus', async () => {
    if (!hasCluster) return;
    const r = Redisson.create({ client: makeCluster() });
    const tag = 'TsJavaCluP21:{c21mc}';
    const name = `${tag}:mc`;
    const optionsKey = suffixName(name, 'redisson_options');
    const ttlKey = prefixName('redisson__timeout__set', name);
    const mc = r.getMapCache<string, string>(name);
    try {
      await r.redis.del(name, optionsKey, ttlKey);
      const events: string[] = [];
      const id = await mc.addListener({
        onCreated: (e) => events.push(`c:${e.key}:${e.value}`),
        onRemoved: (e) => events.push(`r:${e.key}:${e.oldValue}`),
      });

      await mc.put('k', 'v1', 60000);
      expect(await mc.get('k')).toBe('v1');
      // classic MapCache keeps per-entry TTLs in a companion ZSET (the HASH
      // itself never expires), and that ZSET lives in the same slot.
      expect(await r.redis.zcard(ttlKey)).toBe(1);

      await mc.remove('k');
      expect(await waitFor(async () => events.includes('c:k:v1') && events.includes('r:k:v1'), 3000)).toBe(true);

      await mc.removeListener(id);
    } finally {
      await mc.delete();
      await r.redis.del(name, optionsKey, ttlKey);
      await r.shutdown();
    }
  }, 120000);

  it('delayed queue background migrator (cluster subscription + timers)', async () => {
    if (!hasCluster) return;
    const r = Redisson.create({ client: makeCluster() });
    const name = 'TsJavaCluP21:{c21dq}:dq';
    const staging = prefixName('redisson_delay_queue', name);
    const timeout = prefixName('redisson_delay_queue_timeout', name);
    const dq = r.getDelayedQueue<string>(name);
    try {
      await r.redis.del(name, staging, timeout);
      await dq.start();
      await dq.offer('c', 700);

      // No poll(), no Java helper: delivery must come from the migrator alone.
      expect(await waitFor(async () => (await dq.targetSize()) >= 1, 3000)).toBe(true);
      expect(await r.redis.lrange(name, 0, -1)).toEqual(['"c"']);
      expect(await dq.size()).toBe(0);
      expect(await dq.pendingSize()).toBe(0);
    } finally {
      await dq.stop();
      await r.redis.del(name, staging, timeout);
      await r.shutdown();
    }
  }, 120000);

  it('rate limiter and topic work over cluster', async () => {
    if (!hasCluster) return;
    const r = Redisson.create({ client: makeCluster() });
    const name = 'TsJavaCluP21:{c21rl}:rl';
    const valueKey = suffixName(name, 'value');
    const permitsKey = suffixName(name, 'permits');
    const topicName = 'TsJavaCluP21:{c21rl}:topic';
    try {
      await r.redis.del(name, valueKey, permitsKey);

      const rl = r.getRateLimiter(name);
      expect(await rl.trySetRate('OVERALL', 2, 10000)).toBe(true);
      expect(await rl.tryAcquire()).toBe(true);
      expect(await rl.tryAcquire()).toBe(true);
      expect(await rl.tryAcquire()).toBe(false);

      const got: string[] = [];
      const topic = r.getTopic<string>(topicName);
      const id = await topic.addListener((m) => got.push(m));
      await r.getTopic<string>(topicName).publish('hi');
      expect(await waitFor(async () => got.includes('hi'), 3000)).toBe(true);
      await topic.removeListener(id);
    } finally {
      await r.redis.del(name, valueKey, permitsKey);
      await r.shutdown();
    }
  }, 120000);
});
