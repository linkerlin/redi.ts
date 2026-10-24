/**
 * Live interop vs **Java Redisson 4.6.1/4.8.0**, round 18 — the final
 * structural gaps: bounded-queue capacity across languages, multi-permit
 * PES atomicity and JSON terminal ops.
 *
 *  - RBoundedBlockingQueue: capacity configured by TS enforces Java's
 *    blocking `put` — the third put parks until a TS `take` frees a slot
 *    (probe `bbq-put` with its result-key protocol; `probeBg` for the
 *    parked one).
 *  - RPermitExpirableSemaphore multi-permit: TS `tryAcquire(lease, 2)`
 *    consumes two permits in ONE atomic Lua; Java sees the exact
 *    remainder, takes the last one, and TS's next 2-permit attempt is
 *    rejected. (Earlier rounds only ever moved single permits.)
 *  - RJsonBucket terminal ops: TS `merge`/`toggle` observed through
 *    Java's path reads; Java path writes consumed by TS `type`/`clear`.
 *
 * Skips with a logged line (never silently) when Redis or a probe is
 * unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe, probeBg } from './helpers/java-probe.js';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48r:';

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

let probeUp: boolean | null = null;
async function java461Up(): Promise<boolean> {
  if (probeUp !== null) return probeUp;
  try {
    probeUp = (await probe(`atomic-get ${NS}warm`)).startsWith('ATOMIC:');
  } catch (e) {
    console.warn(`[live interop48r] Java 4.6.1 probe unavailable — bounded-queue checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeUp = false;
  }
  return probeUp;
}

let probe48Ok: boolean | null = null;
async function java480Up(): Promise<boolean> {
  if (probe48Ok !== null) return probe48Ok;
  try {
    probe48Ok = (await probe48.probe(`cb48-capacity ${NS}warm`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48r] Java 4.8.0 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probe48Ok = false;
  }
  return probe48Ok;
}

describe('live interop48r: bounded queue + multi-permit PES + JSON terminal ops', () => {
  it('RBoundedBlockingQueue: TS capacity gates Java puts; a TS take unparks one', async () => {
    if (!hasRedis) return;
    if (!(await java461Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q = `${NS}bbq`;
    try {
      // incl. the capacity companion (prefixName hash-tag form) — law 4/9
      await r.getKeys().delete(q, `${q}:put-result`, `${q}:r1`, `${q}:r2`, `${q}:r3`, `redisson_bqs:{${q}}`);
      const bq = r.getBoundedBlockingQueue<string>(q);
      expect(await bq.trySetCapacity(2)).toBe(true);

      // two Java puts fill the capacity
      expect(await probe(`bbq-put ${q} v1 ${q}:r1`)).toBe('BQ_PUT_OK');
      expect(await probe(`bbq-put ${q} v2 ${q}:r2`)).toBe('BQ_PUT_OK');
      expect(await bq.remainingCapacity()).toBe(0);

      // the third parks; a TS take frees a slot and completes it
      const third = probeBg(`bbq-put ${q} v3 ${q}:r3`);
      await new Promise((res) => setTimeout(res, 400));
      expect(await r.getBucket<any>(`${q}:r3`).get()).toBe('BQ_PUT_STARTED'); // parked
      expect(await bq.take()).toBe('v1');
      expect(await third).toEqual(['BQ_PUT_OK']);
      expect(await bq.remainingCapacity()).toBe(0); // freed slot immediately consumed

      // drain in order, queue empty afterwards
      expect(await bq.take()).toBe('v2');
      expect(await bq.take()).toBe('v3');
      expect(await bq.remainingCapacity()).toBe(2);
    } finally {
      await r.getKeys().delete(q, `${q}:put-result`, `${q}:r1`, `${q}:r2`, `${q}:r3`);
      await r.shutdown();
    }
  }, 300000);

  it('RPermitExpirableSemaphore: multi-permit acquire is atomic across languages', async () => {
    if (!hasRedis) return;
    if (!(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes`;
    const timeout = `{${name}}:timeout`; // suffixName hash-tag form
    try {
      await r.getKeys().delete(name, timeout);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(3)).toBe(true);

      // TS takes TWO permits in one Lua; Java sees exactly one left
      const two = await pes.tryAcquire(600000, 2);
      expect(two).not.toBeNull();
      expect(two!).toHaveLength(2);
      expect(await probe48.probe(`pes48-available ${name}`)).toBe('available=1');

      // Java takes the last one; TS's next 2-permit attempt is rejected whole
      const jid = await probe48.probe(`pes48-tryacquire ${name} 600000`);
      expect(jid).not.toBe('permit=null');
      expect(await pes.tryAcquire(600000, 2)).toBeNull(); // no partial grant
      expect(await pes.acquiredPermits()).toBe(3);
    } finally {
      await r.getKeys().delete(name, timeout);
      await r.shutdown();
    }
  }, 300000);

  it('RJsonBucket terminal ops: merge/toggle seen by Java, Java writes typed/cleared by TS', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    let jsonUp = true;
    try {
      await raw.connect();
      const info = await raw.call('COMMAND', 'INFO', 'JSON.SET');
      jsonUp = Array.isArray(info) && info[0] !== null;
    } catch {
      jsonUp = false;
    }
    raw.disconnect();
    if (!jsonUp) {
      console.log('[live interop48r] JSON.* unavailable — skipping terminal ops');
      return;
    }
    if (!(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}json`;
    try {
      await r.getKeys().delete(name);
      const jb = r.getJsonBucket<any>(name);

      // TS merges an object into a path; Java reads the merged shape
      await jb.set({ a: { x: 1 } });
      await jb.merge('$.a', { y: 2 });
      expect(await probe48.probe(`json48-get ${name} $.a`)).toBe('json=[{"x":1,"y":2}]');

      // TS toggles a boolean; Java sees the flip
      await jb.set(false, '$.flag');
      expect(await jb.toggle('$.flag')).toBe(true);
      expect(await probe48.probe(`json48-get ${name} $.flag`)).toBe('json=[true]');

      // Java path-writes an array; TS types and clears it
      await probe48.probe(`json48-setpath ${name} $.arr {"v":[1,2,3]}`);
      expect(await jb.arraySize('$.arr.v')).toBe(3);
      expect(await jb.clear('$.arr.v')).toBe(1);
      expect(await probe48.probe(`json48-get ${name} $.arr.v`)).toBe('json=[[]]');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
