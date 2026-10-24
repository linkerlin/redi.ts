/**
 * Live interop vs **Java Redisson 4.8.0**, round 26 — queue/topic/deque
 * edges the earlier rounds didn't cover:
 *
 *  - RShardedTopic: in 4.8 it extends RTopic, but the wire is SPUBLISH/
 *    SSUBSCRIBE (TS delivers via the `smessage` event) — codec payloads
 *    must decode on both sides and the SPUBLISH receiver count must be
 *    visible cross-language.
 *  - RBuckets conditional MSETEX: `setIfAllKeysExist` (XX) and
 *    `setIfAllKeysAbsent` (NX) are all-or-nothing — a failed conditional
 *    must write NEITHER key, a successful one lands codec values both
 *    sides can read, with optional TTL.
 *  - RBlockingQueue: a TS `take()` parked on BLPOP is woken by Java's
 *    plain RPUSH offer, and a Java background take() by a TS offer.
 *  - RDeque: both-ends ops (addFirst/addLast, pollFirst/pollLast — the
 *    no-arg polls are inherited from java.util.Deque in 4.8) agree in
 *    both directions.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48z:';

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

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48z] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

function withSafety<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`${what} never woke within ${ms}ms`)), ms).unref?.() ?? undefined),
  ]) as Promise<T>;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48z: sharded topic / MSETEX / blocking wake / deque vs Java 4.8.0', () => {
  it('RShardedTopic: SSUBSCRIBE delivers codec payloads both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ch = `${NS}sharded`;
    try {
      // Java SPUBLISHes → TS listener receives the decoded payload
      const topic = r.getShardedTopic<string>(ch); // ONE instance
      let resolveGot: (v: string) => void = () => undefined;
      const gotP = new Promise<string>((res) => { resolveGot = res; });
      const id = await topic.addListener((msg) => resolveGot(msg));
      expect(await probe48.probe(`sharded48-pub ${ch} java-sharded-msg`)).toBe('published=ok');
      expect(await withSafety(gotP, 15000, 'sharded message')).toBe('java-sharded-msg');
      await topic.removeListener(id);

      // Java SSUBSCRIBEs → TS SPUBLISHes; receiver count visible to TS
      expect(await probe48.probe(`sharded48-listen ${ch}`)).toMatch(/^listening=/);
      expect(await topic.publish('ts-sharded-msg')).toBe(1);
      expect(await probe48.probe(`sharded48-poll 8000`)).toBe('msg=ts-sharded-msg');
    } finally {
      await r.getShardedTopic<string>(ch).removeAllListeners();
      await r.shutdown();
    }
  }, 300000);

  it('RBuckets MSETEX: XX/NX all-or-nothing, both directions, with TTL', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}xx-a`, k2 = `${NS}xx-b`, k3 = `${NS}nx-c`, k4 = `${NS}nx-d`;
    const k5 = `${NS}tsx-e`, k6 = `${NS}tsx-f`;
    const all = [k1, k2, k3, k4, k5, k6];
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, all);

      // Java XX fails when ONE key is missing — neither key is written
      await r.getBucket<string>(k1).set('ts-a');
      expect(await probe48.probe(`buckets48-casexist ${k1} j1 ${k2} j2`)).toBe('ok=false');
      expect(await r.getBucket<string>(k1).get()).toBe('ts-a');

      // Java XX succeeds once both exist; optional TTL lands on both keys
      await r.getBucket<string>(k2).set('ts-b');
      expect(await probe48.probe(`buckets48-casexist ${k1} j1 ${k2} j2 30`)).toBe('ok=true');
      expect(await r.getBucket<string>(k1).get()).toBe('j1');
      expect(await r.getBucket<string>(k2).get()).toBe('j2');
      for (const k of [k1, k2]) {
        const ttl = Number(await raw.call('PTTL', k));
        expect(ttl).toBeGreaterThan(10000);
        expect(ttl).toBeLessThanOrEqual(30000);
      }

      // Java NX succeeds on fresh keys, fails atomically on existing ones
      expect(await probe48.probe(`buckets48-casabsent ${k3} j3 ${k4} j4`)).toBe('ok=true');
      expect(await r.getBucket<string>(k3).get()).toBe('j3');
      expect(await probe48.probe(`buckets48-casabsent ${k3} x ${k4} x`)).toBe('ok=false');
      expect(await r.getBucket<string>(k3).get()).toBe('j3');

      // TS conditional writes; Java reads the landed values back
      expect(await r.getBuckets().setIfAllKeysAbsent({ [k5]: 'ts-nx1', [k6]: 'ts-nx2' })).toBe(true);
      expect(await r.getBuckets().setIfAllKeysAbsent({ [k5]: 'x', [k6]: 'x' })).toBe(false);
      expect(await probe48.probeLines(`buckets48-get ${k5} ${k6}`)).toEqual([`${k5}=ts-nx1`, `${k6}=ts-nx2`]);
      expect(await r.getBuckets().setIfAllKeysExist({ [k5]: 'ts-xx1', [k6]: 'ts-xx2' })).toBe(true);
      expect(await probe48.probeLines(`buckets48-get ${k5} ${k6}`)).toEqual([`${k5}=ts-xx1`, `${k6}=ts-xx2`]);
    } finally {
      await delAll(raw, all);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBlockingQueue: parked take() woken cross-language in both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q1 = `${NS}bq-ts`;
    const q2 = `${NS}bq-java`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [q1, q2]);

      // TS take() parks on BLPOP; Java's plain RPUSH offer wakes it
      const takeP = r.getBlockingQueue<string>(q1).take();
      await sleep(300); // let BLPOP arm
      expect(await probe48.probe(`q48-offer ${q1} java-wakes-ts`)).toBe('offered=true');
      expect(await withSafety(takeP, 15000, 'ts take')).toBe('java-wakes-ts');

      // Java take() parks on a background thread; TS offer wakes it
      expect(await probe48.probe(`bq48-bgtake ${q2}`)).toBe('armed');
      expect(await r.getBlockingQueue<string>(q2).offer('ts-wakes-java')).toBe(true);
      expect(await probe48.probe(`bq48-bgresult 8000`)).toBe('value=ts-wakes-java');
    } finally {
      await delAll(raw, [q1, q2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RDeque: both-ends ops agree in both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const d1 = `${NS}deq-ts`;
    const d2 = `${NS}deq-java`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [d1, d2]);

      // TS pushes both ends; Java pops both ends in order
      const d = r.getDeque<string>(d1);
      expect(await d.addFirst('ts-f')).toBe(1);
      expect(await d.addLast('ts-l')).toBe(2);
      expect(await probe48.probe(`deque48-pop ${d1}`)).toBe('first=ts-f;last=ts-l');

      // Java pushes both ends; TS pops both ends in order
      expect(await probe48.probe(`deque48-push ${d2} java-f java-l`)).toBe('OK');
      const dj = r.getDeque<string>(d2);
      expect(await dj.pollFirst()).toBe('java-f');
      expect(await dj.pollLast()).toBe('java-l');
    } finally {
      await delAll(raw, [d1, d2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
