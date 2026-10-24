/**
 * Live interop vs **Java Redisson 4.8.0**, round 20 — three surfaces never
 * exercised cross-language before:
 *
 *  - RFairLock FIFO queue: Java holds → TS lock() queues → Java unlock wakes
 *    TS (forward); TS holds → Java background waiter queues → TS unlock
 *    wakes Java (reverse). Queue member / HASH field / wake-channel suffix
 *    are `id:threadId` on BOTH sides (RedissonBaseLock.getLockName); the
 *    wake itself is the wire proof, no format re-parsing involved.
 *  - RTopic pub/sub payload: channel = raw name (no prefix — Java wraps the
 *    name in ChannelName directly). TS listener decodes Java's
 *    JsonJacksonCodec payload and vice versa; PUBLISH's receiver count must
 *    see the other side's subscriber.
 *  - RMapCache per-entry TTL: Java put(k, v, ttl) → TS get unpacks Java's
 *    packed value + deadline ZSET; TS put(k, v, ttlMs) → Java get unpacks
 *    TS's packing while alive. Expiry asserted gone-after only (law 10).
 *
 * Probe commands flock48-*, topic48-*, mc48-*.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { prefixName } from '../src/util';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48t:';

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
    console.warn(`[live interop48t] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

function withSafety<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`${what} never woke within ${ms}ms`)), ms).unref?.() ?? undefined),
  ]) as Promise<T>;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

describe('live interop48t: fair-lock queue / topic pubsub / mapcache TTL vs Java 4.8.0', () => {
  it('RFairLock: TS lock() queued behind a Java holder, Java unlock wakes TS', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fl-fwd`;
    const fl = r.getFairLock(name);
    try {
      await r.getKeys().delete(name, prefixName('redisson_lock_queue', name), prefixName('redisson_lock_timeout', name));

      expect(await probe48.probe(`flock48-lock ${name}`)).toBe('locked=ok');
      expect(await fl.isLocked()).toBe(true);

      const p = withSafety(fl.lock(), 30000, 'fair lock acquire');
      await sleep(800); // TS enqueues behind the Java holder and subscribes

      expect(await probe48.probe(`flock48-unlock ${name}`)).toBe('unlocked=ok');
      await p; // resolution IS the wake proof (queue member + channel format)
      expect(await fl.isLocked()).toBe(true);
      await fl.unlock();
      expect(await fl.isLocked()).toBe(false);
    } finally {
      await fl.delete();
      await r.shutdown();
    }
  }, 300000);

  it('RFairLock: Java bg waiter queued behind a TS holder, TS unlock wakes Java', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fl-rev`;
    const fl = r.getFairLock(name);
    try {
      await r.getKeys().delete(name, prefixName('redisson_lock_queue', name), prefixName('redisson_lock_timeout', name));

      await fl.lock(); // TS holds
      expect(await probe48.probe(`flock48-bglock ${name} 8000 60000`)).toBe('started');
      await sleep(800); // Java bg thread subscribes + enqueues

      await fl.unlock(); // wakes the queued Java waiter
      expect(await probe48.probe(`flock48-bgwaitacquired 10000`)).toBe('acquired=true');
      expect(await fl.isLocked()).toBe(true); // Java now holds (60s lease)

      expect(await probe48.probe(`flock48-bggo`)).toBe('go=ok');
      expect(await probe48.probe(`flock48-bgjoin 10000`)).toBe('acquired=true released=1');
      expect(await fl.isLocked()).toBe(false);
    } finally {
      await fl.delete();
      await r.shutdown();
    }
  }, 300000);

  it('RFairLock: TS tryLock() fails immediately against a Java holder', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}fl-try`;
    const fl = r.getFairLock(name);
    try {
      await r.getKeys().delete(name, prefixName('redisson_lock_queue', name), prefixName('redisson_lock_timeout', name));

      expect(await probe48.probe(`flock48-lock ${name}`)).toBe('locked=ok');
      expect(await fl.tryLock()).toBe(false);
      expect(await probe48.probe(`flock48-unlock ${name}`)).toBe('unlocked=ok');
      await fl.delete(); // clears the stale waiter entry tryLock() left queued
      expect(await fl.isLocked()).toBe(false);
    } finally {
      await fl.delete();
      await r.shutdown();
    }
  }, 300000);

  it('RTopic: both directions decode the other side codec payload', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const ch = `${NS}topic`;
    try {
      // Java publishes → TS listener receives the decoded payload
      const topic = r.getTopic<string>(ch); // ONE instance: removeListener is per-instance
      let got: string | null = null;
      let resolveGot: (v: string) => void = () => undefined;
      const tsListener = new Promise<string>((res) => { resolveGot = res; });
      const id = await topic.addListener((msg) => resolveGot(msg));
      const p = withSafety(tsListener, 15000, 'topic message');
      expect(await probe48.probe(`topic48-pub ${ch} hello-48`)).toBe('published=ok');
      expect(await p).toBe('hello-48');
      await topic.removeListener(id);

      // TS publishes → Java listener receives; PUBLISH receiver count = 1
      expect(await probe48.probe(`topic48-listen ${ch}`)).toMatch(/^listening=/);
      expect(await topic.publish('ts-payload-48')).toBe(1);
      expect(await probe48.probe(`topic48-poll 8000`)).toBe('msg=ts-payload-48');
    } finally {
      await r.getTopic<string>(ch).removeAllListeners();
      await r.shutdown();
    }
  }, 300000);

  it('RMapCache per-entry TTL: both sides unpack the other packed layout', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}mc`;
    const mc = r.getMapCache<string, string>(name);
    const companions = [
      name,
      prefixName('redisson__timeout__set', name),
      prefixName('redisson__idle__set', name),
      prefixName('redisson__map_cache__last_access__set', name),
    ];
    try {
      await r.getKeys().delete(...companions);

      // Java writes with TTL → TS get unpacks Java's packed value + deadline
      expect(await probe48.probe(`mc48-putttl ${name} k1 java-val 15000`)).toBe('put=ok');
      expect(await mc.get('k1')).toBe('java-val');
      await sleep(17000); // gone-after (law 10)
      expect(await mc.get('k1')).toBe(null);

      // TS writes with TTL → Java get unpacks TS's packing while alive
      expect(await mc.put('k2', 'ts-val', 15000)).toBe(null);
      expect(await probe48.probe(`mc48-get ${name} k2`)).toBe('value=ts-val');
    } finally {
      await r.getKeys().delete(...companions);
      await r.shutdown();
    }
  }, 300000);
});
