/**
 * Live interop vs **Java Redisson 4.8.0**, round 28 — mutual exclusion and
 * conditional writes, the semantics layer behind the wire shapes the
 * earlier rounds only probed non-blockingly:
 *
 *  - RLock: a TS-held lock rejects Java's tryLock(0) and vice versa;
 *    forceUnlock from one side releases the other's view.
 *  - RReadWriteLock: concurrent readers across languages (Java read held →
 *    TS read tryLock OK), writer exclusion both ways (write tryLock fails
 *    against any reader/writer), writer priority flips after release.
 *  - RMap conditional ops: putIfAbsent / 3-arg replace / remove(key,value)
 *    CAS chains alternating sides — each side sees the other's writes and
 *    rejects stale expectations.
 *  - RMap lazy HSCAN iterator over 12 Java-written fields (COUNT 5 forces
 *    multiple scan rounds), plus a TS put read back by Java.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ab:';

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
    console.warn(`[live interop48ab] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ab: lock mutex / rwlock semantics / map CAS vs Java 4.8.0', () => {
  it('RLock: TS-held lock rejects Java tryLock(0) and vice versa', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock-a`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name]);

      // TS holds; Java's immediate tryLock is rejected
      const lock = r.getLock(name);
      expect(await lock.tryLock()).toBe(true);
      expect(await probe48.probe(`lock48-trylock ${name}`)).toBe('trylock=false');
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=true');
      await lock.unlock();
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=false');

      // Java locks (10s lease); TS's immediate tryLock is rejected
      expect(await probe48.probe(`lock48-lock ${name} 10000`)).toBe('locked=true');
      expect(await lock.tryLock()).toBe(false);
      // Java force-unlock; TS can lock again
      expect(await probe48.probe(`lock48-forceunlock ${name}`)).toBe('forced=true');
      expect(await lock.tryLock()).toBe(true);
      await lock.unlock();
    } finally {
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RReadWriteLock: cross-language reader sharing and writer exclusion', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rw`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name, `redisson_rwlock:{${name}}`]);

      // Java holds the read lock → TS read tryLock succeeds (shared),
      // TS write tryLock fails
      expect(await probe48.probe(`rw48-readtry ${name}`)).toBe('got=true');
      const rw = r.getReadWriteLock(name);
      expect(await rw.readLock.tryLock()).toBe(true);
      expect(await rw.writeLock.tryLock()).toBe(false);
      await rw.readLock.unlock();
      await probe48.probe(`rw48-readunlock ${name}`);

      // All readers released → TS writer holds → Java write tryLock fails
      expect(await rw.writeLock.tryLock()).toBe(true);
      expect(await probe48.probe(`rw48-writetry ${name}`)).toBe('got=false');
      await rw.writeLock.unlock();
      expect(await probe48.probe(`rw48-writetry ${name}`)).toBe('got=true');
      await probe48.probe(`rw48-writeunlock ${name}`);
    } finally {
      await delAll(raw, [name, `redisson_rwlock:{${name}}`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RMap conditional CAS chain alternating sides, then lazy HSCAN iterator', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}map-cas`;
    const iter = `${NS}map-iter`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [name, iter]);
      const m = r.getMap<string, string>(name);

      // TS writes; both sides' CAS ops must see it
      await m.put('f', 'ts-v1');
      expect(await m.putIfAbsent('f', 'x')).toBe('ts-v1');
      expect(await probe48.probe(`map48-putifabsent ${name} f java-x`)).toBe('prev=ts-v1');

      // Java 3-arg replace succeeds on the exact old value
      expect(await probe48.probe(`map48-replace3 ${name} f ts-v1 java-v2`)).toBe('ok=true');
      // TS 3-arg replace: stale expectation rejected, fresh one accepted
      expect(await m.replace('f', 'stale', 'y')).toBe(false);
      expect(await m.replace('f', 'java-v2', 'ts-v3')).toBe(true);
      // Java remove with the wrong value rejected; TS removes with the exact one
      expect(await probe48.probe(`map48-removeval ${name} f java-v2`)).toBe('ok=false');
      expect(await m.remove('f', 'ts-v3')).toBe(true);
      // Field gone → Java putIfAbsent creates it
      expect(await probe48.probe(`map48-putifabsent ${name} f java-v4`)).toBe('prev=null');
      expect(await m.get('f')).toBe('java-v4');

      // Lazy HSCAN over 12 Java-written fields (COUNT 5 forces multi-round)
      const javaFields: Record<string, string> = {};
      for (let i = 0; i < 12; i++) javaFields[`jf${i}`] = `v${i}`;
      const args = Object.entries(javaFields).map(([k, v]) => `${k} ${v}`).join(' ');
      expect(await probe48.probe(`map48-putall ${iter} ${args}`)).toBe('OK');
      const mi = r.getMap<string, string>(iter);
      const seen = new Map<string, string>();
      for await (const [k, v] of mi.entriesIterator('*', 5)) seen.set(k, v);
      expect(seen).toEqual(new Map(Object.entries(javaFields)));
      // TS put read back by Java
      await mi.put('ts-f', 'ts-iter-v');
      expect(await probe48.probe(`map48-get ${iter} ts-f`)).toBe('value=ts-iter-v');
    } finally {
      await delAll(raw, [name, iter]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
