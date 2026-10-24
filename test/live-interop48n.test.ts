/**
 * Live interop, round 14 — TRI-LATERAL (TS + Java 4.6.1 + Java 4.8.0) and
 * Unicode payloads. Both resident probes run in the same worker:
 *
 *  - Rolling upgrade: one key's state relayed TS → 4.6.1 → 4.8.0 → TS
 *    (atomic counter, 3-producer queue) and a lock held against 4.6.1,
 *    force-released by 4.8.0, re-acquired by 4.6.1 — the wire is stable
 *    across Java versions.
 *  - Unicode: CJK/emoji/accented values written by TS verified by Java via
 *    boolean field-match commands (never echoed through stdout, whose
 *    charset is host-dependent); Java-written CJK read exactly by TS.
 *
 * Skips with a logged line (never silently) when Redis or either probe
 * is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe.js';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48n:';

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
    console.warn(`[live interop48n] Java 4.6.1 probe unavailable — tri-lateral checks skipped: ${(e as Error).message.split('\n')[0]}`);
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
    console.warn(`[live interop48n] Java 4.8.0 probe unavailable — tri-lateral checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probe48Ok = false;
  }
  return probe48Ok;
}

describe('live interop48n: tri-lateral (TS+4.6.1+4.8.0) + unicode', () => {
  it('rolling upgrade: one counter and one queue relayed across all three', async () => {
    if (!hasRedis) return;
    if (!(await java461Up()) || !(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}at`;
    const q = `${NS}q`;
    try {
      // counter: TS seeds, 4.6.1 sets, 4.8.0 increments, both Javas + TS agree
      await r.getKeys().delete(k);
      await r.getAtomicLong(k).set(0);
      await probe(`atomic-set-long ${k} 10`);
      await probe48.probe(`atomic48-incrargs ${k} 1 - - 0`);
      expect(await probe(`atomic-get ${k}`)).toBe('ATOMIC:11');
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=11');
      expect(await r.getAtomicLong(k).get()).toBe(11);

      // queue: three producers (one per runtime), one TS consumer
      await r.getKeys().delete(q);
      await r.getQueue<string>(q).add('ts');
      await probe(`q-offer ${q} java461`);
      await probe48.probe(`q48-offer ${q} java480`);
      expect(await r.getQueue<string>(q).readAll()).toEqual(['ts', 'java461', 'java480']);
    } finally {
      await r.getKeys().delete(k, q);
      await r.shutdown();
    }
  }, 300000);

  it('lock held against 4.6.1, force-released by 4.8.0, re-acquired by 4.6.1', async () => {
    if (!hasRedis) return;
    if (!(await java461Up()) || !(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock`;
    try {
      await r.getKeys().delete(name);
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED'); // free

      const lock = r.getLock(name);
      await lock.lock(600000); // long lease — WSL clock-jump safe
      expect(await probe(`lock-try ${name}`)).toBe('CONTENDED'); // 4.6.1 denied
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=true'); // 4.8 sees it
      expect(await probe48.probe(`lock48-forceunlock ${name}`)).toBe('forced=true'); // 4.8 frees it
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED'); // 4.6.1 again
      expect(await lock.tryLock(0)).toBe(true); // TS too
      await lock.unlock();
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('unicode payloads: CJK/emoji/accented values cross languages intact', async () => {
    if (!hasRedis) return;
    if (!(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const bkt = `${NS}uni-b`;
    const q = `${NS}uni-q`;
    try {
      await r.getKeys().delete(bkt, q);

      // TS writes CJK+emoji+accents; Java verifies via boolean field match
      // (values never echo through stdout — its charset is host-dependent)
      await r.getBucket<any>(bkt).set({ s: '你好🌏', t: 'café' });
      expect(await probe48.probe(`bkt48-expectfield ${bkt} s 你好🌏`)).toBe('match=true');
      expect(await probe48.probe(`bkt48-expectfield ${bkt} t café`)).toBe('match=true');
      expect(await probe48.probe(`bkt48-expectfield ${bkt} s 不是这个`)).toBe('match=false');

      // Java writes CJK (UTF-8 stdin arg); TS reads it exactly
      await probe48.probe(`bkt48-set ${bkt} {"s":"日本語です"}`);
      expect(await r.getBucket<any>(bkt).get()).toEqual({ s: '日本語です' });

      // queue members carry unicode both ways
      await r.getQueue<string>(q).add('中文值');
      expect(await probe48.probe(`q48-expectpoll ${q} 中文值`)).toBe('match=true');
      await probe48.probe(`q48-offer ${q} 한글값`);
      expect(await r.getQueue<string>(q).poll()).toBe('한글값');
    } finally {
      await r.getKeys().delete(bkt, q);
      await r.shutdown();
    }
  }, 300000);
});
