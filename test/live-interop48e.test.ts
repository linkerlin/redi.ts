/**
 * Live interop vs **Java Redisson 4.8.0**, round 5 — RBloomFilterNative,
 * blocking queue move (BLMOVEM) and RRateLimiter permit recycling:
 *
 *  - BFN is the native `BF.*` bloom filter — unlike the Lua RBloomFilter,
 *    elements travel RAW (Java uses StringCodec, no encode): the e2e pins
 *    that both languages address the same bits.
 *  - BLMOVEM: the TS consumer blocks on a dedicated connection; Java feeds
 *    the source queue; the move completes across languages. (TsProbe48 is a
 *    single-threaded synchronous REPL — the blocking side must be TS.)
 *  - RRateLimiter: permits acquired in TS are recycled by `release()` and
 *    immediately visible to Java `tryAcquire` — the release/recalc branch
 *    phase68 covered TS-only.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, QueueMoveElementsArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48e:';

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

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await raw.call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
    if (!probeOk) console.warn('[live interop48e] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48e] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48e: native bloom + blocking move + rate recycling vs Java 4.8.0', () => {
  it('RBloomFilterNative: raw-element bits shared TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'BF.RESERVE'))) {
      console.log('[live interop48e] BF.* unavailable — skipping RBloomFilterNative');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bfn`;
    const jname = `${NS}bfn-j`;
    try {
      await r.getKeys().delete(name, jname);

      // TS reserves + adds; Java probes the same RAW element bits
      const bfn = r.getBloomFilterNative(name);
      await bfn.reserve(1000, 0.01);
      expect(await bfn.add('ts-bfn-1')).toBe(true);
      expect(await bfn.add('ts-bfn-2')).toBe(true);
      expect(await probe48.probe(`bfn48-exists ${name} ts-bfn-1`)).toBe('exists=true');
      expect(await probe48.probe(`bfn48-exists ${name} ts-bfn-2`)).toBe('exists=true');
      expect(await probe48.probe(`bfn48-exists ${name} never-there`)).toBe('exists=false');
      expect(await probe48.probe(`bfn48-count ${name}`)).toBe('count=2');

      // Java reserves + adds; TS sees its elements (same raw domain)
      await probe48.probe(`bfn48-init ${jname} 0.01 1000`);
      expect(await probe48.probe(`bfn48-add ${jname} java-bfn-1`)).toBe('added=true');
      const jbfn = r.getBloomFilterNative(jname);
      expect(await jbfn.exists('java-bfn-1')).toBe(true);
      expect(await jbfn.exists('ts-bfn-1')).toBe(false);
      expect(await jbfn.count()).toBe(1);
    } finally {
      await r.getKeys().delete(name, jname);
      await r.shutdown();
    }
  }, 300000);

  it('RBlockingQueue.move(timeout, args): TS blocks, Java feeds, move completes', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'LMOVEM'))) {
      console.log('[live interop48e] LMOVEM unavailable — skipping blocking move');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}bq-src`;
    const dst = `${NS}bq-dst`;
    try {
      await r.getKeys().delete(src, dst);
      const bq = r.getBlockingQueue<string>(src);

      // TS consumer blocks on a dedicated connection (10s window); EXACTLY 2 —
      // unlike COUNT, it completes only when both elements are available
      const mv = bq.move(10000, QueueMoveElementsArgs.to(dst).exactly(2).bulk());
      await new Promise((res) => setTimeout(res, 200)); // let it reach BLMOVEM

      // first offer alone must not complete the move (checked implicitly by
      // the second being required); Java feeds both, then it migrates
      expect(await probe48.probe(`q48-offer ${src} w1`)).toBe('offered=true');
      expect(await probe48.probe(`q48-offer ${src} w2`)).toBe('offered=true');

      expect(await mv).toEqual(['w1', 'w2']); // decoded, arrival order
      expect(await probe48.probeLines(`q48-read ${dst}`)).toEqual(['value=w1', 'value=w2']);
      expect(await r.getQueue<string>(src).readAll()).toEqual([]);
    } finally {
      await r.getKeys().delete(src, dst);
      await r.shutdown();
    }
  }, 300000);

  it('RRateLimiter: TS-acquired permits recycled by release() are visible to Java', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rl`;
    try {
      await r.getKeys().delete(name, `${name}:value`, `${name}:permits`);
      const rl = r.getRateLimiter(name);
      expect(await rl.trySetRate('OVERALL', 2, 60000)).toBe(true);

      // TS drains the window
      expect(await rl.tryAcquire()).toBe(true);
      expect(await rl.tryAcquire()).toBe(true);
      expect(await probe48.probe(`rate48-try ${name}`)).toBe('allowed=false');

      // TS release recycles the permit — Java acquires it immediately
      await rl.release(1);
      expect(await probe48.probe(`rate48-try ${name}`)).toBe('allowed=true');

      // and the reverse read: Java drains, TS sees the emptiness
      expect(await probe48.probe(`rate48-try ${name}`)).toBe('allowed=false');
      expect(await rl.tryAcquire()).toBe(false);
    } finally {
      await r.getKeys().delete(name, `${name}:value`, `${name}:permits`);
      await r.shutdown();
    }
  }, 300000);
});
