/**
 * Live interop vs **Java Redisson 4.8.0**, round 8 — multi-queue blocking
 * polls, semaphore permit drain and the BLMOVE relay:
 *
 *  - `pollFromAny`/`pollFirstFromAny`: the TS consumer blocks across
 *    several queues at once (BLPOP/BLMPOP over multiple keys); Java feeds
 *    one of them; the pop resolves from the fed queue (BLMPOP batch too).
 *  - `RSemaphore.drainPermits`: permits configured in one language are
 *    drained to zero by the other; `release` restores cross-language
 *    visibility. Probe commands `sem48-*`.
 *  - `pollLastAndOfferFirstTo(dest, timeout)` (BLMOVE RIGHT→LEFT): TS
 *    blocks on the source, Java feeds; the tailed element relays into the
 *    destination queue where Java reads it back.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48h:';

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
    if (!probeOk) console.warn('[live interop48h] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48h] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48h: multi-queue polls + semaphore drain + BLMOVE relay vs Java 4.8.0', () => {
  it('pollFromAny/pollFirstFromAny: TS blocks across queues, Java feeds one', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q1 = `${NS}q1`;
    const q2 = `${NS}q2`;
    try {
      await r.getKeys().delete(q1, q2);
      const bq = r.getBlockingQueue<string>(q1);

      // single-element BLPOP over [q1, q2]; Java feeds the SECOND queue
      const any = bq.pollFromAny(10000, q2);
      await new Promise((res) => setTimeout(res, 150));
      expect(await probe48.probe(`q48-offer ${q2} any1`)).toBe('offered=true');
      expect(await any).toBe('any1');

      // BLMPOP batch of 2: Java loads q1, TS multi-pops from [q1, q2]
      await probe48.probe(`q48-offer ${q1} b1`);
      await probe48.probe(`q48-offer ${q1} b2`);
      const batch = await bq.pollFirstFromAny(2000, 2, q2);
      expect([...batch.keys()]).toEqual([q1]);
      expect(batch.get(q1)).toEqual(['b1', 'b2']);
      expect(await r.getQueue<string>(q2).readAll()).toEqual([]);
    } finally {
      await r.getKeys().delete(q1, q2);
      await r.shutdown();
    }
  }, 300000);

  it('RSemaphore: drainPermits and release recycle permits across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sem`;
    const jname = `${NS}sem-j`;
    try {
      await r.getKeys().delete(name, jname);

      // TS configures 3 permits; Java sees them, drains all three
      const sem = r.getSemaphore(name);
      expect(await sem.trySetPermits(3)).toBe(true);
      expect(await probe48.probe(`sem48-available ${name}`)).toBe('available=3');
      expect(await probe48.probe(`sem48-drain ${name}`)).toBe('drained=3');
      expect(await sem.tryAcquire()).toBe(false);

      // TS release restores; Java consumes one
      await sem.release(2);
      expect(await probe48.probe(`sem48-available ${name}`)).toBe('available=2');
      expect(await probe48.probe(`sem48-acquire ${name}`)).toBe('acquired=true');
      expect(await sem.availablePermits()).toBe(1);

      // reverse: Java configures, TS drains to zero
      expect(await probe48.probe(`sem48-tryset ${jname} 4`)).toBe('set=true');
      expect(await r.getSemaphore(jname).drainPermits()).toBe(4);
      expect(await probe48.probe(`sem48-acquire ${jname}`)).toBe('acquired=false');
    } finally {
      await r.getKeys().delete(name, jname);
      await r.shutdown();
    }
  }, 300000);

  it('pollLastAndOfferFirstTo (BLMOVE): tailed element relays across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}blm-src`;
    const dst = `${NS}blm-dst`;
    try {
      await r.getKeys().delete(src, dst);
      const bq = r.getBlockingQueue<string>(src);

      // TS blocks on the source with a 10s window
      const relay = bq.pollLastAndOfferFirstTo(dst, 10000);
      await new Promise((res) => setTimeout(res, 150));
      await probe48.probe(`q48-offer ${src} r1`);
      await probe48.probe(`q48-offer ${src} r2`);

      // the blocking BLMOVE fires on the FIRST push (r1 was head and tail at
      // that moment); r2 arrived after and stays in the source
      expect(await relay).toBe('r1');
      expect(await probe48.probeLines(`q48-read ${dst}`)).toEqual(['value=r1']);
      expect(await r.getQueue<string>(src).readAll()).toEqual(['r2']);

      // with two resident elements the TAIL relays: r2 + r3 in the source,
      // the non-blocking variant (RPOPLPUSH) moves the newest tail to dst
      await probe48.probe(`q48-offer ${src} r3`);
      expect(await r.getBlockingQueue<string>(src).pollLastAndOfferFirstTo(dst)).toBe('r3');
      // RPOPLPUSH LPUSHes into dst — newest relayed element lands at the head
      expect(await probe48.probeLines(`q48-read ${dst}`)).toEqual(['value=r3', 'value=r1']);
      expect(await r.getQueue<string>(src).readAll()).toEqual(['r2']);
    } finally {
      await r.getKeys().delete(src, dst);
      await r.shutdown();
    }
  }, 300000);
});
