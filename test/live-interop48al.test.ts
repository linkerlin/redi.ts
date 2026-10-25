/**
 * Live interop vs **Java Redisson 4.8.0**, round 38 — two permit/queue
 * paths the earlier rounds did not cover:
 *
 *  - BLMPOP multi-queue blocking polls: TS `pollLastFromAnyWithName`
 *    (BRPOP with name, over two queues) parks and a Java offer into the
 *    SECOND queue wakes it and reports the source; a Java background
 *    `pollFirstFromAny(wait, MS, names...)` is woken by a TS offer into
 *    the second queue.
 *  - RSemaphore `addPermits(delta)` accounting across languages: TS
 *    grant/acquire, Java acquire/release, then positive and negative
 *    addPermits from either side — `availablePermits` agrees everywhere
 *    and a cross-language acquire succeeds at the end.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48al:';

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
    console.warn(`[live interop48al] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48al: BLMPOP multi-queue wakes / semaphore addPermits vs Java 4.8.0', () => {
  it('BLMPOP: multi-queue blocking polls wake across languages, source named', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const qa = `${NS}qa`;
    const qb = `${NS}qb`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [qa, qb]);

      // TS parks on pollLastFromAnyWithName over [qa, qb]; a Java offer
      // into the SECOND queue (qb) wakes it and reports the source
      const p = r.getBlockingDeque<string>(qa).pollLastFromAnyWithName(8000, qb);
      await sleep(300);
      expect(await probe48.probe(`bd48-offerlast ${qb} java-blm`)).toBe('offered=true');
      const got = await p;
      expect(got).toEqual({ key: qb, value: 'java-blm' });

      // Java background parks on BLMPOP pollFirstFromAny(qa, qb, COUNT 1);
      // a TS offer into the second queue (qb) wakes it. (The DEQUE variant
      // of pollFirstFromAny ignores the extra names — parks on self only;
      // RedissonBlockingDeque line 121. The queue count variant is the real
      // multi-queue BLMPOP.)
      expect(await probe48.probe(`blm48-bgpollfirstany 8000 ${qa} ${qb}`)).toBe('armed');
      await sleep(300);
      await r.getQueue<string>(qb).add('ts-blm');
      expect(await probe48.probe(`blm48-bgresult 9000`)).toBe('value=ts-blm');
    } finally {
      await delAll(raw, [qa, qb]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RSemaphore addPermits: cross-language grant/deficit accounting agrees', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}sem`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s]);
      const sem = r.getSemaphore(s);

      // TS grants 2; Java acquires one → 1 left, both sides agree
      expect(await sem.trySetPermits(2)).toBe(true);
      expect(await probe48.probe(`sem48-acquire ${s}`)).toBe('acquired=true');
      expect(await probe48.probe(`sem48-available ${s}`)).toBe('available=1');
      expect(await sem.availablePermits()).toBe(1);

      // TS addPermits(+3) → 4 everywhere; Java releases → 5
      await sem.addPermits(3);
      expect(await probe48.probe(`sem48-available ${s}`)).toBe('available=4');
      expect(await probe48.probe(`sem48-release ${s}`)).toBe('released=ok');
      expect(await sem.availablePermits()).toBe(5);

      // Java addPermits(-2) → 3; TS acquire succeeds and sees 2 left
      expect(await probe48.probe(`sem48-addpermits ${s} -2`)).toBe('added=ok');
      expect(await sem.availablePermits()).toBe(3);
      expect(await sem.tryAcquire()).toBe(true);
      expect(await probe48.probe(`sem48-available ${s}`)).toBe('available=2');
    } finally {
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
