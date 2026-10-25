/**
 * Live interop vs **Java Redisson 4.8.0**, round 44 — three families the
 * earlier rounds did not cover:
 *
 *  - RExpirable conditional expire (4.8 `expireIfSet/IfNotSet/IfGreater/
 *    IfLess`, Duration overloads): applied from one language, observed
 *    from the other — Set arms only when a TTL exists, NotSet only when
 *    none does, Greater/Less compare against the current TTL.
 *  - RTimeSeriesNative rules (TS.CREATERULE): TS adds samples → Java
 *    attaches an AVG 10s compaction rule to an existing dest → the dest
 *    fills with compacted samples; reverse via TS's own createRule over
 *    Java-added samples.
 *  - RStream XACKDEL (Redis 8.2+): TS consumes entries into a group PEL,
 *    Java's `ack(StreamAckArgs)` ack+deletes them — the entries leave
 *    BOTH the PEL and the stream; TS reads statuses for a mixed
 *    existing/absent id set.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, TSRuleArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ar:';

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
    console.warn(`[live interop48ar] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ar: conditional expire / TS-native rules / stream ackdel vs Java 4.8.0', () => {
  it('RExpirable expireIf*: conditions evaluated cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}cond`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k]);
      const b = r.getBucket<string>(k);
      await b.set('v');

      // NX arms only WITHOUT a ttl (Java then TS both refuse a second arm)
      expect(await probe48.probe(`exp48-if ${k} notset 25000`)).toBe('ok=true');
      expect(await b.expireIfNotSet(30000)).toBe(false);
      expect(await b.remainTimeToLive()).toBeGreaterThan(20000);

      // XX arms only WITH a ttl — cleared first, both sides refuse
      await b.clearExpire();
      expect(await b.expireIfSet(25000)).toBe(false);
      expect(await probe48.probe(`exp48-if ${k} set 20000`)).toBe('ok=false');

      // re-arm (TS NX) and now XX works from Java
      expect(await b.expireIfNotSet(20000)).toBe(true);
      expect(await probe48.probe(`exp48-if ${k} set 25000`)).toBe('ok=true');

      // GT arms only above the current ttl
      expect(await b.expireIfGreater(10000)).toBe(false);
      expect(await b.expireIfGreater(40000)).toBe(true);
      expect(await b.remainTimeToLive()).toBeGreaterThan(35000);

      // LT arms only below it — Java observes
      expect(await probe48.probe(`exp48-if ${k} less 45000`)).toBe('ok=false');
      expect(await probe48.probe(`exp48-if ${k} less 10000`)).toBe('ok=true');
      expect(await b.remainTimeToLive()).toBeLessThanOrEqual(10500);
    } finally {
      await delAll(raw, [k]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RTimeSeriesNative rules: compaction destinations shared across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}tsn-src`;
    const dst = `${NS}tsn-dst`;
    const raw = new Redis(REDIS_URL);
    try {
      // TS.CREATERULE needs BOTH series to exist; buckets compact only
      // when CLOSED (a later-bucket sample arrives)
      const tsn = r.getTimeSeriesNative(src);
      await tsn.add(500, 1); // creates the source
      expect(await probe48.probe(`tsn48-create ${dst} kind=avg`)).toBe('create=ok');

      // Java attaches the AVG 10s rule; TS writes land and the bucket
      // closes on the next-bucket sample → dst holds AVG(4,6)=5 at ts 0
      expect(await probe48.probe(`tsn48-addrule ${src} ${dst} AVG 10000`)).toBe('rule=ok');
      await tsn.add(1000, 4);
      await tsn.add(2000, 6);
      await tsn.add(11000, 8); // closes bucket 0
      expect(await probe48.probe(`tsn48-ruledest ${dst}`)).toBe('firstTs=0 size=1');
      expect(await r.getTimeSeriesNative(dst).first()).toEqual({ timestamp: 0, values: [5] });

      // Reverse: TS attaches the SUM rule, THEN Java writes — compaction
      // only processes post-rule samples (the forward case's AVG(4,6)=5
      // likewise excluded its pre-rule seed)
      const src2 = `${NS}tsn-src2`;
      const dst2 = `${NS}tsn-dst2`;
      await delAll(raw, [src2, dst2]);
      await probe48.probe(`tsn48-add ${src2} 500 1`); // creates the source (pre-rule)
      await probe48.probe(`tsn48-create ${dst2} kind=avg2`);
      await r.getTimeSeriesNative(src2).createRule(TSRuleArgs.destination(dst2, 'SUM', 10000));
      await probe48.probe(`tsn48-add ${src2} 1000 10`);
      await probe48.probe(`tsn48-add ${src2} 2000 20`);
      await probe48.probe(`tsn48-add ${src2} 11000 5`); // closes bucket 0
      expect(await r.getTimeSeriesNative(dst2).first()).toEqual({ timestamp: 0, values: [30] });
    } finally {
      await delAll(raw, [src, dst, `${NS}tsn-src2`, `${NS}tsn-dst2`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream XACKDEL: Java ack+deletes TS-consumed PEL entries', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}st`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s]);
      const stream = r.getStream<string>(s);

      // group FIRST (Java's builder defaults to '$' — only new entries
      // deliver), then TS adds and consumes into its PEL
      expect(await probe48.probe(`stream48-create-group ${s} g44`)).toBe('group=ok');
      const id1 = await stream.add({ f: 'v1' });
      const id2 = await stream.add({ f: 'v2' });
      const got = await stream.readGroup('g44', 'c44', { count: 10 });
      expect(got.map((e) => e.id)).toEqual([id1, id2]);

      // Java ack+deletes both (XACKDEL: PEL + stream entries)
      const lines = await probe48.probeLines(`stream48-ackdel ${s} g44 DELREF ${id1} ${id2}`);
      expect(lines.some((x) => x.includes(`id=${id1}`))).toBe(true);
      expect(lines.some((x) => x.includes(`id=${id2}`))).toBe(true);
      expect(await stream.size()).toBe(0);

      // TS-side statuses for a mixed existing/absent set
      await stream.add({ f: 'v3' });
      const id3 = (await stream.read(10))[0].id;
      await stream.readGroup('g44', 'c44', { count: 10 });
      const statuses = await stream.ackWithDelete('g44', 'DELREF', id3, '9999999999999-0');
      expect(statuses.get(id3)).toBeGreaterThanOrEqual(0);
      expect(statuses.get('9999999999999-0')).toBe(-1);
      expect(await stream.size()).toBe(0);
    } finally {
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
