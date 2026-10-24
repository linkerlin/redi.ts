/**
 * Live interop vs **Java Redisson 4.8.0**, round 29 — reliable delivery and
 * set algebra, the last surfaces with only 4.6.1-era evidence:
 *
 *  - RReliableTopic: STREAM field `m` + one subscriber group per client
 *    (created at 0, destroyed when the last listener leaves). publish()
 *    returns the live group count on both sides; payloads are codec JSON
 *    read through XREADGROUP. TS's consume loop must decode Java's
 *    XADD-made entries and vice versa.
 *  - RSet algebra: union/diff/intersection STORE variants overwrite SELF
 *    with union(others) — self is only the SUNIONSTORE DESTINATION, NOT an
 *    input (bytecode-identical in 4.6.1 and 4.8.0; TS already matched);
 *    read* variants DO read self as first input.
 *  - RScoredSortedSet algebra: Java's `union(Aggregate, String...)` MAX
 *    merge vs TS `unionWith({names, aggregate})` SUM merge — scores meet
 *    on the same members both directions (Java prints whole doubles as
 *    "1.0").
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ac:';

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
    console.warn(`[live interop48ac] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
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

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ac: reliable topic / set algebra vs Java 4.8.0', () => {
  it('RReliableTopic: XADD field m + per-client groups, both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const n1 = `${NS}rt1`;
    const n2 = `${NS}rt2`;
    const n3 = `${NS}rt3`;
    const raw = new Redis(REDIS_URL);
    const companions = [n1, n2, n3, `{${n1}}:timeout`, `{${n2}}:timeout`, `{${n3}}:timeout`];
    try {
      await delAll(raw, companions);

      // TS listens (its group created at 0) → Java publishes → TS decodes
      const rt1 = r.getReliableTopic<string>(n1);
      let resolveGot: (v: string) => void = () => undefined;
      const gotP = new Promise<string>((res) => { resolveGot = res; });
      const id = await rt1.addListener((msg) => resolveGot(msg));
      expect(await probe48.probe(`rtopic48-pub ${n1} java-rt-msg`)).toBe('published=1');
      expect(await withSafety(gotP, 15000, 'reliable topic message')).toBe('java-rt-msg');
      await rt1.removeAllListeners(); // last listener out → group destroyed

      // nobody has a group on a fresh topic → publish counts 0 and the
      // entry STAYS (consumed-by-all entries are deleted; groupless aren't)
      expect(await probe48.probe(`rtopic48-pub ${n3} lonely`)).toBe('published=0');
      expect(await probe48.probe(`rtopic48-size ${n3}`)).toBe('size=1');

      // Java listens → TS publishes → Java receives; publish returns 1
      expect(await probe48.probe(`rtopic48-listen ${n2}`)).toMatch(/^listening=/);
      expect(await r.getReliableTopic<string>(n2).publish('ts-rt-msg')).toBe(1);
      expect(await probe48.probe(`rtopic48-poll 8000`)).toBe('msg=ts-rt-msg');
      await probe48.probe(`rtopic48-rmlistener ${n2}`);
    } finally {
      await delAll(raw, companions);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RSet: union store into self (Java→TS), intersection store (TS), readUnion', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}set1`;
    const s2 = `${NS}set2`;
    const s3 = `${NS}set3`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s1, s2, s3]);

      // TS writes s1={a,b,c}; Java writes s2={d,b,e}
      await r.getSet<string>(s1).addAll(['a', 'b', 'c']);
      expect(await probe48.probe(`set48-addall ${s2} d b e`)).toBe('OK');

      // Java unions s2 INTO s1 — self is only the DESTINATION (args[0] of
      // SUNIONSTORE), NOT an input: s1 is overwritten with {b,d,e}
      expect(await probe48.probe(`set48-union ${s1} ${s2}`)).toBe('count=3');
      expect([...(await r.getSet<string>(s1).readAll())].sort()).toEqual(['b', 'd', 'e']);

      // TS intersects s1 ∩ s2 into s3 — seeded with {f,x} to PROVE self
      // exclusion (self-as-input would yield {}): after the union s1==s2,
      // so the result is {b,d,e}
      await r.getSet<string>(s3).addAll(['f', 'x']);
      expect(await r.getSet<string>(s3).intersection(s1, s2)).toBe(3);
      expect(await probe48.probeLines(`set48-readall ${s3}`)).toEqual(['value=b', 'value=d', 'value=e']);

      // TS readUnion READS with self as first input (SUNION self others):
      // a fresh s4={f,x} reads as s4 ∪ s1 ∪ s2 = {b,d,e,f,x}, and s4 itself
      // is untouched (read does not store)
      const s4 = `${NS}set4`;
      await delAll(raw, [s4]);
      await r.getSet<string>(s4).addAll(['f', 'x']);
      const u = await r.getSet<string>(s4).readUnion(s1, s2);
      expect([...u].sort()).toEqual(['b', 'd', 'e', 'f', 'x']);
      expect([...(await r.getSet<string>(s4).readAll())].sort()).toEqual(['f', 'x']);
    } finally {
      await delAll(raw, [s1, s2, s3]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RScoredSortedSet: Java union MAX and TS unionWith SUM merge cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const z1 = `${NS}z1`;
    const z2 = `${NS}z2`;
    const z3 = `${NS}z3-max`;
    const z4 = `${NS}z4-sum`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [z1, z2, z3, z4]);

      // Java writes z1: a=1 b=2 c=3; TS writes z2: b=2.5 c=0.5 d=4
      expect(await probe48.probe(`zset48-addpairs ${z1} a 1.0 b 2.0 c 3.0`)).toBe('added=3');
      const z = r.getScoredSortedSet<string>(z2);
      expect(await z.add(2.5, 'b')).toBe(true);
      expect(await z.add(0.5, 'c')).toBe(true);
      expect(await z.add(4, 'd')).toBe(true);

      // Java unions z1+z2 into z3 with MAX: a=1 b=2.5 c=3 d=4
      expect(await probe48.probe(`zset48-union ${z3} MAX ${z1} ${z2}`)).toBe('count=4');
      // TS reads the Java-merged set (whole doubles print as "1.0")
      expect(await probe48.probeLines(`zset48-dump ${z3}`)).toEqual([
        'entry=a@1.0', 'entry=b@2.5', 'entry=c@3.0', 'entry=d@4.0',
      ]);

      // TS unions z1+z2 into z4 with SUM: a=1 b=4.5 c=3.5 d=4
      const z4s = r.getScoredSortedSet<string>(z4);
      expect(await z4s.unionWith({ names: [z1, z2], aggregate: 'SUM' })).toBe(4);
      // Java reads the TS-merged set
      expect(await probe48.probeLines(`zset48-dump ${z4}`)).toEqual([
        'entry=a@1.0', 'entry=b@4.5', 'entry=c@3.5', 'entry=d@4.0',
      ]);
    } finally {
      await delAll(raw, [z1, z2, z3, z4]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
