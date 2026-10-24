/**
 * Live interop vs **Java Redisson 4.8.0**, round 21 — the ZSET / ring /
 * set-cache families had thorough 4.6.1-probe coverage but NO 4.8.0-jar
 * evidence until now:
 *
 *  - RScoredSortedSet: codec-encoded members + scores read back in both
 *    directions, rank/first/last/pollFirst/addScore/count agreement.
 *  - RRingBuffer: capacity trim round-trips through both writers
 *    (`redisson_rb:{name}` settings key on both sides).
 *  - RSetCache: **found + fixed a real wire bug** — TS scored no-TTL
 *    entries MaxInt64, but Java scores them `92233720368547758L - now`
 *    (Long.MAX_VALUE/100, bytecode-verified on 4.6.1 AND 4.8.0) and
 *    readAll's ZRANGEBYSCORE ceiling is that same constant, so TS-written
 *    no-TTL entries were INVISIBLE to Java readAll. src/caches.ts now
 *    writes Java's formula; the sc48 test below asserts the fixed
 *    direction explicitly (TS no-TTL entry read back by Java).
 *
 * Probe commands zset48-*, ring48-*, sc48-*.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { prefixName, suffixName } from '../src/util';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48u:';

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
    console.warn(`[live interop48u] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

describe('live interop48u: scored-sorted-set / ring buffer / set cache vs Java 4.8.0', () => {
  it('RScoredSortedSet: Java writes → TS reads; TS arithmetic → Java reads', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}z1`;
    try {
      await r.getKeys().delete(name);

      expect(await probe48.probe(`zset48-addmap ${name} 1.5 a 2.5 b 3.5 c`)).toBe('added=3');
      const z = r.getScoredSortedSet<string>(name);
      expect(await z.getScore('a')).toBe(1.5);
      expect(await z.getScore('b')).toBe(2.5);
      expect(await z.first()).toBe('a');
      expect(await z.last()).toBe('c');
      expect(await z.rank('b')).toBe(1);
      expect(await z.size()).toBe(3);
      expect(await z.contains('b')).toBe(true);

      // TS poll removes the lowest; Java's first() shifts accordingly
      const first = await z.pollFirstEntry();
      expect(first?.value).toBe('a');
      expect(first?.score).toBe(1.5);
      expect(await probe48.probe(`zset48-firstlast ${name}`)).toBe('first=b last=c');

      // TS score arithmetic visible to Java; Java add visible to TS
      expect(await z.addScore('c', 10)).toBe(13.5);
      expect(await probe48.probe(`zset48-score ${name} c`)).toBe('score=13.5');
      expect(await probe48.probe(`zset48-add ${name} 14.5 d`)).toBe('added=true');
      expect(await z.last()).toBe('d');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RScoredSortedSet: TS writes → Java reads, GT conditionals agree', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}z2`;
    try {
      await r.getKeys().delete(name);
      const z = r.getScoredSortedSet<string>(name);

      expect(await z.addAll(new Map([['x', 10.5], ['y', 20.5]]))).toBe(2);
      expect(await probe48.probe(`zset48-score ${name} x`)).toBe('score=10.5');
      expect(await probe48.probe(`zset48-rank ${name} y`)).toBe('rank=1');

      expect(await z.add(7, 'm')).toBe(true);
      expect(await z.addIfGreater(3, 'm')).toBe(false);
      expect(await probe48.probe(`zset48-score ${name} m`)).toBe('score=7.0'); // Java Double.toString
      expect(await z.addIfGreater(9, 'm')).toBe(true);
      expect(await probe48.probe(`zset48-score ${name} m`)).toBe('score=9.0');

      const lines = await probe48.probeLines(`zset48-readall ${name}`);
      expect(lines.map((l) => l.replace('value=', '')).sort()).toEqual(['m', 'x', 'y']);
      expect(await probe48.probe(`zset48-count ${name} 5 15`)).toBe('count=2'); // m=9, x=10.5
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RRingBuffer: capacity trim round-trips through both writers', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}rb`;
    const settings = prefixName('redisson_rb', name);
    try {
      await r.getKeys().delete(name, settings);

      expect(await probe48.probe(`ring48-setcap ${name} 3`)).toBe('set=true');
      const rb = r.getRingBuffer<string>(name);
      expect(await rb.capacity()).toBe(3);

      // Java overfills: a is trimmed
      for (const v of ['a', 'b', 'c', 'd']) {
        expect(await probe48.probe(`ring48-add ${name} ${v}`)).toBe('added=ok');
      }
      expect(await rb.size()).toBe(3);
      expect(await rb.readOldest(3)).toEqual(['b', 'c', 'd']);
      expect(await rb.peekLast()).toBe('d');

      // TS overfills; Java sees the trimmed window
      await rb.add('e');
      const lines = await probe48.probeLines(`ring48-readnewest ${name} 3`);
      expect(lines.map((l) => l.replace('value=', '')).sort()).toEqual(['c', 'd', 'e']);
      expect(await probe48.probe(`ring48-peeklast ${name}`)).toBe('last=e');
      expect(await probe48.probe(`ring48-stat ${name}`)).toBe('size=3 capacity=3 remaining=0');
    } finally {
      await r.getKeys().delete(name, settings);
      await r.shutdown();
    }
  }, 300000);

  it('RSetCache: no-TTL score formula fix — Java readAll sees TS no-TTL entries', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sc`;
    const sc = r.getSetCache<string>(name);
    const companions = [name, prefixName('redisson__idle__set', name), suffixName(name, 'redisson_touch')];
    try {
      await r.getKeys().delete(...companions);

      // Java writes one no-TTL + one TTL entry; TS reads both
      expect(await probe48.probe(`sc48-add ${name} j-noTtl 0`)).toBe('added=true');
      expect(await probe48.probe(`sc48-add ${name} j-ttl 25000`)).toBe('added=true');
      expect((await sc.readAll()).sort()).toEqual(['j-noTtl', 'j-ttl']);

      // TS writes a NO-TTL entry (Java formula since this round's fix) —
      // Java readAll's 92233720368547758 ceiling must include it
      expect(await sc.add('ts-noTtl')).toBe(true);
      const lines = await probe48.probeLines(`sc48-readall ${name}`);
      expect(lines.map((l) => l.replace('value=', ''))).toEqual(['j-noTtl', 'j-ttl', 'ts-noTtl']);
      expect(await sc.size()).toBe(3);
      expect(await sc.contains('ts-noTtl')).toBe(true);
      expect(await probe48.probe(`sc48-contains ${name} j-noTtl`)).toBe('contains=true');

      // gone-after (law 10): the 25s TTL entry drops out on both sides
      await sleep(27000);
      expect((await sc.readAll()).sort()).toEqual(['j-noTtl', 'ts-noTtl']);
      expect(await probe48.probe(`sc48-size ${name}`)).toBe('size=2');
    } finally {
      await r.getKeys().delete(...companions);
      await r.shutdown();
    }
  }, 300000);
});
