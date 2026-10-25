/**
 * Live interop vs **Java Redisson 4.8.0**, round 46 — interop evidence
 * for the families closed by the systematic API-surface audit (javap of
 * every `org.redisson.api.R*` in the 4.8.0 jar diffed against the TS
 * classes; this round closes the three biggest gaps):
 *
 *  - RLexSortedSet full family (~17 NEW TS methods): range/rangeHead/
 *    rangeTail (+Reversed, inclusive-flag bounds), count/countHead/
 *    countTail, removeRange family, revRank, pollFirst/pollLast,
 *    random — both languages read the identical views of one ZSET.
 *  - RScoredSortedSet additions (11 NEW TS methods): rankEntry/
 *    revRankEntry, pollFirstEntries/pollLastEntries (ZPOPMIN/MAX n),
 *    rangeTo/revRangeTo (ZRANGESTORE), removeAll/retainAll, isEmpty,
 *    toArray.
 *  - RSetCache conditional add family (Java 4.8 Lua, ported verbatim):
 *    addIfAbsent/IfExists/IfLess/IfGreater(Duration, V) + bulk variants
 *    + tryAdd — conditions agree cross-language on the same member.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48at:';

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
    console.warn(`[live interop48at] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48at: audit-closed families (lex / scored / setcache) vs Java 4.8.0', () => {
  it('RLexSortedSet family: identical lex views across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const z = `${NS}lex`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [z]);
      const lex = r.getLexSortedSet(z);
      for (const v of ['apple', 'banana', 'cherry', 'date']) await lex.add(v);

      // Java rangeHead (exclusive) vs TS rangeHead (inclusive)
      expect(await probe48.probeLines(`lex48-rangehead ${z} cherry 0`)).toEqual(['v=apple', 'v=banana']);
      expect(await lex.rangeHead('cherry', true)).toEqual(['apple', 'banana', 'cherry']);

      // TS rangeTail + count + reversed views; Java reads the same
      expect(await lex.rangeTail('banana', false)).toEqual(['cherry', 'date']);
      expect(await lex.countHead('cherry', true)).toBe(3);
      // '[d' inclusive of 'd' still EXCLUDES 'date' lexically ('date' > 'd')
      expect(await lex.rangeReversed('a', true, 'd', true)).toEqual(['cherry', 'banana', 'apple']);
      expect(await lex.revRank('apple')).toBe(3);
      expect(await probe48.probe(`lex48-revrank ${z} apple`)).toBe('revrank=3');

      // poll both ends cross-language (Java pops first, TS pops last)
      expect(await probe48.probe(`lex48-pollfirst ${z}`)).toBe('value=apple');
      expect(await lex.pollLast()).toBe('date');
      expect(await lex.size()).toBe(2);

      // TS removeRangeTail (INCLUSIVE drops cherry); only banana remains
      expect(await lex.removeRangeTail('cherry', true)).toBe(1);
      expect(await lex.random()).toBe('banana');
    } finally {
      await delAll(raw, [z]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RScoredSortedSet additions: rankEntry/pollEntries/rangeTo/retainAll across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const z = `${NS}z`;
    const dest = `${NS}zdest`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [z, dest]);
      const zs = r.getScoredSortedSet<string>(z);
      await zs.add(1, 'a');
      await zs.add(2, 'b');
      await zs.add(3, 'c');

      // rankEntry both directions (Java reads, TS reads)
      expect(await probe48.probe(`zset48-rankentry ${z} b`)).toBe('rank=1 score=2.0');
      const re = await zs.rankEntry('b');
      expect(re).toEqual({ rank: 1, score: 2 });

      // Java pops the two lowest as entries; TS's rank of the survivor shifts
      const popped = await probe48.probeLines(`zset48-pollentries ${z} first 2`);
      expect(popped).toEqual(['value=a@1.0', 'value=b@2.0']);
      expect(await zs.rank('c')).toBe(0);

      // TS re-adds and stores a range into a destination (ZRANGESTORE)
      await zs.add(1, 'a');
      await zs.add(2, 'b');
      expect(await zs.rangeTo(dest, 0, 1)).toBe(2);
      expect(await probe48.probe(`zset48-rangeto ${z} ${dest} 0 0`)).toBe('stored=1');

      // TS retainAll; Java observes the drop
      expect(await zs.retainAll(['c', 'a'])).toBe(true); // drops b
      expect((await zs.readAll()).sort()).toEqual(['a', 'c']);
      expect(await zs.isEmpty()).toBe(false);
      expect(await zs.removeAll(['a', 'ghost'])).toBe(true);
      expect(await probe48.probe(`zset48-retainall ${z} c zz`)).toBe('changed=false'); // zz missing → false? no: keep {c,zz}, c stays, nothing dropped
      expect((await zs.readAll())).toEqual(['c']);
    } finally {
      await delAll(raw, [z, dest]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RSetCache conditional adds: the same conditions agree across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}sc`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [s]);
      const sc = r.getSetCache<string>(s);

      // Java addIfAbsent on a fresh member → true; TS addIfAbsent sees it live → false
      expect(await probe48.probe(`sc48-addif ${s} absent m 60000`)).toBe('ok=true');
      expect(await sc.addIfAbsent('m', 60000)).toBe(false);

      // Java addIfExists refreshes the live member → true; TS addIfGreater
      // with a LONGER ttl succeeds, addIfLess with a shorter one fails
      expect(await probe48.probe(`sc48-addif ${s} exists m 120000`)).toBe('ok=true');
      expect(await sc.addIfGreater('m', 240000)).toBe(true);
      // addIfLess SHORTENS: 30s < the current 240s window → allowed
      expect(await sc.addIfLess('m', 30000)).toBe(true);
      // lengthening through addIfLess is refused
      expect(await sc.addIfLess('m', 300000)).toBe(false);

      // TS addIfAbsent on an EXPIRED window refreshes (gone-after style:
      // short lease, wait, re-add wins)
      expect(await sc.addIfAbsent('n', 500)).toBe(true);
      await new Promise((res) => setTimeout(res, 900));
      expect(await sc.addIfAbsent('n', 60000)).toBe(true);
      expect(await sc.contains('n')).toBe(true);

      // tryAdd (no TTL) lands the no-TTL score Java can read
      expect(await sc.tryAdd('t')).toBe(true);
      expect(await probe48.probe(`sc48-contains ${s} t`)).toBe('contains=true');
    } finally {
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
