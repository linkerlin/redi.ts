/**
 * Live interop vs **Java Redisson 4.8.0**, round 9 — the hash-based
 * RBloomFilter, RLexSortedSet and RJsonBucket array/string ops:
 *
 *  - RBloomFilter here is the Lua/HighwayHash-128 one (bitmap + `{name}:config`
 *    HASH — NOT round 5's native BF.* filter). Cross-language contains proves
 *    the HighwayHash-128 port produces identical bit positions in both
 *    languages. Probe commands `bfl48-*`.
 *  - RLexSortedSet: codec'd ZSET ordered lexicographically; TS writes read
 *    by Java `range` and vice versa. Probe commands `lex48-*`.
 *  - RJsonBucket array/string ops (phase43 TS-only until now): TS
 *    `arrayAppend`/`stringAppend` observed by Java path reads; Java path
 *    writes consumed by TS `arrayIndex`/`arrayPop`.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48i:';

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
    if (!probeOk) console.warn('[live interop48i] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48i] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48i: hash bloom + lex set + JSON array ops vs Java 4.8.0', () => {
  it('RBloomFilter (HighwayHash): identical bit positions in both languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
      const name = `${NS}bfl`;
      const jname = `${NS}bfl-j`;
      const cfg = (n: string) => `{${n}}:config`; // suffixName hash-tag form
      try {
      await r.getKeys().delete(name, jname, cfg(name), cfg(jname));

      // TS inits the bitmap and adds; Java's contains hashes to the same bits
      const bf = r.getBloomFilter<string>(name);
      expect(await bf.tryInit(1000, 0.01)).toBe(true);
      expect(await bf.add('ts-hh-1')).toBe(true);
      expect(await bf.add('ts-hh-2')).toBe(true);
      expect(await probe48.probe(`bfl48-contains ${name} ts-hh-1`)).toBe('contains=true');
      expect(await probe48.probe(`bfl48-contains ${name} ts-hh-2`)).toBe('contains=true');
      expect(await probe48.probe(`bfl48-contains ${name} surely-absent-xyz`)).toBe('contains=false');

      // Java inits and adds; TS contains agrees on the same bits
      expect(await probe48.probe(`bfl48-init ${jname} 1000 0.01`)).toBe('init=true');
      expect(await probe48.probe(`bfl48-add ${jname} java-hh-1`)).toBe('added=true');
      const jbf = r.getBloomFilter<string>(jname);
      expect(await jbf.contains('java-hh-1')).toBe(true);
      expect(await jbf.contains('ts-hh-1')).toBe(false);
      expect(await jbf.count()).toBe(1);
    } finally {
      await r.getKeys().delete(name, jname, cfg(name), cfg(jname));
      await r.shutdown();
    }
  }, 300000);

  it('RLexSortedSet: lexicographic ordering shared across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lex`;
    try {
      await r.getKeys().delete(name);
      const lex = r.getLexSortedSet(name);

      // TS seeds; Java ranges by ELEMENT bounds (it adds the [ brackets
      // itself — passing raw lex syntax like '-'/ '+' would corrupt them)
      await lex.add('banana');
      await lex.add('apple');
      await lex.add('cherry');
      expect(await probe48.probeLines(`lex48-range ${name} apple cherry`))
        .toEqual(['v=apple', 'v=banana', 'v=cherry']);
      expect(await probe48.probe(`lex48-contains ${name} banana`)).toBe('contains=true');

      // Java appends; TS ranges with its own lex-syntax bounds + rank/count
      expect(await probe48.probe(`lex48-add ${name} date`)).toBe('added=true');
      expect(await lex.rangeByLex('[apple', '[date')).toEqual(['apple', 'banana', 'cherry', 'date']);
      expect(await lex.rank('cherry')).toBe(2);
      expect(await lex.countByLex('-', '+')).toBe(4);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RJsonBucket array/string ops: cross-language array mutation reads', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    let jsonUp = true;
    try {
      await raw.connect();
      const info = await raw.call('COMMAND', 'INFO', 'JSON.SET');
      jsonUp = Array.isArray(info) && info[0] !== null;
    } catch {
      jsonUp = false;
    }
    raw.disconnect();
    if (!jsonUp) {
      console.log('[live interop48i] JSON.* unavailable — skipping RJsonBucket array ops');
      return;
    }
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}jarr`;
    try {
      await r.getKeys().delete(name);
      const jb = r.getJsonBucket<any>(name);

      // TS appends into an array; Java reads the grown array back
      await jb.set({ arr: [1] });
      expect(await jb.arrayAppend('$.arr', 2, 3)).toBe(3);
      expect(await probe48.probe(`json48-get ${name} $.arr`)).toBe('json=[[1,2,3]]');

      // Java writes an array via a path; TS indexes and pops it
      await probe48.probe(`json48-setpath ${name} $.b {"x":[7,8]}`);
      expect(await jb.arrayIndex('$.b.x', 8)).toBe(1);
      expect(await jb.arrayPop('$.b.x', -1)).toBe(8);
      expect(await jb.arraySize('$.b.x')).toBe(1);

      // string append grows the string, visible to Java
      await jb.set('hi', '$.s');
      expect(await jb.stringAppend('$.s', '!')).toBe(3);
      expect(await probe48.probe(`json48-get ${name} $.s`)).toBe('json=["hi!"]');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
