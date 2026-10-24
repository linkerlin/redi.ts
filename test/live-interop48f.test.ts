/**
 * Live interop vs **Java Redisson 4.8.0**, round 6 — TimeSeries labels and
 * RJsonBucket deep paths, the two subtlest wire surfaces left:
 *
 *  - Labels (AGENTS law 12): the label travels as a codec-encoded blob
 *    (`"jlbl"` WITH quotes) on flag-2/3 members. Java 4.8's
 *    entryRangeByLabel must find TS-written members by label and vice
 *    versa — same encoded bytes both directions.
 *  - JSON deep paths: TS document writes read by Java through JacksonCodec
 *    path queries; Java path writes read precisely by TS; NX (setIfAbsent)
 *    semantics agree cross-language. TS `get(path)` returns the RedisJSON
 *    match ARRAY (phase35 convention: `$.a` → `[1]`).
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48f:';

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
    if (!probeOk) console.warn('[live interop48f] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48f] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48f: TimeSeries labels + RJsonBucket deep paths vs Java 4.8.0', () => {
  it('TimeSeries labels: encoded blobs match, query-by-label works both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    // two separate series: Java 4.8's add(long,V,L) runs the seq-HASH Lua
    // while TS's old API keeps seq STRING — one series must not mix the
    // families (AGENTS law 12), so each direction gets its own key
    const nameA = `${NS}ts-a`;
    const nameB = `${NS}ts-b`;
    try {
      await r.getKeys().delete(nameA, nameB, `redisson__ts_seq:{${nameA}}`, `redisson__ts_ttl:{${nameA}}`,
        `redisson__ts_seq:{${nameB}}`, `redisson__ts_ttl:{${nameB}}`);
      const t0 = Date.now() - 10000;

      // direction 1 (series A): TS writes with the codec-encoded label
      // (flag-2/3 shape) + one unlabeled member
      const tsA = r.getTimeSeries<string>(nameA);
      await tsA.add(t0 + 100, 'v1', 0, 'jlbl');
      await tsA.add(t0 + 200, 'v2');
      // Java queries BY label: only the labeled member — the encoded blob TS
      // wrote must equal what Java compares against
      expect(await probe48.probeLines(`ts48-rangebylabel ${nameA} ${t0} ${t0 + 1000} jlbl`))
        .toEqual([`${t0 + 100}=v1`]);
      expect(await tsA.rangeByLabel(t0, t0 + 1000, 'jlbl')).toEqual(['v1']);

      // direction 2 (series B): Java writes labeled with its own old-API
      // signature; TS queries BY label back (read side handles the shape)
      await probe48.probe(`ts48-addlabel ${nameB} ${t0 + 300} v3 jlbl2`);
      expect(await r.getTimeSeries<string>(nameB).rangeByLabel(t0, t0 + 1000, 'jlbl2')).toEqual(['v3']);
    } finally {
      await r.getKeys().delete(nameA, nameB, `redisson__ts_seq:{${nameA}}`, `redisson__ts_ttl:{${nameA}}`,
        `redisson__ts_seq:{${nameB}}`, `redisson__ts_ttl:{${nameB}}`);
      await r.shutdown();
    }
  }, 300000);

  it('RJsonBucket deep paths: doc/path writes and NX semantics cross-language', async () => {
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
      console.log('[live interop48f] JSON.* unavailable — skipping RJsonBucket');
      return;
    }
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}json`;
    try {
      await r.getKeys().delete(name);
      const jb = r.getJsonBucket<any>(name);

      // TS writes the document; Java reads through a path query (single-path
      // JSON.GET returns the match array, not a path-keyed object)
      await jb.set({ a: 1, nested: { x: 2 } });
      expect(await probe48.probe(`json48-get ${name} $.nested.x`)).toBe('json=[2]');

      // Java writes a nested path; TS reads it precisely (match-array shape)
      await probe48.probe(`json48-setpath ${name} $.b {"c":3}`);
      expect(await jb.get('$.b.c')).toEqual([3]);

      // TS overwrites a path; Java sees the new value
      await jb.set({ c: 4 }, '$.b');
      expect(await probe48.probe(`json48-get ${name} $.b.c`)).toContain('4');

      // NX semantics agree: occupied path rejected on both sides
      expect(await jb.setIfAbsent('$.a', 9)).toBe(false);
      expect(await probe48.probe(`json48-setnx ${name} $.a {"z":9}`)).toBe('setnx=false');
      await probe48.probe(`json48-setnx ${name} $.d {"e":5}`);
      expect(await jb.get('$.d.e')).toEqual([5]);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
