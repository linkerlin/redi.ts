/**
 * Live interop vs **Java Redisson 4.8.0**, round 32 — RFunction, the last
 * execution-layer surface without 4.8.0-jar bidirectional evidence:
 *
 *  - TS `FUNCTION LOAD` (full payload incl. shebang) → Java `FCALL` runs
 *    the TS-registered function; Java's vararg VALUES are codec-encoded
 *    (bytecode: `encode(Collection, codec)`), so TS's bucket decode sees
 *    the clean string. Same asymmetry as RScript.
 *  - TS `FCALL` with an explicitly JSON-encoded arg → Java VALUE decode
 *    (FunctionResult.VALUE → codec) reads it; Java `FCALL_RO` runs the
 *    TS no-writes function.
 *  - Java `loadAndReplace(libName, BODY)` — Java builds the shebang line
 *    itself ("#!lua name=<lib> \n"; body hardcoded in the probe) → TS
 *    `FCALL`/`FCALL_RO` run the Java-registered functions; TS reads the
 *    raw bulk reply (`JSON.parse` to strip codec quotes).
 *  - `FUNCTION LIST` both ways: Java `list(lib)` function names, TS
 *    `list()` library discovery, Java `list()` all-libraries listing.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48af:';

// Full payload INCLUDING the shebang — TS load() sends it verbatim
// (Redisson's Java load() builds the shebang itself, TS does not).
const TS_LIB = [
  '#!lua name=p31tslib',
  "redis.register_function('p31tsset', function(keys, args) redis.call('set', keys[1], args[1]) return 1 end)",
  "redis.register_function{function_name='p31tsget', callback=function(keys, args) return redis.call('get', keys[1]) end, flags={'no-writes'}}",
].join('\n');

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
    console.warn(`[live interop48af] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

async function delLib(r: Redisson, lib: string): Promise<void> {
  try {
    await r.getFunction().delete(lib);
  } catch {
    // FUNCTION DELETE of an absent library errors — nothing to clean
  }
}

describe('live interop48af: RFunction FUNCTION LOAD / FCALL vs Java 4.8.0', () => {
  it('TS FUNCTION LOAD → Java FCALL / FCALL_RO runs TS-registered functions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}k1`;
    const k1b = `${NS}k1b`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k1, k1b]);
      await delLib(r, 'p31tslib');

      // TS loads the library (full payload with shebang)
      expect(await r.getFunction().load(TS_LIB)).toBe('p31tslib');

      // Java FCALLs the TS write function; its ARGV is codec-encoded
      // (quoted JSON), so TS's bucket decode sees the clean string
      expect(await probe48.probe(`fn48-callwrite p31tsset ${k1} java-via-ts-fn`)).toBe('value=1');
      expect(await r.getBucket<string>(k1).get()).toBe('java-via-ts-fn');

      // Java FCALL_ROs the TS no-writes read function → codec-decoded VALUE
      expect(await probe48.probe(`fn48-callread p31tsget ${k1}`)).toBe('value=java-via-ts-fn');

      // TS FCALLs its own function with an explicitly JSON-encoded arg →
      // Java VALUE decode reads the string (same asymmetry as RScript)
      expect(await r.getFunction().callFn('p31tsset', [k1b], [JSON.stringify('ts-via-ts-fn')])).toBe(1);
      expect(await r.getBucket<string>(k1b).get()).toBe('ts-via-ts-fn');
      expect(await probe48.probe(`fn48-callread p31tsget ${k1b}`)).toBe('value=ts-via-ts-fn');

      // Java FUNCTION LIST LIBRARYNAME sees both TS functions (sorted)
      expect(await probe48.probe(`fn48-fnexists p31tslib`)).toBe('fns=p31tsget,p31tsset');

      // TS pointed list() of the freshly loaded library round-trips
      const tslib = (await r.getFunction().list('p31tslib')).find((l) => l.name === 'p31tslib');
      expect(tslib).toBeDefined();
      expect([...tslib!.functions].sort()).toEqual(['p31tsget', 'p31tsset']);

      // Java FUNCTION LIST (all libraries) sees the TS library too
      expect(await probe48.probeLines(`fn48-listlibs`)).toContain('lib=p31tslib');
    } finally {
      await delAll(raw, [k1, k1b]);
      await delLib(r, 'p31tslib');
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('Java loadAndReplace → TS FCALL / FCALL_RO runs Java-registered functions; FUNCTION LIST both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k2 = `${NS}k2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k2]);
      await delLib(r, 'p31jlib');

      // Java loadAndReplace(libName, BODY) — Java builds the shebang itself
      expect(await probe48.probe(`fn48-load p31jlib`)).toBe('loaded=ok');

      // TS FCALLs the Java write function; arg JSON-encoded so a codec
      // reader would also decode it — raw reply is the integer
      expect(await r.getFunction().callFn('p31jset', [k2], [JSON.stringify('ts-via-java-fn')])).toBe(1);

      // TS FCALL_ROs the Java no-writes read function → RAW bulk reply
      // (the stored codec bytes, quotes included — JSON.parse to compare)
      const rawReply = await r.getFunction().callReadOnly('p31jget', [k2]);
      expect(JSON.parse(String(rawReply))).toBe('ts-via-java-fn');

      // TS FUNCTION LIST sees the Java library and its functions
      const libs = await r.getFunction().list();
      expect(libs.map((l) => l.name)).toContain('p31jlib');
      const jlib = libs.find((l) => l.name === 'p31jlib')!;
      expect([...jlib.functions].sort()).toEqual(['p31jget', 'p31jset']);

      // Java FUNCTION LIST (all libraries) sees the Java library
      expect(await probe48.probeLines(`fn48-listlibs`)).toContain('lib=p31jlib');
    } finally {
      await delAll(raw, [k2]);
      await delLib(r, 'p31jlib');
      await delLib(r, 'p31tslib');
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
