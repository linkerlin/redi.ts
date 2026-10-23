/**
 * Phase 74 live: bidirectional interop against **Java Redisson 4.8.0**
 * (interop/pom48.xml → TsProbe48, resident REPL via helpers/java-probe48.ts).
 *
 * The 4.6.1 probe suite proves the legacy wire; this file is the equivalent
 * evidence for the 4.8-only surfaces:
 *  - RTimeSeries `add(TimeSeriesAddArgs)` family — shared flag-4 members AND
 *    the shared `redisson__ts_seq:{name}` HASH counter (identical key names,
 *    verified against RedissonTimeSeries 4.8 source);
 *  - RArray AR.* — codec byte parity (stored bytes are JSON-quoted strings);
 *  - RCountMin CMS.* — shared sketch.
 *
 * Skips with a logged line (never silently) when Redis, a module command or
 * the Java 4.8 probe (mvn + jar download) is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, TimeSeriesAddArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:p74:';

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
    probeOk = (await probe48.probe(`arr48-len ${NS}warmup`)) === 'len=0';
    if (!probeOk) console.warn('[live phase74] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live phase74] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional 4.8 checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live phase 74: Java Redisson 4.8.0 bidirectional interop', () => {
  it('RTimeSeries 4.8 family: TS writes ⇄ Java reads; shared flag-4 members and seq counter', async () => {
    if (!hasRedis || !(await probe48Up())) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}ts`;
    const t0 = 1700000000000;
    await r.getTimeSeries(name).delete(); // clears ZSET + ttl/seq companions
    try {
      const ts = r.getTimeSeries<number>(name);
      expect(await ts.add(TimeSeriesAddArgs.entry(t0, 1.5))).toBe(true);
      expect(await ts.add(TimeSeriesAddArgs.entry(t0 + 1000, 2.5))).toBe(true);
      // Java 4.8 reads both entries our 4.8 family wrote
      const lines = await probe48.probeLines(`ts48-entryrange ${name} ${t0 - 1} ${t0 + 1500}`);
      expect(lines).toContain(`${t0}=1.5`);
      expect(lines).toContain(`${t0 + 1000}=2.5`);
      // Java writes; our reader sees it interleaved at the right timestamps
      expect(await probe48.probe(`ts48-add ${name} ${t0 + 2000} 7.25`)).toBe('added=true');
      const entries = await ts.entryRange(t0 - 1, t0 + 3000);
      expect(entries.map((e) => [e.timestamp, e.value])).toEqual([
        [t0, 1.5], [t0 + 1000, 2.5], [t0 + 2000, 7.25],
      ]);
      // raw member shape: every member starts with the flag-4 family byte,
      // and ids from both writers share the ONE counter (all distinct)
      const members: Buffer[] = await raw.zrangeBuffer(name, 0, -1);
      expect(members.length).toBe(3);
      const ids = new Set<string>();
      for (const m of members) {
        expect(m[0]).toBe(4);
        ids.add(m.subarray(2, 2 + m[1]).toString('latin1'));
      }
      expect(ids.size).toBe(3);
      // both writers draw from the same seq HASH (identical companion names)
      expect(await raw.type('redisson__ts_seq:{' + name + '}')).toBe('hash');
      await r.getTimeSeries(name).delete();
    } finally {
      await raw.unlink(name).catch(() => undefined);
      raw.disconnect();
      await r.shutdown();
    }
  }, 180000);

  it('RArray AR.*: TS ⇄ Java with codec byte parity', async () => {
    if (!hasRedis || !(await probe48Up())) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'ARSET'))) {
      console.warn('[live phase74] ARSET unavailable (Redis < 8.8?) — RArray interop skipped');
      raw.disconnect();
      return;
    }
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}arr`;
    await raw.unlink(name);
    try {
      const arr = r.getArray<string>(name);
      await arr.set(0, 'hello');
      await arr.set(2, 'world');
      expect(await probe48.probe(`arr48-get ${name} 0`)).toBe('value=hello');
      expect(await probe48.probe(`arr48-get ${name} 1`)).toBe('value=null');
      expect(await probe48.probe(`arr48-set ${name} 1 java`)).toBe('set=1');
      expect(await arr.get(1)).toBe('java');
      expect(await arr.length()).toBe(3);
      expect(await probe48.probe(`arr48-len ${name}`)).toBe('len=3');
      // raw wire: the stored bytes are the codec-encoded (JSON-quoted) value,
      // exactly what Java's JsonJacksonCodec writes
      const b: Buffer = await raw.callBuffer('ARGET', name, 0);
      expect(b.toString('latin1')).toBe('"hello"');
      await raw.unlink(name);
    } finally {
      raw.disconnect();
      await r.shutdown();
    }
  }, 120000);

  it('RCountMin CMS.*: shared sketch across languages', async () => {
    if (!hasRedis || !(await probe48Up())) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'CMS.INITBYDIM'))) {
      console.warn('[live phase74] CMS.* module unavailable — RCountMin interop skipped');
      raw.disconnect();
      return;
    }
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}cm`;
    await raw.unlink(name);
    try {
      expect(await probe48.probe(`cms48-init ${name} 2000 5`)).toBe('init=ok');
      const cm = r.getCountMin<string>(name);
      await cm.add('a', 2);
      expect(await probe48.probe(`cms48-count ${name} a`)).toBe('count=2');
      expect(await cm.count('a')).toBe(2);
      expect(await probe48.probe(`cms48-add ${name} b 3`)).toBe('count=3');
      expect(await cm.count('b')).toBe(3);
      await raw.unlink(name);
    } finally {
      raw.disconnect();
      await r.shutdown();
    }
  }, 120000);
});
