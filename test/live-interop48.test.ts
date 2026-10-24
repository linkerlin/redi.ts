/**
 * Live interop vs **Java Redisson 4.8.0** for the probabilistic + ring
 * surfaces that had no Java-side probe coverage (phases 36/66/73 were
 * TS-only): RCuckooFilter (CF.*), RTopK (TOPK.*), RTDigest (TDIGEST.*) and
 * RCircularBuffer (AR.* ring) — via interop/pom48.xml → TsProbe48
 * `cf48-*`/`topk48-*`/`td48-*`/`cb48-*` commands.
 *
 * The load-bearing wire fact this file proves: CuckooFilter and TopK items
 * travel **codec-encoded** (Java `encode(element)` → JSON-quoted strings),
 * so `cf.add('x')` in TS and `getCuckooFilter(n).add("x")` in Java address
 * the same sketch member. TDigest is raw doubles; CircularBuffer values are
 * codec-encoded like RArray.
 *
 * Skips with a logged line (never silently) when Redis, a module command or
 * the Java 4.8 probe is unavailable. Companion keys are cleaned by exact DEL
 * (AGENTS law 9 — no SCAN MATCH patterns).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48:';

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
    if (!probeOk) console.warn('[live interop48] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48: probabilistic + ring vs Java Redisson 4.8.0', () => {
  it('RCuckooFilter: codec-encoded members shared TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'CF.RESERVE'))) {
      console.log('[live interop48] CF.* unavailable — skipping CuckooFilter');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}cf`;
    try {
      await r.getKeys().delete(name);
      const cf = r.getCuckooFilter(name);
      await cf.init(1000);

      // TS writes → Java sees the same codec-encoded member
      expect(await cf.add('ts-cf-1')).toBe(true);
      expect(await cf.addIfAbsent('ts-cf-2')).toBe(true);
      expect(await probe48.probe(`cf48-exists ${name} ts-cf-1`)).toBe('exists=true');
      expect(await probe48.probe(`cf48-exists ${name} ts-cf-2`)).toBe('exists=true');
      expect(await probe48.probe(`cf48-exists ${name} never-added`)).toBe('exists=false');
      // Java addIfAbsent of a TS-inserted member must dedup on the same bytes
      expect(await probe48.probe(`cf48-addnx ${name} ts-cf-1`)).toBe('added=false');

      // Java writes → TS sees it; duplicate counts accumulate per language
      expect(await probe48.probe(`cf48-add ${name} java-cf-1`)).toBe('added=true');
      expect(await cf.exists('java-cf-1')).toBe(true);
      expect(await cf.add('ts-cf-1')).toBe(true);
      expect(await cf.add('ts-cf-1')).toBe(true);
      expect(await probe48.probe(`cf48-count ${name} ts-cf-1`)).toBe('count=3');

      // delete parity
      expect(await probe48.probe(`cf48-remove ${name} java-cf-1`)).toBe('removed=true');
      expect(await cf.exists('java-cf-1')).toBe(false);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RTopK: encoded items, decoded returns, shared heap TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'TOPK.RESERVE'))) {
      console.log('[live interop48] TOPK.* unavailable — skipping TopK');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}tk`;
    try {
      await r.getKeys().delete(name);
      const tk = r.getTopK(name);
      await tk.init(3);

      // TS builds the heap; returns must be decoded (no JSON quotes)
      expect(await tk.add('ts-a', 'ts-a', 'ts-a')).toEqual([null, null, null]);
      expect(await tk.incrementBy('ts-b', 2)).toBeNull();
      expect(await tk.contains('ts-a')).toBe(true);

      // Java queries the TS-written members (same encoded bytes)
      expect(await probe48.probe(`topk48-contains ${name} ts-a`)).toBe('contains=true');
      expect(await probe48.probe(`topk48-count ${name} ts-a`)).toBe('count=3');
      expect(await probe48.probe(`topk48-contains ${name} nope`)).toBe('contains=false');

      // Java writes; TS queries back — evicted return decoded by Java too
      expect(await probe48.probe(`topk48-add ${name} java-x`)).toBe('evicted=null');
      expect(await tk.contains('java-x')).toBe(true);
      expect(await tk.count('java-x')).toBe(1);

      // list/listWithCount decode parity (unquoted keys on both sides)
      expect(await tk.list()).toContain('ts-a');
      const wc = await tk.listWithCount();
      expect(wc.get('ts-a')).toBe(3);
      const javaList = await probe48.probeLines(`topk48-listwc ${name}`);
      expect(javaList.some((l) => l.startsWith('ts-a=3'))).toBe(true);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RTDigest: shared sketch over raw doubles TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'TDIGEST.CREATE'))) {
      console.log('[live interop48] TDIGEST.* unavailable — skipping TDigest');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}td`;
    try {
      await r.getKeys().delete(name);
      const td = r.getTDigest(name);
      await td.create(100);

      // TS seeds 1..10; Java aggregates the same sketch
      await td.add(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
      expect(await probe48.probe(`td48-min ${name}`)).toBe('min=1.0');
      expect(await probe48.probe(`td48-max ${name}`)).toBe('max=10.0');

      // Java extends; TS sees it
      await probe48.probe(`td48-add ${name} 20 30`);
      expect(await td.getMin()).toBeCloseTo(1);
      expect(await td.getMax()).toBeCloseTo(30);

      // cross-language rank/cdf/quantile agree (approximate structures)
      const rankTs = (await td.rank(10))[0];
      const rankJava = Number((await probe48.probe(`td48-rank ${name} 10`)).split('=')[1]);
      expect(Math.abs(rankTs - rankJava)).toBeLessThanOrEqual(1);
      const cdfTs = (await td.cumulativeProbability(10))[0];
      const cdfJava = Number((await probe48.probe(`td48-cdf ${name} 10`)).split('=')[1]);
      expect(Math.abs(cdfTs - cdfJava)).toBeLessThanOrEqual(0.05);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RCircularBuffer: shared AR.* ring (values codec-encoded) TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'ARSET'))) { // 'ARSET' — NOT 'AR.SET' (see phase73)
      console.log('[live interop48] AR.* unavailable — skipping CircularBuffer');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}cb`;
    // companion settings key (prefixName("redisson_acb", name)) — exact DEL, law 9
    const settings = `redisson_acb:{${name}}`;
    try {
      await r.getKeys().delete(name, settings);
      const cb = r.getCircularBuffer<string>(name);
      expect(await cb.trySetCapacity(3)).toBe(true);

      // TS rings 4 values through a 3-slot buffer: oldest evicted
      for (const v of ['one', 'two', 'three', 'four']) await cb.add(v);
      expect(await cb.readAll()).toEqual(['two', 'three', 'four']);

      // Java sees the same capacity, size and ring contents
      expect(await probe48.probe(`cb48-capacity ${name}`)).toBe('capacity=3');
      expect(await probe48.probe(`cb48-size ${name}`)).toBe('size=3');
      expect(await probe48.probeLines(`cb48-read ${name}`)).toEqual(['value=two', 'value=three', 'value=four']);

      // Java rings one more in; TS observes the eviction
      expect(await probe48.probe(`cb48-add ${name} five`)).toBe('added=true');
      expect(await cb.readAll()).toEqual(['three', 'four', 'five']);
      expect(await cb.lastItems(2, false)).toEqual(['four', 'five']);
      expect(await probe48.probeLines(`cb48-last ${name} 2`)).toEqual(['value=four', 'value=five']);
    } finally {
      await r.getKeys().delete(name, settings);
      await r.shutdown();
    }
  }, 300000);
});
