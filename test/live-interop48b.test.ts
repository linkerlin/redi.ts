/**
 * Live interop vs **Java Redisson 4.8.0**, round 2 — the 4.8 OSS surfaces
 * phase 69/71/72 covered TS-only: RGcra (shared GCRA limiter over the
 * `{name}:config` hash + native GCRA state key), RTimeSeriesNative (TS.*
 * module series, raw doubles — no codec), RMaps mass set (codec HASHes),
 * RSet countDiff/countUnionApprox (SDIFFCARD/SUNIONCARD APPROX) and
 * RQueue.move (LMOVEM). Probe commands `gcra48-*`/`tsn48-*`/`maps48-*`/
 * `set48-*`/`q48-*` in interop TsProbe48.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, QueueMoveElementsArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48b:';

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
    // warmup must not touch TS.INFO — it errors on a nonexistent series
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
    if (!probeOk) console.warn('[live interop48b] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48b] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48b: GCRA / TS-native / RMaps / set-approx / queue-move vs Java 4.8.0', () => {
  it('RGcra: TS-configured limiter enforced by Java and vice versa', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'GCRA'))) {
      console.log('[live interop48b] GCRA command unavailable on this server build (not in OSS redis:8) — skipping RGcra');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const shared = `${NS}g-shared`; // burst 2, 1 token / 60s — refill slow, deterministic
    const other = `${NS}g-other`;
    try {
      await r.getKeys().delete(shared, `${shared}:config`, other, `${other}:config`);

      // TS writes the config hash; Java reads it as its own GcraConfig
      expect(await r.getGcra(shared).trySetRate(2, 1, 60000)).toBe(true);
      expect(await r.getGcra(shared).trySetRate(9, 9, 90000)).toBe(false); // already set
      const cfg = await probe48.probeLines(`gcra48-config ${shared}`);
      expect(cfg).toContain('burst=2');
      expect(cfg).toContain('rate=1');
      expect(cfg).toContain('periodSec=60');

      // Shared enforcement: each side consumes from the same GCRA state key
      const ts1 = await r.getGcra(shared).tryAcquire();
      expect(ts1.limited).toBe(false);
      expect(ts1.availableTokens).toBe(1);
      const j1 = await probe48.probe(`gcra48-tryacquire ${shared}`);
      expect(j1).toContain('limited=false');
      expect(j1).toContain('available=0');
      const j2 = await probe48.probe(`gcra48-tryacquire ${shared}`);
      expect(j2).toContain('limited=true');
      const ts2 = await r.getGcra(shared).tryAcquire();
      expect(ts2.limited).toBe(true);
      expect(ts2.retryAfterSeconds).toBeGreaterThan(0);

      // Java writes the config; TS reads it back (period seconds ↔ ms)
      expect(await probe48.probe(`gcra48-setrate ${other} 5 10 300`)).toBe('set=true');
      expect(await probe48.probe(`gcra48-setrate ${other} 1 1 60`)).toBe('set=false');
      const c = await r.getGcra(other).getConfig();
      expect(c).toEqual({ maxBurst: 5, tokensPerPeriod: 10, periodMs: 300000 });

      // tokens≠1 path (GCRA ... TOKENS n) shared across languages
      const big = `${NS}g-big`;
      await r.getGcra(big).trySetRate(5, 1, 60000);
      const t = await r.getGcra(big).tryAcquire(3);
      expect(t.limited).toBe(false);
      expect(t.availableTokens).toBe(2);
      const j3 = await probe48.probe(`gcra48-tryacquire ${big} 3`);
      expect(j3).toContain('limited=true'); // only 2 < 3 available
      await r.getKeys().delete(big, `${big}:config`);
    } finally {
      await r.getKeys().delete(shared, `${shared}:config`, other, `${other}:config`);
      await r.shutdown();
    }
  }, 300000);

  it('RTimeSeriesNative: raw-double TS.* series shared TS ⇄ Java', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'TS.ADD'))) {
      console.log('[live interop48b] TS.* unavailable — skipping RTimeSeriesNative');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}tsn`;
    try {
      await r.getKeys().delete(name);
      const tsn = r.getTimeSeriesNative(name);
      const t0 = Date.now() - 10000;
      expect(await tsn.add(t0 + 100, 1.5)).toBe(t0 + 100);
      expect(await tsn.add(t0 + 200, 2.5)).toBe(t0 + 200);

      // Java aggregates the TS-written series
      expect(await probe48.probe(`tsn48-size ${name}`)).toBe('size=2');
      expect(await probe48.probe(`tsn48-firstts ${name}`)).toBe(`firstTs=${t0 + 100}`);
      expect(await probe48.probe(`tsn48-lastts ${name}`)).toBe(`lastTs=${t0 + 200}`);
      expect(await probe48.probeLines(`tsn48-range ${name} ${t0} ${t0 + 150}`)).toEqual([`${t0 + 100}=1.5`]);

      // Java appends; TS sees all three samples
      expect(await probe48.probe(`tsn48-add ${name} ${t0 + 300} 3.5`)).toBe(`ts=${t0 + 300}`);
      expect(await tsn.size()).toBe(3);
      expect(await tsn.lastTimestamp()).toBe(t0 + 300);
      const all = await tsn.range(t0, t0 + 1000);
      expect(all.map((s) => [s.timestamp, s.values[0]])).toEqual([
        [t0 + 100, 1.5],
        [t0 + 200, 2.5],
        [t0 + 300, 3.5],
      ]);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RMaps: mass-written codec HASHes readable both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}maps-a`;
    const b = `${NS}maps-b`;
    const j = `${NS}maps-java`;
    try {
      await r.getKeys().delete(a, b, j);

      // TS mass-writes two maps in one RMaps.set; Java reads plain RMaps
      await r.getMaps<string, string>().set(
        new Map([
          [a, new Map([['k1', 'v1'], ['k2', 'v2']])],
          [b, new Map([['x', '1']])],
        ]),
      );
      expect(await probe48.probe(`map48-get ${a} k1`)).toBe('value=v1');
      expect(await probe48.probe(`map48-get ${a} k2`)).toBe('value=v2');
      expect(await probe48.probe(`map48-get ${b} x`)).toBe('value=1');

      // Java RMaps.set; TS reads through plain getMap (probe map is
      // Map<String,String>, so 42 travels as the JSON string "42")
      await probe48.probe(`maps48-set ${j} jk1 jv1 jk2 42`);
      expect(await r.getMap<string, any>(j).get('jk1')).toBe('jv1');
      expect(await r.getMap<string, any>(j).get('jk2')).toBe('42');
    } finally {
      await r.getKeys().delete(a, b, j);
      await r.shutdown();
    }
  }, 300000);

  it('RSet countDiff/countUnionApprox: same cardinalities from both languages', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'SDIFFCARD'))) {
      console.log('[live interop48b] SDIFFCARD unavailable — skipping set approx');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}s-a`;
    const b = `${NS}s-b`;
    try {
      await r.getKeys().delete(a, b);
      const sa = r.getSet<string>(a);
      const sb = r.getSet<string>(b);
      await sa.add('s1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10');
      await sb.add('s6', 's7', 's8', 's9', 's10', 's11', 's12', 's13', 's14', 's15');

      // |a \ b| = 5, |a ∪ b| = 15 — identical server state, both ask the same command
      expect(await sa.countDiff(b)).toBe(5);
      expect(await probe48.probe(`set48-countdiff ${a} ${b}`)).toBe('count=5');
      expect(await sa.countUnionApprox(b)).toBe(15);
      expect(await probe48.probe(`set48-countunion ${a} ${b}`)).toBe('count=15');
    } finally {
      await r.getKeys().delete(a, b);
      await r.shutdown();
    }
  }, 300000);

  it('RQueue.move (LMOVEM): elements migrate under both languages', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'LMOVEM'))) {
      console.log('[live interop48b] LMOVEM unavailable — skipping queue move');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}q-src`;
    const dst = `${NS}q-dst`;
    try {
      await r.getKeys().delete(src, dst);
      const q = r.getQueue<string>(src);
      await q.addAll(['m1', 'm2', 'm3', 'm4']); // offer(V) is single-value (Java shape)

      // TS bulk-moves two; decoded returns, Java sees them at the dest tail
      expect(await q.move(QueueMoveElementsArgs.to(dst).count(2).bulk())).toEqual(['m1', 'm2']);
      expect(await probe48.probeLines(`q48-read ${dst}`)).toEqual(['value=m1', 'value=m2']);

      // Java moves one more; TS observes the final state on both ends
      expect(await probe48.probe(`q48-move ${src} ${dst} 1`)).toBe('moved=m3');
      expect(await q.readAll()).toEqual(['m4']);
      expect(await r.getQueue<string>(dst).readAll()).toEqual(['m1', 'm2', 'm3']);
    } finally {
      await r.getKeys().delete(src, dst);
      await r.shutdown();
    }
  }, 300000);
});
