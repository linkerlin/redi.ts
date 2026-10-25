/**
 * Live interop vs **Java Redisson 4.8.0**, round 36 — two module surfaces
 * whose only prior evidence was TS-only phase suites:
 *
 *  - RVectorSet: TS `add(element, vector)` builds the set (DIM from the
 *    first vector) — Java `contains`/`size`/`dimensions`/
 *    `getSimilar(element, count)` see the identical elements and rank the
 *    nearest neighbors first; a Java `VectorAddArgs.element().vector()`
 *    add is visible to TS `contains`/`size` and TS `sim` ranks it with the
 *    nearest cluster.
 *  - RJsonBuckets: Java `set(Map)` (JacksonCodec, plain JSON — 4.8's
 *    getJsonBuckets REQUIRES a JsonCodec; JsonJacksonCodec is NOT one) →
 *    TS `get(...)` parses the docs; TS `set(mapping)` → Java `get(keys)`
 *    reads the same values.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable; VectorSet needs the redis vector-set module.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48aj:';

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
    console.warn(`[live interop48aj] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function hasVectorSet(raw: Redis): Promise<boolean> {
  try {
    await (raw as any).call('VLEN', `${NS}__probe_nokey__`);
    return true;
  } catch (e: any) {
    console.warn(`[live interop48aj] vector-set module unavailable — VectorSet case skipped: ${String(e?.message ?? e).split('\n')[0]}`);
    return false;
  }
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48aj: RVectorSet / RJsonBuckets vs Java 4.8.0', () => {
  it('RVectorSet: elements, dimensions and similarity ranking shared across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const raw = new Redis(REDIS_URL);
    if (!(await hasVectorSet(raw))) {
      raw.disconnect();
      return;
    }
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}vs`;
    try {
      await delAll(raw, [name]);
      const vs = r.getVectorSet(name);

      // TS adds three 3-dim elements — first VADD fixes DIM=3
      expect(await vs.add('e1', [1, 0, 0])).toBe(true);
      expect(await vs.add('e2', [0.9, 0.1, 0])).toBe(true);
      expect(await vs.add('e3', [0, 1, 0])).toBe(true);

      // Java sees the same set: size, dimensions, membership
      expect(await probe48.probe(`vs48-size ${name}`)).toBe('size=3');
      expect(await probe48.probe(`vs48-dim ${name}`)).toBe('dim=3');
      expect(await probe48.probe(`vs48-contains ${name} e2`)).toBe('contains=true');
      expect(await probe48.probe(`vs48-contains ${name} e9`)).toBe('contains=false');

      // Java's similarity search from e1 ranks e2 (near cluster) ahead of e3
      const jhits = (await probe48.probe(`vs48-sim ${name} e1 3`)).slice('hits='.length).split(',');
      expect(jhits.indexOf('e2')).toBeLessThan(jhits.indexOf('e3'));

      // Java adds a near-e1 element; TS sees it and ranks it with the cluster
      expect(await probe48.probe(`vs48-add ${name} e4 0.95,0.05,0`)).toBe('added=true');
      expect(await vs.contains('e4')).toBe(true);
      expect(await vs.size()).toBe(4);
      const tHits = await vs.sim('e1', 4);
      expect(tHits.indexOf('e4')).toBeLessThan(tHits.indexOf('e3'));
    } finally {
      await delAll(raw, [name]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RJsonBuckets: plain-JSON docs shared across languages both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}jb1`;
    const k2 = `${NS}jb2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [k1, k2]);

      // Java set(Map) with JacksonCodec (plain JSON; 4.8 requires a JsonCodec
      // — JsonJacksonCodec is NOT one) → TS get parses both docs
      expect(await probe48.probe(`jb48-set ${k1} java-a ${k2} java-b`)).toBe('set=ok');
      const got = await r.getJsonBuckets().get<string>(k1, k2);
      expect(got.get(k1)).toBe('java-a');
      expect(got.get(k2)).toBe('java-b');

      // TS set(mapping) → Java get(keys) reads the same values
      await r.getJsonBuckets().set({ [k1]: 'ts-x', [k2]: 'ts-y' });
      const lines = await probe48.probeLines(`jb48-get ${k1} ${k2}`);
      expect(lines).toContain(`k=${k1} v=ts-x`);
      expect(lines).toContain(`k=${k2} v=ts-y`);
    } finally {
      await delAll(raw, [k1, k2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
