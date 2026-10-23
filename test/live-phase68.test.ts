/**
 * Phase 68 live: upstream Redisson 4.7/4.8 interop alignment —
 * RateLimiter release (member un-acquire, oldest first) + acquire recalc
 * branch, renamenx missing-source false, RKeys.time, ring buffer
 * readNewest/readOldest/peekLast, RSearch.getAliases (FT-gated).
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, packRatePermit } from '../src';

const NS = 'TsJavaT:p68:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    r.on('error', () => undefined);
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

describe('live phase 68: upstream 4.7/4.8 interop fixes', () => {
  it('rate limiter release un-acquires members; the 4.8 recalc branch caps drift', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const name = `${NS}rl`;
    const permitsKey = `{${name}}:permits`;
    const rl = r.getRateLimiter(name);
    await rl.delete();
    await rl.trySetRate('OVERALL', 4, 500, 60000);
    expect(await rl.tryAcquire(1)).toBe(true); // value 3, live member(1)
    // Simulate drift: an already-expired 5-permit member (as if a client died
    // mid-release). 3 + 5 > 4 must take the 4.8 recalc branch, which
    // recomputes value from the permits actually held: 4 - 1 = 3, then -1.
    // 5s old, far beyond the 500ms window: the original 600ms seed left only
    // 100ms of margin, which a host↔server clock skew (WSL docker ±300ms)
    // can eat, making the "stale" member look live to the release Lua
    const stale = Buffer.from(packRatePermit('deadbeefdeadbeefdeadbeefdeadbeef', 5), 'latin1');
    await raw.zadd(permitsKey, String(Date.now() - 5000), stale);
    await sleep(50);
    expect(await rl.tryAcquire(1)).toBe(true);
    expect(await rl.availablePermits()).toBe(2); // pre-fix shape: 3 + 5 - 1 = 7
    // release(1): removes the oldest window member and raises the counter.
    const before = await raw.zcard(permitsKey);
    await rl.release(1);
    expect(await rl.availablePermits()).toBe(3);
    expect(await raw.zcard(permitsKey)).toBe(before - 1);
    await rl.delete();
    raw.disconnect();
    await r.shutdown();
  });

  it('renamenx / time / ring buffer / getAliases parity', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    // Java 4.8: missing source renames to false, not an error
    expect(await r.getKeys().renamenx(`${NS}nope`, `${NS}x`)).toBe(false);
    const b = r.getBucket<string>(`${NS}b`);
    await b.delete();
    await r.getBucket(`${NS}b2`).delete();
    await b.set('v');
    expect(await b.renamenx(`${NS}b2`)).toBe(true);
    expect(await r.getBucket<string>(`${NS}b2`).get()).toBe('v');
    // Java 4.8 RKeys.time(): server TIME
    const t = await r.getKeys().time();
    expect(t.seconds).toBeGreaterThan(1_700_000_000);
    expect(t.microseconds).toBeGreaterThanOrEqual(0);
    expect(Math.abs(t.seconds * 1000 - Date.now())).toBeLessThan(10_000);
    // Java 4.7 ring buffer reads
    const rb = r.getRingBuffer<number>(`${NS}rb`);
    await rb.delete();
    await rb.trySetCapacity(3);
    for (const v of [1, 2, 3, 4]) await rb.add(v); // 4 evicts 1
    expect(await rb.readOldest(2)).toEqual([2, 3]);
    expect(await rb.readNewest(2)).toEqual([3, 4]);
    expect(await rb.peekLast()).toBe(4);
    // Java 4.7 RSearch.getAliases (FT-gated)
    const search = r.getSearch();
    const idx = `${NS}idx`;
    try {
      await search.createIndex(idx, ['title', 'TEXT'], { prefixes: [`${NS}doc`] });
    } catch (e) {
      if (/unknown command|unsupported command|is not a function/i.test(String((e as any)?.message ?? e))) {
        console.warn('[live phase68] FT.* unavailable — getAliases check skipped');
        await r.shutdown();
        return;
      }
      throw e;
    }
    try {
      await search.addAlias(`${NS}alias`, idx);
      const aliases = await search.getAliases(idx);
      expect(aliases).toContain(`${NS}alias`);
    } catch (e) {
      // local RediSearch builds may lack FT.ALIASLIST (Java 4.7 command)
      if (!/unknown command/i.test(String((e as any)?.message ?? e))) throw e;
      console.warn('[live phase68] FT.ALIASLIST unavailable — getAliases check skipped');
    } finally {
      try { await search.delAlias(`${NS}alias`); } catch { /* already gone */ }
      try { await search.dropIndex(idx); } catch { /* already gone */ }
    }
    await r.shutdown();
  });
});
