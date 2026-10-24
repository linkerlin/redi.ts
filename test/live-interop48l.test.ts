/**
 * Live interop vs **Java Redisson 4.8.0**, round 12 — wake paths and
 * expiry args: PES blocking acquire, RArray inserts and INCREX PX:
 *
 *  - PES: a TS `acquire()` blocked on the last permit is woken by Java
 *    `release(permitId)` — the `redisson_sc:{name}` wake channel is shared
 *    (and the retry loop covers a missed publish anyway).
 *  - RArray (phase73 TS-only surfaces): TS `insert` read by Java `get`;
 *    Java `ARINSERT` appends observed by TS `range`.
 *  - `incrementAndGet(LongIncrementArgs.timeToLive)`: TS's Lua-CAS PX and
 *    Java's native `INCREX ... PX` both leave a live TTL on the key,
 *    mutually readable; a TS renewal shortens it.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48l:';

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
    if (!probeOk) console.warn('[live interop48l] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48l] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48l: PES wake + array insert + INCREX PX vs Java 4.8.0', () => {
  it('RPermitExpirableSemaphore: blocked TS acquire woken by Java release', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes`;
    const timeout = `{${name}}:timeout`; // suffixName hash-tag form
    try {
      await r.getKeys().delete(name, timeout);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(1)).toBe(true);

      // TS takes the only permit; a second acquire blocks.
      // 10-minute leases: the 6390 redis lives in WSL docker and its clock
      // can JUMP FORWARD under load (resync) — a short lease would be
      // purged as expired mid-test and skew the counters (observed once as
      // counter=4/zset=[]). No realistic jump covers 10 minutes.
      const held = await pes.tryAcquire(600000);
      expect(held).not.toBeNull();
      const second = pes.acquire(600000);
      await new Promise((res) => setTimeout(res, 200));

      // Java's release frees the permit and publishes the wake
      await probe48.probe(`pes48-release ${name} ${held![0]}`);
      const ids = await second;
      expect(ids).toHaveLength(1);
      expect(ids[0]).not.toBe(held![0]); // fresh permit id
      expect(await pes.availablePermits()).toBe(0);
    } finally {
      await r.getKeys().delete(name, timeout);
      await r.shutdown();
    }
  }, 300000);

  it('RArray: TS insert read by Java, Java ARINSERT read by TS', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'ARSET'))) {
      console.log('[live interop48l] ARSET unavailable — skipping RArray');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}arr`;
    try {
      await r.getKeys().delete(name);
      const arr = r.getArray<string>(name);

      // TS appends via ARINSERT; Java reads the positions
      expect(await arr.insert('a', 'b')).toBe(1); // last-inserted index
      expect(await probe48.probe(`arr48-get ${name} 0`)).toBe('value=a');
      expect(await probe48.probe(`arr48-get ${name} 1`)).toBe('value=b');

      // Java appends; TS ranges over the whole array (explicit bounds —
      // TS range() rejects negative indexes)
      expect(await probe48.probe(`arr48-insert ${name} c`)).toBe('at=2');
      expect(await arr.range(0, 2)).toEqual(['a', 'b', 'c']);
      expect(await probe48.probe(`arr48-len ${name}`)).toBe('len=3');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('INCREX timeToLive: TS Lua-CAS PX and Java native PX both leave live TTLs', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'INCREX'))) {
      console.log('[live interop48l] INCREX unavailable — skipping PX case');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}al`;
    try {
      await r.getKeys().delete(k);
      const al = r.getAtomicLong(k);

      // TS CAS-increments with a 60s TTL; Java reads the value, the TTL is live
      await al.set(5);
      expect(await al.incrementAndGet({ by: 1, timeToLiveMs: 60000 })).toBe(6);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=6');
      const ttl1 = await rawPTTL(k);
      expect(ttl1).toBeGreaterThan(30000);
      expect(ttl1).toBeLessThanOrEqual(60000);

      // Java's native INCREX PX re-sets the expiry; TS reads the value
      expect(await probe48.probe(`atomic48-incrargs ${k} 1 - - 0 60000`)).toBe('value=7');
      expect(await al.get()).toBe(7);
      expect(await rawPTTL(k)).toBeGreaterThan(30000);

      // TS renewal with a shorter window shortens the TTL
      expect(await al.incrementAndGet({ by: 1, timeToLiveMs: 15000 })).toBe(8);
      expect(await probe48.probe(`atomic48-get ${k}`)).toBe('value=8');
      expect(await rawPTTL(k)).toBeLessThanOrEqual(15000);
    } finally {
      await r.getKeys().delete(k);
      await r.shutdown();
    }

    async function rawPTTL(key: string): Promise<number> {
      const c = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
      try {
        await c.connect();
        return Number(await c.call('PTTL', key));
      } finally {
        c.disconnect();
      }
    }
  }, 300000);
});
