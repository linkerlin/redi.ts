/**
 * Phase 15 live interop: plain-set STORE ops (union/intersection/diff).
 * Store semantics mirror Redisson 4.6.1: SELF is only the DESTINATION
 * (overwritten by the result) and is NOT an input; command shape is
 * SOPSTORE self others... with bare keys and no numkeys.
 * History: this suite once asserted the opposite ("numkeys REQUIRED"), and
 * because the health gate used the same wrong shape it always deemed healthy
 * servers broken, silently skipping inter/diff (see the poison regression
 * below, which pins the wrong shape to a failing result).
 *
 * SUNIONCARD/SDIFFCARD are Redis 8.10+ commands (absent on 8.6) — the
 * corresponding asserts are capability-gated.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p15:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

async function redisUp(): Promise<boolean> {
  try {
    const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}


async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

/** Textbook SINTERSTORE round-trip (Java shape, bare keys); false when the
 * server genuinely cannot do set-store. */
async function setStoreHealthy(r: Redisson): Promise<boolean> {
  const t1 = `${NS}gate:t1`;
  const t2 = `${NS}gate:t2`;
  const td = `${NS}gate:td`;
  try {
    await cleanExact(r, t1, t2, td);
    await r.redis.sadd(t1, 'a', 'b');
    await r.redis.sadd(t2, 'b', 'c');
    const n: number = await r.redis.sinterstore(td, t1, t2);
    const m: string[] = await r.redis.smembers(td);
    await cleanExact(r, t1, t2, td);
    return n === 1 && m.length === 1 && m[0] === 'b';
  } catch {
    return false;
  }
}

/** true when the server knows the command (COMMAND INFO returns a block). */
async function hasCmd(r: Redisson, name: string): Promise<boolean> {
  try {
    const info: unknown = await (r.redis as any).call('COMMAND', 'INFO', name);
    return Array.isArray(info) && info.length > 0 && Array.isArray(info[0]);
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 15: plain-set store ops', () => {
  it('union store lands in Java reads (SELF is only the destination)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const sa = `${NS}sa`;
    const sb = `${NS}sb`;
    const sd = `${NS}sd`;
    try {
      await cleanExact(r, sa, sb, sd);
      await r.getSet<string>(sa).add('a', 'b');
      await r.getSet<string>(sb).add('b', 'c');
      // discrimination: 'zz' pre-seeded in the dest and a real key named "3"
      // (the bogus numkeys of the old shape) must both be absent — SELF is
      // not an input and bare-key STORE never references key "3"
      await r.getSet<string>(sd).add('zz');
      await r.getSet<string>('3').add('poison');
      expect(await r.getSet<string>(sd).union(sa, sb)).toBe(3);
      expect(await probe(`set-read ${sd}`)).toBe('SET:[a, b, c]');
      // Java-computed union over the same inputs is identical
      expect(await probe(`set-union ${sd} ${sa} ${sb}`)).toBe('SET_UNION:3');
      // countUnion needs SUNIONCARD (Redis 8.10+)
      if (await hasCmd(r, 'SUNIONCARD')) {
        expect(await r.getSet<string>(sd).countUnion(sa, sb)).toBe(3);
      } else {
        console.log('SKIP countUnion assert: SUNIONCARD absent on this server (needs Redis 8.10+)');
      }
    } finally {
      await cleanExact(r, '3');
      await r.shutdown();
    }
  }, 180000);

  it('intersection/difference store match Java (gated on healthy S*STORE)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await setStoreHealthy(r))) {
      console.log('SKIP inter/diff store: server SINTERSTORE/SDIFFSTORE broken (see phase15 header)');
      return;
    }
    const sa = `${NS}ga`;
    const sb = `${NS}gb`;
    try {
      await cleanExact(r, sa, sb);
      await r.getSet<string>(sa).add('a', 'b');
      await r.getSet<string>(sb).add('b', 'c');
      // poison regression (see union test): old shape made intersection empty
      await r.getSet<string>('3').add('poison');
      // dests pre-seeded with members that would survive only if SELF were an
      // input — SELF is just the destination (Redisson 4.6.1)
      const di = `${NS}di`;
      const dd = `${NS}dd`;
      await cleanExact(r, di, dd);
      await r.getSet<string>(di).add('a'); // self-inclusive would collapse to ∅
      await r.getSet<string>(dd).add('a', 'b', 'c', 'x'); // 'x' would survive
      expect(await r.getSet<string>(di).intersection(sa, sb)).toBe(1);
      expect(await probe(`set-read ${di}`)).toBe('SET:[b]');
      expect(await r.getSet<string>(dd).diff(sa, sb)).toBe(1);
      expect(await probe(`set-read ${dd}`)).toBe('SET:[a]');
      // Java-computed intersection/diff over the same inputs are identical
      const ji = `${NS}ji`;
      const jd = `${NS}jd`;
      await cleanExact(r, ji, jd);
      await r.getSet<string>(ji).add('a'); // self-inclusive would collapse to ∅
      await r.getSet<string>(jd).add('a', 'b', 'c', 'x'); // 'x' would survive
      expect(await probe(`set-intersection ${ji} ${sa} ${sb}`)).toBe('SET_INTERSECTION:1');
      expect(await probe(`set-read ${ji}`)).toBe('SET:[b]');
      expect(await probe(`set-diff ${jd} ${sa} ${sb}`)).toBe('SET_DIFF:1');
      expect(await probe(`set-read ${jd}`)).toBe('SET:[a]');
    } finally {
      await cleanExact(r, '3');
      await r.shutdown();
    }
  }, 240000);
});
