/**
 * Phase 15 live interop: plain-set STORE ops (union/intersection/diff).
 * Store semantics mirror Redisson: SELF is an input, result overwrites SELF,
 * command shape is SOPSTORE self (1+N) self others... (numkeys REQUIRED —
 * ioredis typings can't express it, hence the `as any` casts in src).
 *
 * Environment caveat: the local msys2 Redis build returns 0/[] for
 * SINTERSTORE/SDIFFSTORE (verified via raw RESP; reads and SUNIONSTORE are
 * fine). Inter/diff asserts are capability-gated and run on healthy servers
 * (e.g. CI official redis:8 image); union asserts always run.
 * Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import { exec } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import Redis from 'ioredis';
import { Redisson } from '../src';

const execAsync = promisify(exec);
const NS = 'TsJavaT:p15:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const POM = path.join(ROOT, 'interop', 'pom.xml');
const MVN = process.env.MVN ?? (process.platform === 'win32' ? 'C:\\javas\\apache-maven-3.9.6-bin\\apache-maven-3.9.6\\bin\\mvn.cmd' : 'mvn');

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

async function probe(args: string, timeoutMs = 120000): Promise<string> {
  const cmd = `"${MVN}" -f "${POM}" -q compile exec:java "-Dexec.args=${args}"`;
  const { stdout } = await execAsync(cmd, { timeout: timeoutMs, cwd: ROOT });
  const lines = stdout.split('\n').map((l) => l.trim()).filter(Boolean);
  return [...lines].reverse().find((l) => !l.startsWith('WARNING') && !l.startsWith('SLF4J')) ?? '';
}

async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

/** Textbook SINTERSTORE round-trip; false on servers with broken set-store. */
async function setStoreHealthy(r: Redisson): Promise<boolean> {
  const t1 = `${NS}gate:t1`;
  const t2 = `${NS}gate:t2`;
  const td = `${NS}gate:td`;
  try {
    await cleanExact(r, t1, t2, td);
    await r.redis.sadd(t1, 'a', 'b');
    await r.redis.sadd(t2, 'b', 'c');
    const n: number = await (r.redis as any).sinterstore(td, 2, t1, t2);
    const m: string[] = await r.redis.smembers(td);
    await cleanExact(r, t1, t2, td);
    return n === 1 && m.length === 1 && m[0] === 'b';
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();

describe('live interop phase 15: plain-set store ops', () => {
  it('union store lands in Java reads (self IS an input)', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const sa = `${NS}sa`;
    const sb = `${NS}sb`;
    const sd = `${NS}sd`;
    try {
      await cleanExact(r, sa, sb, sd);
      await r.getSet<string>(sa).add('a', 'b');
      await r.getSet<string>(sb).add('b', 'c');
      // fresh dest: union(dest=∅, sa, sb) = {a,b,c}
      expect(await r.getSet<string>(sd).union(sa, sb)).toBe(3);
      expect(await probe(`set-read ${sd}`)).toBe('SET:[a, b, c]');
      // Java-computed union over the same inputs is identical
      expect(await probe(`set-union ${sd} ${sa} ${sb}`)).toBe('SET_UNION:3');
      // countUnion needs no store and works everywhere
      expect(await r.getSet<string>(sd).countUnion(sa, sb)).toBe(3);
    } finally {
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
      // pre-seeded dests (self participates): {a,b,c,x} ∩ sa ∩ sb = {b}
      const di = `${NS}di`;
      const dd = `${NS}dd`;
      await cleanExact(r, di, dd);
      await r.getSet<string>(di).add('a', 'b', 'c', 'x');
      await r.getSet<string>(dd).add('a', 'b', 'c', 'x');
      expect(await r.getSet<string>(di).intersection(sa, sb)).toBe(1);
      expect(await probe(`set-read ${di}`)).toBe('SET:[b]');
      expect(await r.getSet<string>(dd).diff(sa, sb)).toBe(1);
      expect(await probe(`set-read ${dd}`)).toBe('SET:[x]');
      // Java-computed intersection over the same inputs is identical
      const ji = `${NS}ji`;
      const jd = `${NS}jd`;
      await cleanExact(r, ji, jd);
      await r.getSet<string>(ji).add('a', 'b', 'c', 'x');
      await r.getSet<string>(jd).add('a', 'b', 'c', 'x');
      expect(await probe(`set-intersection ${ji} ${sa} ${sb}`)).toBe('SET_INTERSECTION:1');
      expect(await probe(`set-read ${ji}`)).toBe('SET:[b]');
      expect(await probe(`set-diff ${jd} ${sa} ${sb}`)).toBe('SET_DIFF:1');
      expect(await probe(`set-read ${jd}`)).toBe('SET:[x]');
    } finally {
      await r.shutdown();
    }
  }, 240000);
});
