/**
 * Live interop vs **Java Redisson 4.8.0**, round 7 — FT.AGGREGATE, blocking
 * deque takes and cross-language lock force-unlock:
 *
 *  - Aggregation (phase44 was TS-only): TS-built TAG index aggregated by
 *    Java `GROUPBY ... REDUCE COUNT`, and a Java-built index aggregated by
 *    TS raw REDUCE tokens — same group rows from both languages.
 *  - RBlockingDeque.takeFirst/takeLast: the TS consumer blocks; Java feeds
 *    the deque (q48-offer); both take ends wake and return the element.
 *  - RLock.forceUnlock: a lock held by one language is force-released by
 *    the other (isLocked flips on both sides) — the disaster-recovery path.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48g:';

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
    if (!probeOk) console.warn('[live interop48g] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48g] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

/** FT indexing is async — poll until the query sees the expected total. */
async function pollTotal(
  search: ReturnType<Redisson['getSearch']>, idx: string, query: string, want: number,
): Promise<number> {
  let total = 0;
  for (let i = 0; i < 100; i++) {
    total = (await search.search(idx, query, { limit: [0, 10] })).total;
    if (total === want) return total;
    await new Promise((res) => setTimeout(res, 100));
  }
  return total;
}

describe('live interop48g: aggregation + blocking deque + force-unlock vs Java 4.8.0', () => {
  it('FT.AGGREGATE: same GROUPBY/COUNT rows from both languages', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'FT.CREATE'))) {
      console.log('[live interop48g] FT.* unavailable — skipping aggregation');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const prefix = `${NS}doc:`;
    const jidx = `${NS}jidx`;
    const jprefix = `${NS}jdoc:`;
    try {
      // --- direction 1: TS builds index + docs; Java aggregates ---
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      await r.redis.hset(`${prefix}a`, 'title', 'apple', 'cat', 'fruit');
      await r.redis.hset(`${prefix}b`, 'title', 'banana', 'cat', 'fruit');
      await r.redis.hset(`${prefix}c`, 'title', 'cherry', 'cat', 'fruit');
      await r.redis.hset(`${prefix}d`, 'title', 'date', 'cat', 'berry');
      await r.redis.hset(`${prefix}e`, 'title', 'elder', 'cat', 'berry');
      await r.getSearch().createIndex(idx, ['title', 'TEXT', 'cat', 'TAG'], { prefixes: [prefix] });
      expect(await pollTotal(r.getSearch(), idx, '*', 5)).toBe(5);

      const jAgg = await probe48.probeLines(`search48-agg ${idx} cat`);
      expect(jAgg[0]).toBe('total=2'); // two groups
      expect(jAgg.length).toBe(3);
      const fruitRow = jAgg.find((l) => l.includes('fruit'));
      const berryRow = jAgg.find((l) => l.includes('berry'));
      expect(fruitRow).toContain('=3');
      expect(berryRow).toContain('=2');

      // --- direction 2: Java builds the index; TS aggregates with raw tokens ---
      try { await probe48.probe(`search48-dropdocs ${jidx}`); } catch { /* absent */ }
      await probe48.probe(`search48-create ${jidx} ${jprefix}`);
      for (const [k, title, cat] of [['a', 'apple', 'fruit'], ['b', 'banana', 'fruit'], ['c', 'cherry', 'berry']] as const) {
        await probe48.probe(`search48-hset ${jprefix}${k} title ${title} cat ${cat}`);
      }
      expect(await pollTotal(r.getSearch(), jidx, '*', 3)).toBe(3);
      const tsAgg = await r.getSearch().aggregate(jidx, '*', {
        groupBy: ['@cat'],
        reduce: ['COUNT', '0', 'AS', 'n'],
      });
      expect(tsAgg.total).toBe(2);
      const byCat = new Map(tsAgg.rows.map((row: any) => [row.cat, Number(row.n)]));
      expect(byCat.get('fruit')).toBe(2);
      expect(byCat.get('berry')).toBe(1);
    } finally {
      try { await r.getSearch().dropIndex(idx, true); } catch { /* absent */ }
      try { await probe48.probe(`search48-dropdocs ${jidx}`); } catch { /* probe gone */ }
      await r.getKeys().delete(...[...'abcde'].map((c) => `${prefix}${c}`));
      await r.shutdown();
    }
  }, 300000);

  it('RBlockingDeque: TS takeFirst/takeLast block, Java feeds', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}bdq`;
    try {
      await r.getKeys().delete(name);
      const bdq = r.getBlockingDeque<string>(name);

      // takeFirst (BLPOP LEFT): Java's RPUSH lands at the tail, TS pops the head
      const first = bdq.takeFirst();
      await new Promise((res) => setTimeout(res, 150));
      expect(await probe48.probe(`q48-offer ${name} head-item`)).toBe('offered=true');
      expect(await first).toBe('head-item');

      // two queued elements, takeLast (BRPOP RIGHT) grabs the newest tail
      await probe48.probe(`q48-offer ${name} t1`);
      await probe48.probe(`q48-offer ${name} t2`);
      expect(await bdq.takeLast()).toBe('t2');
      expect(await bdq.takeFirst()).toBe('t1');
      expect(await r.getDeque<string>(name).readAll()).toEqual([]);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RLock.forceUnlock: a lock held in one language is force-released by the other', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock`;
    try {
      // TS holds; Java observes and force-unlocks; TS re-acquires
      const lock = r.getLock(name);
      await lock.lock(30000);
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=true');
      expect(await probe48.probe(`lock48-forceunlock ${name}`)).toBe('forced=true');
      expect(await lock.tryLock(0)).toBe(true);
      await lock.unlock();

      // Java holds; TS force-unlocks; Java observes the release
      await probe48.probe(`lock48-lock ${name} 30000`);
      expect(await lock.isLocked()).toBe(true);
      expect(await lock.forceUnlock()).toBe(true);
      expect(await probe48.probe(`lock48-islocked ${name}`)).toBe('locked=false');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
