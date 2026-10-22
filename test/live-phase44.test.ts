/**
 * Phase 44 live: RSearch 4.6.1 remaining surface — FT.HYBRID (KNN/RANGE,
 * RRF/LINEAR), FT.PROFILE (SEARCH/AGGREGATE), FT.CURSOR READ/DEL and FT.ALTER.
 *
 * Needs RediSearch with FT.HYBRID (Redis 8.2+); capability-gated.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p44:';
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

async function hasCmd(redis: Redis, cmd: string): Promise<boolean> {
  try {
    const info: any = await (redis as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(info) && info.length > 0 && info[0] !== null && info[0] !== undefined;
  } catch {
    return false;
  }
}

function vec(a: number, b: number): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeFloatLE(a, 0);
  buf.writeFloatLE(b, 4);
  return buf;
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live phase 44: RSearch hybrid/profile/cursor/alter', () => {
  it('hybrid KNN/RANGE with RRF/LINEAR, profiles, cursors and alter', async () => {
    // RediSearch can transiently fail index writes under heavy parallel load
    // (SEARCH_SAFE_DEPLETER_FAILURE ... "Please retry") — one retry, then fail.
    const attempt = async (): Promise<void> => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}hy`;
    const altIdx = `${NS}alt`;
    const prefix = `${NS}h:`;
    const altPrefix = `${NS}a:`;
    try {
      if (!(await hasCmd(r.redis, 'FT.HYBRID'))) {
        console.log('[phase44] FT.HYBRID unsupported — skipping');
        return;
      }
      const search = r.getSearch();
      await search.dropIndex(idx).catch(() => undefined);
      await search.createIndex(idx, [
        'title', 'TEXT',
        'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'COSINE',
      ], { prefixes: [prefix] });
      const h1 = `${prefix}1`;
      const h2 = `${prefix}2`;
      const h3 = `${prefix}3`;
      await r.redis.hset(h1, 'title', 'red shoes', 'vector', vec(1, 0));
      await r.redis.hset(h2, 'title', 'red hat', 'vector', vec(0.9, 0.1));
      await r.redis.hset(h3, 'title', 'blue shoes', 'vector', vec(0, 1));

      const rrf = await search.hybridSearch(idx, {
        query: '@title:red',
        vectorSimilarity: { field: '@vector', param: '$q', mode: 'KNN', k: 2 },
        combine: { type: 'RRF', constant: 60 },
        params: { q: vec(1, 0) },
        limit: [0, 10],
      });
      expect(rrf.total).toBe(2);
      const rrfKeys = rrf.results.map((x) => x.__key);
      expect(rrfKeys).toContain(h1);
      expect(rrfKeys).toContain(h2);
      expect(rrf.warnings).toEqual([]);

      const linear = await search.hybridSearch(idx, {
        query: '@title:red',
        vectorSimilarity: { field: '@vector', param: '$q', k: 2, scoreAlias: 'vscore' },
        combine: { type: 'LINEAR', alpha: 0.7, beta: 0.3 },
        params: { q: vec(1, 0) },
        load: ['@title', '@vscore'],
        sortBy: { field: '@title', order: 'ASC' },
        limit: [0, 10],
      });
      expect(linear.total).toBe(2);
      expect(linear.results.map((x) => x.title)).toEqual(['red hat', 'red shoes']);
      expect(linear.results[0].vscore).toBeDefined();

      const range = await search.hybridSearch(idx, {
        query: '@title:shoes',
        vectorSimilarity: { field: '@vector', param: '$q', mode: 'RANGE', radius: 0.5 },
        combine: { type: 'RRF', window: 10 },
        params: { q: vec(1, 0) },
        limit: [0, 10],
      });
      expect(range.results.length).toBeGreaterThanOrEqual(1);
      expect(range.results.map((x) => x.__key)).toContain(h1);

      const ps = await search.profileSearch(idx, '@title:red', { limit: [0, 1] });
      expect(ps.result.total).toBe(2);
      expect(ps.result.docs).toHaveLength(1);
      expect(Object.keys(ps.info)).toEqual(expect.arrayContaining(['Shards', 'Coordinator']));

      const pa = await search.profileAggregate(idx, '@title:red', { load: ['@title'] });
      expect(pa.result.rows).toHaveLength(2);
      expect(Object.keys(pa.info)).toEqual(expect.arrayContaining(['Shards', 'Coordinator']));

      const agg = await search.aggregate(idx, '*', { load: ['@title'], withCursor: [1] });
      expect(agg.rows).toHaveLength(1);
      expect(agg.cursor).toBeGreaterThan(0);
      const cursor = agg.cursor as number;
      const next = await search.readCursor(idx, cursor, 1);
      expect(next.rows).toHaveLength(1);
      expect(next.cursor).toBeGreaterThan(0);
      expect(agg.rows[0].title).not.toBe(next.rows[0].title);
      await search.delCursor(idx, next.cursor as number);
      await expect(search.readCursor(idx, next.cursor as number)).rejects.toThrow();

      await search.dropIndex(altIdx).catch(() => undefined);
      await search.createIndex(altIdx, ['title', 'TEXT'], { prefixes: [altPrefix] });
      const a1 = `${altPrefix}1`;
      await r.redis.hset(a1, 'title', 'old', 'tag', 'sale');
      await search.alter(altIdx, ['tag', 'TAG']);
      const a2 = `${altPrefix}2`;
      await r.redis.hset(a2, 'title', 'new', 'tag', 'fresh');
      const alt = await search.search(altIdx, '@tag:{fresh}');
      expect(alt.total).toBe(1);
      expect(alt.docs[0].id).toBe(a2);
      const info = await search.info(altIdx);
      expect(JSON.stringify(info)).toContain('tag');
    } finally {
      await r.getKeys().delete(`${prefix}1`, `${prefix}2`, `${prefix}3`, `${altPrefix}1`, `${altPrefix}2`);
      await r.getSearch().dropIndex(idx, true).catch(() => undefined);
      await r.getSearch().dropIndex(altIdx, true).catch(() => undefined);
      await r.shutdown();
    }
    };
    try {
      await attempt();
    } catch (e) {
      if (!/SEARCH_SAFE_DEPLETER_FAILURE|please retry/i.test(String((e as any)?.message ?? e))) throw e;
      console.warn('[phase44] transient RediSearch index-lock failure — retrying once');
      await new Promise((x) => setTimeout(x, 1000));
      await attempt();
    }
  }, 120000);
});
