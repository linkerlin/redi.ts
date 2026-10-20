/**
 * Phase 37 live: RSearch (RediSearch module in Redis 8) — index metadata,
 * aliases, enhanced search (sort/return/params/dialect), aggregation,
 * synonyms, dictionaries and FT.CONFIG.
 *
 * Skips with a log when FT.CREATE is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p37:';
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

const hasRedis = await redisUp();
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

describe('live interop phase 37: RSearch', () => {
  it('index/search/aggregate/aliases/synonyms/dicts/config', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const alias = `${NS}alias`;
    const dict = `${NS}dict`;
    const keys = [`${NS}a`, `${NS}b`];
    const search = r.getSearch();
    try {
      if (!(await hasCmd(r.redis, 'FT.CREATE'))) {
        console.log('[phase37] RediSearch unsupported — skipping');
        return;
      }
      try { await search.dropIndex(idx, true); } catch { /* noop */ }
      try { await search.delAlias(alias); } catch { /* noop */ }
      try { await search.delDict(dict, 'a', 'b'); } catch { /* noop */ }
      await r.getKeys().delete(...keys);

      await search.createIndex(idx, ['title', 'TEXT', 'price', 'NUMERIC', 'cat', 'TAG'], { prefixes: [`${NS}`] });
      // plain HASH fields (RMap would JSON-encode field names, which the
      // unquoted FT schema would not index)
      await r.redis.hset(`${NS}a`, 'title', 'apple', 'price', '10', 'cat', 'fruit');
      await r.redis.hset(`${NS}b`, 'title', 'banana', 'price', '5', 'cat', 'fruit');

      let res = { total: 0, docs: [] as any[] };
      for (let i = 0; i < 50; i++) {
        res = await search.search(idx, '*', { limit: [0, 10] });
        if (res.total === 2) break;
        await sleep(100);
      }
      expect(res.total).toBe(2);

      const sorted = await search.search(idx, '*', {
        sortBy: 'price', sortAsc: true, returnFields: ['title', 'price'], limit: [0, 2],
      });
      expect(sorted.docs.map((d) => d.title)).toEqual(['banana', 'apple']);

      const byParams = await search.search(idx, '@price:[$min $max]', {
        params: { min: '6', max: '20' }, dialect: 2, limit: [0, 10],
      });
      expect(byParams.total).toBe(1);
      expect(byParams.docs[0].title).toBe('apple');

      expect(await search.hasIndex(idx)).toBe(true);
      expect(await search.getIndexes()).toContain(idx);
      const info = await search.info(idx);
      expect(Number(info.num_docs)).toBe(2);

      await search.addAlias(alias, idx);
      expect((await search.search(alias, '*', { limit: [0, 1] })).total).toBe(2);
      await search.delAlias(alias);

      const agg = await search.aggregate(idx, '*', {
        load: ['@price'],
        groupBy: ['@cat'],
        reduce: ['SUM', '1', '@price', 'AS', 'total'],
      });
      expect(agg.total).toBe(1);
      expect(agg.rows[0].cat).toBe('fruit');
      expect(Number(agg.rows[0].total)).toBe(15);

      await search.updateSynonyms(idx, 'g1', 'apple', 'aple');
      const synonyms = await search.dumpSynonyms(idx);
      expect(synonyms.get('apple')).toEqual(['g1']);

      expect(await search.addDict(dict, 'a', 'b')).toBe(2);
      expect(await search.dumpDict(dict)).toEqual(['a', 'b']);
      expect(await search.delDict(dict, 'a')).toBe(1);
      await search.delDict(dict, 'b');

      const cfg = await search.getConfig('timeout');
      expect(cfg.TIMEOUT).toBeDefined();

      await search.dropIndexAndDocuments(idx);
      expect(await search.hasIndex(idx)).toBe(false);
      expect(await r.redis.exists(`${NS}a`)).toBe(0);
    } finally {
      try { await search.dropIndex(idx, true); } catch { /* noop */ }
      try { await search.delAlias(alias); } catch { /* noop */ }
      try { await search.delDict(dict, 'a', 'b'); } catch { /* noop */ }
      await r.getKeys().delete(...keys);
      await r.shutdown();
    }
  }, 120000);
});
