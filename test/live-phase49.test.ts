/**
 * Phase 49 live: structured RSearch index definitions (TS shape of Java
 * IndexOptions/FieldIndex) — TEXT/TAG/NUMERIC/GEO/VECTOR (FLAT+HNSW) with
 * AS/sortable/options, plus structured FT.ALTER.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p49:';
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

describe('live phase 49: structured search indexes', () => {
  it('creates structured schemas, queries aliases and alters with fields', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const alt = `${NS}alt`;
    const prefix = `${NS}h:`;
    const altPrefix = `${NS}a:`;
    const h1 = `${prefix}1`;
    const h2 = `${prefix}2`;
    try {
      if (!(await hasCmd(r.redis, 'FT.CREATE'))) {
        console.log('[phase49] RediSearch unsupported — skipping');
        return;
      }
      const search = r.getSearch();
      await search.dropIndex(idx, true).catch(() => undefined);
      await search.dropIndex(alt, true).catch(() => undefined);
      await search.createIndex(idx, [
        { type: 'TEXT', property: 'title', as: 'name', sortable: true, noStem: true, weight: 2, indexEmpty: true },
        { type: 'TAG', property: 'tag', separator: ',', sortable: true, caseSensitive: true },
        { type: 'NUMERIC', property: 'price', sortable: 'UNF', indexMissing: true },
        { type: 'GEO', property: 'loc', sortable: true },
        {
          type: 'VECTOR', property: 'vector', algorithm: 'FLAT', dataType: 'FLOAT32',
          dim: 2, distanceMetric: 'COSINE', initialCap: 100, blockSize: 64,
        },
        {
          type: 'VECTOR', property: 'vector_hnsw', algorithm: 'HNSW', dataType: 'FLOAT32',
          dim: 2, distanceMetric: 'COSINE', initialCap: 100, m: 16, efConstruction: 200, efRuntime: 10, epsilon: 0.01,
        },
      ], {
        on: 'HASH',
        prefixes: [prefix],
        filter: '@price >= 0',
        language: 'english',
        noOffsets: true,
        noHL: true,
        stopwords: ['the'],
      });

      await r.redis.hset(h1, 'title', 'red shoes', 'tag', 'sale,new', 'price', '10',
        'loc', '13.361389,38.115556', 'vector', vec(1, 0), 'vector_hnsw', vec(1, 0));
      await r.redis.hset(h2, 'title', 'red hat', 'tag', 'new', 'price', '20',
        'loc', '15.087269,37.502669', 'vector', vec(0.9, 0.1), 'vector_hnsw', vec(0.9, 0.1));

      const tagHit = await search.search(idx, '@tag:{sale}');
      expect(tagHit.total).toBe(1);
      expect(tagHit.docs[0].id).toBe(h1);
      expect((await search.search(idx, '@tag:{NEW}')).total).toBe(0);
      expect((await search.search(idx, '@price:[0 15]')).total).toBe(1);
      const geo = await search.search(idx, '@loc:[13.361389 38.115556 10 km]');
      expect(geo.total).toBe(1);
      expect(geo.docs[0].id).toBe(h1);

      const sorted = await search.search(idx, '@tag:{new}', { sortBy: 'name', sortAsc: true, returnFields: ['name', 'price'] });
      expect(sorted.docs.map((d) => d.name)).toEqual(['red hat', 'red shoes']);

      const knn = await search.search(idx, '*=>[KNN 1 @vector $q AS dist]', {
        params: { q: vec(1, 0) }, returnFields: ['name', 'dist'], dialect: 2,
      });
      expect(knn.total).toBe(1);
      expect(knn.docs[0].id).toBe(h1);
      const hnsw = await search.search(idx, '*=>[KNN 1 @vector_hnsw $q AS dist]', {
        params: { q: vec(1, 0) }, returnFields: ['name'], dialect: 2,
      });
      expect(hnsw.docs[0].id).toBe(h1);
      expect(JSON.stringify(await search.info(idx))).toContain('name');

      await search.createIndex(alt, [{ type: 'TEXT', property: 'title' }], { prefixes: [altPrefix] });
      await search.alter(alt, [{ type: 'TAG', property: 'tag' }], true);
      const a1 = `${altPrefix}1`;
      await r.redis.hset(a1, 'title', 'altered', 'tag', 'fresh');
      const altHit = await search.search(alt, '@tag:{fresh}');
      expect(altHit.total).toBe(1);
      expect(altHit.docs[0].id).toBe(a1);
    } finally {
      await r.getKeys().delete(h1, h2, `${altPrefix}1`);
      await r.getSearch().dropIndex(idx, true).catch(() => undefined);
      await r.getSearch().dropIndex(alt, true).catch(() => undefined);
      await r.shutdown();
    }
  }, 120000);
});
