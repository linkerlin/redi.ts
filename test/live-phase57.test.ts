/**
 * Phase 57 live: structured SVS-VAMANA vector indexes (compression and build
 * parameters), plus KNN search on the plain SVS field.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p57:';
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

function vec(a: number, b: number): Buffer {
  const buf = Buffer.alloc(8);
  buf.writeFloatLE(a, 0);
  buf.writeFloatLE(b, 4);
  return buf;
}

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live phase 57: SVS-VAMANA indexes', () => {
  it('creates SVS fields with compression/build options and runs KNN', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const prefix = `${NS}h:`;
    const h1 = `${prefix}1`;
    const h2 = `${prefix}2`;
    try {
      const search = r.getSearch();
      await search.dropIndex(idx, true).catch(() => undefined);
      await search.createIndex(idx, [
        {
          type: 'VECTOR', property: 'v', algorithm: 'SVS-VAMANA',
          dataType: 'FLOAT32', dim: 2, distanceMetric: 'COSINE',
        },
        {
          type: 'VECTOR', property: 'vc', algorithm: 'SVS-VAMANA',
          dataType: 'FLOAT32', dim: 2, distanceMetric: 'COSINE',
          compression: 'LVQ8', constructionWindowSize: 200, graphMaxDegree: 32,
          searchWindowSize: 10, epsilon: 0.01, trainingThreshold: 1024,
        },
      ], { on: 'HASH', prefixes: [prefix] });

      await r.redis.hset(h1, 'v', vec(1, 0), 'vc', vec(1, 0));
      await r.redis.hset(h2, 'v', vec(0, 1), 'vc', vec(0, 1));
      const knn = await search.search(idx, '*=>[KNN 2 @v $q AS dist]', {
        params: { q: vec(1, 0) }, dialect: 2, returnFields: ['dist'],
      });
      expect(knn.total).toBe(2);
      const ids = knn.docs.map((d) => d.id);
      expect(ids).toEqual(expect.arrayContaining([h1, h2]));
      expect(JSON.stringify(await search.info(idx))).toContain('SVS-VAMANA');
    } finally {
      await r.getKeys().delete(h1, h2);
      await r.getSearch().dropIndex(idx, true).catch(() => undefined);
      await r.shutdown();
    }
  }, 120000);
});
