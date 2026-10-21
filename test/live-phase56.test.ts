/**
 * Phase 56 live: RMap.putAll(Map, batchSize) — field batching preserved on the
 * wire, Java reads back the HASH.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p56:';
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

describe('live phase 56: RMap putAll batchSize', () => {
  it('writes all batches and leaves a Java-readable hash', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}m`;
    try {
      await r.getKeys().delete(name);
      const m = r.getMap<string, string>(name);
      await m.putAll(new Map([
        ['k1', 'v1'], ['k2', 'v2'], ['k3', 'v3'], ['k4', 'v4'], ['k5', 'v5'],
      ]), 2);
      expect(await m.size()).toBe(5);
      expect(await m.get('k3')).toBe('v3');
      expect(await probe(`map-get ${name} k5`)).toBe('MAP_GET:v5');
      await expect(m.putAll({ x: '1' }, 0)).rejects.toThrow(/batchSize/);
      expect(await m.size()).toBe(5);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 120000);
});
