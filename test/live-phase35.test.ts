/**
 * Phase 35 live: RJsonBucket (Redis 8 built-in JSON module) — document and
 * path get/set, NX/XX, atomic CAS/getAndSet/getAndDelete, string/array/number
 * operations, merge/keys/type/toggle/clear/delete, plus Java 4.6.1
 * cross-language reads and writes (json-set/json-get probes use JacksonCodec,
 * which writes plain JSON).
 *
 * Skips with a log when the JSON module is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p35:';
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
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

describe('live interop phase 35: RJsonBucket', () => {
  it('document/path APIs + Java cross-language JSON', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}doc`;
    const jname = `${NS}java`;
    try {
      if (!(await hasCmd(r.redis, 'JSON.SET'))) {
        console.log('[phase35] JSON module unsupported — skipping');
        return;
      }
      await r.getKeys().delete(name, jname);
      await probe(`json-get ${jname}`); // warm
      const b = r.getJsonBucket<any>(name);
      await b.set({ a: 1, arr: [1, 2, 3], s: 'hi', b: true, o: { x: 1 } });
      expect(await b.get()).toEqual({ a: 1, arr: [1, 2, 3], s: 'hi', b: true, o: { x: 1 } });
      expect(await b.get('$.a')).toEqual([1]);
      expect(await b.getType()).toBe('object');
      expect(await b.getType('$.a')).toBe('integer');

      expect(await b.stringSize('$.s')).toBe(2);
      expect(await b.stringAppend('$.s', '!')).toBe(3);
      expect(await b.get('$.s')).toEqual(['hi!']);

      expect(await b.arrayAppend('$.arr', 4)).toBe(4);
      expect(await b.arrayIndex('$.arr', 2)).toBe(1);
      expect(await b.arrayInsert('$.arr', 0, 0)).toBe(5);
      expect(await b.arraySize('$.arr')).toBe(5);
      expect(await b.arrayPop('$.arr', -1)).toBe(4);
      expect(await b.arrayPollFirst('$.arr')).toBe(0);
      expect(await b.arrayTrim('$.arr', 0, 0)).toBe(1);
      expect(await b.arraySize('$.arr')).toBe(1);

      expect(await b.incrementAndGet('$.o.x', 2)).toBe(3);
      await b.merge('$.o', { y: 2 });
      expect(await b.get('$.o.y')).toEqual([2]);
      expect(await b.countKeys()).toBe(5);
      expect((await b.getKeys('$.o')).sort()).toEqual(['x', 'y']);
      expect(await b.toggle('$.b')).toBe(false);
      expect(await b.clear('$.arr')).toBe(1);
      expect(await b.get('$.arr')).toEqual([[]]);

      expect(await b.setIfAbsent('$.newk', 7)).toBe(true);
      expect(await b.setIfAbsent('$.newk', 8)).toBe(false);
      expect(await b.setIfExists('$.newk', 9)).toBe(true);
      expect(await b.get('$.newk')).toEqual([9]);

      // CAS path/root and atomic get/set/delete
      expect(await b.compareAndSet('$.o.x', 3, 4)).toBe(true);
      expect(await b.compareAndSet('$.o.x', 3, 5)).toBe(false);
      expect(await b.get('$.o.x')).toEqual([4]);
      expect(await b.getAndSet('$.o.x', 7)).toEqual([4]);
      const prev = await b.getAndSet({ z: 1 });
      expect(typeof prev).toBe('object');
      expect(prev).toHaveProperty('o');
      expect(prev).toHaveProperty('s');
      expect(await b.getAndDelete()).toEqual({ z: 1 });
      expect(await b.get()).toBeNull();

      // Java writes plain JSON via JacksonCodec; TS reads it.
      expect(await probe(`json-set ${jname}`)).toBe('JSON_SET');
      expect(await b.get()).toBeNull(); // different key
      const jb = r.getJsonBucket<any>(jname);
      expect(await jb.get()).toEqual({ a: 1 });
      // TS plain JSON is readable by Java's JacksonCodec<Map>.
      await jb.set({ b: 2 });
      expect(await probe(`json-get ${jname}`)).toBe('JSON_GET:{b=2}');
    } finally {
      await r.getKeys().delete(name, jname);
      await r.shutdown();
    }
  }, 120000);
});
