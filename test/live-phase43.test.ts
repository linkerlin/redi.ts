/**
 * Phase 43 live: RJsonBucket 4.6.1 multi-match surface — `*Multi` variants,
 * multi-path `get(codec, paths...)`, and the two-argument `arrayIndex`
 * overload (full-array search, matching Java).
 *
 * Needs Redis with the JSON module; capability-gated.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p43:';
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

describe('live phase 43: RJsonBucket multi-match surface', () => {
  it('*Multi variants, multi-path get and full-array arrayIndex', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const docName = `${NS}doc`;
    const arrName = `${NS}arr`;
    const objName = `${NS}obj`;
    const numName = `${NS}num`;
    try {
      if (!(await hasCmd(r.redis, 'JSON.SET'))) {
        console.log('[phase43] JSON unsupported — skipping');
        return;
      }
      await r.getKeys().delete(docName, arrName, objName, numName);

      const doc = r.getJsonBucket<any>(docName);
      await doc.set({ nums: [1, 2, 1], b: { c: 1, s: 'xy' } });
      // two-arg arrayIndex searches the whole array; the four-arg form honours [start, stop)
      expect(await doc.arrayIndex('$.nums', 1)).toBe(0);
      expect(await doc.arrayIndex('$.nums', 2, 0, 2)).toBe(1);
      expect(await doc.arrayIndex('$.nums', 2, 2, 3)).toBe(-1);
      const multi = await doc.get(null, '$.b.c', '$.b.s');
      expect(multi).toEqual({ '$.b.c': [1], '$.b.s': ['xy'] });

      const arr = r.getJsonBucket<any>(arrName);
      await arr.set({ x: { arr: [1, 2] }, y: { arr: [3] } });
      expect(await arr.arraySizeMulti('$..arr')).toEqual([2, 1]);
      expect(await arr.arrayAppendMulti('$..arr', 9)).toEqual([3, 2]);
      expect(await arr.arrayIndexMulti('$..arr', 9)).toEqual([2, 1]);
      expect(await arr.arrayIndexMulti('$..arr', 9, 0, 2)).toEqual([-1, 1]);
      expect(await arr.arrayInsertMulti('$..arr', 0, 0)).toEqual([4, 3]);
      expect(await arr.arrayPollFirstMulti(null, '$..arr')).toEqual([0, 0]);
      expect(await arr.arrayPopMulti(null, '$..arr', 1)).toEqual([2, 9]);
      expect(await arr.arrayPollLastMulti(null, '$..arr')).toEqual([9, 3]);
      expect(await arr.arrayTrimMulti('$..arr', 0, 0)).toEqual([1, 0]);
      expect(await arr.get('$..arr')).toEqual([[1], []]);

      const obj = r.getJsonBucket<any>(objName);
      await obj.set({ b: { c: 1, s: 'xy' }, x: { flag: true }, y: { flag: false } });
      expect(await obj.stringSizeMulti('$..s')).toEqual([2]);
      expect(await obj.stringAppendMulti('$..s', '!')).toEqual([3]);
      expect(await obj.countKeysMulti('$.b')).toEqual([2]);
      expect(await obj.getKeysMulti('$.b')).toEqual([['c', 's']]);
      expect(await obj.toggleMulti('$..flag')).toEqual([false, true]);

      const num = r.getJsonBucket<any>(numName);
      await num.set({ x: { n: 1 }, y: { n: 2 } });
      expect(await num.incrementAndGetMulti('$..n', 5)).toEqual([6, 7]);
      expect(await num.get('$..n')).toEqual([6, 7]);
    } finally {
      await r.getKeys().delete(docName, arrName, objName, numName);
      await r.shutdown();
    }
  }, 120000);
});
