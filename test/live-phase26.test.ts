/**
 * Phase 26 live interop: topic management surfaces (channel names / listener
 * counts / active topics / sharded subscriber counts) and RStream
 * fastClaim (XCLAIM JUSTID), fastAutoClaim (XAUTOCLAIM JUSTID) and readMany
 * (single XREAD over several streams).
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p26:';
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

const hasRedis = await redisUp();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

describe('live interop phase 26: topic management + stream claim', () => {
  it('topic surfaces reflect live subscriptions', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}topic`;
    const shName = `${NS}shard`;
    try {
      const t = r.getTopic<string>(name);
      expect(await t.getChannelNames()).toEqual([name]);
      const seen: string[] = [];
      const id = await t.addListener((m) => seen.push(m));
      try {
        expect(await t.countListeners()).toBe(1);
        expect(await t.countSubscribers()).toBe(1); // PUBSUB NUMSUB
        const active = await r.getPatternTopic<string>(`${NS}*`).getActiveTopics();
        expect(active).toContain(name);
        expect(await t.publish('hello')).toBeGreaterThanOrEqual(1);
        for (let i = 0; i < 50 && seen.length === 0; i++) await sleep(100);
        expect(seen).toEqual(['hello']);
      } finally {
        await t.removeListener(id);
        await t.removeAllListeners();
      }
      expect(await t.countListeners()).toBe(0);

      const sh = r.getShardedTopic<string>(shName);
      expect(await sh.getChannelNames()).toEqual([shName]);
      const sid = await sh.addListener(() => undefined);
      try {
        expect(await sh.countListeners()).toBe(1);
        expect(await sh.countSubscribers()).toBeGreaterThanOrEqual(1); // SHARDNUMSUB
      } finally {
        await sh.removeListener(sid);
        await sh.removeAllListeners();
      }
      expect(await sh.countListeners()).toBe(0);
    } finally {
      await r.shutdown();
    }
  }, 120000);

  it('fastClaim/fastAutoClaim return ids and readMany reads several streams', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}s1`;
    const s2 = `${NS}s2`;
    try {
      await r.getKeys().delete(s1, s2);
      const st = r.getStream(s1);
      await st.add({ m: 'a' });
      await st.add({ m: 'b' });
      await st.createGroup('g1', '0');
      const read = await st.readGroup('g1', 'c1', 10);
      expect(read.map((e) => e.fields.m)).toEqual(['a', 'b']);

      // JUSTID replies: plain id arrays
      const claimed = await st.fastClaim('g1', 'c2', 0, ...read.map((e) => e.id));
      expect(claimed.sort()).toEqual(read.map((e) => e.id).sort());
      const ac = await st.fastAutoClaim('g1', 'c3', 0, '0-0', 10);
      expect(ac.nextId).toMatch(/^\d+-\d+$/);
      expect(ac.ids.length).toBeGreaterThanOrEqual(1);

      const st2 = r.getStream(s2);
      await st2.add({ m: 'x' });
      const many = await st.readMany({ [s1]: '0', [s2]: '0' }, 10);
      expect(many.get(s1)?.map((e) => e.fields.m)).toEqual(['a', 'b']);
      expect(many.get(s2)?.map((e) => e.fields.m)).toEqual(['x']);
      // count cap applies to the single XREAD
      const one = await st.readMany({ [s1]: '0' }, 1);
      expect(one.get(s1)?.length).toBe(1);
    } finally {
      await r.getKeys().delete(s1, s2);
      await r.shutdown();
    }
  }, 120000);
});
