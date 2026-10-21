/**
 * Phase 31 live interop: ObjectListener via Redis keyspace notifications
 * (RStream xadd/xdel/xgroup-*, RTimeSeries zadd/zrem) and multi-stream
 * XREADGROUP (`readGroupMany`).
 *
 * Keyspace notifications need `notify-keyspace-events` with K+E ("KEAn" is
 * used so concurrent RKeys new-event tests retain the `n` flag); the test
 * serializes config changes with other keyspace tests, saves and restores the
 * previous value and skips (with a log) when the server refuses CONFIG SET.
 *
 * Needs real Redis; skips when Redis is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { prefixName } from '../src/util';
import { acquireKeyspaceConfigLock } from './helpers/keyspace-lock';

const NS = 'TsJavaT:p31:';
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
const sleep = (ms: number) => new Promise<void>((x) => setTimeout(x, ms));

async function waitFor(check: () => boolean, timeoutMs: number): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return true;
    await sleep(50);
  }
  return check();
}

describe('live interop phase 31: keyspace listeners + multi-stream group read', () => {
  it('ObjectListener receives stream/time-series keyspace events', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const sn = `${NS}stream`;
    const tn = `${NS}ts`;
    let prev = '';
    let canConfig = false;
    let releaseLock: (() => Promise<void>) | null = null;
    try {
      releaseLock = await acquireKeyspaceConfigLock(r.redis);
      const cur: any = await r.redis.config('GET', 'notify-keyspace-events');
      prev = Array.isArray(cur) ? String(cur[1] ?? '') : '';
      await r.redis.config('SET', 'notify-keyspace-events', 'KEAn');
      canConfig = true;
    } catch {
      console.log('[phase31] notify-keyspace-events not configurable — skipping keyspace listener assertions');
    }
    try {
      if (!canConfig) return;
      await r.getKeys().delete(sn, tn, prefixName('redisson__ts_ttl', tn), prefixName('redisson__ts_seq', tn));

      const st = r.getStream(sn);
      const events: string[] = [];
      const sid = await st.addListener({
        onAdd: () => events.push('add'),
        onRemove: () => events.push('remove'),
        onCreateGroup: () => events.push('creategroup'),
        onRemoveGroup: () => events.push('removegroup'),
        onCreateConsumer: () => events.push('createconsumer'),
        onRemoveConsumer: () => events.push('removeconsumer'),
      });
      await st.createGroup('g1', '0');
      await st.createConsumer('g1', 'c1');
      const id = await st.add({ m: 'a' });
      await st.deleteEntries(id);
      await st.removeConsumer('g1', 'c1');
      await st.removeGroup('g1');
      expect(await waitFor(() => ['add', 'remove', 'creategroup', 'removegroup', 'createconsumer', 'removeconsumer'].every((e) => events.includes(e)), 4000)).toBe(true);
      const addsBefore = events.filter((e) => e === 'add').length;
      await st.removeListener(sid);
      await st.add({ m: 'b' });
      await sleep(400);
      expect(events.filter((e) => e === 'add').length).toBe(addsBefore); // removed listener is silent
      await st.removeAllListeners();

      const ts = r.getTimeSeries<string>(tn);
      const tsEvents: string[] = [];
      await ts.addListener({
        onAdd: () => tsEvents.push('add'),
        onRemove: () => tsEvents.push('remove'),
      });
      await ts.add(1000, 'v');
      await ts.remove(1000);
      expect(await waitFor(() => tsEvents.includes('add') && tsEvents.includes('remove'), 4000)).toBe(true);
      await ts.removeAllListeners();
    } finally {
      await r.getKeys().delete(sn, tn, prefixName('redisson__ts_ttl', tn), prefixName('redisson__ts_seq', tn));
      if (canConfig) {
        try { await r.redis.config('SET', 'notify-keyspace-events', prev); } catch { /* noop */ }
      }
      await releaseLock?.();
      await r.shutdown();
    }
  }, 180000);

  it('readGroupMany reads several streams in one XREADGROUP', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = `${NS}a`;
    const s2 = `${NS}b`;
    try {
      await r.getKeys().delete(s1, s2);
      const a = r.getStream(s1);
      const b = r.getStream(s2);
      await a.add({ m: 'x' });
      await b.add({ m: 'y' });
      await a.createGroup('g1', '0');
      await b.createGroup('g1', '0');
      const read = await a.readGroupMany('g1', 'c1', { [s1]: '>', [s2]: '>' }, 10);
      expect(read.get(s1)?.map((e) => e.fields.m)).toEqual(['x']);
      expect(read.get(s2)?.map((e) => e.fields.m)).toEqual(['y']);
      // second read with '>' sees nothing new
      const again = await a.readGroupMany('g1', 'c1', { [s1]: '>', [s2]: '>' }, 10);
      expect(again.size).toBe(0);
    } finally {
      await r.getKeys().delete(s1, s2);
      await r.shutdown();
    }
  }, 120000);
});
