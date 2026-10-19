/**
 * Phase 13 live interop: RStream consumer-group ops (group/consumer lifecycle,
 * pending info/range, claim/autoClaim, info/list). Needs real Redis + probe.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p13:';
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


async function cleanExact(r: Redisson, ...keys: string[]): Promise<void> {
  if (keys.length > 0) await r.getKeys().delete(...keys);
}

const hasRedis = await redisUp();

describe('live interop phase 13: stream groups/pending/claim', () => {
  it('group lifecycle + pending + claim across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}grouplife`;
    try {
      await cleanExact(r, s);
      const st = r.getStream(s);
      await st.add({ n: 'one' });
      await st.add({ n: 'two' });
      await st.createGroup('gx');
      expect(await probe(`stream-groups ${s}`)).toBe('GROUPS:[gx]');
      // Java consumes both as jx
      const resKey = `${s}:read-result`;
      await cleanExact(r, resKey);
      expect(await probe(`stream-group-read ${s} gx jx ${resKey}`)).toMatch(/^STREAM_GOT:/);
      const info = await st.getPendingInfo('gx');
      expect(info.count).toBe(2);
      expect(info.consumers).toEqual({ jx: 2 });
      const rows = await st.listPending('gx');
      expect(rows.length).toBe(2);
      expect(rows[0].consumer).toBe('jx');
      // Java claims the first entry over to rescuer (id handed over by TS)
      expect(await probe(`stream-claim ${s} gx rescuer ${rows[0].id}`)).toBe('CLAIMED:1');
      const after = await st.getPendingInfo('gx');
      expect(after.consumers).toEqual({ jx: 1, rescuer: 1 });
      // pendingRange fetches bodies regardless of owner
      const bodies = await st.pendingRange('gx');
      expect(bodies.length).toBe(2);
      // TS acks everything; Java sees an empty PEL
      await st.ack('gx', ...rows.map((e) => e.id));
      expect(await probe(`stream-pending ${s} gx`)).toBe('PENDING:0');
      // SETID rewinds delivery: Java gets both again as new
      await st.updateGroupMessageId('gx', '0-0');
      expect(await probe(`stream-group-read ${s} gx jx ${resKey}`)).toMatch(/^STREAM_GOT:/);
      // teardown visible on both sides
      await st.removeConsumer('gx', 'jx');
      await st.removeConsumer('gx', 'rescuer');
      await st.removeGroup('gx');
      expect(await probe(`stream-groups ${s}`)).toBe('GROUPS:[]');
    } finally {
      await r.shutdown();
    }
  }, 240000);

  it('autoClaim + info + consumers across languages', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}autoclaim`;
    try {
      await cleanExact(r, s);
      const st = r.getStream(s);
      await st.add({ n: 'a' });
      await st.add({ n: 'b' });
      await st.add({ n: 'c' });
      await st.createGroup('ga');
      expect(await st.readGroup('ga', 'c-old', 10, 500)).toHaveLength(3);
      // idle 0 => deterministic immediate takeover, no sleep needed
      const taken = await st.autoClaim('ga', 'c-new', 0);
      expect(taken.entries).toHaveLength(3);
      expect(taken.nextId).toBe('0-0');
      // group-level count unchanged; ownership moved
      expect(await probe(`stream-pending ${s} ga`)).toBe('PENDING:3');
      const rows = await st.listPending('ga');
      expect(new Set(rows.map((e) => e.consumer))).toEqual(new Set(['c-new']));
      // info / groups / consumers shapes
      const info = await st.getInfo();
      expect(info.length).toBe(3);
      expect(info.groups).toBe(1);
      expect(info.lastGeneratedId).toMatch(/^\d+-\d+$/);
      expect((await st.listGroups()).map((g) => g.name)).toEqual(['ga']);
      const consumers = await st.listConsumers('ga');
      const byName = new Map(consumers.map((c) => [c.name, c.pending]));
      expect(byName.get('c-new')).toBe(3);
      expect(await probe(`stream-groups ${s}`)).toBe('GROUPS:[ga]');
      // Java claims one entry for itself; TS sees the split
      const firstId = rows[0].id;
      expect(await probe(`stream-claim ${s} ga jx2 ${firstId}`)).toBe('CLAIMED:1');
      const split = await st.getPendingInfo('ga');
      expect(split.consumers['jx2']).toBe(1);
      expect(split.consumers['c-new']).toBe(2);
    } finally {
      await r.shutdown();
    }
  }, 240000);
});
