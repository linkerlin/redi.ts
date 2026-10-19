/**
 * Phase 14 live interop: RStream add/readGroup full options, nack, ackWithDelete.
 * Arg order mirrors RedissonStream exactly (NOMKSTREAM, IDMP/IDMPAUTO, MAXLEN
 * [~], MINID [~], LIMIT, id; GROUP g c [COUNT] [BLOCK] [CLAIM] [NOACK]).
 * Needs real Redis + probe. XNACK requires Redis 8.10+ (absent on 8.6) —
 * the nack asserts are capability-gated; XACKDEL/XDELEX need 8.2+.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe';

const NS = 'TsJavaT:p14:';
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

/** true when the server knows the command (COMMAND INFO returns a block). */
async function hasCmd(r: Redisson, name: string): Promise<boolean> {
  try {
    const info: unknown = await (r.redis as any).call('COMMAND', 'INFO', name);
    return Array.isArray(info) && info.length > 0 && Array.isArray(info[0]);
  } catch {
    return false;
  }
}

const hasRedis = await redisUp();

describe('live interop phase 14: stream add/read options, nack, ackdel', () => {
  it('add NOMKSTREAM/MINID/strict/IDMP + noAck/claim reads', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}opts`;
    try {
      await cleanExact(r, s);
      const st = r.getStream(s);
      // NOMKSTREAM on a missing key creates nothing
      expect(await st.addWithOptions({ n: 'x' }, { noMakeStream: true })).toBeNull();
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:0');
      const id1 = (await st.add({ n: 'a' })) as string;
      const id2 = (await st.add({ n: 'b' })) as string;
      await st.add({ n: 'c' });
      // MINID trims everything below id2
      await st.addWithOptions({ n: 'd' }, { minId: id2, strict: true });
      expect(await st.size()).toBe(3);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:3');
      const first = await st.read(1, '0');
      expect(first[0].id).toBe(id2);
      void id1;
      // strict MAXLEN caps exactly (unlike '~' parity default)
      await st.addWithOptions({ n: 'e' }, { maxLen: 2, strict: true });
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:2');
      // idempotent producer: same (producer, id) returns the same entry id
      const e1 = await st.addWithOptions({ n: 'f' }, { idempotentProducerId: 'p1', idempotentId: 'e1' });
      const e2 = await st.addWithOptions({ n: 'f' }, { idempotentProducerId: 'p1', idempotentId: 'e1' });
      expect(e1).toBe(e2);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:3');
    } finally {
      await r.shutdown();
    }
  }, 180000);

  it('noAck leaves PEL empty; CLAIM takes over; nack requeues; ackdel deletes', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}ackflow`;
    try {
      await cleanExact(r, s);
      const st = r.getStream(s);
      await st.add({ n: 'one' });
      await st.add({ n: 'two' });
      await st.createGroup('gn');
      // NOACK delivery is invisible to the PEL on both sides
      expect(await st.readGroupWith('gn', 'c-noack', { noAck: true })).toHaveLength(2);
      expect(await probe(`stream-pending ${s} gn`)).toBe('PENDING:0');
      // normal read creates PEL; nack FAIL requeues with bumped delivery count
      await st.updateGroupMessageId('gn', '0-0');
      const got = await st.readGroup('gn', 'c1', 10);
      expect(got).toHaveLength(2);
      expect(await probe(`stream-pending ${s} gn`)).toBe('PENDING:2');
      if (await hasCmd(r, 'XNACK')) {
        expect(await st.nack('gn', 'FAIL', got.map((e) => e.id))).toBe(2);
        // NACK only requeues (counter bumps on actual redelivery, not on nack)
        expect((await st.listPending('gn')).map((e) => e.deliveredCount)).toEqual([1, 1]);
        expect(await probe(`stream-pending ${s} gn`)).toBe('PENDING:2');
      } else {
        console.log('SKIP nack asserts: XNACK absent on this server (needs Redis 8.10+)');
      }
      const redelivered = await st.autoClaim('gn', 'c9', 0);
      expect(redelivered.entries).toHaveLength(2);
      expect((await st.listPending('gn')).map((e) => e.deliveredCount)).toEqual([2, 2]);
      // CLAIM takeover without XAUTOCLAIM: sleep past idle, read with CLAIM
      await new Promise((x) => setTimeout(x, 1200));
      const taken = await st.readGroupWith('gn', 'c2', { claimIdleMs: 1000 });
      expect(taken).toHaveLength(2);
      // ackWithDelete DELREF removes entries physically (plain ack would not)
      const del = await st.ackWithDelete('gn', 'DELREF', ...redelivered.entries.map((e) => e.id));
      expect(del.size).toBe(2);
      expect(await probe(`stream-size ${s}`)).toBe('STREAM_SIZE:0');
      expect(await probe(`stream-pending ${s} gn`)).toBe('PENDING:0');
    } finally {
      await r.shutdown();
    }
  }, 240000);
});
