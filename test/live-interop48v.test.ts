/**
 * Live interop vs **Java Redisson 4.8.0**, round 22 — RStream: the biggest
 * family still without 4.8.0-jar evidence (4.6.1 probe covered it; 4.8 had
 * zero stream commands until now).
 *
 *  - add/read both directions: field names AND values are codec-encoded on
 *    the wire; each side decodes the other's entries through its own codec.
 *  - PEL is cross-language: a Java consumer's unacked reads appear in TS
 *    listPending and vice versa; XCLAIM/XAUTOCLAIM/ack all work across the
 *    language boundary (same PEL + consumer shapes).
 *
 * Probe commands stream48-*.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48v:';

const hasRedis = await (async () => {
  const r = new Redis(REDIS_URL, { lazyConnect: true, connectTimeout: 3000, maxRetriesPerRequest: 1 });
  try {
    await r.connect();
    return true;
  } catch {
    return false;
  } finally {
    r.disconnect();
  }
})();
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48v] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48v: streams vs Java 4.8.0', () => {
  it('RStream: add/read both directions decode the other codec entries', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}x1`;
    try {
      await r.getKeys().delete(name);

      expect(await probe48.probe(`stream48-create-group ${name} g1`)).toBe('group=ok');
      const javaLine = await probe48.probe(`stream48-add ${name} sensor 42 unit C`);
      const javaId = javaLine.replace('id=', '');
      expect(javaId).toMatch(/^\d+-\d+$/);
      expect(await probe48.probe(`stream48-size ${name}`)).toBe('size=1');

      // TS group-read decodes Java's codec-encoded fields
      const st = r.getStream(name);
      const got = await st.readGroup('g1', 'ts-c', 10);
      expect(got).toHaveLength(1);
      expect(got[0].id).toBe(javaId);
      expect(got[0].fields).toEqual({ sensor: '42', unit: 'C' });

      // TS add → Java group-read decodes TS's codec-encoded fields
      const tsId = await st.add({ temp: '19' });
      expect(await probe48.probe(`stream48-size ${name}`)).toBe('size=2');
      const lines = await probe48.probeLines(`stream48-read-group ${name} g1 j-c 10`);
      expect(lines).toHaveLength(1);
      expect(lines[0]).toBe(`id=${tsId} fields={temp=19}`);
      expect(await probe48.probe(`stream48-groups ${name}`)).toBe('groups=[g1]');
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RStream: pending/claim/ack cross-language on a Java-consumer PEL', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}x2`;
    try {
      await r.getKeys().delete(name);
      const st = r.getStream(name);

      expect(await probe48.probe(`stream48-create-group ${name} g2`)).toBe('group=ok');
      const id1 = (await probe48.probe(`stream48-add ${name} k v1`)).replace('id=', '');
      const id2 = (await probe48.probe(`stream48-add ${name} k v2`)).replace('id=', '');
      expect(await probe48.probeLines(`stream48-read-group ${name} g2 j-a 10`)).toHaveLength(2);

      // Java's unacked reads are visible from TS (same PEL wire shape)
      const pending = await st.listPending('g2');
      expect(pending.map((p) => p.id).sort()).toEqual([id1, id2].sort());
      expect(pending.every((p) => p.consumer === 'j-a')).toBe(true);

      // TS claims one back and acks it; Java-side pending shrinks
      const claimed = await st.claim('g2', 'ts-r', 0, id1);
      expect(claimed.map((e) => e.id)).toEqual([id1]);
      expect(claimed[0].fields).toEqual({ k: 'v1' });
      expect(await st.ack('g2', id1)).toBe(1);
      expect((await probe48.probeLines(`stream48-pending ${name} g2`)).map((l) => l.split(' ')[0])).toEqual([`id=${id2}`]);

      // Java acks its own remaining entry → PEL empty for both
      expect(await probe48.probe(`stream48-ack ${name} g2 ${id2}`)).toBe('acked=1');
      expect(await probe48.probeLines(`stream48-pending ${name} g2`)).toHaveLength(0);
      expect(await st.listPending('g2')).toHaveLength(0);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('RStream: Java XAUTOCLAIM takes over a TS-consumer PEL', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}x3`;
    try {
      await r.getKeys().delete(name);
      const st = r.getStream(name);

      await st.createGroup('g3'); // TS group creation (Java reads it)
      await st.add({ a: '1' });
      await st.add({ a: '2' });
      const read = await st.readGroup('g3', 'ts-a', 10); // unacked
      expect(read).toHaveLength(2);

      // Java sees TS's pending entries, then XAUTOCLAIMs them (min-idle 0)
      const before = await probe48.probeLines(`stream48-pending ${name} g3`);
      expect(before.every((l) => l.endsWith('consumer=ts-a'))).toBe(true);
      const claimed = await probe48.probeLines(`stream48-autoclaim ${name} g3 j-b 0`);
      expect(claimed).toHaveLength(2);
      expect(claimed.filter((l) => l.includes('fields={a=1}'))).toHaveLength(1);
      expect(claimed.some((l) => l.includes('fields={a=2}'))).toBe(true);

      // ownership transferred: TS now sees consumer j-b; TS acks both
      const pending = await st.listPending('g3');
      expect(pending.map((p) => p.consumer)).toEqual(['j-b', 'j-b']);
      await st.ack('g3', ...pending.map((p) => p.id));
      expect(await probe48.probeLines(`stream48-pending ${name} g3`)).toHaveLength(0);
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);
});
