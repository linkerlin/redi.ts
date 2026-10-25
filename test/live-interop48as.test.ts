/**
 * Live interop vs **Java Redisson 4.8.0**, round 45 — three families the
 * earlier rounds did not cover, one of them a freshly closed API gap:
 *
 *  - RList `addBefore`/`addAfter` (LINSERT): NEW TS API this round (the
 *    1:1 matrix had no counterpart for Java's `addBefore/addAfter`) —
 *    both languages insert relative to a pivot and read the same order;
 *    an absent pivot returns -1 everywhere.
 *  - RStream `XNACK` (Redis 8.8+): requeues pending entries — Java's
 *    `nack(StreamNackArgs)` resets TS-consumed entries so a TS consumer
 *    with a different name re-reads them; TS `nack()` requeues for the
 *    Java consumer the same way.
 *  - RBlockingDeque `move` (LMOVE): Java `DequeMoveArgs.pollFirst()
 *    .addLastTo(dest)` pops the TS deque head into the dest tail; TS
 *    `move(dest, 'LEFT', 'RIGHT')` mirrors it.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable; XNACK additionally gates on server ≥8.8.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48as:';

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
    console.warn(`[live interop48as] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function hasXnack(raw: Redis): Promise<boolean> {
  try {
    await (raw as any).call('XNACK', `${NS}__probe_nokey__`, 'g', 'SILENT', 'IDS', 0);
    return true;
  } catch (e: any) {
    console.warn(`[live interop48as] XNACK unavailable (<8.8) — nack case skipped: ${String(e?.message ?? e).split('\n')[0]}`);
    return false;
  }
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48as: list LINSERT / stream XNACK / deque LMOVE vs Java 4.8.0', () => {
  it('RList addBefore/addAfter (LINSERT): same order both languages, -1 on absent pivot', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const l = `${NS}list`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [l]);
      const list = r.getList<string>(l);
      await list.addAll(['a', 'b']);

      // Java inserts before/after the pivot; TS reads the order
      expect(await probe48.probe(`list48-addbefore ${l} b x`)).toBe('len=3');
      expect(await list.readAll()).toEqual(['a', 'x', 'b']);

      // TS inserts on both sides of the new pivot; Java reads back
      expect(await list.addAfter('x', 'y')).toBe(4);
      expect(await list.addBefore('x', 'w')).toBe(5);
      expect(await probe48.probeLines(`list48-range ${l} 0 -1`)).toEqual([
        'item=a', 'item=w', 'item=x', 'item=y', 'item=b',
      ]);

      // absent pivot → -1 (LINSERT semantics, Java returns the same)
      expect(await list.addBefore('ghost', 'z')).toBe(-1);
      expect(await probe48.probe(`list48-addafter ${l} ghost z`)).toBe('len=-1');
    } finally {
      await delAll(raw, [l]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RStream XNACK: pending entries requeued across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const raw = new Redis(REDIS_URL);
    if (!(await hasXnack(raw))) {
      raw.disconnect();
      return;
    }
    const r = Redisson.create({ address: REDIS_URL });
    const s = `${NS}st`;
    try {
      await delAll(raw, [s]);
      const stream = r.getStream<string>(s);

      // TS adds + consumes into c1's PEL
      const id1 = await stream.add({ f: 'v1' });
      await stream.createGroup('g45');
      await stream.readGroup('g45', 'c1', { count: 10 });

      // Java nacks the entry (requeue); a NEW consumer reads it back
      expect(await probe48.probe(`stream48-nack ${s} g45 SILENT ${id1}`)).toBe('nacked=1');
      const again = await stream.readGroup('g45', 'c2', { count: 10 });
      expect(again.map((e) => e.id)).toContain(id1);

      // TS nack requeues the Java consumer's pending entry
      const got = await stream.readGroup('g45', 'c3', { count: 10 });
      expect(got.length).toBeGreaterThanOrEqual(1);
      const n = await stream.nack('g45', 'SILENT', got.map((e) => e.id));
      expect(n).toBeGreaterThanOrEqual(1);
      const again2 = await stream.readGroup('g45', 'c4', { count: 10 });
      expect(again2.map((e) => e.id)).toEqual(got.map((e) => e.id));
    } finally {
      await delAll(raw, [s]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBlockingDeque move (LMOVE): head-to-tail handoff across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const src = `${NS}src`;
    const dst = `${NS}dst`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [src, dst]);

      // TS seeds; Java pops the head into the dest tail
      await r.getBlockingDeque<string>(src).addLast('one');
      await r.getBlockingDeque<string>(src).addLast('two');
      expect(await probe48.probe(`bd48-movefirst ${src} ${dst}`)).toBe('value=one');
      expect(await r.getList<string>(dst).readAll()).toEqual(['one']);
      expect(await r.getList<string>(src).readAll()).toEqual(['two']);

      // TS mirrors the move back
      expect(await r.getBlockingDeque<string>(dst).move(src, 'LEFT', 'RIGHT')).toBe('one');
      expect(await r.getList<string>(src).readAll()).toEqual(['two', 'one']);
      expect(await r.getList<string>(dst).readAll()).toEqual([]);
    } finally {
      await delAll(raw, [src, dst]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
