/**
 * Phase 69 live: upstream Redisson 4.8 OSS surface — RCountMin (CMS),
 * RSet countDiff/countUnionApprox (SDIFFCARD / SUNIONCARD APPROX),
 * RQueue/RBlockingQueue move (LMOVEM / BLMOVEM). All command-gated.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, CountMinInitArgs, QueueMoveElementsArgs } from '../src';

const NS = 'TsJavaT:p69:';
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
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await (raw as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

describe('live phase 69: upstream 4.8 OSS surface', () => {
  it('RCountMin full CMS surface', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    if (!(await canCmd(raw, 'CMS.INITBYDIM'))) {
      console.warn('[live phase69] CMS.* unavailable — RCountMin check skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const cm = r.getCountMin<string>(`${NS}cm`);
    const cm2 = r.getCountMin<string>(`${NS}cm2`);
    const cmp = r.getCountMin<string>(`${NS}cmp`);
    await cm.delete();
    await cm2.delete();
    await cmp.delete();
    // CMS.MERGE requires equal width/depth, so the merge pair shares dims;
    // the probability initializer is covered on its own sketch.
    await cm.init(CountMinInitArgs.byDimensions(1000, 5));
    await cm2.init(CountMinInitArgs.byDimensions(1000, 5));
    await cmp.init(CountMinInitArgs.byProbability(0.01, 0.99));
    expect(await cm.add('a')).toBe(1);
    expect(await cm.add('a', 4)).toBe(5);
    const batch = await cm.add(new Map([['a', 1], ['b', 2]]));
    expect(batch.get('a')).toBe(6);
    expect(batch.get('b')).toBe(2);
    expect(await cm.count('a')).toBe(6);
    const counts = await cm.count(['a', 'b']);
    expect(counts.get('b')).toBe(2);
    await cm2.add('a', 3);
    // CMS.MERGE overwrites the destination with the merged SOURCES (the
    // destination's own counts are not included unless listed as a source) —
    // same overwrite semantics as TDIGEST.MERGE.
    await cm.mergeWith({ keys: [`${NS}cm2`] });
    expect(await cm.count('a')).toBe(3);
    const info = await cm.getInfo();
    expect(Number(info.width)).toBeGreaterThan(0);
    expect(Number(info.depth)).toBeGreaterThan(0);
    await r.getKeys().unlink(`${NS}cm`, `${NS}cm2`, `${NS}cmp`);
    raw.disconnect();
    await r.shutdown();
  });

  it('RSet countDiff / countUnionApprox and queue LMOVEM move', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const s1 = r.getSet<string>(`${NS}s1`);
    const s2 = r.getSet<string>(`${NS}s2`);
    await s1.delete();
    await s2.delete();
    await s1.addAll(['a', 'b', 'c']);
    await s2.addAll(['b', 'c', 'd']);
    if (await canCmd(raw, 'SDIFFCARD')) {
      expect(await s1.countDiff(`${NS}s2`)).toBe(1); // {a}
      expect(await s1.countDiff(1, `${NS}s2`)).toBe(1);
    } else {
      console.warn('[live phase69] SDIFFCARD unavailable — countDiff check skipped');
    }
    if (await canCmd(raw, 'SUNIONCARD')) {
      expect(await s1.countUnionApprox(`${NS}s2`)).toBe(4); // a,b,c,d
    } else {
      console.warn('[live phase69] SUNIONCARD unavailable — countUnionApprox check skipped');
    }
    const q = r.getQueue<number>(`${NS}q`);
    const dest = r.getQueue<number>(`${NS}dest`);
    await q.delete();
    await dest.delete();
    if (await canCmd(raw, 'LMOVEM')) {
      await q.addAll([1, 2, 3, 4]);
      const moved = await q.move(QueueMoveElementsArgs.to(`${NS}dest`).count(2).bulk());
      expect(moved).toEqual([1, 2]);
      expect(await q.readAll()).toEqual([3, 4]);
      expect(await dest.readAll()).toEqual([1, 2]);
      const single = await q.move(QueueMoveElementsArgs.to(`${NS}dest`) as any);
      expect(single).toEqual([3]);
      const bq = r.getBlockingQueue<number>(`${NS}q`);
      const bm = await bq.move(0, QueueMoveElementsArgs.to(`${NS}dest`).exactly(1).bulk());
      expect(bm).toEqual([4]);
    } else {
      console.warn('[live phase69] LMOVEM unavailable — move check skipped');
    }
    await r.getKeys().unlink(`${NS}s1`, `${NS}s2`, `${NS}q`, `${NS}dest`);
    raw.disconnect();
    await r.shutdown();
  });
});
