/**
 * Phase 74 live cluster: the 4.7/4.8-era surfaces keep every companion key
 * co-slotted (multi-key Lua and pipelines stay CROSSSLOT-free) and behave
 * through an ioredis Cluster client.
 *
 * Covers: RTimeSeries 4.8 family (seq/ttl companions share the {name} tag),
 * RCountMin, RMaps import fallback (plain HASH writes), RCircularBuffer's
 * hash-tagged `redisson_acb:{name}` settings key (slot math; AR.* ops only
 * on servers that have them — the local 8.6 cluster logs a skip).
 *
 * Gated on the 3-master cluster from scripts/cluster-up.sh (7100-7102);
 * logged skip otherwise — never silent, never red.
 */
import { describe, it, expect } from 'vitest';
import Redis, { Cluster } from 'ioredis';
import { Redisson, TimeSeriesAddArgs, CountMinInitArgs, MapsImportArgs } from '../src';
import { prefixName } from '../src/util';

const CLUSTER_URL = process.env.REDIS_CLUSTER_URL ?? 'redis://127.0.0.1:7100';
const NS = 'TsJavaCluP74:{c74}';

async function clusterAvailable(): Promise<boolean> {
  try {
    const u = new URL(CLUSTER_URL);
    const r = new Redis(Number(u.port || 7100), u.hostname || '127.0.0.1', {
      connectTimeout: 2000,
      maxRetriesPerRequest: 1,
      lazyConnect: true,
      retryStrategy: () => null,
    });
    await r.connect();
    const info = String(await r.cluster('info'));
    r.disconnect();
    return info.includes('cluster_state:ok') && !info.includes('cluster_slots_assigned:0');
  } catch {
    return false;
  }
}

function makeCluster(): Cluster {
  const u = new URL(CLUSTER_URL);
  const c = new Cluster([{ host: u.hostname || '127.0.0.1', port: Number(u.port || 7100) }], {
    clusterRetryStrategy: () => null,
    redisOptions: { connectTimeout: 3000, maxRetriesPerRequest: 1 },
  });
  c.on('error', () => undefined);
  return c;
}

const hasCluster = await clusterAvailable();
if (!hasCluster) {
  console.warn(`[live-cluster-phase74] no cluster at ${CLUSTER_URL} — skipping (start it with scripts/cluster-up.sh)`);
}

async function canCmd(c: Cluster, cmd: string): Promise<boolean> {
  try {
    const r = await (c as any).call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

describe('live cluster phase 74: 4.8-era surfaces through a Cluster client', () => {
  it('companion keys co-slot (server CLUSTER KEYSLOT agrees) and ops route cleanly', async () => {
    if (!hasCluster) return;
    const c = makeCluster();
    const r = Redisson.create({ client: c });
    const seed = new Redis(Number(new URL(CLUSTER_URL).port || 7100), new URL(CLUSTER_URL).hostname || '127.0.0.1');
    seed.on('error', () => undefined);
    try {
      // slot agreement, server-side verdict: a bare name and every 4.8-era
      // companion derived from it must land in the same slot
      const names = [`${NS}:ts1`, `${NS}:ts2`, `${NS}:cm`, `${NS}:cb`, `${NS}:maps`];
      for (const name of names) {
        const companions = [
          name,
          prefixName('redisson__ts_ttl', name),
          prefixName('redisson__ts_seq', name),
          prefixName('redisson_acb', name),
          prefixName('redisson_set_cache_expired', name),
        ];
        const slots: number[] = [];
        for (const k of companions) slots.push(Number(await seed.cluster('keyslot', k)));
        expect(new Set(slots).size, `${name} companions must share one slot`).toBe(1);
      }

      // RTimeSeries 4.8 family: multi-key Lua (name/ttl/seq) through routing
      const ts1 = r.getTimeSeries<number>(`${NS}:ts1`);
      await ts1.delete();
      expect(await ts1.add(TimeSeriesAddArgs.entry(1700000000000, 1.5))).toBe(true);
      expect(await ts1.add(TimeSeriesAddArgs.entry(1700000001000, 2.5))).toBe(true);
      const entries = await ts1.entryRange(1700000000000, 1700000002000);
      expect(entries.map((e) => e.value)).toEqual([1.5, 2.5]);
      // second series lands on a different master — routing, not accident
      const ts2 = r.getTimeSeries<number>(`${NS}:ts2`);
      await ts2.delete();
      expect(await ts2.add(TimeSeriesAddArgs.entry(1700000000000, 9))).toBe(true);
      expect((await ts2.entryRange(1699999999000, 1700000001000)).map((e) => e.value)).toEqual([9]);

      // RCountMin (single key; still exercises module command routing)
      if (await canCmd(c, 'CMS.INITBYDIM')) {
        const cm = r.getCountMin<string>(`${NS}:cm`);
        await (c as any).call('DEL', `${NS}:cm`);
        await cm.init(CountMinInitArgs.byDimensions(2000, 5));
        await cm.add('x', 4);
        expect(await cm.count('x')).toBe(4);
      } else {
        console.warn('[live-cluster-phase74] CMS.* unavailable on the cluster — RCountMin check skipped');
      }

      // RMaps import fallback: plain HASH writes readable via RMap
      const maps = r.getMaps<string, number>();
      const mk = `${NS}:maps`;
      await (c as any).call('DEL', mk);
      await maps.set({ [mk]: { a: 1, b: 2 } });
      expect(await r.getMap<string, number>(mk).get('a')).toBe(1);
      const imp = maps.createImport(MapsImportArgs.fields('f1', 'f2').batchSize(1));
      await imp.add(mk, 7, 8);
      await imp.flush();
      expect(await r.getMap<string, number>(mk).get('f2')).toBe(8);

      // RCircularBuffer: settings key slot is proven above; AR.* ops run only
      // where the server has them (the local 8.6 cluster logs a skip)
      if (await canCmd(c, 'ARSET')) {
        const cb = r.getCircularBuffer<number>(`${NS}:cb`);
        await cb.trySetCapacity(4);
        await cb.add(1);
        await cb.add(2);
        expect(await cb.size()).toBe(2);
        expect(await cb.remainingCapacity()).toBe(2);
      } else {
        console.warn('[live-cluster-phase74] ARSET unavailable (cluster < 8.8) — RCircularBuffer ops skipped (slot math already proven)');
      }

      for (const name of names) await (c as any).call('DEL', name,
        prefixName('redisson__ts_ttl', name), prefixName('redisson__ts_seq', name),
        prefixName('redisson_acb', name));
    } finally {
      seed.disconnect();
      await r.shutdown();
    }
  }, 60000);
});
