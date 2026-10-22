/**
 * Phase 46 live: RKeys migrate/remote copy/MigrateArgs (needs a second Redis,
 * default 127.0.0.1:6380 or REDIS_MIGRATE_URL), swapdb and parallel flushes.
 * The migration half is capability-gated; DB-local operations always run.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';

const NS = 'TsJavaT:p46:';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const TARGET_URL = process.env.REDIS_MIGRATE_URL ?? 'redis://127.0.0.1:6380';

async function up(url: string): Promise<boolean> {
  try {
    const r = new Redis(url, { lazyConnect: true, connectTimeout: 1000, maxRetriesPerRequest: 1 });
    r.on('error', () => undefined);
    await r.ping();
    r.disconnect();
    return true;
  } catch {
    return false;
  }
}

const hasRedis = await up(REDIS_URL);
if (!hasRedis) console.warn(`[live] Redis at ${REDIS_URL} unreachable - live cases in this file no-op`);
const hasTarget = hasRedis ? await up(TARGET_URL) : false;

describe('live phase 46: RKeys migrate/copy/swapdb/parallel flush', () => {
  it('migrates single and multiple keys, remote copy, swapdb and async flushes', async () => {
    if (!hasRedis) return;
    const r = Redisson.create({ address: REDIS_URL });
    const extra: Redis[] = [];
    try {
      if (!hasTarget) {
        console.log('[phase46] no second Redis at ' + TARGET_URL + ' — migration assertions skipped');
      } else {
        const target = new Redis(TARGET_URL);
        extra.push(target);
        const rt = Redisson.create({ address: TARGET_URL });
        try {
          await r.getKeys().delete(`${NS}m1`, `${NS}c1`, `${NS}k1`, `${NS}k2`, `${NS}k3`);
          await rt.getKeys().delete(`${NS}m1`, `${NS}c1`, `${NS}k1`, `${NS}k2`, `${NS}k3`, `${NS}flushall`);
          const host = new URL(TARGET_URL).hostname;
          const port = Number(new URL(TARGET_URL).port || 6379);

          await r.getBucket(`${NS}m1`).set('migrated');
          await r.getKeys().migrate(`${NS}m1`, host, port, 0, 5000);
          expect(await r.getKeys().countExists(`${NS}m1`)).toBe(0);
          expect(await rt.getBucket<any>(`${NS}m1`).get()).toBe('migrated');

          await r.getBucket(`${NS}c1`).set('copied');
          await r.getKeys().copy(`${NS}c1`, host, port, 0, 5000);
          expect(await r.getKeys().countExists(`${NS}c1`)).toBe(1);
          expect(await rt.getBucket<any>(`${NS}c1`).get()).toBe('copied');

          await r.getBucket(`${NS}k1`).set('a');
          await r.getBucket(`${NS}k2`).set('b');
          await r.getKeys().migrate({
            keys: [`${NS}k1`, `${NS}k2`],
            host,
            port,
            database: 0,
            timeoutMs: 5000,
          });
          expect(await r.getKeys().countExists(`${NS}k1`, `${NS}k2`)).toBe(0);
          expect(await rt.getBucket<any>(`${NS}k1`).get()).toBe('a');
          expect(await rt.getBucket<any>(`${NS}k2`).get()).toBe('b');

          await r.getBucket(`${NS}k3`).set('new');
          await rt.getBucket(`${NS}k3`).set('old');
          await r.getKeys().migrate({
            keys: [`${NS}k3`],
            host,
            port,
            database: 0,
            timeoutMs: 5000,
            copy: true,
            replace: true,
          });
          expect(await r.getBucket<any>(`${NS}k3`).get()).toBe('new');
          expect(await rt.getBucket<any>(`${NS}k3`).get()).toBe('new');

          await rt.getBucket(`${NS}flushall`).set('x');
          await rt.getKeys().flushallParallel();
          expect(await rt.getKeys().count()).toBe(0);
        } finally {
          await rt.getKeys().delete(`${NS}m1`, `${NS}c1`, `${NS}k1`, `${NS}k2`, `${NS}k3`).catch(() => undefined);
          await rt.shutdown();
        }
      }

      // SWAPDB exchanges whole logical databases.
      const db10 = new Redis(REDIS_URL, { db: 10 });
      const db11 = new Redis(REDIS_URL, { db: 11 });
      extra.push(db10, db11);
      await db10.set(`${NS}swap`, 'ten');
      await db11.set(`${NS}swap`, 'eleven');
      await r.getKeys().swapdb(10, 11);
      expect(await db10.get(`${NS}swap`)).toBe('eleven');
      expect(await db11.get(`${NS}swap`)).toBe('ten');

      // FLUSHDB ASYNC only touches the selected database.
      const db12 = new Redis(REDIS_URL, { db: 12 });
      extra.push(db12);
      const r12 = Redisson.create({ client: db12 as any });
      await r12.getBucket(`${NS}flushdb`).set('x');
      await r12.getKeys().flushdbParallel();
      expect(await r12.getKeys().count()).toBe(0);
      expect(await r.getKeys().countExists(`${NS}keep`)).toBe(0);
      await r.getBucket(`${NS}keep`).set('still');
      expect(await r.getKeys().countExists(`${NS}keep`)).toBe(1);
    } finally {
      await r.getKeys().delete(`${NS}keep`).catch(() => undefined);
      for (const c of extra) {
        try { await c.del(`${NS}swap`, `${NS}flushdb`); } catch { /* noop */ }
        c.disconnect();
      }
      await r.shutdown();
    }
  }, 120000);
});
