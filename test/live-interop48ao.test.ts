/**
 * Live interop vs **Java Redisson 4.8.0**, round 41 — two surfaces the
 * earlier rounds did not cover:
 *
 *  - RSearch synonyms: Java `updateSynonyms(index, groupId, terms...)`
 *    writes groups TS `dumpSynonyms` reads term→groups; TS updates →
 *    Java dumps. Shared index wire (FT.SYNUPDATE/FT.SYNDUMP).
 *  - RBlockingQueue `subscribeOnElements`: CONTINUOUS consumption — a
 *    Java background subscriber (take() loop) drains every element TS
 *    offers, one at a time in order; a TS subscriber drains Java's
 *    offers the same way; `unsubscribe` stops the pump.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48ao:';

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
    console.warn(`[live interop48ao] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48ao: search synonyms / queue continuous subscription vs Java 4.8.0', () => {
  it('RSearch synonyms: synonym groups shared across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const idx = `${NS}idx`;
    const raw = new Redis(REDIS_URL);
    try {
      // Java updates synonyms on an existing index → TS dumps term→groups
      expect(await probe48.probe(`search48-create ${idx} ${NS}doc1:`)).toBe('create=ok');
      expect(await probe48.probe(`fts48-synupdate ${idx} grp1 cat dog`)).toBe('syn=ok');
      const dumped = await r.getSearch().dumpSynonyms(idx);
      expect([...dumped.get('cat')!]).toEqual(['grp1']);
      expect([...dumped.get('dog')!]).toEqual(['grp1']);

      // TS updates another group → Java dumps it back
      await r.getSearch().updateSynonyms(idx, 'grp2', 'dog', 'puppy');
      const lines = await probe48.probeLines(`fts48-syndump ${idx}`);
      expect(lines).toContain('term=cat groups=[grp1]');
      expect(lines).toContain('term=dog groups=[grp1, grp2]');
      expect(lines).toContain('term=puppy groups=[grp2]');
    } finally {
      await raw.call('FT.DROPINDEX', idx, 'DD').catch(() => undefined);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBlockingQueue subscribeOnElements: continuous cross-language drain', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const q1 = `${NS}q-jsub`; // Java subscriber drains TS offers
    const q2 = `${NS}q-tsub`; // TS subscriber drains Java offers
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [q1, q2]);

      // Java background subscriber takes continuously
      expect(await probe48.probe(`bqs48-subscribe ${q1}`)).toMatch(/^subscribed=/);
      await sleep(300);
      for (const v of ['a', 'b', 'c']) await r.getQueue<string>(q1).add(v);
      await sleep(1500);
      expect(await probe48.probe(`bqs48-seen`)).toBe('n=3 items=a,b,c');
      expect(await probe48.probe(`bqs48-unsubscribe ${q1}`)).toBe('unsub=ok');

      // TS subscriber drains Java offers one at a time, in order
      const seen: string[] = [];
      const id = r.getBlockingQueue<string>(q2).subscribeOnElements((v) => seen.push(v));
      await sleep(300);
      for (const v of ['x', 'y', 'z']) await probe48.probe(`bd48-offerlast ${q2} ${v}`);
      await sleep(1500);
      expect(seen).toEqual(['x', 'y', 'z']);
      expect(await raw.llen(q2)).toBe(0);
      r.getBlockingQueue<string>(q2).unsubscribe(id);
    } finally {
      await delAll(raw, [q1, q2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
