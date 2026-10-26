/**
 * Live interop vs **Java Redisson 4.8.0**, round 48 — audit-ledger batch 2:
 *
 *  - RScoredSortedSet: pollFirst/LastEntriesFromAny — the (count, names...)
 *    overload is ZMPOP (non-blocking), the (Duration, count, names...)
 *    overload is BZMPOP; both directions decode the same [key, [member,
 *    score]...] reply. takeFirst/takeLast (BZPOPMIN/MAX forever) woken
 *    cross-language, and subscribeOnFirstElements (the same
 *    ElementsSubscribeService the blocking queue uses, round 41).
 *  - Multimap core ops (RMultimapBase): getAll/putAll/replaceValues/
 *    removeAll/entries/values/containsEntry/containsValue/keySize/
 *    readAllKeySet/fastRemove/fastRemoveValue over the same HASH→id→
 *    collection scheme.
 *  - RBloomFilter: getExpectedInsertions/getFalseProbability read the same
 *    `{name}:config` hash both languages write.
 *  - RBloomFilterNative: add(Collection) (BF.INSERT), insert(args),
 *    exists(Collection) (BF.MEXISTS), getInfo (BF.INFO), and byte-identical
 *    SCANDUMP/LOADCHUNK round-trips both ways.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48av:';

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
    console.warn(`[live interop48av] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48av: audit-ledger batch 2 (bzmpop/multimap/bloom) vs Java 4.8.0', () => {
  it('RScoredSortedSet ZMPOP/BZMPOP entries family across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const za = `${NS}za`;
    const zb = `${NS}zb`;
    const zc = `${NS}zc`;
    const zd = `${NS}zd`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [za, zb, zc, zd]);
      const zs = r.getScoredSortedSet<string>(za);
      await zs.add(1, 'a1');
      await zs.add(2, 'a2');
      await r.getScoredSortedSet<string>(zb).add(5, 'b1');

      // Java ZMPOP (count, names) pops two lowest from the TS data
      expect(await probe48.probeLines(`zset48-zmpop MIN 2 ${za},${zb}`)).toEqual([`${za}|a1@1.0`, `${za}|a2@2.0`]);
      // TS non-blocking ZMPOP over Java's leftovers
      const m = await zs.pollFirstEntriesFromAny(2, zb);
      expect(m.get(zb)).toEqual(new Map([['b1', 5]]));
      expect(await zs.size()).toBe(0);

      // TS ZMPOP MAX over Java-written members
      await zs.add(3, 'j3');
      await zs.add(4, 'j4');
      const m2 = await zs.pollLastEntriesFromAny(0, 1, zb);
      expect(m2.get(za)).toEqual(new Map([['j4', 4]]));
      // drain the leftover j3 so the BZMPOP below can only be woken from zc
      expect((await zs.pollFirstEntriesFromAny(1)).get(za)).toEqual(new Map([['j3', 3]]));

      // blocking entries: TS parks on BZMPOP, Java zadd wakes it
      const pending = zs.pollFirstEntriesFromAny(60000, 1, zc);
      await new Promise((res) => setTimeout(res, 300));
      await r.getScoredSortedSet<string>(zc).add(7, 'wake1');
      const got = await pending;
      expect(got.get(zc)).toEqual(new Map([['wake1', 7]]));

      // reverse: Java BZMPOP parks 2s, TS zadd delivers into it
      const jpop = probe48.probeLines(`zset48-bzmpop MIN 2 1 ${zd}`);
      await new Promise((res) => setTimeout(res, 400));
      await r.getScoredSortedSet<string>(zd).add(9, 'ts-wake');
      expect(await jpop).toEqual([`${zd}|ts-wake@9.0`]);
    } finally {
      await delAll(raw, [za, zb, zc, zd]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('zset takeFirst blocks across languages + subscribeOnFirstElements delivers', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const z = `${NS}take`;
    const z2 = `${NS}sub`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [z, z2]);
      const zs = r.getScoredSortedSet<string>(z);

      // TS takeFirst parks; Java zadd wakes it
      const pending = zs.takeFirst();
      await new Promise((res) => setTimeout(res, 300));
      await probe48.probe(`zset48-add ${z} 11 java-one`);
      expect(await pending).toBe('java-one');

      // Java bg pollFirst parks on the empty set; TS zadd wakes it
      expect(await probe48.probe(`zset48-bgpoll ${z}`)).toBe('armed');
      await zs.add(12, 'ts-two');
      expect(await probe48.probe(`zset48-bgresult 5000`)).toBe('value=ts-two');

      // TS subscribeOnFirstElements: continuous delivery of Java writes
      const zs2 = r.getScoredSortedSet<string>(z2);
      const got: string[] = [];
      const id = zs2.subscribeOnFirstElements((v) => got.push(v as string));
      await probe48.probe(`zset48-add ${z2} 21 s-one`);
      await probe48.probe(`zset48-add ${z2} 22 s-two`);
      await new Promise((res) => setTimeout(res, 600));
      zs2.unsubscribe(id);
      expect(got).toEqual(['s-one', 's-two']);
    } finally {
      await delAll(raw, [z, z2]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('multimap core ops: getAll/replaceValues/removeAll/entries/fastRemove across languages', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const mm = `${NS}mm`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [mm]);
      const lm = r.getListMultimap<string, string>(mm);

      // TS putAll → Java getAll; Java putAll → TS get
      expect(await lm.putAll('k1', ['v1', 'v2'])).toBe(true);
      expect(await probe48.probe(`mm48-getalllist ${mm} k1`)).toBe('values=v1,v2');
      await probe48.probe(`mm48-putall ${mm} k2 jv1,jv2`);
      expect(await lm.get('k2')).toEqual(['jv1', 'jv2']);
      expect(await lm.keySize()).toBe(2);
      expect(await probe48.probe(`mm48-keysize ${mm}`)).toBe('keys=2');

      // replaceValues returns the PREVIOUS collection, both directions
      expect(await lm.replaceValues('k1', ['n1'])).toEqual(['v1', 'v2']);
      expect(await probe48.probe(`mm48-getalllist ${mm} k1`)).toBe('values=n1');
      expect(await probe48.probe(`mm48-replacevalues ${mm} k2 jv3`)).toBe('prev=jv1,jv2');
      expect(await lm.getAll('k2')).toEqual(['jv3']);

      // removeAll returns the removed collection; key gone after
      expect(await lm.removeAll('k1')).toEqual(['n1']);
      expect(await probe48.probe(`mm48-keysize ${mm}`)).toBe('keys=1');
      expect(await probe48.probe(`mm48-removeall ${mm} k2`)).toBe('prev=jv3');
      expect(await lm.keySize()).toBe(0);
      expect(await lm.isEmpty()).toBe(true);

      // entries/values/contains across the boundary
      await lm.putAll('e1', ['x1', 'x2']);
      await probe48.probe(`mm48-putall ${mm} e2 y1`);
      expect((await lm.entries()).sort()).toEqual([['e1', 'x1'], ['e1', 'x2'], ['e2', 'y1']]);
      expect((await lm.values()).sort()).toEqual(['x1', 'x2', 'y1']);
      expect(await lm.containsEntry('e1', 'x1')).toBe(true);
      expect(await lm.containsEntry('e1', 'nope')).toBe(false);
      expect(await lm.containsValue('y1')).toBe(true);
      expect(await lm.containsValue('nope')).toBe(false);
      expect(await probe48.probe(`mm48-containsentry ${mm} e1 x1`)).toBe('ok=true');
      expect(await probe48.probe(`mm48-readallkeyset ${mm}`)).toBe('keys=e1,e2');

      // fastRemove drops whole keys; emptied key set visible to Java
      expect(await lm.fastRemove('e1', 'e2', 'ghost')).toBe(2);
      expect(await probe48.probe(`mm48-readallkeyset ${mm}`)).toBe('keys=');
      expect(await lm.readAllKeySet()).toEqual([]);
    } finally {
      await delAll(raw, [mm]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('bloom filter config getters read the same {name}:config hash both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b1 = `${NS}bf1`;
    const b2 = `${NS}bf2`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [b1, `{${b1}}:config`, b2, `{${b2}}:config`]);
      const bf = r.getBloomFilter<string>(b1);
      expect(await bf.tryInit(1000, 0.01)).toBe(true);

      // Java reads the TS-written config
      expect(await probe48.probe(`bf48-cfg ${b1}`)).toBe(
        `exp=1000 fp=0.01 size=${await bf.getSize()} iters=${await bf.getHashIterations()}`);
      expect(await bf.getExpectedInsertions()).toBe(1000);
      expect(await bf.getFalseProbability()).toBe(0.01);

      // Java tryInit writes its own config; TS getters read it back
      await probe48.probe(`bf48-init ${b2} 2000 0.02`);
      const bf2 = r.getBloomFilter<string>(b2);
      expect(await bf2.getExpectedInsertions()).toBe(2000);
      expect(await bf2.getFalseProbability()).toBe(0.02);
    } finally {
      await delAll(raw, [b1, `{${b1}}:config`, b2, `{${b2}}:config`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('BF-native insert/info/exists + byte-identical SCANDUMP round-trips', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const b = `${NS}bfn`;
    const b2 = `${NS}bfn2`;
    const b3 = `${NS}bfn3`;
    const b4 = `${NS}bfn4`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [b, b2, b3, b4]);
      const bfn = r.getBloomFilterNative<string>(b);
      await bfn.init(0.01, 100);
      expect(await bfn.add('x1')).toBe(true);

      // Java sees TS's write; Java insert lands for TS
      expect(await probe48.probe(`bfn48-existsmany ${b} x1,x2`)).toBe('exists=x1');
      expect(await probe48.probe(`bfn48-insert ${b} 0 0 x9`)).toBe('added=x9');
      expect(await bfn.exists('x9')).toBe(true);

      // getInfo shape over the TS-reserved filter
      const info = await bfn.getInfo();
      expect(info.capacity).toBeGreaterThanOrEqual(100);
      expect(info.subFilterCount).toBeGreaterThanOrEqual(1);
      expect(await probe48.probe(`bfn48-info ${b}`)).toBe(
        `info=${info.capacity},${info.size},${info.subFilterCount},${info.itemCount},${info.expansionRate}`);

      // SCANDUMP chunks are byte-identical in both directions
      const tsChunks = await dumpChunks(bfn);
      const jLines = await probe48.probeLines(`bfn48-scandump ${b}`);
      expect(jLines[jLines.length - 1]).toBe('dumpend');
      const jChunks = jLines.slice(0, -1).map((l) => {
        const q = l.slice('chunk='.length).split(' ');
        return { iterator: Number(q[0]), hex: q[1] };
      });
      expect(jChunks.map((c) => [c.iterator, c.hex])).toEqual(tsChunks.map((c) => [c.iterator, c.data!.toString('hex')]));

      // Java dumps → TS rebuilds via loadChunk; membership survives
      const bfn2 = r.getBloomFilterNative<string>(b2);
      for (const c of jChunks) await bfn2.loadChunk(c.iterator, Buffer.from(c.hex, 'hex'));
      expect(await bfn2.exists(['x1', 'x9'])).toEqual(['x1', 'x9']);

      // TS dumps → Java rebuilds; membership survives
      const bfn3 = r.getBloomFilterNative<string>(b3);
      await bfn3.init(0.01, 100);
      await bfn3.add('t1');
      for (const c of await dumpChunks(bfn3)) {
        await probe48.probe(`bfn48-loadchunk ${b4} ${c.iterator} ${c.data!.toString('hex')}`);
      }
      expect(await probe48.probe(`bfn48-existsmany ${b4} t1`)).toBe('exists=t1');
    } finally {
      await delAll(raw, [b, b2, b3, b4]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});

/** All SCANDUMP (iterator, data) chunks of a filter, in iterator order. */
async function dumpChunks(f: { scanDump: (i: number) => Promise<{ iterator: number; data: Buffer | null }>; }): Promise<{ iterator: number; data: Buffer }[]> {
  const chunks: { iterator: number; data: Buffer }[] = [];
  let it = 0;
  for (;;) {
    const c = await f.scanDump(it);
    if (c.iterator === 0) break;
    it = c.iterator;
    chunks.push({ iterator: c.iterator, data: c.data! });
  }
  return chunks;
}
