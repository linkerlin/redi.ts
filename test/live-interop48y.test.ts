/**
 * Live interop vs **Java Redisson 4.8.0**, round 25 — the execution layer,
 * the last surfaces without 4.8.0-jar evidence:
 *
 *  - RBatch: pipeline of codec-encoded writes crosses languages both ways.
 *    Java's batch facades expose only the `*Async` methods (RFutures settle
 *    on execute()); TS resolves per-op Deferreds the same way.
 *  - RScript: Java eval encodes ARGV through the codec (`"v"` with quotes),
 *    TS eval passes values raw — so a shared script only interops when the
 *    TS side pre-encodes or reads via the codec. VALUE replies: Java decodes
 *    through the codec, TS returns the raw bytes (documented passthrough).
 *  - RBuckets: MSET/MGET of codec values both directions.
 *
 * Script bodies are space-free constants duplicated byte-identically in the
 * probe (probe args split on spaces — AGENTS law 11).
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48y:';

const PUT_SCRIPT = "redis.call('set',KEYS[1],ARGV[1])return 1";
const GET_SCRIPT = "return redis.call('get',KEYS[1])";

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
    console.warn(`[live interop48y] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

describe('live interop48y: batch / script / buckets execution layer vs Java 4.8.0', () => {
  it('RBatch: TS pipeline writes read by Java; Java batch writes read by TS', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const tsB = `${NS}tsb-bucket`, tsM = `${NS}tsb-map`, tsL = `${NS}tsb-list`, tsA = `${NS}tsb-atomic`;
    const jB = `${NS}jav-bucket`, jM = `${NS}jav-map`, jL = `${NS}jav-list`, jA = `${NS}jav-atomic`;
    const all = [tsB, tsM, tsL, tsA, jB, jM, jL, jA];
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, all);

      // TS queues four codec-encoded writes, then executes
      const b = r.createBatch();
      b.getBucket<string>(tsB).set('ts-batch-v');
      b.getMap<string, string>(tsM).put('tf', 'ts-batch-fv');
      const listAddP = b.getList<string>(tsL).add('ts-li1', 'ts-li2');
      const incrP = b.getAtomicLong(tsA).incrementAndGet();
      await b.execute();
      // per-op Deferreds settle with decoded values (Redisson RFuture style)
      expect(await incrP).toBe(1);
      expect(await listAddP).toBe(2);

      // Java reads every write back
      expect(await probe48.probe(`bkt48-get ${tsB}`)).toBe('json="ts-batch-v"');
      expect(await probe48.probe(`map48-get ${tsM} tf`)).toBe('value=ts-batch-fv');
      expect(await probe48.probeLines(`list48-range ${tsL}`)).toEqual(['item=ts-li1', 'item=ts-li2']);
      expect(await probe48.probe(`atomic48-get ${tsA}`)).toBe('value=1');

      // Java batches the same four shapes; TS reads them back
      expect(await probe48.probe(`batch48-run ${jB} ${jM} ${jL} ${jA}`)).toBe('responses=4');
      expect(await r.getBucket<string>(jB).get()).toBe('java-batch-v');
      expect(await r.getMap<string, string>(jM).get('jf')).toBe('java-batch-fv');
      expect(await r.getList<string>(jL).readAll()).toEqual(['java-li1', 'java-li2']);
      expect(await r.getAtomicLong(jA).get()).toBe(1);
    } finally {
      await delAll(raw, all);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RScript: codec ARGV one way, VALUE decode back, sha shared both ways', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k1 = `${NS}scr-jts`, k2 = `${NS}scr-stj`, k3 = `${NS}scr-sha`, k4 = `${NS}scr-sha2`;
    const all = [k1, k2, k3, k4];
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, all);

      // a. Java eval: ARGV is codec-encoded ("..." with quotes); TS bucket
      //    get decodes the key it SETs
      expect(await probe48.probe(`script48-put ${k1} java-script-v`)).toBe('value=1');
      expect(await r.getBucket<string>(k1).get()).toBe('java-script-v');

      // b. TS writes codec bytes via the bucket; Java's script RETURNs the
      //    GET through ReturnType.VALUE — decoded through the codec
      await r.getBucket<string>(k2).set('ts-for-java');
      expect(await probe48.probe(`script48-get ${k2}`)).toBe('value=ts-for-java');

      // c. TS loads the put-script; Java scriptExists + evalsha executes the
      //    TS-loaded body with Java codec ARGV
      const sha = await r.getScript().scriptLoad(PUT_SCRIPT);
      expect(await probe48.probe(`script48-exists ${sha}`)).toBe('exists=true');
      expect(await probe48.probe(`script48-evalsha ${sha} ${k3} java-sha-v`)).toBe('value=1');
      expect(await r.getBucket<string>(k3).get()).toBe('java-sha-v');

      // d. Java loads the get-script; TS evalSha runs it. TS RScript returns
      //    the RAW reply (passthrough convention) — decode to compare.
      const jsha = (await probe48.probe('script48-load')).slice('sha='.length);
      expect(await r.getScript().scriptExists(jsha)).toEqual([true]);
      await r.getBucket<string>(k4).set('ts-sha-read');
      const reply = await r.getScript().evalSha(jsha, 'VALUE', [k4]);
      expect(JSON.parse(reply)).toBe('ts-sha-read');
    } finally {
      await delAll(raw, all);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RBuckets: multi-key MSET/MGET both directions', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const t1 = `${NS}bkt-ts1`, t2 = `${NS}bkt-ts2`;
    const j1 = `${NS}bkt-j1`, j2 = `${NS}bkt-j2`;
    const all = [t1, t2, j1, j2];
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, all);

      // TS MSETs two; Java gets both (probe prints sorted k=v lines)
      await r.getBuckets().set({ [t1]: 'ts-bkt1', [t2]: 'ts-bkt2' });
      expect(await probe48.probeLines(`buckets48-get ${t1} ${t2}`)).toEqual([
        `${t1}=ts-bkt1`,
        `${t2}=ts-bkt2`,
      ]);

      // Java sets two; TS MGETs and decodes both
      expect(await probe48.probe(`buckets48-set ${j1} java-b1 ${j2} java-b2`)).toBe('OK');
      const got = await r.getBuckets().get<string>(j1, j2);
      expect(got.get(j1)).toBe('java-b1');
      expect(got.get(j2)).toBe('java-b2');
    } finally {
      await delAll(raw, all);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
