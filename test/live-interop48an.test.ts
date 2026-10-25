/**
 * Live interop vs **Java Redisson 4.8.0**, round 40 — RMapCache ENTRY
 * EVENTS over the `redisson_map_cache_{created,updated,removed,expired}:{name}`
 * topics: the last untouched wire surface in the interop matrix.
 *
 *  - TS publish side verified at BYTE level: a raw subscriber on the three
 *    channels sees u64LE-length-prefixed frames with codec-encoded parts
 *    ([key,value] / [key,value,old] / [key,old]) — the exact framing Java's
 *    own writer produces (struct.pack('Lc0Lc0',...) in its put/remove Lua).
 *  - Java writer → TS listener: full dispatch — TS addListener receives
 *    CREATED/UPDATED/REMOVED with key/value/oldValue decoded.
 *  - DISCOVERY (Java-side quirk): 4.8's RMapCache.addListener registers
 *    NOTHING observable in this standalone-RESP2 setup — NUMSUB/CHANNELS/
 *    SHARDCHANNELS/NUMPAT all zero, and the listener does not receive even
 *    ITS OWN writes (plain RTopic in the same probe self-delivers fine, so
 *    the probe's pubsub is healthy). Also: one addListener registration
 *    serves exactly ONE kind (instanceof first-match — Removed is checked
 *    first) and the bare MapEntryListener is rejected ("Wrong listener
 *    type"). The TS→Java direction is therefore not testable here; the
 *    byte-level framing check above is the evidence for TS's publish side.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48an:';

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
    console.warn(`[live interop48an] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

/** Split u64LE-length-prefixed event frames into their raw payloads. */
function decodeFrames(buf: Buffer): string[] {
  const parts: string[] = [];
  let o = 0;
  while (o + 8 <= buf.length) {
    const len = Number(buf.readBigUInt64LE(o));
    o += 8;
    if (o + len > buf.length) break;
    parts.push(buf.subarray(o, o + len).toString('utf8'));
    o += len;
  }
  return parts;
}

describe('live interop48an: RMapCache entry events across languages vs Java 4.8.0', () => {
  it('TS publishes u64LE-framed events on the redisson_map_cache_* channels', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const m = `${NS}mce-w`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [m]);
      const mc = r.getMapCache<string, string>(m);

      // raw subscriber on the three channels — verify the byte framing
      // directly (u64LE length prefixes, codec-encoded parts)
      const sub = raw.duplicate();
      const seen: Array<{ ch: string; parts: string[] }> = [];
      const done = new Promise<void>((resolve) => {
        sub.on('messageBuffer', (chBuf: Buffer, msgBuf: Buffer) => {
          const ch = chBuf.toString('utf8');
          if (!ch.includes(m)) return;
          // strip the trailing :{name} to get the kind
          seen.push({ ch: ch.slice(0, ch.indexOf(':')), parts: decodeFrames(msgBuf) });
          if (seen.length >= 3) resolve();
        });
      });
      await sub.subscribe(
        `redisson_map_cache_created:{${m}}`,
        `redisson_map_cache_updated:{${m}}`,
        `redisson_map_cache_removed:{${m}}`,
      );
      await sleep(300);

      await mc.put('k1', 'v1');
      await mc.put('k1', 'v2');
      await mc.remove('k1');
      await Promise.race([done, sleep(8000)]);

      expect(seen.map((x) => x.ch)).toEqual([
        'redisson_map_cache_created',
        'redisson_map_cache_updated',
        'redisson_map_cache_removed',
      ]);
      // created/updated: [key, value(, oldValue)]; removed: [key, old]
      expect(seen[0].parts).toEqual(['"k1"', '"v1"']);
      expect(seen[1].parts).toEqual(['"k1"', '"v2"', '"v1"']);
      expect(seen[2].parts).toEqual(['"k1"', '"v2"']);
      sub.disconnect();
    } finally {
      await delAll(raw, [m]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('TS listener receives CREATED/UPDATED/REMOVED from Java writes', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const m = `${NS}mce-t`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [m]);
      const mc = r.getMapCache<string, string>(m);

      const got = new Promise<string[]>((resolve) => {
        const seen: string[] = [];
        mc.addListener({
          onCreated: (e) => { seen.push(`created:${e.key}:${e.value}`); },
          onUpdated: (e) => { seen.push(`updated:${e.key}:${e.value}`); },
          onRemoved: (e) => { seen.push(`removed:${e.key}:${e.oldValue}`); if (seen.length >= 3) resolve(seen); },
        }).then(() => undefined, () => undefined);
      });
      await sleep(300); // subscription settle

      // Java put → UPDATED fires on TS listener
      expect(await probe48.probe(`mc48-putttl ${m} k1 v1 60000`)).toBe('put=ok');
      await sleep(500);
      expect(await probe48.probe(`mc48-putttl ${m} k1 v2 60000`)).toBe('put=ok');
      await sleep(500);
      expect(await probe48.probe(`mc48-remove ${m} k1`)).toBe('removed=true');

      const seen = await Promise.race([got, sleep(10000).then(() => null)]);
      expect(seen).toEqual([
        'created:k1:v1',
        'updated:k1:v2',
        'removed:k1:v2',
      ]);
    } finally {
      await delAll(raw, [m]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
