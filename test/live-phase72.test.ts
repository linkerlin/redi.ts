/**
 * Phase 72 live: Redisson 4.7 SetExpiredListener (RSetCache) and 4.8
 * NameMapper / PrefixNameMapper + the RGcra config surface
 * (trySetRate/setRate/getConfig/tryAcquire — the plain Redis GCRA command,
 * gated via COMMAND INFO like the TS-dot and BF-dot module commands). Expired-event
 * wire shape follows Java SetCacheEvictionTask verbatim: PUBLISH of
 * struct.pack('Lc0', len, member) (u64LE length prefix + codec-encoded value
 * bytes) on `redisson_set_cache_expired` — asserted here by a raw subscriber,
 * since no Java probe command fits the new surface.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, PrefixNameMapper } from '../src';

const NS = 'TsJavaT:p72:';
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

/** Poll for `pred` until it holds or `ms` elapses (pub/sub delivery latency). */
async function waitFor(ms: number, pred: () => boolean): Promise<boolean> {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (pred()) return true;
    await new Promise((res) => setTimeout(res, 50));
  }
  return pred();
}

describe('live phase 72: SetCache expired listener + NameMapper + Gcra', () => {
  it('SetExpiredListener delivers expired values with the Java wire payload', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}sce`;
    const channel = `redisson_set_cache_expired:{${name}}`;
    await raw.unlink(name);

    // raw subscriber captures the untouched published bytes (wire assertion)
    const rawMsgs: Buffer[] = [];
    (raw as any).on('messageBuffer', (ch: Buffer, payload: Buffer) => {
      if (String(ch) === channel) rawMsgs.push(payload);
    });
    await raw.subscribe(channel);

    const sc = r.getSetCache<string>(name);
    const got: string[] = [];
    const id = await sc.addListener({ onExpired: (v: any) => got.push(v) });
    await sc.add('victim', 300); // 300ms TTL
    await sc.add('survivor', 60000);
    await new Promise((res) => setTimeout(res, 400)); // let the TTL lapse

    const n = await sc.pollExpiry();
    expect(n).toBe(1);
    // delivery through the channel subscription (publish loops back)
    expect(await waitFor(5000, () => got.length === 1)).toBe(true);
    expect(got).toEqual(['victim']);
    // survivor untouched, victim removed
    expect(await sc.contains('survivor')).toBe(true);
    expect(await sc.size()).toBe(1);

    // wire: u64LE length prefix + codec-encoded member (`"victim"` with quotes)
    expect(rawMsgs.length).toBe(1);
    const msg = rawMsgs[0];
    const len = Number(msg.readBigUInt64LE(0));
    expect(len).toBe(msg.length - 8);
    const body = msg.subarray(8).toString('utf8');
    expect(JSON.parse(body)).toBe('victim'); // JsonJacksonCodec member shape

    // removal stops delivery and the poll machinery
    await sc.removeListener(id);
    await sc.add('gone', 200);
    await new Promise((res) => setTimeout(res, 300));
    expect(await sc.pollExpiry()).toBe(1);
    await new Promise((res) => setTimeout(res, 300));
    expect(got).toEqual(['victim']); // no further callbacks

    await sc.delete();
    raw.disconnect();
    await r.shutdown();
  });

  it('PrefixNameMapper maps live structure keys (raw key carries the prefix)', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL, nameMapper: new PrefixNameMapper('p72pre:') });
    const b = r.getBucket<any>(`${NS}nm`);
    await b.set({ x: 1 });
    expect(await raw.exists(`${NS}nm`)).toBe(0);
    expect(await raw.exists(`p72pre:${NS}nm`)).toBe(1);
    expect(await b.get()).toEqual({ x: 1 });
    // unmap round-trip on the same mapper instance
    const nm = new PrefixNameMapper('p72pre:');
    expect(nm.unmap(nm.map(`${NS}nm`))).toBe(`${NS}nm`);
    await raw.unlink(`p72pre:${NS}nm`);
    raw.disconnect();
    await r.shutdown();
  });

  it('RGcra trySetRate/setRate/getConfig/tryAcquire manage {name}:config', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL);
    raw.on('error', () => undefined);
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}gcra`;
    await raw.unlink(name, `{${name}}:config`);
    if (!(await canCmd(raw, 'GCRA'))) {
      console.warn('[live phase72] GCRA command unavailable — RGcra checks skipped');
      raw.disconnect();
      await r.shutdown();
      return;
    }
    const g = r.getGcra(name);
    expect(await g.trySetRate(2, 10, 60000)).toBe(true);
    expect(await g.trySetRate(9, 9, 90000)).toBe(false); // already configured
    expect(await g.getConfig()).toEqual({ maxBurst: 2, tokensPerPeriod: 10, periodMs: 60000 });
    // raw hash shape (Java StringCodec fields)
    expect(await raw.hgetall(`{${name}}:config`)).toEqual({
      maxBurst: '2', rate: '10', period: '60',
    });

    const first = await g.tryAcquire();
    expect(first.limited).toBe(false);
    // burst exhausted -> limited with retry hints
    let limited: any = null;
    for (let i = 0; i < 10; i++) {
      const res = await g.tryAcquire();
      if (res.limited) { limited = res; break; }
    }
    expect(limited).not.toBeNull();
    expect(limited.retryAfterSeconds).toBeGreaterThan(0);

    // setRate resets state: full burst available again
    await g.setRate(5, 100, 1000);
    expect(await g.getConfig()).toEqual({ maxBurst: 5, tokensPerPeriod: 100, periodMs: 1000 });
    expect((await g.tryAcquire()).limited).toBe(false);
    // tokens > 1 rides the TOKENS argument
    const two = await g.tryAcquire(2);
    expect(two.maxTokens).toBeGreaterThan(0);
    // unset rate rejects (Java Lua assert message)
    await g.delete();
    await expect(g.tryAcquire()).rejects.toThrow('GCRA rate is not set');
    expect(await g.getConfig()).toBeNull();
    await raw.unlink(name, `{${name}}:config`);
    raw.disconnect();
    await r.shutdown();
  });
});
