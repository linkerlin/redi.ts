/**
 * Live interop vs **Java Redisson 4.8.0**, round 4 — RAtomicDouble's args
 * family and RPermitExpirableSemaphore shared permits:
 *
 *  - `RAtomicDouble.incrementAndGet(DoubleIncrementArgs)` — TS Lua-CAS
 *    emulation vs Java's native INCREX (double), both directions.
 *  - `compareAndDelete(CompareAndDeleteArgs)` on doubles — same six Lua ops.
 *  - **PES cross-language release**: the fix this suite forced — Java ZADDs
 *    permit members as the hex id decoded to 16 raw bytes
 *    (ByteBufUtil.decodeHexDump, 4.6.1+ bytecode); TS used to store the hex
 *    TEXT, so a permit acquired in one language was unreleasable in the
 *    other. TS members now go over the wire as raw bytes (Buffer), and the
 *    two languages trade acquire/release by id.
 *
 * Skips with a logged line (never silently) when Redis, a command or the
 * Java 4.8 probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson, CompareAndDeleteArgs } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48d:';

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

async function canCmd(raw: Redis, cmd: string): Promise<boolean> {
  try {
    const r = await raw.call('COMMAND', 'INFO', cmd);
    return Array.isArray(r) && r.length > 0 && r[0] !== null;
  } catch {
    return false;
  }
}

let probeOk: boolean | null = null;
async function probe48Up(): Promise<boolean> {
  if (probeOk !== null) return probeOk;
  try {
    probeOk = (await probe48.probe(`cb48-capacity ${NS}warmup`)) === 'capacity=0';
    if (!probeOk) console.warn('[live interop48d] Java 4.8 probe answered unexpectedly — bidirectional checks skipped');
  } catch (e) {
    console.warn(`[live interop48d] Java 4.8 probe unavailable (mvn/redisson-4.8.0 missing?) — bidirectional checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

describe('live interop48d: RAtomicDouble args + PES cross-language permits vs Java 4.8.0', () => {
  it('RAtomicDouble incrementAndGet(DoubleIncrementArgs): TS Lua-CAS ≡ Java native INCREX', async () => {
    if (!hasRedis) return;
    const raw = new Redis(REDIS_URL, { maxRetriesPerRequest: 1 });
    raw.on('error', () => undefined);
    if (!(await canCmd(raw, 'INCREX'))) {
      console.log('[live interop48d] INCREX unavailable (needs redis >=8.8 — Java side requires it) — skipping');
      raw.disconnect();
      return;
    }
    raw.disconnect();
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const k = `${NS}ad`;
    try {
      const ad = r.getAtomicDouble(k);
      await ad.set(1.5);

      // TS reject (no saturate): 1.5 + 5 > 3.5 → unchanged, returns current
      expect(await ad.incrementAndGet({ by: 5, upperBound: 3.5 })).toBeCloseTo(1.5);
      expect(Number((await probe48.probe(`atomicd48-get ${k}`)).split('=')[1])).toBeCloseTo(1.5);
      // TS saturate: clamps to the bound
      expect(await ad.incrementAndGet({ by: 5, upperBound: 3.5, saturate: true })).toBeCloseTo(3.5);

      // Java reject: 3.5 - 7.25 < LBOUND 1 → unchanged
      expect(Number((await probe48.probe(`atomicd48-incrargs ${k} -7.25 1 - 0`)).split('=')[1])).toBeCloseTo(3.5);
      // Java saturate: clamps down to 1
      expect(Number((await probe48.probe(`atomicd48-incrargs ${k} -7.25 1 - 1`)).split('=')[1])).toBeCloseTo(1);
      expect(await ad.get()).toBeCloseTo(1);

      // CAD on doubles, both languages
      await ad.set(2.5);
      expect(await probe48.probe(`atomicd48-cadd ${k} > 3`)).toBe('deleted=false');
      expect(await ad.compareAndDelete(CompareAndDeleteArgs.equal(2.5))).toBe(true);
    } finally {
      await r.getKeys().delete(k);
      await r.shutdown();
    }
  }, 300000);

  it('RPermitExpirableSemaphore: permits acquired in one language released in the other', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes`;
    const timeout = `{${name}}:timeout`; // suffixName hash-tag form
    try {
      await r.getKeys().delete(name, timeout);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(2)).toBe(true);

      // TS acquires one; Java sees availability drop and acquires the other
      const tsIds = await pes.tryAcquire(60000);
      expect(tsIds).not.toBeNull();
      expect(await probe48.probe(`pes48-available ${name}`)).toBe('available=1');
      const jPermit = (await probe48.probe(`pes48-tryacquire ${name} 60000`)).slice('permit='.length);
      expect(jPermit).not.toBeNull();
      expect(jPermit).not.toBe('null');
      expect(await pes.availablePermits()).toBe(0);

      // Java releases the TS-acquired permit id (raw-byte member lookup);
      // Java release(String) is void — availability is the observable
      await probe48.probe(`pes48-release ${name} ${tsIds![0]}`);
      expect(await pes.availablePermits()).toBe(1);

      // TS releases the Java-acquired permit id (hex→Buffer member)
      expect(await pes.release(jPermit)).toBe(true);
      expect(await probe48.probe(`pes48-available ${name}`)).toBe('available=2');

      // lease bookkeeping crosses too: TS renews a Java-acquired permit
      const j2 = (await probe48.probe(`pes48-tryacquire ${name} 2000`)).slice('permit='.length);
      expect(await pes.updateLeaseTime(j2, 60000)).toBe(true);
      expect(await pes.getLeaseTime(j2)).toBeGreaterThan(30000);
      expect(await pes.release(j2)).toBe(true);
    } finally {
      await r.getKeys().delete(name, timeout);
      await r.shutdown();
    }
  }, 300000);
});
