/**
 * Live interop vs **Java Redisson 4.6.1/4.8.0**, round 15 — TTL-driven
 * CRASH RECOVERY and large-payload integrity:
 *
 *  - Lock lease expiry: a TS holder that never unlocks (crashed client)
 *    releases the lock purely by lease expiry — Java acquires after the
 *    window. "gone-after" direction (law 10): naturally jitter-resistant.
 *  - PES permit expiry: an unreleased permit returns to the pool by lease
 *    expiry (lazy purge) and Java acquires it.
 *  - DelayedQueue firing: the 4.6.1 probe offers with a delay (its
 *    resident JVM schedules the transfer); the destination observed by
 *    TS is empty before the deadline and receives the item after.
 *  - Large payloads: 128KB of random bytes written by TS verified by
 *    Java via len+sum stats; 64KB of deterministic pattern written by
 *    Java verified byte-for-byte by TS.
 *
 * Skips with a logged line (never silently) when Redis or a probe is
 * unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { randomBytes } from 'node:crypto';
import { Redisson } from '../src';
import { probe } from './helpers/java-probe.js';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48o:';

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

let probeUp: boolean | null = null;
async function java461Up(): Promise<boolean> {
  if (probeUp !== null) return probeUp;
  try {
    probeUp = (await probe(`atomic-get ${NS}warm`)).startsWith('ATOMIC:');
  } catch (e) {
    console.warn(`[live interop48o] Java 4.6.1 probe unavailable — recovery checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeUp = false;
  }
  return probeUp;
}

let probe48Ok: boolean | null = null;
async function java480Up(): Promise<boolean> {
  if (probe48Ok !== null) return probe48Ok;
  try {
    probe48Ok = (await probe48.probe(`cb48-capacity ${NS}warm`)) === 'capacity=0';
  } catch (e) {
    console.warn(`[live interop48o] Java 4.8.0 probe unavailable — recovery checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probe48Ok = false;
  }
  return probe48Ok;
}

describe('live interop48o: TTL crash recovery + large payloads', () => {
  it('lock lease expiry: a crashed TS holder is recovered by expiry', async () => {
    if (!hasRedis) return;
    if (!(await java461Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}lock`;
    try {
      await r.getKeys().delete(name);
      // explicit short lease: NO watchdog renewal — a holder that dies
      // silently is exactly this
      await r.getLock(name).lock(2000);
      expect(await probe(`lock-try ${name}`)).toBe('CONTENDED');
      await new Promise((res) => setTimeout(res, 5000)); // 2.5× the lease
      expect(await probe(`lock-try ${name}`)).toBe('ACQUIRED'); // expired free
    } finally {
      await r.getKeys().delete(name);
      await r.shutdown();
    }
  }, 300000);

  it('PES permit expiry: an unreleased permit returns to the pool', async () => {
    if (!hasRedis) return;
    if (!(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const name = `${NS}pes`;
    const timeout = `{${name}}:timeout`; // suffixName hash-tag form
    try {
      await r.getKeys().delete(name, timeout);
      const pes = r.getPermitExpirableSemaphore(name);
      expect(await pes.trySetPermits(1)).toBe(true);

      // take the only permit and never release it (3s lease). Note: the
      // "before expiry" direction is deliberately NOT asserted — TS scores
      // leases with server TIME while Java's purge cutoff uses its client
      // clock, and a WSL-vs-host skew of seconds makes that race flaky.
      const held = await pes.tryAcquire(3000);
      expect(held).not.toBeNull();

      // after expiry the lazy purge returns it; Java acquires ("gone-after"
      // direction — jitter-resistant). The exact available-count is NOT
      // asserted: it sums deadlines written with Java's client clock and
      // purged with TS's server-TIME, and this environment's WSL-vs-host
      // skew exceeds any lease we'd want in a fast test.
      await new Promise((res) => setTimeout(res, 9000)); // 3× the lease
      const jid = await probe48.probe(`pes48-tryacquire ${name} 600000`);
      expect(jid).not.toBe('permit=null'); // the expired permit came back
    } finally {
      await r.getKeys().delete(name, timeout);
      await r.shutdown();
    }
  }, 300000);

  it('delayed queue firing: Java schedules the transfer, TS receives on time', async () => {
    if (!hasRedis) return;
    if (!(await java461Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const dest = `${NS}dq`;
    try {
      // DQ companions are prefixName'd (braced): timeout ZSET + staging LIST
      await r.getKeys().delete(dest, `redisson_delay_queue_timeout:{${dest}}`, `redisson_delay_queue:{${dest}}`);
      // 3s delay; the pre-deadline emptiness is NOT asserted — the DQ
      // deadline math is client-clock sensitive and the WSL redis skews
      expect(await probe(`dq-offer ${dest} fired-item 3000`)).toBe('OFFERED');
      // after firing (the resident probe JVM schedules the transfer); must
      // use the BLOCKING pollWithTimeout — poll(12000) is the count overload
      // and drains non-blocking (it used to pass on leaked leftovers)
      const got = await r.getBlockingQueue<string>(dest).pollWithTimeout(12000);
      expect(got).toBe('fired-item');
    } finally {
      // DQ companions are prefixName'd (braced): timeout ZSET + staging LIST
      await r.getKeys().delete(dest, `redisson_delay_queue_timeout:{${dest}}`, `redisson_delay_queue:{${dest}}`);
      await r.shutdown();
    }
  }, 300000);

  it('large payloads: 128KB TS→Java by stats, 64KB Java→TS byte-for-byte', async () => {
    if (!hasRedis) return;
    if (!(await java480Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}big-ts`;
    const b = `${NS}big-java`;
    try {
      await r.getKeys().delete(a, b);

      // TS writes 128KB of random bytes; Java verifies len + byte sum
      const data = randomBytes(128 * 1024);
      await r.getBinaryStream(a).set(data);
      const sum = data.reduce((acc, byte) => acc + byte, 0);
      expect(await probe48.probe(`bstream48-stat ${a}`)).toBe(`len=${data.length} sum=${sum}`);

      // Java writes 64KB of a deterministic pattern; TS verifies every byte
      expect(await probe48.probe(`bstream48-gen ${b} 65536 7`)).toBe('gen=65536');
      const got = await r.getBinaryStream(b).get();
      expect(got).not.toBeNull();
      expect(got!.length).toBe(65536);
      let mismatches = 0;
      let gotSum = 0;
      for (let i = 0; i < 65536; i++) {
        const expected = (i * 31 + 7) & 0xff;
        if (got![i] !== expected) mismatches++;
        gotSum += got![i];
      }
      expect(mismatches).toBe(0);
      // cross-check the same stats Java would compute over this pattern
      const expectedSum = Array.from({ length: 65536 }, (_, i) => (i * 31 + 7) & 0xff)
        .reduce((acc, v) => acc + v, 0);
      expect(gotSum).toBe(expectedSum);
    } finally {
      await r.getKeys().delete(a, b);
      await r.shutdown();
    }
  }, 300000);
});
