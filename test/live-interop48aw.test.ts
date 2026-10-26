/**
 * Live interop vs **Java Redisson 4.8.0**, round 49 — audit-ledger batch 3,
 * closing the remaining round-46 ledger:
 *
 *  - RBlockingDeque subscribeOnFirstElements/subscribeOnLastElements (the
 *    deque variants of the ElementsSubscribeService pump; TS takeFirst/
 *    takeLast deliver into Java consumers, Java consumers receive TS
 *    writes, unsubscribe stops delivery).
 *  - RDoubleAdder sum()/reset() — the adder wire ({name}:adder-topic,
 *    {name}:{id}:semaphore/:counter, "1:<id>"/"0:<id>" messages) is shared:
 *    a TS sum() flushes the Java instance's LOCAL buffer and vice versa.
 *  - RFunction Java-shaped load(libName, code) / loadAndReplace(libName,
 *    code): the `#!lua name=<lib> \n <code>` shebang is built verbatim like
 *    Java's loadAsync; plain load FAILS while the library name exists
 *    (server-side duplicate rejection), loadAndReplace overwrites.
 *  - RDelayedQueue offerAsync: TS `offer(value, delayMs)` IS the async form
 *    (Java `offer` = `get(offerAsync)` — no distinct wire); the staging
 *    wire itself was proven in round 33.
 *
 * Skips with a logged line (never silently) when Redis or the Java 4.8
 * probe is unavailable.
 */
import { describe, it, expect } from 'vitest';
import Redis from 'ioredis';
import { Redisson } from '../src';
import { probe48 } from './helpers/java-probe48.js';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const NS = 'TsJavaT:i48aw:';

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
    console.warn(`[live interop48aw] Java 4.8 probe unavailable — checks skipped: ${(e as Error).message.split('\n')[0]}`);
    probeOk = false;
  }
  return probeOk;
}

async function delAll(raw: Redis, keys: string[]): Promise<void> {
  if (keys.length > 0) await raw.del(...keys);
}

/** Wait until fn() returns the wanted string (Java pump arms within ~2s). */
async function until(fn: () => Promise<string>, want: string, ms = 10000): Promise<string> {
  const t0 = Date.now();
  for (;;) {
    const v = await fn();
    if (v === want || Date.now() - t0 > ms) return v;
    await new Promise((res) => setTimeout(res, 250));
  }
}

describe('live interop48aw: audit-ledger batch 3 (deque subs/double adder/function load) vs Java 4.8.0', () => {
  it('RBlockingDeque subscribeOnFirst/LastElements deliver cross-language', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const d1a = `${NS}d1a`; // Java FIRST pump only
    const d1b = `${NS}d1b`; // Java LAST pump only — one pending client per
    const d2a = `${NS}d2a`; // TS FIRST pump only — key, so a slow-arming
    const d2b = `${NS}d2b`; // subscription just finds the element waiting
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [d1a, d1b, d2a, d2b]);

      // Java FIRST pump; TS addFirst lands at its head
      await probe48.probe(`bd48-subfirst ${d1a}`);
      await r.getBlockingDeque<string>(d1a).addFirst('f1');
      expect(await until(() => probe48.probe(`bd48-seen ${d1a}`), 'first=f1 last=')).toBe('first=f1 last=');
      // Java LAST pump; TS addLast lands at its tail
      await probe48.probe(`bd48-sublast ${d1b}`);
      await r.getBlockingDeque<string>(d1b).addLast('l1');
      // bd48-seen prints the JVM-global cumulative lists — f1 (d1a) is still there
      expect(await until(() => probe48.probe(`bd48-seen ${d1b}`), 'first=f1 last=l1')).toBe('first=f1 last=l1');

      // TS pumps; Java offerFirst/offerLast land at each end
      const dqA = r.getBlockingDeque<string>(d2a);
      const dqB = r.getBlockingDeque<string>(d2b);
      const firstGot: string[] = [];
      const lastGot: string[] = [];
      const idF = dqA.subscribeOnFirstElements((v) => firstGot.push(v as string));
      const idL = dqB.subscribeOnLastElements((v) => lastGot.push(v as string));
      await probe48.probe(`bd48-offerfirst ${d2a} jf`);
      const t0 = Date.now();
      while (firstGot.length === 0 && Date.now() - t0 < 10000) {
        await new Promise((res) => setTimeout(res, 250));
      }
      await probe48.probe(`bd48-offerlast ${d2b} jl`);
      const t1 = Date.now();
      while (lastGot.length === 0 && Date.now() - t1 < 10000) {
        await new Promise((res) => setTimeout(res, 250));
      }
      expect(firstGot).toEqual(['jf']);
      expect(lastGot).toEqual(['jl']);
      dqA.unsubscribe(idF);
      dqB.unsubscribe(idL);

      // unsubscribed TS pump stops delivering (a zombie take may still
      // consume, but the cancelled consumer must never fire)
      await dqA.addLast('after-unsub');
      await new Promise((res) => setTimeout(res, 1500));
      expect(firstGot).toEqual(['jf']);
      expect(lastGot).toEqual(['jl']);
    } finally {
      await delAll(raw, [d1a, d1b, d2a, d2b]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RDoubleAdder sum/reset flush the OTHER language local buffer', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const a = `${NS}da`;
    const signal = `${NS}da-sig`;
    const raw = new Redis(REDIS_URL);
    try {
      await delAll(raw, [a, signal, `${signal}:ready`, `${signal}:result`]);

      // Java cohort: add 2.5 locally, mark ready, wait for the TS signal,
      // then sum() — its own 2.5 plus whatever TS flushed into its request.
      // TS adds 1.5+0.25, waits for the ready flag, then sums: the Java
      // buffer must arrive through the shared adder-topic.
      const cohort = probe48.probe(`adder48-dcohort ${a} 2.5 ${signal}`);
      const readyKey = `${signal}:ready`;
      const rawWait = new Redis(REDIS_URL);
      for (let i = 0; i < 300; i++) {
        if ((await rawWait.get(readyKey)) !== null) break;
        await new Promise((res) => setTimeout(res, 200));
      }
      expect(await rawWait.get(readyKey)).toBe('"1"');
      rawWait.disconnect();

      const da = r.getDoubleAdder(a);
      await da.add(1.5);
      await da.add(0.25);
      await raw.set(signal, 'go'); // releases the Java cohort's wait
      const jSum = await cohort;
      // both sides sum the SAME total: 2.5 (Java local) + 1.75 (TS local)
      expect(Number(jSum.replace('sum=', ''))).toBeCloseTo(4.25, 6);
      expect(await da.sum()).toBeCloseTo(4.25, 6);

      // reset clears BOTH sides' buffers (Java counter + TS local)
      await da.reset();
      await new Promise((res) => setTimeout(res, 300));
      expect(await da.sum()).toBe(0);
      await probe48.probe(`adder48-dadd ${a} 7.5`);
      expect(await da.sum()).toBeCloseTo(7.5, 6);
      await probe48.probe(`adder48-dreset ${a}`);
      await new Promise((res) => setTimeout(res, 300));
      expect(await da.sum()).toBe(0);
    } finally {
      await delAll(raw, [a, signal, `${signal}:ready`, `${signal}:result`]);
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);

  it('RFunction load(lib, code) fails on duplicates, loadAndReplace overwrites', async () => {
    if (!hasRedis) return;
    if (!(await probe48Up())) return;
    const r = Redisson.create({ address: REDIS_URL });
    const raw = new Redis(REDIS_URL);
    const fn = r.getFunction();
    try {
      await (raw as any).call('FUNCTION', 'FLUSH');

      // TS Java-shaped load: fresh name succeeds and Java FCALL runs it
      // (returned strings are JSON-quoted: the probe decodes with Jackson)
      await fn.load('p49tslib', "redis.register_function{function_name='p49tsf', callback=function(keys, args) return '\"ts-' .. keys[1] .. '\"' end, flags={'no-writes'}}");
      expect(await probe48.probe(`fn48-callread p49tsf hello`)).toBe('value=ts-hello');

      // plain load of an EXISTING name fails (TS side AND Java side)
      await expect(fn.load('p49tslib', "redis.register_function('x', function() return 1 end)")).rejects.toThrow();
      const j = await probe48.probe(`fn48-loadplain p49tslib return 1`);
      expect(j.startsWith('err=')).toBe(true);

      // loadAndReplace overwrites from BOTH directions
      await fn.loadAndReplace('p49tslib', "redis.register_function{function_name='p49tsf', callback=function(keys, args) return '\"v2-' .. keys[1] .. '\"' end, flags={'no-writes'}}");
      expect(await probe48.probe(`fn48-callread p49tsf hello`)).toBe('value=v2-hello');
      await probe48.probe(`fn48-loadandreplace p49tslib p49v3`);
      expect(await probe48.probe(`fn48-callread p49tsf hello`)).toBe('value=p49v3');

      // Java-shaped plain load on a FRESH name works (space-free Lua body)
      await probe48.probe(`fn48-loadplain p49jlib redis.register_function('p49j',function(k,a)return(42)end)`);
      expect(await probe48.probe(`fn48-fnexists p49jlib`)).toBe('fns=p49j');
      // ...and a second plain load of that fresh name now fails too
      const j2 = await probe48.probe(`fn48-loadplain p49jlib redis.register_function('p49j',function(k,a)return(42)end)`);
      expect(j2.startsWith('err=')).toBe(true);
    } finally {
      try {
        await (raw as any).call('FUNCTION', 'FLUSH');
      } catch {
        /* mock/live variance — live-only file */
      }
      raw.disconnect();
      await r.shutdown();
    }
  }, 300000);
});
