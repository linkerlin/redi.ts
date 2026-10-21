/**
 * Atomic structures + adders + id generator.
 * Wire: plain decimal strings (StringCodec semantics), BaseAdder pub/sub protocol.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { luaEval, prefixName, randomHex, serverNowMs, sleep, suffixName, toNumber } from './util.js';
import { KeyspaceListenerHub, type ObjectListener } from './listeners.js';

const ATOMIC_CAS = `local currValue = redis.call('get', KEYS[1])
if currValue == ARGV[1] or (tonumber(ARGV[1]) == 0 and currValue == false) then
    redis.call('set', KEYS[1], ARGV[2])
    return 1
else
    return 0
end`;

const ATOMIC_DEL = `local v = redis.call('get', KEYS[1]);
if v ~= false and v == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;

const ATOMIC_GET_AND_ADD = `local old = redis.call('get', KEYS[1])
if old == false then old = '0' end
redis.call('incrby', KEYS[1], ARGV[1])
return old`;

const ATOMIC_ADD_AND_GET = `redis.call('incrby', KEYS[1], ARGV[1])
return redis.call('get', KEYS[1])`;

const ATOMIC_SET_IF = `local current = tonumber(redis.call('get', KEYS[1]) or '0')
if (ARGV[1] == 'less' and current < tonumber(ARGV[2]))
        or (ARGV[1] == 'greater' and current > tonumber(ARGV[2])) then
    redis.call('set', KEYS[1], ARGV[3])
    return 1
end
return 0`;

const DOUBLE_COMPARE_DELETE = `local current = redis.call('get', KEYS[1])
if current ~= false and tonumber(current) == tonumber(ARGV[1]) then
    redis.call('del', KEYS[1])
    return 1
end
return 0`;

export type LongValue = number | bigint;

export type CompareCondition = '<' | '<=' | '>' | '>=' | '==' | '~=';

export interface CompareAndDeleteArgs {
  condition: CompareCondition;
  threshold: number;
}

/** TS shape of Java's static `CompareAndDeleteArgs` factories. */
export const CompareAndDeleteArgs = {
  less: (threshold: number): CompareAndDeleteArgs => ({ condition: '<', threshold }),
  lessOrEqual: (threshold: number): CompareAndDeleteArgs => ({ condition: '<=', threshold }),
  greater: (threshold: number): CompareAndDeleteArgs => ({ condition: '>', threshold }),
  greaterOrEqual: (threshold: number): CompareAndDeleteArgs => ({ condition: '>=', threshold }),
  equal: (threshold: number): CompareAndDeleteArgs => ({ condition: '==', threshold }),
  notEqual: (threshold: number): CompareAndDeleteArgs => ({ condition: '~=', threshold }),
};

const COMPARE_AND_DELETE = `local currValue = redis.call('get', KEYS[1])
if currValue == false then return 0 end
currValue = tonumber(currValue)
local threshold = tonumber(ARGV[1])
local op = ARGV[2]
local match = false
if op == '<' then match = currValue < threshold
elseif op == '<=' then match = currValue <= threshold
elseif op == '>' then match = currValue > threshold
elseif op == '>=' then match = currValue >= threshold
elseif op == '==' then match = currValue == threshold
elseif op == '~=' then match = currValue ~= threshold
end
if match then
    redis.call('del', KEYS[1])
    return 1
end
return 0`;

function parseLong(raw: string | null): LongValue {
  if (raw === null || raw === undefined) return 0;
  if (/^-?\d+$/.test(raw) && !Number.isSafeInteger(Number(raw))) {
    try { return BigInt(raw); } catch { return Number(raw); }
  }
  return Number(raw);
}

export class RAtomicLong extends RedisDataStructure {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string) {
    super(redis, name, new JsonCodec());
    this.listenerHub = new KeyspaceListenerHub(redis, name, { onChange: 'incrby' });
  }
  async get(): Promise<LongValue> {
    return parseLong(await this.redis.get(this.name));
  }
  async set(value: LongValue): Promise<void> {
    await this.redis.set(this.name, String(value));
  }
  async getAndSet(value: LongValue): Promise<LongValue> {
    return parseLong(await this.redis.getset(this.name, String(value)));
  }
  async getAndDelete(): Promise<LongValue | null> {
    const v = await (this.redis as any).getdel?.(this.name);
    if (v !== undefined) return v === null ? null : parseLong(String(v));
    const old = await this.redis.get(this.name);
    if (old !== null) await this.redis.del(this.name);
    return old === null ? null : parseLong(old);
  }
  async incrementAndGet(): Promise<LongValue> {
    return this.addAndGet(1);
  }
  async decrementAndGet(): Promise<LongValue> {
    return this.addAndGet(-1);
  }
  async addAndGet(delta: LongValue): Promise<LongValue> {
    // Return GET after INCRBY: Redis Lua numbers are doubles, but GET keeps
    // the exact decimal string for values beyond 2^53.
    const raw = await luaEval(this.redis, ATOMIC_ADD_AND_GET, 1, this.name, String(delta));
    return parseLong(raw === null ? null : String(raw));
  }
  async getAndAdd(delta: LongValue): Promise<LongValue> {
    const raw = await luaEval(this.redis, ATOMIC_GET_AND_ADD, 1, this.name, String(delta));
    return parseLong(raw === null ? null : String(raw));
  }
  async getAndIncrement(): Promise<LongValue> {
    return this.getAndAdd(1);
  }
  async getAndDecrement(): Promise<LongValue> {
    return this.getAndAdd(-1);
  }
  async setIfLess(threshold: LongValue, value: LongValue): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ATOMIC_SET_IF, 1, this.name, 'less', String(threshold), String(value))) === 1;
  }
  async setIfGreater(threshold: LongValue, value: LongValue): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ATOMIC_SET_IF, 1, this.name, 'greater', String(threshold), String(value))) === 1;
  }
  async compareAndSet(expect: LongValue, update: LongValue): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ATOMIC_CAS, 1, this.name, String(expect), String(update))) === 1;
  }
  async compareAndDelete(expect: LongValue): Promise<boolean>;
  async compareAndDelete(args: CompareAndDeleteArgs): Promise<boolean>;
  async compareAndDelete(expectOrArgs: LongValue | CompareAndDeleteArgs): Promise<boolean> {
    if (typeof expectOrArgs === 'object') {
      return toNumber(await luaEval(
        this.redis, COMPARE_AND_DELETE, 1, this.name,
        String(expectOrArgs.threshold), expectOrArgs.condition,
      )) === 1;
    }
    return toNumber(await luaEval(this.redis, ATOMIC_DEL, 1, this.name, String(expectOrArgs))) === 1;
  }
  /** Java `addListener(ObjectListener)`: IncrByListener → `incrby` keyspace
   *  events (requires K/E notifications). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
  override async delete(): Promise<boolean> {
    return super.delete();
  }
}

export class RAtomicDouble extends RedisDataStructure {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string) {
    super(redis, name, new JsonCodec());
    // Java maps IncrByListener to `incrby`; INCRBYFLOAT emits `incrbyfloat`,
    // so subscribe both to keep addAndGet observable.
    this.listenerHub = new KeyspaceListenerHub(redis, name, { onChange: ['incrby', 'incrbyfloat'] });
  }
  async get(): Promise<number> {
    const v = await this.redis.get(this.name);
    return v === null ? 0 : Number(v);
  }
  async set(value: number): Promise<void> {
    await this.redis.set(this.name, String(value));
  }
  async getAndSet(value: number): Promise<number> {
    const old = await this.redis.getset(this.name, String(value));
    return old === null ? 0 : Number(old);
  }
  async addAndGet(delta: number): Promise<number> {
    return Number(await this.redis.incrbyfloat(this.name, delta));
  }
  async incrementAndGet(): Promise<number> {
    return this.addAndGet(1);
  }
  async decrementAndGet(): Promise<number> {
    return this.addAndGet(-1);
  }
  async getAndAdd(delta: number): Promise<number> {
    // Single Lua round-trip: return the OLD value exactly as stored. The old
    // `newValue - delta` client-side reverse calculation accumulated float
    // error (0.1 + 0.2 -> 0.10000000000000003).
    const old = await luaEval(this.redis, `local old = redis.call('get', KEYS[1])
if old == false then old = '0' end
redis.call('incrbyfloat', KEYS[1], ARGV[1])
return old`, 1, this.name, String(delta));
    return Number(old ?? 0);
  }
  async getAndIncrement(): Promise<number> {
    return this.getAndAdd(1);
  }
  async getAndDecrement(): Promise<number> {
    return this.getAndAdd(-1);
  }
  async getAndDelete(): Promise<number | null> {
    const v = await (this.redis as any).getdel?.(this.name);
    if (v !== undefined) return v === null ? null : Number(v);
    const old = await this.redis.get(this.name);
    if (old !== null) await this.redis.del(this.name);
    return old === null ? null : Number(old);
  }
  async setIfLess(threshold: number, value: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ATOMIC_SET_IF, 1, this.name, 'less', String(threshold), String(value))) === 1;
  }
  async setIfGreater(threshold: number, value: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ATOMIC_SET_IF, 1, this.name, 'greater', String(threshold), String(value))) === 1;
  }
  async compareAndDelete(expect: number): Promise<boolean>;
  async compareAndDelete(args: CompareAndDeleteArgs): Promise<boolean>;
  async compareAndDelete(expectOrArgs: number | CompareAndDeleteArgs): Promise<boolean> {
    if (typeof expectOrArgs === 'object') {
      return toNumber(await luaEval(
        this.redis, COMPARE_AND_DELETE, 1, this.name,
        String(expectOrArgs.threshold), expectOrArgs.condition,
      )) === 1;
    }
    return toNumber(await luaEval(this.redis, DOUBLE_COMPARE_DELETE, 1, this.name, String(expectOrArgs))) === 1;
  }
  async compareAndSet(expect: number, update: number): Promise<boolean> {
    const s = `local v = redis.call('get', KEYS[1]); if v == false then v = '0' end;
if tonumber(v) == tonumber(ARGV[1]) then redis.call('set', KEYS[1], ARGV[2]); return 1 else return 0 end`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, String(expect), String(update))) === 1;
  }
  /** Java `addListener(ObjectListener)`: IncrByListener; subscribes to both
   *  `incrby` and `incrbyfloat` (Redis emits the latter for INCRBYFLOAT). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
}

// -- BaseAdder (Redisson protocol, source-verified): -------------------------------
// channel {name}:adder-topic     messages "1:<reqId>" = SUM, "0:<reqId>" = CLEAR
// string {name}:<reqId>:counter  flush target: every live instance INCRBYs its
//                                full local total here; requester GETDELs it.
//                                Sum returns the counter ONLY (requester is a
//                                subscriber, so its own flush is included).
// string {name}:<reqId>:semaphore barrier: PUBLISH returns subscriber count n;
//                                requester acquires n permits then DELs it.
// All companion keys are hash-tagged (suffixName), matching redi.go/Java.

const ADDER_ACQUIRE = `local value = redis.call('get', KEYS[1])
if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then
    redis.call('decrby', KEYS[1], ARGV[1])
    return 1
end
return 0`;

const ADDER_RELEASE = `local value = redis.call('incrby', KEYS[1], ARGV[1])
redis.call('publish', KEYS[2], ARGV[1])
return value`;

abstract class RBaseAdder extends RedisDataStructure {
  private local = 0;
  private localD = 0;
  private isDouble: boolean;
  private sub: Redis | null = null;
  private readyP: Promise<void> = Promise.resolve();

  constructor(redis: Redis, name: string, isDouble: boolean) {
    super(redis, name);
    this.isDouble = isDouble;
    this.readyP = this.ensureListener();
  }

  private topic(): string {
    return suffixName(this.name, 'adder-topic');
  }
  private counterKey(id: string): string {
    return suffixName(this.name, `${id}:counter`);
  }
  private semKey(id: string): string {
    return suffixName(this.name, `${id}:semaphore`);
  }
  private semChannel(sem: string): string {
    return prefixName('redisson_sc', sem);
  }

  private async ensureListener(): Promise<void> {
    try {
      const Dup = (this.redis as any).duplicate?.bind(this.redis);
      if (!Dup) return;
      this.sub = Dup();
      // attach BEFORE subscribe so no broadcast is lost in the gap
      (this.sub as any).on('message', (_ch: string, msg: string) => {
        void this.handle(msg);
      });
      await (this.sub as any).subscribe(this.topic());
    } catch {
      this.sub = null;
    }
  }

  private async handle(msg: string): Promise<void> {
    const idx = msg.indexOf(':');
    if (idx < 0) return;
    const kind = msg.slice(0, idx);
    const id = msg.slice(idx + 1);
    try {
      if (kind === '1') {
        // SUM: flush full local total (non-destructive, like Java/Go).
        if (this.isDouble) {
          const v = this.localD;
          if (v !== 0) await this.redis.incrbyfloat(this.counterKey(id), v);
        } else {
          const v = this.local;
          if (v !== 0) await this.redis.incrby(this.counterKey(id), v);
        }
      } else if (kind === '0') {
        if (this.isDouble) this.localD = 0;
        else this.local = 0;
      } else {
        return;
      }
      // release one barrier permit
      const sem = this.semKey(id);
      await luaEval(this.redis, ADDER_RELEASE, 2, sem, this.semChannel(sem), '1');
    } catch { /* noop */ }
  }

  protected addLocal(n: number): void {
    if (this.isDouble) this.localD += n;
    else this.local += Math.trunc(n);
  }

  private async request(kind: '1' | '0'): Promise<number> {
    await this.readyP;
    const id = randomHex(16);
    let n = 0;
    try {
      n = toNumber(await this.redis.publish(this.topic(), `${kind}:${id}`));
    } catch {
      n = 0;
    }
    if (n === 0) {
      // No live subscribers (own listener included): local buffer is everything.
      // Only reachable when pub/sub is unavailable (e.g. mock without dup).
      return this.isDouble ? this.localD : this.local;
    }
    const sem = this.semKey(id);
    const deadline = Date.now() + 60000;
    for (;;) {
      const ok = toNumber(await luaEval(this.redis, ADDER_ACQUIRE, 1, sem, String(n)));
      if (ok === 1) break;
      if (Date.now() >= deadline) throw new Error(`redi: adder ${this.name} sum barrier timed out waiting for ${n} instances`);
      await sleep(10);
    }
    try {
      if (kind === '1') {
        const raw = await this.redis.getdel(this.counterKey(id));
        return raw === null ? 0 : Number(raw);
      }
      return 0;
    } finally {
      try {
        await this.redis.del(sem);
      } catch { /* noop */ }
    }
  }

  async sum(): Promise<number> {
    return this.request('1');
  }

  async reset(): Promise<void> {
    await this.request('0');
  }

  async sumThenReset(): Promise<number> {
    const s = await this.sum();
    await this.reset();
    return s;
  }

  async destroy(): Promise<void> {
    try {
      await (this.sub as any)?.unsubscribe?.(this.topic());
      (this.sub as any)?.disconnect?.();
    } catch { /* noop */ }
    this.sub = null;
  }
}

export class RLongAdder extends RBaseAdder {
  constructor(redis: Redis, name: string) {
    super(redis, name, false);
  }
  async add(x: number): Promise<void> {
    this.addLocal(Math.trunc(x));
  }
  async increment(): Promise<void> {
    this.addLocal(1);
  }
  async decrement(): Promise<void> {
    this.addLocal(-1);
  }
}

export class RDoubleAdder extends RBaseAdder {
  constructor(redis: Redis, name: string) {
    super(redis, name, true);
  }
  async add(x: number): Promise<void> {
    this.addLocal(x);
  }
  async increment(): Promise<void> {
    this.addLocal(1);
  }
  async decrement(): Promise<void> {
    this.addLocal(-1);
  }
}

// -- RIdGenerator --------------------------------------------------------------

export class RIdGenerator extends RedisDataStructure {
  private next = 0;
  private maxId = 0;
  private allocSize = 5000;
  constructor(redis: Redis, name: string) {
    super(redis, name);
  }
  private allocKey(): string {
    return suffixName(this.name, 'allocation');
  }
  async tryInit(value: number, allocationSize = 5000): Promise<boolean> {
    // Bug-compatible with Redisson: only the SECOND setnx is returned.
    // (K1-exists + K2-missing still reports true; do not "fix" this.)
    const s = `redis.call('setnx', KEYS[1], ARGV[1])
return redis.call('setnx', KEYS[2], ARGV[2])`;
    const r = await luaEval(this.redis, s, 2, this.name, this.allocKey(), String(value), String(allocationSize));
    if (toNumber(r) === 1) {
      this.allocSize = allocationSize;
      return true;
    }
    return false;
  }
  private async allocate(): Promise<void> {
    const s = `local alloc = redis.call('get', KEYS[2])
if alloc == false then
    redis.call('set', KEYS[2], '5000')
    alloc = '5000'
end
local v = redis.call('get', KEYS[1])
if v == false then
    redis.call('set', KEYS[1], '1')
    v = '1'
else
    v = tostring(tonumber(v))
end
redis.call('incrby', KEYS[1], alloc)
return {v, alloc}`;
    const r: any = await luaEval(this.redis, s, 2, this.name, this.allocKey());
    const start = Array.isArray(r) ? toNumber(r[0], 1) : 1;
    const size = Array.isArray(r) ? toNumber(r[1], 5000) : 5000;
    this.next = start;
    this.maxId = start + size;
    this.allocSize = size;
  }
  async nextId(): Promise<number> {
    if (this.next >= this.maxId) await this.allocate();
    return this.next++;
  }
  async nextIds(count: number): Promise<number[]> {
    const out: number[] = [];
    for (let i = 0; i < count; i++) out.push(await this.nextId());
    return out;
  }
}
