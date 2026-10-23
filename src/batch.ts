/**
 * Typed RBatch facades. Each op queues plain Redis commands (or the same Lua
 * the non-batch structure uses) against the batch pipeline; reads return
 * promises that resolve when `execute()` runs — mirroring Redisson's
 * RFuture-per-op semantics. Wire-identical to direct ops (same keys/values).
 *
 * Mock note: ioredis-mock pipelines lack `eval`, so Lua-backed ops
 * (Map.put/remove/putIfAbsent, AtomicLong.compareAndSet) are covered by live
 * tests only; everything else runs on mock too.
 */
import type { Redis } from 'ioredis';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import type { RBatch } from './infra.js';
import type { ScoredEntry } from './collections.js';
import { toNumber } from './util.js';
import type { CountMinInitArgs, TSSample, TSInfo } from './probabilistic.js';
import { TSCreateArgs, TSAddArgs, TSRangeArgs, tsToSample, tsToSamples, tsInfoDecode } from './probabilistic.js';

function dec<T>(fn: (raws: any[]) => T) {
  return fn;
}

const asBool = dec((r) => r[0] === 1);
const asCount = dec((r) => Number(r[0] ?? 0));

export class BatchBucket<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  set(value: V, ttlMs?: number): void {
    const enc = this.enc(value);
    if (ttlMs && ttlMs > 0) this.batch.enqueue((p) => p.set(this.name, enc, 'PX', ttlMs), () => undefined);
    else this.batch.enqueue((p) => p.set(this.name, enc), () => undefined);
  }
  trySet(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.set(this.name, this.enc(value), 'NX'), asBool);
  }
  getAndSet(value: V): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.getset(this.name, this.enc(value)),
      dec((r) => this.dec(r[0])),
    );
  }
  delete(): Promise<boolean> {
    return this.batch.enqueue(
      (p) => p.del(this.name),
      dec((r) => Number(r[0] ?? 0) > 0),
    );
  }
  get(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.get(this.name),
      dec((r) => this.dec(r[0])),
    );
  }
}

export class BatchMap<K = any, V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private ek(k: unknown): string {
    return this.codec.encode(k);
  }
  private ev(v: unknown): string {
    return this.codec.encode(v);
  }
  private dv(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  put(key: K, value: V): Promise<V | null> {
    const s = `local old = redis.call('hget', KEYS[1], ARGV[1])
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
return old`;
    return this.batch.enqueue(
      (p) => p.eval(s, 1, this.name, this.ek(key), this.ev(value)),
      dec((r) => this.dv(r[0])),
    );
  }
  fastPut(key: K, value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.hset(this.name, this.ek(key), this.ev(value)), dec((r) => Number(r[0] ?? 0) === 1));
  }
  putIfAbsent(key: K, value: V): Promise<V | null> {
    const s = `if redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]) == 1 then
return nil else return redis.call('hget', KEYS[1], ARGV[1]) end`;
    return this.batch.enqueue(
      (p) => p.eval(s, 1, this.name, this.ek(key), this.ev(value)),
      dec((r) => this.dv(r[0])),
    );
  }
  remove(key: K): Promise<V | null> {
    const s = `local old = redis.call('hget', KEYS[1], ARGV[1])
redis.call('hdel', KEYS[1], ARGV[1])
return old`;
    return this.batch.enqueue(
      (p) => p.eval(s, 1, this.name, this.ek(key)),
      dec((r) => this.dv(r[0])),
    );
  }
  fastRemove(...keys: K[]): Promise<number> {
    if (keys.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.hdel(this.name, ...keys.map((k) => this.ek(k))), asCount);
  }
  get(key: K): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.hget(this.name, this.ek(key)),
      dec((r) => this.dv(r[0])),
    );
  }
}

export class BatchList<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  add(...values: V[]): Promise<number> {
    if (values.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.rpush(this.name, ...values.map((v) => this.enc(v))), asCount);
  }
  trim(from: number, to: number): void {
    this.batch.enqueue((p) => p.ltrim(this.name, from, to), () => undefined);
  }
  get(index: number): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lindex(this.name, index),
      dec((r) => this.dec(r[0])),
    );
  }
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.llen(this.name), asCount);
  }
}

export class BatchSet<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  add(...values: V[]): Promise<number> {
    if (values.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.sadd(this.name, ...values.map((v) => this.enc(v))), asCount);
  }
  remove(...values: V[]): Promise<number> {
    if (values.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.srem(this.name, ...values.map((v) => this.enc(v))), asCount);
  }
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.scard(this.name), asCount);
  }
  contains(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.sismember(this.name, this.enc(value)), asBool);
  }
  readAll(): Promise<V[]> {
    return this.batch.enqueue(
      (p) => p.smembers(this.name),
      dec((r) => ((r[0] as string[]) ?? []).map((x) => this.dec(x) as V)),
    );
  }
}

export class BatchQueue<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  offer(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.rpush(this.name, this.enc(value)), dec(() => true));
  }
  poll(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lpop(this.name),
      dec((r) => this.dec(r[0])),
    );
  }
  peek(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lindex(this.name, 0),
      dec((r) => this.dec(r[0])),
    );
  }
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.llen(this.name), asCount);
  }
}

export class BatchAtomicLong {
  constructor(private batch: RBatch, private name: string) {}
  set(value: number): void {
    this.batch.enqueue((p) => p.set(this.name, String(value)), () => undefined);
  }
  get(): Promise<number> {
    return this.batch.enqueue(
      (p) => p.get(this.name),
      dec((r) => (r[0] === null || r[0] === undefined ? 0 : Number(r[0]))),
    );
  }
  incrementAndGet(): Promise<number> {
    return this.batch.enqueue(
      (p) => p.incr(this.name),
      dec((r) => Number(r[0])),
    );
  }
  addAndGet(delta: number): Promise<number> {
    return this.batch.enqueue(
      (p) => p.incrby(this.name, delta),
      dec((r) => Number(r[0])),
    );
  }
  compareAndSet(expect: number, update: number): Promise<boolean> {
    const s = `local v = redis.call('get', KEYS[1]); if v == false then v = '0' end;
if tonumber(v) == tonumber(ARGV[1]) then redis.call('set', KEYS[1], ARGV[2]); return 1 else return 0 end`;
    return this.batch.enqueue((p) => p.eval(s, 1, this.name, String(expect), String(update)), asBool);
  }
}

export class BatchScoredSortedSet<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  add(score: number, value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.zadd(this.name, String(score), this.enc(value)), dec((r) => Number(r[0] ?? 0) > 0));
  }
  remove(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.zrem(this.name, this.enc(value)), dec((r) => Number(r[0] ?? 0) > 0));
  }
  getScore(value: V): Promise<number | null> {
    return this.batch.enqueue(
      (p) => p.zscore(this.name, this.enc(value)),
      dec((r) => (r[0] === null || r[0] === undefined ? null : Number(r[0]))),
    );
  }
  rank(value: V): Promise<number> {
    return this.batch.enqueue(
      (p) => p.zrank(this.name, this.enc(value)),
      dec((r) => (r[0] === null || r[0] === undefined ? -1 : Number(r[0]))),
    );
  }
}

export class BatchDeque<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  private dec(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  offerFirst(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.lpush(this.name, this.enc(value)), dec(() => true));
  }
  offerLast(value: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.rpush(this.name, this.enc(value)), dec(() => true));
  }
  pollFirst(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lpop(this.name),
      dec((r) => this.dec(r[0])),
    );
  }
  pollLast(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.rpop(this.name),
      dec((r) => this.dec(r[0])),
    );
  }
  peekFirst(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lindex(this.name, 0),
      dec((r) => this.dec(r[0])),
    );
  }
  peekLast(): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.lindex(this.name, -1),
      dec((r) => this.dec(r[0])),
    );
  }
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.llen(this.name), asCount);
  }
}

export class BatchAtomicDouble {
  constructor(private batch: RBatch, private name: string) {}
  set(value: number): void {
    this.batch.enqueue((p) => p.set(this.name, String(value)), () => undefined);
  }
  get(): Promise<number> {
    return this.batch.enqueue(
      (p) => p.get(this.name),
      dec((r) => (r[0] === null || r[0] === undefined ? 0 : Number(r[0]))),
    );
  }
  incrementAndGet(): Promise<number> {
    return this.batch.enqueue(
      (p) => p.incrbyfloat(this.name, 1),
      dec((r) => Number(r[0])),
    );
  }
  addAndGet(delta: number): Promise<number> {
    return this.batch.enqueue(
      (p) => p.incrbyfloat(this.name, delta),
      dec((r) => Number(r[0])),
    );
  }
  /** Single-Lua atomic get-and-add (float-exact old value, phase 18 shape). */
  getAndAdd(delta: number): Promise<number> {
    const s = `local v = redis.call('get', KEYS[1]); if v == false then v = '0' end;
redis.call('incrbyfloat', KEYS[1], ARGV[1]); return v`;
    return this.batch.enqueue(
      (p) => p.eval(s, 1, this.name, String(delta)),
      dec((r) => Number(r[0])),
    );
  }
  compareAndSet(expect: number, update: number): Promise<boolean> {
    const s = `local v = redis.call('get', KEYS[1]); if v == false then v = '0' end;
if tonumber(v) == tonumber(ARGV[1]) then redis.call('set', KEYS[1], ARGV[2]); return 1 else return 0 end`;
    return this.batch.enqueue((p) => p.eval(s, 1, this.name, String(expect), String(update)), asBool);
  }
}

export class BatchBitSet {
  constructor(private batch: RBatch, private name: string) {}
  set(bit: number, value = true): Promise<boolean> {
    return this.batch.enqueue((p) => p.setbit(this.name, bit, value ? 1 : 0), dec((r) => Number(r[0] ?? 0) === 1));
  }
  get(bit: number): Promise<boolean> {
    return this.batch.enqueue((p) => p.getbit(this.name, bit), asBool);
  }
  cardinality(): Promise<number> {
    return this.batch.enqueue((p) => p.bitcount(this.name), asCount);
  }
  /** Java `size()`: STRLEN × 8 (allocated bits; differs from cardinality). */
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.strlen(this.name), dec((r) => Number(r[0] ?? 0) * 8));
  }
  clear(bit?: number): void {
    if (bit === undefined) this.batch.enqueue((p) => p.del(this.name), () => undefined);
    else this.batch.enqueue((p) => p.setbit(this.name, bit, 0), () => undefined);
  }
}

export class BatchGeo<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  add(lon: number, lat: number, member: V): Promise<boolean> {
    return this.batch.enqueue((p) => p.geoadd(this.name, lon, lat, this.enc(member)), dec((r) => Number(r[0] ?? 0) > 0));
  }
  remove(...members: V[]): Promise<number> {
    if (members.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.zrem(this.name, ...members.map((m) => this.enc(m))), asCount);
  }
  dist(m1: V, m2: V, unit: 'm' | 'km' | 'mi' | 'ft' = 'm'): Promise<number | null> {
    return this.batch.enqueue(
      (p) => p.geodist(this.name, this.enc(m1), this.enc(m2), unit),
      dec((r) => (r[0] === null || r[0] === undefined ? null : Number(r[0]))),
    );
  }
  pos(member: V): Promise<{ lon: number; lat: number } | null> {
    return this.batch.enqueue(
      (p) => p.geopos(this.name, this.enc(member)),
      dec((r) => {
        const first = r[0];
        if (!first) return null;
        // ioredis returns [[lon, lat]]; tolerate a flat [lon, lat] too.
        const pair = Array.isArray(first) && Array.isArray(first[0]) ? first[0] : first;
        return { lon: Number(pair[0]), lat: Number(pair[1]) };
      }),
    );
  }
}

export class BatchStream {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  /** XADD (same field layout as `RStream.addWithOptions`); resolves with the id. */
  add(fields: Record<string, any>, id = '*'): Promise<string> {
    const flat: string[] = [];
    for (const [k, v] of Object.entries(fields)) flat.push(this.enc(k), this.enc(v));
    return this.batch.enqueue((p) => p.xadd(this.name, id, ...flat), dec((r) => String(r[0])));
  }
  size(): Promise<number> {
    return this.batch.enqueue((p) => p.xlen(this.name), asCount);
  }
  remove(...ids: string[]): Promise<number> {
    if (ids.length === 0) return Promise.resolve(0);
    return this.batch.enqueue((p) => p.xdel(this.name, ...ids), asCount);
  }
}

export class BatchMapCacheNative<K = any, V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private ek(k: unknown): string {
    return this.codec.encode(k);
  }
  private ev(v: unknown): string {
    return this.codec.encode(v);
  }
  private dv(raw: unknown): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw as string);
    } catch {
      return raw;
    }
  }
  /** HGET old value + HSET (+ HPEXPIRE FIELDS when ttl > 0, Redis ≥ 7.4). */
  put(key: K, value: V, ttlMs = 0): Promise<V | null> {
    const field = this.ek(key);
    return this.batch.enqueue((p) => {
      p.hget(this.name, field);
      p.hset(this.name, field, this.ev(value));
      if (ttlMs > 0) (p as any).hpexpire(this.name, ttlMs, 'FIELDS', 1, field);
    }, dec((r) => this.dv(r[0])));
  }
  fastPut(key: K, value: V, ttlMs = 0): Promise<boolean> {
    const field = this.ek(key);
    return this.batch.enqueue((p) => {
      p.hset(this.name, field, this.ev(value));
      if (ttlMs > 0) (p as any).hpexpire(this.name, ttlMs, 'FIELDS', 1, field);
    }, dec(() => true));
  }
  get(key: K): Promise<V | null> {
    return this.batch.enqueue(
      (p) => p.hget(this.name, this.ek(key)),
      dec((r) => this.dv(r[0])),
    );
  }
  remove(key: K): Promise<V | null> {
    const field = this.ek(key);
    return this.batch.enqueue((p) => {
      p.hget(this.name, field);
      p.hdel(this.name, field);
    }, dec((r) => this.dv(r[0])));
  }
  /** HPTTL FIELDS 1 (live-only; resolves -2 for a missing field). */
  remainTTL(key: K): Promise<number> {
    return this.batch.enqueue(
      (p) => (p as any).hpttl(this.name, 'FIELDS', 1, this.ek(key)),
      dec((r) => {
        const v = Array.isArray(r[0]) ? r[0][0] : r[0];
        return v === null || v === undefined ? -2 : Number(v);
      }),
    );
  }
}

/** Java 4.8 `RedissonBatch.getCountMin(...)`: batched `RCountMin` facade —
 *  the same CMS.* commands src/probabilistic.ts RCountMin issues, queued on
 *  the pipeline; reads resolve when `execute()` runs. Core ops only
 *  (init/add/count/getInfo). */
export class BatchCountMin<V = any> {
  constructor(private batch: RBatch, private name: string, private codec: Codec = new JsonCodec()) {}
  private enc(v: unknown): string {
    return this.codec.encode(v);
  }
  /** Java `init(CountMinInitArgs)`: CMS.INITBYDIM / CMS.INITBYPROB. */
  init(args: CountMinInitArgs): void {
    if (args.byDimensions) {
      this.batch.enqueue((p) => (p as any).call('CMS.INITBYDIM', this.name, args.width, args.depth), () => undefined);
    } else {
      this.batch.enqueue((p) => (p as any).call('CMS.INITBYPROB', this.name, args.errorRate, args.probability), () => undefined);
    }
  }
  /** Java `add(element[, increment])`: CMS.INCRBY — count after increment. */
  add(element: V, increment = 1): Promise<number> {
    return this.batch.enqueue(
      (p) => (p as any).call('CMS.INCRBY', this.name, this.enc(element), String(increment)),
      dec((r) => toNumber(Array.isArray(r[0]) ? r[0][0] : r[0])),
    );
  }
  /** Java `count(element)`: CMS.QUERY. */
  count(element: V): Promise<number> {
    return this.batch.enqueue(
      (p) => (p as any).call('CMS.QUERY', this.name, this.enc(element)),
      dec((r) => toNumber(Array.isArray(r[0]) ? r[0][0] : r[0])),
    );
  }
  /** Java `getInfo()`: CMS.INFO flat fields. */
  getInfo(): Promise<Record<string, any>> {
    return this.batch.enqueue(
      (p) => (p as any).call('CMS.INFO', this.name),
      dec((r) => {
        const out: Record<string, any> = {};
        const flat: any[] = (Array.isArray(r[0]) ? r[0] : []) as any[];
        for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = flat[i + 1];
        return out;
      }),
    );
  }
}

/** Java 4.8 `RedissonBatch.getTimeSeriesNative(...)`: batched
 *  `RTimeSeriesNative` facade — the same TS.* commands
 *  src/probabilistic.ts RTimeSeriesNative issues, queued on the pipeline.
 *  Core ops only (create/add/addCurrent/get/range/rangeReversed/getInfo). */
export class BatchTimeSeriesNative {
  constructor(private batch: RBatch, private name: string) {}
  /** Java `create()` / `create(TSCreateArgs)`: TS.CREATE. */
  create(args?: TSCreateArgs): void {
    const cmdArgs: (string | number)[] = [this.name];
    args?.appendCreationOptions(cmdArgs);
    this.batch.enqueue((p) => (p as any).call('TS.CREATE', ...cmdArgs.map(String)), () => undefined);
  }
  /** Java `add(long, double)` / `add(TSAddArgs)`: TS.ADD — resolves with the
   *  sample timestamp. */
  add(a: number | TSAddArgs, value?: number): Promise<number> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') cmdArgs.push(a, value!);
    else a.appendArgs(cmdArgs);
    return this.batch.enqueue(
      (p) => (p as any).call('TS.ADD', ...cmdArgs.map(String)),
      dec((r) => toNumber(r[0])),
    );
  }
  /** Java `addCurrent(double)`: TS.ADD key * value (server-stamped). */
  addCurrent(value: number): Promise<number> {
    return this.batch.enqueue(
      (p) => (p as any).call('TS.ADD', this.name, '*', String(value)),
      dec((r) => toNumber(r[0])),
    );
  }
  /** Java `get()`: TS.GET — last sample, null when empty. */
  get(): Promise<TSSample | null> {
    return this.batch.enqueue((p) => (p as any).call('TS.GET', this.name), dec((r) => tsToSample(r[0])));
  }
  /** Java `range(long, long)` / `range(TSRangeArgs)`: TS.RANGE. */
  range(a: number | TSRangeArgs, to?: number): Promise<TSSample[]> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') cmdArgs.push(a, to!);
    else a.appendRangeOptions(cmdArgs, []);
    return this.batch.enqueue((p) => (p as any).call('TS.RANGE', ...cmdArgs.map(String)), dec((r) => tsToSamples(r[0])));
  }
  /** Java `rangeReversed(long, long)` / `rangeReversed(TSRangeArgs)`: TS.REVRANGE. */
  rangeReversed(a: number | TSRangeArgs, to?: number): Promise<TSSample[]> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') cmdArgs.push(a, to!);
    else a.appendRangeOptions(cmdArgs, []);
    return this.batch.enqueue((p) => (p as any).call('TS.REVRANGE', ...cmdArgs.map(String)), dec((r) => tsToSamples(r[0])));
  }
  /** Java `getInfo()`: TS.INFO. */
  getInfo(): Promise<TSInfo> {
    return this.batch.enqueue((p) => (p as any).call('TS.INFO', this.name), dec((r) => tsInfoDecode(r[0])));
  }
}
