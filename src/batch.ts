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
import type { Codec } from './codec';
import { JsonCodec } from './codec';
import type { RBatch } from './infra';
import type { ScoredEntry } from './collections';

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
