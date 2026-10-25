/**
 * Buckets / Maps / Sets / Lists / Queues / Sorted sets.
 * Wire: bare name + JSON codec values (map keys/geo members/stream fields JSON-encoded).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import { RedisKeyedStructure } from './synchronizers.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { highwayHash128, isUnsupportedCommand, luaEval, prefixName, sleep, toNumber, withDedicatedRedis } from './util.js';
import { KeyspaceListenerHub, type ObjectListener } from './listeners.js';

/** Java 4.7 `QueueMoveElementsArgs` builder shape: `to(dest)` → `.count(n)` /
 *  `.exactly(n)` → `.bulk()` / `.oneByOne()`. Without an amount selector the
 *  move is a single element. */
export interface QueueMoveElementsArgs {
  dest: string;
  selector?: 'COUNT' | 'EXACTLY';
  count?: number;
  ordering: 'BULK' | 'OBO';
}
export const QueueMoveElementsArgs = {
  to(dest: string) {
    const args: QueueMoveElementsArgs = { dest, ordering: 'BULK' };
    const order = () => ({
      bulk(): QueueMoveElementsArgs { args.ordering = 'BULK'; return args; },
      oneByOne(): QueueMoveElementsArgs { args.ordering = 'OBO'; return args; },
    });
    return {
      count(n: number) { args.selector = 'COUNT'; args.count = n; return order(); },
      exactly(n: number) { args.selector = 'EXACTLY'; args.count = n; return order(); },
    };
  },
};

export class RBucket<V = any> extends RedisDataStructure {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.listenerHub = new KeyspaceListenerHub(redis, name, {
      onSet: 'set',
      onExpired: 'expired',
      onDeleted: 'del',
    });
  }
  async get(): Promise<V | null> {
    const raw = await this.redis.get(this.name);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async set(value: V, ttlMs?: number): Promise<void> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) await this.redis.set(this.name, enc, 'PX', ttlMs);
    else await this.redis.set(this.name, enc);
  }
  async setAndKeepTTL(value: V): Promise<void> {
    const enc = this.encodeValue(value);
    try {
      await this.redis.set(this.name, enc, 'KEEPTTL');
    } catch {
      await this.redis.set(this.name, enc);
    }
  }
  async trySet(value: V, ttlMs?: number): Promise<boolean> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) {
      const r = await this.redis.set(this.name, enc, 'PX', ttlMs, 'NX');
      return r === 'OK';
    }
    return ((await this.redis.set(this.name, enc, 'NX')) as unknown) === 'OK';
  }
  async setIfExists(value: V, ttlMs?: number): Promise<boolean> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) {
      const r = await this.redis.set(this.name, enc, 'PX', ttlMs, 'XX');
      return r === 'OK';
    }
    return ((await this.redis.set(this.name, enc, 'XX')) as unknown) === 'OK';
  }
  async getAndSet(value: V, ttlMs?: number): Promise<V | null> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) {
      const s = `local old = redis.call('get', KEYS[1])
redis.call('set', KEYS[1], ARGV[1], 'px', ARGV[2])
return old`;
      const raw = await luaEval(this.redis, s, 1, this.name, enc, String(ttlMs));
      return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
    }
    const raw = await this.redis.getset(this.name, enc);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async getAndDelete(): Promise<V | null> {
    const raw = await (this.redis as any).getdel?.(this.name);
    if (raw !== undefined) return raw === null ? null : (this.decodeValue(raw) as V);
    const v = await this.get();
    if (v !== null) await this.redis.del(this.name);
    return v;
  }
  async compareAndSet(expect: V, update: V): Promise<boolean> {
    const s = `local cur = redis.call('get', KEYS[1])
if cur == ARGV[1] then redis.call('set', KEYS[1], ARGV[2]); return 1 end
return 0`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeValue(expect), this.encodeValue(update))) === 1;
  }
  async compareAndDelete(expect: V): Promise<boolean> {
    const s = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeValue(expect))) === 1;
  }
  async size(): Promise<number> {
    return this.redis.strlen(this.name);
  }
  async getAndExpire(ttlMs: number): Promise<V | null> {
    try {
      const raw = await this.redis.getex(this.name, 'PX', ttlMs);
      return raw === null ? null : (this.decodeValue(raw) as V);
    } catch {
      const v = await this.get();
      if (v !== null) await this.redis.pexpire(this.name, ttlMs);
      return v;
    }
  }
  /** Java `addListener(ObjectListener)`: onSet → `set`, plus expired/del. */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
  override async delete(): Promise<boolean> {
    return super.delete();
  }
  override async isExists(): Promise<boolean> {
    return super.isExists();
  }
}

export class RBuckets {
  constructor(private redis: Redis, private codec: Codec = new JsonCodec()) {}
  async get<V = any>(...keys: string[]): Promise<Map<string, V>> {
    if (keys.length === 0) return new Map();
    const vals = await this.redis.mget(...keys);
    const out = new Map<string, V>();
    keys.forEach((k, i) => {
      const raw = vals[i];
      if (raw !== null && raw !== undefined) {
        try {
          out.set(k, this.codec.decode(raw) as V);
        } catch {
          out.set(k, raw as unknown as V);
        }
      }
    });
    return out;
  }
  async set(mapping: Record<string, unknown>, ttlMs?: number): Promise<void> {
    const entries = Object.entries(mapping);
    if (ttlMs && ttlMs > 0) {
      const pipe = (this.redis as any).pipeline?.() ?? (this.redis as any).multi?.();
      if (pipe) {
        for (const [k, v] of entries) pipe.set(k, this.codec.encode(v), 'PX', ttlMs);
        await pipe.exec();
        return;
      }
    }
    if (entries.length === 0) return;
    const flat: string[] = [];
    for (const [k, v] of entries) flat.push(k, this.codec.encode(v));
    await this.redis.mset(...flat);
  }
  async trySet(mapping: Record<string, unknown>): Promise<boolean> {
    const entries = Object.entries(mapping);
    if (entries.length === 0) return true;
    const flat: string[] = [];
    for (const [k, v] of entries) flat.push(k, this.codec.encode(v));
    return ((await this.redis.msetnx(...flat)) as number) === 1;
  }
  /** Java `setIfAllKeysExist(SetArgs)`: MSETEX ... XX — all keys must already
   *  exist, else nothing is written. Requires Redis 8+ (MSETEX). */
  async setIfAllKeysExist(mapping: Record<string, unknown>, ttlMs?: number): Promise<boolean> {
    return this.setIfAll('XX', mapping, ttlMs);
  }
  /** Java `setIfAllKeysAbsent(SetArgs)`: MSETEX ... NX. Requires Redis 8+. */
  async setIfAllKeysAbsent(mapping: Record<string, unknown>, ttlMs?: number): Promise<boolean> {
    return this.setIfAll('NX', mapping, ttlMs);
  }
  private async setIfAll(mode: 'NX' | 'XX', mapping: Record<string, unknown>, ttlMs?: number): Promise<boolean> {
    const entries = Object.entries(mapping);
    if (entries.length === 0) return false;
    const args: (string | number)[] = [entries.length];
    for (const [k, v] of entries) args.push(k, this.codec.encode(v));
    args.push(mode);
    if (ttlMs && ttlMs > 0) args.push('PX', ttlMs);
    try {
      return toNumber(await (this.redis as any).msetex(...args)) === 1;
    } catch (e) {
      // Missing on Redis < 8 and on ioredis-mock (the method itself is absent).
      if (/unknown command|unsupported|is not a function/i.test(String((e as Error)?.message ?? e))) {
        throw new Error('redi.ts: setIfAllKeysExist/Absent requires Redis 8+ (MSETEX)');
      }
      throw e;
    }
  }
}

export class RMap<K = any, V = any> extends RedisKeyedStructure<K> {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.listenerHub = new KeyspaceListenerHub(redis, name, {
      onPut: 'hset',
      onIncrement: 'hincrbyfloat',
      onRemove: 'hdel',
    });
  }
  async get(key: K): Promise<V | null> {
    const raw = await this.redis.hget(this.name, this.encodeKey(key));
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async getAll(keys: K[]): Promise<Map<K, V>> {
    const out = new Map<K, V>();
    if (keys.length === 0) return out;
    const raws = await this.redis.hmget(this.name, ...keys.map((k) => this.encodeKey(k)));
    keys.forEach((k, i) => {
      const raw = raws[i];
      if (raw !== null && raw !== undefined) out.set(k, this.decodeValue(raw) as V);
    });
    return out;
  }
  async put(key: K, value: V): Promise<V | null> {
    // Single Lua (was GET+HSET): atomic old-value return under concurrency.
    const s = `local old = redis.call('hget', KEYS[1], ARGV[1])
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
return old`;
    const raw = await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), this.encodeValue(value));
    return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
  }
  async fastPut(key: K, value: V): Promise<boolean> {
    return ((await this.redis.hset(this.name, this.encodeKey(key), this.encodeValue(value))) as number) === 1;
  }
  async putIfAbsent(key: K, value: V): Promise<V | null> {
    // Single Lua: HSETNX + old-value return without a TOCTOU window.
    const s = `if redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]) == 1 then
return nil else return redis.call('hget', KEYS[1], ARGV[1]) end`;
    const raw = await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), this.encodeValue(value));
    return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
  }
  async putIfExists(key: K, value: V): Promise<V | null> {
    const ek = this.encodeKey(key);
    const s = `if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
local old = redis.call('hget', KEYS[1], ARGV[1])
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
return old else return nil end`;
    const raw = await luaEval(this.redis, s, 1, this.name, ek, this.encodeValue(value));
    return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
  }
  async fastPutIfAbsent(key: K, value: V): Promise<boolean> {
    return ((await this.redis.hsetnx(this.name, this.encodeKey(key), this.encodeValue(value))) as number) === 1;
  }
  async fastPutIfExists(key: K, value: V): Promise<boolean> {
    const s = `if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); return 1 else return 0 end`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), this.encodeValue(value))) === 1;
  }
  async fastReplace(key: K, value: V): Promise<boolean> {
    return this.fastPutIfExists(key, value);
  }
  async replace(key: K, value: V): Promise<V | null>;
  async replace(key: K, oldValue: V, newValue: V): Promise<boolean>;
  async replace(key: K, a: V, b?: V): Promise<V | null | boolean> {
    if (b === undefined) return this.putIfExists(key, a);
    return this.replaceWithCheck(key, a, b);
  }
  async replaceWithCheck(key: K, oldValue: V, newValue: V): Promise<boolean> {
    const s = `local cur = redis.call('hget', KEYS[1], ARGV[1])
if cur == ARGV[2] then redis.call('hset', KEYS[1], ARGV[1], ARGV[3]); return 1 else return 0 end`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), this.encodeValue(oldValue), this.encodeValue(newValue))) === 1;
  }
  async remove(key: K, value?: V): Promise<V | null | boolean> {
    if (value === undefined) {
      // Single Lua (was GET+HDEL): atomic old-value return under concurrency.
      const s = `local old = redis.call('hget', KEYS[1], ARGV[1])
redis.call('hdel', KEYS[1], ARGV[1])
return old`;
      const raw = await luaEval(this.redis, s, 1, this.name, this.encodeKey(key));
      return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
    }
    const s = `local cur = redis.call('hget', KEYS[1], ARGV[1])
if cur == ARGV[2] then return redis.call('hdel', KEYS[1], ARGV[1]) else return 0 end`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), this.encodeValue(value))) === 1;
  }
  async fastRemove(...keys: K[]): Promise<number> {
    if (keys.length === 0) return 0;
    return this.redis.hdel(this.name, ...keys.map((k) => this.encodeKey(k)));
  }
  async containsKey(key: K): Promise<boolean> {
    return ((await this.redis.hexists(this.name, this.encodeKey(key))) as number) === 1;
  }
  async containsValue(value: V): Promise<boolean> {
    const enc = this.encodeValue(value);
    let cursor = '0';
    do {
      const [next, fields] = (await this.redis.hscan(this.name, cursor)) as [string, string[]];
      for (let i = 1; i < fields.length; i += 2) if (fields[i] === enc) return true;
      cursor = next;
    } while (cursor !== '0');
    return false;
  }
  async size(): Promise<number> {
    return this.redis.hlen(this.name);
  }
  async readAllMap(): Promise<Map<K, V>> {
    const all = await this.redis.hgetall(this.name);
    const out = new Map<K, V>();
    for (const [k, v] of Object.entries(all)) {
      try {
        out.set(this.decodeKey(k) as K, this.decodeValue(v) as V);
      } catch {
        out.set(k as unknown as K, v as unknown as V);
      }
    }
    return out;
  }
  async readAllKeySet(): Promise<K[]> {
    return [...(await this.readAllMap()).keys()];
  }
  async readAllValues(): Promise<V[]> {
    return [...(await this.readAllMap()).values()];
  }
  async readAllEntrySet(): Promise<[K, V][]> {
    return [...(await this.readAllMap()).entries()];
  }
  /** Java `keySet()` views: no args, `keySet(count)`, or `keySet(pattern[, count])`. */
  async keySet(): Promise<K[]>;
  async keySet(count: number): Promise<K[]>;
  async keySet(pattern: string, count?: number): Promise<K[]>;
  async keySet(patternOrCount?: string | number, count = 100): Promise<K[]> {
    const pattern = typeof patternOrCount === 'string' ? patternOrCount : '*';
    const c = typeof patternOrCount === 'number' ? patternOrCount : count;
    const out: K[] = [];
    for await (const k of this.keysIterator(pattern, c)) out.push(k);
    return out;
  }
  /** Java `values()` views. */
  async values(): Promise<V[]>;
  async values(count: number): Promise<V[]>;
  async values(pattern: string, count?: number): Promise<V[]>;
  async values(patternOrCount?: string | number, count = 100): Promise<V[]> {
    const pattern = typeof patternOrCount === 'string' ? patternOrCount : '*';
    const c = typeof patternOrCount === 'number' ? patternOrCount : count;
    const out: V[] = [];
    for await (const v of this.valuesIterator(pattern, c)) out.push(v);
    return out;
  }
  /** Java `entrySet()` views (TS returns [key, value] pairs). */
  async entrySet(): Promise<[K, V][]>;
  async entrySet(count: number): Promise<[K, V][]>;
  async entrySet(pattern: string, count?: number): Promise<[K, V][]>;
  async entrySet(patternOrCount?: string | number, count = 100): Promise<[K, V][]> {
    const pattern = typeof patternOrCount === 'string' ? patternOrCount : '*';
    const c = typeof patternOrCount === 'number' ? patternOrCount : count;
    const out: [K, V][] = [];
    for await (const e of this.entriesIterator(pattern, c)) out.push(e);
    return out;
  }
  /** Java `RMap.valueSize(K)`: byte length of the encoded value (HSTRLEN). */
  async valueSize(key: K): Promise<number> {
    return toNumber(await this.redis.hstrlen(this.name, this.encodeKey(key)));
  }
  /** Java `RMap.randomKeys(int)`: HRANDFIELD; count > 0 returns distinct keys. */
  async randomKeys(count = 1): Promise<K[]> {
    const raw = (await this.redis.hrandfield(this.name, count)) as string[] | string | null;
    const arr = raw === null || raw === undefined ? [] : Array.isArray(raw) ? raw : [raw];
    return arr.map((k) => this.decodeKey(k) as K);
  }
  /** Java `RMap.randomEntries(int)`: HRANDFIELD ... WITHVALUES. */
  async randomEntries(count = 1): Promise<Map<K, V>> {
    const raw = (await this.redis.hrandfield(this.name, count, 'WITHVALUES')) as string[] | null;
    const out = new Map<K, V>();
    if (!raw) return out;
    for (let i = 0; i + 1 < raw.length; i += 2) {
      out.set(this.decodeKey(raw[i]) as K, this.decodeValue(raw[i + 1]) as V);
    }
    return out;
  }
  /** Lazy HSCAN iterator over keys (Java `RMap.keySet(pattern, count)` view). */
  async *keysIterator(pattern = '*', count = 100): AsyncGenerator<K> {
    let cursor = '0';
    do {
      const [next, fields] = (await this.redis.hscan(
        this.name, cursor, 'MATCH', pattern, 'COUNT', String(count),
      )) as [string, string[]];
      for (let i = 0; i < fields.length; i += 2) yield this.decodeKey(fields[i]) as K;
      cursor = next;
    } while (cursor !== '0');
  }
  /** Lazy HSCAN iterator over values (Java `RMap.values(pattern, count)` view). */
  async *valuesIterator(pattern = '*', count = 100): AsyncGenerator<V> {
    let cursor = '0';
    do {
      const [next, fields] = (await this.redis.hscan(
        this.name, cursor, 'MATCH', pattern, 'COUNT', String(count),
      )) as [string, string[]];
      for (let i = 1; i < fields.length; i += 2) yield this.decodeValue(fields[i]) as V;
      cursor = next;
    } while (cursor !== '0');
  }
  /** Lazy HSCAN iterator over entries (Java `RMap.entrySet(pattern, count)` view). */
  async *entriesIterator(pattern = '*', count = 100): AsyncGenerator<[K, V]> {
    let cursor = '0';
    do {
      const [next, fields] = (await this.redis.hscan(
        this.name, cursor, 'MATCH', pattern, 'COUNT', String(count),
      )) as [string, string[]];
      for (let i = 0; i + 1 < fields.length; i += 2) {
        yield [this.decodeKey(fields[i]) as K, this.decodeValue(fields[i + 1]) as V];
      }
      cursor = next;
    } while (cursor !== '0');
  }
  /** Java `putAll(Map)` / `putAll(Map, batchSize)`: HSET in field batches. */
  async putAll(mapping: Record<string, V> | Map<K, V>, batchSize?: number): Promise<void> {
    if (batchSize !== undefined && (!Number.isInteger(batchSize) || batchSize <= 0)) {
      throw new Error('batchSize must be a positive integer');
    }
    const entries = mapping instanceof Map ? [...mapping.entries()] : Object.entries(mapping);
    if (entries.length === 0) return;
    const size = batchSize ?? 1000;
    for (let i = 0; i < entries.length; i += size) {
      const flat: string[] = [];
      for (const [k, v] of entries.slice(i, i + size) as [K, V][]) {
        flat.push(this.encodeKey(k), this.encodeValue(v));
      }
      await this.redis.hset(this.name, ...flat as [string, string]);
    }
  }
  async addAndGet(key: K, delta: number): Promise<number> {
    const s = `local cur = redis.call('hget', KEYS[1], ARGV[1])
local n = 0
if cur ~= false then n = tonumber(cur) or 0 end
n = n + tonumber(ARGV[2])
redis.call('hset', KEYS[1], ARGV[1], tostring(n))
return tostring(n)`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeKey(key), String(delta)));
  }
  async clear(): Promise<void> {
    await this.redis.del(this.name);
  }
  /** Java `addListener(ObjectListener)`: MapPut/MapIncr/MapRemove events from
   *  hset/hincrbyfloat/hdel keyspace notifications. Field is null on servers
   *  without `__subkeyevent` (the Java fallback path). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  /** Java `removeListener(int)`; unknown ids are ignored. */
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
  override async delete(): Promise<boolean> {
    return super.delete();
  }
}

// -- RMaps (Java 4.8 mass map operations) -------------------------------------------------
//
// Bulk writer for many same-shaped Map objects: maps sharing one set of field
// names are written declaring those names once (Redis keeps a single copy).
// Preferred path is the HIMPORT command (unknown on plain Redis — detected
// once, then permanently falling back); the fallback is a pipelined
// DEL + HSET per object, which stores a plain HASH with codec-encoded
// field names and values — byte-identical to what RMap writes and reads.

/** Java 4.8 `MapsImportArgs`: `fields(...)` with the shared field names, then
 *  optional `batchSize(n)` (rows buffered before an automatic flush). */
export class MapsImportArgs<K = any> {
  private batchSizeValue?: number;
  private constructor(readonly fields: K[]) {}
  static fields<K>(...fields: K[]): MapsImportArgs<K> {
    return new MapsImportArgs<K>(fields);
  }
  batchSize(batchSize: number): this {
    this.batchSizeValue = batchSize;
    return this;
  }
  getBatchSize(): number | undefined {
    return this.batchSizeValue;
  }
}

/** Field-set name: `"rs" + %08x` of the low 32 bits of the HighwayHash-128
 *  (Redisson key) of the length-prefixed encoded fields — verbatim Java
 *  `RedissonMapsImport.fieldsetName` / `fieldsHash`. */
function mapsFieldHashName(encodedFields: string[]): string {
  const buffers = encodedFields.map((f) => Buffer.from(f, 'utf8'));
  const lengths = Buffer.alloc(buffers.length * 4);
  buffers.forEach((b, i) => lengths.writeUInt32BE(b.length, i * 4));
  const [h1] = highwayHash128(Buffer.concat([lengths, ...buffers]));
  const low32 = Number(h1 & 0xffffffffn) >>> 0;
  return 'rs' + low32.toString(16).padStart(8, '0');
}

/** Java byte[] comparison (unsigned, length as tiebreak) — orders encoded
 *  fields within one object deterministically, like encodeSortedByField. */
function compareBuffers(left: Buffer, right: Buffer): number {
  const n = Math.min(left.length, right.length);
  for (let i = 0; i < n; i++) {
    const diff = left[i] - right[i];
    if (diff !== 0) return diff;
  }
  return left.length - right.length;
}

/** Process-wide mirror of Java ServiceManager.isHashImportDisabled(): once a
 *  server answers HIMPORT with unknown-command (or the client has no `.call`,
 *  e.g. ioredis-mock), every import falls back to DEL+HSET. */
let mapsHashImportDisabled = false;

interface MapsImportRow { name: string; values: string[] }

/** Java 4.8 `RMapsImport`: buffered import session for Map objects sharing
 *  the field names given at creation. Values are matched to the fields by
 *  position; rows flush when the buffer reaches batchSize and on flush(). */
export class RMapsImport<K = any, V = any> {
  private readonly fieldsetName: string;
  private buffer: MapsImportRow[] = [];
  private importedCountValue = 0;

  /** @internal built by RMaps.createImport / RMaps.set — fields are already encoded. */
  constructor(private redis: Redis, private codec: Codec, private fields: string[], private batchSize = 500) {
    if (fields.length === 0) throw new Error("redi.ts: fields can't be empty");
    for (let i = 0; i < fields.length; i++) {
      for (let j = i + 1; j < fields.length; j++) {
        if (fields[i] === fields[j]) throw new Error('redi.ts: field name duplication detected');
      }
    }
    this.fieldsetName = mapsFieldHashName(fields);
  }

  /** Java `add(String, V...)`: encode now, write when the buffer fills. */
  async add(name: string, ...values: V[]): Promise<void> {
    if (values.length !== this.fields.length) {
      throw new Error(`redi.ts: amount of values ${values.length} doesn't match amount of fields ${this.fields.length}`);
    }
    this.buffer.push({ name, values: values.map((v) => this.codec.encode(v)) });
    if (this.buffer.length >= this.batchSize) await this.flush();
  }

  /** @internal queue pre-encoded values (RMaps.set path). */
  addEncoded(name: string, values: string[]): void {
    this.buffer.push({ name, values });
  }

  /** Java `flush()`: writes every buffered row (batchSize rows per round trip). */
  async flush(): Promise<void> {
    while (this.buffer.length > 0) {
      const rows = this.buffer.slice(0, this.batchSize);
      this.buffer = this.buffer.slice(rows.length);
      await this.write(rows);
      this.importedCountValue += rows.length;
    }
  }

  /** Java `getImportedCount()`. */
  getImportedCount(): number {
    return this.importedCountValue;
  }

  private async write(rows: MapsImportRow[]): Promise<void> {
    if (mapsHashImportDisabled) return this.writeWithHset(rows);
    try {
      const res = await this.writeWithHashImport(rows);
      // pipelines resolve even when individual commands fail — on a cluster
      // the error object sits inside the results (exec never rejects there),
      // so an unknown HIMPORT or a CROSSSLOT silently wrote NOTHING
      for (const err of execErrors(res)) {
        if (!isUnsupportedCommand(err) && !/CROSSSLOT/i.test(String((err as Error).message ?? err))) throw err;
        mapsHashImportDisabled = true;
        return this.writeWithHset(rows);
      }
    } catch (e) {
      // Unknown HIMPORT (plain Redis) or a client without `.call` (ioredis-mock
      // pipelines) — permanently fall back to the plain-HASH path.
      if (!isUnsupportedCommand(e)) throw e;
      mapsHashImportDisabled = true;
      await this.writeWithHset(rows);
    }
  }

  /** HIMPORT PREPARE fieldsetName f...; then HIMPORT SET name fieldsetName v... per row.
   *  Note: the fieldset key has no hash-tag (Java `rs%08x` naming parity), so
   *  this fast path is effectively single-node; clusters fall back to HSET. */
  private async writeWithHashImport(rows: MapsImportRow[]): Promise<[unknown, unknown][]> {
    const pipe = (this.redis as any).pipeline?.() ?? (this.redis as any).multi?.();
    if (!pipe) throw new Error('redi.ts: redis client does not support pipeline');
    pipe.call('HIMPORT', 'PREPARE', this.fieldsetName, ...this.fields);
    for (const row of rows) pipe.call('HIMPORT', 'SET', row.name, this.fieldsetName, ...row.values);
    return await pipe.exec();
  }

  /** DEL name; HSET name f1 v1 f2 v2 ... — one plain HASH per object. */
  private async writeWithHset(rows: MapsImportRow[]): Promise<void> {
    const pipe = (this.redis as any).pipeline?.() ?? (this.redis as any).multi?.();
    if (!pipe) throw new Error('redi.ts: redis client does not support pipeline');
    for (const row of rows) {
      pipe.del(row.name);
      const params: string[] = [row.name];
      for (let i = 0; i < row.values.length; i++) params.push(this.fields[i], row.values[i]);
      pipe.hset(...params as [string, string]);
    }
    const res = await pipe.exec();
    for (const err of execErrors(res)) throw err;
  }
}

/** Per-command errors from a pipeline exec(): ioredis pairs [err, reply];
 *  ioredis Cluster pipelines instead put the error object in the slot. */
function execErrors(res: unknown): unknown[] {
  const out: unknown[] = [];
  if (!Array.isArray(res)) return out;
  for (const entry of res) {
    if (Array.isArray(entry)) {
      if (entry[0]) out.push(entry[0]);
    } else if (entry && typeof entry === 'object' && (entry as any).command) {
      out.push(entry);
    }
  }
  return out;
}

/** Java 4.8 `RMaps<K, V>`: mass operations with Map objects. Every object is
 *  stored as a plain HASH under its own name (RMap-compatible wire); objects
 *  sharing a field set are written in one import group. */
export class RMaps<K = any, V = any> {
  private static readonly DEFAULT_BATCH_SIZE = 500;

  constructor(private redis: Redis, private codec: Codec = new JsonCodec()) {}

  /** Java `set(Map<String, Map<K, V>>)` / `set(Map, int)`: each object replaces
   *  the whole Map stored under the same name; fields within a group are
   *  sorted by their encoded bytes (encodeSortedByField). */
  async set(maps: Map<string, Map<K, V>> | Record<string, Record<string, V>>): Promise<void>;
  async set(maps: Map<string, Map<K, V>> | Record<string, Record<string, V>>, batchSize: number): Promise<void>;
  async set(maps: Map<string, Map<K, V>> | Record<string, Record<string, V>>, batchSize?: number): Promise<void> {
    if (batchSize !== undefined && (!Number.isInteger(batchSize) || batchSize <= 0)) {
      throw new Error('redi.ts: batchSize should be greater than zero');
    }
    const size = batchSize ?? RMaps.DEFAULT_BATCH_SIZE;
    const objects = maps instanceof Map ? [...maps.entries()] : Object.entries(maps);
    if (objects.length === 0) return;

    // group objects by their encoded (sorted) field set — insertion ordered
    const groups = new Map<string, { fields: string[]; rows: MapsImportRow[] }>();
    for (const [name, map] of objects) {
      const entries = map instanceof Map ? [...map.entries() as Iterable<[K, V]>] : (Object.entries(map) as [K, V][]);
      if (entries.length === 0) throw new Error(`redi.ts: Map object ${name} can't be empty`);
      const encoded = entries.map(([k, v]) => [this.codec.encode(k), this.codec.encode(v)] as [string, string]);
      encoded.sort((a, b) => compareBuffers(Buffer.from(a[0], 'utf8'), Buffer.from(b[0], 'utf8')));
      const fields = encoded.map((e) => e[0]);
      const groupKey = JSON.stringify(fields);
      let group = groups.get(groupKey);
      if (!group) {
        group = { fields, rows: [] };
        groups.set(groupKey, group);
      }
      group.rows.push({ name, values: encoded.map((e) => e[1]) });
    }

    for (const group of groups.values()) {
      const imp = new RMapsImport<K, V>(this.redis, this.codec, group.fields, size);
      for (const row of group.rows) imp.addEncoded(row.name, row.values);
      await imp.flush();
    }
  }

  /** Java `createImport(MapsImportArgs)`: an import session for more objects
   *  than fit in memory — added one by one, flushed per batchSize/flush(). */
  createImport(args: MapsImportArgs<K>): RMapsImport<K, V> {
    return new RMapsImport<K, V>(this.redis, this.codec, args.fields.map((f) => this.codec.encode(f)), args.getBatchSize() ?? RMaps.DEFAULT_BATCH_SIZE);
  }
}


export class RSet<V = any> extends RedisKeyedStructure<V> {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.listenerHub = new KeyspaceListenerHub(redis, name, {
      onAdd: 'sadd',
      onRemove: 'srem',
      onRandomRemove: 'spop',
      onStore: ['sinterstore', 'sunionstore', 'sdiffstore'],
    });
  }
  async add(...values: V[]): Promise<number> {
    if (values.length === 0) return 0;
    return this.redis.sadd(this.name, ...values.map((v) => this.encodeValue(v)));
  }
  async addAll(values: V[]): Promise<number> {
    return this.add(...values);
  }
  async remove(...values: V[]): Promise<number> {
    if (values.length === 0) return 0;
    return this.redis.srem(this.name, ...values.map((v) => this.encodeValue(v)));
  }
  async contains(value: V): Promise<boolean> {
    return ((await this.redis.sismember(this.name, this.encodeValue(value))) as number) === 1;
  }
  async containsAll(...values: V[]): Promise<number> {
    if (values.length === 0) return 0;
    const r = await this.redis.smismember(this.name, ...values.map((v) => this.encodeValue(v)));
    return (r as number[]).filter((x) => x === 1).length;
  }
  /** Java `containsEach(Collection)`: the subset that IS present. */
  async containsEach(values: V[]): Promise<V[]> {
    if (values.length === 0) return [];
    const r = (await this.redis.smismember(this.name, ...values.map((v) => this.encodeValue(v)))) as number[];
    return values.filter((_, i) => r[i] === 1);
  }
  /** Java `addAllCounted(Collection)`: SADD count. */
  async addAllCounted(values: V[]): Promise<number> {
    return this.add(...values);
  }
  /** Java `removeAllCounted(Collection)`: SREM count. */
  async removeAllCounted(values: V[]): Promise<number> {
    return this.remove(...values);
  }
  async size(): Promise<number> {
    return this.redis.scard(this.name);
  }
  async readAll(): Promise<V[]> {
    const m = await this.redis.smembers(this.name);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async random(count = 1): Promise<V[]> {
    if (count === 1) {
      const v = await this.redis.srandmember(this.name);
      return v === null ? [] : [this.decodeValue(v) as V];
    }
    const m = await this.redis.srandmember(this.name, count);
    return (m as string[]).map((x) => this.decodeValue(x) as V);
  }
  async removeRandom(count = 1): Promise<V[]> {
    if (count <= 1) {
      const v = await this.redis.spop(this.name);
      return v === null ? [] : [this.decodeValue(v) as V];
    }
    const m = await this.redis.spop(this.name, count);
    return ((m as string[]) ?? []).map((x) => this.decodeValue(x) as V);
  }
  /**
   * Store variants mirror Redisson 4.6.1 exactly: SELF is only the
   * DESTINATION (overwritten by the result) and is NOT one of the inputs.
   * Command shape is `SOPSTORE self others...` — bare keys, no numkeys
   * (a stray numkeys of the zset/SINTERCARD kind would be parsed as a key
   * name and silently corrupt the result). Semantics pinned by the live
   * probe against the 4.6.1 jar; the read-only counterparts below DO include
   * SELF as first input, like Java's read* variants.
   */
  async union(...names: string[]): Promise<number> {
    return toNumber(await this.redis.sunionstore(this.name, ...names));
  }
  async diff(...names: string[]): Promise<number> {
    return toNumber(await this.redis.sdiffstore(this.name, ...names));
  }
  async intersection(...names: string[]): Promise<number> {
    return toNumber(await this.redis.sinterstore(this.name, ...names));
  }
  async readUnion(...otherKeys: string[]): Promise<V[]> {
    const m = await this.redis.sunion(this.name, ...otherKeys);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readDiff(...otherKeys: string[]): Promise<V[]> {
    const m = await this.redis.sdiff(this.name, ...otherKeys);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readIntersection(...otherKeys: string[]): Promise<V[]> {
    const m = await this.redis.sinter(this.name, ...otherKeys);
    return m.map((x) => this.decodeValue(x) as V);
  }
  /** Cardinality of the union without storing (SUNIONCARD — Redis 8.10+,
   * matching Java 4.6.1 RedisCommands.SUNIONCARD_INT; `numkeys` IS required
   * here, unlike the store variants above). */
  async countUnion(...names: string[]): Promise<number> {
    return toNumber(await this.redis.call('SUNIONCARD', 1 + names.length, this.name, ...names));
  }
  /** Java 4.7 `countUnionApprox(names...)` / `(limit, names...)`:
   *  SUNIONCARD with APPROX (HyperLogLog estimate above 100k). */
  async countUnionApprox(...names: string[]): Promise<number>;
  async countUnionApprox(limit: number, ...names: string[]): Promise<number>;
  async countUnionApprox(a: string | number, ...rest: string[]): Promise<number> {
    const limit = typeof a === 'number' ? a : 0;
    const names = typeof a === 'number' ? rest : [a, ...rest];
    const args: (string | number)[] = ['SUNIONCARD', 1 + names.length, this.name, ...names, 'APPROX'];
    if (limit > 0) args.push('LIMIT', limit);
    return toNumber(await (this.redis as any).call(...args));
  }
  /** Java 4.7 `countDiff(names...)` / `(limit, names...)`: SDIFFCARD. */
  async countDiff(...names: string[]): Promise<number>;
  async countDiff(limit: number, ...names: string[]): Promise<number>;
  async countDiff(a: string | number, ...rest: string[]): Promise<number> {
    const limit = typeof a === 'number' ? a : 0;
    const names = typeof a === 'number' ? rest : [a, ...rest];
    const args: (string | number)[] = ['SDIFFCARD', 1 + names.length, this.name, ...names];
    if (limit > 0) args.push('LIMIT', limit);
    return toNumber(await (this.redis as any).call(...args));
  }
  /** Java `countIntersection(names...)` / `countIntersection(limit, names...)`:
   *  SINTERCARD with an optional LIMIT (0 = unlimited), with a client-side
   *  SMEMBERS fallback for mocks/older Redis. */
  async countIntersection(...names: string[]): Promise<number>;
  async countIntersection(limit: number, ...names: string[]): Promise<number>;
  async countIntersection(a?: string | number, ...rest: string[]): Promise<number> {
    const limit = typeof a === 'number' ? a : 0;
    const names = a === undefined ? [] : (typeof a === 'number' ? rest : [a, ...rest]);
    const keys = [this.name, ...names];
    const args: (string | number)[] = [keys.length, ...keys];
    if (limit > 0) args.push('LIMIT', limit);
    try {
      return toNumber(await (this.redis as any).call('SINTERCARD', ...args));
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      const sets = await Promise.all(keys.map((k) => this.redis.smembers(k)));
      const others = sets.slice(1).map((s) => new Set(s));
      const count = sets[0].filter((m) => others.every((s) => s.has(m))).length;
      return limit > 0 ? Math.min(count, limit) : count;
    }
  }
  async move(dest: string, value: V): Promise<boolean> {
    return ((await this.redis.smove(this.name, dest, this.encodeValue(value))) as number) === 1;
  }
  async tryAdd(...values: V[]): Promise<boolean> {
    const s = `for i, v in ipairs(ARGV) do
if redis.call('sismember', KEYS[1], v) == 1 then return 0 end
end
redis.call('sadd', KEYS[1], unpack(ARGV))
return 1`;
    if (values.length === 0) return true;
    return toNumber(await luaEval(this.redis, s, 1, this.name, ...values.map((v) => this.encodeValue(v)))) === 1;
  }
  /** Java `iterator(pattern, count)` / `stream`: lazy SSCAN generator.
   *  MATCH applies to encoded members (same as HSCAN field semantics). */
  async *iterator(pattern = '*', count = 100): AsyncGenerator<V> {
    let cursor = '0';
    do {
      const [next, members] = (await this.redis.sscan(
        this.name, cursor, 'MATCH', pattern, 'COUNT', String(count),
      )) as [string, string[]];
      for (const m of members) yield this.decodeValue(m) as V;
      cursor = next;
    } while (cursor !== '0');
  }
  async *distributedIterator(pattern = '*', count = 100): AsyncGenerator<V> {
    yield* this.iterator(pattern, count);
  }
  async *stream(pattern = '*', count = 100): AsyncGenerator<V> {
    yield* this.iterator(pattern, count);
  }
  /** Java `addListener(ObjectListener)`: sadd/srem/spop and store events.
   *  Requires K/E keyspace notifications (the same server dependency as Java). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  /** Java `removeListener(int)`; unknown ids are ignored. */
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
}

export class RList<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async add(...values: V[]): Promise<number> {
    if (values.length === 0) return this.size();
    return this.redis.rpush(this.name, ...values.map((v) => this.encodeValue(v)));
  }
  async addAt(index: number, ...values: V[]): Promise<void> {
    if (values.length === 0) return;
    if (index === 0) {
      await this.redis.lpush(this.name, ...[...values].reverse().map((v) => this.encodeValue(v)));
      return;
    }
    const s = `local len = redis.call('llen', KEYS[1])
local idx = tonumber(ARGV[1])
if idx < 0 then idx = len + idx end
if idx < 0 or idx > len then return redis.error_reply('index out of bounds') end
local tail = redis.call('lrange', KEYS[1], idx, -1)
redis.call('ltrim', KEYS[1], 0, idx - 1)
for i = 2, #ARGV do redis.call('rpush', KEYS[1], ARGV[i]) end
for i, v in ipairs(tail) do redis.call('rpush', KEYS[1], v) end
return 1`;
    await luaEval(this.redis, s, 1, this.name, String(index), ...values.map((v) => this.encodeValue(v)));
  }
  async get(index: number): Promise<V | null> {
    const raw = await this.redis.lindex(this.name, index);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async set(index: number, value: V): Promise<void> {
    await this.redis.lset(this.name, index, this.encodeValue(value));
  }
  /** Java `addBefore(V existingElement, V newElement)`: LINSERT BEFORE —
   *  the new list length, or -1 when the pivot is absent (same as Java). */
  async addBefore(existingElement: V, newElement: V): Promise<number> {
    return toNumber(await (this.redis as any).linsert(
      this.name, 'BEFORE', this.encodeValue(existingElement), this.encodeValue(newElement)), -1);
  }
  /** Java `addAfter(V existingElement, V newElement)`: LINSERT AFTER. */
  async addAfter(existingElement: V, newElement: V): Promise<number> {
    return toNumber(await (this.redis as any).linsert(
      this.name, 'AFTER', this.encodeValue(existingElement), this.encodeValue(newElement)), -1);
  }
  /** Java `Collection.addAll(Collection)`: batch RPUSH; false for empty. */
  async addAll(values: V[]): Promise<boolean> {
    if (values.length === 0) return false;
    await this.redis.rpush(this.name, ...values.map((v) => this.encodeValue(v)));
    return true;
  }
  async size(): Promise<number> {
    return this.redis.llen(this.name);
  }
  async readAll(): Promise<V[]> {
    const m = await this.redis.lrange(this.name, 0, -1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async range(from: number, to: number): Promise<V[]> {
    const m = await this.redis.lrange(this.name, from, to);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async remove(value: V, count = 1): Promise<number> {
    return this.redis.lrem(this.name, count, this.encodeValue(value));
  }
  async trim(from: number, to: number): Promise<void> {
    await this.redis.ltrim(this.name, from, to);
  }
  async indexOf(value: V): Promise<number> {
    const s = `local all = redis.call('lrange', KEYS[1], 0, -1)
for i, v in ipairs(all) do if v == ARGV[1] then return i - 1 end end
return -1`;
    return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeValue(value)), -1);
  }
  async lastIndexOf(value: V): Promise<number> {
    const all = await this.redis.lrange(this.name, 0, -1);
    const enc = this.encodeValue(value);
    for (let i = all.length - 1; i >= 0; i--) if (all[i] === enc) return i;
    return -1;
  }
}

export class RQueue<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async offer(value: V): Promise<boolean> {
    await this.redis.rpush(this.name, this.encodeValue(value));
    return true;
  }
  async add(value: V): Promise<boolean> {
    return this.offer(value);
  }
  /** Java `Collection.addAll(Collection)`: batch RPUSH; false for empty input. */
  async addAll(values: V[]): Promise<boolean> {
    if (values.length === 0) return false;
    await this.redis.rpush(this.name, ...values.map((v) => this.encodeValue(v)));
    return true;
  }
  async poll(): Promise<V | null>;
  async poll(count: number): Promise<V[]>;
  async poll(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const raw = await this.redis.lpop(this.name);
      return raw === null ? null : (this.decodeValue(raw) as V);
    }
    if (count <= 0) return [];
    let arr: string[];
    try {
      const raw: any = await this.redis.lpop(this.name, count);
      arr = raw === null || raw === undefined ? [] : Array.isArray(raw) ? raw : [String(raw)];
    } catch {
      arr = [];
      for (let i = 0; i < count; i++) {
        const v = await this.redis.lpop(this.name);
        if (v === null) break;
        arr.push(v);
      }
    }
    return arr.map((x) => this.decodeValue(x) as V);
  }
  /** Java `RQueue.pollLastAndOfferFirstTo(dest)`: RPOPLPUSH. */
  async pollLastAndOfferFirstTo(dest: string): Promise<V | null> {
    const raw = await this.redis.rpoplpush(this.name, dest);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java 4.7 `move(QueueMoveElementsArgs)`: LMOVEM — head of this queue to
   *  the tail of dest; no amount selector moves a single element, `count(n)`
   *  moves up to n, `exactly(n)` fails unless n are available. */
  async move(args: QueueMoveElementsArgs): Promise<V[]> {
    const a: (string | number)[] = [this.name, args.dest, 'LEFT', 'RIGHT'];
    if (args.selector) a.push(args.selector, args.count!, args.ordering);
    const r = await (this.redis as any).call('LMOVEM', ...a);
    return (Array.isArray(r) ? r : []).map((x: any) => this.decodeValue(x) as V);
  }
  /** Java `RQueue.indexOf(V)`: first matching index (encoded comparison). */
  async indexOf(value: V): Promise<number> {
    const all = await this.redis.lrange(this.name, 0, -1);
    return all.indexOf(this.encodeValue(value));
  }
  async peek(): Promise<V | null> {
    const raw = await this.redis.lindex(this.name, 0);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async size(): Promise<number> {
    return this.redis.llen(this.name);
  }
  async readAll(): Promise<V[]> {
    const m = await this.redis.lrange(this.name, 0, -1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async contains(value: V): Promise<boolean> {
    const all = await this.redis.lrange(this.name, 0, -1);
    return all.includes(this.encodeValue(value));
  }
  async remove(value: V): Promise<boolean> {
    return ((await this.redis.lrem(this.name, 1, this.encodeValue(value))) as number) > 0;
  }
  async clear(): Promise<void> {
    await this.redis.del(this.name);
  }
}

export class RDeque<V = any> extends RQueue<V> {
  /** Java `addFirst(V...)`: LPUSH, returns the new length. */
  async addFirst(...values: V[]): Promise<number> {
    if (values.length === 0) return this.size();
    return this.redis.lpush(this.name, ...values.map((v) => this.encodeValue(v)));
  }
  /** Java `addLast(V...)`: RPUSH, returns the new length. */
  async addLast(...values: V[]): Promise<number> {
    if (values.length === 0) return this.size();
    return this.redis.rpush(this.name, ...values.map((v) => this.encodeValue(v)));
  }
  /** Java `addFirstIfExists(V...)`: LPUSHX (no-op when the list is absent). */
  async addFirstIfExists(...values: V[]): Promise<number> {
    if (values.length === 0) return 0;
    const enc = values.map((v) => this.encodeValue(v));
    try {
      return toNumber(await (this.redis as any).lpushx(this.name, ...enc));
    } catch {
      if ((await this.redis.exists(this.name)) === 0) return 0;
      return this.redis.lpush(this.name, ...enc);
    }
  }
  /** Java `addLastIfExists(V...)`: RPUSHX. */
  async addLastIfExists(...values: V[]): Promise<number> {
    if (values.length === 0) return 0;
    const enc = values.map((v) => this.encodeValue(v));
    try {
      return toNumber(await (this.redis as any).rpushx(this.name, ...enc));
    } catch {
      if ((await this.redis.exists(this.name)) === 0) return 0;
      return this.redis.rpush(this.name, ...enc);
    }
  }
  async removeFirst(): Promise<V | null> {
    return this.poll();
  }
  async removeLast(): Promise<V | null> {
    const raw = await this.redis.rpop(this.name);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async peekFirst(): Promise<V | null> {
    return this.peek();
  }
  async peekLast(): Promise<V | null> {
    const raw = await this.redis.lindex(this.name, -1);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async pollFirst(): Promise<V | null>;
  async pollFirst(count: number): Promise<V[]>;
  async pollFirst(count?: number): Promise<V | null | V[]> {
    if (count === undefined) return this.poll();
    return this.poll(count);
  }
  async pollLast(): Promise<V | null>;
  async pollLast(count: number): Promise<V[]>;
  async pollLast(count?: number): Promise<V | null | V[]> {
    if (count === undefined) return this.removeLast();
    if (count <= 0) return [];
    let arr: string[];
    try {
      const raw: any = await this.redis.rpop(this.name, count);
      arr = raw === null || raw === undefined ? [] : Array.isArray(raw) ? raw : [String(raw)];
    } catch {
      arr = [];
      for (let i = 0; i < count; i++) {
        const v = await this.redis.rpop(this.name);
        if (v === null) break;
        arr.push(v);
      }
    }
    return arr.map((x) => this.decodeValue(x) as V);
  }
  /** Java `move(DequeMoveArgs)`: atomic LMOVE into `dest`.
   *  (Java 4.7 `move(QueueMoveElementsArgs)` LMOVEM is inherited from RQueue.) */
  move(args: QueueMoveElementsArgs): Promise<V[]>;
  move(dest: string, from?: 'LEFT' | 'RIGHT', to?: 'LEFT' | 'RIGHT'): Promise<V | null>;
  async move(a: QueueMoveElementsArgs | string, from: 'LEFT' | 'RIGHT' = 'LEFT', to: 'LEFT' | 'RIGHT' = 'LEFT'): Promise<V[] | V | null> {
    if (typeof a !== 'string') return super.move(a);
    const raw = await (this.redis as any).lmove(this.name, a, from, to);
    return raw === null || raw === undefined ? null : (this.decodeValue(raw) as V);
  }
}

interface ChannelWaker {
  wait(ms: number): Promise<void>;
  close(): Promise<void>;
}

/** Lazily SUBSCRIBE a dedicated connection and resolve one waiter per message
 * (or timeout). The capacity key itself is authoritative, so losing a wake is
 * safe: the caller re-runs the atomic try-offer before each wait. */
function createChannelWaker(conn: Redis, channel: string): ChannelWaker {
  let notify: (() => void) | null = null;
  let subscribed = false;
  let failed = false;
  const onMessage = () => { notify?.(); };
  (conn as any).on?.('message', onMessage);
  return {
    async wait(ms: number) {
      if (!subscribed && !failed) {
        try {
          await conn.subscribe(channel);
          subscribed = true;
        } catch {
          failed = true;
        }
      }
      if (!subscribed) {
        await sleep(Math.min(Math.max(1, ms), 50));
        return;
      }
      await new Promise<void>((resolve) => {
        let done = false;
        let timer: any;
        const finish = () => {
          if (done) return;
          done = true;
          notify = null;
          clearTimeout(timer);
          resolve();
        };
        notify = finish;
        timer = setTimeout(finish, Math.max(1, ms));
      });
    },
    async close() {
      try {
        (conn as any).off?.('message', onMessage);
        if (subscribed) await (conn as any).unsubscribe?.(channel);
      } catch { /* noop */ }
    },
  };
}

export type ListSide = 'LEFT' | 'RIGHT';

function popOne(conn: Redis, name: string, side: ListSide): Promise<string | null> {
  return side === 'LEFT' ? conn.lpop(name) : conn.rpop(name);
}

async function scanPop(redis: Redis, names: string[], side: ListSide): Promise<{ key: string; value: string } | null> {
  for (const name of names) {
    const value = await popOne(redis, name, side);
    if (value !== null) return { key: name, value };
  }
  return null;
}

/** Blocking multi-key pop (BLPOP/BRPOP). `timeoutMs === null` blocks forever;
 *  `<= 0` is a non-blocking scan in argument order (matching Redis BLPOP key
 *  priority). Mock clients without BLPOP fall back to short-interval polling. */
async function blockingListPopFrom(
  redis: Redis,
  names: string[],
  side: ListSide,
  timeoutMs: number | null,
  signal?: AbortSignal,
): Promise<{ key: string; value: string } | null> {
  if (signal?.aborted) return null;
  if (timeoutMs !== null && timeoutMs <= 0) return scanPop(redis, names, side);
  const immediate = await scanPop(redis, names, side);
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<{ key: string; value: string } | null> => {
      for (;;) {
        if (signal?.aborted) return null;
        const r = await scanPop(redis, names, side);
        if (r !== null) return r;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const cmd = side === 'LEFT' ? 'blpop' : 'brpop';
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r: any = await (conn as any)[cmd](...names, seconds);
      if (r === null || r === undefined) return null;
      return Array.isArray(r) ? { key: String(r[0]), value: String(r[1]) } : { key: names[0], value: String(r) };
    } catch (e) {
      if (signal?.aborted) return null;
      if (!isUnsupportedCommand(e)) throw e;
      return poll();
    }
  }, signal);
}

/** Blocking multi-key, multi-value pop (BLMPOP). Returns the source key and at
 *  least one value. `timeoutMs === null` blocks forever; `<= 0` is a
 *  non-blocking scan. Falls back to polling without BLMPOP (mock). */
async function blockingListMultiPop(
  redis: Redis,
  names: string[],
  side: ListSide,
  count: number,
  timeoutMs: number | null,
): Promise<{ key: string; values: string[] } | null> {
  const scan = async (): Promise<{ key: string; values: string[] } | null> => {
    for (const name of names) {
      const values: string[] = [];
      for (let i = 0; i < count; i++) {
        const v = await popOne(redis, name, side);
        if (v === null) break;
        values.push(v);
      }
      if (values.length > 0) return { key: name, values };
    }
    return null;
  };
  if (timeoutMs !== null && timeoutMs <= 0) return scan();
  const immediate = await scan();
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<{ key: string; values: string[] } | null> => {
      for (;;) {
        const r = await scan();
        if (r !== null) return r;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r: any = await (conn as any).blmpop(seconds, names.length, ...names, side, 'COUNT', count);
      if (r === null || r === undefined) return null;
      const key = String(r[0]);
      const vals = Array.isArray(r[1]) ? r[1] : [r[1]];
      return { key, values: vals.map((x: any) => String(x)) };
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      return poll();
    }
  });
}

/** Blocking LMOVE (BLMOVE). `timeoutMs === null` blocks forever; `<= 0` is a
 *  non-blocking LMOVE. Mock clients without BLMOVE fall back to polling. */
async function blockingListMove(
  redis: Redis,
  src: string,
  dest: string,
  from: ListSide,
  to: ListSide,
  timeoutMs: number | null,
): Promise<string | null> {
  const move = async (conn: Redis): Promise<string | null> => {
    const r: any = await (conn as any).lmove(src, dest, from, to);
    return r === undefined ? null : r;
  };
  if (timeoutMs !== null && timeoutMs <= 0) return move(redis);
  const immediate = await move(redis);
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<string | null> => {
      for (;;) {
        const v = await move(redis);
        if (v !== null) return v;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r: any = await (conn as any).blmove(src, dest, from, to, seconds);
      return r === undefined ? null : r;
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      return poll();
    }
  });
}

/** BLPOP/BRPOP on a dedicated connection. `timeoutMs === null` blocks forever;
 * `<= 0` is non-blocking (LPOP/RPOP); positive values use Redis 6+ float
 * seconds. Mock clients without BLPOP fall back to short-interval polling. */
async function blockingListPop(
  redis: Redis,
  name: string,
  side: ListSide,
  timeoutMs: number | null,
  signal?: AbortSignal,
): Promise<string | null> {
  const r = await blockingListPopFrom(redis, [name], side, timeoutMs, signal);
  return r === null ? null : r.value;
}

// -- Element subscriptions (Java ElementsSubscribeService) ---------------------
// Each listener runs a take loop: deliver the popped element to the consumer,
// await it, then take again. Failures retry after one second. Cancellation
// aborts the dedicated BLPOP connection so unsubscribe() is immediate.

interface ElementSubscription {
  cancelled: boolean;
  controller: AbortController;
  consumer: (value: any) => unknown;
}

const elementSubscriptions = new WeakMap<object, Map<number, ElementSubscription>>();
const elementConsumerIds = new WeakMap<object, Map<Function, number>>();
let elementSubscriptionSeq = 0;

function registerElementSubscription<V>(
  owner: object,
  consumer: (value: V) => unknown,
  take: (signal: AbortSignal) => Promise<V | null>,
): number {
  let subs = elementSubscriptions.get(owner);
  if (!subs) { subs = new Map(); elementSubscriptions.set(owner, subs); }
  let consumers = elementConsumerIds.get(owner);
  if (!consumers) { consumers = new Map(); elementConsumerIds.set(owner, consumers); }
  if (consumers.has(consumer)) {
    throw new Error(`Consumer object with listener id ${consumers.get(consumer)} already registered`);
  }
  const id = ++elementSubscriptionSeq;
  const sub: ElementSubscription = { cancelled: false, controller: new AbortController(), consumer };
  subs.set(id, sub);
  consumers.set(consumer, id);
  void (async () => {
    while (!sub.cancelled) {
      let value: V | null;
      try {
        value = await take(sub.controller.signal);
      } catch {
        if (sub.cancelled) return;
        await sleep(1000);
        continue;
      }
      if (sub.cancelled) return;
      if (value === null) continue;
      try {
        await sub.consumer(value);
      } catch {
        if (sub.cancelled) return;
        await sleep(1000);
      }
    }
  })();
  return id;
}

function unregisterElementSubscription(owner: object, listenerId: number): void {
  const subs = elementSubscriptions.get(owner);
  const sub = subs?.get(listenerId);
  if (!subs || !sub) return;
  sub.cancelled = true;
  sub.controller.abort();
  subs.delete(listenerId);
  const consumers = elementConsumerIds.get(owner);
  consumers?.delete(sub.consumer);
}

/** ZPOPMIN/ZPOPMAX reply is a flat [member, score] pair. */
function zPair(raw: any): { value: string; score: number } | null {
  if (!raw) return null;
  const arr = Array.isArray(raw) ? raw : [raw];
  if (arr.length < 2) return null;
  return { value: String(arr[0]), score: Number(arr[1]) };
}

/** Blocking sorted-set pop (BZPOPMIN/BZPOPMAX). `timeoutMs === null` blocks
 *  forever; `<= 0` is a non-blocking ZPOPMIN/ZPOPMAX. Mocks without BZPOP*
 *  fall back to short-interval polling. */
async function blockingZPop(
  redis: Redis,
  name: string,
  side: 'MIN' | 'MAX',
  timeoutMs: number | null,
): Promise<{ value: string; score: number } | null> {
  const pop = async (): Promise<{ value: string; score: number } | null> => {
    const raw: any = side === 'MIN'
      ? await (redis as any).zpopmin(name, 1)
      : await (redis as any).zpopmax(name, 1);
    return zPair(raw);
  };
  if (timeoutMs !== null && timeoutMs <= 0) return pop();
  const immediate = await pop();
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<{ value: string; score: number } | null> => {
      for (;;) {
        const r = await pop();
        if (r !== null) return r;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const cmd = side === 'MIN' ? 'bzpopmin' : 'bzpopmax';
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r: any = await (conn as any)[cmd](name, seconds);
      if (!r) return null;
      // [key, member, score]
      return Array.isArray(r) ? zPair([r[1], r[2]]) : zPair(r);
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      return poll();
    }
  });
}

/** Blocking multi-key sorted-set pop (BZMPOP side MIN/MAX, COUNT 1).
 *  Returns the source key plus member/score; null on timeout. */
async function blockingZPopFromAny(
  redis: Redis,
  names: string[],
  side: 'MIN' | 'MAX',
  timeoutMs: number | null,
): Promise<{ key: string; value: string; score: number } | null> {
  const scan = async (): Promise<{ key: string; value: string; score: number } | null> => {
    for (const name of names) {
      const raw: any = side === 'MIN'
        ? await (redis as any).zpopmin(name, 1)
        : await (redis as any).zpopmax(name, 1);
      const e = zPair(raw);
      if (e !== null) return { key: name, ...e };
    }
    return null;
  };
  if (timeoutMs !== null && timeoutMs <= 0) return scan();
  const immediate = await scan();
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<{ key: string; value: string; score: number } | null> => {
      for (;;) {
        const r = await scan();
        if (r !== null) return r;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r: any = await (conn as any).bzmpop(seconds, names.length, ...names, side, 'COUNT', 1);
      if (r === null || r === undefined) return null;
      const key = String(r[0]);
      const payload = Array.isArray(r[1]) ? r[1] : [];
      const first = Array.isArray(payload[0]) ? payload[0] : payload;
      const e = zPair(first);
      return e === null ? null : { key, ...e };
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      return poll();
    }
  });
}

const QUEUE_DRAIN = `local max = tonumber(ARGV[1])
local len = redis.call('llen', KEYS[1])
local count = len
if max >= 0 and max < count then count = max end
if count <= 0 then return {} end
local vals = redis.call('lrange', KEYS[1], 0, count - 1)
if count >= len then
    redis.call('del', KEYS[1])
else
    redis.call('ltrim', KEYS[1], count, -1)
end
return vals`;

export class RBlockingQueue<V = any> extends RQueue<V> {
  async take(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'LEFT', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  /** Take used by element subscriptions; overriding classes may alter it. */
  protected async takeForElementSubscription(signal: AbortSignal): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'LEFT', null, signal);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `subscribeOnElements(consumer)`: continuously take and deliver.
   *  Async consumers provide back-pressure; failures retry after one second. */
  subscribeOnElements(consumer: (value: V) => unknown): number {
    return registerElementSubscription(this, consumer, (signal) => this.takeForElementSubscription(signal));
  }
  /** Java `unsubscribe(listenerId)`. Unknown ids are ignored. */
  unsubscribe(listenerId: number): void {
    unregisterElementSubscription(this, listenerId);
  }
  async pollWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'LEFT', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async put(value: V): Promise<void> {
    await this.offer(value);
  }
  async drainTo(target: RQueue<V>, maxElements = -1): Promise<number> {
    if (maxElements === 0) return 0;
    const raw: any = await luaEval(this.redis, QUEUE_DRAIN, 1, this.name, String(maxElements));
    const all: string[] = Array.isArray(raw)
      ? raw
      : (raw === null || raw === undefined || raw === false ? [] : [String(raw)]);
    for (const v of all) await target.offer(this.decodeValue(v) as V);
    return all.length;
  }
  /** Java `RBlockingQueue.pollFromAny(timeout, unit, queueNames...)`: BLPOP
   *  over this queue plus the given ones (this one first). `timeoutMs === null`
   *  blocks forever; `<= 0` is a non-blocking scan. */
  async pollFromAny(timeoutMs: number | null, ...otherNames: string[]): Promise<V | null> {
    const r = await blockingListPopFrom(this.redis, [this.name, ...otherNames], 'LEFT', timeoutMs);
    return r === null ? null : (this.decodeValue(r.value) as V);
  }
  /** Java `pollFromAnyWithName(Duration, queueNames...)`: value plus the source
   *  queue name that produced it. */
  async pollFromAnyWithName(timeoutMs: number | null, ...otherNames: string[]): Promise<{ key: string; value: V } | null> {
    const r = await blockingListPopFrom(this.redis, [this.name, ...otherNames], 'LEFT', timeoutMs);
    return r === null ? null : { key: r.key, value: this.decodeValue(r.value) as V };
  }
  /** Java `pollFirstFromAny(Duration, count, queueNames...)`: BLMPOP LEFT with
   *  `COUNT count`; the result maps the source queue to its popped values (at
   *  most one entry — the pop is atomic on one queue). */
  async pollFirstFromAny(timeoutMs: number | null, count: number, ...otherNames: string[]): Promise<Map<string, V[]>> {
    const r = await blockingListMultiPop(this.redis, [this.name, ...otherNames], 'LEFT', Math.max(1, count), timeoutMs);
    const out = new Map<string, V[]>();
    if (r !== null) out.set(r.key, r.values.map((x) => this.decodeValue(x) as V));
    return out;
  }
  /** Java `pollLastFromAny(Duration, count, queueNames...)`: BLMPOP RIGHT. */
  async pollLastFromAny(timeoutMs: number | null, count: number, ...otherNames: string[]): Promise<Map<string, V[]>> {
    const r = await blockingListMultiPop(this.redis, [this.name, ...otherNames], 'RIGHT', Math.max(1, count), timeoutMs);
    const out = new Map<string, V[]>();
    if (r !== null) out.set(r.key, r.values.map((x) => this.decodeValue(x) as V));
    return out;
  }
  /** Java `pollLastFromAnyWithName(Duration, queueNames...)`: BRPOP with the
   *  source queue name. Negative timeouts return null without scanning. */
  async pollLastFromAnyWithName(timeoutMs: number | null, ...otherNames: string[]): Promise<{ key: string; value: V } | null> {
    if (timeoutMs !== null && timeoutMs < 0) return null;
    const r = await blockingListPopFrom(this.redis, [this.name, ...otherNames], 'RIGHT', timeoutMs);
    return r === null ? null : { key: r.key, value: this.decodeValue(r.value) as V };
  }
  /** Java `pollLastAndOfferFirstTo(dest)`: non-blocking RPOPLPUSH. */
  override async pollLastAndOfferFirstTo(dest: string): Promise<V | null>;
  /** Java `pollLastAndOfferFirstTo(dest, timeout, unit)`: BLMOVE RIGHT→LEFT.
   *  `timeoutMs === null` blocks forever. */
  override async pollLastAndOfferFirstTo(dest: string, timeoutMs: number | null): Promise<V | null>;
  override async pollLastAndOfferFirstTo(dest: string, timeoutMs?: number | null): Promise<V | null> {
    if (timeoutMs === undefined) return super.pollLastAndOfferFirstTo(dest);
    const raw = await blockingListMove(this.redis, this.name, dest, 'RIGHT', 'LEFT', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `takeLastAndOfferFirstTo(dest)`: BLMOVE RIGHT→LEFT, block forever. */
  async takeLastAndOfferFirstTo(dest: string): Promise<V | null> {
    return this.pollLastAndOfferFirstTo(dest, null);
  }
  /** Java 4.7 blocking `move(Duration, QueueMoveElementsArgs)`:
   *  BLMOVEM on a dedicated connection (float seconds; null = forever).
   *  The non-blocking `move(args)` form is inherited from RQueue. */
  move(args: QueueMoveElementsArgs): Promise<V[]>;
  move(timeoutMs: number | null, args: QueueMoveElementsArgs): Promise<V[]>;
  async move(a: number | null | QueueMoveElementsArgs, b?: QueueMoveElementsArgs): Promise<V[]> {
    if (typeof a !== 'number' && a !== null) return super.move(a);
    const args = b!;
    return withDedicatedRedis(this.redis, async (conn: any) => {
      const c: (string | number)[] = [this.name, args.dest, 'LEFT', 'RIGHT', a === null ? 0 : a / 1000];
      if (args.selector) c.push(args.selector, args.count!, args.ordering);
      const r = await conn.call('BLMOVEM', ...c);
      return (Array.isArray(r) ? r : []).map((x: any) => this.decodeValue(x) as V);
    });
  }
}

export class RBlockingDeque<V = any> extends RDeque<V> {
  async takeFirst(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'LEFT', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  protected async takeForElementSubscription(signal: AbortSignal): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'LEFT', null, signal);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `subscribeOnElements(consumer)`: continuously takeFirst and deliver. */
  subscribeOnElements(consumer: (value: V) => unknown): number {
    return registerElementSubscription(this, consumer, (signal) => this.takeForElementSubscription(signal));
  }
  /** Java `unsubscribe(listenerId)`. Unknown ids are ignored. */
  unsubscribe(listenerId: number): void {
    unregisterElementSubscription(this, listenerId);
  }
  async takeLast(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'RIGHT', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  async pollFirstWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'LEFT', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async pollLastWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'RIGHT', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `RBlockingDeque.move(Duration, DequeMoveArgs)` → LMOVE: atomically
   *  pop one element from `from` here and push it to `to` of `dest` (no
   *  blocking; `null` when this deque is empty). */
  move(args: QueueMoveElementsArgs): Promise<V[]>;
  move(dest: string, from?: ListSide, to?: ListSide): Promise<V | null>;
  async move(a: QueueMoveElementsArgs | string, from: ListSide = 'LEFT', to: ListSide = 'LEFT'): Promise<V[] | V | null> {
    if (typeof a !== 'string') return super.move(a as QueueMoveElementsArgs);
    const raw = await blockingListMove(this.redis, this.name, a, from, to, 0);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Blocking LMOVE (`BLMOVE`). `timeoutMs === null` blocks forever. */
  async moveWithTimeout(dest: string, from: ListSide, to: ListSide, timeoutMs: number | null): Promise<V | null> {
    const raw = await blockingListMove(this.redis, this.name, dest, from, to, timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `pollFirstFromAny(Duration, count, queueNames...)`: BLMPOP LEFT. */
  async pollFirstFromAny(timeoutMs: number | null, count: number, ...otherNames: string[]): Promise<Map<string, V[]>> {
    const r = await blockingListMultiPop(this.redis, [this.name, ...otherNames], 'LEFT', Math.max(1, count), timeoutMs);
    const out = new Map<string, V[]>();
    if (r !== null) out.set(r.key, r.values.map((x) => this.decodeValue(x) as V));
    return out;
  }
  /** Java `pollLastFromAny(Duration, count, queueNames...)`: BLMPOP RIGHT. */
  async pollLastFromAny(timeoutMs: number | null, count: number, ...otherNames: string[]): Promise<Map<string, V[]>> {
    const r = await blockingListMultiPop(this.redis, [this.name, ...otherNames], 'RIGHT', Math.max(1, count), timeoutMs);
    const out = new Map<string, V[]>();
    if (r !== null) out.set(r.key, r.values.map((x) => this.decodeValue(x) as V));
    return out;
  }
  /** Java `pollLastFromAnyWithName(Duration, queueNames...)`: BRPOP with the
   *  source queue name. Negative timeouts return null without scanning. */
  async pollLastFromAnyWithName(timeoutMs: number | null, ...otherNames: string[]): Promise<{ key: string; value: V } | null> {
    if (timeoutMs !== null && timeoutMs < 0) return null;
    const r = await blockingListPopFrom(this.redis, [this.name, ...otherNames], 'RIGHT', timeoutMs);
    return r === null ? null : { key: r.key, value: this.decodeValue(r.value) as V };
  }
  /** Java `pollLastAndOfferFirstTo(dest)`: non-blocking RPOPLPUSH. */
  override async pollLastAndOfferFirstTo(dest: string): Promise<V | null>;
  /** Java `pollLastAndOfferFirstTo(dest, timeout, unit)`: BLMOVE RIGHT→LEFT. */
  override async pollLastAndOfferFirstTo(dest: string, timeoutMs: number | null): Promise<V | null>;
  override async pollLastAndOfferFirstTo(dest: string, timeoutMs?: number | null): Promise<V | null> {
    if (timeoutMs === undefined) return super.pollLastAndOfferFirstTo(dest);
    const raw = await blockingListMove(this.redis, this.name, dest, 'RIGHT', 'LEFT', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  /** Java `takeLastAndOfferFirstTo(dest)`: BLMOVE RIGHT→LEFT, block forever. */
  async takeLastAndOfferFirstTo(dest: string): Promise<V | null> {
    return this.pollLastAndOfferFirstTo(dest, null);
  }
}

// Bounded queue semaphore protocol (Redisson 4.6.1 / redi.go):
//   redisson_bqs:{name} = AVAILABLE permits, not the configured maximum.
//   trySetCapacity SETNX-equivalent + PUBLISH; offer DECRBY + RPUSH atomically;
//   every removal INCRBY + PUBLISH redisson_sc:redisson_bqs:{name}.
const BQ_TRY_OFFER = `local value = redis.call('get', KEYS[1])
if value == false then return -1 end
if tonumber(value) >= tonumber(ARGV[2]) then
    redis.call('decrby', KEYS[1], ARGV[2])
    redis.call('rpush', KEYS[2], ARGV[1])
    return 1
end
return 0`;

const BQ_RELEASE = `local value = redis.call('incrby', KEYS[1], ARGV[1])
redis.call('publish', KEYS[2], value)
return value`;

const BQ_POLL = `local res = redis.call('lpop', KEYS[1])
if res ~= false then
    local value = redis.call('incrby', KEYS[2], ARGV[1])
    redis.call('publish', KEYS[3], value)
end
return res`;

const BQ_REMOVE = `local count = redis.call('lrem', KEYS[1], 0, ARGV[1])
if count > 0 then
    local value = redis.call('incrby', KEYS[2], count)
    redis.call('publish', KEYS[3], value)
    return 1
end
return 0`;

const BQ_CLEAR = `local len = redis.call('llen', KEYS[1])
if len > 0 then
    redis.call('del', KEYS[1])
    local value = redis.call('incrby', KEYS[2], len)
    redis.call('publish', KEYS[3], value)
end
return len`;

const BQ_DRAIN = `local max = tonumber(ARGV[1])
local len = redis.call('llen', KEYS[1])
local count = len
if max >= 0 and max < count then count = max end
if count <= 0 then return {} end
local vals = redis.call('lrange', KEYS[1], 0, count - 1)
if count >= len then
    redis.call('del', KEYS[1])
else
    redis.call('ltrim', KEYS[1], count, -1)
end
local value = redis.call('incrby', KEYS[2], count)
redis.call('publish', KEYS[3], value)
return vals`;

const BQ_CAPACITY = `local value = redis.call('get', KEYS[1])
if value == false then
    redis.call('set', KEYS[1], ARGV[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;

export class RBoundedBlockingQueue<V = any> extends RBlockingQueue<V> {
  private capacityKey: string;
  private channel: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.capacityKey = prefixName('redisson_bqs', name);
    this.channel = prefixName('redisson_sc', this.capacityKey);
  }

  /** Bounded queues subscribe through the capacity-aware poll so permits are
   *  released on every delivered element (Java uses takeAsync). */
  protected override async takeForElementSubscription(signal: AbortSignal): Promise<V | null> {
    while (!signal.aborted) {
      const v = await this.poll();
      if (v !== null) return v;
      await sleep(25);
    }
    return null;
  }

  async trySetCapacity(capacity: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, BQ_CAPACITY, 2, this.capacityKey, this.channel, String(capacity))) === 1;
  }

  /** Remaining permits (Java `remainingCapacity()`); NOT the configured max. */
  async remainingCapacity(): Promise<number> {
    const v = await this.redis.get(this.capacityKey);
    return v === null ? 0 : Number(v);
  }

  /** Back-compat alias for {@link remainingCapacity}. */
  async getCapacity(): Promise<number> {
    return this.remainingCapacity();
  }

  private async tryOffer(encoded: string): Promise<boolean> {
    const n = toNumber(await luaEval(this.redis, BQ_TRY_OFFER, 2, this.capacityKey, this.name, encoded, '1'));
    if (n === -1) throw new Error(`Capacity of queue ${this.capacityKey} has not been set`);
    return n === 1;
  }

  private async releaseCapacity(permits = 1): Promise<number> {
    return toNumber(await luaEval(this.redis, BQ_RELEASE, 2, this.capacityKey, this.channel, String(permits)));
  }

  private async waitCapacity(encoded: string, timeoutMs: number | null): Promise<boolean> {
    return withDedicatedRedis(this.redis, async (conn, dedicated) => {
      // Java blocks on the capacity channel. The key is authoritative; if the
      // subscription is unavailable we still poll the atomic try-offer.
      const waker = dedicated ? createChannelWaker(conn, this.channel) : null;
      try {
        const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
        for (;;) {
          if (await this.tryOffer(encoded)) return true;
          const remain = deadline === Infinity ? 1000 : deadline - Date.now();
          if (remain <= 0) return false;
          if (waker) await waker.wait(Math.min(remain, 1000));
          else await sleep(Math.min(remain, 50));
        }
      } finally {
        await waker?.close();
      }
    });
  }

  override async offer(value: V, timeoutMs = 0): Promise<boolean> {
    const encoded = this.encodeValue(value);
    if (await this.tryOffer(encoded)) return true;
    if (timeoutMs <= 0) return false;
    return this.waitCapacity(encoded, timeoutMs);
  }

  override async put(value: V): Promise<void> {
    await this.waitCapacity(this.encodeValue(value), null);
  }

  override async poll(): Promise<V | null>;
  override async poll(count: number): Promise<V[]>;
  override async poll(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const raw = await luaEval(this.redis, BQ_POLL, 3, this.name, this.capacityKey, this.channel, '1');
      return raw === null || raw === undefined || raw === false ? null : (this.decodeValue(raw) as V);
    }
    if (count <= 0) return [];
    const out: V[] = [];
    for (let i = 0; i < count; i++) {
      const v = await this.poll();
      if (v === null) break;
      out.push(v);
    }
    return out;
  }

  override async take(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'LEFT', null);
      if (raw !== null) {
        await this.releaseCapacity(1);
        return this.decodeValue(raw) as V;
      }
    }
  }

  override async pollWithTimeout(timeoutMs: number): Promise<V | null> {
    if (timeoutMs <= 0) return this.poll();
    const raw = await blockingListPop(this.redis, this.name, 'LEFT', timeoutMs);
    if (raw === null) return null;
    await this.releaseCapacity(1);
    return this.decodeValue(raw) as V;
  }

  override async remove(value: V): Promise<boolean> {
    const n = toNumber(await luaEval(this.redis, BQ_REMOVE, 3, this.name, this.capacityKey, this.channel, this.encodeValue(value)));
    return n === 1;
  }

  override async clear(): Promise<void> {
    await luaEval(this.redis, BQ_CLEAR, 3, this.name, this.capacityKey, this.channel);
  }

  override async drainTo(target: RQueue<V>, maxElements = -1): Promise<number> {
    if (maxElements === 0) return 0;
    const raw: any = await luaEval(this.redis, BQ_DRAIN, 3, this.name, this.capacityKey, this.channel, String(maxElements));
    const vals: string[] = Array.isArray(raw)
      ? raw
      : (raw === null || raw === undefined || raw === false ? [] : [String(raw)]);
    for (const v of vals) await target.offer(this.decodeValue(v) as V);
    return vals.length;
  }

  // Java deletes the queue LIST together with the semaphore capacity key.
  // Without this, trySetCapacity() can never succeed again after delete().
  override async delete(): Promise<boolean> {
    return this.deleteKeys([this.name, this.capacityKey]);
  }

  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name, this.capacityKey]);
  }
}

export interface ScoredEntry<V> {
  value: V;
  score: number;
}

/** Aggregation for ZUNIONSTORE/ZINTERSTORE (mirrors Redisson Aggregate). */
export type ZAggregate = 'SUM' | 'MIN' | 'MAX';

export class RScoredSortedSet<V = any> extends RedisDataStructure {
  private listenerHub: KeyspaceListenerHub;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.listenerHub = new KeyspaceListenerHub(redis, name, {
      onAdd: 'zadd',
      onRemove: 'zrem',
      onIncrement: 'zincr',
      onStore: ['zunionstore', 'zinterstore', 'zdiffstore'],
    });
  }
  async add(score: number, value: V): Promise<boolean> {
    return ((await this.redis.zadd(this.name, String(score), this.encodeValue(value))) as number) > 0;
  }
  async addIfAbsent(score: number, value: V): Promise<boolean> {
    return ((await this.redis.zadd(this.name, 'NX', String(score), this.encodeValue(value))) as number) > 0;
  }
  /** Java `addIfExists(double, V)`: ZADD XX CH — true when the score changed. */
  async addIfExists(score: number, value: V): Promise<boolean> {
    return ((await (this.redis as any).zadd(this.name, 'XX', 'CH', String(score), this.encodeValue(value))) as number) === 1;
  }
  /** Java `addIfLess(double, V)`: ZADD LT CH — adds missing, lowers existing. */
  async addIfLess(score: number, value: V): Promise<boolean> {
    return ((await (this.redis as any).zadd(this.name, 'LT', 'CH', String(score), this.encodeValue(value))) as number) === 1;
  }
  /** Java `addIfGreater(double, V)`: ZADD GT CH. */
  async addIfGreater(score: number, value: V): Promise<boolean> {
    return ((await (this.redis as any).zadd(this.name, 'GT', 'CH', String(score), this.encodeValue(value))) as number) === 1;
  }
  /** Java `addAll(Map<V, Number>)`: ZADD ... CH — changed/added count. */
  async addAll(entries: Map<V, number>): Promise<number> {
    return this.zaddMany(entries);
  }
  /** Java `addAllIfAbsent(Map)`: ZADD NX CH. */
  async addAllIfAbsent(entries: Map<V, number>): Promise<number> {
    return this.zaddMany(entries, 'NX');
  }
  /** Java `addAllIfExist(Map)`: ZADD XX CH. */
  async addAllIfExist(entries: Map<V, number>): Promise<number> {
    return this.zaddMany(entries, 'XX');
  }
  /** Java `addAllIfGreater(Map)`: ZADD GT CH. */
  async addAllIfGreater(entries: Map<V, number>): Promise<number> {
    return this.zaddMany(entries, 'GT');
  }
  /** Java `addAllIfLess(Map)`: ZADD LT CH. */
  async addAllIfLess(entries: Map<V, number>): Promise<number> {
    return this.zaddMany(entries, 'LT');
  }
  private async zaddMany(entries: Map<V, number>, flag?: 'NX' | 'XX' | 'GT' | 'LT'): Promise<number> {
    if (entries.size === 0) return 0;
    const args: (string | number)[] = [];
    if (flag) args.push(flag);
    args.push('CH');
    for (const [v, s] of entries) args.push(String(s), this.encodeValue(v));
    return toNumber(await (this.redis as any).zadd(this.name, ...args));
  }
  async remove(value: V): Promise<boolean> {
    return ((await this.redis.zrem(this.name, this.encodeValue(value))) as number) > 0;
  }
  async getScore(value: V): Promise<number | null> {
    const s = await this.redis.zscore(this.name, this.encodeValue(value));
    return s === null ? null : Number(s);
  }
  async contains(value: V): Promise<boolean> {
    return (await this.getScore(value)) !== null;
  }
  /** Java `rank(V)`: 0-based ascending rank, `null` when absent. */
  async rank(value: V): Promise<number | null> {
    const r = await this.redis.zrank(this.name, this.encodeValue(value));
    return r === null ? null : r;
  }
  /** Java `revRank(V)`: 0-based descending rank, `null` when absent. */
  async revRank(value: V): Promise<number | null> {
    const r = await this.redis.zrevrank(this.name, this.encodeValue(value));
    return r === null ? null : r;
  }
  /** Java `rankEntry(V)`/`revRankEntry(V)`: rank + current score, or null. */
  async rankEntry(value: V): Promise<{ rank: number; score: number } | null> {
    const rank = await this.rank(value);
    if (rank === null || rank === -1) return null;
    const score = await this.getScore(value);
    return { rank, score: Number(score) };
  }
  async revRankEntry(value: V): Promise<{ rank: number; score: number } | null> {
    const rank = await this.revRank(value);
    if (rank === null) return null;
    const score = await this.getScore(value);
    return { rank, score: Number(score) };
  }
  /** Java `pollFirstEntries(count)`/`pollLastEntries(count)`: ZPOPMIN/MAX n. */
  async pollFirstEntries(count: number): Promise<ScoredEntry<V>[]> {
    const raw = await (this.redis as any).zpopmin(this.name, count);
    return this.decodeScoredPairs(raw);
  }
  async pollLastEntries(count: number): Promise<ScoredEntry<V>[]> {
    const raw = await (this.redis as any).zpopmax(this.name, count);
    return this.decodeScoredPairs(raw);
  }
  /** ZPOPMIN/MAX reply [member, score, member, score...] → entries. */
  private decodeScoredPairs(raw: unknown): ScoredEntry<V>[] {
    const out: ScoredEntry<V>[] = [];
    if (Array.isArray(raw)) {
      for (let i = 0; i + 1 < raw.length; i += 2) {
        out.push({ value: this.decodeValue(String(raw[i])) as V, score: Number(raw[i + 1]) });
      }
    }
    return out;
  }
  /** Java `isEmpty()`. */
  async isEmpty(): Promise<boolean> {
    return (await this.size()) === 0;
  }
  /** Java `toArray()`. */
  async toArray(): Promise<V[]> {
    return this.range(0, -1);
  }
  /** Java `readAll()`. */
  async readAll(): Promise<V[]> {
    return this.range(0, -1);
  }
  /** Java `removeAll(Collection)`: true when anything was removed. */
  async removeAll(values: V[]): Promise<boolean> {
    if (values.length === 0) return false;
    const n = await this.redis.zrem(this.name, ...values.map((v) => this.encodeValue(v)));
    return Number(n) > 0;
  }
  /** Java `retainAll(Collection)`: true when others were removed. */
  async retainAll(values: V[]): Promise<boolean> {
    const keep = new Set(values.map((v) => this.encodeValue(v)));
    const all = await this.range(0, -1);
    const drop: V[] = [];
    for (const v of all) if (!keep.has(this.encodeValue(v))) drop.push(v);
    if (drop.length === 0) return false;
    await this.redis.zrem(this.name, ...drop.map((v) => this.encodeValue(v)));
    return true;
  }
  /** Java `rangeTo(dest, startIndex, endIndex)` / `revRangeTo`: ZRANGESTORE. */
  async rangeTo(destName: string, startIndex: number, endIndex: number): Promise<number>;
  async rangeTo(destName: string, startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<number>;
  async rangeTo(destName: string, a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<number> {
    return this.rangeToImpl(destName, false, a, b, c, d, offset, count);
  }
  async revRangeTo(destName: string, startIndex: number, endIndex: number): Promise<number>;
  async revRangeTo(destName: string, startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<number>;
  async revRangeTo(destName: string, a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<number> {
    return this.rangeToImpl(destName, true, a, b, c, d, offset, count);
  }
  private async rangeToImpl(destName: string, rev: boolean, a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<number> {
    const args: (string | number)[] = [destName, this.name];
    if (typeof b === 'boolean') {
      // score range: rev swaps bounds (Java passes max first with REV)
      const lo = rev ? c! : a;
      const hi = rev ? a : c!;
      const loInc = rev ? d! : b;
      const hiInc = rev ? b : d;
      args.push((loInc ? '[' : '(') + lo, (hiInc ? '[' : '(') + hi, 'BYSCORE');
      if (rev) args.push('REV');
      if (offset !== undefined && count !== undefined) args.push('LIMIT', offset, count);
    } else {
      if (rev) args.push(b, a, 'REV');
      else args.push(a, b);
    }
    return toNumber(await (this.redis as any).call('ZRANGESTORE', ...args));
  }
  async size(): Promise<number> {
    return this.redis.zcard(this.name);
  }
  /** Java `iterator(pattern, count)` / `stream`: lazy ZSCAN over members.
   *  MATCH applies to encoded member names. */
  async *iterator(pattern = '*', count = 100): AsyncGenerator<V> {
    for await (const e of this.entriesIterator(pattern, count)) yield e.value;
  }
  /** Lazy ZSCAN over {value, score} entries (TS extension used by iterator). */
  async *entriesIterator(pattern = '*', count = 100): AsyncGenerator<ScoredEntry<V>> {
    try {
      let cursor = '0';
      do {
        const [next, rows] = (await (this.redis as any).zscan(
          this.name, cursor, 'MATCH', pattern, 'COUNT', String(count),
        )) as [string, string[]];
        for (let i = 0; i + 1 < rows.length; i += 2) {
          yield { value: this.decodeValue(rows[i]) as V, score: Number(rows[i + 1]) };
        }
        cursor = next;
      } while (cursor !== '0');
    } catch {
      // Mocks without ZSCAN: full range; glob matching beyond '*' is not
      // emulated (real servers always take the ZSCAN path above).
      if (pattern !== '*') return;
      for (const e of await this.rangeWithScores(0, -1)) yield e;
    }
  }
  async *stream(pattern = '*', count = 100): AsyncGenerator<V> {
    yield* this.iterator(pattern, count);
  }
  async range(from: number, to: number): Promise<V[]> {
    const m = await this.redis.zrange(this.name, from, to);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async rangeWithScores(from: number, to: number): Promise<ScoredEntry<V>[]> {
    const m = await this.redis.zrange(this.name, from, to, 'WITHSCORES');
    const out: ScoredEntry<V>[] = [];
    for (let i = 0; i + 1 < m.length; i += 2) out.push({ value: this.decodeValue(m[i]) as V, score: Number(m[i + 1]) });
    return out;
  }
  async rangeByScore(min: number | string, max: number | string, offset = 0, count = -1): Promise<V[]> {
    const args: (string | number)[] = [String(min), String(max), 'BYSCORE'];
    if (count >= 0) args.push('LIMIT', offset, count);
    const m = await (this.redis as any).zrange(this.name, ...args);
    return (m as string[]).map((x) => this.decodeValue(x) as V);
  }
  /** Java `random()`: ZRANDMEMBER one member (`null` when empty). */
  async random(): Promise<V | null>;
  /** Java `random(int)`: ZRANDMEMBER count (negative = allow repeats). */
  async random(count: number): Promise<V[]>;
  async random(count?: number): Promise<V | null | V[]> {
    try {
      if (count === undefined) {
        const m: any = await (this.redis as any).zrandmember(this.name);
        return m === null || m === undefined ? null : (this.decodeValue(String(m)) as V);
      }
      const m: string[] = await (this.redis as any).zrandmember(this.name, count);
      return (m ?? []).map((x) => this.decodeValue(x) as V);
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      // mock fallback (no ZRANDMEMBER): sample client-side from the full set
      const all = await this.range(0, -1);
      if (count === undefined) return all.length === 0 ? null : all[Math.floor(Math.random() * all.length)];
      const pool = [...all];
      const out: V[] = [];
      const n = Math.abs(count);
      for (let i = 0; i < n; i++) {
        if (pool.length === 0) {
          if (count < 0) pool.push(...all);
          else break;
        }
        out.push(pool.splice(Math.floor(Math.random() * pool.length), 1)[0]);
      }
      return out;
    }
  }
  /** Java `randomEntries(int)`: ZRANDMEMBER count WITHSCORES → member→score. */
  async randomEntries(count = 1): Promise<Map<V, number>> {
    const out = new Map<V, number>();
    if (count <= 0) return out;
    try {
      const m: string[] = await (this.redis as any).zrandmember(this.name, count, 'WITHSCORES');
      for (let i = 0; i + 1 < m.length; i += 2) out.set(this.decodeValue(m[i]) as V, Number(m[i + 1]));
      return out;
    } catch (e) {
      if (!isUnsupportedCommand(e)) throw e;
      const all = await this.rangeWithScores(0, -1);
      for (let i = all.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [all[i], all[j]] = [all[j], all[i]];
      }
      for (const entry of all.slice(0, Math.min(count, all.length))) out.set(entry.value, entry.score);
      return out;
    }
  }
  async addAndGetRank(score: number, value: V): Promise<number> {
    const s = `redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrank', KEYS[1], ARGV[2])`;
    const r = await luaEval(this.redis, s, 1, this.name, String(score), this.encodeValue(value));
    return r === null ? -1 : Number(r);
  }
  /** Java `addAndGetRevRank(double, V)`: atomic ZADD + ZREVRANK (null rank → -1). */
  async addAndGetRevRank(score: number, value: V): Promise<number> {
    const s = `redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrevrank', KEYS[1], ARGV[2])`;
    const r = await luaEval(this.redis, s, 1, this.name, String(score), this.encodeValue(value));
    return r === null ? -1 : Number(r);
  }
  /** Java `addScore(V, Number)`: ZINCRBY — returns the resulting score. */
  async addScore(value: V, score: number): Promise<number> {
    return Number(await this.redis.zincrby(this.name, String(score), this.encodeValue(value)));
  }
  /** Java `addScoreAndGetRank(V, Number)`: atomic ZINCRBY + ZRANK (null rank → -1). */
  async addScoreAndGetRank(value: V, score: number): Promise<number> {
    const s = `redis.call('zincrby', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrank', KEYS[1], ARGV[2])`;
    const r = await luaEval(this.redis, s, 1, this.name, String(score), this.encodeValue(value));
    return r === null ? -1 : Number(r);
  }
  /** Java `addScoreAndGetRevRank(V, Number)`: atomic ZINCRBY + ZREVRANK (null rank → -1). */
  async addScoreAndGetRevRank(value: V, score: number): Promise<number> {
    const s = `redis.call('zincrby', KEYS[1], ARGV[1], ARGV[2])
return redis.call('zrevrank', KEYS[1], ARGV[2])`;
    const r = await luaEval(this.redis, s, 1, this.name, String(score), this.encodeValue(value));
    return r === null ? -1 : Number(r);
  }
  async pollFirst(count = 1): Promise<V[]> {
    const m: any = await (this.redis as any).zpopmin?.(this.name, count);
    if (!m) return [];
    const arr = Array.isArray(m) ? m : [m];
    const vals: V[] = [];
    for (let i = 0; i < arr.length; i += 2) vals.push(this.decodeValue(arr[i]) as V);
    return vals;
  }
  async pollLast(count = 1): Promise<V[]> {
    const m: any = await (this.redis as any).zpopmax?.(this.name, count);
    if (!m) return [];
    const arr = Array.isArray(m) ? m : [m];
    const vals: V[] = [];
    for (let i = 0; i < arr.length; i += 2) vals.push(this.decodeValue(arr[i]) as V);
    return vals;
  }
  /** Java `pollFirstEntry()`: non-blocking ZPOPMIN 1. */
  async pollFirstEntry(): Promise<ScoredEntry<V> | null> {
    const e = zPair(await (this.redis as any).zpopmin(this.name, 1));
    return e === null ? null : { value: this.decodeValue(e.value) as V, score: e.score };
  }
  /** Java `pollLastEntry()`. */
  async pollLastEntry(): Promise<ScoredEntry<V> | null> {
    const e = zPair(await (this.redis as any).zpopmax(this.name, 1));
    return e === null ? null : { value: this.decodeValue(e.value) as V, score: e.score };
  }
  /** Java blocking `pollFirst(long, TimeUnit)`: BZPOPMIN. `timeoutMs === null`
   *  blocks forever; `<= 0` is non-blocking. */
  async pollFirstWithTimeout(timeoutMs: number | null): Promise<ScoredEntry<V> | null> {
    const e = await blockingZPop(this.redis, this.name, 'MIN', timeoutMs);
    return e === null ? null : { value: this.decodeValue(e.value) as V, score: e.score };
  }
  /** Java blocking `pollLast(long, TimeUnit)`: BZPOPMAX. */
  async pollLastWithTimeout(timeoutMs: number | null): Promise<ScoredEntry<V> | null> {
    const e = await blockingZPop(this.redis, this.name, 'MAX', timeoutMs);
    return e === null ? null : { value: this.decodeValue(e.value) as V, score: e.score };
  }
  /** Java `pollFirstFromAny(...)`: BZMPOP MIN over this set + names; returns
   *  the source key plus member/score. `timeoutMs === null` blocks forever. */
  async pollFirstFromAny(timeoutMs: number | null, ...otherNames: string[]): Promise<{ key: string; value: V; score: number } | null> {
    const r = await blockingZPopFromAny(this.redis, [this.name, ...otherNames], 'MIN', timeoutMs);
    return r === null ? null : { key: r.key, value: this.decodeValue(r.value) as V, score: r.score };
  }
  /** Java `pollLastFromAny(...)`: BZMPOP MAX. */
  async pollLastFromAny(timeoutMs: number | null, ...otherNames: string[]): Promise<{ key: string; value: V; score: number } | null> {
    const r = await blockingZPopFromAny(this.redis, [this.name, ...otherNames], 'MAX', timeoutMs);
    return r === null ? null : { key: r.key, value: this.decodeValue(r.value) as V, score: r.score };
  }
  async first(): Promise<V | null> {
    const m = await this.redis.zrange(this.name, 0, 0);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
  }
  async last(): Promise<V | null> {
    const m = await this.redis.zrevrange(this.name, 0, 0);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
  }
  /** Java `firstEntry()`: ZRANGE 0 0 WITHSCORES. */
  async firstEntry(): Promise<ScoredEntry<V> | null> {
    const m = await this.redis.zrange(this.name, 0, 0, 'WITHSCORES');
    return m.length < 2 ? null : { value: this.decodeValue(m[0]) as V, score: Number(m[1]) };
  }
  /** Java `lastEntry()`: ZREVRANGE 0 0 WITHSCORES. */
  async lastEntry(): Promise<ScoredEntry<V> | null> {
    const m = await this.redis.zrevrange(this.name, 0, 0, 'WITHSCORES');
    return m.length < 2 ? null : { value: this.decodeValue(m[0]) as V, score: Number(m[1]) };
  }
  /** Java `firstScore()`: ZRANGE 0 0 WITHSCORES — `null` when empty. */
  async firstScore(): Promise<number | null> {
    const m = await this.redis.zrange(this.name, 0, 0, 'WITHSCORES');
    return m.length < 2 ? null : Number(m[1]);
  }
  /** Java `lastScore()`: ZRANGE -1 -1 WITHSCORES — `null` when empty. */
  async lastScore(): Promise<number | null> {
    const m = await this.redis.zrange(this.name, -1, -1, 'WITHSCORES');
    return m.length < 2 ? null : Number(m[1]);
  }
  /** Java score-bound formatting: exclusive → '(' prefix, ±inf → ±inf. */
  private scoreBound(score: number, inclusive: boolean): string {
    const s = score === Infinity ? '+inf' : score === -Infinity ? '-inf' : String(score);
    return inclusive ? s : `(${s}`;
  }
  /** Java `valueRangeReversed(int, int)`: ZREVRANGE by index. */
  async valueRangeReversed(startIndex: number, endIndex: number): Promise<V[]>;
  /** Java `valueRangeReversed(double, boolean, double, boolean[, int, int])`: ZREVRANGEBYSCORE (end bound first). */
  async valueRangeReversed(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<V[]>;
  async valueRangeReversed(a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<V[]> {
    if (typeof b === 'number') {
      const m = await this.redis.zrevrange(this.name, a, b);
      return m.map((x) => this.decodeValue(x) as V);
    }
    const args: (string | number)[] = [this.scoreBound(c!, d ?? true), this.scoreBound(a, b), 'BYSCORE', 'REV'];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrange(this.name, ...args);
    return m.map((x) => this.decodeValue(x) as V);
  }
  /** Java `entryRangeReversed(int, int)`: ZREVRANGE ... WITHSCORES by index. */
  async entryRangeReversed(startIndex: number, endIndex: number): Promise<ScoredEntry<V>[]>;
  /** Java `entryRangeReversed(double, boolean, double, boolean[, int, int])`: ZREVRANGEBYSCORE ... WITHSCORES. */
  async entryRangeReversed(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]>;
  async entryRangeReversed(a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]> {
    if (typeof b === 'number') {
      const m = await this.redis.zrevrange(this.name, a, b, 'WITHSCORES');
      return this.decodeScoredFlat(m);
    }
    const args: (string | number)[] = [this.scoreBound(c!, d ?? true), this.scoreBound(a, b), 'BYSCORE', 'REV', 'WITHSCORES'];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrange(this.name, ...args);
    return this.decodeScoredFlat(m);
  }
  /** Java `entryRange(int, int)`: ZRANGE ... WITHSCORES by index. */
  async entryRange(startIndex: number, endIndex: number): Promise<ScoredEntry<V>[]>;
  /** Java `entryRange(double, boolean, double, boolean[, int, int])`: ZRANGE ... BYSCORE WITHSCORES. */
  async entryRange(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]>;
  async entryRange(a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]> {
    if (typeof b === 'number') {
      const m = await this.redis.zrange(this.name, a, b, 'WITHSCORES');
      return this.decodeScoredFlat(m);
    }
    const args: (string | number)[] = [
      this.scoreBound(a, b), this.scoreBound(c!, d ?? true), 'BYSCORE', 'WITHSCORES',
    ];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrange(this.name, ...args);
    return this.decodeScoredFlat(m);
  }
  /** Java `count(double, boolean, double, boolean)`: ZCOUNT between the score bounds. */
  async count(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean): Promise<number> {
    return toNumber(await this.redis.zcount(
      this.name, this.scoreBound(startScore, startScoreInclusive), this.scoreBound(endScore, endScoreInclusive),
    ));
  }
  /** Java `removeRangeByRank(int, int)`: ZREMRANGEBYRANK. */
  async removeRangeByRank(startIndex: number, endIndex: number): Promise<number> {
    return toNumber(await this.redis.zremrangebyrank(this.name, startIndex, endIndex));
  }
  /** Java `removeRangeByScore(double, boolean, double, boolean)`: ZREMRANGEBYSCORE. */
  async removeRangeByScore(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean): Promise<number> {
    return toNumber(await this.redis.zremrangebyscore(
      this.name, this.scoreBound(startScore, startScoreInclusive), this.scoreBound(endScore, endScoreInclusive),
    ));
  }

  // -- Set operations (mirror Redisson argument order exactly; see
  // RedissonScoredSortedSet.java). Store variants compute over the OTHER
  // named sets and write the result into SELF (self is NOT an input);
  // read variants include SELF as the first input. Weights use plain
  // decimal strings (Java BigDecimal.toPlainString equivalent for our range).
  private async storeOp(cmd: 'zunionstore' | 'zinterstore' | 'zdiffstore', others: string[], weights?: number[], aggregate: ZAggregate = 'SUM'): Promise<number> {
    const args: (string | number)[] = [this.name, others.length, ...others];
    if ((cmd === 'zunionstore' || cmd === 'zinterstore') && weights && weights.length > 0) {
      args.push('WEIGHTS', ...weights.map(String));
    }
    if (cmd !== 'zdiffstore') args.push('AGGREGATE', aggregate);
    return toNumber(await (this.redis as any)[cmd](...args));
  }
  /** Union of the OTHER sets, stored into SELF. Returns result size. */
  async union(...names: string[]): Promise<number> {
    return this.storeOp('zunionstore', names);
  }
  async unionWith(opts: { names: string[]; weights?: number[]; aggregate?: ZAggregate }): Promise<number> {
    return this.storeOp('zunionstore', opts.names, opts.weights, opts.aggregate);
  }
  private decodeScoredFlat(m: string[]): ScoredEntry<V>[] {
    const out: ScoredEntry<V>[] = [];
    for (let i = 0; i + 1 < m.length; i += 2) out.push({ value: this.decodeValue(m[i]) as V, score: Number(m[i + 1]) });
    return out;
  }
  /** Union of SELF + others (read-only). */
  async readUnion(...names: string[]): Promise<V[]> {
    const m: string[] = await this.redis.zunion(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readUnionEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await this.redis.zunion(1 + names.length, this.name, ...names, 'WITHSCORES');
    return this.decodeScoredFlat(m);
  }
  /** Intersection of the OTHER sets, stored into SELF. Returns result size. */
  async intersection(...names: string[]): Promise<number> {
    return this.storeOp('zinterstore', names);
  }
  async intersectionWith(opts: { names: string[]; weights?: number[]; aggregate?: ZAggregate }): Promise<number> {
    return this.storeOp('zinterstore', opts.names, opts.weights, opts.aggregate);
  }
  /** Intersection of SELF + others (read-only). */
  async readIntersection(...names: string[]): Promise<V[]> {
    const m: string[] = await this.redis.zinter(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readIntersectionEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await this.redis.zinter(1 + names.length, this.name, ...names, 'WITHSCORES');
    return this.decodeScoredFlat(m);
  }
  /** Cardinality of SELF ∩ others (0 limit = exact). */
  async countIntersection(...names: string[]): Promise<number> {
    return toNumber(await this.redis.zintercard(1 + names.length, this.name, ...names));
  }
  async countIntersectionLimit(limit: number, ...names: string[]): Promise<number> {
    const args: (string | number)[] = [1 + names.length, this.name, ...names];
    if (limit > 0) args.push('LIMIT', limit);
    return toNumber(await (this.redis as any).zintercard(...args));
  }
  /** SELF-excluded difference (others[0] − others[1:]) stored into SELF. */
  async diff(...names: string[]): Promise<number> {
    return this.storeOp('zdiffstore', names);
  }
  /** SELF minus others (read-only). */
  async readDiff(...names: string[]): Promise<V[]> {
    const m: string[] = await this.redis.zdiff(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readDiffEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await this.redis.zdiff(1 + names.length, this.name, ...names, 'WITHSCORES');
    return this.decodeScoredFlat(m);
  }
  /** Java `addListener(ObjectListener)`: zadd/zrem/zincr and store events. */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.listenerHub.add(listener);
  }
  async removeListener(listenerId: number): Promise<void> {
    await this.listenerHub.remove(listenerId);
  }
}

/** RLexSortedSet stores RAW members (no codec) to preserve lexicographic byte order. */
export class RLexSortedSet extends RedisDataStructure {
  async add(value: string): Promise<boolean> {
    return ((await this.redis.zadd(this.name, '0', value)) as number) > 0;
  }
  async remove(value: string): Promise<boolean> {
    return ((await this.redis.zrem(this.name, value)) as number) > 0;
  }
  async contains(value: string): Promise<boolean> {
    return ((await this.redis.zscore(this.name, value)) as string | null) !== null;
  }
  async size(): Promise<number> {
    return this.redis.zcard(this.name);
  }
  async rangeByLex(min: string, max: string, offset = 0, count = -1): Promise<string[]> {
    const args: (string | number)[] = [this.name, min, max];
    if (count >= 0) args.push('LIMIT', offset, count);
    return (this.redis as any).zrangebylex(...args);
  }
  async removeRangeByLex(min: string, max: string): Promise<number> {
    return this.redis.zremrangebylex(this.name, min, max);
  }
  async countByLex(min: string, max: string): Promise<number> {
    return this.redis.zlexcount(this.name, min, max);
  }
  /** inclusive/exclusive lex bound token ('[' / '(' + value) — Java's
   *  range(from, fromInclusive, to, toInclusive) family builds these. */
  private lexBound(value: string, inclusive: boolean): string {
    return (inclusive ? '[' : '(') + value;
  }
  /** Java `range(from, fromInclusive, to, toInclusive[, offset, count])`. */
  async range(from: string, fromInclusive: boolean, to: string, toInclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.rangeByLex(this.lexBound(from, fromInclusive), this.lexBound(to, toInclusive), offset, count);
  }
  /** Java `rangeHead(value, inclusive[, offset, count])`: (-inf, value]. */
  async rangeHead(value: string, inclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.rangeByLex('-', this.lexBound(value, inclusive), offset, count);
  }
  /** Java `rangeTail(value, inclusive[, offset, count])`: [value, +inf). */
  async rangeTail(value: string, inclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.rangeByLex(this.lexBound(value, inclusive), '+', offset, count);
  }
  /** Java `rangeReversed(from, fromInclusive, to, toInclusive[, offset, count])`:
   *  ZREVRANGEBYLEX takes max before min. */
  async rangeReversed(from: string, fromInclusive: boolean, to: string, toInclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.revRangeByLex(this.lexBound(to, toInclusive), this.lexBound(from, fromInclusive), offset, count);
  }
  async rangeHeadReversed(value: string, inclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.revRangeByLex(this.lexBound(value, inclusive), '-', offset, count);
  }
  async rangeTailReversed(value: string, inclusive: boolean, offset = 0, count = -1): Promise<string[]> {
    return this.revRangeByLex('+', this.lexBound(value, inclusive), offset, count);
  }
  private async revRangeByLex(max: string, min: string, offset = 0, count = -1): Promise<string[]> {
    const args: (string | number)[] = [this.name, max, min];
    if (count >= 0) args.push('LIMIT', offset, count);
    return (this.redis as any).zrevrangebylex(...args);
  }
  /** Java `count(from, fromInclusive, to, toInclusive)`. */
  async count(from: string, fromInclusive: boolean, to: string, toInclusive: boolean): Promise<number> {
    return this.countByLex(this.lexBound(from, fromInclusive), this.lexBound(to, toInclusive));
  }
  async countHead(value: string, inclusive: boolean): Promise<number> {
    return this.countByLex('-', this.lexBound(value, inclusive));
  }
  async countTail(value: string, inclusive: boolean): Promise<number> {
    return this.countByLex(this.lexBound(value, inclusive), '+');
  }
  /** Java `removeRange(from, fromInclusive, to, toInclusive)`: removed count. */
  async removeRange(from: string, fromInclusive: boolean, to: string, toInclusive: boolean): Promise<number> {
    return this.removeRangeByLex(this.lexBound(from, fromInclusive), this.lexBound(to, toInclusive));
  }
  async removeRangeHead(value: string, inclusive: boolean): Promise<number> {
    return this.removeRangeByLex('-', this.lexBound(value, inclusive));
  }
  async removeRangeTail(value: string, inclusive: boolean): Promise<number> {
    return this.removeRangeByLex(this.lexBound(value, inclusive), '+');
  }
  /** Java `rank(String)`/`revRank(String)`: -1 when absent. */
  async revRank(value: string): Promise<number> {
    const r = await this.redis.zrevrank(this.name, value);
    return r === null ? -1 : Number(r);
  }
  async pollFirst(): Promise<string | null> {
    const r = await (this.redis as any).zpopmin(this.name);
    return Array.isArray(r) && r.length > 0 ? String(r[0]) : null;
  }
  async pollLast(): Promise<string | null> {
    const r = await (this.redis as any).zpopmax(this.name);
    return Array.isArray(r) && r.length > 0 ? String(r[0]) : null;
  }
  /** Java `random()`. */
  async random(): Promise<string | null> {
    const r = await (this.redis as any).zrandmember(this.name);
    return r === null || r === undefined ? null : String(r);
  }
  async rank(value: string): Promise<number> {
    const r = await this.redis.zrank(this.name, value);
    return r === null ? -1 : r;
  }
}
