/**
 * Buckets / Maps / Sets / Lists / Queues / Sorted sets.
 * Wire: bare name + JSON codec values (map keys/geo members/stream fields JSON-encoded).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import { RedisKeyedStructure } from './synchronizers.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { luaEval, prefixName, sleep, toNumber, withDedicatedRedis } from './util.js';

export class RBucket<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
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
}

export class RMap<K = any, V = any> extends RedisKeyedStructure<K> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
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
  async replace(key: K, value: V): Promise<V | null> {
    return this.putIfExists(key, value);
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
  async putAll(mapping: Record<string, V> | Map<K, V>): Promise<void> {
    const entries = mapping instanceof Map ? [...mapping.entries()] : Object.entries(mapping);
    if (entries.length === 0) return;
    const flat: string[] = [];
    for (const [k, v] of entries as any[]) flat.push(this.encodeKey(k), this.encodeValue(v));
    // chunk to avoid oversized commands
    for (let i = 0; i < flat.length; i += 2000) {
      await this.redis.hset(this.name, ...flat.slice(i, i + 2000) as [string, string]);
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
  override async delete(): Promise<boolean> {
    return super.delete();
  }
}

export class RSet<V = any> extends RedisKeyedStructure<V> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
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
  async poll(): Promise<V | null> {
    const raw = await this.redis.lpop(this.name);
    return raw === null ? null : (this.decodeValue(raw) as V);
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
  async addFirst(value: V): Promise<void> {
    await this.redis.lpush(this.name, this.encodeValue(value));
  }
  async addLast(value: V): Promise<void> {
    await this.redis.rpush(this.name, this.encodeValue(value));
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
  async pollFirst(): Promise<V | null> {
    return this.poll();
  }
  async pollLast(): Promise<V | null> {
    return this.removeLast();
  }
}

function isUnsupportedCommand(e: unknown): boolean {
  const msg = String((e as any)?.message ?? e);
  return /unsupported command|unknown command/i.test(msg);
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

/** BLPOP/BRPOP on a dedicated connection. `timeoutMs === null` blocks forever;
 * `<= 0` is non-blocking (LPOP/RPOP); positive values use Redis 6+ float
 * seconds. Mock clients without BLPOP fall back to short-interval polling. */
async function blockingListPop(
  redis: Redis,
  name: string,
  side: 'left' | 'right',
  timeoutMs: number | null,
): Promise<string | null> {
  const pop = (conn: Redis) => (side === 'left' ? conn.lpop(name) : conn.rpop(name));
  if (timeoutMs !== null && timeoutMs <= 0) return pop(redis);
  const immediate = await pop(redis);
  if (immediate !== null) return immediate;

  return withDedicatedRedis(redis, async (conn, dedicated) => {
    const deadline = timeoutMs === null ? Infinity : Date.now() + timeoutMs;
    const poll = async (): Promise<string | null> => {
      for (;;) {
        const v = await pop(redis);
        if (v !== null) return v;
        const remain = deadline === Infinity ? 50 : deadline - Date.now();
        if (remain <= 0) return null;
        await sleep(Math.min(50, remain));
      }
    };
    if (!dedicated) return poll();
    try {
      const cmd = side === 'left' ? 'blpop' : 'brpop';
      const seconds = timeoutMs === null ? 0 : String(timeoutMs / 1000);
      const r = await (conn as any)[cmd](name, seconds);
      return r ? (Array.isArray(r) ? r[1] : r) : null;
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
      const raw = await blockingListPop(this.redis, this.name, 'left', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  async pollWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'left', timeoutMs);
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
}

export class RBlockingDeque<V = any> extends RDeque<V> {
  async takeFirst(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'left', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  async takeLast(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'right', null);
      if (raw !== null) return this.decodeValue(raw) as V;
    }
  }
  async pollFirstWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'left', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async pollLastWithTimeout(timeoutMs: number): Promise<V | null> {
    const raw = await blockingListPop(this.redis, this.name, 'right', timeoutMs);
    return raw === null ? null : (this.decodeValue(raw) as V);
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

  override async poll(): Promise<V | null> {
    const raw = await luaEval(this.redis, BQ_POLL, 3, this.name, this.capacityKey, this.channel, '1');
    return raw === null || raw === undefined || raw === false ? null : (this.decodeValue(raw) as V);
  }

  override async take(): Promise<V> {
    for (;;) {
      const raw = await blockingListPop(this.redis, this.name, 'left', null);
      if (raw !== null) {
        await this.releaseCapacity(1);
        return this.decodeValue(raw) as V;
      }
    }
  }

  override async pollWithTimeout(timeoutMs: number): Promise<V | null> {
    if (timeoutMs <= 0) return this.poll();
    const raw = await blockingListPop(this.redis, this.name, 'left', timeoutMs);
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
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async add(score: number, value: V): Promise<boolean> {
    return ((await this.redis.zadd(this.name, String(score), this.encodeValue(value))) as number) > 0;
  }
  async addIfAbsent(score: number, value: V): Promise<boolean> {
    return ((await this.redis.zadd(this.name, 'NX', String(score), this.encodeValue(value))) as number) > 0;
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
  async size(): Promise<number> {
    return this.redis.zcard(this.name);
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
    const args: (string | number)[] = [this.name, String(min), String(max)];
    if (count >= 0) args.push('LIMIT', offset, count);
    const m = await (this.redis as any).zrangebyscore(...args);
    return (m as string[]).map((x) => this.decodeValue(x) as V);
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
  async first(): Promise<V | null> {
    const m = await this.redis.zrange(this.name, 0, 0);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
  }
  async last(): Promise<V | null> {
    const m = await this.redis.zrevrange(this.name, 0, 0);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
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
    const args: (string | number)[] = [this.name, this.scoreBound(c!, d ?? true), this.scoreBound(a, b)];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrevrangebyscore(...args);
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
    const args: (string | number)[] = [this.name, this.scoreBound(c!, d ?? true), this.scoreBound(a, b), 'WITHSCORES'];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrevrangebyscore(...args);
    return this.decodeScoredFlat(m);
  }
  /** Java `entryRange(int, int)`: ZRANGE ... WITHSCORES by index. */
  async entryRange(startIndex: number, endIndex: number): Promise<ScoredEntry<V>[]>;
  /** Java `entryRange(double, boolean, double, boolean[, int, int])`: ZRANGEBYSCORE ... WITHSCORES. */
  async entryRange(startScore: number, startScoreInclusive: boolean, endScore: number, endScoreInclusive: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]>;
  async entryRange(a: number, b: number | boolean, c?: number, d?: boolean, offset?: number, count?: number): Promise<ScoredEntry<V>[]> {
    if (typeof b === 'number') {
      const m = await this.redis.zrange(this.name, a, b, 'WITHSCORES');
      return this.decodeScoredFlat(m);
    }
    const args: (string | number)[] = [
      this.name, this.scoreBound(a, b), this.scoreBound(c!, d ?? true), 'WITHSCORES',
    ];
    if (count !== undefined && count >= 0) args.push('LIMIT', offset ?? 0, count);
    const m: string[] = await (this.redis as any).zrangebyscore(...args);
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
  async rank(value: string): Promise<number> {
    const r = await this.redis.zrank(this.name, value);
    return r === null ? -1 : r;
  }
}
