/**
 * Buckets / Maps / Sets / Lists / Queues / Sorted sets.
 * Wire: bare name + JSON codec values (map keys/geo members/stream fields JSON-encoded).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base';
import type { Codec } from './codec';
import { JsonCodec } from './codec';
import { luaEval, prefixName, toNumber } from './util';

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
      await (this.redis as any).set(this.name, enc, 'KEEPTTL');
    } catch {
      await this.redis.set(this.name, enc);
    }
  }
  async trySet(value: V, ttlMs?: number): Promise<boolean> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) {
      const r = await (this.redis as any).set(this.name, enc, 'PX', ttlMs, 'NX');
      return r === 'OK';
    }
    return ((await this.redis.set(this.name, enc, 'NX')) as unknown) === 'OK';
  }
  async setIfExists(value: V, ttlMs?: number): Promise<boolean> {
    const enc = this.encodeValue(value);
    if (ttlMs && ttlMs > 0) {
      const r = await (this.redis as any).set(this.name, enc, 'PX', ttlMs, 'XX');
      return r === 'OK';
    }
    return ((await (this.redis as any).set(this.name, enc, 'XX')) as unknown) === 'OK';
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
      const raw = await (this.redis as any).getex(this.name, 'PX', ttlMs);
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

export class RMap<K = any, V = any> extends RedisDataStructure {
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

export class RSet<V = any> extends RedisDataStructure {
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
   * Store variants mirror Redisson exactly: the result is written into SELF
   * (self IS an input, unlike the scored-sorted-set variants) and the
   * returned number is the new cardinality. Command shape is
   * `SOPSTORE self (1+N) self others...` — the explicit numkeys is required;
   * without it correct servers reject the command. The previous read-only
   * methods of the same names are now readUnion/readDiff/readIntersection.
   */
  async union(...names: string[]): Promise<number> {
    return toNumber(await (this.redis as any).sunionstore(this.name, 1 + names.length, this.name, ...names));
  }
  async diff(...names: string[]): Promise<number> {
    return toNumber(await (this.redis as any).sdiffstore(this.name, 1 + names.length, this.name, ...names));
  }
  async intersection(...names: string[]): Promise<number> {
    return toNumber(await (this.redis as any).sinterstore(this.name, 1 + names.length, this.name, ...names));
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
  /** Cardinality of the union without storing (SUNIONCARD, Redis 7+; via
   * `.call` — absent from ioredis typed methods like XACKDEL). */
  async countUnion(...names: string[]): Promise<number> {
    return toNumber(await (this.redis as any).call('SUNIONCARD', 1 + names.length, this.name, ...names));
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

export class RBlockingQueue<V = any> extends RQueue<V> {
  async take(): Promise<V> {
    for (;;) {
      const r = await (this.redis as any).blpop(this.name, 0);
      if (r) {
        const raw = Array.isArray(r) ? r[1] : r;
        return this.decodeValue(raw) as V;
      }
    }
  }
  async pollWithTimeout(timeoutMs: number): Promise<V | null> {
    const secs = Math.max(1, Math.ceil(timeoutMs / 1000));
    const r = await (this.redis as any).blpop(this.name, timeoutMs <= 0 ? 0 : secs);
    if (!r) return null;
    const raw = Array.isArray(r) ? r[1] : r;
    return this.decodeValue(raw) as V;
  }
  async put(value: V): Promise<void> {
    await this.offer(value);
  }
  async drainTo(target: RQueue<V>, maxElements = -1): Promise<number> {
    const all = await this.redis.lrange(this.name, 0, maxElements < 0 ? -1 : maxElements - 1);
    if (all.length === 0) return 0;
    await this.redis.ltrim(this.name, all.length, -1);
    for (const raw of all) await target.offer(this.decodeValue(raw) as V);
    return all.length;
  }
}

export class RBlockingDeque<V = any> extends RDeque<V> {
  async takeFirst(): Promise<V> {
    for (;;) {
      const r = await (this.redis as any).blpop(this.name, 0);
      if (r) return this.decodeValue(Array.isArray(r) ? r[1] : r) as V;
    }
  }
  async takeLast(): Promise<V> {
    for (;;) {
      const r = await (this.redis as any).brpop(this.name, 0);
      if (r) return this.decodeValue(Array.isArray(r) ? r[1] : r) as V;
    }
  }
  async pollFirstWithTimeout(timeoutMs: number): Promise<V | null> {
    const r = await (this.redis as any).blpop(this.name, Math.max(1, Math.ceil(timeoutMs / 1000)));
    if (!r) return null;
    return this.decodeValue(Array.isArray(r) ? r[1] : r) as V;
  }
  async pollLastWithTimeout(timeoutMs: number): Promise<V | null> {
    const r = await (this.redis as any).brpop(this.name, Math.max(1, Math.ceil(timeoutMs / 1000)));
    if (!r) return null;
    return this.decodeValue(Array.isArray(r) ? r[1] : r) as V;
  }
}

export class RBoundedBlockingQueue<V = any> extends RBlockingQueue<V> {
  private capacityKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.capacityKey = prefixName('redisson_bqs', name);
  }
  async trySetCapacity(capacity: number): Promise<boolean> {
    return ((await this.redis.setnx(this.capacityKey, String(capacity))) as number) === 1;
  }
  async getCapacity(): Promise<number> {
    const v = await this.redis.get(this.capacityKey);
    return v === null ? 0 : Number(v);
  }
  override async offer(value: V): Promise<boolean> {
    const cap = await this.getCapacity();
    const s = `local cap = tonumber(redis.call('get', KEYS[2]))
if cap ~= nil then
    local size = redis.call('llen', KEYS[1])
    if size >= cap then return 0 end
end
redis.call('rpush', KEYS[1], ARGV[1])
return 1`;
    void cap;
    return toNumber(await luaEval(this.redis, s, 2, this.name, this.capacityKey, this.encodeValue(value))) === 1;
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
    return ((await (this.redis as any).zadd(this.name, 'NX', String(score), this.encodeValue(value))) as number) > 0;
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
  async rank(value: V): Promise<number> {
    const r = await this.redis.zrank(this.name, this.encodeValue(value));
    return r === null ? -1 : r;
  }
  async revRank(value: V): Promise<number> {
    const r = await this.redis.zrevrank(this.name, this.encodeValue(value));
    return r === null ? -1 : r;
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
    const m: string[] = await (this.redis as any).zunion(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readUnionEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await (this.redis as any).zunion(1 + names.length, this.name, ...names, 'WITHSCORES');
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
    const m: string[] = await (this.redis as any).zinter(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readIntersectionEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await (this.redis as any).zinter(1 + names.length, this.name, ...names, 'WITHSCORES');
    return this.decodeScoredFlat(m);
  }
  /** Cardinality of SELF ∩ others (0 limit = exact). */
  async countIntersection(...names: string[]): Promise<number> {
    return toNumber(await (this.redis as any).zintercard(1 + names.length, this.name, ...names));
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
    const m: string[] = await (this.redis as any).zdiff(1 + names.length, this.name, ...names);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async readDiffEntries(...names: string[]): Promise<ScoredEntry<V>[]> {
    const m: string[] = await (this.redis as any).zdiff(1 + names.length, this.name, ...names, 'WITHSCORES');
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
    return (this.redis as any).zremrangebylex(this.name, min, max);
  }
  async countByLex(min: string, max: string): Promise<number> {
    return (this.redis as any).zlexcount(this.name, min, max);
  }
  async rank(value: string): Promise<number> {
    const r = await this.redis.zrank(this.name, value);
    return r === null ? -1 : r;
  }
}
