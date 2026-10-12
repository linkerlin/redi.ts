/**
 * Cache structures: RMapCache (+Native), RSetCache, Multimaps (+Cache/Native),
 * RLocalCachedMap. Wire matches Redisson 4.6.1 (struct-packed values, companion ZSETs).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base';
import type { Codec } from './codec';
import { JsonCodec } from './codec';
import {
  encodeMapCacheEvent,
  multimapInternalId,
  packMapCacheValue,
  prefixName,
  serverNowMs,
  sleep,
  suffixName,
  unpackMapCacheValue,
} from './util';
import { RMap } from './collections';

export class RMapCache<K = any, V = any> extends RedisDataStructure {
  private ttlKey: string;
  private idleKey: string;
  private accessKey: string;
  private optionsKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.ttlKey = prefixName('redisson__timeout__set', name);
    this.idleKey = prefixName('redisson__idle__set', name);
    this.accessKey = prefixName('redisson__map_cache__last_access__set', name);
    this.optionsKey = suffixName(name, 'redisson_options');
  }

  // NOTE: plain commands (no Lua) throughout: packed values carry arbitrary
  // bytes, which must travel as Buffers. ioredis string args/replies are
  // UTF-8 and would corrupt them on real Redis. Layout and packed format are
  // unchanged (Java/Go wire-compatible); only transport is binary-safe.
  // Capacity eviction is best-effort client-side (heals on next op).

  private async evict(): Promise<void> {
    const now = await serverNowMs(this.redis);
    for (const setKey of [this.ttlKey, this.idleKey]) {
      let expired: string[];
      try {
        expired = await this.redis.zrangebyscore(setKey, '0', String(now));
      } catch {
        continue;
      }
      if (expired.length === 0) continue;
      // exact bytes for event payloads
      let raws: (Buffer | null)[] = [];
      try {
        raws = await (this.redis as any).hmgetBuffer(this.name, ...expired);
      } catch {
        raws = expired.map(() => null);
      }
      for (let i = 0; i < expired.length; i++) {
        await this.redis.hdel(this.name, expired[i]);
        await this.redis.zrem(this.ttlKey, expired[i]);
        await this.redis.zrem(this.idleKey, expired[i]);
        await this.redis.zrem(this.accessKey, expired[i]);
        if (raws[i] !== null && raws[i] !== undefined) {
          const v = this.decodeStored(raws[i]);
          if (v !== null) this.publishEvent('expired', expired[i], this.encodeValue(v));
        }
      }
    }
  }

  private toBuf(raw: unknown): Buffer | null {
    if (raw === null || raw === undefined) return null;
    if (Buffer.isBuffer(raw)) return raw;
    return Buffer.from(String(raw), 'latin1');
  }

  private decodeStored(raw: Buffer | string | null): V | null {
    const buf = this.toBuf(raw);
    if (buf === null) return null;
    const u = unpackMapCacheValue(buf);
    const payload = u.ok ? u.codec : buf.toString('utf8');
    try {
      return this.codec.decode(payload) as V;
    } catch {
      return payload as unknown as V;
    }
  }

  private publishEvent(kind: 'created' | 'updated' | 'removed' | 'expired', field: string, ...values: string[]): void {
    const ch = `redisson_map_cache_${kind}:{${this.name}}`;
    const msg = encodeMapCacheEvent([field, ...values]);
    void this.redis.publish(ch, msg).catch(() => undefined);
  }

  private async touchAccess(field: string, now: number): Promise<void> {
    try {
      const mode = await this.redis.hget(this.optionsKey, 'mode');
      if (mode === null || mode === 'LRU') await this.redis.zadd(this.accessKey, String(now), field);
      else await this.redis.zincrby(this.accessKey, 1, field);
    } catch { /* noop */ }
  }

  async get(key: K): Promise<V | null> {
    await this.evict();
    const field = this.encodeKey(key);
    let raw: unknown;
    try {
      raw = await (this.redis as any).hgetBuffer(this.name, field);
    } catch {
      raw = await this.redis.hget(this.name, field);
    }
    const buf = this.toBuf(raw);
    if (buf === null) return null;
    const u = unpackMapCacheValue(buf);
    if (!u.ok) {
      // bare value (RMap-written): touch last-access only
      try {
        const now = await serverNowMs(this.redis);
        await this.touchAccess(field, now);
      } catch { /* noop */ }
      try {
        return this.codec.decode(buf.toString('utf8')) as V;
      } catch {
        return buf.toString('utf8') as unknown as V;
      }
    }
    try {
      const now = await serverNowMs(this.redis);
      if (u.maxIdle > 0) await this.redis.zadd(this.idleKey, String(now + u.maxIdle), field);
      await this.touchAccess(field, now);
    } catch { /* noop */ }
    try {
      return this.codec.decode(u.codec) as V;
    } catch {
      return u.codec as unknown as V;
    }
  }

  async put(key: K, value: V, ttlMs = 0, maxIdleMs = 0): Promise<V | null> {
    await this.evict();
    const field = this.encodeKey(key);
    const enc = this.encodeValue(value);
    const old = await this.get(key);
    const now = await serverNowMs(this.redis);
    const packed = Buffer.from(packMapCacheValue(enc, maxIdleMs), 'latin1');
    const ttlScore = ttlMs > 0 ? now + ttlMs : 0;
    const idleScore = maxIdleMs > 0 ? now + maxIdleMs : 0;
    await this.redis.hset(this.name, field, packed);
    if (ttlScore > 0) await this.redis.zadd(this.ttlKey, String(ttlScore), field);
    else await this.redis.zrem(this.ttlKey, field);
    if (idleScore > 0) await this.redis.zadd(this.idleKey, String(idleScore), field);
    else await this.redis.zrem(this.idleKey, field);
    await this.touchAccess(field, now);
    // capacity eviction (best-effort; heals on next op on race)
    try {
      const maxSizeRaw = await this.redis.hget(this.optionsKey, 'max-size');
      const maxSize = maxSizeRaw === null ? 0 : Number(maxSizeRaw);
      if (maxSize > 0) {
        const size = await this.redis.hlen(this.name);
        const excess = size - maxSize;
        if (excess > 0) {
          const victims = await this.redis.zrange(this.accessKey, 0, excess - 1);
          for (const v of victims) {
            if (v === field) continue;
            await this.redis.hdel(this.name, v);
            await this.redis.zrem(this.ttlKey, v);
            await this.redis.zrem(this.idleKey, v);
            await this.redis.zrem(this.accessKey, v);
          }
        }
      }
    } catch { /* noop */ }
    this.publishEvent(
      old === null ? 'created' : 'updated',
      field,
      ...(old === null ? [enc] : [enc, this.encodeValue(old)]),
    );
    return old;
  }

  async fastPut(key: K, value: V, ttlMs = 0, maxIdleMs = 0): Promise<boolean> {
    const old = await this.put(key, value, ttlMs, maxIdleMs);
    return old === null;
  }

  async putIfAbsent(key: K, value: V, ttlMs = 0, maxIdleMs = 0): Promise<V | null> {
    const cur = await this.get(key);
    if (cur !== null) return cur;
    await this.put(key, value, ttlMs, maxIdleMs);
    return null;
  }

  async remove(key: K): Promise<V | null> {
    await this.evict();
    const field = this.encodeKey(key);
    const old = await this.get(key);
    await this.redis.hdel(this.name, field);
    await this.redis.zrem(this.ttlKey, field);
    await this.redis.zrem(this.idleKey, field);
    await this.redis.zrem(this.accessKey, field);
    if (old !== null) this.publishEvent('removed', field, this.encodeValue(old));
    return old;
  }

  async containsKey(key: K): Promise<boolean> {
    return (await this.get(key)) !== null;
  }

  async size(): Promise<number> {
    await this.evict();
    return this.redis.hlen(this.name);
  }

  async readAllMap(): Promise<Map<K, V>> {
    await this.evict();
    let all: Record<string, Buffer | string>;
    try {
      all = await (this.redis as any).hgetallBuffer(this.name);
    } catch {
      all = await this.redis.hgetall(this.name);
    }
    const out = new Map<K, V>();
    for (const [f, raw] of Object.entries(all ?? {})) {
      const v = this.decodeStored(raw as Buffer);
      if (v !== null) {
        try {
          out.set(this.decodeKey(f) as K, v);
        } catch { /* skip */ }
      }
    }
    return out;
  }

  async trySetMaxSize(size: number, mode: 'LRU' | 'LFU' = 'LRU'): Promise<boolean> {
    const a = await this.redis.hsetnx(this.optionsKey, 'max-size', String(size));
    await this.redis.hsetnx(this.optionsKey, 'mode', mode);
    return a === 1;
  }
  async setMaxSize(size: number, mode: 'LRU' | 'LFU' = 'LRU'): Promise<void> {
    await this.redis.hset(this.optionsKey, 'max-size', String(size), 'mode', mode);
  }
  async clear(): Promise<void> {
    await this.redis.del(this.name, this.ttlKey, this.idleKey, this.accessKey);
  }
  override async delete(): Promise<boolean> {
    await this.clear();
    await this.redis.del(this.optionsKey);
    return true;
  }
  async destroy(): Promise<void> {
    await this.clear();
  }
}

export class RMapCacheNative<K = any, V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async put(key: K, value: V, ttlMs = 0): Promise<V | null> {
    const field = this.encodeKey(key);
    const old = await this.get(key);
    await this.redis.hset(this.name, field, this.encodeValue(value));
    if (ttlMs > 0) {
      try {
        await (this.redis as any).hpexpire(this.name, ttlMs, 'FIELDS', 1, field);
      } catch { /* server too old */ }
    }
    return old;
  }
  async get(key: K): Promise<V | null> {
    const raw = await this.redis.hget(this.name, this.encodeKey(key));
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async remove(key: K): Promise<V | null> {
    const old = await this.get(key);
    await this.redis.hdel(this.name, this.encodeKey(key));
    return old;
  }
  async remainTTL(key: K): Promise<number> {
    try {
      const r: any = await (this.redis as any).hpttl(this.name, 'FIELDS', 1, this.encodeKey(key));
      if (Array.isArray(r)) return Number(r[0]);
      return Number(r);
    } catch {
      return -1;
    }
  }
}

export class RSetCache<V = any> extends RedisDataStructure {
  private idleKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.idleKey = prefixName('redisson__idle__set', name);
  }
  private async purge(now: number): Promise<void> {
    try {
      const expired: string[] = await this.redis.zrangebyscore(this.name, '0', String(now));
      if (expired.length > 0) {
        await this.redis.zrem(this.name, ...expired);
        await this.redis.zrem(this.idleKey, ...expired);
      }
    } catch { /* noop */ }
  }
  async add(value: V, ttlMs = 0, maxIdleMs = 0): Promise<boolean> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const enc = this.encodeValue(value);
    const expiry = ttlMs > 0 ? now + ttlMs : 9223372036854775807;
    const exists = await this.redis.zscore(this.name, enc);
    await this.redis.zadd(this.name, String(expiry), enc);
    if (maxIdleMs > 0) await this.redis.zadd(this.idleKey, String(maxIdleMs), enc);
    else await this.redis.zrem(this.idleKey, enc);
    return exists === null;
  }
  async contains(value: V): Promise<boolean> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const enc = this.encodeValue(value);
    const score = await this.redis.zscore(this.name, enc);
    if (score === null) return false;
    const idle = await this.redis.zscore(this.idleKey, enc);
    if (idle !== null) {
      await this.redis.zadd(this.name, String(now + Number(idle)), enc);
    }
    return true;
  }
  async remove(value: V): Promise<boolean> {
    const enc = this.encodeValue(value);
    const n = await this.redis.zrem(this.name, enc);
    await this.redis.zrem(this.idleKey, enc);
    return n > 0;
  }
  async size(): Promise<number> {
    await this.purge(await serverNowMs(this.redis));
    return this.redis.zcard(this.name);
  }
  async readAll(): Promise<V[]> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const m = await this.redis.zrangebyscore(this.name, String(now + 1), '9223372036854775807');
    return m.map((x) => this.decodeValue(x) as V);
  }
}

// -- Multimaps ---------------------------------------------------------------

abstract class RMultimapBase<K = any, V = any> extends RedisDataStructure {
  protected isSet: boolean;
  constructor(redis: Redis, name: string, codec: Codec, isSet: boolean) {
    super(redis, name, codec);
    this.isSet = isSet;
  }
  protected collKey(id: string): string {
    return suffixName(this.name, id);
  }
  protected async idFor(field: string, create: boolean): Promise<string | null> {
    const existing = await this.redis.hget(this.name, field);
    if (existing !== null) return existing;
    if (!create) return null;
    const id = multimapInternalId(field);
    await this.redis.hset(this.name, field, id);
    return id;
  }
  async put(key: K, value: V): Promise<boolean> {
    const field = this.encodeKey(key);
    const enc = this.encodeValue(value);
    const id = (await this.idFor(field, true))!;
    const ck = this.collKey(id);
    if (this.isSet) return ((await this.redis.sadd(ck, enc)) as number) > 0;
    await this.redis.rpush(ck, enc);
    return true;
  }
  async get(key: K): Promise<V[]> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return [];
    const ck = this.collKey(id);
    const raw = this.isSet ? await this.redis.smembers(ck) : await this.redis.lrange(ck, 0, -1);
    return raw.map((x) => this.decodeValue(x) as V);
  }
  async remove(key: K, value?: V): Promise<boolean> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return false;
    const ck = this.collKey(id);
    if (value === undefined) {
      await this.redis.del(ck);
      await this.redis.hdel(this.name, field);
      return true;
    }
    const enc = this.encodeValue(value);
    const n = this.isSet ? await this.redis.srem(ck, enc) : await this.redis.lrem(ck, 1, enc);
    const left = this.isSet ? await this.redis.scard(ck) : await this.redis.llen(ck);
    if (left === 0) {
      await this.redis.del(ck);
      await this.redis.hdel(this.name, field);
    }
    return n > 0;
  }
  async containsKey(key: K): Promise<boolean> {
    return ((await this.redis.hexists(this.name, this.encodeKey(key))) as number) === 1;
  }
  async keySet(): Promise<K[]> {
    const fields = await this.redis.hkeys(this.name);
    return fields.map((f) => {
      try {
        return this.decodeKey(f) as K;
      } catch {
        return f as unknown as K;
      }
    });
  }
  async size(): Promise<number> {
    return this.redis.hlen(this.name);
  }
  async clear(): Promise<void> {
    const ids = await this.redis.hvals(this.name);
    if (ids.length > 0) {
      for (let i = 0; i < ids.length; i += 500) {
        await this.redis.del(...ids.slice(i, i + 500).map((id) => this.collKey(id)));
      }
    }
    await this.redis.del(this.name);
  }
}

export class RSetMultimap<K = any, V = any> extends RMultimapBase<K, V> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec, true);
  }
}
export class RListMultimap<K = any, V = any> extends RMultimapBase<K, V> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec, false);
  }
}

abstract class RMultimapCacheBase<K = any, V = any> extends RMultimapBase<K, V> {
  protected timeoutKey: string;
  constructor(redis: Redis, name: string, codec: Codec, isSet: boolean, kind: 'set' | 'list') {
    super(redis, name, codec, isSet);
    this.timeoutKey = suffixName(name, `redisson_${kind}_multimap_ttl`);
  }
  private async checkExpiry(field: string): Promise<void> {
    const score = await this.redis.zscore(this.timeoutKey, field);
    if (score !== null) {
      const now = await serverNowMs(this.redis);
      if (Number(score) <= now) {
        const id = await this.redis.hget(this.name, field);
        if (id !== null) await this.redis.del(this.collKey(id));
        await this.redis.hdel(this.name, field);
        await this.redis.zrem(this.timeoutKey, field);
      }
    }
  }
  override async get(key: K): Promise<V[]> {
    await this.checkExpiry(this.encodeKey(key));
    return super.get(key);
  }
  override async put(key: K, value: V): Promise<boolean> {
    await this.checkExpiry(this.encodeKey(key));
    return super.put(key, value);
  }
  async expireKey(key: K, ttlMs: number): Promise<boolean> {
    const field = this.encodeKey(key);
    if (!await this.containsKey(key)) return false;
    const now = await serverNowMs(this.redis);
    if (ttlMs <= 0) {
      await this.redis.zrem(this.timeoutKey, field);
      return true;
    }
    await this.redis.zadd(this.timeoutKey, String(now + ttlMs), field);
    return true;
  }
}

export class RSetMultimapCache<K = any, V = any> extends RMultimapCacheBase<K, V> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec, true, 'set');
  }
}
export class RListMultimapCache<K = any, V = any> extends RMultimapCacheBase<K, V> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec, false, 'list');
  }
}

// -- RLocalCachedMap (data layer = RMap wire; invalidation = local JSON broadcast) --

export interface LocalCachedMapOptions {
  maxSize?: number;
  timeToLiveMs?: number;
  maxIdleMs?: number;
  evictionPolicy?: 'NONE' | 'LRU' | 'LFU';
  syncStrategy?: 'INVALIDATE' | 'UPDATE' | 'NONE';
  storeCacheMiss?: boolean;
  disableNearCache?: boolean;
}

export class RLocalCachedMap<K = any, V = any> extends RedisDataStructure {
  private cache = new Map<string, { value: V | null; expireAt: number }>();
  private opts: LocalCachedMapOptions;
  private sub: Redis | null = null;
  private clientTag: string;
  private invalChannel: string;

  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec(), opts: LocalCachedMapOptions = {}, clientTag = '') {
    super(redis, name, codec);
    this.opts = { evictionPolicy: 'NONE', syncStrategy: 'INVALIDATE', ...opts };
    this.clientTag = clientTag || Math.random().toString(36).slice(2);
    this.invalChannel = `${name}:inval`;
    if (!this.opts.disableNearCache) void this.listen();
  }

  private async listen(): Promise<void> {
    try {
      const Dup = (this.redis as any).duplicate?.bind(this.redis);
      if (!Dup) return;
      this.sub = Dup();
      // attach BEFORE subscribe so broadcasts in the gap are not lost
      (this.sub as any).on('message', (_ch: string, msg: string) => {
        try {
          const m = JSON.parse(msg);
          if (m.from === this.clientTag) return;
          if (m.op === 'clear') this.cache.clear();
          else if (m.op === 'del') this.cache.delete(m.field);
          else if (m.op === 'put' && this.opts.syncStrategy === 'UPDATE') {
            this.cache.set(m.field, { value: this.decodeValue(m.value) as V, expireAt: Date.now() + 60000 });
          } else this.cache.delete(m.field);
        } catch { /* noop */ }
      });
      await (this.sub as any).subscribe(this.invalChannel);
    } catch {
      this.sub = null;
    }
  }

  private broadcast(msg: unknown): void {
    void this.redis.publish(this.invalChannel, JSON.stringify({ from: this.clientTag, ...(msg as object) })).catch(() => undefined);
  }

  private fieldOf(key: K): string {
    return this.encodeKey(key);
  }
  private getCached(field: string): V | null | undefined {
    const e = this.cache.get(field);
    if (!e) return undefined;
    if (e.expireAt < Date.now()) {
      this.cache.delete(field);
      return undefined;
    }
    return e.value;
  }
  private setCached(field: string, value: V | null): void {
    if (this.opts.disableNearCache) return;
    const max = this.opts.maxSize ?? 0;
    if (max > 0 && this.cache.size >= max) {
      const first = this.cache.keys().next().value;
      if (first !== undefined) this.cache.delete(first);
    }
    this.cache.set(field, { value, expireAt: Date.now() + 60000 });
  }

  async get(key: K): Promise<V | null> {
    const f = this.fieldOf(key);
    const c = this.getCached(f);
    if (c !== undefined) return c;
    const raw = await this.redis.hget(this.name, f);
    const v = raw === null ? null : (this.decodeValue(raw) as V);
    if (v !== null || this.opts.storeCacheMiss) this.setCached(f, v);
    return v;
  }
  async put(key: K, value: V): Promise<V | null> {
    const f = this.fieldOf(key);
    const old = await this.get(key);
    await this.redis.hset(this.name, f, this.encodeValue(value));
    this.setCached(f, value);
    this.broadcast(this.opts.syncStrategy === 'UPDATE' ? { op: 'put', field: f, value: this.encodeValue(value) } : { op: 'del', field: f });
    return old;
  }
  async remove(key: K): Promise<V | null> {
    const old = await this.get(key);
    const f = this.fieldOf(key);
    await this.redis.hdel(this.name, f);
    this.cache.delete(f);
    this.broadcast({ op: 'del', field: f });
    return old;
  }
  async containsKey(key: K): Promise<boolean> {
    return (await this.get(key)) !== null;
  }
  async clearLocalCache(): Promise<void> {
    this.cache.clear();
  }
  async clear(): Promise<void> {
    await this.redis.del(this.name);
    this.cache.clear();
    this.broadcast({ op: 'clear' });
  }
  async destroy(): Promise<void> {
    try {
      await (this.sub as any)?.unsubscribe?.(this.invalChannel);
      (this.sub as any)?.disconnect?.();
    } catch { /* noop */ }
  }
  /** Back-compat: preload all entries into near cache. */
  async preloadCache(): Promise<void> {
    const all = await this.redis.hgetall(this.name);
    for (const [f, raw] of Object.entries(all)) this.setCached(f, this.decodeValue(raw) as V);
    void sleep;
  }
}
