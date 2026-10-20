/**
 * Cache structures: RMapCache (+Native), RSetCache, Multimaps (+Cache/Native),
 * RLocalCachedMap. Wire matches Redisson 4.6.1 (struct-packed values, companion ZSETs).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import { RedisKeyedStructure } from './synchronizers.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import {
  decodeMapCacheEvent,
  duplicateRedis,
  encodeMapCacheEvent,
  luaEval,
  multimapInternalId,
  packMapCacheValue,
  prefixName,
  randomHex,
  serverNowMs,
  sleep,
  suffixName,
  toNumber,
  unpackMapCacheValue,
} from './util.js';
import { RMap } from './collections.js';

/** Map-cache entry event (mirrors Redisson EntryEvent + MapEntryListener). */
export type MapEntryEventType = 'CREATED' | 'UPDATED' | 'REMOVED' | 'EXPIRED';

export interface MapEntryEvent<K = any, V = any> {
  readonly type: MapEntryEventType;
  readonly key: K;
  /** New value for CREATED/UPDATED/EXPIRED; null for REMOVED. */
  readonly value: V | null;
  /** Previous value for UPDATED/REMOVED; null otherwise. */
  readonly oldValue: V | null;
}

export interface MapEntryListener<K = any, V = any> {
  onCreated?(event: MapEntryEvent<K, V>): void;
  onUpdated?(event: MapEntryEvent<K, V>): void;
  onRemoved?(event: MapEntryEvent<K, V>): void;
  onExpired?(event: MapEntryEvent<K, V>): void;
}

export class RMapCache<K = any, V = any> extends RedisKeyedStructure<K> {
  private ttlKey: string;
  private idleKey: string;
  private accessKey: string;
  private optionsKey: string;
  private channels: Record<'created' | 'updated' | 'removed' | 'expired', string>;
  private listenerSub: Redis | null = null;
  private listeners = new Map<number, MapEntryListener<K, V>>();
  private listenerKinds = new Map<number, Set<'created' | 'updated' | 'removed' | 'expired'>>();
  private nextListenerId = 1;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.ttlKey = prefixName('redisson__timeout__set', name);
    this.idleKey = prefixName('redisson__idle__set', name);
    this.accessKey = prefixName('redisson__map_cache__last_access__set', name);
    this.optionsKey = suffixName(name, 'redisson_options');
    // Java getCreatedChannelName() etc. all use prefixName (hash-tag aware).
    this.channels = {
      created: prefixName('redisson_map_cache_created', name),
      updated: prefixName('redisson_map_cache_updated', name),
      removed: prefixName('redisson_map_cache_removed', name),
      expired: prefixName('redisson_map_cache_expired', name),
    };
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
        raws = await this.redis.hmgetBuffer(this.name, ...expired);
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
    const ch = this.channels[kind];
    const msg = encodeMapCacheEvent([field, ...values]);
    // Buffer transport: ioredis encodes string args as UTF-8, which would
    // corrupt the u64LE length prefixes (bytes >= 0x80) on real Redis.
    void this.redis.publish(ch, Buffer.from(msg, 'latin1')).catch(() => undefined);
  }

  private async readStoredBuffer(field: string): Promise<unknown> {
    try {
      return await this.redis.hgetBuffer(this.name, field);
    } catch {
      return this.redis.hget(this.name, field);
    }
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
    const raw = await this.readStoredBuffer(field);
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

  /** Batch put with a shared TTL / maxIdle (Java RMapCache.putAll(map, ttl, ...)).
   * Per-key equivalent of the Java batch Lua: replaces TTL and idle scores,
   * repacks values with the new maxIdle, publishes created/updated events. */
  async putAll(mapping: Map<K, V> | Record<string, V>, ttlMs = 0, maxIdleMs = 0): Promise<void> {
    if (ttlMs < 0) throw new Error("redi.ts: ttl can't be negative");
    if (maxIdleMs < 0) throw new Error("redi.ts: maxIdleTime can't be negative");
    const entries: [K, V][] = mapping instanceof Map
      ? [...mapping.entries()]
      : (Object.entries(mapping) as [K, V][]);
    for (const [k, v] of entries) await this.put(k, v, ttlMs, maxIdleMs);
  }

  /** Java `expireEntries(Set<K>, ttl, maxIdle)`: apply expiry to many keys;
   *  returns how many existing (non-expired) entries were updated. */
  async expireEntries(keys: K[], ttlMs: number, maxIdleMs = 0): Promise<number> {
    let n = 0;
    for (const k of keys) if (await this.expireEntry(k, ttlMs, maxIdleMs)) n += 1;
    return n;
  }

  /** Java `getWithTTLOnly(K)`: value without touching idle/access bookkeeping. */
  async getWithTTLOnly(key: K): Promise<V | null> {
    const field = this.encodeKey(key);
    const raw = await this.readStoredBuffer(field);
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

  /** Java `getMaxSize()`: configured max-size (0 = unbounded). */
  async getMaxSize(): Promise<number> {
    const raw = await this.redis.hget(this.optionsKey, 'max-size');
    return raw === null ? 0 : Number(raw);
  }

  /** Java `computeIfAbsent(K, Duration, Function)`: per-key lock, read, map, put. */
  async computeIfAbsent(key: K, ttlMs: number, mappingFunction: (key: K) => V | null): Promise<V | null> {
    const tid = `compute:${randomHex(8)}`;
    const lock = this.getLock(key);
    await lock.lock(undefined, tid);
    try {
      const old = await this.get(key);
      if (old !== null) return old;
      const next = mappingFunction(key);
      if (next === null || next === undefined) return null;
      const existing = await this.putIfAbsent(key, next, ttlMs);
      return existing === null ? next : existing;
    } finally {
      try { await lock.unlock(tid); } catch { /* noop */ }
    }
  }

  /** Java `compute(K, Duration, BiFunction)`: per-key lock, read, apply,
   *  write (or remove when the function returns null). The lock uses a unique
   *  per-call thread id so concurrent computes in one Node process serialize
   *  (Java serializes by actual thread). */
  async compute(key: K, ttlMs: number, remappingFunction: (key: K, oldValue: V | null) => V | null): Promise<V | null> {
    const tid = `compute:${randomHex(8)}`;
    const lock = this.getLock(key);
    await lock.lock(undefined, tid);
    try {
      const old = await this.get(key);
      const next = remappingFunction(key, old);
      if (next === null || next === undefined) {
        if (old !== null) await this.remove(key);
        return null;
      }
      await this.fastPut(key, next, ttlMs);
      return next;
    } finally {
      try { await lock.unlock(tid); } catch { /* noop */ }
    }
  }

  /** Redisson RMapCache.expireEntry: set TTL / maxIdle on an existing entry.
   * Returns false when the entry is missing or already expired (Java: no
   * resurrection) and true when the new expiry was written. */
  async expireEntry(key: K, ttlMs: number, maxIdleMs = 0): Promise<boolean> {
    const field = this.encodeKey(key);
    const raw = await this.readStoredBuffer(field);
    const buf = this.toBuf(raw);
    if (buf === null) return false;
    const now = await serverNowMs(this.redis);
    const u = unpackMapCacheValue(buf);
    const payload = u.ok ? u.codec : buf.toString('utf8');
    const curMaxIdle = u.ok ? u.maxIdle : 0;
    // effective expiry = min(ttl score, idle score when packed maxIdle is set)
    const ttlScore = await this.redis.zscore(this.ttlKey, field);
    let expireAt = ttlScore === null ? Infinity : Number(ttlScore);
    if (curMaxIdle !== 0) {
      const idleScore = await this.redis.zscore(this.idleKey, field);
      if (idleScore !== null) expireAt = Math.min(expireAt, Number(idleScore));
    }
    if (expireAt <= now) return false;
    const ttlAt = ttlMs > 0 ? now + ttlMs : 0;
    const idleAt = maxIdleMs > 0 ? now + maxIdleMs : 0;
    await this.redis.hset(this.name, field, Buffer.from(packMapCacheValue(payload, maxIdleMs), 'latin1'));
    if (ttlAt > 0) await this.redis.zadd(this.ttlKey, String(ttlAt), field);
    else await this.redis.zrem(this.ttlKey, field);
    if (idleAt > 0) await this.redis.zadd(this.idleKey, String(idleAt), field);
    else await this.redis.zrem(this.idleKey, field);
    await this.touchAccess(field, now);
    return true;
  }

  /** Java RMapCache.remainTimeToLive(key): -2 when the entry is missing or
   * already expired, -1 when it has no expiry, else remaining ms.
   * The no-arg call (RedisDataStructure) returns the HASH's own PTTL. */
  override async remainTimeToLive(key?: K): Promise<number> {
    if (key === undefined) return super.remainTimeToLive();
    const field = this.encodeKey(key);
    const raw = await this.readStoredBuffer(field);
    const buf = this.toBuf(raw);
    if (buf === null) return -2;
    const u = unpackMapCacheValue(buf);
    const now = await serverNowMs(this.redis);
    const ttlScore = await this.redis.zscore(this.ttlKey, field);
    let expireAt = ttlScore === null ? null : Number(ttlScore);
    if (u.ok && u.maxIdle !== 0) {
      const idleScore = await this.redis.zscore(this.idleKey, field);
      if (idleScore !== null) {
        expireAt = expireAt === null ? Number(idleScore) : Math.min(expireAt, Number(idleScore));
      }
    }
    if (expireAt === null) return -1;
    const remain = Math.round(expireAt - now);
    return remain <= 0 ? -2 : remain;
  }

  // -- entry events (Java MapEntryListener over redisson_map_cache_* topics) --

  private kindOfChannel(ch: string): 'created' | 'updated' | 'removed' | 'expired' | null {
    if (ch === this.channels.created) return 'created';
    if (ch === this.channels.updated) return 'updated';
    if (ch === this.channels.removed) return 'removed';
    if (ch === this.channels.expired) return 'expired';
    return null;
  }

  private dispatch(channel: string, message: string | Buffer): void {
    const kind = this.kindOfChannel(channel);
    if (kind === null) return;
    const parts = decodeMapCacheEvent(message);
    if (parts.length < 2) return;
    let key: K;
    try {
      key = this.decodeKey(parts[0]) as K;
    } catch {
      key = parts[0] as unknown as K;
    }
    const dec = (s: string | undefined): V | null => {
      if (s === undefined) return null;
      try {
        return this.decodeValue(s) as V;
      } catch {
        return s as unknown as V;
      }
    };
    const second = dec(parts[1]);
    for (const [id, listener] of this.listeners) {
      if (!this.listenerKinds.get(id)?.has(kind)) continue;
      try {
        if (kind === 'created') listener.onCreated?.({ type: 'CREATED', key, value: second, oldValue: null });
        else if (kind === 'updated') listener.onUpdated?.({ type: 'UPDATED', key, value: second, oldValue: dec(parts[2]) });
        else if (kind === 'removed') listener.onRemoved?.({ type: 'REMOVED', key, value: null, oldValue: second });
        else listener.onExpired?.({ type: 'EXPIRED', key, value: second, oldValue: null });
      } catch { /* listener exceptions must not kill the subscriber */ }
    }
  }

  /** Register a MapEntryListener; returns the listener id.
   * Subscribes on a dedicated connection (never the main one) and sets the
   * Java `has-listeners` option flag so Java writers publish events too. */
  async addListener(listener: MapEntryListener<K, V>): Promise<number> {
    const kinds = new Set<'created' | 'updated' | 'removed' | 'expired'>();
    if (listener.onCreated) kinds.add('created');
    if (listener.onUpdated) kinds.add('updated');
    if (listener.onRemoved) kinds.add('removed');
    if (listener.onExpired) kinds.add('expired');
    if (kinds.size === 0) throw new Error('redi.ts: MapEntryListener needs at least one handler');
    const id = this.nextListenerId++;
    this.listeners.set(id, listener);
    this.listenerKinds.set(id, kinds);
    try {
      await this.redis.hset(this.optionsKey, 'has-listeners', '1');
    } catch { /* mock / old server */ }
    if (!this.listenerSub) {
      const conn = await duplicateRedis(this.redis);
      if (conn) {
        // messageBuffer keeps arbitrary packed bytes intact (length prefixes
        // may contain invalid UTF-8); 'message' decodes as UTF-8 and would
        // corrupt them on real Redis.
        const onMessage = (ch: Buffer | string, payload: Buffer) => {
          try {
            this.dispatch(String(ch), payload);
          } catch { /* noop */ }
        };
        (conn as any).on('messageBuffer', onMessage);
        try {
          await conn.subscribe(...Object.values(this.channels));
        } catch (e) {
          try {
            (conn as any).off?.('messageBuffer', onMessage);
            (conn as any).disconnect?.();
          } catch { /* noop */ }
          this.listeners.delete(id);
          this.listenerKinds.delete(id);
          throw e;
        }
        this.listenerSub = conn;
      }
    }
    return id;
  }

  async removeListener(id: number): Promise<void> {
    this.listeners.delete(id);
    this.listenerKinds.delete(id);
    if (this.listeners.size === 0 && this.listenerSub) {
      const conn = this.listenerSub;
      this.listenerSub = null;
      try {
        await conn.unsubscribe(...Object.values(this.channels));
      } catch { /* noop */ }
      try {
        (conn as any).disconnect?.();
      } catch { /* noop */ }
    }
  }

  async removeAllListeners(): Promise<void> {
    for (const id of [...this.listeners.keys()]) await this.removeListener(id);
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
      all = await this.redis.hgetallBuffer(this.name);
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
  override async unlink(): Promise<boolean> {
    await this.unlinkKeys([this.name, this.ttlKey, this.idleKey, this.accessKey, this.optionsKey]);
    return true;
  }
  async destroy(): Promise<void> {
    await this.clear();
  }
}

export class RMapCacheNative<K = any, V = any> extends RedisKeyedStructure<K> {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async put(key: K, value: V, ttl: number | Date = 0): Promise<V | null> {
    const field = this.encodeKey(key);
    const old = await this.get(key);
    await this.redis.hset(this.name, field, this.encodeValue(value));
    await this.applyFieldExpiry(field, ttl);
    return old;
  }

  private async applyFieldExpiry(field: string, ttl: number | Date): Promise<void> {
    try {
      if (ttl instanceof Date) {
        await (this.redis as any).hpexpireat(this.name, ttl.getTime(), 'FIELDS', 1, field);
      } else if (typeof ttl === 'number' && ttl > 0) {
        await this.redis.hpexpire(this.name, ttl, 'FIELDS', 1, field);
      }
    } catch { /* server too old */ }
  }

  /** Java `fastPut(K, V[, Duration|Instant])`: HSET without reading the old value.
   *  Returns whether the field was newly added (HSET reply 1). */
  async fastPut(key: K, value: V, ttl: number | Date = 0): Promise<boolean> {
    const field = this.encodeKey(key);
    const added = toNumber(await this.redis.hset(this.name, field, this.encodeValue(value))) === 1;
    await this.applyFieldExpiry(field, ttl);
    return added;
  }

  /** Java `putIfExist(K, V[, Duration|Instant])`: write only when the field
   *  exists; returns the previous value (null when absent, nothing written). */
  async putIfExist(key: K, value: V, ttl: number | Date = 0): Promise<V | null> {
    const field = this.encodeKey(key);
    const ttlMode = ttl instanceof Date ? '2' : (typeof ttl === 'number' && ttl > 0 ? '1' : '0');
    const ttlArg = ttl instanceof Date ? String(ttl.getTime()) : String(typeof ttl === 'number' ? ttl : 0);
    const lua = `local old = redis.call('hget', KEYS[1], ARGV[1])
if old == false then return nil end
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
if ARGV[4] == '1' then
    redis.call('hpexpire', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
elseif ARGV[4] == '2' then
    redis.call('hpexpireat', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
end
return old`;
    try {
      const old: any = await luaEval(this.redis, lua, 1, this.name, field, this.encodeValue(value), ttlArg, ttlMode);
      return old === null || old === undefined ? null : (this.decodeValue(String(old)) as V);
    } catch {
      const old = await this.get(key);
      if (old === null) return null;
      await this.redis.hset(this.name, field, this.encodeValue(value));
      await this.applyFieldExpiry(field, ttl);
      return old;
    }
  }

  /** Java `fastPutIfAbsent(K, V[, Duration|Instant])`: HSETNX + optional TTL. */
  async fastPutIfAbsent(key: K, value: V, ttl: number | Date = 0): Promise<boolean> {
    const field = this.encodeKey(key);
    const ttlMode = ttl instanceof Date ? '2' : (typeof ttl === 'number' && ttl > 0 ? '1' : '0');
    const ttlArg = ttl instanceof Date ? String(ttl.getTime()) : String(typeof ttl === 'number' ? ttl : 0);
    const lua = `if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then return 0 end
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
if ARGV[4] == '1' then
    redis.call('hpexpire', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
elseif ARGV[4] == '2' then
    redis.call('hpexpireat', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
end
return 1`;
    try {
      return toNumber(await luaEval(this.redis, lua, 1, this.name, field, this.encodeValue(value), ttlArg, ttlMode)) === 1;
    } catch {
      const set = toNumber(await this.redis.hsetnx(this.name, field, this.encodeValue(value))) === 1;
      if (set) await this.applyFieldExpiry(field, ttl);
      return set;
    }
  }

  /** Convenience boolean form of `putIfExist` (TS extension: 4.6.1 exposes
   *  only `fastPut`/`fastPutIfAbsent`; newer Redisson adds `fastPutIfExist`). */
  async fastPutIfExist(key: K, value: V, ttl: number | Date = 0): Promise<boolean> {
    return (await this.putIfExist(key, value, ttl)) !== null;
  }

  /** Java RedissonMapCacheNative.putIfAbsent(key, value, ttl). With a TTL
   *  the 4.6.1 script guards on `hget` then applies `HPEXPIRE ... FIELDS 1`
   *  atomically (existing entries keep value and TTL). ioredis-mock (and
   *  Redis < 7.4) cannot run HPEXPIRE: one side-effect-free probe against a
   *  nonexistent field decides whether the script is usable, otherwise the
   *  base `hsetnx`/`hget` script guards without a TTL. */
  private fieldExpireInLua: boolean | null = null;

  private async canExpireFieldInLua(): Promise<boolean> {
    if (this.fieldExpireInLua === null) {
      try {
        // missing field → reply -2; Redis neither creates the field nor the key
        await this.redis.hpexpire(this.name, 1, 'FIELDS', 1, '\u0000__redi_probe__');
        this.fieldExpireInLua = true;
      } catch {
        this.fieldExpireInLua = false;
      }
    }
    return this.fieldExpireInLua;
  }

  async putIfAbsent(key: K, value: V, ttl: number | Date = 0): Promise<V | null> {
    if (typeof ttl === 'number' && ttl < 0) throw new Error("redi.ts: ms can't be negative");
    const field = this.encodeKey(key);
    if (value === null || value === undefined) return this.get(key);
    const enc = this.encodeValue(value);
    const hasTtl = ttl instanceof Date || ttl > 0;
    if (hasTtl && (await this.canExpireFieldInLua())) {
      const lua = "local currValue = redis.call('hget', KEYS[1], ARGV[2]); if currValue ~= false then return currValue; end; redis.call('hset', KEYS[1], ARGV[2], ARGV[3]); redis.call(ARGV[4], KEYS[1], ARGV[1], 'fields', 1, ARGV[2]); return nil;";
      const at = ttl instanceof Date ? String(ttl.getTime()) : String(ttl);
      const cmd = ttl instanceof Date ? 'HPEXPIREAT' : 'HPEXPIRE';
      try {
        const r: any = await this.redis.eval(lua, 1, this.name, at, field, enc, cmd);
        return r == null ? null : (this.decodeValue(r) as V);
      } catch { /* fall through to the set-if-absent path */ }
    }
    const lua = "if redis.call('hsetnx', KEYS[1], ARGV[1], ARGV[2]) == 1 then return nil else return redis.call('hget', KEYS[1], ARGV[1]) end";
    try {
      const r: any = await this.redis.eval(lua, 1, this.name, field, enc);
      return r == null ? null : (this.decodeValue(r) as V);
    } catch {
      const set: any = await this.redis.hsetnx(this.name, field, enc);
      return toNumber(set) === 1 ? null : this.get(key);
    }
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
      const r: any = await this.redis.hpttl(this.name, 'FIELDS', 1, this.encodeKey(key));
      if (Array.isArray(r)) return Number(r[0]);
      return Number(r);
    } catch {
      return -1;
    }
  }

  /** Java RMapCacheNative.remainTimeToLive(key) — HPTTL FIELDS 1. */
  override async remainTimeToLive(key?: K): Promise<number> {
    if (key === undefined) return super.remainTimeToLive();
    return this.remainTTL(key);
  }

  // -- conditional field expiry (Redis 8 HEXPIRE family) ----------------------
  // Java RedissonMapCacheNative.expireEntryAsync: HPEXPIRE name ms [NX|GT|LT]
  // FIELDS 1 field; per-field reply >= 1 means the new expiry was written.

  private async hpexpireField(key: K, ttl: number | Date, param?: 'NX' | 'GT' | 'LT'): Promise<boolean> {
    const field = this.encodeKey(key);
    try {
      const at = ttl instanceof Date;
      const args: any[] = [this.name, at ? (ttl as Date).getTime() : ttl];
      if (param) args.push(param);
      args.push('FIELDS', 1, field);
      const r: any = at
        ? await (this.redis as any).hpexpireat(...args)
        : await (this.redis as any).hpexpire(...args);
      const first = Array.isArray(r) ? r[0] : r;
      return toNumber(first) >= 1;
    } catch {
      return false;
    }
  }

  /** Set the field TTL (HPEXPIRE, or HPEXPIREAT for a Date). */
  async expireEntry(key: K, ttl: number | Date): Promise<boolean> {
    return this.hpexpireField(key, ttl);
  }

  /** Set the field TTL only when the field has no expiry (HPEXPIRE NX). */
  async expireEntryIfNotSet(key: K, ttl: number | Date): Promise<boolean> {
    return this.hpexpireField(key, ttl, 'NX');
  }

  /** Set the field TTL only when it is longer than the current one (HPEXPIRE GT). */
  async expireEntryIfGreater(key: K, ttl: number | Date): Promise<boolean> {
    return this.hpexpireField(key, ttl, 'GT');
  }

  /** Set the field TTL only when it is shorter than the current one (HPEXPIRE LT). */
  async expireEntryIfLess(key: K, ttl: number | Date): Promise<boolean> {
    return this.hpexpireField(key, ttl, 'LT');
  }

  /** Java `putIfAllKeysExist(PutArgs)` / `putIfAllKeysAbsent(PutArgs)`:
   *  atomic HSET of every field (+ per-field expiry) only when the condition
   *  holds for all of them. */
  async putIfAllKeysExist(mapping: Record<string, V> | Map<K, V>, ttl: number | Date = 0): Promise<boolean> {
    return this.putAllConditional('XX', mapping, ttl);
  }
  async putIfAllKeysAbsent(mapping: Record<string, V> | Map<K, V>, ttl: number | Date = 0): Promise<boolean> {
    return this.putAllConditional('NX', mapping, ttl);
  }
  private async putAllConditional(mode: 'XX' | 'NX', mapping: Record<string, V> | Map<K, V>, ttl: number | Date): Promise<boolean> {
    const entries = mapping instanceof Map ? [...mapping.entries()] : Object.entries(mapping) as [K, V][];
    if (entries.length === 0) return false;
    const fields = entries.map(([k]) => this.encodeKey(k));
    const values = entries.map(([, v]) => this.encodeValue(v));
    const ttlMode = ttl instanceof Date ? '2' : (typeof ttl === 'number' && ttl > 0 ? '1' : '0');
    const ttlArg = ttl instanceof Date ? String(ttl.getTime()) : String(typeof ttl === 'number' ? ttl : 0);
    const lua = `local mode = ARGV[1]
local n = tonumber(ARGV[2])
for i = 0, n - 1 do
    local exists = redis.call('hexists', KEYS[1], ARGV[5 + i * 2])
    if mode == 'XX' and exists == 0 then return 0 end
    if mode == 'NX' and exists == 1 then return 0 end
end
for i = 0, n - 1 do
    local field = ARGV[5 + i * 2]
    redis.call('hset', KEYS[1], field, ARGV[6 + i * 2])
    if ARGV[3] == '1' then
        redis.call('hpexpire', KEYS[1], ARGV[4], 'FIELDS', 1, field)
    elseif ARGV[3] == '2' then
        redis.call('hpexpireat', KEYS[1], ARGV[4], 'FIELDS', 1, field)
    end
end
return 1`;
    const args: (string | number)[] = [this.name, mode, String(entries.length), ttlMode, ttlArg];
    for (let i = 0; i < fields.length; i++) args.push(fields[i], values[i]);
    try {
      return toNumber(await luaEval(this.redis, lua, 1, ...args)) === 1;
    } catch {
      // mock fallback: conditional HSET only (no HPEXPIRE family)
      for (const f of fields) {
        const exists = toNumber(await this.redis.hexists(this.name, f)) === 1;
        if (mode === 'XX' && !exists) return false;
        if (mode === 'NX' && exists) return false;
      }
      for (let i = 0; i < fields.length; i++) await this.redis.hset(this.name, fields[i], values[i]);
      return true;
    }
  }

  /** Java `expireEntries(Set, ttl[, mode])`: batch field expiry; returns the
   *  number of fields whose expiry was written. */
  async expireEntries(keys: K[], ttl: number | Date, mode?: 'NX' | 'GT' | 'LT'): Promise<number> {
    let n = 0;
    for (const k of keys) {
      const ok = mode === 'NX'
        ? await this.expireEntryIfNotSet(k, ttl)
        : mode === 'GT'
          ? await this.expireEntryIfGreater(k, ttl)
          : mode === 'LT'
            ? await this.expireEntryIfLess(k, ttl)
            : await this.expireEntry(k, ttl);
      if (ok) n += 1;
    }
    return n;
  }
  async expireEntriesIfNotSet(keys: K[], ttl: number | Date): Promise<number> {
    return this.expireEntries(keys, ttl, 'NX');
  }
  async expireEntriesIfGreater(keys: K[], ttl: number | Date): Promise<number> {
    return this.expireEntries(keys, ttl, 'GT');
  }
  async expireEntriesIfLess(keys: K[], ttl: number | Date): Promise<number> {
    return this.expireEntries(keys, ttl, 'LT');
  }

  /** Remove the field TTL (HPERSIST; Java clearExpire). The no-arg call
   *  (RedisDataStructure) PERSISTs the whole HASH. */
  override async clearExpire(key?: K): Promise<boolean> {
    if (key === undefined) return super.clearExpire();
    const field = this.encodeKey(key);
    try {
      const hpersist = (this.redis as any).hpersist;
      const r: any = typeof hpersist === 'function'
        ? await hpersist.call(this.redis, this.name, 'FIELDS', 1, field)
        : await this.redis.call('HPERSIST', this.name, 'FIELDS', 1, field);
      const first = Array.isArray(r) ? r[0] : r;
      return toNumber(first) === 1;
    } catch {
      return false;
    }
  }
}

export class RSetCache<V = any> extends RedisKeyedStructure<V> {
  private idleKey: string;
  private touchKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.idleKey = prefixName('redisson__idle__set', name);
    // TS extension (Redisson has no SetCache maxIdle): last-touch timestamps.
    // Java never reads this key, so cross-language presence is unaffected.
    this.touchKey = suffixName(name, 'redisson_touch');
  }
  // NOTE on idle semantics: idle ZSET holds the DURATION; last-touch rides in
  // touchKey. contains() enforces now-lastTouch<=duration and refreshes the
  // touch (sliding window, like RMapCache maxIdle). Without the touch record
  // an entry would either never expire or die on first read — both wrong.
  private async purge(now: number): Promise<void> {
    try {
      const expired: string[] = await this.redis.zrangebyscore(this.name, '0', String(now));
      if (expired.length > 0) {
        await this.redis.zrem(this.name, ...expired);
        await this.redis.zrem(this.idleKey, ...expired);
        await this.redis.hdel(this.touchKey, ...expired);
      }
    } catch { /* noop */ }
  }
  private async evictOne(enc: string): Promise<void> {
    try {
      await this.redis.zrem(this.name, enc);
      await this.redis.zrem(this.idleKey, enc);
      await this.redis.hdel(this.touchKey, enc);
    } catch { /* noop */ }
  }
  async add(value: V, ttlMs = 0, maxIdleMs = 0): Promise<boolean> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const enc = this.encodeValue(value);
    const expiry = ttlMs > 0 ? now + ttlMs : 9223372036854775807;
    const exists = await this.redis.zscore(this.name, enc);
    await this.redis.zadd(this.name, String(expiry), enc);
    if (maxIdleMs > 0) {
      await this.redis.zadd(this.idleKey, String(maxIdleMs), enc);
      await this.redis.hset(this.touchKey, enc, String(now));
    } else {
      await this.redis.zrem(this.idleKey, enc);
      await this.redis.hdel(this.touchKey, enc);
    }
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
      const touched = await this.redis.hget(this.touchKey, enc);
      const lastTouch = touched === null ? now : Number(touched);
      if (now - lastTouch > Number(idle)) {
        await this.evictOne(enc);
        return false;
      }
      await this.redis.hset(this.touchKey, enc, String(now));
    }
    return true;
  }
  async remove(value: V): Promise<boolean> {
    const enc = this.encodeValue(value);
    const n = await this.redis.zrem(this.name, enc);
    await this.redis.zrem(this.idleKey, enc);
    await this.redis.hdel(this.touchKey, enc);
    return n > 0;
  }
  /** Drop idle-expired members from a candidate list (bulk touch lookup). */
  private async sweepIdle(members: string[], now: number): Promise<string[]> {
    if (members.length === 0) return members;
    let durs: (string | null)[];
    let touches: (string | null)[];
    try {
      const pipe = this.redis.pipeline();
      for (const m of members) pipe.zscore(this.idleKey, m);
      const r1: any[] = (await pipe.exec()) ?? [];
      durs = r1.map((r) => (r?.[1] === null || r?.[1] === undefined ? null : String(r[1])));
      const needTouch = durs.some((d) => d !== null);
      touches = members.map(() => null);
      if (needTouch) {
        touches = await this.redis.hmget(this.touchKey, ...members);
      }
    } catch {
      return members;
    }
    const alive: string[] = [];
    for (let i = 0; i < members.length; i++) {
      const dur = durs[i];
      if (dur === null) {
        alive.push(members[i]);
        continue;
      }
      const lastTouch = touches[i] === null ? now : Number(touches[i]);
      if (now - lastTouch > Number(dur)) {
        await this.evictOne(members[i]);
      } else {
        alive.push(members[i]);
      }
    }
    return alive;
  }
  async size(): Promise<number> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const members: string[] = await this.redis.zrangebyscore(this.name, '0', '9223372036854775807');
    return (await this.sweepIdle(members, now)).length;
  }
  async readAll(): Promise<V[]> {
    const now = await serverNowMs(this.redis);
    await this.purge(now);
    const m = await this.redis.zrangebyscore(this.name, String(now + 1), '9223372036854775807');
    const alive = await this.sweepIdle(m, now);
    return alive.map((x) => this.decodeValue(x) as V);
  }

  // Java RSetCache.clear() delegates to delete(). The idle/touch pair is a TS
  // extension, so delete() must remove it explicitly (main ZSET alone would
  // leave orphaned sliding-window metadata behind).
  override async delete(): Promise<boolean> {
    return this.deleteKeys([this.name, this.idleKey, this.touchKey]);
  }

  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name, this.idleKey, this.touchKey]);
  }

  async clear(): Promise<void> {
    await this.delete();
  }
}

// -- Multimaps ---------------------------------------------------------------

abstract class RMultimapBase<K = any, V = any> extends RedisKeyedStructure<K> {
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
  /** Extra hash-tagged companion keys owned by subclasses (e.g. TTL ZSET). */
  protected companionKeys(): string[] {
    return [];
  }

  private async removeAll(mode: 'del' | 'unlink'): Promise<boolean> {
    const existed = ((await this.redis.exists(this.name)) as number) > 0;
    // Read the field→id map BEFORE deleting it: collections are only
    // discoverable through the HASH (AGENTS.md:9).
    const ids = await this.redis.hvals(this.name);
    const childKeys = ids.map((id) => this.collKey(id));
    for (let i = 0; i < childKeys.length; i += 500) {
      const batch = childKeys.slice(i, i + 500);
      if (mode === 'unlink') await this.unlinkKeys(batch);
      else await this.deleteKeys(batch);
    }
    const tails = [this.name, ...this.companionKeys()];
    if (mode === 'unlink') await this.unlinkKeys(tails);
    else await this.deleteKeys(tails);
    return existed;
  }

  async clear(): Promise<void> {
    await this.removeAll('del');
  }

  // Java RedissonMultimap.deleteAsync() removes the HASH + every child
  // collection in one Lua script. Direct delete() must not orphan children.
  override async delete(): Promise<boolean> {
    return this.removeAll('del');
  }

  override async unlink(): Promise<boolean> {
    return this.removeAll('unlink');
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
  // TTL ZSET holds fields (not child ids); clear never touched it before, so
  // delete()/clear() left an orphan after the main HASH was gone.
  protected override companionKeys(): string[] {
    return [this.timeoutKey];
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

  private evictTimer: ReturnType<typeof setInterval> | null = null;

  /** One sweep over the expiry ZSET: drop every field whose deadline passed
   *  (child collection + HASH entry + ZSET member). Returns the count. */
  async evictExpired(): Promise<number> {
    const now = await serverNowMs(this.redis);
    const fields = await this.redis.zrangebyscore(this.timeoutKey, 0, now);
    let removed = 0;
    for (const field of fields) {
      const id = await this.redis.hget(this.name, field);
      if (id !== null) await this.redis.del(this.collKey(id));
      await this.redis.hdel(this.name, field);
      await this.redis.zrem(this.timeoutKey, field);
      removed += 1;
    }
    return removed;
  }

  /** Opt-in client-side purge timer (eviction stays lazy by default — see
   *  `checkExpiry`). The timer is `unref()`ed and must be stopped with
   *  `disablePeriodicEviction()` when no longer needed. */
  enablePeriodicEviction(intervalMs = 1000): void {
    this.disablePeriodicEviction();
    const t = setInterval(() => {
      void this.evictExpired().catch(() => undefined);
    }, Math.max(50, intervalMs));
    (t as any).unref?.();
    this.evictTimer = t;
  }

  disablePeriodicEviction(): void {
    if (this.evictTimer !== null) {
      clearInterval(this.evictTimer);
      this.evictTimer = null;
    }
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
  private cache = new Map<string, { value: V | null; expireAt: number; lastAccess: number; hits: number }>();
  private opts: LocalCachedMapOptions;
  private sub: Redis | null = null;
  private clientTag: string;
  private invalChannel: string;
  private ready: Promise<void> = Promise.resolve();

  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec(), opts: LocalCachedMapOptions = {}, clientTag = '') {
    super(redis, name, codec);
    this.opts = { evictionPolicy: 'NONE', syncStrategy: 'INVALIDATE', ...opts };
    this.clientTag = clientTag || Math.random().toString(36).slice(2);
    this.invalChannel = `${name}:inval`;
    if (!this.opts.disableNearCache) this.ready = this.listen();
  }

  /** Absolute local-expiry timestamp; Infinity when timeToLiveMs is unset. */
  private expiryAt(now: number): number {
    const ttl = this.opts.timeToLiveMs ?? 0;
    return ttl > 0 ? now + ttl : Infinity;
  }

  private buildEntry(value: V | null, now: number): { value: V | null; expireAt: number; lastAccess: number; hits: number } {
    return { value, expireAt: this.expiryAt(now), lastAccess: now, hits: 0 };
  }

  private isStale(e: { expireAt: number; lastAccess: number }, now: number): boolean {
    if (e.expireAt !== Infinity && now >= e.expireAt) return true;
    const maxIdle = this.opts.maxIdleMs ?? 0;
    return maxIdle > 0 && now - e.lastAccess > maxIdle;
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
            this.cache.set(m.field, this.buildEntry(this.decodeValue(m.value) as V, Date.now()));
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
    const now = Date.now();
    if (this.isStale(e, now)) {
      this.cache.delete(field);
      return undefined;
    }
    e.lastAccess = now;
    e.hits += 1;
    return e.value;
  }
  /** Victim for size-bounded local cache: NONE = FIFO (insertion order, the
   *  historical behavior); LRU = oldest lastAccess; LFU = fewest accesses
   *  (ties broken by oldest lastAccess). */
  private victimField(): string | undefined {
    const policy = this.opts.evictionPolicy ?? 'NONE';
    if (policy === 'NONE') return this.cache.keys().next().value;
    let victim: string | undefined;
    let bestA = Infinity;
    let bestB = Infinity;
    for (const [f, e] of this.cache) {
      if (policy === 'LFU') {
        if (e.hits < bestA || (e.hits === bestA && e.lastAccess < bestB)) {
          victim = f;
          bestA = e.hits;
          bestB = e.lastAccess;
        }
      } else if (e.lastAccess < bestA) {
        victim = f;
        bestA = e.lastAccess;
      }
    }
    return victim;
  }
  private setCached(field: string, value: V | null): void {
    if (this.opts.disableNearCache) return;
    const max = this.opts.maxSize ?? 0;
    if (max > 0 && !this.cache.has(field)) {
      const now = Date.now();
      for (const [f, e] of this.cache) if (this.isStale(e, now)) this.cache.delete(f);
      while (this.cache.size >= max) {
        const victim = this.victimField();
        if (victim === undefined) break;
        this.cache.delete(victim);
      }
    }
    this.cache.set(field, this.buildEntry(value, Date.now()));
  }

  async get(key: K): Promise<V | null> {
    // Wait for the invalidation subscription ack before populating the near
    // cache; otherwise a write racing the constructor can be missed and the
    // stale value would live in the local map indefinitely.
    await this.ready;
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
  /** Java `getCachedMap()`: snapshot of the live local cache (stale/idle
   *  entries already dropped; empty when the near cache is disabled). */
  getCachedMap(): Map<K, V | null> {
    const now = Date.now();
    const out = new Map<K, V | null>();
    for (const [f, e] of this.cache) {
      if (this.isStale(e, now)) continue;
      let k: any = f;
      try { k = this.decodeKey(f); } catch { /* raw field */ }
      out.set(k, e.value);
    }
    return out;
  }
  async clear(): Promise<void> {
    await this.ready;
    await this.redis.del(this.name);
    this.cache.clear();
    this.broadcast({ op: 'clear' });
  }
  async destroy(): Promise<void> {
    try {
      await this.ready;
      await (this.sub as any)?.unsubscribe?.(this.invalChannel);
      (this.sub as any)?.disconnect?.();
    } catch { /* noop */ }
  }
  /** Back-compat: preload entries into near cache. `count > 0` loads at most
   *  that many via HSCAN; 0/omitted loads all (Java `preloadCache(count)`). */
  async preloadCache(count = 0): Promise<void> {
    if (count > 0) {
      let cursor = '0';
      let loaded = 0;
      do {
        const [next, fields] = (await this.redis.hscan(
          this.name, cursor, 'COUNT', String(Math.max(count, 100)),
        )) as [string, string[]];
        for (let i = 0; i + 1 < fields.length && loaded < count; i += 2, loaded++) {
          this.setCached(fields[i], this.decodeValue(fields[i + 1]) as V);
        }
        cursor = next;
      } while (cursor !== '0' && loaded < count);
      return;
    }
    const all = await this.redis.hgetall(this.name);
    for (const [f, raw] of Object.entries(all)) this.setCached(f, this.decodeValue(raw) as V);
    void sleep;
  }
}
