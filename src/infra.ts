/**
 * Infrastructure facades: RKeys, RScript, RFunction, RSearch, JsonBucket(s),
 * VectorSet, RBatch, RPriority*, RTransferQueue, RArray, RCircularBuffer, RMaps.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { luaEval, multimapInternalId, sleep, suffixName, toNumber, withDedicatedRedis } from './util.js';
import { BatchAtomicDouble, BatchAtomicLong, BatchBitSet, BatchBucket, BatchDeque, BatchGeo, BatchList, BatchMap, BatchMapCacheNative, BatchQueue, BatchScoredSortedSet, BatchSet, BatchStream } from './batch.js';

// -- RKeys -----------------------------------------------------------------------

export class RKeys {
  constructor(private redis: Redis) {}
  async count(): Promise<number> {
    return this.redis.dbsize();
  }
  async countExists(...names: string[]): Promise<number> {
    if (names.length === 0) return 0;
    return this.redis.exists(...names);
  }
  async getKeys(pattern = '*', count = 100): Promise<string[]> {
    const out: string[] = [];
    let cursor = '0';
    do {
      const [next, keys] = (await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', count)) as [string, string[]];
      out.push(...keys);
      cursor = next;
    } while (cursor !== '0');
    return out;
  }
  async delete(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    return this.redis.del(...keys);
  }
  async deleteByPattern(pattern: string): Promise<number> {
    const keys = await this.getKeys(pattern);
    if (keys.length === 0) return 0;
    return this.delete(...keys);
  }
  async unlink(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    try {
      return await this.redis.unlink(...keys);
    } catch {
      return this.delete(...keys);
    }
  }
  async randomKey(): Promise<string | null> {
    return this.redis.randomkey();
  }
  async getType(key: string): Promise<string> {
    return this.redis.type(key);
  }
  async rename(oldKey: string, newKey: string): Promise<void> {
    await this.redis.rename(oldKey, newKey);
  }
  async renamenx(oldKey: string, newKey: string): Promise<boolean> {
    return ((await this.redis.renamenx(oldKey, newKey)) as number) === 1;
  }
  async expire(key: string, ttlMs: number): Promise<boolean> {
    return ((await this.redis.pexpire(key, ttlMs)) as number) === 1;
  }
  async touch(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    try {
      return await this.redis.touch(...keys);
    } catch {
      return 0;
    }
  }
  async flushdb(): Promise<void> {
    await this.redis.flushdb();
  }
  async flushall(): Promise<void> {
    await this.redis.flushall();
  }
  async copy(src: string, dest: string, replace = false): Promise<boolean> {
    // NOTE: exact form omits REPLACE entirely — sending '' is a syntax error.
    const args: (string | number)[] = replace ? [src, dest, 'DB', 0, 'REPLACE'] : [src, dest];
    try {
      return toNumber(await (this.redis as any).copy(...args)) === 1;
    } catch {
      return false;
    }
  }
}

// -- RScript ----------------------------------------------------------------------

export type ScriptReturnType = 'BOOLEAN' | 'INTEGER' | 'LONG' | 'STATUS' | 'VALUE' | 'MULTI' | 'LIST' | 'MAPVALUE';

export class RScript {
  constructor(private redis: Redis) {}
  async eval(mode: 'READ_ONLY' | 'READ_WRITE' | string, script: string, returnType: ScriptReturnType, keys: string[] = [], values: unknown[] = []): Promise<any> {
    void mode;
    const raw = await luaEval(this.redis, script, keys.length, ...keys, ...values.map(String));
    return this.cast(raw, returnType);
  }
  async evalSha(sha: string, returnType: ScriptReturnType, keys: string[] = [], values: unknown[] = []): Promise<any> {
    const raw = await this.redis.evalsha(sha, keys.length, ...keys, ...values.map(String));
    return this.cast(raw, returnType);
  }
  async scriptLoad(script: string): Promise<string> {
    return this.redis.script('LOAD', script) as unknown as string;
  }
  async scriptExists(...shas: string[]): Promise<boolean[]> {
    const r: any = await this.redis.script('EXISTS', ...shas);
    return (r as number[]).map((x) => x === 1);
  }
  private cast(raw: any, t: ScriptReturnType): any {
    switch (t) {
      case 'BOOLEAN':
        return toNumber(raw) !== 0;
      case 'INTEGER':
      case 'LONG':
        return toNumber(raw);
      case 'STATUS':
        return String(raw ?? 'OK');
      case 'MULTI':
      case 'LIST':
        return raw;
      default:
        return raw;
    }
  }
}

// -- RFunction ---------------------------------------------------------------------

export class RFunction {
  constructor(private redis: Redis) {}
  private async call(...args: string[]): Promise<any> {
    return (this.redis as any).call(...args);
  }
  async load(code: string, replace = false): Promise<string> {
    return this.call('FUNCTION', 'LOAD', replace ? 'REPLACE' : '', code) as Promise<string>;
  }
  async delete(lib: string): Promise<void> {
    await this.call('FUNCTION', 'DELETE', lib);
  }
  async flush(): Promise<void> {
    await this.call('FUNCTION', 'FLUSH');
  }
  async callFn(name: string, keys: string[] = [], args: string[] = []): Promise<any> {
    return this.call('FCALL', name, String(keys.length), ...keys, ...args);
  }
  async callReadOnly(name: string, keys: string[] = [], args: string[] = []): Promise<any> {
    return this.call('FCALL_RO', name, String(keys.length), ...keys, ...args);
  }
}

// -- RSearch (RediSearch subset; native protocol, auto interop) ---------------------

export class RSearch {
  constructor(private redis: Redis) {}
  private async call(...args: (string | number)[]): Promise<any> {
    return (this.redis as any).call(...args.map(String));
  }
  async createIndex(name: string, schema: string[], options: { on?: string; prefixes?: string[] } = {}): Promise<void> {
    const args: string[] = ['FT.CREATE', name, 'ON', options.on ?? 'HASH'];
    if (options.prefixes?.length) args.push('PREFIX', String(options.prefixes.length), ...options.prefixes);
    args.push('SCHEMA', ...schema);
    await this.call(...args);
  }
  async dropIndex(name: string, deleteDocs = false): Promise<void> {
    await this.call('FT.DROPINDEX', name, ...(deleteDocs ? ['DD'] : []));
  }
  async search(name: string, query: string, params: { limit?: [number, number]; returnFields?: string[]; noContent?: boolean } = {}): Promise<{ total: number; docs: any[] }> {
    const args: string[] = ['FT.SEARCH', name, query];
    if (params.noContent) args.push('NOCONTENT');
    if (params.returnFields?.length) args.push('RETURN', String(params.returnFields.length), ...params.returnFields);
    if (params.limit) args.push('LIMIT', String(params.limit[0]), String(params.limit[1]));
    const r: any = await this.call(...args);
    if (r && typeof r === 'object' && !Array.isArray(r)) {
      // RESP3
      return { total: Number((r as any).total_results ?? 0), docs: (r as any).results ?? [] };
    }
    const arr = r as any[];
    if (!Array.isArray(arr) || arr.length === 0) return { total: 0, docs: [] };
    const total = Number(arr[0]);
    const docs: any[] = [];
    for (let i = 1; i < arr.length; i++) {
      const id = arr[i];
      const fields = arr[i + 1];
      if (Array.isArray(fields)) {
        const doc: any = { id };
        for (let j = 0; j + 1 < fields.length; j += 2) doc[String(fields[j])] = fields[j + 1];
        docs.push(doc);
        i++;
      } else docs.push({ id });
    }
    return { total, docs };
  }
}

// -- JSON buckets (RedisJSON; values use plain JSON, not Java codec) ----------------

export class RJsonBucket<V = any> {
  constructor(private redis: Redis, readonly name: string) {}
  async set(value: V, path = '$'): Promise<void> {
    await this.redis.call('JSON.SET', this.name, path, JSON.stringify(value));
  }
  async get(path = '$'): Promise<V | null> {
    const raw: any = await this.redis.call('JSON.GET', this.name, path);
    if (raw === null || raw === undefined) return null;
    try {
      return JSON.parse(typeof raw === 'string' ? raw : JSON.stringify(raw)) as V;
    } catch {
      return raw as V;
    }
  }
  async delete(path = '$'): Promise<number> {
    return toNumber(await this.redis.call('JSON.DEL', this.name, path));
  }
}

export class RJsonBuckets {
  constructor(private redis: Redis) {}
  bucket<V>(name: string): RJsonBucket<V> {
    return new RJsonBucket<V>(this.redis, name);
  }
}

// -- VectorSet (Redis 8+ VSET) -------------------------------------------------------

export class RVectorSet extends RedisDataStructure {
  async add(element: string, vector: number[]): Promise<boolean> {
    const r: any = await this.redis.call('VADD', this.name, 'VALUES', vector.length, ...vector.map(String), element);
    return toNumber(r) === 1 || r === true;
  }
  async sim(element: string, count = 10): Promise<string[]> {
    return (await this.redis.call('VSIM', this.name, 'ELE', element, 'COUNT', count)) as string[];
  }
  async card(): Promise<number> {
    return toNumber(await this.redis.call('VCARD', this.name));
  }
  async remove(element: string): Promise<boolean> {
    return toNumber(await this.redis.call('VREM', this.name, element)) === 1;
  }
}

// -- Priority queues (GO_ONLY: ZSET+score; NOT Java Comparator protocol) --------------

export class RPriorityQueue<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async offer(value: V, score = 0): Promise<void> {
    await this.redis.zadd(this.name, String(score), this.encodeValue(value));
  }
  async poll(): Promise<V | null> {
    const m: any = await (this.redis as any).zpopmin?.(this.name, 1);
    if (!m || m.length === 0) return null;
    return this.decodeValue(m[0]) as V;
  }
  async peek(): Promise<V | null> {
    const m = await this.redis.zrange(this.name, 0, 0);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
  }
  async size(): Promise<number> {
    return this.redis.zcard(this.name);
  }
}

export class RPriorityBlockingQueue<V = any> extends RPriorityQueue<V> {
  async take(): Promise<V> {
    return withDedicatedRedis(this.redis, async (conn, dedicated) => {
      for (;;) {
        const immediate = await this.poll();
        if (immediate !== null) return immediate;
        if (!dedicated) {
          await sleep(20);
          continue;
        }
        try {
          const m: any = await conn.bzpopmin(this.name, 0);
          if (m) {
            const member = Array.isArray(m) ? m[1] : m;
            return this.decodeValue(member) as V;
          }
        } catch (e) {
          const msg = String((e as any)?.message ?? e);
          if (!/unsupported command|unknown command/i.test(msg)) throw e;
          await sleep(20);
        }
      }
    });
  }
}

export class RPriorityDeque<V = any> extends RPriorityQueue<V> {
  async pollLast(): Promise<V | null> {
    const m: any = await (this.redis as any).zpopmax?.(this.name, 1);
    if (!m || m.length === 0) return null;
    return this.decodeValue(m[0]) as V;
  }
}

export class RPriorityBlockingDeque<V = any> extends RPriorityDeque<V> {
  async takeFirst(): Promise<V> {
    for (;;) {
      const v = await this.poll();
      if (v !== null) return v;
      await new Promise((r) => setTimeout(r, 20));
    }
  }
  async takeLast(): Promise<V> {
    for (;;) {
      const v = await this.pollLast();
      if (v !== null) return v;
      await new Promise((r) => setTimeout(r, 20));
    }
  }
}

// -- TransferQueue (GO_ONLY: atomic queue-to-queue migration) --------------------------

export class RTransferQueue<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async transferTo(dest: string): Promise<V | null> {
    const raw = await this.redis.lpop(this.name);
    if (raw === null) return null;
    await this.redis.rpush(dest, raw);
    return this.decodeValue(raw) as V;
  }
}

// -- Array / CircularBuffer (Redis 8.8+ ARRAY; skip when missing) -----------------------

export class RArray<V = any> extends RedisDataStructure {
  private async call(...args: (string | number)[]): Promise<any> {
    return (this.redis as any).call(...args.map(String));
  }
  async set(index: number, value: V): Promise<void> {
    await this.call('AR.SET', this.name, index, this.encodeValue(value));
  }
  async get(index: number): Promise<V | null> {
    const raw: any = await this.call('AR.GET', this.name, index);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async len(): Promise<number> {
    return toNumber(await this.call('AR.LEN', this.name));
  }
}

export class RCircularBuffer<V = any> extends RedisDataStructure {
  private async call(...args: (string | number)[]): Promise<any> {
    return (this.redis as any).call(...args.map(String));
  }
  async add(value: V): Promise<void> {
    // ARRAY-backed ring: best-effort RPUSH-style via AR.APPEND when available
    await this.call('AR.APPEND', this.name, this.encodeValue(value));
  }
  async readAll(): Promise<V[]> {
    const raw: any = await this.call('AR.GET', this.name, '0', '-1');
    const arr = Array.isArray(raw) ? raw : [];
    return arr.map((x: string) => this.decodeValue(x) as V);
  }
}

// -- RMaps (mass HASH helpers; HASH readable as Java RMap) ------------------------------

export class RMaps {
  constructor(private redis: Redis, private codec: Codec = new JsonCodec()) {}
  async setMap(name: string, mapping: Record<string, unknown>): Promise<void> {
    const entries = Object.entries(mapping);
    await this.redis.del(name);
    if (entries.length === 0) return;
    const flat: string[] = [];
    for (const [k, v] of entries) flat.push(this.codec.encode(k), this.codec.encode(v));
    await this.redis.hset(name, ...flat as [string, string]);
  }
  async readAll(name: string): Promise<Map<unknown, unknown>> {
    const all = await this.redis.hgetall(name);
    const out = new Map();
    for (const [k, v] of Object.entries(all)) {
      try {
        out.set(this.codec.decode(k), this.codec.decode(v));
      } catch {
        out.set(k, v);
      }
    }
    return out;
  }
}

// -- RBatch (pipeline; reads return queued placeholders) ---------------------------------

interface Deferred {
  count: number;
  decode: (raws: any[]) => unknown;
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
}

export class RBatch {
  private pipe: any;
  private pending: Deferred[] = [];
  constructor(private redis: Redis) {
    this.pipe = (redis as any).pipeline?.() ?? (redis as any).multi?.();
    if (!this.pipe) throw new Error('redis client does not support pipeline');
  }
  get pipeline(): any {
    return this.pipe;
  }
  /**
   * Queue commands against the pipeline. `fn` issues one or more commands;
   * the returned promise resolves with `decode` applied to their raw results
   * once `execute()` runs (mirrors Redisson's RFuture-per-op semantics).
   * Command count is tracked via pipeline length, so multi-command ops
   * (HGET+HSET etc.) map back correctly.
   */
  enqueue<T>(fn: (pipe: any) => void, decode: (raws: any[]) => T): Promise<T> {
    const before: number = this.pipe.length ?? 0;
    fn(this.pipe);
    const after: number = this.pipe.length ?? before;
    const count = Math.max(0, after - before);
    return new Promise<T>((resolve, reject) => {
      this.pending.push({ count, decode: decode as (raws: any[]) => unknown, resolve: resolve as (v: unknown) => void, reject });
    });
  }
  async execute(): Promise<any[]> {
    const pending = this.pending;
    this.pending = [];
    let results: [unknown, unknown][] | null = null;
    try {
      results = await this.pipe.exec();
    } catch (e) {
      for (const d of pending) d.reject(e);
      throw e;
    }
    const rows: [unknown, unknown][] = results ?? [];
    let i = 0;
    for (const d of pending) {
      const slice = rows.slice(i, i + d.count);
      i += d.count;
      try {
        const err = slice.find(([e]) => e !== null && e !== undefined);
        if (err) d.reject(err[0]);
        else d.resolve(d.decode(slice.map(([, r]) => r)));
      } catch (e) {
        d.reject(e);
      }
    }
    return rows;
  }
  async discard(): Promise<void> {
    const pending = this.pending;
    this.pending = [];
    for (const d of pending) d.reject(new Error('redi: batch discarded'));
    // ioredis pipeline has no discard; reset by dropping reference
    this.pipe = (this.redis as any).pipeline();
  }

  // Typed facades (see src/batch.ts). Writes queue, reads resolve on execute().
  getBucket<V>(name: string, codec?: Codec): BatchBucket<V> {
    return new BatchBucket<V>(this, name, codec ?? new JsonCodec());
  }
  getMap<K, V>(name: string, codec?: Codec): BatchMap<K, V> {
    return new BatchMap<K, V>(this, name, codec ?? new JsonCodec());
  }
  getList<V>(name: string, codec?: Codec): BatchList<V> {
    return new BatchList<V>(this, name, codec ?? new JsonCodec());
  }
  getSet<V>(name: string, codec?: Codec): BatchSet<V> {
    return new BatchSet<V>(this, name, codec ?? new JsonCodec());
  }
  getQueue<V>(name: string, codec?: Codec): BatchQueue<V> {
    return new BatchQueue<V>(this, name, codec ?? new JsonCodec());
  }
  getAtomicLong(name: string): BatchAtomicLong {
    return new BatchAtomicLong(this, name);
  }
  getAtomicDouble(name: string): BatchAtomicDouble {
    return new BatchAtomicDouble(this, name);
  }
  getDeque<V>(name: string, codec?: Codec): BatchDeque<V> {
    return new BatchDeque<V>(this, name, codec ?? new JsonCodec());
  }
  getBitSet(name: string): BatchBitSet {
    return new BatchBitSet(this, name);
  }
  getGeo<V>(name: string, codec?: Codec): BatchGeo<V> {
    return new BatchGeo<V>(this, name, codec ?? new JsonCodec());
  }
  getStream(name: string, codec?: Codec): BatchStream {
    return new BatchStream(this, name, codec ?? new JsonCodec());
  }
  getMapCacheNative<K, V>(name: string, codec?: Codec): BatchMapCacheNative<K, V> {
    return new BatchMapCacheNative<K, V>(this, name, codec ?? new JsonCodec());
  }
  getScoredSortedSet<V>(name: string, codec?: Codec): BatchScoredSortedSet<V> {
    return new BatchScoredSortedSet<V>(this, name, codec ?? new JsonCodec());
  }
}

// -- Native multimap-cache (Redis >= 7.4 HPEXPIRE; NATIVE_OK) --------------------------

/** Native multimaps keep the same HASH field→id + child-collection layout as
 * the base multimaps; delete/unlink must read the HASH before removing it. */
async function removeNativeMultimap(
  redis: Redis,
  name: string,
  collKey: (id: string) => string,
  unlink: boolean,
): Promise<boolean> {
  const existed = ((await redis.exists(name)) as number) > 0;
  const ids = await redis.hvals(name);
  const childKeys = ids.map(collKey);
  for (let i = 0; i < childKeys.length; i += 500) {
    const batch = childKeys.slice(i, i + 500);
    if (unlink) {
      try { await redis.unlink(...batch); } catch { await redis.del(...batch); }
    } else {
      await redis.del(...batch);
    }
  }
  if (unlink) {
    try { await redis.unlink(name); } catch { await redis.del(name); }
  } else {
    await redis.del(name);
  }
  return existed;
}

export class RSetMultimapCacheNative extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  private collKey(id: string): string {
    return suffixName(this.name, id);
  }
  async put(key: unknown, value: unknown): Promise<boolean> {
    const field = this.encodeKey(key);
    const id = multimapInternalId(field);
    await this.redis.hset(this.name, field, id);
    return ((await this.redis.sadd(this.collKey(id), this.encodeValue(value))) as number) > 0;
  }
  async get(key: unknown): Promise<unknown[]> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return [];
    return (await this.redis.smembers(this.collKey(id))).map((x) => this.decodeValue(x));
  }
  async expireKey(key: unknown, ttlMs: number): Promise<boolean> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return false;
    try {
      const r: any = await this.redis.hpexpire(this.name, ttlMs, 'FIELDS', 1, field);
      if (Array.isArray(r) ? r[0] === 1 : toNumber(r) === 1) {
        await this.redis.pexpire(this.collKey(id), ttlMs);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }
  async clear(): Promise<void> {
    await this.delete();
  }
  override async delete(): Promise<boolean> {
    return removeNativeMultimap(this.redis, this.name, (id) => this.collKey(id), false);
  }
  override async unlink(): Promise<boolean> {
    return removeNativeMultimap(this.redis, this.name, (id) => this.collKey(id), true);
  }
}

export class RListMultimapCacheNative extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  private collKey(id: string): string {
    return suffixName(this.name, id);
  }
  async put(key: unknown, value: unknown): Promise<boolean> {
    const field = this.encodeKey(key);
    const id = multimapInternalId(field);
    await this.redis.hset(this.name, field, id);
    await this.redis.rpush(this.collKey(id), this.encodeValue(value));
    return true;
  }
  async get(key: unknown): Promise<unknown[]> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return [];
    return (await this.redis.lrange(this.collKey(id), 0, -1)).map((x) => this.decodeValue(x));
  }
  async expireKey(key: unknown, ttlMs: number): Promise<boolean> {
    const field = this.encodeKey(key);
    const id = await this.redis.hget(this.name, field);
    if (id === null) return false;
    try {
      const r: any = await this.redis.hpexpire(this.name, ttlMs, 'FIELDS', 1, field);
      if (Array.isArray(r) ? r[0] === 1 : toNumber(r) === 1) {
        await this.redis.pexpire(this.collKey(id), ttlMs);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }
  async clear(): Promise<void> {
    await this.delete();
  }
  override async delete(): Promise<boolean> {
    return removeNativeMultimap(this.redis, this.name, (id) => this.collKey(id), false);
  }
  override async unlink(): Promise<boolean> {
    return removeNativeMultimap(this.redis, this.name, (id) => this.collKey(id), true);
  }
}

// -- Operational facades ---------------------------------------------------------------

export class RRedisNodes {
  constructor(private redis: Redis) {}
  async ping(): Promise<string> {
    return this.redis.ping();
  }
  async info(section = 'server'): Promise<string> {
    return this.redis.info(section);
  }
}

/** PARTIAL: ioredis has no Java EvictionPolicy read-proxy; thin pass-through. */
export class RClientSideCaching {
  constructor(private redis: Redis) {}
  async get(key: string): Promise<string | null> {
    return this.redis.get(key);
  }
  async clear(): Promise<void> {
    // no-op: tracking is connection-level in ioredis/RESP3
  }
}
