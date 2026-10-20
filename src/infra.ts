/**
 * Infrastructure facades: RKeys, RScript, RFunction, RSearch, JsonBucket(s),
 * VectorSet, RBatch, RPriority*, RTransferQueue, RArray, RCircularBuffer, RMaps.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { clusterSlot, luaEval, multimapInternalId, sleep, suffixName, toNumber, withDedicatedRedis } from './util.js';
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
  /** Java `getKeysStream()` / `getKeysStreamByPattern(pattern)`: lazy SCAN. */
  async *getKeysStream(pattern = '*', count = 100): AsyncGenerator<string> {
    let cursor = '0';
    do {
      const [next, keys] = (await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', String(count))) as [string, string[]];
      for (const k of keys) yield k;
      cursor = next;
    } while (cursor !== '0');
  }
  async *getKeysStreamByPattern(pattern: string, count = 100): AsyncGenerator<string> {
    yield* this.getKeysStream(pattern, count);
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
  /** Java `expire(String, long, TimeUnit)`: single key. */
  async expire(key: string, ttlMs: number): Promise<boolean>;
  /** Java `expire(Duration, String...)`: many keys, returns how many got a TTL. */
  async expire(ttlMs: number, ...keys: string[]): Promise<number>;
  async expire(a: string | number, ...rest: (number | string)[]): Promise<boolean | number> {
    if (typeof a === 'string') {
      return ((await this.redis.pexpire(a, Number(rest[0]))) as number) === 1;
    }
    const keys = rest as string[];
    if (keys.length === 0) return 0;
    const pipe = (this.redis as any).pipeline?.();
    if (!pipe) {
      let n = 0;
      for (const k of keys) n += ((await this.redis.pexpire(k, a)) as number) === 1 ? 1 : 0;
      return n;
    }
    for (const k of keys) pipe.pexpire(k, a);
    const res = await pipe.exec();
    return (res ?? []).reduce((n: number, [e, r]: [unknown, unknown]) => n + (e === null && Number(r) === 1 ? 1 : 0), 0);
  }
  /** Java `expireAt(String, long)` / `expireAt(Instant, String...)` (ms epoch). */
  async expireAt(key: string, timestampMs: number): Promise<boolean>;
  async expireAt(timestampMs: number, ...keys: string[]): Promise<number>;
  async expireAt(a: string | number, ...rest: (number | string)[]): Promise<boolean | number> {
    if (typeof a === 'string') {
      return ((await this.redis.pexpireat(a, Number(rest[0]))) as number) === 1;
    }
    const keys = rest as string[];
    let n = 0;
    for (const k of keys) n += ((await this.redis.pexpireat(k, a)) as number) === 1 ? 1 : 0;
    return n;
  }
  /** Java `clearExpire(String)`: PERSIST — true when a TTL was removed. */
  async clearExpire(key: string): Promise<boolean> {
    return ((await this.redis.persist(key)) as number) === 1;
  }
  /** Java `remainTimeToLive(String)`: PTTL — ms, `-1` no TTL, `-2` missing. */
  async remainTimeToLive(key: string): Promise<number> {
    return this.redis.pttl(key);
  }
  /** Java `getSlot(String)`: Redis Cluster slot (hash tag honoured). */
  async getSlot(key: string): Promise<number> {
    return clusterSlot(key);
  }
  /** Java `move(String, int)`: MOVE key to another logical database. */
  async move(key: string, db: number): Promise<boolean> {
    return ((await this.redis.move(key, db)) as number) === 1;
  }
  async touch(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    try {
      return await this.redis.touch(...keys);
    } catch {
      return 0;
    }
  }
  /** Java `unlinkByPattern(String)`: SCAN MATCH + UNLINK (DELETE fallback). */
  async unlinkByPattern(pattern: string): Promise<number> {
    const keys = await this.getKeys(pattern);
    if (keys.length === 0) return 0;
    let total = 0;
    for (let i = 0; i < keys.length; i += 500) {
      const chunk = keys.slice(i, i + 500);
      try {
        total += await this.redis.unlink(...chunk);
      } catch {
        total += await this.redis.del(...chunk);
      }
    }
    return total;
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
  /** Java `scriptKill()`: SCRIPT KILL (errors when no script is running). */
  async scriptKill(): Promise<void> {
    await (this.redis as any).script('KILL');
  }
  /** Java `scriptFlush()`: SCRIPT FLUSH. */
  async scriptFlush(): Promise<void> {
    await (this.redis as any).script('FLUSH');
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

export interface FunctionLibraryInfo {
  name: string;
  engine?: string;
  functions: string[];
  code?: string;
}

function flatRecord(flat: any): Record<string, any> {
  const out: Record<string, any> = {};
  if (Array.isArray(flat)) for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = flat[i + 1];
  return out;
}

export class RFunction {
  constructor(private redis: Redis) {}
  private async call(...args: string[]): Promise<any> {
    return (this.redis as any).call(...args);
  }
  async load(code: string, replace = false): Promise<string> {
    return replace
      ? (this.call('FUNCTION', 'LOAD', 'REPLACE', code) as Promise<string>)
      : (this.call('FUNCTION', 'LOAD', code) as Promise<string>);
  }
  async delete(lib: string): Promise<void> {
    await this.call('FUNCTION', 'DELETE', lib);
  }
  async flush(): Promise<void> {
    await this.call('FUNCTION', 'FLUSH');
  }
  /** Java `kill()`: FUNCTION KILL (only while a function is running). */
  async kill(): Promise<void> {
    await this.call('FUNCTION', 'KILL');
  }
  /** Java `list([libraryName])`: FUNCTION LIST → typed library info. */
  async list(lib?: string): Promise<FunctionLibraryInfo[]> {
    const args: string[] = ['FUNCTION', 'LIST'];
    if (lib) args.push('LIBRARYNAME', lib);
    const raw: any = await this.call(...args);
    const out: FunctionLibraryInfo[] = [];
    for (const item of (raw ?? []) as any[]) {
      const rec = flatRecord(item);
      const fns: string[] = [];
      for (const f of (rec.functions ?? []) as any[]) {
        const fr = flatRecord(f);
        if (fr.name !== undefined) fns.push(String(fr.name));
      }
      out.push({
        name: String(rec.library_name ?? rec.name ?? ''),
        engine: rec.engine === undefined ? undefined : String(rec.engine),
        functions: fns,
        code: rec.library_code === undefined ? undefined : String(rec.library_code),
      });
    }
    return out;
  }
  /** Java `stats()`: FUNCTION STATS as a flat field/value record. */
  async stats(): Promise<Record<string, any>> {
    return flatRecord(await this.call('FUNCTION', 'STATS'));
  }
  /** Java `dump()`: FUNCTION DUMP payload as bytes. */
  async dump(): Promise<Buffer> {
    try {
      return Buffer.from(await (this.redis as any).callBuffer('FUNCTION', 'DUMP'));
    } catch {
      return Buffer.from(String(await this.call('FUNCTION', 'DUMP')), 'latin1');
    }
  }
  /** Java `restore(byte[])`: FUNCTION RESTORE. */
  async restore(payload: Buffer | string): Promise<void> {
    const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'latin1');
    await (this.redis as any).call('FUNCTION', 'RESTORE', buf);
  }
  /** Java `restoreAndReplace(byte[])`: FUNCTION RESTORE … REPLACE. */
  async restoreAndReplace(payload: Buffer | string): Promise<void> {
    const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'latin1');
    await (this.redis as any).call('FUNCTION', 'RESTORE', buf, 'REPLACE');
  }
  /** Java `restoreAfterFlush(byte[])`: FUNCTION RESTORE … FLUSH. */
  async restoreAfterFlush(payload: Buffer | string): Promise<void> {
    const buf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'latin1');
    await (this.redis as any).call('FUNCTION', 'RESTORE', buf, 'FLUSH');
  }
  async callFn(name: string, keys: string[] = [], args: string[] = []): Promise<any> {
    return this.call('FCALL', name, String(keys.length), ...keys, ...args);
  }
  async callReadOnly(name: string, keys: string[] = [], args: string[] = []): Promise<any> {
    return this.call('FCALL_RO', name, String(keys.length), ...keys, ...args);
  }
}

// -- RSearch (RediSearch subset; native protocol, auto interop) ---------------------

export interface SearchParams {
  limit?: [number, number];
  returnFields?: string[];
  noContent?: boolean;
  sortBy?: string;
  sortAsc?: boolean;
  filter?: string;
  inKeys?: string[];
  params?: Record<string, string>;
  dialect?: number;
  timeoutMs?: number;
}

export interface AggregateParams {
  load?: string[];
  groupBy?: string[];
  /** Raw REDUCE tokens, e.g. ['SUM', '1', '@price', 'AS', 'total']. */
  reduce?: string[];
  /** Raw APPLY tokens, e.g. ['@price * 2', 'AS', 'double']. */
  apply?: string[];
  /** Raw SORTBY tokens, e.g. ['2', '@total', 'DESC']. */
  sortBy?: string[];
  limit?: [number, number];
  params?: Record<string, string>;
  dialect?: number;
  /** Appended verbatim for advanced options. */
  rawArgs?: string[];
}

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
  /** Java `dropIndexAndDocuments`. */
  async dropIndexAndDocuments(name: string): Promise<void> {
    return this.dropIndex(name, true);
  }
  /** Java `getIndexes()`: FT._LIST. */
  async getIndexes(): Promise<string[]> {
    return ((await this.call('FT._LIST')) as string[]) ?? [];
  }
  /** Java `hasIndex(String)`. */
  async hasIndex(name: string): Promise<boolean> {
    return (await this.getIndexes()).includes(name);
  }
  /** Java `info(String)`: FT.INFO flat field/value record. */
  async info(name: string): Promise<Record<string, any>> {
    const flat: any = await this.call('FT.INFO', name);
    const out: Record<string, any> = {};
    if (Array.isArray(flat)) for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = flat[i + 1];
    return out;
  }
  /** Java `addAlias(String alias, String index)`. */
  async addAlias(alias: string, index: string): Promise<void> {
    await this.call('FT.ALIASADD', alias, index);
  }
  async delAlias(alias: string): Promise<void> {
    await this.call('FT.ALIASDEL', alias);
  }
  async updateAlias(alias: string, index: string): Promise<void> {
    await this.call('FT.ALIASUPDATE', alias, index);
  }
  /** Java `getConfig(String)`: FT.CONFIG GET → key/value map. */
  async getConfig(key: string): Promise<Record<string, string>> {
    const r: any = await this.call('FT.CONFIG', 'GET', key);
    const out: Record<string, string> = {};
    for (const row of (r ?? []) as any[]) {
      if (Array.isArray(row) && row.length >= 2) out[String(row[0])] = String(row[1]);
    }
    return out;
  }
  async setConfig(key: string, value: string): Promise<void> {
    await this.call('FT.CONFIG', 'SET', key, value);
  }
  /** Java `updateSynonyms(index, groupId, terms...)`: FT.SYNUPDATE. */
  async updateSynonyms(index: string, groupId: string, ...terms: string[]): Promise<void> {
    await this.call('FT.SYNUPDATE', index, groupId, ...terms);
  }
  /** Java `dumpSynonyms()`: FT.SYNDUMP → term → group ids. */
  async dumpSynonyms(index: string): Promise<Map<string, string[]>> {
    const r: any = await this.call('FT.SYNDUMP', index);
    const out = new Map<string, string[]>();
    for (let i = 0; i + 1 < ((r ?? []) as any[]).length; i += 2) out.set(String(r[i]), ((r[i + 1] ?? []) as any[]).map(String));
    return out;
  }
  /** Java `addDict(name, terms...)`: FT.DICTADD. */
  async addDict(name: string, ...terms: string[]): Promise<number> {
    return toNumber(await this.call('FT.DICTADD', name, ...terms));
  }
  async delDict(name: string, ...terms: string[]): Promise<number> {
    return toNumber(await this.call('FT.DICTDEL', name, ...terms));
  }
  async dumpDict(name: string): Promise<string[]> {
    return ((await this.call('FT.DICTDUMP', name)) as string[]) ?? [];
  }
  /** Java `spellcheck(index, query, options?)`: FT.SPELLCHECK → suggestion map. */
  async spellcheck(index: string, query: string, opts: { distance?: number; includeTerm?: boolean } = {}): Promise<Map<string, Map<string, number>>> {
    const args: (string | number)[] = ['FT.SPELLCHECK', index, query];
    if (opts.distance !== undefined) args.push('DISTANCE', opts.distance);
    if (opts.includeTerm) args.push('TERMS', 'INCLUDE');
    const r: any = await this.call(...args);
    const out = new Map<string, Map<string, number>>();
    for (const row of (r ?? []) as any[]) {
      const term = String(row[0]);
      const suggestions = new Map<string, number>();
      for (const s of (row[1] ?? []) as any[]) {
        if (Array.isArray(s) && s.length >= 2) suggestions.set(String(s[0]), Number(s[1]));
      }
      out.set(term, suggestions);
    }
    return out;
  }
  async search(name: string, query: string, params: SearchParams = {}): Promise<{ total: number; docs: any[] }> {
    const args: string[] = ['FT.SEARCH', name, query];
    if (params.noContent) args.push('NOCONTENT');
    if (params.inKeys?.length) args.push('INKEYS', String(params.inKeys.length), ...params.inKeys);
    if (params.returnFields?.length) args.push('RETURN', String(params.returnFields.length), ...params.returnFields);
    if (params.filter) args.push('FILTER', params.filter);
    if (params.sortBy) {
      args.push('SORTBY', params.sortBy, params.sortAsc === false ? 'DESC' : 'ASC');
    }
    if (params.limit) args.push('LIMIT', String(params.limit[0]), String(params.limit[1]));
    if (params.params) {
      const entries = Object.entries(params.params);
      args.push('PARAMS', String(entries.length * 2), ...entries.flat());
    }
    if (params.dialect !== undefined) args.push('DIALECT', String(params.dialect));
    if (params.timeoutMs !== undefined) args.push('TIMEOUT', String(params.timeoutMs));
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
  /** Java `aggregate(index, query, options)`: FT.AGGREGATE → rows. */
  async aggregate(name: string, query: string, params: AggregateParams = {}): Promise<{ total: number; rows: Record<string, any>[] }> {
    const args: (string | number)[] = ['FT.AGGREGATE', name, query];
    if (params.load?.length) args.push('LOAD', params.load.length, ...params.load);
    if (params.groupBy?.length) {
      args.push('GROUPBY', params.groupBy.length, ...params.groupBy);
      if (params.reduce?.length) args.push('REDUCE', ...params.reduce);
    }
    if (params.apply?.length) args.push('APPLY', ...params.apply);
    if (params.sortBy?.length) args.push('SORTBY', ...params.sortBy);
    if (params.limit) args.push('LIMIT', params.limit[0], params.limit[1]);
    if (params.params) {
      const entries = Object.entries(params.params);
      args.push('PARAMS', entries.length * 2, ...entries.flat());
    }
    if (params.dialect !== undefined) args.push('DIALECT', params.dialect);
    if (params.rawArgs?.length) args.push(...params.rawArgs);
    const r: any = await this.call(...args);
    const arr = (r ?? []) as any[];
    const total = arr.length > 0 ? Number(arr[0]) : 0;
    const rows: Record<string, any>[] = [];
    for (const row of (arr.slice(1) ?? []) as any[]) {
      if (!Array.isArray(row)) continue;
      const rec: Record<string, any> = {};
      for (let i = 0; i + 1 < row.length; i += 2) rec[String(row[i])] = row[i + 1];
      rows.push(rec);
    }
    return { total, rows };
  }
}

// -- JSON buckets (RedisJSON; values use plain JSON, not Java codec) ----------------

export class RJsonBucket<V = any> {
  constructor(private redis: Redis, readonly name: string, private codec?: Codec) {}
  private enc(value: unknown): string {
    return this.codec ? this.codec.encode(value) : JSON.stringify(value);
  }
  private dec(raw: any): any {
    return this.decWith(raw, this.codec);
  }
  private decWith(raw: any, codec?: Codec | null): any {
    if (raw === null || raw === undefined) return null;
    if (codec) {
      try { return codec.decode(typeof raw === 'string' ? raw : JSON.stringify(raw)); } catch { /* fall through */ }
    }
    try {
      return JSON.parse(typeof raw === 'string' ? raw : JSON.stringify(raw));
    } catch {
      return raw;
    }
  }
  /** Normalize a RedisJSON reply into an array (scalar → singleton). */
  private list(raw: any): any[] {
    if (raw === null || raw === undefined) return [];
    return Array.isArray(raw) ? raw : [raw];
  }
  private numList(raw: any): number[] {
    return this.list(raw).map((x) => toNumber(x));
  }
  private decodeList(raw: any, codec?: Codec | null): any[] {
    return this.list(raw).map((x) => this.decWith(x, codec ?? this.codec));
  }
  private unwrap(raw: any): any {
    return Array.isArray(raw) ? raw[0] : raw;
  }
  private payload(value: unknown): string {
    return this.enc(value);
  }
  /** JSON.SET key path value [NX|XX]. */
  async set(value: V, path = '$', opts: { nx?: boolean; xx?: boolean } = {}): Promise<void> {
    const args: string[] = [this.name, path, this.enc(value)];
    if (opts.nx) args.push('NX');
    else if (opts.xx) args.push('XX');
    await this.redis.call('JSON.SET', ...args);
  }
  /** JSON.GET: without a path returns the document; with one, the RedisJSON
   *  match array (paths starting with `$`). The `(codec, paths...)` overload
   *  mirrors Java's multi-path read and decodes with the supplied codec. */
  async get(path?: string): Promise<V | null>;
  async get(codec: Codec | null, ...paths: string[]): Promise<any>;
  async get(a?: string | Codec | null, ...paths: string[]): Promise<any> {
    if (typeof a !== 'string' && a !== undefined) {
      const codec = a;
      const args: string[] = [this.name, ...paths];
      const raw: any = await this.redis.call('JSON.GET', ...args);
      return this.decWith(raw, codec);
    }
    const raw: any = a === undefined
      ? await this.redis.call('JSON.GET', this.name)
      : await this.redis.call('JSON.GET', this.name, a);
    return this.dec(raw) as V | null;
  }
  async delete(path = '$'): Promise<number> {
    return toNumber(await this.redis.call('JSON.DEL', this.name, path));
  }
  /** JSON.SET ... NX (root value or explicit path). */
  async setIfAbsent(value: V): Promise<boolean>;
  async setIfAbsent(path: string, value: unknown): Promise<boolean>;
  async setIfAbsent(a: V | string, b?: unknown): Promise<boolean> {
    const [path, value] = arguments.length >= 2 ? [String(a), b] : ['$', a];
    const r: any = await this.redis.call('JSON.SET', this.name, path, this.enc(value), 'NX');
    return r === 'OK' || r === true;
  }
  async trySet(value: V): Promise<boolean>;
  async trySet(path: string, value: unknown): Promise<boolean>;
  async trySet(a: V | string, b?: unknown): Promise<boolean> {
    return arguments.length >= 2 ? this.setIfAbsent(String(a), b) : this.setIfAbsent(a as V);
  }
  /** JSON.SET ... XX. */
  async setIfExists(value: V): Promise<boolean>;
  async setIfExists(path: string, value: unknown): Promise<boolean>;
  async setIfExists(a: V | string, b?: unknown): Promise<boolean> {
    const [path, value] = arguments.length >= 2 ? [String(a), b] : ['$', a];
    const r: any = await this.redis.call('JSON.SET', this.name, path, this.enc(value), 'XX');
    return r === 'OK' || r === true;
  }
  /** Atomic compare-and-set; `$` paths are compared against RedisJSON's match
   *  array (like Java's path-based CAS). */
  async compareAndSet(expect: V, update: V | null): Promise<boolean>;
  async compareAndSet(path: string, expect: unknown, update: unknown | null): Promise<boolean>;
  async compareAndSet(a: V | string, b: unknown, c?: unknown): Promise<boolean> {
    const three = arguments.length >= 3;
    const path = three ? String(a) : '$';
    const expect = three ? b : a;
    const update = three ? c : b;
    const expected = path.startsWith('$')
      ? (this.codec ? this.enc([expect]) : JSON.stringify([expect]))
      : this.enc(expect);
    const lua = `local cur = redis.call('json.get', KEYS[1], ARGV[1])
if cur == ARGV[2] then
    if ARGV[4] == '1' then
        redis.call('json.del', KEYS[1], ARGV[1])
    else
        redis.call('json.set', KEYS[1], ARGV[1], ARGV[3])
    end
    return 1
end
return 0`;
    if (update === null || update === undefined) {
      return toNumber(await this.redis.eval(lua, 1, this.name, path, expected, '', '1')) === 1;
    }
    return toNumber(await this.redis.eval(lua, 1, this.name, path, expected, this.enc(update), '0')) === 1;
  }
  /** Atomic get-and-set (root or path); returns the previous value. */
  async getAndSet(newValue: V): Promise<V | null>;
  async getAndSet(path: string, newValue: unknown): Promise<V | null>;
  async getAndSet(a: V | string, b?: unknown): Promise<V | null> {
    const three = arguments.length >= 2;
    if (!three) {
      const lua = `local cur = redis.call('json.get', KEYS[1])
redis.call('json.set', KEYS[1], '$', ARGV[1])
return cur`;
      return this.dec(await this.redis.eval(lua, 1, this.name, this.enc(a))) as V | null;
    }
    const lua = `local cur = redis.call('json.get', KEYS[1], ARGV[1])
redis.call('json.set', KEYS[1], ARGV[1], ARGV[2])
return cur`;
    return this.dec(await this.redis.eval(lua, 1, this.name, String(a), this.enc(b))) as V | null;
  }
  /** Atomic get-and-delete (root or path). */
  async getAndDelete(): Promise<V | null>;
  async getAndDelete(path: string): Promise<V | null>;
  async getAndDelete(path = '$'): Promise<V | null> {
    const lua = path === '$'
      ? `local cur = redis.call('json.get', KEYS[1])
redis.call('json.del', KEYS[1], '$')
return cur`
      : `local cur = redis.call('json.get', KEYS[1], ARGV[1])
redis.call('json.del', KEYS[1], ARGV[1])
return cur`;
    return this.dec(await this.redis.eval(lua, 1, this.name, path)) as V | null;
  }
  async stringSize(path = '$'): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.STRLEN', this.name, path)));
  }
  /** Java `stringSizeMulti(path)`: all JSON.STRLEN matches as a list. */
  async stringSizeMulti(path: string): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.STRLEN', this.name, path));
  }
  async stringAppend(path: string, value: unknown): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.STRAPPEND', this.name, path, this.enc(value))));
  }
  /** Java `stringAppendMulti(path, value)`. */
  async stringAppendMulti(path: string, value: unknown): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.STRAPPEND', this.name, path, this.enc(value)));
  }
  async arrayAppend(path: string, ...values: unknown[]): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.ARRAPPEND', this.name, path, ...values.map((v) => this.enc(v)))));
  }
  /** Java `arrayAppendMulti(path, values...)`. */
  async arrayAppendMulti(path: string, ...values: unknown[]): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.ARRAPPEND', this.name, path, ...values.map((v) => this.enc(v))));
  }
  async arrayIndex(path: string, value: unknown): Promise<number>;
  async arrayIndex(path: string, value: unknown, start: number, stop: number): Promise<number>;
  async arrayIndex(path: string, value: unknown, start?: number, stop?: number): Promise<number> {
    const args: (string | number)[] = [this.name, path, this.enc(value)];
    if (start !== undefined) args.push(String(start), String(stop));
    return toNumber(this.unwrap(await this.redis.call('JSON.ARRINDEX', ...args as any)));
  }
  /** Java `arrayIndexMulti(path, value[, start, stop])`: all matches. */
  async arrayIndexMulti(path: string, value: unknown): Promise<number[]>;
  async arrayIndexMulti(path: string, value: unknown, start: number, stop: number): Promise<number[]>;
  async arrayIndexMulti(path: string, value: unknown, start?: number, stop?: number): Promise<number[]> {
    const args: (string | number)[] = [this.name, path, this.enc(value)];
    if (start !== undefined) args.push(String(start), String(stop));
    return this.numList(await this.redis.call('JSON.ARRINDEX', ...args as any));
  }
  async arrayInsert(path: string, index: number, ...values: unknown[]): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.ARRINSERT', this.name, path, String(index), ...values.map((v) => this.enc(v)))));
  }
  /** Java `arrayInsertMulti(path, index, values...)`. */
  async arrayInsertMulti(path: string, index: number, ...values: unknown[]): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.ARRINSERT', this.name, path, String(index), ...values.map((v) => this.enc(v))));
  }
  async arraySize(path = '$'): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.ARRLEN', this.name, path)));
  }
  /** Java `arraySizeMulti(path)`. */
  async arraySizeMulti(path: string): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.ARRLEN', this.name, path));
  }
  async arrayPop(path: string, index = -1): Promise<any> {
    return this.dec(this.unwrap(await this.redis.call('JSON.ARRPOP', this.name, path, String(index))));
  }
  /** Java `arrayPopMulti(codec, path, index)`: all popped values. */
  async arrayPopMulti<T = any>(codec: Codec | null, path: string, index: number): Promise<T[]> {
    return this.decodeList(await this.redis.call('JSON.ARRPOP', this.name, path, String(index)), codec) as T[];
  }
  async arrayPollFirst(path: string): Promise<any> {
    return this.arrayPop(path, 0);
  }
  /** Java `arrayPollFirstMulti(codec, path)`. */
  async arrayPollFirstMulti<T = any>(codec: Codec | null, path: string): Promise<T[]> {
    return this.decodeList(await this.redis.call('JSON.ARRPOP', this.name, path, '0'), codec) as T[];
  }
  async arrayPollLast(path: string): Promise<any> {
    return this.arrayPop(path, -1);
  }
  /** Java `arrayPollLastMulti(codec, path)`. */
  async arrayPollLastMulti<T = any>(codec: Codec | null, path: string): Promise<T[]> {
    return this.decodeList(await this.redis.call('JSON.ARRPOP', this.name, path), codec) as T[];
  }
  async arrayTrim(path: string, start: number, stop: number): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.ARRTRIM', this.name, path, String(start), String(stop))));
  }
  /** Java `arrayTrimMulti(path, start, stop)`. */
  async arrayTrimMulti(path: string, start: number, stop: number): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.ARRTRIM', this.name, path, String(start), String(stop)));
  }
  async clear(path?: string): Promise<number> {
    return toNumber(path === undefined
      ? await this.redis.call('JSON.CLEAR', this.name)
      : await this.redis.call('JSON.CLEAR', this.name, path));
  }
  async incrementAndGet(path: string, delta: number): Promise<number> {
    const raw: any = await this.redis.call('JSON.NUMINCRBY', this.name, path, String(delta));
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return Number(this.unwrap(parsed));
  }
  /** Java `incrementAndGetMulti(path, delta)`: all matched numbers. */
  async incrementAndGetMulti(path: string, delta: number): Promise<number[]> {
    const raw: any = await this.redis.call('JSON.NUMINCRBY', this.name, path, String(delta));
    const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return this.numList(parsed);
  }
  async merge(path: string, value: unknown): Promise<void> {
    await this.redis.call('JSON.MERGE', this.name, path, this.enc(value));
  }
  async countKeys(path = '$'): Promise<number> {
    return toNumber(this.unwrap(await this.redis.call('JSON.OBJLEN', this.name, path)));
  }
  /** Java `countKeysMulti(path)`: object sizes for every match. */
  async countKeysMulti(path: string): Promise<number[]> {
    return this.numList(await this.redis.call('JSON.OBJLEN', this.name, path));
  }
  async getKeys(path = '$'): Promise<string[]> {
    const raw: any = await this.redis.call('JSON.OBJKEYS', this.name, path);
    const first = Array.isArray(raw) && Array.isArray(raw[0]) ? raw[0] : raw;
    return (first ?? []) as string[];
  }
  /** Java `getKeysMulti(path)`: key lists for every match; a legacy
   *  single-object path is wrapped as one list so the shape stays nested. */
  async getKeysMulti(path: string): Promise<string[][]> {
    const raw: any = await this.redis.call('JSON.OBJKEYS', this.name, path);
    if (!Array.isArray(raw) || raw.length === 0) return [];
    return raw.some((x: any) => Array.isArray(x)) ? (raw as string[][]) : [raw as string[]];
  }
  async toggle(path: string): Promise<boolean> {
    return toNumber(this.unwrap(await this.redis.call('JSON.TOGGLE', this.name, path))) === 1;
  }
  /** Java `toggleMulti(path)`: null entries stay null (non-boolean matches). */
  async toggleMulti(path: string): Promise<(boolean | null)[]> {
    return this.list(await this.redis.call('JSON.TOGGLE', this.name, path))
      .map((x) => (x === null || x === undefined ? null : toNumber(x) === 1));
  }
  async getType(path = '$'): Promise<string> {
    return String(this.unwrap(await this.redis.call('JSON.TYPE', this.name, path)));
  }
}

export class RJsonBuckets {
  constructor(private redis: Redis, private codec?: Codec) {}
  bucket<V = any>(name: string): RJsonBucket<V> {
    return new RJsonBucket<V>(this.redis, name, this.codec);
  }
  /** Java `set(Map)` / `set(codec, path, Map)`: JSON.MSET (path defaults to `.`). */
  async set(mapping: Record<string, unknown> | Map<string, unknown>, path = '.'): Promise<void> {
    const entries = mapping instanceof Map ? [...mapping.entries()] : Object.entries(mapping);
    if (entries.length === 0) return;
    const args: unknown[] = [];
    for (const [key, value] of entries) {
      args.push(key, path, this.codec ? this.codec.encode(value) : JSON.stringify(value));
    }
    await (this.redis as any).call('JSON.MSET', ...args);
  }
  /** Java `get(String...)` / `get(codec, path, String...)`: JSON.MGET.
   *  Missing keys (null replies) are omitted from the result map. */
  async get<V = any>(...keys: string[]): Promise<Map<string, V>> {
    return this.getWithPath<V>('.', ...keys);
  }
  async getWithPath<V = any>(path: string, ...keys: string[]): Promise<Map<string, V>> {
    const out = new Map<string, V>();
    if (keys.length === 0) return out;
    const raw: any = await (this.redis as any).call('JSON.MGET', ...keys, path);
    const arr = Array.isArray(raw) ? raw : [raw];
    keys.forEach((k, i) => {
      const v = arr[i];
      if (v === null || v === undefined) return;
      let decoded: any;
      try {
        decoded = this.codec ? this.codec.decode(typeof v === 'string' ? v : JSON.stringify(v)) : JSON.parse(typeof v === 'string' ? v : JSON.stringify(v));
      } catch {
        decoded = v;
      }
      out.set(k, decoded as V);
    });
    return out;
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
  /** Java `size()`: VCARD. */
  async size(): Promise<number> {
    return this.card();
  }
  /** Java `dimensions()`: VDIM. */
  async dimensions(): Promise<number> {
    return toNumber(await this.redis.call('VDIM', this.name));
  }
  async remove(element: string): Promise<boolean> {
    return toNumber(await this.redis.call('VREM', this.name, element)) === 1;
  }
  /** Java `getVector(element)`: VEMB → flat float array. */
  async getVector(element: string): Promise<number[]> {
    const r: any = await this.redis.call('VEMB', this.name, element);
    return (r ?? []).map((x: any) => Number(x));
  }
  /** Java `contains(element)`: VEMB returns nil for absent members. */
  async contains(element: string): Promise<boolean> {
    const r: any = await this.redis.call('VEMB', this.name, element);
    return r !== null && r !== undefined;
  }
  /** Java `random()` / `random(int)`: VRANDMEMBER. */
  async random(): Promise<string | null>;
  async random(count: number): Promise<string[]>;
  async random(count?: number): Promise<string | null | string[]> {
    if (count === undefined) {
      const r: any = await this.redis.call('VRANDMEMBER', this.name);
      return r === null || r === undefined ? null : String(r);
    }
    return ((await this.redis.call('VRANDMEMBER', this.name, String(count))) as string[]) ?? [];
  }
  /** Java `range(start, end[, count])`: VRANGE over lexicographic element names. */
  async range(start: string, end: string, count?: number): Promise<string[]> {
    const args: string[] = ['VRANGE', this.name, start, end];
    if (count !== undefined) args.push(String(count));
    return ((await (this.redis as any).call(...args)) as string[]) ?? [];
  }
  /** Java `getNeighbors(element)`: VLINKS (member names only). */
  async getNeighbors(element: string): Promise<string[]> {
    return ((await this.redis.call('VLINKS', this.name, element)) as string[]) ?? [];
  }
  /** Java `getInfo()`: VINFO flat field/value pairs. */
  async getInfo(): Promise<Record<string, any>> {
    const flat: any = await this.redis.call('VINFO', this.name);
    const out: Record<string, any> = {};
    if (Array.isArray(flat)) for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = flat[i + 1];
    return out;
  }
  /** Java `getAttributes(element)`: VGETATTR + codec decode. */
  async getAttributes(element: string): Promise<any | null> {
    const r: any = await this.redis.call('VGETATTR', this.name, element);
    if (r === null || r === undefined) return null;
    try {
      return this.codec.decode(String(r));
    } catch {
      return String(r);
    }
  }
  /** Java `setAttributes(element, value)`: VSETATTR with codec-encoded JSON. */
  async setAttributes(element: string, value: unknown): Promise<boolean> {
    return toNumber(await this.redis.call('VSETATTR', this.name, element, this.encodeValue(value))) === 1;
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
