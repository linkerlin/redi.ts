/**
 * Infrastructure facades: RKeys, RScript, RFunction, RSearch, JsonBucket(s),
 * VectorSet, RBatch, RPriority*, RTransferQueue, RMaps.
 * (RArray/RCircularBuffer live in src/arrays.ts.)
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { sharedKeysListenerHub, type ObjectListener } from './listeners.js';
import { clusterSlot, luaEval, multimapInternalId, sleep, suffixName, toNumber, withDedicatedRedis } from './util.js';
import { BatchAtomicDouble, BatchAtomicLong, BatchBitSet, BatchBucket, BatchCountMin, BatchDeque, BatchGeo, BatchList, BatchMap, BatchMapCacheNative, BatchQueue, BatchScoredSortedSet, BatchSet, BatchStream, BatchTimeSeriesNative } from './batch.js';

// -- RKeys -----------------------------------------------------------------------

/** Java `MigrateArgs` (multiple keys, optional COPY/REPLACE/AUTH/AUTH2). */
export interface MigrateArgs {
  keys: string[];
  host: string;
  port: number;
  database: number;
  timeoutMs: number;
  copy?: boolean;
  replace?: boolean;
  password?: string;
  username?: string;
}

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
    return this.scanKeys(pattern, undefined, count);
  }
  private async scanKeys(pattern: string, limit?: number, count = 100): Promise<string[]> {
    const out: string[] = [];
    let cursor = '0';
    do {
      const [next, keys] = (await this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', count)) as [string, string[]];
      for (const key of keys) {
        out.push(key);
        if (limit !== undefined && out.length >= limit) return out;
      }
      cursor = next;
    } while (cursor !== '0');
    return out;
  }
  /** Java `getKeysWithLimit(limit)`: at most `limit` keys. */
  async getKeysWithLimit(limit: number): Promise<string[]>;
  /** Java `getKeysWithLimit(pattern, limit)`. */
  async getKeysWithLimit(pattern: string, limit: number): Promise<string[]>;
  async getKeysWithLimit(a: string | number, b?: number): Promise<string[]> {
    const pattern = typeof a === 'string' ? a : '*';
    const limit = typeof a === 'string' ? (b as number) : a;
    if (limit <= 0) return [];
    return this.scanKeys(pattern, limit);
  }
  /** Java `getKeysByPattern(pattern[, count])`: SCAN MATCH. */
  async getKeysByPattern(pattern: string, count = 100): Promise<string[]> {
    return this.scanKeys(pattern, undefined, count);
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
    // Java 4.8: EXISTS-guarded — a missing source returns false, not an error.
    const s = "if redis.call('exists', KEYS[1]) == 1 then return redis.call('renamenx', KEYS[1], KEYS[2]) else return 0 end";
    try {
      return Number(await luaEval(this.redis, s, 2, oldKey, newKey)) === 1;
    } catch {
      if (!((await this.redis.exists(oldKey)) as number)) return false;
      return ((await this.redis.renamenx(oldKey, newKey)) as number) === 1;
    }
  }
  /** Java 4.8 `RKeys.time()`: server TIME as { seconds, microseconds }. */
  async time(): Promise<{ seconds: number; microseconds: number }> {
    const r = await this.redis.time();
    return { seconds: Number(r[0]), microseconds: Number(r[1]) };
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
  /** Java `migrate(name, host, port, database, timeout)`: single-key MIGRATE. */
  async migrate(name: string, host: string, port: number, database: number, timeoutMs: number): Promise<void>;
  /** Java `migrate(MigrateArgs)`: optional COPY/REPLACE/AUTH2 and multiple KEYS. */
  async migrate(args: MigrateArgs): Promise<void>;
  async migrate(nameOrArgs: string | MigrateArgs, host?: string, port?: number, database?: number, timeoutMs?: number): Promise<void> {
    if (typeof nameOrArgs === 'string') {
      await (this.redis as any).call('MIGRATE', host, String(port), nameOrArgs, String(database), String(timeoutMs));
      return;
    }
    const a = nameOrArgs;
    if (a.keys.length === 0) throw new Error('MigrateArgs.keys must not be empty');
    const args: string[] = [a.host, String(a.port), '', String(a.database), String(a.timeoutMs)];
    if (a.copy) args.push('COPY');
    if (a.replace) args.push('REPLACE');
    if (a.username !== undefined && a.username !== '') args.push('AUTH2', a.username, a.password ?? '');
    else if (a.password !== undefined && a.password !== '') args.push('AUTH', a.password);
    args.push('KEYS', ...a.keys);
    await (this.redis as any).call('MIGRATE', ...args);
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
  /** Java `flushdbParallel()`: FLUSHDB ASYNC (single node here). */
  async flushdbParallel(): Promise<void> {
    await (this.redis as any).call('FLUSHDB', 'ASYNC');
  }
  async flushall(): Promise<void> {
    await this.redis.flushall();
  }
  /** Java `flushallParallel()`: FLUSHALL ASYNC (single node here). */
  async flushallParallel(): Promise<void> {
    await (this.redis as any).call('FLUSHALL', 'ASYNC');
  }
  /** Java `swapdb(db1, db2)`: SWAPDB. */
  async swapdb(db1: number, db2: number): Promise<void> {
    await (this.redis as any).call('SWAPDB', String(db1), String(db2));
  }
  /** Local Redis COPY (`copy(src, dest, replace?)`, TS extension). */
  async copy(src: string, dest: string, replace?: boolean): Promise<boolean>;
  /** Java `copy(name, host, port, database, timeout)`: MIGRATE ... COPY. */
  async copy(name: string, host: string, port: number, database: number, timeoutMs: number): Promise<void>;
  async copy(a: string, b: string, c?: boolean | number, d?: number, e?: number): Promise<boolean | void> {
    if (typeof c === 'number') {
      await (this.redis as any).call('MIGRATE', b, String(c), a, String(d), String(e), 'COPY');
      return;
    }
    // NOTE: exact form omits REPLACE entirely — sending '' is a syntax error.
    const args: (string | number)[] = c ? [a, b, 'DB', 0, 'REPLACE'] : [a, b];
    try {
      return toNumber(await (this.redis as any).copy(...args)) === 1;
    } catch {
      return false;
    }
  }
  /** Java `addListener(ObjectListener)`: global keyspace events
   *  (`new`/`set`/`expired`/`del`). `onFlush` requires RESP3 in Java 4.6.1,
   *  which this client does not negotiate, so a flush-only listener throws. */
  async addListener(listener: ObjectListener): Promise<number> {
    const supported = [listener.onNew, listener.onSet, listener.onExpired, listener.onDeleted]
      .filter((fn) => typeof fn === 'function');
    if (supported.length === 0) {
      if (typeof listener.onFlush === 'function') {
        throw new Error('`protocol` config setting should be set to RESP3 value');
      }
      throw new Error('Unsupported ObjectListener');
    }
    return sharedKeysListenerHub(this.redis).add(listener);
  }
  /** Java `removeListener(int)`: idempotent across getKeys() facades. */
  async removeListener(listenerId: number): Promise<void> {
    await sharedKeysListenerHub(this.redis).remove(listenerId);
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
  /** Redisson shebang builder — Java loadAsync concatenates
   *  `"#!lua name=" + libraryName + " \n " + code` verbatim (the \n is a
   *  two-char escape here; the FUNCTION LOAD payload is one string). */
  private shebang(libName: string, code: string): string {
    return '#!lua name=' + libName + ' \n ' + code;
  }
  /** Java `load(libName, code)`: FUNCTION LOAD of the Redisson-shaped
   *  `#!lua name=<lib> \n <code>` body — FAILS while the library name is
   *  already registered (the server rejects a duplicate name). */
  async load(libName: string, code: string): Promise<void>;
  /** Raw form: the caller's `code` carries its own `#!lua name=...` line. */
  async load(code: string, replace?: boolean): Promise<string>;
  async load(a: string, b?: string | boolean): Promise<string | void> {
    if (typeof b === 'string') {
      await this.call('FUNCTION', 'LOAD', this.shebang(a, b));
      return;
    }
    return b === true
      ? (this.call('FUNCTION', 'LOAD', 'REPLACE', a) as Promise<string>)
      : (this.call('FUNCTION', 'LOAD', a) as Promise<string>);
  }
  /** Java `loadAndReplace(libName, code)`: FUNCTION LOAD REPLACE. */
  async loadAndReplace(libName: string, code: string): Promise<void> {
    await this.call('FUNCTION', 'LOAD', 'REPLACE', this.shebang(libName, code));
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
  params?: Record<string, string | number | Buffer>;
  dialect?: number;
  timeoutMs?: number;
  /** FT.PROFILE only: add the LIMITED flag. */
  limited?: boolean;
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
  params?: Record<string, string | number | Buffer>;
  dialect?: number;
  /** Appended verbatim for advanced options. */
  rawArgs?: string[];
  /** WITHCURSOR, optionally `[count]` or `[count, maxIdleMs]`. */
  withCursor?: boolean | [number] | [number, number];
  /** FT.PROFILE only: add the LIMITED flag. */
  limited?: boolean;
}

export interface HybridVectorParams {
  field: string;
  /** Parameter name, e.g. `$q`. */
  param: string;
  mode?: 'KNN' | 'RANGE';
  /** KNN only. */
  k?: number;
  efRuntime?: number;
  yieldDistanceAs?: string;
  shardKRatio?: number;
  /** RANGE only. */
  radius?: number;
  epsilon?: number;
  scoreAlias?: string;
  filter?: string;
}

export interface HybridCombineParams {
  type: 'RRF' | 'LINEAR';
  constant?: number;
  window?: number;
  alpha?: number;
  beta?: number;
  scoreAlias?: string;
}

export interface HybridGroupBy {
  fields: string[];
  /** Raw REDUCE token groups, e.g. [['COUNT', '0', 'AS', 'cnt']]. */
  reduce?: (string | number)[][];
}

export interface HybridParams {
  query: string;
  vectorSimilarity: HybridVectorParams;
  scorer?: string;
  queryScoreAlias?: string;
  combine?: HybridCombineParams;
  limit?: [number, number];
  sortBy?: { field: string; order?: 'ASC' | 'DESC' };
  noSort?: boolean;
  load?: string[];
  groupBy?: HybridGroupBy[];
  apply?: { value: string; as: string }[];
  filter?: string;
  params?: Record<string, string | number | Buffer>;
  timeoutMs?: number;
}

export interface HybridSearchResult {
  total: number;
  results: Record<string, string>[];
  warnings: string[];
  executionTime: number;
}

export interface CursorParams {
  total: number;
  rows: Record<string, any>[];
  cursor?: number;
}

/** Structured FT.CREATE options (TS shape of Java `IndexOptions`). */
export interface SearchIndexOptions {
  on?: 'HASH' | 'JSON';
  prefixes?: string[];
  filter?: string;
  language?: string;
  languageField?: string;
  score?: number;
  scoreField?: string;
  maxTextFields?: boolean;
  noOffsets?: boolean;
  /** TEMPORARY <ms>. */
  temporaryMs?: number;
  noHL?: boolean;
  noFields?: boolean;
  noFreqs?: boolean;
  stopwords?: string[];
  skipInitialScan?: boolean;
  payloadField?: string;
}

export type SearchSortMode = boolean | 'UNF';

interface SearchFieldBase {
  property: string;
  as?: string;
  sortable?: SearchSortMode;
  noIndex?: boolean;
  indexMissing?: boolean;
}

export interface SearchTextField extends SearchFieldBase {
  type: 'TEXT';
  noStem?: boolean;
  phonetic?: string;
  weight?: number;
  withSuffixTrie?: boolean;
  indexEmpty?: boolean;
}

export interface SearchTagField extends SearchFieldBase {
  type: 'TAG';
  separator?: string;
  caseSensitive?: boolean;
  withSuffixTrie?: boolean;
  indexEmpty?: boolean;
}

export interface SearchNumericField extends SearchFieldBase {
  type: 'NUMERIC';
}

export interface SearchGeoField extends SearchFieldBase {
  type: 'GEO';
}

export interface SearchGeoShapeField extends SearchFieldBase {
  type: 'GEOSHAPE';
  coordinateSystem?: 'FLAT' | 'SPHERICAL';
}

export interface SearchVectorField extends SearchFieldBase {
  type: 'VECTOR';
  algorithm?: 'FLAT' | 'HNSW' | 'SVS-VAMANA';
  dataType?: 'FLOAT32' | 'FLOAT64' | 'BFLOAT16' | 'FLOAT16' | 'INT8' | 'UINT8';
  dim: number;
  distanceMetric?: 'L2' | 'IP' | 'COSINE';
  initialCap?: number;
  /** FLAT. */
  blockSize?: number;
  /** HNSW. */
  m?: number;
  efConstruction?: number;
  efRuntime?: number;
  /** HNSW/SVS. */
  epsilon?: number;
  /** SVS-VAMANA. */
  compression?: 'LVQ8' | 'LVQ4' | 'LVQ4x4' | 'LVQ4x8' | 'LeanVec4x8' | 'LeanVec8x8';
  constructionWindowSize?: number;
  graphMaxDegree?: number;
  searchWindowSize?: number;
  trainingThreshold?: number;
  leanVecDim?: number;
}

export type SearchFieldIndex =
  | SearchTextField
  | SearchTagField
  | SearchNumericField
  | SearchGeoField
  | SearchGeoShapeField
  | SearchVectorField;

export class RSearch {
  constructor(private redis: Redis) {}
  private async call(...args: (string | number | Buffer)[]): Promise<any> {
    return (this.redis as any).call(...args);
  }
  private pushSortable(out: string[], mode?: SearchSortMode): void {
    if (!mode) return;
    out.push('SORTABLE');
    if (mode === 'UNF') out.push('UNF');
  }

  private pushCommonFlags(out: string[], f: SearchFieldIndex): void {
    if (f.indexMissing) out.push('INDEXMISSING');
    this.pushSortable(out, f.sortable);
    if (f.noIndex) out.push('NOINDEX');
  }

  /** Convert structured fields to FT.CREATE/FT.ALTER schema tokens. */
  private structuredSchemaTokens(fields: SearchFieldIndex[]): string[] {
    const out: string[] = [];
    for (const f of fields) {
      if (!f.property) throw new Error('SearchFieldIndex.property is required');
      out.push(f.property);
      if (f.as) out.push('AS', f.as);
      switch (f.type) {
        case 'TEXT':
          out.push('TEXT');
          if (f.noStem) out.push('NOSTEM');
          if (f.phonetic) out.push('PHONETIC', f.phonetic);
          if (f.weight !== undefined) out.push('WEIGHT', String(f.weight));
          if (f.withSuffixTrie) out.push('WITHSUFFIXTRIE');
          if (f.indexEmpty) out.push('INDEXEMPTY');
          this.pushCommonFlags(out, f);
          break;
        case 'TAG':
          out.push('TAG');
          if (f.caseSensitive) out.push('CASESENSITIVE');
          if (f.separator !== undefined) out.push('SEPARATOR', f.separator);
          if (f.withSuffixTrie) out.push('WITHSUFFIXTRIE');
          if (f.indexEmpty) out.push('INDEXEMPTY');
          this.pushCommonFlags(out, f);
          break;
        case 'NUMERIC':
          out.push('NUMERIC');
          this.pushCommonFlags(out, f);
          break;
        case 'GEO':
          out.push('GEO');
          this.pushCommonFlags(out, f);
          break;
        case 'GEOSHAPE':
          out.push('GEOSHAPE');
          if (f.coordinateSystem) out.push(f.coordinateSystem);
          if (f.noIndex) out.push('NOINDEX');
          if (f.indexMissing) out.push('INDEXMISSING');
          break;
        case 'VECTOR': {
          const algorithm = f.algorithm ?? 'FLAT';
          const params: (string | number)[] = [
            'TYPE', f.dataType ?? 'FLOAT32',
            'DIM', f.dim,
            'DISTANCE_METRIC', f.distanceMetric ?? 'COSINE',
          ];
          if (algorithm === 'FLAT') {
            if (f.initialCap !== undefined) params.push('INITIAL_CAP', f.initialCap);
            if (f.blockSize !== undefined) params.push('BLOCK_SIZE', f.blockSize);
          } else if (algorithm === 'HNSW') {
            if (f.initialCap !== undefined) params.push('INITIAL_CAP', f.initialCap);
            if (f.m !== undefined) params.push('M', f.m);
            if (f.efConstruction !== undefined) params.push('EF_CONSTRUCTION', f.efConstruction);
            if (f.efRuntime !== undefined) params.push('EF_RUNTIME', f.efRuntime);
            if (f.epsilon !== undefined) params.push('EPSILON', f.epsilon);
          } else {
            if (f.compression) params.push('COMPRESSION', f.compression);
            if (f.constructionWindowSize !== undefined) params.push('CONSTRUCTION_WINDOW_SIZE', f.constructionWindowSize);
            if (f.graphMaxDegree !== undefined) params.push('GRAPH_MAX_DEGREE', f.graphMaxDegree);
            if (f.searchWindowSize !== undefined) params.push('SEARCH_WINDOW_SIZE', f.searchWindowSize);
            if (f.epsilon !== undefined) params.push('EPSILON', f.epsilon);
            if (f.trainingThreshold !== undefined) params.push('TRAINING_THRESHOLD', f.trainingThreshold);
            if (f.leanVecDim !== undefined) params.push('LEANVEC_DIM', f.leanVecDim);
          }
          out.push('VECTOR', algorithm, String((3 + (params.length - 6) / 2) * 2), ...params.map(String));
          break;
        }
      }
    }
    return out;
  }

  private indexOptionsArgs(options: SearchIndexOptions): string[] {
    const args: string[] = ['ON', options.on ?? 'HASH'];
    if (options.prefixes?.length) args.push('PREFIX', String(options.prefixes.length), ...options.prefixes);
    if (options.filter) args.push('FILTER', options.filter);
    if (options.language) args.push('LANGUAGE', options.language);
    if (options.languageField) args.push('LANGUAGE_FIELD', options.languageField);
    if (options.score !== undefined) args.push('SCORE', String(options.score));
    if (options.scoreField) args.push('SCORE_FIELD', options.scoreField);
    if (options.payloadField) args.push('PAYLOAD_FIELD', options.payloadField);
    if (options.maxTextFields) args.push('MAXTEXTFIELDS');
    if (options.temporaryMs !== undefined) args.push('TEMPORARY', String(options.temporaryMs));
    if (options.noOffsets) args.push('NOOFFSETS');
    if (options.noHL) args.push('NOHL');
    if (options.noFields) args.push('NOFIELDS');
    if (options.noFreqs) args.push('NOFREQS');
    if (options.stopwords) args.push('STOPWORDS', String(options.stopwords.length), ...options.stopwords);
    if (options.skipInitialScan) args.push('SKIPINITIALSCAN');
    return args;
  }

  async createIndex(name: string, schema: string[] | SearchFieldIndex[], options: SearchIndexOptions = {}): Promise<void> {
    if (schema.length === 0) throw new Error('At least one field index should be defined');
    const structured = typeof schema[0] !== 'string';
    const tokens = structured
      ? this.structuredSchemaTokens(schema as SearchFieldIndex[])
      : (schema as string[]);
    const args: string[] = ['FT.CREATE', name, ...this.indexOptionsArgs(options), 'SCHEMA', ...tokens];
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
  /** Java 4.7 `getAliases(String)`: aliases pointing at an index (FT.ALIASLIST). */
  async getAliases(indexName: string): Promise<string[]> {
    const r = await this.call('FT.ALIASLIST', indexName);
    return Array.isArray(r) ? r.map(String) : [];
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
  private buildSearchArgs(name: string, query: string, params: SearchParams): (string | number | Buffer)[] {
    const args: (string | number | Buffer)[] = ['FT.SEARCH', name, query];
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
    return args;
  }

  private parseSearchResult(r: any): { total: number; docs: any[] } {
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

  private buildAggregateArgs(name: string, query: string, params: AggregateParams): (string | number | Buffer)[] {
    const args: (string | number | Buffer)[] = ['FT.AGGREGATE', name, query];
    if (params.load?.length) args.push('LOAD', params.load.length, ...params.load);
    if (params.groupBy?.length) {
      args.push('GROUPBY', params.groupBy.length, ...params.groupBy);
      if (params.reduce?.length) args.push('REDUCE', ...params.reduce);
    }
    if (params.apply?.length) args.push('APPLY', ...params.apply);
    if (params.sortBy?.length) args.push('SORTBY', ...params.sortBy);
    if (params.limit) args.push('LIMIT', params.limit[0], params.limit[1]);
    if (params.withCursor) {
      args.push('WITHCURSOR');
      if (Array.isArray(params.withCursor)) {
        args.push('COUNT', String(params.withCursor[0]));
        if (params.withCursor.length > 1) args.push('MAXIDLE', String(params.withCursor[1]));
      }
    }
    if (params.params) {
      const entries = Object.entries(params.params);
      args.push('PARAMS', entries.length * 2, ...entries.flat());
    }
    if (params.dialect !== undefined) args.push('DIALECT', params.dialect);
    if (params.rawArgs?.length) args.push(...params.rawArgs);
    return args;
  }

  private kvRow(row: any): Record<string, any> | null {
    if (row && typeof row === 'object' && !Array.isArray(row)) return row as Record<string, any>;
    if (!Array.isArray(row)) return null;
    const rec: Record<string, any> = {};
    for (let i = 0; i + 1 < row.length; i += 2) rec[String(row[i])] = row[i + 1];
    return rec;
  }

  private parseAggregateResult(r: any): { total: number; rows: Record<string, any>[] } {
    if (r && typeof r === 'object' && !Array.isArray(r)) {
      const total = Number((r as any).total_results ?? 0);
      const rows: Record<string, any>[] = [];
      for (const item of ((r as any).results ?? []) as any[]) {
        const rec = this.kvRow(item);
        if (rec) rows.push(rec);
      }
      return { total, rows };
    }
    const arr = (r ?? []) as any[];
    const total = arr.length > 0 ? Number(arr[0]) : 0;
    const rows: Record<string, any>[] = [];
    for (const row of (arr.slice(1) ?? []) as any[]) {
      const rec = this.kvRow(row);
      if (rec) rows.push(rec);
    }
    return { total, rows };
  }

  private pairMap(raw: any): Record<string, any> {
    const out: Record<string, any> = {};
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) return { ...(raw as Record<string, any>) };
    if (!Array.isArray(raw)) return out;
    for (let i = 0; i + 1 < raw.length; i += 2) out[String(raw[i])] = raw[i + 1];
    return out;
  }

  async search(name: string, query: string, params: SearchParams = {}): Promise<{ total: number; docs: any[] }> {
    return this.parseSearchResult(await this.call(...this.buildSearchArgs(name, query, params)));
  }

  /** Java `profileSearch(index, query, options)`: FT.PROFILE ... SEARCH. */
  async profileSearch(name: string, query: string, params: SearchParams = {}): Promise<{ result: { total: number; docs: any[] }; info: Record<string, any> }> {
    const inner = this.buildSearchArgs(name, query, params).slice(2);
    const args: (string | number | Buffer)[] = ['FT.PROFILE', name, 'SEARCH'];
    if (params.limited) args.push('LIMITED');
    args.push('QUERY', ...inner);
    const raw: any = await this.call(...args);
    const outer = Array.isArray(raw) ? raw : [];
    return { result: this.parseSearchResult(outer[0]), info: this.pairMap(outer[1]) };
  }

  /** Java `aggregate(index, query, options)`: FT.AGGREGATE → rows (plus cursor when requested). */
  async aggregate(name: string, query: string, params: AggregateParams = {}): Promise<{ total: number; rows: Record<string, any>[]; cursor?: number }> {
    const raw: any = await this.call(...this.buildAggregateArgs(name, query, params));
    if (params.withCursor && Array.isArray(raw) && raw.length === 2 && Array.isArray(raw[0]) && !Array.isArray(raw[1])) {
      const parsed = this.parseAggregateResult(raw[0]);
      return { ...parsed, cursor: toNumber(raw[1]) };
    }
    return this.parseAggregateResult(raw);
  }

  /** Java `profileAggregate(index, query, options)`: FT.PROFILE ... AGGREGATE. */
  async profileAggregate(name: string, query: string, params: AggregateParams = {}): Promise<{ result: { total: number; rows: Record<string, any>[] }; info: Record<string, any> }> {
    const inner = this.buildAggregateArgs(name, query, params).slice(2);
    const args: (string | number | Buffer)[] = ['FT.PROFILE', name, 'AGGREGATE'];
    if (params.limited) args.push('LIMITED');
    args.push('QUERY', ...inner);
    const raw: any = await this.call(...args);
    const outer = Array.isArray(raw) ? raw : [];
    return { result: this.parseAggregateResult(outer[0]), info: this.pairMap(outer[1]) };
  }

  /** Java `readCursor(index, cursorId[, count])`: FT.CURSOR READ. */
  async readCursor(index: string, cursorId: number, count?: number): Promise<CursorParams> {
    const args: (string | number)[] = ['FT.CURSOR', 'READ', index, cursorId];
    if (count !== undefined) args.push('COUNT', count);
    const raw: any = await this.call(...args);
    const outer = Array.isArray(raw) ? raw : [];
    const parsed = this.parseAggregateResult(outer[0]);
    return { ...parsed, cursor: outer.length > 1 ? toNumber(outer[1]) : 0 };
  }

  /** Java `delCursor(index, cursorId)`: FT.CURSOR DEL. */
  async delCursor(index: string, cursorId: number): Promise<void> {
    await this.call('FT.CURSOR', 'DEL', index, cursorId);
  }

  /** Java `alter(index, skipInitialScan, fields...)`: FT.ALTER ... SCHEMA ADD,
   *  accepting raw tokens or structured `SearchFieldIndex` objects. */
  async alter(index: string, schema: string[] | SearchFieldIndex[], skipInitialScan = false): Promise<void> {
    if (schema.length === 0) throw new Error('At least one field index should be defined');
    const args: string[] = ['FT.ALTER', index];
    if (skipInitialScan) args.push('SKIPINITIALSCAN');
    args.push('SCHEMA');
    if (typeof schema[0] === 'string') {
      args.push('ADD', ...(schema as string[]));
    } else {
      for (const field of schema as SearchFieldIndex[]) args.push('ADD', ...this.structuredSchemaTokens([field]));
    }
    await this.call(...args);
  }

  /** Java `hybridSearch(index, args)`: FT.HYBRID (Redis 8.2+). */
  async hybridSearch(index: string, params: HybridParams): Promise<HybridSearchResult> {
    const args: (string | number | Buffer)[] = ['FT.HYBRID', index, 'SEARCH', params.query];
    if (params.scorer) args.push('SCORER', params.scorer);
    if (params.queryScoreAlias) args.push('YIELD_SCORE_AS', params.queryScoreAlias);

    const vs = params.vectorSimilarity;
    if (!vs) throw new Error('vectorSimilarity is required for hybrid search');
    args.push('VSIM', vs.field, vs.param);
    if ((vs.mode ?? 'KNN') === 'KNN') {
      if (vs.k === undefined) throw new Error('vectorSimilarity.k is required for KNN');
      const knn: (string | number)[] = ['K', vs.k];
      if (vs.efRuntime !== undefined) knn.push('EF_RUNTIME', vs.efRuntime);
      if (vs.yieldDistanceAs) knn.push('YIELD_DISTANCE_AS', vs.yieldDistanceAs);
      if (vs.shardKRatio !== undefined) knn.push('SHARD_K_RATIO', vs.shardKRatio);
      args.push('KNN', String(knn.length), ...knn);
    } else {
      if (vs.radius === undefined) throw new Error('vectorSimilarity.radius is required for RANGE');
      const range: (string | number)[] = ['RADIUS', vs.radius];
      if (vs.epsilon !== undefined) range.push('EPSILON', vs.epsilon);
      args.push('RANGE', String(range.length), ...range);
    }
    if (vs.scoreAlias) args.push('YIELD_SCORE_AS', vs.scoreAlias);
    if (vs.filter) args.push('FILTER', vs.filter);

    if (params.combine) {
      const c = params.combine;
      const cArgs: (string | number)[] = [];
      if (c.type === 'RRF') {
        if (c.constant !== undefined) cArgs.push('CONSTANT', c.constant);
        if (c.window !== undefined) cArgs.push('WINDOW', c.window);
      } else {
        if (c.alpha !== undefined) cArgs.push('ALPHA', c.alpha);
        if (c.beta !== undefined) cArgs.push('BETA', c.beta);
        if (c.window !== undefined) cArgs.push('WINDOW', c.window);
      }
      if (cArgs.length === 0 && c.scoreAlias !== undefined) {
        // Redis rejects zero-argument RRF/LINEAR (Java 4.6.1 sends RRF 0);
        // materialise the documented defaults so YIELD_SCORE_AS stays legal.
        if (c.type === 'RRF') cArgs.push('CONSTANT', 60);
        else cArgs.push('ALPHA', 0.5, 'BETA', 0.5);
      }
      if (cArgs.length > 0) {
        args.push('COMBINE', c.type, String(cArgs.length), ...cArgs);
        if (c.scoreAlias) args.push('YIELD_SCORE_AS', c.scoreAlias);
      }
    }

    if (params.limit) args.push('LIMIT', String(params.limit[0]), String(params.limit[1]));
    if (params.sortBy) {
      if (params.sortBy.order) args.push('SORTBY', '2', params.sortBy.field, params.sortBy.order);
      else args.push('SORTBY', '1', params.sortBy.field);
    }
    if (params.noSort) args.push('NOSORT');
    if (params.load?.length) args.push('LOAD', String(params.load.length), ...params.load);
    if (params.groupBy?.length) {
      for (const group of params.groupBy) {
        args.push('GROUPBY', String(group.fields.length), ...group.fields);
        for (const reduce of group.reduce ?? []) args.push('REDUCE', ...reduce);
      }
    }
    for (const expr of params.apply ?? []) args.push('APPLY', expr.value, 'AS', expr.as);
    if (params.filter) args.push('FILTER', params.filter);
    if (params.params) {
      const entries = Object.entries(params.params);
      args.push('PARAMS', String(entries.length * 2), ...entries.flat());
    }
    if (params.timeoutMs !== undefined) args.push('TIMEOUT', String(params.timeoutMs));

    const raw: any = await this.call(...args);
    let total = 0;
    let results: Record<string, string>[] = [];
    let warnings: string[] = [];
    let executionTime = 0;
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
      total = Number((raw as any).total_results ?? 0);
      results = ((raw as any).results ?? []).map((row: any) => this.kvRow(row) ?? {});
      warnings = ((raw as any).warnings ?? []).map(String);
      executionTime = Number((raw as any).execution_time ?? 0);
    } else {
      const flat = (raw ?? []) as any[];
      for (let i = 0; i + 1 < flat.length; i += 2) {
        const key = String(flat[i]);
        const value = flat[i + 1];
        if (key === 'total_results') total = toNumber(value);
        else if (key === 'results') results = ((value ?? []) as any[]).map((row: any) => this.kvRow(row) ?? {});
        else if (key === 'warnings') warnings = ((value ?? []) as any[]).map(String);
        else if (key === 'execution_time') executionTime = Number(value);
      }
    }
    return { total, results, warnings, executionTime };
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

export interface VectorAddArgs {
  element: string;
  vector: number[] | Buffer;
  /** REDUCE dimension factor. */
  reduce?: number;
  /** CAS (check-and-set attributes). */
  cas?: boolean;
  quantization?: 'NOQUANT' | 'Q8' | 'BIN';
  explorationFactor?: number;
  attributes?: unknown;
  /** Codec used for attributes (defaults to the instance codec). */
  codec?: Codec;
  maxConnections?: number;
}

export interface VectorSimilarArgs {
  /** Exactly one of `element` or `vector` is required. */
  element?: string;
  vector?: number[] | Buffer;
  count?: number;
  epsilon?: number;
  explorationFactor?: number;
  filter?: string;
  filterEffort?: number;
  /** TRUTH: brute-force linear scan. */
  linearScan?: boolean;
  /** NOTHREAD: run on the main thread. */
  mainThread?: boolean;
}

export interface VectorScoredEntry {
  value: string;
  score: number;
}

export interface VectorScoredAttributesEntry extends VectorScoredEntry {
  attributes: string | null;
}

export class RVectorSet extends RedisDataStructure {
  async add(element: string, vector: number[] | Buffer): Promise<boolean>;
  async add(args: VectorAddArgs): Promise<boolean>;
  async add(a: string | VectorAddArgs, b?: number[] | Buffer): Promise<boolean> {
    const args: VectorAddArgs = typeof a === 'string' ? { element: a, vector: b as number[] | Buffer } : a;
    if (!args.element) throw new Error('VectorAddArgs.element is required');
    const cmd: (string | number | Buffer)[] = ['VADD', this.name];
    if (args.reduce !== undefined) cmd.push('REDUCE', String(args.reduce));
    if (Buffer.isBuffer(args.vector)) cmd.push('FP32', args.vector);
    else cmd.push('VALUES', String(args.vector.length), ...args.vector.map(String));
    cmd.push(args.element);
    if (args.cas) cmd.push('CAS');
    if (args.quantization) cmd.push(args.quantization);
    if (args.explorationFactor !== undefined) cmd.push('EF', String(args.explorationFactor));
    if (args.attributes !== undefined) {
      const codec = args.codec ?? this.codec;
      cmd.push('SETATTR', codec.encode(args.attributes));
    }
    if (args.maxConnections !== undefined) cmd.push('M', String(args.maxConnections));
    const r: any = await (this.redis as any).call(...cmd);
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
  /** Java `getRawVector(element)`: VEMB ... RAW → raw parts (binary kept). */
  async getRawVector(element: string): Promise<any[]> {
    const raw: any = (this.redis as any).callBuffer
      ? await (this.redis as any).callBuffer('VEMB', this.name, element, 'RAW')
      : await (this.redis as any).call('VEMB', this.name, element, 'RAW');
    if (raw === null || raw === undefined) return [];
    return Array.isArray(raw) ? raw : [raw];
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
  /** Java `iterator()`: VRANGE pages of `batchSize`, resuming with `(` + last. */
  async *iterator(batchSize = 10): AsyncGenerator<string> {
    let last: string | null = null;
    for (;;) {
      const start = last === null ? '-' : `(${last}`;
      const buffer = await this.range(start, '+', batchSize);
      if (buffer.length === 0) return;
      last = buffer[buffer.length - 1];
      for (const item of buffer) yield item;
      if (buffer.length < batchSize) return;
    }
  }
  /** Java `stream()`: lazy iterator. */
  async *stream(batchSize = 10): AsyncGenerator<string> {
    yield* this.iterator(batchSize);
  }
  /** Java `getNeighbors(element)`: VLINKS → member names. Redis returns one
   *  array per HNSW layer; Java flattens all layers, and so do we. */
  async getNeighbors(element: string): Promise<string[]> {
    const r: any = await this.redis.call('VLINKS', this.name, element);
    return this.flattenLinks(r) as string[];
  }
  /** Java `getNeighborEntries(element)`: VLINKS ... WITHSCORES. */
  async getNeighborEntries(element: string): Promise<VectorScoredEntry[]> {
    const r: any = await this.redis.call('VLINKS', this.name, element, 'WITHSCORES');
    const flat = this.flattenLinks(r);
    const out: VectorScoredEntry[] = [];
    for (let i = 0; i + 1 < flat.length; i += 2) {
      out.push({ value: String(flat[i]), score: Number(flat[i + 1]) });
    }
    return out;
  }

  private flattenLinks(raw: any): any[] {
    if (!Array.isArray(raw)) return raw === null || raw === undefined ? [] : [raw];
    const out: any[] = [];
    for (const layer of raw) {
      if (Array.isArray(layer)) out.push(...layer);
      else out.push(layer);
    }
    return out;
  }

  private similarArgs(args: VectorSimilarArgs, extras: string[] = []): (string | number | Buffer)[] {
    if (args.element === undefined && args.vector === undefined) {
      throw new Error('VectorSimilarArgs requires element or vector');
    }
    if (args.element !== undefined && args.vector !== undefined) {
      throw new Error('VectorSimilarArgs accepts either element or vector, not both');
    }
    const cmd: (string | number | Buffer)[] = ['VSIM', this.name];
    if (args.element !== undefined) cmd.push('ELE', args.element);
    else if (Buffer.isBuffer(args.vector)) cmd.push('FP32', args.vector);
    else cmd.push('VALUES', String(args.vector!.length), ...args.vector!.map(String));
    if (args.count !== undefined) cmd.push('COUNT', String(args.count));
    if (args.epsilon !== undefined) cmd.push('EPSILON', String(args.epsilon));
    if (args.explorationFactor !== undefined) cmd.push('EF', String(args.explorationFactor));
    if (args.filter) cmd.push('FILTER', args.filter);
    if (args.filterEffort !== undefined) cmd.push('FILTER-EF', String(args.filterEffort));
    if (args.linearScan) cmd.push('TRUTH');
    if (args.mainThread) cmd.push('NOTHREAD');
    cmd.push(...extras);
    return cmd;
  }

  /** Java `getSimilar(args)`: VSIM → member names. */
  async getSimilar(args: VectorSimilarArgs): Promise<string[]> {
    return ((await (this.redis as any).call(...this.similarArgs(args))) as string[]) ?? [];
  }

  /** Java `getSimilarEntries(args)`: VSIM ... WITHSCORES. */
  async getSimilarEntries(args: VectorSimilarArgs): Promise<VectorScoredEntry[]> {
    const r: any = await (this.redis as any).call(...this.similarArgs(args, ['WITHSCORES']));
    const flat = Array.isArray(r) && Array.isArray(r[0]) ? r[0] : (r ?? []);
    const out: VectorScoredEntry[] = [];
    for (let i = 0; i + 1 < flat.length; i += 2) {
      out.push({ value: String(flat[i]), score: Number(flat[i + 1]) });
    }
    return out;
  }

  /** Java `getSimilarEntriesWithAttributes(args)`: VSIM ... WITHSCORES WITHATTRIBS. */
  async getSimilarEntriesWithAttributes(args: VectorSimilarArgs): Promise<VectorScoredAttributesEntry[]> {
    const r: any = await (this.redis as any).call(...this.similarArgs(args, ['WITHSCORES', 'WITHATTRIBS']));
    const flat = Array.isArray(r) && Array.isArray(r[0]) ? r[0] : (r ?? []);
    const out: VectorScoredAttributesEntry[] = [];
    for (let i = 0; i + 2 < flat.length; i += 3) {
      out.push({
        value: String(flat[i]),
        score: Number(flat[i + 1]),
        attributes: flat[i + 2] === null || flat[i + 2] === undefined ? null : String(flat[i + 2]),
      });
    }
    return out;
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
  /** Java `setAttributes(element, value[, codec])`: VSETATTR with codec JSON. */
  async setAttributes(element: string, value: unknown, codec: Codec = this.codec): Promise<boolean> {
    return toNumber(await this.redis.call('VSETATTR', this.name, element, codec.encode(value))) === 1;
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

// -- Array / CircularBuffer moved to src/arrays.ts (Redis 8.8+ AR.* family) ------------

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
  /** Java 4.8 `RedissonBatch.getCountMin(...)`: batched CMS.* facade. */
  getCountMin<V>(name: string, codec?: Codec): BatchCountMin<V> {
    return new BatchCountMin<V>(this, name, codec ?? new JsonCodec());
  }
  /** Java 4.8 `RedissonBatch.getTimeSeriesNative(...)`: batched TS.* facade. */
  getTimeSeriesNative(name: string): BatchTimeSeriesNative {
    return new BatchTimeSeriesNative(this, name);
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
