/**
 * Java 4.8 `RArray` / `RCircularBuffer`: the Redis 8.8+ ARRAY type (AR.*
 * command family — wire names are `ARSET`/`ARGET`/... without dots, exactly
 * as the Java RedisCommands constants spell them).
 *
 * Wire notes (vs C:/GitHub/redisson 4.8.1-SNAPSHOT):
 *  - Values (ARSET/ARGET/ARMGET/ARGETRANGE/ARSCAN/ARGREP WITHVALUES/ARLASTITEMS
 *    and the ARRING payloads inside the CircularBuffer Lua) are codec-encoded
 *    with the instance codec, exactly like Java `RedissonObject.encode(value)`.
 *    The `StringCodec.INSTANCE` Java passes for the command only covers the
 *    name/index arguments, which travel as plain strings/numbers here.
 *  - Indexes/bounds/counts are plain numbers (Java String.valueOf).
 *  - No Lua in RArray; RCircularBuffer Lua is ported verbatim below
 *    (byte-identical modulo JS escaping) and runs through `luaEval`.
 *  - Replies: Long→number via toNumber, ObjectListReplayDecoder→array,
 *    ArrayEntryDecoder/ArrayInfoDecoder logic ported 1:1 (both the flat RESP2
 *    form and the nested RESP3-ish form are accepted, like Java).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { luaEval, prefixName, toNumber } from './util.js';

async function arCall(redis: Redis, ...args: (string | number)[]): Promise<any> {
  return (redis as any).call(...args.map(String));
}

function checkIndex(index: number): void {
  if (index === null || index === undefined || index < 0) {
    throw new Error('redi.ts: Index must be non-negative');
  }
}

function checkCapacity(capacity: number): void {
  if (capacity <= 0) throw new Error('redi.ts: Capacity must be positive');
}

// ---- args / result shapes (Java org.redisson.api.array) -----------------------

/** Java `ArrayEntry<V>`: array index + stored value. */
export interface ArrayEntry<V = any> {
  index: number;
  value: V;
}

/** Java `ArrayInfo` (ARINFO base fields). */
export interface ArrayInfo {
  count: number;
  length: number;
  nextInsertIndex: number;
  slices: number;
  directorySize: number;
  superDirectoryEntries: number;
  sliceSize: number;
}

/** Java `ArrayFullInfo` (ARINFO FULL — extends ArrayInfo). */
export interface ArrayFullInfo extends ArrayInfo {
  denseSlices?: number;
  sparseSlices?: number;
  averageDenseSize?: number;
  averageDenseFill?: number;
  averageSparseSize?: number;
}

/** Java `ArrayGrepParams.Predicate.Type` (wire token == enum name). */
export type ArrayGrepPredicateType = 'EXACT' | 'MATCH' | 'GLOB' | 'RE';

/** Java `ArrayGrepArgs` / `ArrayGrepParams` builder. EXACT/MATCH values are
 *  codec-encoded on the wire; GLOB/RE patterns travel raw.
 *  Caveat (Java parity): ARGREP compares the STORED BYTES, so with the
 *  default JSON codec the encoded quotes become part of the needle and
 *  substring/glob matching misses — Java's own tests use StringCodec for
 *  grep; do the same for MATCH/GLOB/RE semantics. */
export class ArrayGrepArgs {
  private predicates: ArrayGrepPredicateType[] = [];
  private predicateValues: any[] = [];
  private andValue?: boolean;
  private limitValue?: number;
  private noCaseValue = false;

  static exact(value: any): ArrayGrepArgs {
    return new ArrayGrepArgs().withExact(value);
  }
  static match(value: any): ArrayGrepArgs {
    return new ArrayGrepArgs().withMatch(value);
  }
  static glob(pattern: string): ArrayGrepArgs {
    return new ArrayGrepArgs().withGlob(pattern);
  }
  static regex(pattern: string): ArrayGrepArgs {
    return new ArrayGrepArgs().withRegex(pattern);
  }

  withExact(value: any): this {
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    this.predicates.push('EXACT');
    this.predicateValues.push(value);
    return this;
  }
  withMatch(value: any): this {
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    this.predicates.push('MATCH');
    this.predicateValues.push(value);
    return this;
  }
  withGlob(pattern: string): this {
    if (pattern === null || pattern === undefined) throw new Error("redi.ts: Pattern can't be null");
    this.predicates.push('GLOB');
    this.predicateValues.push(pattern);
    return this;
  }
  withRegex(pattern: string): this {
    if (pattern === null || pattern === undefined) throw new Error("redi.ts: Pattern can't be null");
    this.predicates.push('RE');
    this.predicateValues.push(pattern);
    return this;
  }
  and(): this {
    this.andValue = true;
    return this;
  }
  or(): this {
    this.andValue = false;
    return this;
  }
  limit(value: number): this {
    if (value <= 0) throw new Error('redi.ts: Limit must be positive');
    this.limitValue = value;
    return this;
  }
  noCase(): this {
    this.noCaseValue = true;
    return this;
  }

  /** @internal Java createGrepArgs tail (predicates, AND/OR, LIMIT,
   *  WITHVALUES, NOCASE) — name/bounds are prepended by the caller. */
  appendArgs(cmdArgs: (string | number)[], encode: (v: any) => string, withValues: boolean): void {
    if (this.predicates.length === 0) {
      throw new Error('redi.ts: At least one grep predicate is required');
    }
    for (let i = 0; i < this.predicates.length; i++) {
      const type = this.predicates[i];
      cmdArgs.push(type);
      if (type === 'EXACT' || type === 'MATCH') cmdArgs.push(encode(this.predicateValues[i]));
      else cmdArgs.push(this.predicateValues[i] as string);
    }
    if (this.andValue !== undefined) cmdArgs.push(this.andValue ? 'AND' : 'OR');
    if (this.limitValue !== undefined) cmdArgs.push('LIMIT', this.limitValue);
    if (withValues) cmdArgs.push('WITHVALUES');
    if (this.noCaseValue) cmdArgs.push('NOCASE');
  }
}

/** @internal Java RedissonArray.createGrepArgs — full ARGREP argv. */
export function buildGrepArgs(
  name: string,
  startIndex: string | number,
  endIndex: string | number,
  args: ArrayGrepArgs,
  encode: (v: any) => string,
  withValues: boolean,
): (string | number)[] {
  const out: (string | number)[] = [name, startIndex, endIndex];
  args.appendArgs(out, encode, withValues);
  return out;
}

/** @internal Java ArrayEntryDecoder — flat [idx, val, ...] or nested pairs. */
export function decodeArrayEntries(raw: any, decodeValue: (v: any) => any): ArrayEntry<any>[] {
  const parts: any[] = Array.isArray(raw) ? raw : [];
  const out: ArrayEntry<any>[] = [];
  if (parts.length > 0 && Array.isArray(parts[0])) {
    for (const part of parts) out.push({ index: toNumber(part[0]), value: decodeValue(part[1]) });
    return out;
  }
  for (let i = 0; i + 1 < parts.length; i += 2) {
    out.push({ index: toNumber(parts[i]), value: decodeValue(parts[i + 1]) });
  }
  return out;
}

/** @internal Java AbstractArrayInfoDecoder.toMap (flat or nested pairs). */
function arToMap(raw: any): Map<string, any> {
  const parts: any[] = Array.isArray(raw) ? raw : [];
  const map = new Map<string, any>();
  if (parts.length > 0 && Array.isArray(parts[0])) {
    for (const pair of parts) {
      if (pair[1] !== null && pair[1] !== undefined) map.set(String(pair[0]), pair[1]);
    }
    return map;
  }
  for (let i = 0; i + 1 < parts.length; i += 2) {
    if (parts[i + 1] !== null && parts[i + 1] !== undefined) map.set(String(parts[i]), parts[i + 1]);
  }
  return map;
}

function arLong(map: Map<string, any>, key: string): number {
  const v = map.get(key);
  return v === undefined ? 0 : toNumber(v);
}

/** @internal Java ArrayInfoDecoder. */
export function decodeArrayInfo(raw: any): ArrayInfo {
  const map = arToMap(raw);
  return {
    count: arLong(map, 'count'),
    length: arLong(map, 'len'),
    nextInsertIndex: arLong(map, 'next-insert-index'),
    slices: arLong(map, 'slices'),
    directorySize: arLong(map, 'directory-size'),
    superDirectoryEntries: arLong(map, 'super-dir-entries'),
    sliceSize: arLong(map, 'slice-size'),
  };
}

/** @internal Java ArrayFullInfoDecoder. */
export function decodeArrayFullInfo(raw: any): ArrayFullInfo {
  const map = arToMap(raw);
  const info: ArrayFullInfo = decodeArrayInfo(raw);
  const denseSlices = map.get('dense-slices');
  if (denseSlices !== undefined) info.denseSlices = toNumber(denseSlices);
  const sparseSlices = map.get('sparse-slices');
  if (sparseSlices !== undefined) info.sparseSlices = toNumber(sparseSlices);
  const averageDenseSize = map.get('avg-dense-size');
  if (averageDenseSize !== undefined) info.averageDenseSize = Number(averageDenseSize);
  const averageDenseFill = map.get('avg-dense-fill');
  if (averageDenseFill !== undefined) info.averageDenseFill = Number(averageDenseFill);
  const averageSparseSize = map.get('avg-sparse-size');
  if (averageSparseSize !== undefined) info.averageSparseSize = Number(averageSparseSize);
  return info;
}

// ---- RArray -------------------------------------------------------------------

/** Java 4.8 `RArray<V>`: sparse non-negative-index array over the Redis 8.8+
 *  ARRAY type (ARSET/ARGET/ARMGET/ARMSET/ARDEL/ARDELRANGE/ARLEN/ARCOUNT/
 *  ARGETRANGE/ARSCAN/ARGREP/AROP/ARINSERT/ARRING/ARNEXT/ARSEEK/ARLASTITEMS/
 *  ARINFO commands). */
export class RArray<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }

  private entries(raw: any): ArrayEntry<V>[] {
    return decodeArrayEntries(raw, (x) => this.decodeValue(x));
  }
  private values(raw: any): (V | null)[] {
    return ((Array.isArray(raw) ? raw : []) as any[]).map((x) => this.decodeValue(x));
  }
  private async longOperation(startIndex: number, endIndex: number, operation: string): Promise<number | null> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    const r = await arCall(this.redis, 'AROP', this.name, startIndex, endIndex, operation);
    return r === null || r === undefined || r === false ? null : toNumber(r);
  }
  private async doubleOperation(startIndex: number, endIndex: number, operation: string): Promise<number | null> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    const r = await arCall(this.redis, 'AROP', this.name, startIndex, endIndex, operation);
    return r === null || r === undefined || r === false ? null : Number(r);
  }

  /** Java `get(long)`: ARGET. */
  async get(index: number): Promise<V | null>;
  /** Java `get(long...)`: ARMGET. */
  async get(...indexes: number[]): Promise<(V | null)[]>;
  async get(...a: number[]): Promise<any> {
    if (a.length === 1) {
      checkIndex(a[0]);
      return this.decodeValue(await arCall(this.redis, 'ARGET', this.name, a[0]));
    }
    if (a.length === 0) return [];
    const args: (string | number)[] = [this.name];
    for (const index of a) {
      checkIndex(index);
      args.push(index);
    }
    return this.values(await arCall(this.redis, 'ARMGET', ...args));
  }

  /** Java `isSet(long)`: AROP idx idx USED > 0. */
  async isSet(index: number): Promise<boolean> {
    const c = await this.count(index, index);
    return c !== null && c > 0;
  }

  /** Java `iterator()` / `iterator(int)`: paged ARSCAN LIMIT walk, ascending
   *  index order (async generator; Java AsyncIterator shape). */
  async *iterator(count = 10): AsyncGenerator<ArrayEntry<V>> {
    if (count <= 0) throw new Error('redi.ts: Count must be positive');
    const endBound = (await this.length()) - 1;
    let nextStart = 0;
    while (nextStart <= endBound) {
      const page = await this.scan(nextStart, endBound, count);
      if (page.length === 0) return;
      for (const entry of page) yield entry;
      nextStart = page[page.length - 1].index + 1;
    }
  }

  /** Java `stream()` / `stream(int)` — same walk as iterator(). */
  stream(count = 10): AsyncGenerator<ArrayEntry<V>> {
    return this.iterator(count);
  }

  /** Java `set(long, V)`: ARSET — returns number of values set. */
  async set(index: number, value: V): Promise<number>;
  /** Java `set(long, V...)`: ARSET — returns number of values set. */
  async set(index: number, ...values: V[]): Promise<number>;
  /** Java `set(Map<Long, V>)`: ARMSET — returns number of values set. */
  async set(entries: Map<number, V>): Promise<number>;
  async set(a: number | Map<number, V>, ...rest: V[]): Promise<number> {
    if (a instanceof Map) {
      if (a.size === 0) return 0;
      const args: (string | number)[] = [this.name];
      for (const [index, value] of a.entries()) {
        checkIndex(index);
        if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
        args.push(index, this.encodeValue(value));
      }
      return toNumber(await arCall(this.redis, 'ARMSET', ...args));
    }
    checkIndex(a);
    if (rest.length === 0) return 0;
    for (const value of rest) {
      if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    }
    return toNumber(await arCall(this.redis, 'ARSET', this.name, a, ...rest.map((v) => this.encodeValue(v))));
  }

  /** Java `delete(long...)`: ARDEL — returns number of deleted values.
   *  With no indexes, the RExpirable key-delete (DEL) applies. */
  override async delete(...indexes: number[]): Promise<any> {
    if (indexes.length === 0) return super.delete();
    const args: (string | number)[] = [this.name];
    for (const index of indexes) {
      checkIndex(index);
      args.push(index);
    }
    return toNumber(await arCall(this.redis, 'ARDEL', ...args));
  }

  /** Java `deleteRange(long, long)`: ARDELRANGE. */
  async deleteRange(startIndex: number, endIndex: number): Promise<number> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    return toNumber(await arCall(this.redis, 'ARDELRANGE', this.name, startIndex, endIndex));
  }

  /** Java `deleteRanges(long...)`: ARDELRANGE with start/end pairs. */
  async deleteRanges(...startEndIndexes: number[]): Promise<number> {
    if (startEndIndexes.length === 0) return 0;
    if (startEndIndexes.length % 2 !== 0) {
      throw new Error('redi.ts: Ranges should contain start and end index pairs');
    }
    const args: (string | number)[] = [this.name];
    for (const index of startEndIndexes) {
      checkIndex(index);
      args.push(index);
    }
    return toNumber(await arCall(this.redis, 'ARDELRANGE', ...args));
  }

  /** Java `count()`: ARCOUNT. */
  async count(): Promise<number>;
  /** Java `count(long, long)`: AROP start end USED. */
  async count(startIndex: number, endIndex: number): Promise<number | null>;
  async count(a?: number, b?: number): Promise<any> {
    if (a === undefined) return toNumber(await arCall(this.redis, 'ARCOUNT', this.name));
    return this.longOperation(a, b!, 'USED');
  }

  /** Java `countMatches(long, long, V)`: AROP start end MATCH encoded-value. */
  async countMatches(startIndex: number, endIndex: number, value: V): Promise<number | null> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    const r = await arCall(this.redis, 'AROP', this.name, startIndex, endIndex, 'MATCH', this.encodeValue(value));
    return r === null || r === undefined || r === false ? null : toNumber(r);
  }

  /** Java `length()`: ARLEN. */
  async length(): Promise<number> {
    return toNumber(await arCall(this.redis, 'ARLEN', this.name));
  }

  /** Back-compat alias kept from the previous minimal stub. */
  len(): Promise<number> {
    return this.length();
  }

  /** Java `range(long, long)`: ARGETRANGE. */
  async range(startIndex: number, endIndex: number): Promise<(V | null)[]> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    return this.values(await arCall(this.redis, 'ARGETRANGE', this.name, startIndex, endIndex));
  }

  /** Java `scan(long, long)`: ARSCAN — entries in the index range. */
  async scan(startIndex: number, endIndex: number): Promise<ArrayEntry<V>[]>;
  /** Java `scan(long, long, long)`: ARSCAN ... LIMIT n. */
  async scan(startIndex: number, endIndex: number, limit: number): Promise<ArrayEntry<V>[]>;
  async scan(startIndex: number, endIndex: number, limit?: number): Promise<ArrayEntry<V>[]> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    if (limit === undefined) {
      return this.entries(await arCall(this.redis, 'ARSCAN', this.name, startIndex, endIndex));
    }
    if (limit <= 0) throw new Error('redi.ts: Limit must be positive');
    return this.entries(await arCall(this.redis, 'ARSCAN', this.name, startIndex, endIndex, 'LIMIT', limit));
  }

  /** Java `insert(V...)`: ARINSERT — index where the last value was inserted. */
  async insert(...values: V[]): Promise<number> {
    for (const value of values) {
      if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    }
    if (values.length === 0) throw new Error("redi.ts: Values can't be empty");
    return toNumber(await arCall(this.redis, 'ARINSERT', this.name, ...values.map((v) => this.encodeValue(v))));
  }

  /** Java `ring(long, V...)`: ARRING size values... */
  async ring(size: number, ...values: V[]): Promise<number> {
    if (size <= 0) throw new Error('redi.ts: Size must be positive');
    for (const value of values) {
      if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    }
    if (values.length === 0) throw new Error("redi.ts: Values can't be empty");
    return toNumber(await arCall(this.redis, 'ARRING', this.name, size, ...values.map((v) => this.encodeValue(v))));
  }

  /** Java `next()`: ARNEXT — next insert index, null when exhausted. */
  async next(): Promise<number | null> {
    const r = await arCall(this.redis, 'ARNEXT', this.name);
    return r === null || r === undefined ? null : toNumber(r);
  }

  /** Java `seek(long)`: ARSEEK. */
  async seek(index: number): Promise<boolean> {
    checkIndex(index);
    return toNumber(await arCall(this.redis, 'ARSEEK', this.name, index)) === 1;
  }

  /** Java `lastItems(long)`: ARLASTITEMS. */
  async lastItems(count: number): Promise<(V | null)[]> {
    return this.values(await arCall(this.redis, 'ARLASTITEMS', this.name, count));
  }

  /** Java `lastItemsReversed(long)`: ARLASTITEMS ... REV. */
  async lastItemsReversed(count: number): Promise<(V | null)[]> {
    return this.values(await arCall(this.redis, 'ARLASTITEMS', this.name, count, 'REV'));
  }

  /** Java `getInfo()`: ARINFO. */
  async getInfo(): Promise<ArrayInfo> {
    return decodeArrayInfo(await arCall(this.redis, 'ARINFO', this.name));
  }

  /** Java `getFullInfo()`: ARINFO FULL. */
  async getFullInfo(): Promise<ArrayFullInfo> {
    return decodeArrayFullInfo(await arCall(this.redis, 'ARINFO', this.name, 'FULL'));
  }

  /** Java `grep(ArrayGrepArgs)` — full range ('-' / '+'). */
  async grep(args: ArrayGrepArgs): Promise<number[]>;
  /** Java `grep(long, long, ArrayGrepArgs)`. */
  async grep(startIndex: number, endIndex: number, args: ArrayGrepArgs): Promise<number[]>;
  async grep(a: number | ArrayGrepArgs, b?: number, args?: ArrayGrepArgs): Promise<number[]> {
    let argv: (string | number)[];
    if (typeof a === 'number') {
      checkIndex(a);
      checkIndex(b!);
      if (args === null || args === undefined) throw new Error("redi.ts: Args can't be null");
      argv = buildGrepArgs(this.name, a, b!, args, (v) => this.encodeValue(v), false);
    } else {
      if (a === null || a === undefined) throw new Error("redi.ts: Args can't be null");
      argv = buildGrepArgs(this.name, '-', '+', a, (v) => this.encodeValue(v), false);
    }
    const r = await arCall(this.redis, 'ARGREP', ...argv);
    return ((Array.isArray(r) ? r : []) as any[]).map((x) => toNumber(x));
  }

  /** Java `grepEntries(ArrayGrepArgs)` — full range, WITHVALUES. */
  async grepEntries(args: ArrayGrepArgs): Promise<ArrayEntry<V>[]>;
  /** Java `grepEntries(long, long, ArrayGrepArgs)`. */
  async grepEntries(startIndex: number, endIndex: number, args: ArrayGrepArgs): Promise<ArrayEntry<V>[]>;
  async grepEntries(a: number | ArrayGrepArgs, b?: number, args?: ArrayGrepArgs): Promise<ArrayEntry<V>[]> {
    if (typeof a === 'number') {
      checkIndex(a);
      checkIndex(b!);
      return this.entries(await arCall(this.redis, 'ARGREP',
        ...buildGrepArgs(this.name, a, b!, args!, (v) => this.encodeValue(v), true)));
    }
    return this.entries(await arCall(this.redis, 'ARGREP',
      ...buildGrepArgs(this.name, '-', '+', a, (v) => this.encodeValue(v), true)));
  }

  /** Java `sum(long, long)`: AROP start end SUM. */
  async sum(startIndex: number, endIndex: number): Promise<number | null> {
    return this.doubleOperation(startIndex, endIndex, 'SUM');
  }
  /** Java `min(long, long)`: AROP start end MIN. */
  async min(startIndex: number, endIndex: number): Promise<number | null> {
    return this.doubleOperation(startIndex, endIndex, 'MIN');
  }
  /** Java `max(long, long)`: AROP start end MAX. */
  async max(startIndex: number, endIndex: number): Promise<number | null> {
    return this.doubleOperation(startIndex, endIndex, 'MAX');
  }
  /** Java `bitAnd(long, long)`: AROP start end AND. */
  async bitAnd(startIndex: number, endIndex: number): Promise<number | null> {
    return this.longOperation(startIndex, endIndex, 'AND');
  }
  /** Java `bitOr(long, long)`: AROP start end OR. */
  async bitOr(startIndex: number, endIndex: number): Promise<number | null> {
    return this.longOperation(startIndex, endIndex, 'OR');
  }
  /** Java `bitXor(long, long)`: AROP start end XOR. */
  async bitXor(startIndex: number, endIndex: number): Promise<number | null> {
    return this.longOperation(startIndex, endIndex, 'XOR');
  }
}

// ---- RCircularBuffer ----------------------------------------------------------

// Lua ports — VERBATIM from RedissonCircularBuffer.java (4.8.1-SNAPSHOT),
// Java string concatenation joined as-is.

const ACB_REMAINING_CAPACITY_LUA =
  "local limit = tonumber(redis.call('get', KEYS[2]) or '0'); " +
  "local size = redis.call('ARLEN', KEYS[1]); " +
  "local rem = limit - size; " +
  "if rem < 0 then rem = 0; end; " +
  "return rem; ";

const ACB_ADD_LUA =
  "local limit = redis.call('get', KEYS[2]); " +
  "assert(limit ~= false, 'CircularBuffer capacity is not defined'); " +
  "redis.call('ARRING', KEYS[1], limit, ARGV[1]); " +
  "return 1; ";

const ACB_ADD_ALL_LUA =
  "local limit = redis.call('get', KEYS[2]); " +
  "assert(limit ~= false, 'CircularBuffer capacity is not defined'); " +
  "for i = 1, #ARGV, 4000 do " +
  "redis.call('ARRING', KEYS[1], limit, unpack(ARGV, i, math.min(i + 3999, #ARGV))); " +
  "end; " +
  "return 1; ";

const ACB_SET_LUA =
  "redis.call('set', KEYS[2], ARGV[1]); " +
  "local last; " +
  "for i = 2, #ARGV, 4000 do " +
  "last = redis.call('ARRING', KEYS[1], ARGV[1], unpack(ARGV, i, math.min(i + 3999, #ARGV))); " +
  "end; " +
  "return last; ";

const ACB_IS_FULL_LUA =
  "local cap = tonumber(redis.call('get', KEYS[2]) or '0'); " +
  "if cap == 0 then " +
  "return 0; " +
  "end; " +
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "return len >= cap and 1 or 0; ";

const ACB_PEEK_FIRST_LUA =
  "local idx = redis.call('ARNEXT', KEYS[1]); " +
  "if not idx then idx = 0; end; " +
  "return redis.call('ARGET', KEYS[1], idx); ";

const ACB_COUNT_LUA =
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "if len == 0 then " +
  "return 0; " +
  "end; " +
  "return redis.call('AROP', KEYS[1], 0, len - 1, 'MATCH', ARGV[1]); ";

const ACB_CONTAINS_LUA =
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "if len == 0 then " +
  "return 0; " +
  "end; " +
  "return redis.call('AROP', KEYS[1], 0, len - 1, 'MATCH', ARGV[1]) > 0 and 1 or 0; ";

const ACB_AVERAGE_LUA =
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "if len == 0 then " +
  "return false; " +
  "end; " +
  "local s = redis.call('AROP', KEYS[1], 0, len - 1, 'SUM'); " +
  "if not s then " +
  "return false; " +
  "end; " +
  "return tostring(tonumber(s) / len); ";

const ACB_AGGREGATE_LUA =
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "if len == 0 then " +
  "return false; " +
  "end; " +
  "local r = redis.call('AROP', KEYS[1], 0, len - 1, ARGV[1]); " +
  "if not r then " +
  "return false; " +
  "end; " +
  "return tostring(r); ";

const ACB_LONG_AGGREGATE_LUA =
  "local len = redis.call('exists', KEYS[1]) == 1 and redis.call('ARLEN', KEYS[1]) or 0; " +
  "if len == 0 then " +
  "return nil; " +
  "end; " +
  "return redis.call('AROP', KEYS[1], 0, len - 1, ARGV[1]); ";

/** Java 4.8 `RCircularBuffer<V>`: fixed-capacity ring over the ARRAY type.
 *  Capacity lives in the hash-tagged `redisson_acb:{name}` settings key
 *  (SETNX/SET plain long, LongCodec on the Java side). */
export class RCircularBuffer<V = any> extends RedisDataStructure {
  /** Java `settingsName` — prefixName("redisson_acb", rawName). */
  readonly settingsName: string = prefixName('redisson_acb', this.name);

  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }

  private values(raw: any): (V | null)[] {
    return ((Array.isArray(raw) ? raw : []) as any[]).map((x) => this.decodeValue(x));
  }
  private async evalDouble(script: string, numKeys: number, ...keysAndArgs: (string | number)[]): Promise<number | null> {
    const r = await luaEval(this.redis, script, numKeys, ...keysAndArgs);
    return r === null || r === undefined || r === false ? null : Number(r);
  }

  /** Java `trySetCapacity(int)`: SETNX on the settings key. */
  async trySetCapacity(capacity: number): Promise<boolean> {
    checkCapacity(capacity);
    return toNumber(await this.redis.setnx(this.settingsName, String(capacity))) === 1;
  }

  /** Java `setCapacity(int)`: SET on the settings key. */
  async setCapacity(capacity: number): Promise<void> {
    checkCapacity(capacity);
    await this.redis.set(this.settingsName, String(capacity));
  }

  /** Java `capacity()`: GET settings (0 when unset). */
  async capacity(): Promise<number> {
    const v = await this.redis.get(this.settingsName);
    return v === null || v === undefined ? 0 : Number(v);
  }

  /** Java `remainingCapacity()`. */
  async remainingCapacity(): Promise<number> {
    return toNumber(await luaEval(this.redis, ACB_REMAINING_CAPACITY_LUA, 2, this.name, this.settingsName));
  }

  /** Java `add(V)`: ARRING via Lua (requires capacity to be defined). */
  async add(value: V): Promise<boolean> {
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    await luaEval(this.redis, ACB_ADD_LUA, 2, this.name, this.settingsName, this.encodeValue(value));
    return true;
  }

  /** Java `addAll(Collection)`: chunked ARRING (4000 values per call). */
  async addAll(values: V[]): Promise<boolean> {
    if (values === null || values === undefined) throw new Error("redi.ts: Values can't be null");
    if (values.length === 0) return false;
    const args = values.map((v) => {
      if (v === null || v === undefined) throw new Error("redi.ts: Value can't be null");
      return this.encodeValue(v);
    });
    await luaEval(this.redis, ACB_ADD_ALL_LUA, 2, this.name, this.settingsName, ...args);
    return true;
  }

  /** Java `set(int, V...)`: (re)configure capacity and ring the values. */
  async set(size: number, ...values: V[]): Promise<number> {
    checkCapacity(size);
    for (const value of values) {
      if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    }
    if (values.length === 0) throw new Error("redi.ts: Values can't be empty");
    return toNumber(await luaEval(this.redis, ACB_SET_LUA, 2, this.name, this.settingsName,
      String(size), ...values.map((v) => this.encodeValue(v))));
  }

  /** Java `get(long)`: ARGET. */
  async get(index: number): Promise<V | null>;
  /** Java `get(long...)`: ARMGET. */
  async get(...indexes: number[]): Promise<(V | null)[]>;
  async get(...a: number[]): Promise<any> {
    if (a.length === 1) {
      checkIndex(a[0]);
      return this.decodeValue(await arCall(this.redis, 'ARGET', this.name, a[0]));
    }
    if (a.length === 0) return [];
    const args: (string | number)[] = [this.name];
    for (const index of a) {
      checkIndex(index);
      args.push(index);
    }
    return this.values(await arCall(this.redis, 'ARMGET', ...args));
  }

  /** Java `lastItems(int, boolean)`: ARLASTITEMS [REV]. */
  async lastItems(count: number, reverse = false): Promise<(V | null)[]> {
    if (count <= 0) return [];
    if (reverse) return this.values(await arCall(this.redis, 'ARLASTITEMS', this.name, count, 'REV'));
    return this.values(await arCall(this.redis, 'ARLASTITEMS', this.name, count));
  }

  /** Java `range(long, long)`: ARGETRANGE. */
  async range(startIndex: number, endIndex: number): Promise<(V | null)[]> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    return this.values(await arCall(this.redis, 'ARGETRANGE', this.name, startIndex, endIndex));
  }

  /** Java `readAll()`: ARLASTITEMS Integer.MAX_VALUE (oldest-first). */
  async readAll(): Promise<(V | null)[]> {
    return this.values(await arCall(this.redis, 'ARLASTITEMS', this.name, 2147483647));
  }

  /** Java `size()`: ARLEN (0 when absent). */
  async size(): Promise<number> {
    return toNumber(await arCall(this.redis, 'ARLEN', this.name), 0);
  }

  /** Java `sum()`: AROP over the whole window (null when empty). */
  async sum(): Promise<number | null>;
  async sum(startIndex: number, endIndex: number): Promise<number | null>;
  async sum(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.evalDouble(ACB_AGGREGATE_LUA, 1, this.name, 'SUM');
    return this.doubleOperation(a, b!, 'SUM');
  }
  /** Java `min()`. */
  async min(): Promise<number | null>;
  async min(startIndex: number, endIndex: number): Promise<number | null>;
  async min(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.evalDouble(ACB_AGGREGATE_LUA, 1, this.name, 'MIN');
    return this.doubleOperation(a, b!, 'MIN');
  }
  /** Java `max()`. */
  async max(): Promise<number | null>;
  async max(startIndex: number, endIndex: number): Promise<number | null>;
  async max(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.evalDouble(ACB_AGGREGATE_LUA, 1, this.name, 'MAX');
    return this.doubleOperation(a, b!, 'MAX');
  }
  private async doubleOperation(startIndex: number, endIndex: number, operation: string): Promise<number | null> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    const r = await arCall(this.redis, 'AROP', this.name, startIndex, endIndex, operation);
    return r === null || r === undefined || r === false ? null : Number(r);
  }
  private async longOperation(startIndex: number, endIndex: number, operation: string): Promise<number | null> {
    checkIndex(startIndex);
    checkIndex(endIndex);
    const r = await arCall(this.redis, 'AROP', this.name, startIndex, endIndex, operation);
    return r === null || r === undefined || r === false ? null : toNumber(r);
  }
  private async longAggregate(operation: string): Promise<number | null> {
    const r = await luaEval(this.redis, ACB_LONG_AGGREGATE_LUA, 1, this.name, operation);
    return r === null || r === undefined ? null : toNumber(r);
  }

  /** Java `clear()`: DEL the array key, keep the configured capacity. */
  async clear(): Promise<void> {
    await this.redis.del(this.name);
  }

  /** Java `isEmpty()`: ARLEN == 0. */
  async isEmpty(): Promise<boolean> {
    return toNumber(await arCall(this.redis, 'ARLEN', this.name), 0) === 0;
  }

  /** Java `isFull()`. */
  async isFull(): Promise<boolean> {
    return toNumber(await luaEval(this.redis, ACB_IS_FULL_LUA, 2, this.name, this.settingsName)) === 1;
  }

  /** Java `peekLast()`: ARLASTITEMS 1 REV, first element. */
  async peekLast(): Promise<V | null> {
    const r = await arCall(this.redis, 'ARLASTITEMS', this.name, 1, 'REV');
    const list = Array.isArray(r) ? r : [];
    return list.length === 0 ? null : this.decodeValue(list[0]);
  }

  /** Java `peekFirst()`: oldest retained value via ARNEXT/ARGET Lua. */
  async peekFirst(): Promise<V | null> {
    return this.decodeValue(await luaEval(this.redis, ACB_PEEK_FIRST_LUA, 2, this.name, this.settingsName));
  }

  /** Java `count(V)`: AROP MATCH over the whole window. */
  async count(value: V): Promise<number> {
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    return toNumber(await luaEval(this.redis, ACB_COUNT_LUA, 1, this.name, this.encodeValue(value)));
  }

  /** Java `contains(V)`. */
  async contains(value: V): Promise<boolean> {
    if (value === null || value === undefined) throw new Error("redi.ts: Value can't be null");
    return toNumber(await luaEval(this.redis, ACB_CONTAINS_LUA, 1, this.name, this.encodeValue(value))) === 1;
  }

  /** Java `average()`: SUM / size (null when empty). */
  async average(): Promise<number | null> {
    return this.evalDouble(ACB_AVERAGE_LUA, 1, this.name);
  }

  /** Java `bitAnd()`. */
  async bitAnd(): Promise<number | null>;
  async bitAnd(startIndex: number, endIndex: number): Promise<number | null>;
  async bitAnd(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.longAggregate('AND');
    return this.longOperation(a, b!, 'AND');
  }
  /** Java `bitOr()`. */
  async bitOr(): Promise<number | null>;
  async bitOr(startIndex: number, endIndex: number): Promise<number | null>;
  async bitOr(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.longAggregate('OR');
    return this.longOperation(a, b!, 'OR');
  }
  /** Java `bitXor()`. */
  async bitXor(): Promise<number | null>;
  async bitXor(startIndex: number, endIndex: number): Promise<number | null>;
  async bitXor(a?: number, b?: number): Promise<any> {
    if (a === undefined) return this.longAggregate('XOR');
    return this.longOperation(a, b!, 'XOR');
  }

  /** Java `delete()`: both the array key and the settings key. */
  override async delete(): Promise<boolean> {
    return ((await this.redis.del(this.name, this.settingsName)) as number) > 0;
  }

  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name, this.settingsName]);
  }

  // Java overrides expire/expireAt/clearExpire to cover the settings key too.
  override async expire(ttlMs: number): Promise<boolean> {
    const a = (await this.redis.pexpire(this.name, ttlMs)) === 1;
    const b = (await this.redis.pexpire(this.settingsName, ttlMs)) === 1;
    return a || b;
  }
  override async expireAt(timestampMs: number): Promise<boolean> {
    const a = (await this.redis.pexpireat(this.name, timestampMs)) === 1;
    const b = (await this.redis.pexpireat(this.settingsName, timestampMs)) === 1;
    return a || b;
  }
  override async clearExpire(): Promise<boolean> {
    const a = (await this.redis.persist(this.name)) === 1;
    const b = (await this.redis.persist(this.settingsName)) === 1;
    return a || b;
  }
}
