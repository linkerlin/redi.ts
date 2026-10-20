/**
 * Probabilistic / geo / bit / time-series / rate-limiter structures.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import {
  bloomBitIndex,
  highwayHash128,
  luaEval,
  optimalBloomBits,
  optimalBloomIters,
  packRatePermit,
  packTimeSeriesMember,
  prefixName,
  serverNowMs,
  suffixName,
  toNumber,
  unpackTimeSeriesMember,
} from './util.js';
import { KeyspaceListenerHub, type ObjectListener } from './listeners.js';
import { randomBytes } from 'node:crypto';

// Per-process fallback tag for PER_CLIENT windows built without a client id
// (mirrors Java's per-RedissonClient-instance sharing for the common
// single-client-per-process case).
let processTag = '';
function processRateTag(): string {
  if (!processTag) processTag = randomBytes(8).toString('hex');
  return processTag;
}

function formatFloat(n: number): string {
  // Go strconv 'f' -1 equivalent: shortest round-trip decimal
  return String(n);
}

/** Flat RESP field/value array → record (e.g. CF.INFO / TOPK.INFO). */
function flatRecord(flat: any): Record<string, any> {
  const out: Record<string, any> = {};
  if (Array.isArray(flat)) for (let i = 0; i + 1 < flat.length; i += 2) out[String(flat[i])] = flat[i + 1];
  return out;
}

// -- BloomFilter -----------------------------------------------------------------

export class RBloomFilter extends RedisDataStructure {
  private size = 0n;
  private hashIters = 0;
  private expected = 55_000_000;
  private falseProb = 0.03;
  private loaded = false;
  constructor(redis: Redis, name: string) {
    super(redis, name, new JsonCodec());
  }
  private configKey(): string {
    return suffixName(this.name, 'config');
  }
  private hash128(element: unknown): [bigint, bigint] {
    return highwayHash128(Buffer.from(this.encodeValue(element), 'utf8'));
  }
  private bitIndex(h1: bigint, h2: bigint, iter: number): bigint {
    return bloomBitIndex(h1, h2, iter, this.size === 0n ? 1n : this.size);
  }
  private async ensureConfig(): Promise<void> {
    if (this.loaded) return;
    const m = await this.redis.hgetall(this.configKey());
    if (m && Object.keys(m).length > 0) {
      this.size = BigInt(m['size'] ?? '0');
      this.hashIters = Number(m['hashIterations'] ?? '0');
      this.expected = Number(m['expectedInsertions'] ?? this.expected);
      this.falseProb = Number(m['falseProbability'] ?? this.falseProb);
      this.loaded = true;
      return;
    }
    await this.tryInit(this.expected, this.falseProb);
  }
  async tryInit(expectedInsertions: number, falseProbability: number): Promise<boolean> {
    this.expected = expectedInsertions;
    this.falseProb = falseProbability;
    this.size = optimalBloomBits(expectedInsertions, falseProbability);
    this.hashIters = optimalBloomIters(expectedInsertions, Number(this.size));
    const s = `if redis.call('exists', KEYS[1]) == 1 then
    return 0
end
redis.call('hset', KEYS[1], 'size', ARGV[1], 'hashIterations', ARGV[2], 'expectedInsertions', ARGV[3], 'falseProbability', ARGV[4])
return 1`;
    let n = 0;
    try {
      n = toNumber(await luaEval(this.redis, s, 1, this.configKey(), this.size.toString(), String(this.hashIters), String(expectedInsertions), formatFloat(falseProbability)));
    } catch {
      const ex = await this.redis.exists(this.configKey());
      if (ex === 0) {
        await this.redis.hset(this.configKey(), 'size', this.size.toString(), 'hashIterations', String(this.hashIters), 'expectedInsertions', String(expectedInsertions), 'falseProbability', formatFloat(falseProbability));
        n = 1;
      }
    }
    if (n === 1) {
      this.loaded = true;
      return true;
    }
    this.loaded = false;
    await this.ensureConfig();
    return false;
  }
  async add(element: unknown): Promise<boolean> {
    await this.ensureConfig();
    const [h1, h2] = this.hash128(element);
    let added = false;
    for (let i = 0; i < this.hashIters; i++) {
      const bit = Number(this.bitIndex(h1, h2, i));
      const v = await this.redis.getbit(this.name, bit);
      if (v === 0) {
        await this.redis.setbit(this.name, bit, 1);
        added = true;
      }
    }
    return added;
  }
  async contains(element: unknown): Promise<boolean> {
    await this.ensureConfig();
    const [h1, h2] = this.hash128(element);
    for (let i = 0; i < this.hashIters; i++) {
      if ((await this.redis.getbit(this.name, Number(this.bitIndex(h1, h2, i)))) === 0) return false;
    }
    return true;
  }
  async count(): Promise<number> {
    await this.ensureConfig();
    const bits = await this.redis.bitcount(this.name);
    if (bits === 0 || this.size === 0n) return 0;
    const frac = bits / Number(this.size);
    if (frac >= 1) return Number.MAX_SAFE_INTEGER;
    return Math.trunc((-Number(this.size) * Math.log(1 - frac)) / this.hashIters);
  }
  async getSize(): Promise<number> {
    await this.ensureConfig();
    return Number(this.size);
  }
  async getHashIterations(): Promise<number> {
    await this.ensureConfig();
    return this.hashIters;
  }
  override async delete(): Promise<boolean> {
    await this.redis.del(this.name, this.configKey());
    this.loaded = false;
    return true;
  }
  override async unlink(): Promise<boolean> {
    await this.unlinkKeys([this.name, this.configKey()]);
    this.loaded = false;
    return true;
  }
}

// -- HyperLogLog ------------------------------------------------------------------

export class RHyperLogLog extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async add(...elements: unknown[]): Promise<boolean> {
    if (elements.length === 0) return false;
    return ((await this.redis.pfadd(this.name, ...elements.map((e) => this.encodeValue(e)))) as number) === 1;
  }
  async count(): Promise<number> {
    return Number(await this.redis.pfcount(this.name));
  }
  async countWith(...otherKeys: string[]): Promise<number> {
    return Number(await this.redis.pfcount(this.name, ...otherKeys));
  }
  async mergeWith(...otherKeys: string[]): Promise<void> {
    await this.redis.pfmerge(this.name, ...otherKeys);
  }
}

// -- Geo --------------------------------------------------------------------------

export interface GeoEntry<V = any> {
  member: V;
  lon: number;
  lat: number;
  dist?: number;
}

export type GeoUnit = 'm' | 'km' | 'mi' | 'ft';
export type GeoOrder = 'ASC' | 'DESC';

/**
 * Java `GeoSearchArgs` builder: origin (member or coordinates), shape
 * (radius or box) and optional ordering/count for `RGeo.search*`/`store*`.
 */
export class GeoSearchArgs<V = any> {
  private memberValue?: V;
  private longitude?: number;
  private latitude?: number;
  private radiusValue?: number;
  private widthValue?: number;
  private heightValue?: number;
  private unitValue?: GeoUnit;
  private orderValue?: GeoOrder;
  private countValue?: number;
  private countAnyValue = false;
  private constructor() {}
  /** Java `GeoSearchArgs.from(V member)`. */
  static from<V>(member: V): GeoSearchArgs<V>;
  /** Java `GeoSearchArgs.from(double longitude, double latitude)`. */
  static from(longitude: number, latitude: number): GeoSearchArgs<any>;
  static from(a: any, b?: number): GeoSearchArgs<any> {
    const args = new GeoSearchArgs<any>();
    if (typeof a === 'number') {
      args.longitude = a;
      args.latitude = b;
    } else {
      args.memberValue = a;
    }
    return args;
  }
  /** Java `radius(double, GeoUnit)`. */
  radius(radius: number, unit: GeoUnit): this {
    this.radiusValue = radius;
    this.unitValue = unit;
    return this;
  }
  /** Java `box(double width, double height, GeoUnit)` — selects GEOSEARCH/GEOSEARCHSTORE. */
  box(width: number, height: number, unit: GeoUnit): this {
    this.widthValue = width;
    this.heightValue = height;
    this.unitValue = unit;
    return this;
  }
  /** Java `order(GeoOrder)`. */
  order(order: GeoOrder): this {
    this.orderValue = order;
    return this;
  }
  /** Java `count(int)`. */
  count(count: number): this {
    this.countValue = count;
    this.countAnyValue = false;
    return this;
  }
  /** Java `countAny(int)`. */
  countAny(count: number): this {
    this.countValue = count;
    this.countAnyValue = true;
    return this;
  }
  getMember(): V | undefined {
    return this.memberValue;
  }
  getLongitude(): number | undefined {
    return this.longitude;
  }
  getLatitude(): number | undefined {
    return this.latitude;
  }
  getRadius(): number | undefined {
    return this.radiusValue;
  }
  getWidth(): number | undefined {
    return this.widthValue;
  }
  getHeight(): number | undefined {
    return this.heightValue;
  }
  getUnit(): GeoUnit | undefined {
    return this.unitValue;
  }
  getOrder(): GeoOrder | undefined {
    return this.orderValue;
  }
  getCount(): number | undefined {
    return this.countValue;
  }
  isCountAny(): boolean {
    return this.countAnyValue;
  }
}

export class RGeo<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async add(lon: number, lat: number, member: V): Promise<boolean> {
    return ((await this.redis.geoadd(this.name, lon, lat, this.encodeValue(member))) as number) > 0;
  }
  async addAll(entries: { lon: number; lat: number; member: V }[]): Promise<number> {
    if (entries.length === 0) return 0;
    const args: (string | number)[] = [];
    for (const e of entries) args.push(e.lon, e.lat, this.encodeValue(e.member));
    return this.redis.geoadd(this.name, ...args);
  }
  async tryAdd(lon: number, lat: number, member: V): Promise<boolean> {
    const s = `local sc = redis.call('zscore', KEYS[1], ARGV[3])
if sc then return 0 end
redis.call('geoadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
return 1`;
    try {
      return toNumber(await luaEval(this.redis, s, 1, this.name, formatFloat(lon), formatFloat(lat), this.encodeValue(member))) === 1;
    } catch {
      const sc = await this.redis.zscore(this.name, this.encodeValue(member));
      if (sc !== null) return false;
      return this.add(lon, lat, member);
    }
  }
  async dist(m1: V, m2: V, unit: 'm' | 'km' | 'mi' | 'ft' = 'm'): Promise<number | null> {
    const d = await (this.redis as any).geodist(this.name, this.encodeValue(m1), this.encodeValue(m2), unit);
    return d === null ? null : Number(d);
  }
  async pos(...members: V[]): Promise<(GeoEntry<V> | null)[]> {
    if (members.length === 0) return [];
    const r: any = await this.redis.geopos(this.name, ...members.map((m) => this.encodeValue(m)));
    return r.map((p: any, i: number) => {
      if (!p) return null;
      return { member: members[i], lon: Number(p[0]), lat: Number(p[1]) };
    });
  }
  async hash(...members: V[]): Promise<(string | null)[]> {
    if (members.length === 0) return [];
    return this.redis.geohash(this.name, ...members.map((m) => this.encodeValue(m)));
  }
  /** Java `search(GeoSearchArgs)`: matching members (JGeoSearchArgs → GEOSEARCH / GEORADIUS_RO family). */
  async search(args: GeoSearchArgs<V>): Promise<V[]>;
  /** Convenience (TS extension): members with positions around a coordinate (GEOSEARCH BYRADIUS). */
  async search(lon: number, lat: number, radius: number, unit?: GeoUnit, count?: number): Promise<GeoEntry<V>[]>;
  async search(a: GeoSearchArgs<V> | number, lat?: number, radius?: number, unit: GeoUnit = 'm', count = 10): Promise<V[] | GeoEntry<V>[]> {
    if (typeof a === 'number') {
      const r: any = await this.redis.geosearch(this.name, 'FROMLONLAT', formatFloat(a), formatFloat(lat!), 'BYRADIUS', formatFloat(radius!), unit, 'WITHCOORD', 'ASC', 'COUNT', count);
      return (r ?? []).map((e: any) => this.toEntry(e));
    }
    const { cmd, params } = this.buildSearch(a, 'none');
    const r: any = await (this.redis as any)[cmd](...params);
    return ((r ?? []) as any[]).map((m) => this.decodeValue(m) as V);
  }
  /** Java `searchWithDistance(GeoSearchArgs)`: members → distance (WITHDIST). */
  async searchWithDistance(args: GeoSearchArgs<V>): Promise<Map<V, number>> {
    const { cmd, params } = this.buildSearch(args, 'dist');
    const r: any = await (this.redis as any)[cmd](...params);
    const out = new Map<V, number>();
    if (!r) return out;
    if (Array.isArray(r) && r.length > 0 && Array.isArray(r[0])) {
      for (const e of r) out.set(this.decodeValue(e[0]) as V, Number(e[1]));
    } else {
      for (let i = 0; i + 1 < r.length; i += 2) out.set(this.decodeValue(r[i]) as V, Number(r[i + 1]));
    }
    return out;
  }
  /** Java `searchWithPosition(GeoSearchArgs)`: members → coordinates (WITHCOORD). */
  async searchWithPosition(args: GeoSearchArgs<V>): Promise<Map<V, { lon: number; lat: number }>> {
    const { cmd, params } = this.buildSearch(args, 'pos');
    const r: any = await (this.redis as any)[cmd](...params);
    const out = new Map<V, { lon: number; lat: number }>();
    if (!r) return out;
    if (Array.isArray(r) && r.length > 0 && Array.isArray(r[0])) {
      for (const e of r) {
        const c = e[1];
        out.set(this.decodeValue(e[0]) as V, { lon: Number(c?.[0]), lat: Number(c?.[1]) });
      }
    }
    return out;
  }
  /** Java `storeSearchTo(String, GeoSearchArgs)`: GEOSEARCHSTORE / GEORADIUS ... STORE. */
  async storeSearchTo(destName: string, args: GeoSearchArgs<V>): Promise<number> {
    const { cmd, params } = this.buildStore(destName, args, false);
    return toNumber(await (this.redis as any)[cmd](...params));
  }
  /** Java `storeSortedSearchTo(String, GeoSearchArgs)`: GEOSEARCHSTORE ... STOREDIST / GEORADIUS ... STOREDIST. */
  async storeSortedSearchTo(destName: string, args: GeoSearchArgs<V>): Promise<number> {
    const { cmd, params } = this.buildStore(destName, args, true);
    return toNumber(await (this.redis as any)[cmd](...params));
  }
  /** Java `addIfExists(double, double, V)`: GEOADD only when the member already exists. */
  async addIfExists(lon: number, lat: number, member: V): Promise<boolean>;
  /** Java `addIfExists(GeoEntry...)`: GEOADD XX — returns the raw reply (elements added; Redis never counts XX score updates, so it is 0), same as Java. */
  async addIfExists(entries: { lon: number; lat: number; member: V }[]): Promise<number>;
  async addIfExists(a: number | { lon: number; lat: number; member: V }[], lat?: number, member?: V): Promise<boolean | number> {
    if (typeof a === 'number') {
      const s = `local value = redis.call('geopos', KEYS[1], ARGV[3])
if value[1] ~= false then
redis.call('geoadd', KEYS[1], ARGV[1], ARGV[2], ARGV[3])
return 1
end
return 0`;
      try {
        return toNumber(await luaEval(this.redis, s, 1, this.name, formatFloat(a), formatFloat(lat!), this.encodeValue(member))) === 1;
      } catch {
        const p: any = await this.redis.geopos(this.name, this.encodeValue(member));
        if (p && p[0]) {
          await this.add(a, lat!, member as V);
          return true;
        }
        return false;
      }
    }
    if (a.length === 0) return 0;
    const args: (string | number)[] = [];
    for (const e of a) args.push(formatFloat(e.lon), formatFloat(e.lat), this.encodeValue(e.member));
    return toNumber(await this.redis.geoadd(this.name, 'XX', ...args));
  }
  /** Java command selection: box → GEOSEARCH, radius → GEORADIUS_RO / GEORADIUSBYMEMBER_RO. */
  private buildSearch(args: GeoSearchArgs<V>, kind: 'none' | 'dist' | 'pos'): { cmd: string; params: (string | number)[] } {
    const hasBox = args.getHeight() !== undefined && args.getWidth() !== undefined;
    const params: (string | number)[] = [this.name];
    let cmd: string;
    if (args.getLongitude() !== undefined && args.getLatitude() !== undefined) {
      cmd = hasBox ? 'geosearch' : 'georadius_ro';
      if (hasBox) params.push('FROMLONLAT');
      params.push(formatFloat(args.getLongitude()!), formatFloat(args.getLatitude()!));
    } else {
      cmd = hasBox ? 'geosearch' : 'georadiusbymember_ro';
      if (hasBox) params.push('FROMMEMBER');
      params.push(this.encodeValue(args.getMember()));
    }
    const unit = args.getUnit();
    if (args.getRadius() !== undefined && unit !== undefined) {
      params.push(formatFloat(args.getRadius()!), unit);
    }
    if (hasBox && unit !== undefined) {
      params.push('BYBOX', formatFloat(args.getWidth()!), formatFloat(args.getHeight()!), unit);
      if (args.getOrder() !== undefined) params.push(args.getOrder()!);
    }
    if (kind === 'dist' && !hasBox) params.push('WITHDIST');
    if (kind === 'pos' && !hasBox) params.push('WITHCOORD');
    if (args.getCount() !== undefined) {
      params.push('COUNT', args.getCount()!);
      if (args.isCountAny()) params.push('ANY');
    }
    if (!hasBox && args.getOrder() !== undefined) params.push(args.getOrder()!);
    if (kind === 'dist' && hasBox) params.push('WITHDIST');
    if (kind === 'pos' && hasBox) params.push('WITHCOORD');
    return { cmd, params };
  }
  private buildStore(destName: string, args: GeoSearchArgs<V>, sorted: boolean): { cmd: string; params: (string | number)[] } {
    const hasBox = args.getHeight() !== undefined && args.getWidth() !== undefined;
    const params: (string | number)[] = [];
    if (hasBox) params.push(destName);
    params.push(this.name);
    let cmd: string;
    if (args.getLongitude() !== undefined && args.getLatitude() !== undefined) {
      cmd = hasBox ? 'geosearchstore' : 'georadius';
      if (hasBox) params.push('FROMLONLAT');
      params.push(formatFloat(args.getLongitude()!), formatFloat(args.getLatitude()!));
    } else {
      cmd = hasBox ? 'geosearchstore' : 'georadiusbymember';
      if (hasBox) params.push('FROMMEMBER');
      params.push(this.encodeValue(args.getMember()));
    }
    const unit = args.getUnit();
    if (args.getRadius() !== undefined && unit !== undefined) {
      params.push(formatFloat(args.getRadius()!), unit);
    }
    if (hasBox && unit !== undefined) {
      params.push('BYBOX', formatFloat(args.getWidth()!), formatFloat(args.getHeight()!), unit);
      if (args.getOrder() !== undefined) params.push(args.getOrder()!);
    }
    if (args.getCount() !== undefined) {
      params.push('COUNT', args.getCount()!);
      if (args.isCountAny()) params.push('ANY');
    }
    if (!hasBox && args.getOrder() !== undefined) params.push(args.getOrder()!);
    if (sorted) params.push('STOREDIST');
    if (!hasBox) params.push(destName);
    return { cmd, params };
  }
  private toEntry(e: any): GeoEntry<V> {
    if (Array.isArray(e)) {
      const c = e[1];
      return { member: this.decodeValue(e[0]) as V, lon: Number(c?.[0]), lat: Number(c?.[1]) };
    }
    return { member: this.decodeValue(e) as V, lon: 0, lat: 0 };
  }
  async size(): Promise<number> {
    return this.redis.zcard(this.name);
  }
  async remove(...members: V[]): Promise<number> {
    if (members.length === 0) return 0;
    return this.redis.zrem(this.name, ...members.map((m) => this.encodeValue(m)));
  }
}

// -- BitSet (native bit numbering, MSB-first = Redis native, no reversal) ----------

export type BitFieldOverflow = 'WRAP' | 'SAT' | 'FAIL';

/** Java `BitOffset`: plain bit offsets, or `#`-prefixed element indexes. */
export const BitOffset = {
  /** Zero-based bit offset. */
  bit: (offset: number): string => String(offset),
  /** Index of the addressed integer element (sent as `#index`). */
  index: (index: number): string => `#${index}`,
};

/**
 * Java `BitFieldArgs` builder for `RBitSet.bitField` (BITFIELD
 * GET/SET/INCRBY operations plus OVERFLOW control). Chain calls and pass
 * the instance to `bitField`.
 */
export class BitFieldArgs {
  private readonly ops: (string | number)[] = [];
  private write = false;
  private pushOp(op: string, encoding: string, offset: number | string, value?: number): void {
    const type = encoding[0];
    const size = Number(encoding.slice(1));
    if (type === 'u') {
      if (size > 63) throw new Error("Size can't be greater than 63 bits");
    } else if (type === 'i') {
      if (size > 64) throw new Error("Size can't be greater than 64 bits");
    } else {
      throw new Error('Invalid encoding');
    }
    this.ops.push(op, encoding, offset);
    if (value !== undefined) this.ops.push(value);
  }
  overflow(overflow: BitFieldOverflow): this {
    this.ops.push('OVERFLOW', overflow);
    return this;
  }
  getSigned(size: number, offset: number | string): this {
    this.pushOp('GET', `i${size}`, offset);
    return this;
  }
  getUnsigned(size: number, offset: number | string): this {
    this.pushOp('GET', `u${size}`, offset);
    return this;
  }
  setSigned(size: number, offset: number | string, value: number): this {
    this.pushOp('SET', `i${size}`, offset, value);
    this.write = true;
    return this;
  }
  setUnsigned(size: number, offset: number | string, value: number): this {
    this.pushOp('SET', `u${size}`, offset, value);
    this.write = true;
    return this;
  }
  incrementSignedBy(size: number, offset: number | string, increment: number): this {
    this.pushOp('INCRBY', `i${size}`, offset, increment);
    this.write = true;
    return this;
  }
  incrementUnsignedBy(size: number, offset: number | string, increment: number): this {
    this.pushOp('INCRBY', `u${size}`, offset, increment);
    this.write = true;
    return this;
  }
  /** @internal */
  commandArgs(): (string | number)[] {
    if (this.ops.length === 0) throw new Error('No bitfield operations defined');
    return [...this.ops];
  }
  /** @internal */
  isWrite(): boolean {
    return this.write;
  }
}

export class RBitSet extends RedisDataStructure {
  async get(bit: number): Promise<boolean>;
  async get(...bits: number[]): Promise<boolean[]>;
  async get(...bits: number[]): Promise<boolean | boolean[]> {
    if (bits.length === 1) return ((await this.redis.getbit(this.name, bits[0])) as number) === 1;
    const pipe = this.redis.pipeline();
    for (const b of bits) pipe.getbit(this.name, b);
    const res = await pipe.exec();
    return (res ?? []).map(([e, r]) => e === null && Number(r) === 1);
  }
  async set(bit: number, value?: boolean): Promise<boolean>;
  async set(from: number, to: number, value: boolean): Promise<void>;
  async set(bits: number[] | Set<number>, value?: boolean): Promise<void>;
  async set(a: number | number[] | Set<number>, b: boolean | number = true, c?: boolean): Promise<boolean | void> {
    if (typeof a === 'number' && typeof b === 'number') {
      await this.setRange(a, b, c ?? true);
      return;
    }
    if (typeof a === 'number') {
      const prev = await this.redis.setbit(this.name, a, b ? 1 : 0);
      return prev === 1;
    }
    const bits = a instanceof Set ? [...a] : a;
    if (bits.length === 0) return;
    const pipe = this.redis.pipeline();
    for (const bit of bits) pipe.setbit(this.name, bit, b ? 1 : 0);
    await pipe.exec();
  }
  /** Java `asBitSet()`: indices of all set bits (TS Set<number> analogue).
   *  Uses pipelined GETBIT so mock and real servers agree on bit ordering. */
  async asBitSet(): Promise<Set<number>> {
    const bits = (await this.redis.strlen(this.name)) * 8;
    const out = new Set<number>();
    const chunk = 1024;
    for (let start = 0; start < bits; start += chunk) {
      const end = Math.min(start + chunk, bits);
      const pipe = this.redis.pipeline();
      for (let i = start; i < end; i++) pipe.getbit(this.name, i);
      const res = await pipe.exec();
      for (let i = 0; i < (res ?? []).length; i++) {
        const row = res![i];
        if (row[0] === null && Number(row[1]) === 1) out.add(start + i);
      }
    }
    return out;
  }
  /** Java `set(long fromIndex, long toIndex, boolean)`: SETBIT loop, `to` exclusive. */
  async setRange(from: number, to: number, value = true): Promise<void> {
    const pipeline = this.redis.pipeline();
    for (let i = from; i < to; i++) pipeline.setbit(this.name, i, value ? 1 : 0);
    await pipeline.exec();
  }
  async clear(bit?: number, to?: number): Promise<void> {
    if (bit === undefined) {
      await this.redis.del(this.name);
    } else if (to === undefined) {
      await this.redis.setbit(this.name, bit, 0);
    } else {
      await this.setRange(bit, to, false);
    }
  }
  async cardinality(): Promise<number> {
    return this.redis.bitcount(this.name);
  }
  /** Java `size()`: STRLEN × 8 — the backing string length in bits. */
  async size(): Promise<number> {
    return toNumber(await this.redis.strlen(this.name)) * 8;
  }
  async length(): Promise<number> {
    // highest set bit + 1 (Java BitSet semantics; same result as the 4.6.1 bitpos Lua)
    const raw = await this.redis.getBuffer?.(this.name);
    const buf: Buffer | null = raw === null || raw === undefined ? null : Buffer.isBuffer(raw) ? raw : Buffer.from(raw as any, 'latin1');
    if (!buf || buf.length === 0) return 0;
    for (let i = buf.length - 1; i >= 0; i--) {
      const b = buf[i];
      if (b !== 0) {
        // Redis bit index k (0..7) inside a byte uses mask 1 << (7 - k); the highest
        // global bit is the largest k, i.e. the smallest mask.
        for (let mask = 1, k = 7; k >= 0; mask <<= 1, k--) {
          if (b & mask) return i * 8 + k + 1;
        }
      }
    }
    return 0;
  }
  async toByteArray(): Promise<Buffer | null> {
    const raw = await this.redis.getBuffer?.(this.name);
    if (raw === null || raw === undefined) return null;
    return Buffer.isBuffer(raw) ? raw : Buffer.from(raw as any, 'latin1');
  }
  async fromByteArray(data: Buffer | Uint8Array | string): Promise<void> {
    if (!data || (Buffer.isBuffer(data) && data.length === 0)) {
      await this.redis.del(this.name);
      return;
    }
    const buf = typeof data === 'string' ? data : Buffer.isBuffer(data) ? data : Buffer.from(data);
    await this.redis.set(this.name, buf);
  }
  private validateBitSize(size: number, unsigned: boolean): void {
    if (unsigned) {
      if (size > 63) throw new Error("Size can't be greater than 63 bits");
    } else if (size > 64) {
      throw new Error("Size can't be greater than 64 bits");
    }
  }
  private async bitFieldCall(...args: (string | number)[]): Promise<(number | null)[]> {
    const r = (await (this.redis as any).bitfield(this.name, ...args)) as (number | null)[];
    return (r ?? []).map((v) => (v === null || v === undefined ? null : Number(v)));
  }
  /** Java `getSigned(int size, long offset)`: BITFIELD GET i<size>. */
  async getSigned(size: number, offset: number): Promise<number> {
    this.validateBitSize(size, false);
    return Number((await this.bitFieldCall('GET', `i${size}`, offset))[0]);
  }
  /** Java `setSigned(int size, long offset, long value)`: BITFIELD SET i<size>, returns the old value. */
  async setSigned(size: number, offset: number, value: number): Promise<number> {
    this.validateBitSize(size, false);
    return Number((await this.bitFieldCall('SET', `i${size}`, offset, value))[0]);
  }
  /** Java `incrementAndGetSigned(int size, long offset, long increment)`: BITFIELD INCRBY i<size>. */
  async incrementAndGetSigned(size: number, offset: number, increment: number): Promise<number> {
    this.validateBitSize(size, false);
    return Number((await this.bitFieldCall('INCRBY', `i${size}`, offset, increment))[0]);
  }
  /** Java `getUnsigned(int size, long offset)`: BITFIELD GET u<size>. */
  async getUnsigned(size: number, offset: number): Promise<number> {
    this.validateBitSize(size, true);
    return Number((await this.bitFieldCall('GET', `u${size}`, offset))[0]);
  }
  /** Java `setUnsigned(int size, long offset, long value)`: BITFIELD SET u<size>, returns the old value. */
  async setUnsigned(size: number, offset: number, value: number): Promise<number> {
    this.validateBitSize(size, true);
    return Number((await this.bitFieldCall('SET', `u${size}`, offset, value))[0]);
  }
  /** Java `incrementAndGetUnsigned(int size, long offset, long increment)`: BITFIELD INCRBY u<size>. */
  async incrementAndGetUnsigned(size: number, offset: number, increment: number): Promise<number> {
    this.validateBitSize(size, true);
    return Number((await this.bitFieldCall('INCRBY', `u${size}`, offset, increment))[0]);
  }
  /** Java `getByte/setByte/incrementAndGetByte`: BITFIELD i8 at the given offset. */
  async getByte(offset: number): Promise<number> {
    return Number((await this.bitFieldCall('GET', 'i8', offset))[0]);
  }
  async setByte(offset: number, value: number): Promise<number> {
    return Number((await this.bitFieldCall('SET', 'i8', offset, value))[0]);
  }
  async incrementAndGetByte(offset: number, increment: number): Promise<number> {
    return Number((await this.bitFieldCall('INCRBY', 'i8', offset, increment))[0]);
  }
  /** Java `getShort/setShort/incrementAndGetShort`: BITFIELD i16 at the given offset. */
  async getShort(offset: number): Promise<number> {
    return Number((await this.bitFieldCall('GET', 'i16', offset))[0]);
  }
  async setShort(offset: number, value: number): Promise<number> {
    return Number((await this.bitFieldCall('SET', 'i16', offset, value))[0]);
  }
  async incrementAndGetShort(offset: number, increment: number): Promise<number> {
    return Number((await this.bitFieldCall('INCRBY', 'i16', offset, increment))[0]);
  }
  /** Java `getInteger/setInteger/incrementAndGetInteger`: BITFIELD i32 at the given offset. */
  async getInteger(offset: number): Promise<number> {
    return Number((await this.bitFieldCall('GET', 'i32', offset))[0]);
  }
  async setInteger(offset: number, value: number): Promise<number> {
    return Number((await this.bitFieldCall('SET', 'i32', offset, value))[0]);
  }
  async incrementAndGetInteger(offset: number, increment: number): Promise<number> {
    return Number((await this.bitFieldCall('INCRBY', 'i32', offset, increment))[0]);
  }
  /** Java `getLong/setLong/incrementAndGetLong`: BITFIELD i64 at the given offset. */
  async getLong(offset: number): Promise<number> {
    return Number((await this.bitFieldCall('GET', 'i64', offset))[0]);
  }
  async setLong(offset: number, value: number): Promise<number> {
    return Number((await this.bitFieldCall('SET', 'i64', offset, value))[0]);
  }
  async incrementAndGetLong(offset: number, increment: number): Promise<number> {
    return Number((await this.bitFieldCall('INCRBY', 'i64', offset, increment))[0]);
  }
  /** Java `bitField(BitFieldArgs)`: one BITFIELD call, replies in operation order (null on OVERFLOW FAIL). */
  async bitField(args: BitFieldArgs): Promise<(number | null)[]> {
    return this.bitFieldCall(...args.commandArgs());
  }
  private async bitOp(op: string, others: string[]): Promise<number> {
    return toNumber(await this.redis.bitop(op, this.name, this.name, ...others));
  }
  /** Java `or(String...)`: BITOP OR — returns the byte length of the result. */
  async or(...otherKeys: string[]): Promise<number> {
    return this.bitOp('OR', otherKeys);
  }
  /** Java `and(String...)`: BITOP AND — returns the byte length of the result. */
  async and(...otherKeys: string[]): Promise<number> {
    return this.bitOp('AND', otherKeys);
  }
  /** Java `xor(String...)`: BITOP XOR — returns the byte length of the result. */
  async xor(...otherKeys: string[]): Promise<number> {
    return this.bitOp('XOR', otherKeys);
  }
  /** Java `not()`: BITOP NOT — returns the byte length of the result. */
  async not(): Promise<number> {
    return this.bitOp('NOT', []);
  }
  /** Java `diff(String...)`: BITOP DIFF (Redis 8) — first minus the rest. */
  async diff(...otherKeys: string[]): Promise<number> {
    return this.bitOp('DIFF', otherKeys);
  }
  /** Java `diffInverse(String...)`: BITOP DIFF1 (Redis 8). */
  async diffInverse(...otherKeys: string[]): Promise<number> {
    return this.bitOp('DIFF1', otherKeys);
  }
  /** Java `andOr(String...)`: BITOP ANDOR (Redis 8) — AND of the first with the OR of the rest. */
  async andOr(...otherKeys: string[]): Promise<number> {
    return this.bitOp('ANDOR', otherKeys);
  }
  /** Java `setExclusive(String...)`: BITOP ONE (Redis 8) — exactly one input set. */
  async setExclusive(...otherKeys: string[]): Promise<number> {
    return this.bitOp('ONE', otherKeys);
  }
}

// -- TimeSeries --------------------------------------------------------------------

export interface TimeSeriesEntry<V = any> {
  timestamp: number;
  value: V;
  label?: string;
}

const TS_NEXT_ID_WIDTH = 20;

const TS_ADD_ALL = `local sequenceWidth = tonumber(ARGV[1])
local previous
local function nextId()
    if previous == nil then
        previous = redis.call('get', KEYS[3])
        if previous == false then previous = '0' end
        previous = string.rep('0', sequenceWidth - string.len(previous)) .. previous
    end
    previous = previous + 1
    redis.call('set', KEYS[3], previous)
    return previous
end
for i = 3, #ARGV, 3 do
    local id = nextId()
    local label = ARGV[i + 2]
    local mark = 2
    if label ~= '' then mark = 3 end
    local val = struct.pack('BBc0Lc0Lc0', mark, string.len(id), id,
                            string.len(ARGV[i + 1]), ARGV[i + 1],
                            string.len(label), label)
    redis.call('zadd', KEYS[1], ARGV[i], val)
    redis.call('zadd', KEYS[2], ARGV[2], val)
end`;

const TS_HEAD_TAIL = `local count = tonumber(ARGV[3])
if count == 0 then return {} end
local cmd = 'zrange'
if ARGV[2] == '1' then cmd = 'zrevrange' end
local members = {}
local scores = {}
local total = redis.call('zcard', KEYS[1])
local offset = 0
local batch = 100
if count > batch then batch = count end
while offset < total and (count < 0 or #members < count) do
    local values = redis.call(cmd, KEYS[1], offset, offset + batch - 1, 'withscores')
    if #values == 0 then break end
    for i = 1, #values, 2 do
        local expirationDate = redis.call('zscore', KEYS[2], values[i])
        if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
            table.insert(members, values[i])
            table.insert(scores, values[i + 1])
            if count > 0 and #members == count then break end
        end
    end
    offset = offset + #values / 2
end
if ARGV[4] == '1' then
    local out = {}
    for i = 1, #members do
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', members[i])
        local lbl = ''
        if n == 4 then
            local mark = string.byte(label, 1)
            if mark == 3 then lbl = string.sub(label, 2) end
        elseif n ~= 2 then
            lbl = label
        end
        table.insert(out, scores[i])
        table.insert(out, val)
        table.insert(out, lbl)
    end
    for i = 1, #members do
        redis.call('zrem', KEYS[1], members[i])
        redis.call('zrem', KEYS[2], members[i])
    end
    return out
end
local out = {}
for i = 1, #members do
    local n, t, val, label = struct.unpack('BBc0Lc0Lc0', members[i])
    local lbl = ''
    if n == 4 then
        local mark = string.byte(label, 1)
        if mark == 3 then lbl = string.sub(label, 2) end
    elseif n ~= 2 then
        lbl = label
    end
    table.insert(out, scores[i])
    table.insert(out, val)
    table.insert(out, lbl)
end
return out`;

const TS_GET_AND_REMOVE = `local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2])
for i = 1, #values do
    local expirationDate = redis.call('zscore', KEYS[2], values[i])
    if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i])
        local lbl = ''
        if n == 4 then
            local mark = string.byte(label, 1)
            if mark == 3 then lbl = string.sub(label, 2) end
        elseif n ~= 2 then
            lbl = label
        end
        redis.call('zrem', KEYS[2], values[i])
        redis.call('zrem', KEYS[1], values[i])
        return {ARGV[2], val, lbl}
    end
end
return nil`;

const TS_REMOVE_RANGE = `local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3])
local removed = 0
for i = 1, #values do
    local expirationDate = redis.call('zscore', KEYS[2], values[i])
    if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
        redis.call('zrem', KEYS[2], values[i])
        redis.call('zrem', KEYS[1], values[i])
        removed = removed + 1
    end
end
return removed`;

export class RTimeSeries<V = any> extends RedisDataStructure {
  private ttlKey: string;
  private seqKey: string;
  private keyspaceHub: KeyspaceListenerHub | null = null;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.ttlKey = prefixName('redisson__ts_ttl', name);
    this.seqKey = prefixName('redisson__ts_seq', name);
  }
  private hub(): KeyspaceListenerHub {
    if (this.keyspaceHub === null) {
      this.keyspaceHub = new KeyspaceListenerHub(this.redis, this.name, {
        onAdd: 'zadd',
        onRemove: 'zrem',
      });
    }
    return this.keyspaceHub;
  }
  /** Java `addListener(ObjectListener)` (ScoredSortedSet add/remove keyspace
   *  events for this series; requires server `notify-keyspace-events`). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.hub().add(listener);
  }
  async removeListener(id: number): Promise<void> {
    return this.hub().remove(id);
  }
  async removeAllListeners(): Promise<void> {
    return this.hub().removeAll();
  }
  async add(timestamp: number, value: V, ttlMs = 0, label = ''): Promise<void> {
    const now = await serverNowMs(this.redis);
    const exp = ttlMs > 0 ? now + ttlMs : now + 365 * 100 * 24 * 3600 * 1000;
    const s = `local previous = nil
local function nextId()
    if previous == nil then
        previous = redis.call('get', KEYS[3])
        if previous == false then
            previous = '0'
        end
        previous = string.rep('0', tonumber(ARGV[1]) - string.len(previous)) .. previous
    end
    previous = previous + 1
    redis.call('set', KEYS[3], previous)
    return previous
end
local id = nextId()
local val = struct.pack('BBc0Lc0Lc0', ARGV[4], string.len(id), id, string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6])
redis.call('zadd', KEYS[1], ARGV[3], val)
redis.call('zadd', KEYS[2], ARGV[2], val)`;
    const mark = label ? 3 : 2;
    // NOTE: the label blob is mark byte + CODEC-ENCODED label (Java packs the
    // encoded form, e.g. `"jlbl"` with quotes). Bare labels break Java reads.
    const encLabel = label ? this.encodeValue(label) : '';
    try {
      await luaEval(this.redis, s, 3, this.name, this.ttlKey, this.seqKey,
        String(TS_NEXT_ID_WIDTH), String(exp), String(timestamp), String(mark), this.encodeValue(value), encLabel);
    } catch {
      // fallback (mock / no struct): store packed member as Buffer
      const seq = toNumber(await this.redis.get(this.seqKey), 0) + 1;
      await this.redis.set(this.seqKey, String(seq));
      const idStr = String(seq).padStart(TS_NEXT_ID_WIDTH, '0');
      const member = Buffer.from(packTimeSeriesMember(idStr, this.encodeValue(value), encLabel), 'latin1');
      await this.redis.zadd(this.name, String(timestamp), member);
      await this.redis.zadd(this.ttlKey, String(exp), member);
    }
  }
  /** Java `addAll(Collection<TimeSeriesEntry>[, Duration])`: one atomic Lua
   *  pass (sequence ids + both ZSETs); mock falls back to per-entry add(). */
  async addAll(entries: TimeSeriesEntry<V>[], ttlMs = 0): Promise<void> {
    if (entries.length === 0) return;
    const now = await serverNowMs(this.redis);
    const exp = ttlMs > 0 ? now + ttlMs : now + 365 * 100 * 24 * 3600 * 1000;
    const args: string[] = [String(TS_NEXT_ID_WIDTH), String(exp)];
    for (const e of entries) {
      args.push(String(e.timestamp), this.encodeValue(e.value), e.label ? this.encodeValue(e.label) : '');
    }
    try {
      await luaEval(this.redis, TS_ADD_ALL, 3, this.name, this.ttlKey, this.seqKey, ...args);
    } catch {
      for (const e of entries) await this.add(e.timestamp, e.value, ttlMs, e.label ?? '');
    }
  }
  private decodeTriples(res: any[]): TimeSeriesEntry<V>[] {
    const out: TimeSeriesEntry<V>[] = [];
    for (let i = 0; i + 2 < res.length; i += 3) {
      let value: V;
      try {
        value = this.decodeValue(String(res[i + 1])) as V;
      } catch {
        value = res[i + 1] as V;
      }
      let label = '';
      if (res[i + 2]) {
        try {
          const d = this.decodeValue(String(res[i + 2]));
          label = typeof d === 'string' ? d : String(res[i + 2]);
        } catch {
          label = String(res[i + 2]);
        }
      }
      out.push({ timestamp: Number(res[i]), value, label });
    }
    return out;
  }
  /** Raw entries in score order with TTL filtering; `limit <= 0` = unlimited. */
  private async rawEntries(from: number | string, to: number | string, reverse: boolean, limit: number): Promise<TimeSeriesEntry<V>[]> {
    const now = await serverNowMs(this.redis);
    // BYSCORE REV takes (max, min); callers always pass ascending bounds.
    const args: (string | number)[] = [this.name, reverse ? String(to) : String(from), reverse ? String(from) : String(to), 'BYSCORE'];
    if (reverse) args.push('REV');
    args.push('WITHSCORES');
    let raw: any[];
    try {
      raw = await (this.redis as any).zrangeBuffer(...args);
    } catch {
      raw = await (this.redis as any).zrange(...args);
    }
    const out: TimeSeriesEntry<V>[] = [];
    for (let i = 0; i + 1 < raw.length; i += 2) {
      const entry = await this.decodeMember(raw[i], raw[i + 1], now);
      if (entry === null) continue;
      out.push(entry);
      if (limit > 0 && out.length >= limit) break;
    }
    return out;
  }
  /** Decode one packed member, dropping expired entries (TTL ZSET). */
  private async decodeMember(rawMember: any, rawScore: any, now: number): Promise<TimeSeriesEntry<V> | null> {
    const member = Buffer.isBuffer(rawMember) ? rawMember : Buffer.from(String(rawMember), 'latin1');
    let exp: any = null;
    try {
      exp = await this.redis.zscore(this.ttlKey, member as any);
    } catch {
      exp = await this.redis.zscore(this.ttlKey, member.toString('latin1'));
    }
    if (exp !== null && exp !== undefined && Number(exp) <= now) return null;
    const u = unpackTimeSeriesMember(member);
    let value: V = member as unknown as V;
    let label = '';
    if (u) {
      try {
        value = this.decodeValue(u.val) as V;
      } catch {
        value = u.val as unknown as V;
      }
      if (u.label !== '') {
        try {
          const d = this.decodeValue(u.label);
          label = typeof d === 'string' ? d : u.label;
        } catch {
          label = u.label;
        }
      }
    } else {
      try {
        value = this.decodeValue(member.toString('utf8')) as V;
      } catch { /* raw */ }
    }
    const score = Buffer.isBuffer(rawScore) ? rawScore.toString() : String(rawScore);
    return { timestamp: Number(score), value, label };
  }
  /** Index page over the data ZSET (used by the lazy stream/iterator). */
  private async rawPage(offset: number, count: number): Promise<{ entries: TimeSeriesEntry<V>[]; rawCount: number }> {
    const now = await serverNowMs(this.redis);
    const args: (string | number)[] = [this.name, offset, offset + count - 1, 'WITHSCORES'];
    let raw: any[];
    try {
      raw = await (this.redis as any).zrangeBuffer(...args);
    } catch {
      raw = await (this.redis as any).zrange(...args);
    }
    const rawCount = Math.floor(raw.length / 2);
    const entries: TimeSeriesEntry<V>[] = [];
    for (let i = 0; i + 1 < raw.length; i += 2) {
      const e = await this.decodeMember(raw[i], raw[i + 1], now);
      if (e !== null) entries.push(e);
    }
    return { entries, rawCount };
  }
  /** Java `stream()` / `stream(int count)`: lazily yield VALUES in ascending
   *  timestamp order, `count` entries per page. */
  async *stream(count = 100): AsyncGenerator<V> {
    const batch = count > 0 ? count : 100;
    let offset = 0;
    for (;;) {
      const { entries, rawCount } = await this.rawPage(offset, batch);
      for (const e of entries) yield e.value;
      if (rawCount < batch) return;
      offset += rawCount;
    }
  }
  /** Java `iterator(int count)`: alias of `stream(count)` (values). */
  async *iterator(count = 100): AsyncGenerator<V> {
    yield* this.stream(count);
  }
  /** Unexpired entries in [from, to] ascending (Java `entryRange`; also the
   *  historical TS `range` — entries, not values). */
  async range(from: number, to: number, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return this.rawEntries(from, to, false, limit);
  }
  /** Unexpired entries descending. */
  async rangeReversed(from: number, to: number, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return this.rawEntries(from, to, true, limit);
  }
  /** Java `entryRange(from, to[, limit])` alias (entries). */
  async entryRange(from: number, to: number, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return this.range(from, to, limit);
  }
  /** Java `entryRangeReversed(from, to[, limit])` alias (entries). */
  async entryRangeReversed(from: number, to: number, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return this.rangeReversed(from, to, limit);
  }
  /** Entry at exactly `timestamp` (Java `getEntry`). */
  async getEntry(timestamp: number): Promise<TimeSeriesEntry<V> | null> {
    const r = await this.range(timestamp, timestamp);
    return r.length === 0 ? null : r[0];
  }
  /** Back-compat TS `get`: returns the entry (Java `get` returns the value —
   *  use `getValue` for that shape). */
  async get(timestamp: number): Promise<TimeSeriesEntry<V> | null> {
    return this.getEntry(timestamp);
  }
  /** Java `get(long)`: the value only. */
  async getValue(timestamp: number): Promise<V | null> {
    const e = await this.getEntry(timestamp);
    return e === null ? null : e.value;
  }
  async getAndRemoveEntry(timestamp: number): Promise<TimeSeriesEntry<V> | null> {
    const now = await serverNowMs(this.redis);
    try {
      const res: any = await luaEval(this.redis, TS_GET_AND_REMOVE, 2, this.name, this.ttlKey, String(now), String(timestamp));
      if (!Array.isArray(res) || res.length < 3) return null;
      return this.decodeTriples(res)[0] ?? null;
    } catch {
      const e = await this.getEntry(timestamp);
      if (e !== null) await this.remove(timestamp);
      return e;
    }
  }
  async getAndRemove(timestamp: number): Promise<V | null> {
    const e = await this.getAndRemoveEntry(timestamp);
    return e === null ? null : e.value;
  }
  /** Remove every entry at exactly `timestamp` (both ZSETs). */
  async remove(timestamp: number): Promise<boolean> {
    let members: any[];
    try {
      members = await (this.redis as any).zrangeBuffer(this.name, String(timestamp), String(timestamp), 'BYSCORE');
    } catch {
      members = await (this.redis as any).zrange(this.name, String(timestamp), String(timestamp), 'BYSCORE');
    }
    if (!members || members.length === 0) return false;
    const plain = members.map((m: any) => (Buffer.isBuffer(m) ? m.toString('latin1') : String(m)));
    try {
      await this.redis.zrem(this.name, ...members);
    } catch {
      await this.redis.zrem(this.name, ...plain);
    }
    try {
      await this.redis.zrem(this.ttlKey, ...members);
    } catch {
      await this.redis.zrem(this.ttlKey, ...plain);
    }
    return true;
  }
  /** Remove live entries in [from, to]; returns the removed count. */
  async removeRange(from: number, to: number): Promise<number> {
    const now = await serverNowMs(this.redis);
    try {
      return toNumber(await luaEval(this.redis, TS_REMOVE_RANGE, 2, this.name, this.ttlKey, String(now), String(from), String(to)));
    } catch {
      const entries = await this.rawEntries(from, to, false, 0);
      let n = 0;
      for (const e of entries) if (await this.remove(e.timestamp)) n += 1;
      return n;
    }
  }
  private async headTail(reverse: boolean, count: number, poll: boolean): Promise<TimeSeriesEntry<V>[]> {
    if (count === 0) return [];
    const now = await serverNowMs(this.redis);
    try {
      const res: any = await luaEval(this.redis, TS_HEAD_TAIL, 2, this.name, this.ttlKey,
        String(now), reverse ? '1' : '0', String(count), poll ? '1' : '0');
      return this.decodeTriples(Array.isArray(res) ? res : []);
    } catch {
      const entries = await this.rawEntries(0, '+inf', reverse, count < 0 ? 0 : count);
      if (poll) for (const e of entries) await this.remove(e.timestamp);
      return entries;
    }
  }
  async firstEntry(): Promise<TimeSeriesEntry<V> | null> {
    const e = await this.headTail(false, 1, false);
    return e.length === 0 ? null : e[0];
  }
  async lastEntry(): Promise<TimeSeriesEntry<V> | null> {
    const e = await this.headTail(true, 1, false);
    return e.length === 0 ? null : e[0];
  }
  async firstTimestamp(): Promise<number | null> {
    const e = await this.firstEntry();
    return e === null ? null : e.timestamp;
  }
  async lastTimestamp(): Promise<number | null> {
    const e = await this.lastEntry();
    return e === null ? null : e.timestamp;
  }
  async firstEntries(count: number): Promise<TimeSeriesEntry<V>[]> {
    return this.headTail(false, count, false);
  }
  async lastEntries(count: number): Promise<TimeSeriesEntry<V>[]> {
    const e = await this.headTail(true, count, false);
    return e.reverse();
  }
  async pollFirstEntries(count: number): Promise<TimeSeriesEntry<V>[]> {
    if (count <= 0) return [];
    return this.headTail(false, count, true);
  }
  async pollLastEntries(count: number): Promise<TimeSeriesEntry<V>[]> {
    if (count <= 0) return [];
    const e = await this.headTail(true, count, true);
    return e.reverse();
  }
  async first(): Promise<V | null>;
  async first(count: number): Promise<V[]>;
  async first(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const e = await this.firstEntry();
      return e === null ? null : e.value;
    }
    return (await this.firstEntries(count)).map((e) => e.value);
  }
  async last(): Promise<V | null>;
  async last(count: number): Promise<V[]>;
  async last(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const e = await this.lastEntry();
      return e === null ? null : e.value;
    }
    return (await this.lastEntries(count)).map((e) => e.value);
  }
  async pollFirstEntry(): Promise<TimeSeriesEntry<V> | null> {
    const e = await this.pollFirstEntries(1);
    return e.length === 0 ? null : e[0];
  }
  async pollLastEntry(): Promise<TimeSeriesEntry<V> | null> {
    const e = await this.pollLastEntries(1);
    return e.length === 0 ? null : e[0];
  }
  async pollFirst(): Promise<V | null>;
  async pollFirst(count: number): Promise<V[]>;
  async pollFirst(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const e = await this.pollFirstEntry();
      return e === null ? null : e.value;
    }
    return (await this.pollFirstEntries(count)).map((e) => e.value);
  }
  async pollLast(): Promise<V | null>;
  async pollLast(count: number): Promise<V[]>;
  async pollLast(count?: number): Promise<V | null | V[]> {
    if (count === undefined) {
      const e = await this.pollLastEntry();
      return e === null ? null : e.value;
    }
    return (await this.pollLastEntries(count)).map((e) => e.value);
  }
  async size(): Promise<number> {
    const total = await this.redis.zcard(this.name);
    const now = await serverNowMs(this.redis);
    let expired = 0;
    try {
      expired = await this.redis.zcount(this.ttlKey, '0', String(now));
    } catch { /* noop */ }
    return Math.max(0, total - expired);
  }
  override async delete(): Promise<boolean> {
    await this.redis.del(this.name, this.ttlKey, this.seqKey);
    return true;
  }
  override async unlink(): Promise<boolean> {
    await this.unlinkKeys([this.name, this.ttlKey, this.seqKey]);
    return true;
  }
}

// -- RateLimiter --------------------------------------------------------------------

export type RateType = 'OVERALL' | 'PER_CLIENT';

const RATE_SET = `if ARGV[4] == '1' and redis.call('exists', KEYS[1]) == 0 then
    return 0
end
local valueName = KEYS[2]
local permitsName = KEYS[4]
if ARGV[3] == '1' then
    valueName = KEYS[3]
    permitsName = KEYS[5]
end
local oldType = redis.call('hget', KEYS[1], 'type')
local keepAlive = tonumber(ARGV[5])
if keepAlive == nil then keepAlive = 0 end
redis.call('hset', KEYS[1], 'rate', ARGV[1], 'interval', ARGV[2], 'type', ARGV[3], 'keepAliveTime', keepAlive)
if oldType ~= false and oldType ~= ARGV[3] then
    redis.call('del', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
else
    redis.call('del', valueName, permitsName)
end
if keepAlive > 0 then
    redis.call('pexpire', KEYS[1], keepAlive)
    redis.call('pexpire', valueName, keepAlive)
    redis.call('pexpire', permitsName, keepAlive)
end
return 1`;

const RATE_ACQUIRE = `local now = tonumber(ARGV[1])
local interval = tonumber(ARGV[2])
local rate = tonumber(ARGV[3])
local permits = tonumber(ARGV[5])
local expired = redis.call('zrangebyscore', KEYS[2], 0, now - interval)
local returned = 0
for i = 1, #expired do
    local rnd, p = struct.unpack('Bc0I', expired[i])
    returned = returned + p
end
if returned > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, now - interval)
end
local value = redis.call('get', KEYS[1])
if value == false then
    value = rate
else
    value = tonumber(value) + returned
end
if value >= permits then
    local member = struct.pack('Bc0I', string.len(ARGV[4]), ARGV[4], permits)
    redis.call('zadd', KEYS[2], now, member)
    redis.call('set', KEYS[1], value - permits)
    redis.call('pexpire', KEYS[2], interval * 2)
    local keepAliveTime = redis.call('hget', KEYS[3], 'keepAliveTime')
    if keepAliveTime ~= false and tonumber(keepAliveTime) > 0 then
        redis.call('pexpire', KEYS[3], keepAliveTime)
        redis.call('pexpire', KEYS[1], keepAliveTime)
        redis.call('pexpire', KEYS[2], keepAliveTime)
    else
        local ttl = redis.call('pttl', KEYS[3])
        if ttl > 0 then
            redis.call('pexpire', KEYS[1], ttl)
            redis.call('pexpire', KEYS[2], ttl)
        end
    end
    return 1
end
redis.call('set', KEYS[1], value)
return 0`;

export class RRateLimiter extends RedisDataStructure {
  private rate = 0;
  private interval = 0;
  private keepAlive = 0;
  private rtype: RateType = 'OVERALL';
  private loaded = false;
  private clientTag: string;
  constructor(redis: Redis, name: string, clientTag = '') {
    super(redis, name, new JsonCodec());
    // Java PER_CLIENT shares one window per RedissonClient instance. The
    // factory passes the client id; direct construction falls back to a
    // per-process tag (never the degenerate '' suffix).
    this.clientTag = clientTag || processRateTag();
  }
  private valueKey(): string {
    return this.rtype === 'PER_CLIENT' ? suffixName(this.name, 'value') + ':' + this.clientTag : suffixName(this.name, 'value');
  }
  private permitsKey(): string {
    return this.rtype === 'PER_CLIENT' ? suffixName(this.name, 'permits') + ':' + this.clientTag : suffixName(this.name, 'permits');
  }
  private rateKeys(): [string, string, string, string, string] {
    return [this.name, suffixName(this.name, 'value'), suffixName(this.name, 'value') + ':' + this.clientTag,
      suffixName(this.name, 'permits'), suffixName(this.name, 'permits') + ':' + this.clientTag];
  }
  private async ensureConfig(): Promise<void> {
    if (this.loaded) return;
    const m = await this.redis.hgetall(this.name);
    if (m && Object.keys(m).length > 0) {
      this.rate = toNumber(m['rate']);
      this.interval = toNumber(m['interval']);
      this.keepAlive = toNumber(m['keepAliveTime']);
      this.rtype = m['type'] === '1' ? 'PER_CLIENT' : 'OVERALL';
    }
    this.loaded = true;
  }
  async trySetRate(type: RateType, rate: number, intervalMs: number, keepAliveMs = 0): Promise<boolean> {
    await this.ensureConfig();
    if (this.rate > 0 || this.interval > 0) return false;
    await this.setRate(type, rate, intervalMs, keepAliveMs);
    return true;
  }
  async setRate(type: RateType, rate: number, intervalMs: number, keepAliveMs = 0): Promise<void> {
    const t = type === 'PER_CLIENT' ? '1' : '0';
    try {
      await luaEval(this.redis, RATE_SET, 5, ...this.rateKeys(), String(rate), String(intervalMs), t, '0', String(keepAliveMs));
    } catch {
      await this.redis.hset(this.name, 'rate', String(rate), 'interval', String(intervalMs), 'type', t, 'keepAliveTime', String(keepAliveMs));
    }
    this.rate = rate;
    this.interval = intervalMs;
    this.keepAlive = keepAliveMs;
    this.rtype = type;
    this.loaded = true;
  }
  async updateRate(type: RateType, rate: number, intervalMs: number): Promise<boolean> {
    const t = type === 'PER_CLIENT' ? '1' : '0';
    try {
      const n = toNumber(await luaEval(this.redis, RATE_SET, 5, ...this.rateKeys(), String(rate), String(intervalMs), t, '1', String(this.keepAlive)));
      if (n === 0) return false;
    } catch {
      if (!await this.isExists()) return false;
    }
    this.rate = rate;
    this.interval = intervalMs;
    this.rtype = type;
    this.loaded = true;
    return true;
  }
  async tryAcquire(permits = 1): Promise<boolean> {
    await this.ensureConfig();
    if (this.rate <= 0) return true;
    const now = await serverNowMs(this.redis);
    // hex text member id (same domain as Java's random ids): latin1 would
    // corrupt on the UTF-8 wire and collapse ZADD dedup. Length differs from
    // Java's 16 raw bytes but struct.unpack reads the len prefix dynamically.
    const id = randomBytes(16).toString('hex');
    void packRatePermit;
    try {
      const n = toNumber(await luaEval(this.redis, RATE_ACQUIRE, 3, this.valueKey(), this.permitsKey(), this.name,
        String(now), String(this.interval), String(this.rate), id, String(permits)));
      // The 4.6.1 script renews keepAlive itself (same atomic write); no extra round trip.
      return n === 1;
    } catch {
      // mock fallback: token bucket via counter
      const key = this.valueKey();
      const cur = await this.redis.get(key);
      const avail = cur === null ? this.rate : Number(cur);
      if (avail >= permits) {
        await this.redis.set(key, String(avail - permits));
        await this.refreshKeepAlive();
        return true;
      }
      return false;
    }
  }
  /** Java `keepAliveTime`: every successful acquisition extends the TTL of the
   *  config HASH and of both active value/permits keys (mirrors redi.go). */
  private async refreshKeepAlive(): Promise<void> {
    if (this.keepAlive <= 0) return;
    try {
      await this.redis.pexpire(this.name, this.keepAlive);
      await this.redis.pexpire(this.valueKey(), this.keepAlive);
      await this.redis.pexpire(this.permitsKey(), this.keepAlive);
    } catch { /* noop */ }
  }
  async acquire(permits = 1): Promise<void> {
    for (;;) {
      if (await this.tryAcquire(permits)) return;
      await new Promise((r) => setTimeout(r, 50));
    }
  }
  async availablePermits(): Promise<number> {
    await this.ensureConfig();
    if (this.rate <= 0) return 0;
    const v = await this.redis.get(this.valueKey());
    if (v === null) return this.rate;
    return Math.max(0, Number(v));
  }
  async getConfig(): Promise<{ rate: number; intervalMs: number; rateType: RateType; keepAliveMs: number }> {
    const m = await this.redis.hgetall(this.name);
    return {
      rate: toNumber(m['rate']),
      intervalMs: toNumber(m['interval']),
      rateType: m['type'] === '1' ? 'PER_CLIENT' : 'OVERALL',
      keepAliveMs: toNumber(m['keepAliveTime']),
    };
  }
  async release(permits = 1): Promise<void> {
    await this.ensureConfig();
    if (this.rate <= 0 || permits <= 0) return;
    const s = `local rate = tonumber(ARGV[1])
local current = redis.call('get', KEYS[1])
if current == false then
    current = rate
else
    current = tonumber(current)
end
local newValue = current + tonumber(ARGV[2])
if newValue > rate then
    newValue = rate
end
redis.call('set', KEYS[1], newValue)`;
    try {
      await luaEval(this.redis, s, 1, this.valueKey(), String(this.rate), String(permits));
    } catch {
      const cur = toNumber(await this.redis.get(this.valueKey()), this.rate);
      await this.redis.set(this.valueKey(), String(Math.min(this.rate, cur + permits)));
    }
  }
  override async delete(): Promise<boolean> {
    await this.redis.del(this.name, this.valueKey(), this.permitsKey());
    this.loaded = false;
    return true;
  }
  override async unlink(): Promise<boolean> {
    await this.unlinkKeys([this.name, this.valueKey(), this.permitsKey()]);
    this.loaded = false;
    return true;
  }
}

// -- Native probabilistic facades (BF/CF/TOPK/TDIGEST/GCRA) -------------------------

async function tryDo(redis: Redis, ...args: (string | number)[]): Promise<any> {
  try {
    return await (redis as any).call(...args as string[]);
  } catch (e: any) {
    const msg = String(e?.message ?? e);
    if (msg.includes('unknown command') || msg.includes('wrong number')) throw e;
    throw e;
  }
}

export class RBloomFilterNative extends RedisDataStructure {
  async reserve(capacity: number, errorRate: number): Promise<void> {
    await tryDo(this.redis, 'BF.RESERVE', this.name, errorRate, capacity);
  }
  async add(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'BF.ADD', this.name, element)) === 1;
  }
  async exists(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'BF.EXISTS', this.name, element)) === 1;
  }
}

export class RCuckooFilter extends RedisDataStructure {
  async reserve(capacity: number): Promise<void> {
    await tryDo(this.redis, 'CF.RESERVE', this.name, capacity);
  }
  /** Java `init(long)`: CF.RESERVE. */
  async init(capacity: number): Promise<void> {
    return this.reserve(capacity);
  }
  async add(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.ADD', this.name, element)) === 1;
  }
  /** Java `addIfAbsent(V)`: CF.ADDNX. */
  async addIfAbsent(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.ADDNX', this.name, element)) === 1;
  }
  async exists(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.EXISTS', this.name, element)) === 1;
  }
  /** Java `exists(Collection)`: CF.MEXISTS → the subset that exists. */
  async existsMany(elements: string[]): Promise<string[]> {
    if (elements.length === 0) return [];
    const r: any = await tryDo(this.redis, 'CF.MEXISTS', this.name, ...elements);
    const arr = (r ?? []) as any[];
    return elements.filter((_, i) => toNumber(arr[i]) === 1);
  }
  async remove(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.DEL', this.name, element)) === 1;
  }
  /** Java `count(V)`: CF.COUNT. */
  async count(element: string): Promise<number> {
    return toNumber(await tryDo(this.redis, 'CF.COUNT', this.name, element));
  }
  /** Java `getInfo()`: CF.INFO flat field/value record. */
  async getInfo(): Promise<Record<string, any>> {
    return flatRecord(await tryDo(this.redis, 'CF.INFO', this.name));
  }
}

export class RTopK extends RedisDataStructure {
  async reserve(k: number): Promise<void> {
    await tryDo(this.redis, 'TOPK.RESERVE', this.name, k);
  }
  /** Java `init(int)` / `init(TopKInitArgs)`: TOPK.RESERVE with optional
   *  width/depth/decay. */
  async init(k: number, width?: number, depth?: number, decay?: number): Promise<void> {
    if (width === undefined) return this.reserve(k);
    const args: (string | number)[] = [this.name, k, width];
    if (depth !== undefined) args.push(depth);
    if (decay !== undefined) args.push(String(decay));
    await tryDo(this.redis, 'TOPK.RESERVE', ...args);
  }
  /** Java `add(V...)`: returns the evicted item per input (`null` when none). */
  async add(...elements: string[]): Promise<(string | null)[]> {
    if (elements.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TOPK.ADD', this.name, ...elements);
    return ((r ?? []) as any[]).map((x) => (x === null || x === undefined ? null : String(x)));
  }
  /** Java `incrementBy(V, int)`: TOPK.INCRBY → evicted item or null. */
  async incrementBy(element: string, increment: number): Promise<string | null> {
    const r: any = await tryDo(this.redis, 'TOPK.INCRBY', this.name, element, increment);
    const v = Array.isArray(r) ? r[0] : r;
    return v === null || v === undefined ? null : String(v);
  }
  async contains(element: string): Promise<boolean> {
    const r: any = await tryDo(this.redis, 'TOPK.QUERY', this.name, element);
    return toNumber(Array.isArray(r) ? r[0] : r) === 1;
  }
  async count(element: string): Promise<number> {
    const r: any = await tryDo(this.redis, 'TOPK.COUNT', this.name, element);
    return toNumber(Array.isArray(r) ? r[0] : r);
  }
  async list(): Promise<string[]> {
    return (await tryDo(this.redis, 'TOPK.LIST', this.name)) as string[];
  }
  /** Java `listWithCount()`: TOPK.LIST ... WITHCOUNT → item→count. */
  async listWithCount(): Promise<Map<string, number>> {
    const r: any = await tryDo(this.redis, 'TOPK.LIST', this.name, 'WITHCOUNT');
    const out = new Map<string, number>();
    const arr = (r ?? []) as any[];
    for (let i = 0; i + 1 < arr.length; i += 2) out.set(String(arr[i]), Number(arr[i + 1]));
    return out;
  }
  async getInfo(): Promise<Record<string, any>> {
    return flatRecord(await tryDo(this.redis, 'TOPK.INFO', this.name));
  }
}

export class RTDigest extends RedisDataStructure {
  /** Java `create([int compression])`: TDIGEST.CREATE [COMPRESSION n]. */
  async create(compression?: number): Promise<void> {
    const args: (string | number)[] = [this.name];
    if (compression !== undefined) args.push('COMPRESSION', compression);
    await tryDo(this.redis, 'TDIGEST.CREATE', ...args);
  }
  async add(...values: number[]): Promise<void> {
    if (values.length === 0) return;
    await tryDo(this.redis, 'TDIGEST.ADD', this.name, ...values.map(String));
  }
  /** Java `mergeWith(String...)`: TDIGEST.MERGE dest numkeys src... */
  async mergeWith(...names: string[]): Promise<void> {
    await tryDo(this.redis, 'TDIGEST.MERGE', this.name, names.length, ...names);
  }
  async quantile(...qs: number[]): Promise<number[]> {
    if (qs.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.QUANTILE', this.name, ...qs.map(String));
    return (r as any[]).map(Number);
  }
  async getMin(): Promise<number> {
    return Number(await tryDo(this.redis, 'TDIGEST.MIN', this.name));
  }
  async getMax(): Promise<number> {
    return Number(await tryDo(this.redis, 'TDIGEST.MAX', this.name));
  }
  async cumulativeProbability(...values: number[]): Promise<number[]> {
    if (values.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.CDF', this.name, ...values.map(String));
    return (r as any[]).map(Number);
  }
  async trimmedMean(low = 0, high = 1): Promise<number> {
    return Number(await tryDo(this.redis, 'TDIGEST.TRIMMED_MEAN', this.name, String(low), String(high)));
  }
  async rank(...values: number[]): Promise<number[]> {
    if (values.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.RANK', this.name, ...values.map(String));
    return (r as any[]).map(Number);
  }
  async revRank(...values: number[]): Promise<number[]> {
    if (values.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.REVRANK', this.name, ...values.map(String));
    return (r as any[]).map(Number);
  }
  async byRank(...ranks: number[]): Promise<number[]> {
    if (ranks.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.BYRANK', this.name, ...ranks.map(String));
    return (r as any[]).map(Number);
  }
  async byRevRank(...ranks: number[]): Promise<number[]> {
    if (ranks.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.BYREVRANK', this.name, ...ranks.map(String));
    return (r as any[]).map(Number);
  }
  async getInfo(): Promise<Record<string, any>> {
    return flatRecord(await tryDo(this.redis, 'TDIGEST.INFO', this.name));
  }
  async reset(): Promise<void> {
    await tryDo(this.redis, 'TDIGEST.RESET', this.name);
  }
}

export class RGcra extends RedisDataStructure {
  async acquire(limit: number, periodMs: number): Promise<boolean> {
    try {
      const r: any = await tryDo(this.redis, 'GCRA', this.name, limit, periodMs);
      return toNumber(r) === 1 || r === 'OK';
    } catch {
      return true;
    }
  }
}
