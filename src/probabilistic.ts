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
    // GEORADIUS/GEORADIUSBYMEMBER need the STORE/STOREDIST token before the
    // destination key (GEOSEARCHSTORE takes the dest positionally instead)
    if (hasBox) {
      if (sorted) params.push('STOREDIST');
    } else if (sorted) {
      params.push('STOREDIST', destName);
    } else {
      params.push('STORE', destName);
    }
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

// -- TimeSeries 4.8 expansion: args builders + result types (Java
// org.redisson.api.ts.*) ---------------------------------------------------------

/** Java 4.8 `TimeSeriesAggregation` enum (wire codes are ordinal + 1). */
export type TimeSeriesAggregation =
  | 'COUNT'
  | 'SUM'
  | 'AVG'
  | 'MIN'
  | 'MAX'
  | 'VALUE_RANGE'
  | 'FIRST'
  | 'LAST'
  | 'STD_DEV_POPULATION'
  | 'STD_DEV_SAMPLE'
  | 'VARIANCE_POPULATION'
  | 'VARIANCE_SAMPLE';

const TS_AGG_CODES: Record<TimeSeriesAggregation, number> = {
  COUNT: 1,
  SUM: 2,
  AVG: 3,
  MIN: 4,
  MAX: 5,
  VALUE_RANGE: 6,
  FIRST: 7,
  LAST: 8,
  STD_DEV_POPULATION: 9,
  STD_DEV_SAMPLE: 10,
  VARIANCE_POPULATION: 11,
  VARIANCE_SAMPLE: 12,
};

/**
 * Java 4.8 `TimeSeriesAddArgs`: `TimeSeriesAddArgs.entry(ts, v)` builder with
 * `.label(l)` / `.timeToLive(ms)` (alias `.ttl(ms)`) / `.retention(ms)`
 * chaining. Durations are milliseconds (the repo's convention).
 */
export class TimeSeriesAddArgs<V = any> {
  private labelValue?: any;
  private retentionMsValue = 0;
  private timeToLiveMsValue = 0;
  constructor(readonly timestamp: number, readonly object: V) {}
  static entry<V = any>(timestamp: number, object: V): TimeSeriesAddArgs<V>;
  static entry<V = any>(timestamp: number, object: V, label: any): TimeSeriesAddArgs<V>;
  static entry<V = any>(timestamp: number, object: V, label?: any): TimeSeriesAddArgs<V> {
    const a = new TimeSeriesAddArgs<V>(timestamp, object);
    return label === undefined ? a : a.label(label);
  }
  label(label: any): this {
    this.labelValue = label;
    return this;
  }
  /** Java `retention(Duration)` — ms; entries further than this behind the
   *  highest timestamp held are dropped when this entry is added, and an
   *  entry outside its own window is not added at all. */
  retention(ms: number): this {
    this.retentionMsValue = ms;
    return this;
  }
  /** Java `timeToLive(Duration)` — ms of per-entry TTL (0/undefined = century). */
  timeToLive(ms: number): this {
    this.timeToLiveMsValue = ms;
    return this;
  }
  /** Alias of `timeToLive(ms)`. */
  ttl(ms: number): this {
    return this.timeToLive(ms);
  }
  getLabel(): any {
    return this.labelValue;
  }
  getRetentionMs(): number {
    return this.retentionMsValue;
  }
  getTimeToLiveMs(): number {
    return this.timeToLiveMsValue;
  }
}

/**
 * Java 4.8 `TimeSeriesAggregationArgs`:
 * `TimeSeriesAggregationArgs.between(from, to).bucket(ms).count().sum()...`
 */
export class TimeSeriesAggregationArgs {
  private bucketMsValue?: number;
  private alignmentValue = 0;
  private valueFilteredValue = false;
  private minValueValue = 0;
  private maxValueValue = 0;
  private labelFilteredValue = false;
  private labelValue?: any;
  private readonly aggregationSet = new Set<TimeSeriesAggregation>();
  private constructor(readonly startTimestamp: number, readonly endTimestamp: number) {}
  static between(startTimestamp: number, endTimestamp: number): TimeSeriesAggregationArgs {
    return new TimeSeriesAggregationArgs(startTimestamp, endTimestamp);
  }
  bucket(ms: number): this {
    this.bucketMsValue = ms;
    return this;
  }
  alignTo(timestamp: number): this {
    this.alignmentValue = timestamp;
    return this;
  }
  filterByValue(min: number, max: number): this {
    this.valueFilteredValue = true;
    this.minValueValue = min;
    this.maxValueValue = max;
    return this;
  }
  /** Java `label(L)` — `null` selects the entries that carry no label. */
  label(label: any): this {
    this.labelFilteredValue = true;
    this.labelValue = label;
    return this;
  }
  aggregations(...values: TimeSeriesAggregation[]): this {
    for (const v of values) this.aggregationSet.add(v);
    return this;
  }
  count(): this {
    return this.aggregations('COUNT');
  }
  sum(): this {
    return this.aggregations('SUM');
  }
  avg(): this {
    return this.aggregations('AVG');
  }
  min(): this {
    return this.aggregations('MIN');
  }
  max(): this {
    return this.aggregations('MAX');
  }
  valueRange(): this {
    return this.aggregations('VALUE_RANGE');
  }
  first(): this {
    return this.aggregations('FIRST');
  }
  last(): this {
    return this.aggregations('LAST');
  }
  stdDevPopulation(): this {
    return this.aggregations('STD_DEV_POPULATION');
  }
  stdDevSample(): this {
    return this.aggregations('STD_DEV_SAMPLE');
  }
  variancePopulation(): this {
    return this.aggregations('VARIANCE_POPULATION');
  }
  varianceSample(): this {
    return this.aggregations('VARIANCE_SAMPLE');
  }
  getBucketMs(): number | undefined {
    return this.bucketMsValue;
  }
  getAlignment(): number {
    return this.alignmentValue;
  }
  isValueFiltered(): boolean {
    return this.valueFilteredValue;
  }
  getMinValue(): number {
    return this.minValueValue;
  }
  getMaxValue(): number {
    return this.maxValueValue;
  }
  isLabelFiltered(): boolean {
    return this.labelFilteredValue;
  }
  getLabel(): any {
    return this.labelValue;
  }
  getAggregations(): TimeSeriesAggregation[] {
    return [...this.aggregationSet];
  }
}

/**
 * Java 4.8 `TimeSeriesReadArgs`: `TimeSeriesReadArgs.after(ts).count(n)`
 * (non-blocking tail read; entries strictly after `ts`).
 */
export class TimeSeriesReadArgs {
  private countValue = 0;
  private labelFilteredValue = false;
  private labelValue?: any;
  private constructor(readonly timestamp: number) {}
  static after(timestamp: number): TimeSeriesReadArgs {
    return new TimeSeriesReadArgs(timestamp);
  }
  count(count: number): this {
    this.countValue = count;
    return this;
  }
  label(label: any): this {
    this.labelFilteredValue = true;
    this.labelValue = label;
    return this;
  }
  getCount(): number {
    return this.countValue;
  }
  isLabelFiltered(): boolean {
    return this.labelFilteredValue;
  }
  getLabel(): any {
    return this.labelValue;
  }
}

/** Java 4.8 `TimeSeriesBucket`: one aggregation bucket; aggregations that were
 *  not requested (or are undefined for the bucket, e.g. sample variance of a
 *  single value) are absent from `values`. */
export interface TimeSeriesBucket {
  timestamp: number;
  values: Partial<Record<TimeSeriesAggregation, number>>;
}

/** Java 4.8 `TimeSeriesInfo`: one-round-trip collection snapshot. */
export interface TimeSeriesInfo {
  /** Live (unexpired) entry count. */
  size: number;
  /** Stored entry count including expired-but-not-evicted ones. */
  totalEntries: number;
  firstTimestamp: number | null;
  lastTimestamp: number | null;
  memoryUsage: number;
  /** Whole-collection pttl ms; -1 no expiry, -2 missing. */
  timeToLive: number;
  /** Entry ids issued (grows monotonically; saturates at MAX_SAFE_INTEGER). */
  entriesIssued: number;
}

/** Smallest double strictly greater than `d` (Java `Math.nextUp`). */
function nextUpDouble(d: number): number {
  if (Number.isNaN(d)) return d;
  if (d === 0) return 5e-324;
  const buf = Buffer.alloc(8);
  buf.writeDoubleLE(d, 0);
  const bits = buf.readBigUInt64LE(0);
  if (d > 0) {
    if (d === Number.POSITIVE_INFINITY) return d;
    buf.writeBigUInt64LE(bits + 1n, 0);
  } else {
    if (d === Number.NEGATIVE_INFINITY) return -Number.MAX_VALUE;
    buf.writeBigUInt64LE(bits - 1n, 0);
  }
  return buf.readDoubleLE(0);
}

function tsReadNumber(text: string, timestamp: number): number {
  const n = Number(text);
  if (!Number.isFinite(n)) {
    throw new Error(`RTimeSeries value at timestamp ${timestamp} is not a finite number`);
  }
  return n;
}

const TS_WHOLE_RANGE_START = '-9223372036854775808';
const TS_WHOLE_RANGE_END = '9223372036854775807';

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

/** Java COLLECT_RANGE shape (server-side): collect live entries of a score
 *  window in ONE round trip — the `anyExpired` fast path (one ZCOUNT) skips
 *  the per-member expiry checks when nothing has expired; members are
 *  struct-unpacked inside the script so raw binary never crosses the wire
 *  as a UTF-8 string (label handling matches TS_GET_AND_REMOVE: n==4 mark-3
 *  strips the mark byte, n==2 carries no label). Reply: flat
 *  [score, value, label, …] triples. ARGV: [nowMs, from, to, reverse?1:0]. */
const TS_COLLECT_LIVE = `local anyExpired = redis.call('zcount', KEYS[2], '-inf', ARGV[1]) > 0
local values
if ARGV[4] ~= '0' then
    values = redis.call('zrevrangebyscore', KEYS[1], ARGV[3], ARGV[2])
else
    values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3])
end
local result = {}
for i = 1, #values do
    local live = true
    if anyExpired then
        local expirationDate = redis.call('zscore', KEYS[2], values[i])
        live = expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1])
    end
    if live then
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i])
        local lbl = ''
        if n == 4 then
            local mark = string.byte(label, 1)
            if mark == 3 then lbl = string.sub(label, 2) end
        elseif n ~= 2 then
            lbl = label
        end
        table.insert(result, redis.call('zscore', KEYS[1], values[i]))
        table.insert(result, val)
        table.insert(result, lbl)
    end
end
return result`;

/** Index-page twin of TS_COLLECT_LIVE for the lazy stream/iterator: live
 *  entries of ZRANGE [offset, offset+count-1] in one round trip. The reply
 *  LEADS with the raw member count of the window (live + expired) so the
 *  caller can advance its cursor; triples follow. ARGV: [nowMs, offset, end]. */
const TS_COLLECT_PAGE = `local anyExpired = redis.call('zcount', KEYS[2], '-inf', ARGV[1]) > 0
local values = redis.call('zrange', KEYS[1], ARGV[2], ARGV[3])
local result = {#values}
for i = 1, #values do
    local live = true
    if anyExpired then
        local expirationDate = redis.call('zscore', KEYS[2], values[i])
        live = expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1])
    end
    if live then
        local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i])
        local lbl = ''
        if n == 4 then
            local mark = string.byte(label, 1)
            if mark == 3 then lbl = string.sub(label, 2) end
        elseif n ~= 2 then
            lbl = label
        end
        table.insert(result, redis.call('zscore', KEYS[1], values[i]))
        table.insert(result, val)
        table.insert(result, lbl)
    end
end
return result`;

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

// -- TimeSeries 4.8 expansion: Lua building blocks, ported VERBATIM from
// RedissonTimeSeries.java (Redisson 4.8). KEYS[1] = data ZSET (raw name),
// KEYS[2] = ttl ZSET (redisson__ts_ttl:{name}), KEYS[3] = seq HASH
// (redisson__ts_seq:{name}); sequenceWidth = Java SEQUENCE_WIDTH = 20.
//
// WIRE NOTE: the 4.8 add scripts write the 4.8-native member shape (flag byte
// 4 + label flag as the first byte of the label field + zero-padded hash-seq
// id via NEXT_ID) — intended byte parity with Java 4.8 for these NEW APIs.
// The legacy add/addAll family above keeps the pre-4.8 flag 2/3 shape and its
// string-counter seq key; avoid mixing the two families on one series (the
// seq key type differs: HASH here, STRING there).

const TS48_DECODE_LABEL = `if n == 4 then
    local flag = string.byte(label, 1);
    if flag == 2 then
        label = 0;
    else
        label = string.sub(label, 2);
    end;
    n = flag;
elseif n == 2 then
    label = 0;
end; `;

const TS48_MATCHES_LABEL = `local matches = true;
if labelMode == '1' then
    matches = label == labelValue;
elseif labelMode == '2' then
    matches = label == 0;
end; `;

const TS48_NEXT_ID = `local sequenceWidth = tonumber(ARGV[1]);
local previous;
local function nextId()
    if previous == nil then
        previous = redis.call('hget', KEYS[3], 'id');
        if previous == false then
            previous = '0';
        end;
    end;
    local sequence = redis.call('hincrbyfloat', KEYS[3], 'id', 1);
    if sequence == previous or #sequence > sequenceWidth then
        error('RTimeSeries sequence overflow');
    end;
    previous = sequence;
    return string.rep('0', sequenceWidth - #sequence) .. sequence;
end; `;

const TS48_REMOVE_MEMBERS = `local function removeMembers(members)
    local from = 1;
    while from <= #members do
        local to = math.min(from + 499, #members);
        redis.call('zrem', KEYS[1], unpack(members, from, to));
        redis.call('zrem', KEYS[2], unpack(members, from, to));
        from = to + 1;
    end;
end; `;

const TS48_ENTRY_ARGS = `local timestamp = ARGV[4];
local labelFlag = ARGV[5];
local entryValue = ARGV[6];
local label = ARGV[7];
local expiration = ARGV[8]; `;

const TS48_STORE_ENTRY = `local function storeEntry(id, timestamp, value)
    local field = string.char(labelFlag) .. label;
    local packed = struct.pack('BBc0Lc0Lc0', 4,
                               string.len(id), id,
                               string.len(value), value,
                               string.len(field), field);
    redis.call('zadd', KEYS[1], timestamp, packed);
    redis.call('zadd', KEYS[2], expiration, packed);
end; `;

const TS48_RETENTION_CUTOFF = `local cutoff;
local retention = tonumber(ARGV[2]);
if retention > 0 then
    local highest = tonumber(timestamp);
    local probes = 0;
    while probes < 100 do
        local top = redis.call('zrevrange', KEYS[1], 0, 0, 'withscores');
        if #top == 0 then
            break;
        end;
        local expirationDate = redis.call('zscore', KEYS[2], top[1]);
        if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[3]) then
            local top1 = tonumber(top[2]);
            if top1 > highest then
                highest = top1;
            end;
            break;
        end;
        redis.call('zrem', KEYS[1], top[1]);
        redis.call('zrem', KEYS[2], top[1]);
        probes = probes + 1;
    end;
    cutoff = highest - retention;
end; `;

const TS48_WITHIN_RETENTION = 'if cutoff == nil or tonumber(timestamp) >= cutoff then ';

const TS48_RETENTION_TRIM = `if cutoff ~= nil then
    removeMembers(redis.call('zrangebyscore', KEYS[1], '-inf',
                              '(' .. string.format('%.0f', cutoff), 'limit', 0, 500));
end; `;

const TS48_FORMAT_NUMBER = `local function formatNumber(value)
    if value ~= value then
        return 'NaN';
    end;
    if value == math.huge then
        return 'Infinity';
    end;
    if value == -math.huge then
        return '-Infinity';
    end;
    local text = string.format('%.15g', value);
    if tonumber(text) ~= value then
        text = string.format('%.16g', value);
        if tonumber(text) ~= value then
            text = string.format('%.17g', value);
        end;
    end;
    return text;
end; `;

const TS48_READ_NUMBER = `local function readNumber(text, timestamp)
    local number = tonumber(text);
    if number == nil or number ~= number
            or number == math.huge or number == -math.huge then
        error('RTimeSeries value at timestamp ' .. timestamp
               .. ' is not a finite number');
    end;
    return number;
end; `;

const TS48_ADD_ENTRY = TS48_NEXT_ID + TS48_REMOVE_MEMBERS + TS48_ENTRY_ARGS +
    TS48_STORE_ENTRY + TS48_RETENTION_CUTOFF +
    'local added = 0; ' +
    TS48_WITHIN_RETENTION +
    'storeEntry(nextId(), timestamp, entryValue); ' +
    'added = 1; ' +
    'end; ' +
    TS48_RETENTION_TRIM +
    'return added;';

const TS48_ADD_IF_ABSENT = TS48_NEXT_ID + TS48_REMOVE_MEMBERS + TS48_ENTRY_ARGS +
    TS48_STORE_ENTRY + TS48_RETENTION_CUTOFF +
    'local now = tonumber(ARGV[3]); ' +
    'local added = 0; ' +
    TS48_WITHIN_RETENTION +
    'local occupied = false; ' +
    'local stale; ' +
    'local existing = redis.call(\'zrangebyscore\', KEYS[1], timestamp, timestamp); ' +
    'for j = 1, #existing do ' +
        'local expirationDate = redis.call(\'zscore\', KEYS[2], existing[j]); ' +
        'if expirationDate == false or tonumber(expirationDate) > now then ' +
            'occupied = true; ' +
        'else ' +
            'if stale == nil then ' +
                'stale = {}; ' +
            'end; ' +
            'table.insert(stale, existing[j]); ' +
        'end; ' +
    'end; ' +
    'if stale ~= nil then ' +
        'removeMembers(stale); ' +
    'end; ' +
    'if occupied == false then ' +
        'storeEntry(nextId(), timestamp, entryValue); ' +
        'added = 1; ' +
    'end; ' +
    'end; ' +
    TS48_RETENTION_TRIM +
    'return added;';

const TS48_ADD_OR_REPLACE = TS48_NEXT_ID + TS48_REMOVE_MEMBERS + TS48_ENTRY_ARGS +
    TS48_STORE_ENTRY + TS48_RETENTION_CUTOFF +
    'local now = tonumber(ARGV[3]); ' +
    'local created = 0; ' +
    TS48_WITHIN_RETENTION +
    'local replaced = false; ' +
    'local existing = redis.call(\'zrangebyscore\', KEYS[1], timestamp, timestamp); ' +
    'for j = 1, #existing do ' +
        'local expirationDate = redis.call(\'zscore\', KEYS[2], existing[j]); ' +
        'if expirationDate == false or tonumber(expirationDate) > now then ' +
            'replaced = true; ' +
        'end; ' +
    'end; ' +
    'if #existing > 0 then ' +
        'removeMembers(existing); ' +
    'end; ' +
    'storeEntry(nextId(), timestamp, entryValue); ' +
    'if replaced == false then ' +
        'created = 1; ' +
    'end; ' +
    'end; ' +
    TS48_RETENTION_TRIM +
    'return created;';

const TS48_ADD_COMPARING = TS48_NEXT_ID + TS48_REMOVE_MEMBERS + TS48_ENTRY_ARGS +
    TS48_STORE_ENTRY + TS48_RETENTION_CUTOFF + TS48_FORMAT_NUMBER + TS48_READ_NUMBER +
    'local now = tonumber(ARGV[3]); ' +
    'local policy = tonumber(ARGV[9]); ' +
    'local result = 0; ' +
    TS48_WITHIN_RETENTION +
    'local folded; ' +
    'local stale; ' +
    'local existing = redis.call(\'zrangebyscore\', KEYS[1], timestamp, timestamp); ' +
    'for j = 1, #existing do ' +
        'local expirationDate = redis.call(\'zscore\', KEYS[2], existing[j]); ' +
        'if expirationDate == false or tonumber(expirationDate) > now then ' +
            'local marker, id, val = struct.unpack(\'BBc0Lc0Lc0\', existing[j]); ' +
            'local number = readNumber(val, timestamp); ' +
            'if folded == nil then ' +
                'folded = number; ' +
            'elseif policy == 3 then ' +
                'folded = folded + number; ' +
            'elseif policy == 1 and number < folded then ' +
                'folded = number; ' +
            'elseif policy == 2 and number > folded then ' +
                'folded = number; ' +
            'end; ' +
        'else ' +
            'if stale == nil then ' +
                'stale = {}; ' +
            'end; ' +
            'table.insert(stale, existing[j]); ' +
        'end; ' +
    'end; ' +
    'local incoming = readNumber(entryValue, timestamp); ' +
    'local store = true; ' +
    'local stored = entryValue; ' +
    'if folded ~= nil then ' +
        'if policy == 1 then ' +
            'store = incoming < folded; ' +
        'elseif policy == 2 then ' +
            'store = incoming > folded; ' +
        'else ' +
            'local sum = folded + incoming; ' +
            'if sum ~= sum or sum == math.huge or sum == -math.huge then ' +
                'error(\'RTimeSeries sum at timestamp \' .. timestamp ' +
                       '.. \' is not a finite number\'); ' +
            'end; ' +
            'stored = formatNumber(sum); ' +
        'end; ' +
    'end; ' +
    'if store then ' +
        'if #existing > 0 then ' +
            'removeMembers(existing); ' +
        'end; ' +
        'storeEntry(nextId(), timestamp, stored); ' +
        'if policy ~= 3 then ' +
            'result = 1; ' +
        'elseif folded == nil then ' +
            'result = 1; ' +
        'end; ' +
    'elseif stale ~= nil then ' +
        'removeMembers(stale); ' +
    'end; ' +
    'end; ' +
    TS48_RETENTION_TRIM +
    'return result;';

const TS48_INCREMENT = TS48_NEXT_ID + TS48_REMOVE_MEMBERS + TS48_ENTRY_ARGS +
    TS48_STORE_ENTRY +
    'local state = redis.call(\'hmget\', KEYS[3], \'incremented\', \'total\'); ' +
    'if state[1] ~= false and tonumber(timestamp) < tonumber(state[1]) then ' +
        'timestamp = state[1]; ' +
    'end; ' +
    TS48_RETENTION_CUTOFF + TS48_FORMAT_NUMBER + TS48_READ_NUMBER +
    TS48_WITHIN_RETENTION +
    'local amount = readNumber(entryValue, timestamp); ' +
    'local previous = \'0\'; ' +
    'if state[2] ~= false then ' +
        'previous = state[2]; ' +
    'end; ' +
    'local moved = tonumber(previous) + amount; ' +
    'if moved ~= moved or moved == math.huge or moved == -math.huge then ' +
        'error(\'RTimeSeries total at timestamp \' .. timestamp ' +
               '.. \' is not a finite number\'); ' +
    'end; ' +
    'local id = nextId(); ' +
    'local total = redis.call(\'hincrbyfloat\', KEYS[3], \'total\', ' +
                             'formatNumber(amount)); ' +
    'if amount ~= 0 and total == previous then ' +
        'error(\'RTimeSeries amount at timestamp \' .. timestamp ' +
               '.. \' is too small to change a total of \' .. previous); ' +
    'end; ' +
    'redis.call(\'hset\', KEYS[3], \'incremented\', timestamp); ' +
    'local existing = redis.call(\'zrangebyscore\', KEYS[1], timestamp, timestamp); ' +
    'if #existing > 0 then ' +
        'removeMembers(existing); ' +
    'end; ' +
    'storeEntry(id, timestamp, total); ' +
    'end; ' +
    TS48_RETENTION_TRIM +
    'local reply = redis.call(\'hget\', KEYS[3], \'total\'); ' +
    'if reply == false then ' +
        'reply = \'0\'; ' +
    'end; ' +
    'return reply;';

const TS48_FORMAT_TIMESTAMP = `local function formatTimestamp(score)
    if score >= 9223372036854775808 then
        return '9223372036854775807';
    end;
    if score <= -9223372036854775808 then
        return '-9223372036854775808';
    end;
    return string.format('%.0f', score);
end; `;

const TS48_KEEP_MEMBERS = TS48_FORMAT_TIMESTAMP +
    'local result = {}; ' +
    'local members = {}; ' +
    'local scores = {}; ' +
    'local keepLimit = tonumber(ARGV[4]); ' +
    'local wholeTimestamps = ARGV[8] == \'1\'; ' +
    'local function keep(member, score) ' +
        'if keepLimit > 0 and #members >= keepLimit then ' +
            'if not wholeTimestamps then ' +
                'return false; ' +
            'end; ' +
            'if score ~= scores[#members] then ' +
                'return false; ' +
            'end; ' +
        'end; ' +
        'table.insert(members, member); ' +
        'table.insert(scores, score); ' +
        'return true; ' +
    'end; ';

const TS48_COLLECT_RANGE = `local labelMode = ARGV[6];
local labelValue = ARGV[7];
local function collect()
    local limit = tonumber(ARGV[4]);
    if limit < 0 then
        return;
    end;
    local pageSize = limit;
    if pageSize <= 0 or pageSize > 500 then
        pageSize = 500;
    elseif pageSize < 100 then
        pageSize = 100;
    end;
    local anyExpired = redis.call('zcount', KEYS[2], '-inf', ARGV[1]) > 0;
    local low = tonumber(ARGV[2]);
    local high = tonumber(ARGV[3]);
    local from, to = ARGV[2], ARGV[3];
    local seek, page, rank = 'zrangebyscore', 'zrange', 'zrank';
    if ARGV[5] ~= '0' then
        from, to = ARGV[3], ARGV[2];
        seek, page, rank = 'zrevrangebyscore', 'zrevrange', 'zrevrank';
    end;
    local head = redis.call(seek, KEYS[1], from, to, 'limit', 0, 1);
    if #head == 0 then
        return;
    end;
    local index = redis.call(rank, KEYS[1], head[1]);
    while true do
        local values = redis.call(page, KEYS[1], index, index + pageSize - 1, 'withscores');
        if #values == 0 then
            return;
        end;
        for i = 1, #values, 2 do
            local score = tonumber(values[i+1]);
            if score < low or score > high then
                return;
            end;
            local live = true;
            if anyExpired then
                local expirationDate = redis.call('zscore', KEYS[2], values[i]);
                live = expirationDate == false
                        or tonumber(expirationDate) > tonumber(ARGV[1]);
            end;
            if live then
                local label = 0;
                if labelMode == '1' or labelMode == '2' then
                    local n, t, val;
                    n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]);
                    ${TS48_DECODE_LABEL}
                end;
                ${TS48_MATCHES_LABEL}
                if matches then
                    if not keep(values[i], values[i+1]) then
                        return;
                    end;
                end;
            end;
        end;
        index = index + #values/2;
    end;
end;
collect(); `;

const TS48_INFO = TS48_FORMAT_TIMESTAMP +
    'local expired = redis.call(\'zcount\', KEYS[2], \'-inf\', ARGV[1]); ' +
    'local total = redis.call(\'zcard\', KEYS[1]); ' +
    'local pageSize = expired + 1; ' +
    'if pageSize > 500 then ' +
        'pageSize = 500; ' +
    'end; ' +
    'local function firstLive(reverse) ' +
        'local page = \'zrange\'; ' +
        'if reverse ~= 0 then ' +
            'page = \'zrevrange\'; ' +
        'end; ' +
        'local index = 0; ' +
        'while true do ' +
            'local values = redis.call(page, KEYS[1], index, index + pageSize - 1, \'withscores\'); ' +
            'if #values == 0 then ' +
                'return \'\'; ' +
            'end; ' +
            'for i = 1, #values, 2 do ' +
                'local expirationDate = redis.call(\'zscore\', KEYS[2], values[i]); ' +
                'if expirationDate == false ' +
                        'or tonumber(expirationDate) > tonumber(ARGV[1]) then ' +
                    'return formatTimestamp(tonumber(values[i+1])); ' +
                'end; ' +
            'end; ' +
            'index = index + #values/2; ' +
        'end; ' +
    'end; ' +
    'local first = \'\'; ' +
    'local last = \'\'; ' +
    'if total > 0 then ' +
        'first = firstLive(0); ' +
        'if first ~= \'\' then ' +
            'last = firstLive(1); ' +
        'end; ' +
    'end; ' +
    'local memory = 0; ' +
    'for j = 1, #KEYS do ' +
        'local used = redis.call(\'memory\', \'usage\', KEYS[j]); ' +
        'if used ~= false then ' +
            'memory = memory + used; ' +
        'end; ' +
    'end; ' +
    'local issued = redis.call(\'hget\', KEYS[3], \'id\'); ' +
    'if issued == false then ' +
        'issued = \'0\'; ' +
    'end; ' +
    'return {string.format(\'%.0f\', total - expired), ' +
            'string.format(\'%.0f\', total), ' +
            'first, ' +
            'last, ' +
            'string.format(\'%.0f\', memory), ' +
            'string.format(\'%.0f\', redis.call(\'pttl\', KEYS[1])), ' +
            'issued};';

const TS48_UNPACK_VALUES = `for i = 1, #members do
    local n, t, val = struct.unpack('BBc0Lc0Lc0', members[i]);
    table.insert(result, val);
end;
return result;`;

const TS48_UNPACK_ENTRIES = `for i = 1, #members do
    local n, t, val, label = struct.unpack('BBc0Lc0Lc0', members[i]);
    ${TS48_DECODE_LABEL}
    table.insert(result, val);
    table.insert(result, label);
    table.insert(result, n);
    table.insert(result, formatTimestamp(tonumber(scores[i])));
end;
return result;`;

const TS48_REMOVE_COLLECTED = 'removeMembers(members); ';

const TS48_COLLECT_VALUES = TS48_KEEP_MEMBERS + TS48_COLLECT_RANGE + TS48_UNPACK_VALUES;
const TS48_COLLECT_ENTRIES = TS48_KEEP_MEMBERS + TS48_COLLECT_RANGE + TS48_UNPACK_ENTRIES;

const TS48_AGGREGATE = TS48_FORMAT_TIMESTAMP + TS48_FORMAT_NUMBER +
    'local result = {}; ' +
    'local bucketSize = tonumber(ARGV[8]); ' +
    'local alignment = tonumber(ARGV[9]); ' +
    'local valueFilter = ARGV[10] == \'1\'; ' +
    'local minValue = tonumber(ARGV[11]); ' +
    'local maxValue = tonumber(ARGV[12]); ' +
    'local index, count, sum, minv, maxv, firstv, lastv, mean, m2; ' +
    'local function close() ' +
        'if index == nil then ' +
            'return; ' +
        'end; ' +
        'table.insert(result, formatTimestamp(alignment + index * bucketSize)); ' +
        'for k = 13, #ARGV do ' +
            'local code = tonumber(ARGV[k]); ' +
            'local value; ' +
            'if code == 1 then ' +
                'value = count; ' +
            'elseif code == 2 then ' +
                'value = sum; ' +
            'elseif code == 3 then ' +
                'value = sum / count; ' +
                'if value ~= value or value == math.huge or value == -math.huge then ' +
                    'value = mean; ' +
                'end; ' +
            'elseif code == 4 then ' +
                'value = minv; ' +
            'elseif code == 5 then ' +
                'value = maxv; ' +
            'elseif code == 6 then ' +
                'value = maxv - minv; ' +
            'elseif code == 7 then ' +
                'value = firstv; ' +
            'elseif code == 8 then ' +
                'value = lastv; ' +
            'elseif code == 9 then ' +
                'value = math.sqrt(m2 / count); ' +
            'elseif code == 11 then ' +
                'value = m2 / count; ' +
            'elseif code == 10 and count > 1 then ' +
                'value = math.sqrt(m2 / (count - 1)); ' +
            'elseif code == 12 and count > 1 then ' +
                'value = m2 / (count - 1); ' +
            'end; ' +
            'if value == nil then ' +
                'table.insert(result, \'\'); ' +
            'else ' +
                'table.insert(result, formatNumber(value)); ' +
            'end; ' +
        'end; ' +
    'end; ' +
    'local function keep(member, score) ' +
        'local n, t, val = struct.unpack(\'BBc0Lc0Lc0\', member); ' +
        'local value = tonumber(val); ' +
        'if value == nil or value ~= value ' +
                'or value == math.huge or value == -math.huge then ' +
            'error(\'RTimeSeries value at timestamp \' ' +
                   '.. formatTimestamp(tonumber(score)) .. \' is not a finite number\'); ' +
        'end; ' +
        'if valueFilter and (value < minValue or value > maxValue) then ' +
            'return true; ' +
        'end; ' +
        'local bucket = math.floor((tonumber(score) - alignment) / bucketSize); ' +
        'if bucket ~= index then ' +
            'close(); ' +
            'index = bucket; ' +
            'count, sum, minv, maxv, firstv, mean, m2 = 0, 0, value, value, value, 0, 0; ' +
        'end; ' +
        'count = count + 1; ' +
        'sum = sum + value; ' +
        'if value < minv then ' +
            'minv = value; ' +
        'end; ' +
        'if value > maxv then ' +
            'maxv = value; ' +
        'end; ' +
        'lastv = value; ' +
        'local delta = value - mean; ' +
        'mean = mean + delta / count; ' +
        'm2 = m2 + delta * (value - mean); ' +
        'return true; ' +
    'end; ';

const TS48_CLOSE_LAST_BUCKET = 'close(); return result;';
const TS48_AGGREGATE_RANGE = TS48_AGGREGATE + TS48_COLLECT_RANGE + TS48_CLOSE_LAST_BUCKET;

const TS48_LIVE_AT_TIMESTAMP = `local result = {};
local doomed = {};
local values = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[2]);
for i = 1, #values do
    local expirationDate = redis.call('zscore', KEYS[2], values[i]);
    if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then `;

const TS48_GET_ALL = TS48_LIVE_AT_TIMESTAMP +
    'local n, t, val, label = struct.unpack(\'BBc0Lc0Lc0\', values[i]); ' +
    'table.insert(result, val); ' +
    'end; ' +
    'end; ' +
    'return result;';

const TS48_GET_ALL_ENTRIES = TS48_LIVE_AT_TIMESTAMP +
    'local n, t, val, label = struct.unpack(\'BBc0Lc0Lc0\', values[i]); ' +
    TS48_DECODE_LABEL +
    'table.insert(result, val); ' +
    'table.insert(result, label); ' +
    'table.insert(result, n); ' +
    'table.insert(result, ARGV[2]); ' +
    'end; ' +
    'end; ' +
    'return result;';

const TS48_REMOVE_ALL = TS48_REMOVE_MEMBERS +
    'local doomed = {}; ' +
    'local values = redis.call(\'zrangebyscore\', KEYS[1], ARGV[2], ARGV[2]); ' +
    'for i = 1, #values do ' +
        'local expirationDate = redis.call(\'zscore\', KEYS[2], values[i]); ' +
        'if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ' +
            'table.insert(doomed, values[i]); ' +
        'end; ' +
    'end; ' +
    'removeMembers(doomed); ' +
    'return #doomed;';

const TS48_GET_AND_REMOVE_ALL = TS48_REMOVE_MEMBERS + TS48_LIVE_AT_TIMESTAMP +
    'local n, t, val, label = struct.unpack(\'BBc0Lc0Lc0\', values[i]); ' +
    'table.insert(result, val); ' +
    'table.insert(doomed, values[i]); ' +
    'end; ' +
    'end; ' +
    'removeMembers(doomed); ' +
    'return result;';

const TS48_GET_AND_REMOVE_ALL_ENTRIES = TS48_REMOVE_MEMBERS + TS48_LIVE_AT_TIMESTAMP +
    'local n, t, val, label = struct.unpack(\'BBc0Lc0Lc0\', values[i]); ' +
    TS48_DECODE_LABEL +
    'table.insert(result, val); ' +
    'table.insert(result, label); ' +
    'table.insert(result, n); ' +
    'table.insert(result, ARGV[2]); ' +
    'table.insert(doomed, values[i]); ' +
    'end; ' +
    'end; ' +
    'removeMembers(doomed); ' +
    'return result;';

const TS48_REMOVE_RANGE_BY_LABEL =
    'local labelMode = ARGV[4]; ' +
    'local labelValue = ARGV[5]; ' +
    TS48_REMOVE_MEMBERS +
    'local values = redis.call(\'zrangebyscore\', KEYS[1], ARGV[2], ARGV[3]);' +
    'local doomed = {}; ' +
    'for i, v in ipairs(values) do ' +
        'local expirationDate = redis.call(\'zscore\', KEYS[2], v); ' +
        'if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then ' +
            'local label = 0; ' +
            'if labelMode == \'1\' or labelMode == \'2\' then ' +
                'local n, t, val; ' +
                'n, t, val, label = struct.unpack(\'BBc0Lc0Lc0\', v); ' +
                TS48_DECODE_LABEL +
            'end; ' +
            TS48_MATCHES_LABEL +
            'if matches then ' +
                'table.insert(doomed, v); ' +
            'end; ' +
        'end;' +
    'end;' +
    'removeMembers(doomed); ' +
    'return #doomed;';

const TS48_COLLECT_LABELS = `local result = {};
local seen = {};
local low = tonumber(ARGV[2]);
local high = tonumber(ARGV[3]);
local head = redis.call('zrangebyscore', KEYS[1], ARGV[2], ARGV[3], 'limit', 0, 1);
if #head == 0 then
    return result;
end;
local index = redis.call('zrank', KEYS[1], head[1]);
while true do
    local values = redis.call('zrange', KEYS[1], index, index + 499, 'withscores');
    if #values == 0 then
        return result;
    end;
    for i = 1, #values, 2 do
        local score = tonumber(values[i+1]);
        if score < low or score > high then
            return result;
        end;
        local expirationDate = redis.call('zscore', KEYS[2], values[i]);
        if expirationDate == false or tonumber(expirationDate) > tonumber(ARGV[1]) then
            local n, t, val, label = struct.unpack('BBc0Lc0Lc0', values[i]);
            ${TS48_DECODE_LABEL}
            if label ~= 0 and seen[label] == nil then
                seen[label] = true;
                table.insert(result, label);
            end;
        end;
    end;
    index = index + #values/2;
end;`;

export class RTimeSeries<V = any> extends RedisDataStructure {
  private ttlKey: string;
  private seqKey: string;
  private keyspaceHub: KeyspaceListenerHub | null = null;
  /** Family this instance has already vetted the seq key for (AGENTS law 12:
   *  legacy ids come from a STRING counter, 4.8 ids from the HASH field `id`).
   *  Caching avoids a TYPE round-trip per add once the family is settled. */
  private seqFamilyVetted: 'string' | 'hash' | null = null;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.ttlKey = prefixName('redisson__ts_ttl', name);
    this.seqKey = prefixName('redisson__ts_seq', name);
  }
  /** Fails loudly when the seq key was created by the OTHER write family —
   *  mixing makes each family's members unreadable to the other and lets
   *  ids collide across schemes. Redis would only surface this later as a
   *  raw WRONGTYPE on the counter. */
  private async guardSeqFamily(hash: boolean): Promise<void> {
    if (this.seqFamilyVetted === (hash ? 'hash' : 'string')) return;
    const t = String(await this.redis.type(this.seqKey));
    if (t === 'none') {
      this.seqFamilyVetted = hash ? 'hash' : 'string';
      return;
    }
    if (t !== 'string' && t !== 'hash') {
      throw new Error(`redi.ts: TimeSeries seq key ${this.seqKey} holds a ${t.toUpperCase()} — corrupted companion state`);
    }
    if ((t === 'hash') !== hash) {
      throw new Error(`redi.ts: mixed TimeSeries write families on "${this.name}" — the seq key ${this.seqKey} was created by the ${t === 'hash' ? '4.8 API family (add(TimeSeriesAddArgs) …)' : 'legacy family (add(ts, v) …)'}; use a fresh series name or stick to one family (AGENTS.md law 12)`);
    }
    this.seqFamilyVetted = t;
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
  async add(timestamp: number, value: V, ttlMs?: number, label?: string): Promise<void>;
  /** Java 4.8 `add(TimeSeriesAddArgs)`: retention-aware add; resolves to
   *  `true` when the entry was stored (outside its own retention window it is
   *  not written and `false` is returned). Writes the 4.8 member shape. */
  async add(entry: TimeSeriesAddArgs<V>): Promise<boolean>;
  async add(a: number | TimeSeriesAddArgs<V>, value?: V, ttlMs = 0, label = ''): Promise<void | boolean> {
    if (typeof a !== 'number') return this.add48(a);
    await this.guardSeqFamily(false);
    const timestamp = a;
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
    await this.guardSeqFamily(false);
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
    // Server-side collection (Java COLLECT_RANGE shape): one round trip, with
    // the `anyExpired` fast path so a series without expiring entries skips
    // the per-member checks entirely. Members are decoded inside the script —
    // raw binary members must never cross the wire as UTF-8 strings. Falls
    // back to the historical client-side loop when the script cannot run
    // (ioredis-mock Lua has no `struct`).
    try {
      const flat: any[] = await luaEval(this.redis, TS_COLLECT_LIVE, 2, this.name, this.ttlKey,
        String(now), String(from), String(to), reverse ? '1' : '0');
      const all = this.decodeTriples(Array.isArray(flat) ? flat : []);
      return limit > 0 ? all.slice(0, limit) : all;
    } catch { /* fall through to the client-side path */ }
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
  /** Index page over the data ZSET (used by the lazy stream/iterator).
   *  Server-side collect (TS_COLLECT_LIVE's index-page twin): one round
   *  trip instead of 1 + N member ZSCOREs. The script replies
   *  [rawMemberCount, score, value, label, ...] — the raw count (live +
   *  expired in the window) leads so the caller can advance its cursor;
   *  falls back client-side when the script cannot run (no Lua `struct`). */
  private async rawPage(offset: number, count: number): Promise<{ entries: TimeSeriesEntry<V>[]; rawCount: number }> {
    const now = await serverNowMs(this.redis);
    try {
      const flat: any[] = await luaEval(this.redis, TS_COLLECT_PAGE, 2, this.name, this.ttlKey,
        String(now), String(offset), String(offset + count - 1));
      const arr = Array.isArray(flat) ? flat : [];
      const rawCount = toNumber(arr[0]);
      const entries = this.decodeTriples(arr.slice(1));
      return { entries, rawCount };
    } catch { /* fall through to the client-side path */ }
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

  // -- TimeSeries 4.8 expansion (Redisson 4.8 args-based API) ------------------------
  //
  // All Lua below is verbatim from RedissonTimeSeries.java 4.8 (TS48_* above):
  // KEYS[1] = data ZSET, KEYS[2] = ttl ZSET, KEYS[3] = seq HASH; every ARGV
  // stays a string (mock-safe), members written/read inside Lua. `lua48` is
  // the per-call try/catch: clients whose Lua lacks `struct` (ioredis-mock's
  // fengari VM — which would otherwise run a write script half-way before
  // failing at struct.pack) or a failing script take the client-side JS
  // equivalent below, mirroring the class's legacy fallback style.

  private luaStructCache?: boolean;
  /** `type(struct)` probe (real Redis yes; fengari mock no), cached. */
  private async luaHasStruct(): Promise<boolean> {
    if (this.luaStructCache !== undefined) return this.luaStructCache;
    try {
      const r = await luaEval(this.redis, "return type(struct) == 'table'", 0);
      this.luaStructCache = r === 1 || r === '1' || r === true;
    } catch {
      this.luaStructCache = false;
    }
    return this.luaStructCache;
  }

  /** Run one TS48 script; null means "no Lua / script failed" and the caller
   *  takes its client-side equivalent. */
  private async lua48(script: string, keys: string[], ...argv: (string | number)[]): Promise<any | null> {
    if (!(await this.luaHasStruct())) return null;
    try {
      return await luaEval(this.redis, script, keys.length, ...keys, ...argv);
    } catch {
      return null;
    }
  }

  /** Java `encodeArgs(TimeSeriesAddArgs)`:
   *  [idWidth, retention, now, timestamp, labelFlag, value, label, expiration]. */
  private async encodeAddArgs48(a: TimeSeriesAddArgs<V>): Promise<string[]> {
    await this.guardSeqFamily(true); // every 4.8-family write funnels through here
    const now = await serverNowMs(this.redis);
    const retentionMs = Math.max(0, a.getRetentionMs());
    const ttlMs = a.getTimeToLiveMs();
    const expiration = ttlMs > 0 ? now + ttlMs : now + 365 * 100 * 24 * 3600 * 1000;
    const label = a.getLabel();
    const hasLabel = label !== null && label !== undefined;
    return [
      String(TS_NEXT_ID_WIDTH),
      String(retentionMs),
      String(now),
      String(a.timestamp),
      hasLabel ? '3' : '2',
      this.encodeValue(a.object),
      hasLabel ? this.encodeValue(label) : '',
      String(expiration),
    ];
  }

  private labelArg(label: any, labelMode: number): string {
    if (labelMode !== 1) return '0';
    return this.encodeValue(label);
  }

  private static labelModeOf(filtered: boolean, label: any): number {
    if (!filtered) return 0;
    return label === null || label === undefined ? 2 : 1;
  }

  /** Decode the flat [val, label, n, timestamp] quadruples the 4.8 entry
   *  readers return (label 0 = the Lua absent marker). */
  private decodeQuads(res: any[]): TimeSeriesEntry<V>[] {
    const out: TimeSeriesEntry<V>[] = [];
    for (let i = 0; i + 3 < res.length; i += 4) {
      let value: V;
      try {
        value = this.decodeValue(String(res[i])) as V;
      } catch {
        value = res[i] as V;
      }
      let label = '';
      const raw = res[i + 1];
      if (raw !== 0 && raw !== '0' && raw !== null && raw !== undefined && raw !== '') {
        try {
          const d = this.decodeValue(String(raw));
          label = typeof d === 'string' ? d : String(raw);
        } catch {
          label = String(raw);
        }
      }
      out.push({ timestamp: Number(res[i + 3]), value, label });
    }
    return out;
  }

  private entryFromU(score: number, u: { val: string; label: string } | null): TimeSeriesEntry<V> {
    if (u === null) return { timestamp: score, value: null as unknown as V, label: '' };
    let value: V;
    try {
      value = this.decodeValue(u.val) as V;
    } catch {
      value = u.val as unknown as V;
    }
    let label = '';
    if (u.label !== '') {
      try {
        const d = this.decodeValue(u.label);
        label = typeof d === 'string' ? d : u.label;
      } catch {
        label = u.label;
      }
    }
    return { timestamp: score, value, label };
  }

  /** 4.8-native member: flag byte 4 + id + value + (labelFlag byte || label). */
  private pack48(id: string, encValue: string, encLabel: string, flag: 2 | 3): Buffer {
    const ib = Buffer.from(id, 'utf8');
    const vb = Buffer.from(encValue, 'utf8');
    const field = Buffer.concat([Buffer.from([flag]), Buffer.from(encLabel, 'utf8')]);
    const out = Buffer.alloc(2 + ib.length + 8 + vb.length + 8 + field.length);
    let o = 0;
    out[o++] = 4;
    out[o++] = ib.length;
    ib.copy(out, o);
    o += ib.length;
    out.writeBigUInt64LE(BigInt(vb.length), o);
    o += 8;
    vb.copy(out, o);
    o += vb.length;
    out.writeBigUInt64LE(BigInt(field.length), o);
    o += 8;
    field.copy(out, o);
    return out;
  }

  /** JS NEXT_ID: ids from the seq HASH field `id` (zero-padded to width 20). */
  private async nextIdJs(): Promise<string> {
    const seq = String(await this.redis.hincrbyfloat(this.seqKey, 'id', 1));
    if (seq.length > TS_NEXT_ID_WIDTH) throw new Error('RTimeSeries sequence overflow');
    return seq.padStart(TS_NEXT_ID_WIDTH, '0');
  }

  private async store48Js(id: string, timestamp: number, encValue: string, encLabel: string, flag: 2 | 3, expiration: number): Promise<void> {
    const member = this.pack48(id, encValue, encLabel, flag);
    await this.redis.zadd(this.name, String(timestamp), member);
    await this.redis.zadd(this.ttlKey, String(expiration), member);
  }

  private async zremBoth(members: (Buffer | string)[]): Promise<void> {
    if (members.length === 0) return;
    const plain = members.map((m) => (Buffer.isBuffer(m) ? m.toString('latin1') : String(m)));
    try {
      await this.redis.zrem(this.name, ...(members as any[]));
    } catch {
      await this.redis.zrem(this.name, ...plain);
    }
    try {
      await this.redis.zrem(this.ttlKey, ...(members as any[]));
    } catch {
      await this.redis.zrem(this.ttlKey, ...plain);
    }
  }

  /** Members at exactly `timestamp` with liveness flags (client-side path). */
  private async membersAtJs(timestamp: number, now: number): Promise<{ member: Buffer; u: { val: string; label: string } | null; expired: boolean }[]> {
    let raw: any[];
    try {
      raw = await (this.redis as any).zrangeBuffer(this.name, String(timestamp), String(timestamp), 'BYSCORE');
    } catch {
      raw = await (this.redis as any).zrange(this.name, String(timestamp), String(timestamp), 'BYSCORE');
    }
    const out: { member: Buffer; u: { val: string; label: string } | null; expired: boolean }[] = [];
    for (const m of raw ?? []) {
      const member = Buffer.isBuffer(m) ? m : Buffer.from(String(m), 'latin1');
      let exp: any = null;
      try {
        exp = await this.redis.zscore(this.ttlKey, member as any);
      } catch {
        exp = await this.redis.zscore(this.ttlKey, member.toString('latin1'));
      }
      out.push({ member, u: unpackTimeSeriesMember(member), expired: exp !== null && exp !== undefined && Number(exp) <= now });
    }
    return out;
  }

  /** Live-member walk over [from, to] ascending (client-side path; handles
   *  both the legacy flag 2/3 and the 4.8 flag-4 member shapes via unpack). */
  private async liveRangeJs(from: number | string, to: number | string): Promise<{ member: Buffer; score: number; u: { val: string; label: string } | null; expired: boolean }[]> {
    const now = await serverNowMs(this.redis);
    const args: (string | number)[] = [this.name, String(from), String(to), 'BYSCORE', 'WITHSCORES'];
    let raw: any[];
    try {
      raw = await (this.redis as any).zrangeBuffer(...args);
    } catch {
      raw = await (this.redis as any).zrange(...args);
    }
    const out: { member: Buffer; score: number; u: { val: string; label: string } | null; expired: boolean }[] = [];
    for (let i = 0; i + 1 < (raw ?? []).length; i += 2) {
      const member = Buffer.isBuffer(raw[i]) ? raw[i] : Buffer.from(String(raw[i]), 'latin1');
      let exp: any = null;
      try {
        exp = await this.redis.zscore(this.ttlKey, member as any);
      } catch {
        exp = await this.redis.zscore(this.ttlKey, member.toString('latin1'));
      }
      out.push({ member, score: Number(raw[i + 1]), u: unpackTimeSeriesMember(member), expired: exp !== null && exp !== undefined && Number(exp) <= now });
    }
    return out;
  }

  private labelPasses(rawEncLabel: string, labelMode: number, encLabelArg: string): boolean {
    if (labelMode === 1) return rawEncLabel === encLabelArg;
    if (labelMode === 2) return rawEncLabel === '';
    return true;
  }

  /** JS RETENTION_CUTOFF: anchor on the highest live score (bounded probes
   *  drop an expired head first, exactly as the script does). */
  private async retentionCutoffJs(retentionMs: number, timestamp: number, now: number): Promise<number | null> {
    if (retentionMs <= 0) return null;
    let highest = timestamp;
    for (let probes = 0; probes < 100; probes++) {
      let top: any[];
      try {
        top = await (this.redis as any).zrevrangeBuffer(this.name, 0, 0, 'WITHSCORES');
      } catch {
        top = await (this.redis as any).zrevrange(this.name, 0, 0, 'WITHSCORES');
      }
      if (!top || top.length === 0) break;
      const m = top[0];
      let exp: any = null;
      try {
        exp = await this.redis.zscore(this.ttlKey, m);
      } catch { /* noop */ }
      if (exp === null || exp === undefined || Number(exp) > now) {
        const top1 = Number(top[1]);
        if (top1 > highest) highest = top1;
        break;
      }
      await this.zremBoth([m]);
    }
    return highest - retentionMs;
  }

  /** JS RETENTION_TRIM (bounded to 500 members). */
  private async retentionTrimJs(cutoff: number | null): Promise<void> {
    if (cutoff === null) return;
    let doomed: any[];
    const max = `(${cutoff}`;
    try {
      doomed = await (this.redis as any).zrangebyscoreBuffer(this.name, '-inf', max, 'LIMIT', 0, 500);
    } catch {
      try {
        doomed = (await this.redis.zrangebyscore(this.name, '-inf', max)).slice(0, 500);
      } catch {
        return;
      }
    }
    if (doomed && doomed.length > 0) await this.zremBoth(doomed);
  }

  private async add48(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    const args = await this.encodeAddArgs48(a);
    const reply = await this.lua48(TS48_ADD_ENTRY, [this.name, this.ttlKey, this.seqKey], ...args);
    if (reply !== null && reply !== undefined) return toNumber(reply) === 1;
    const now = Number(args[2]);
    const cutoff = await this.retentionCutoffJs(a.getRetentionMs(), a.timestamp, now);
    let added = false;
    if (cutoff === null || a.timestamp >= cutoff) {
      await this.store48Js(await this.nextIdJs(), a.timestamp, args[5], args[6], args[4] === '3' ? 3 : 2, Number(args[7]));
      added = true;
    }
    await this.retentionTrimJs(cutoff);
    return added;
  }

  /** Java 4.8 `addIfAbsent(TimeSeriesAddArgs)`: stores only when the timestamp
   *  holds no live entry (expired slots are cleared and reused). */
  async addIfAbsent(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    const args = await this.encodeAddArgs48(a);
    const reply = await this.lua48(TS48_ADD_IF_ABSENT, [this.name, this.ttlKey, this.seqKey], ...args);
    if (reply !== null && reply !== undefined) return toNumber(reply) === 1;
    const now = Number(args[2]);
    const cutoff = await this.retentionCutoffJs(a.getRetentionMs(), a.timestamp, now);
    let added = false;
    if (cutoff === null || a.timestamp >= cutoff) {
      const existing = await this.membersAtJs(a.timestamp, now);
      const stale = existing.filter((e) => e.expired).map((e) => e.member);
      if (stale.length > 0) await this.zremBoth(stale);
      if (!existing.some((e) => !e.expired)) {
        await this.store48Js(await this.nextIdJs(), a.timestamp, args[5], args[6], args[4] === '3' ? 3 : 2, Number(args[7]));
        added = true;
      }
    }
    await this.retentionTrimJs(cutoff);
    return added;
  }

  /** Java 4.8 `addOrReplace(TimeSeriesAddArgs)`: replaces everything at the
   *  timestamp; returns `true` when it created the slot (false = replaced). */
  async addOrReplace(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    const args = await this.encodeAddArgs48(a);
    const reply = await this.lua48(TS48_ADD_OR_REPLACE, [this.name, this.ttlKey, this.seqKey], ...args);
    if (reply !== null && reply !== undefined) return toNumber(reply) === 1;
    const now = Number(args[2]);
    const cutoff = await this.retentionCutoffJs(a.getRetentionMs(), a.timestamp, now);
    let created = false;
    if (cutoff === null || a.timestamp >= cutoff) {
      const existing = await this.membersAtJs(a.timestamp, now);
      const replaced = existing.some((e) => !e.expired);
      if (existing.length > 0) await this.zremBoth(existing.map((e) => e.member));
      await this.store48Js(await this.nextIdJs(), a.timestamp, args[5], args[6], args[4] === '3' ? 3 : 2, Number(args[7]));
      created = !replaced;
    }
    await this.retentionTrimJs(cutoff);
    return created;
  }

  private async addComparing48(a: TimeSeriesAddArgs<V>, policy: 1 | 2 | 3): Promise<boolean> {
    const args = await this.encodeAddArgs48(a);
    const reply = await this.lua48(TS48_ADD_COMPARING, [this.name, this.ttlKey, this.seqKey], ...args, String(policy));
    if (reply !== null && reply !== undefined) return toNumber(reply) === 1;
    const now = Number(args[2]);
    const cutoff = await this.retentionCutoffJs(a.getRetentionMs(), a.timestamp, now);
    let result = false;
    if (cutoff === null || a.timestamp >= cutoff) {
      const existing = await this.membersAtJs(a.timestamp, now);
      let folded: number | undefined;
      for (const e of existing) {
        if (e.expired) continue;
        const number = tsReadNumber(e.u ? e.u.val : '', a.timestamp);
        if (folded === undefined) folded = number;
        else if (policy === 3) folded += number;
        else if (policy === 1 && number < folded) folded = number;
        else if (policy === 2 && number > folded) folded = number;
      }
      const incoming = tsReadNumber(args[5], a.timestamp);
      let store = true;
      let stored = args[5];
      if (folded !== undefined) {
        if (policy === 1) store = incoming < folded;
        else if (policy === 2) store = incoming > folded;
        else {
          const sum = folded + incoming;
          if (!Number.isFinite(sum)) throw new Error(`RTimeSeries sum at timestamp ${a.timestamp} is not a finite number`);
          stored = String(sum);
        }
      }
      if (store) {
        if (existing.length > 0) await this.zremBoth(existing.map((e) => e.member));
        await this.store48Js(await this.nextIdJs(), a.timestamp, stored, args[6], args[4] === '3' ? 3 : 2, Number(args[7]));
        if (policy !== 3) result = true;
        else if (folded === undefined) result = true;
      } else {
        const stale = existing.filter((e) => e.expired).map((e) => e.member);
        if (stale.length > 0) await this.zremBoth(stale);
      }
    }
    await this.retentionTrimJs(cutoff);
    return result;
  }

  /** Java 4.8 `addIfLess`: stores only when the incoming numeric value is less
   *  than every live one at the timestamp (values must encode as numbers). */
  async addIfLess(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    return this.addComparing48(a, 1);
  }

  /** Java 4.8 `addIfGreater`: stores only when the incoming numeric value is
   *  greater than every live one at the timestamp. */
  async addIfGreater(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    return this.addComparing48(a, 2);
  }

  /** Java 4.8 `addAndSum`: folds the timestamp into the sum of its live values
   *  and the incoming one; returns `true` when the total entry was created. */
  async addAndSum(a: TimeSeriesAddArgs<V>): Promise<boolean> {
    return this.addComparing48(a, 3);
  }

  /** Java 4.8 `addAndGet(TimeSeriesAddArgs)`: adds the numeric delta to a
   *  running total (seq HASH field `total`) and records the new total as the
   *  entry; returns the total. Out-of-order increments land at the highest
   *  previously incremented timestamp. */
  async addAndGet(a: TimeSeriesAddArgs<V>): Promise<number> {
    const args = await this.encodeAddArgs48(a);
    const reply = await this.lua48(TS48_INCREMENT, [this.name, this.ttlKey, this.seqKey], ...args);
    if (reply !== null && reply !== undefined) return Number(reply);
    const now = Number(args[2]);
    let incremented: string | null = null;
    try {
      incremented = await this.redis.hget(this.seqKey, 'incremented');
    } catch { /* noop */ }
    let timestamp = a.timestamp;
    if (incremented !== null && incremented !== undefined && timestamp < Number(incremented)) timestamp = Number(incremented);
    const cutoff = await this.retentionCutoffJs(a.getRetentionMs(), timestamp, now);
    if (cutoff === null || timestamp >= cutoff) {
      const amount = tsReadNumber(args[5], timestamp);
      let previous = '0';
      try {
        const t = await this.redis.hget(this.seqKey, 'total');
        if (t !== null && t !== undefined) previous = String(t);
      } catch { /* noop */ }
      const moved = Number(previous) + amount;
      if (!Number.isFinite(moved)) throw new Error(`RTimeSeries total at timestamp ${timestamp} is not a finite number`);
      const id = await this.nextIdJs();
      const total = String(await this.redis.hincrbyfloat(this.seqKey, 'total', amount));
      if (amount !== 0 && total === previous) {
        throw new Error(`RTimeSeries amount at timestamp ${timestamp} is too small to change a total of ${previous}`);
      }
      await this.redis.hset(this.seqKey, 'incremented', String(timestamp));
      const existing = await this.membersAtJs(timestamp, now);
      if (existing.length > 0) await this.zremBoth(existing.map((e) => e.member));
      await this.store48Js(id, timestamp, total, args[6], args[4] === '3' ? 3 : 2, Number(args[7]));
    }
    await this.retentionTrimJs(cutoff);
    let totalReply: any = null;
    try {
      totalReply = await this.redis.hget(this.seqKey, 'total');
    } catch { /* noop */ }
    return totalReply === null || totalReply === undefined ? 0 : Number(totalReply);
  }

  /** Java 4.8 `aggregate(TimeSeriesAggregationArgs)`: folds the live entries
   *  of a range into buckets (Welford variance), one bucket per populated
   *  interval; values must encode as numbers. */
  async aggregate(a: TimeSeriesAggregationArgs): Promise<TimeSeriesBucket[]> {
    const bucketMs = a.getBucketMs() ?? 0;
    if (bucketMs <= 0) {
      throw new Error('bucket interval should be defined and at least one millisecond');
    }
    const aggs = a.getAggregations();
    if (aggs.length === 0) throw new Error('at least one aggregation should be defined');
    const labelMode = RTimeSeries.labelModeOf(a.isLabelFiltered(), a.getLabel());
    const now = await serverNowMs(this.redis);
    const argv = [
      String(now),
      String(a.startTimestamp),
      String(a.endTimestamp),
      '0',
      '0',
      String(labelMode),
      this.labelArg(a.getLabel(), labelMode),
      String(bucketMs),
      String(a.getAlignment()),
      a.isValueFiltered() ? '1' : '0',
      String(a.getMinValue()),
      String(a.getMaxValue()),
      ...aggs.map((g) => String(TS_AGG_CODES[g])),
    ];
    const rows: any[] = (await this.lua48(TS48_AGGREGATE_RANGE, [this.name, this.ttlKey], ...argv)) ?? null;
    if (rows !== null) {
      const stride = aggs.length + 1;
      const out: TimeSeriesBucket[] = [];
      for (let i = 0; i + stride <= rows.length; i += stride) {
        const values: Partial<Record<TimeSeriesAggregation, number>> = {};
        for (let j = 0; j < aggs.length; j++) {
          const v = String(rows[i + 1 + j] ?? '');
          if (v !== '') values[aggs[j]] = Number(v);
        }
        out.push({ timestamp: Number(rows[i]), values });
      }
      return out;
    }
    const encLabelArg = labelMode === 1 ? this.labelArg(a.getLabel(), 1) : '';
    const entries = await this.liveRangeJs(a.startTimestamp, a.endTimestamp);
    const alignment = a.getAlignment();
    const buckets: TimeSeriesBucket[] = [];
    let index: number | undefined;
    let count = 0;
    let sum = 0;
    let minv = 0;
    let maxv = 0;
    let firstv = 0;
    let lastv = 0;
    let mean = 0;
    let m2 = 0;
    const close = () => {
      if (index === undefined) return;
      const values: Partial<Record<TimeSeriesAggregation, number>> = {};
      for (const g of aggs) {
        const code = TS_AGG_CODES[g];
        let value: number | undefined;
        if (code === 1) value = count;
        else if (code === 2) value = sum;
        else if (code === 3) {
          value = sum / count;
          if (!Number.isFinite(value)) value = mean;
        } else if (code === 4) value = minv;
        else if (code === 5) value = maxv;
        else if (code === 6) value = maxv - minv;
        else if (code === 7) value = firstv;
        else if (code === 8) value = lastv;
        else if (code === 9) value = Math.sqrt(m2 / count);
        else if (code === 11) value = m2 / count;
        else if (code === 10 && count > 1) value = Math.sqrt(m2 / (count - 1));
        else if (code === 12 && count > 1) value = m2 / (count - 1);
        if (value !== undefined) values[g] = value;
      }
      buckets.push({ timestamp: Math.round(alignment + index * bucketMs), values });
    };
    for (const row of entries) {
      if (row.expired) continue;
      if (!this.labelPasses(row.u ? row.u.label : '', labelMode, encLabelArg)) continue;
      const value = tsReadNumber(row.u ? row.u.val : '', row.score);
      if (a.isValueFiltered() && (value < a.getMinValue() || value > a.getMaxValue())) continue;
      const bucket = Math.floor((row.score - alignment) / bucketMs);
      if (bucket !== index) {
        close();
        index = bucket;
        count = 0;
        sum = 0;
        minv = value;
        maxv = value;
        firstv = value;
        mean = 0;
        m2 = 0;
      }
      count += 1;
      sum += value;
      if (value < minv) minv = value;
      if (value > maxv) maxv = value;
      lastv = value;
      const delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    }
    close();
    return buckets;
  }

  /** Java 4.8 `readTail(TimeSeriesReadArgs)`: non-blocking entries strictly
   *  after the cursor timestamp (count is a floor — a batch never ends inside
   *  a timestamp). */
  async readTail(a: TimeSeriesReadArgs): Promise<TimeSeriesEntry<V>[]> {
    if (a.getCount() < 0) return [];
    const labelMode = RTimeSeries.labelModeOf(a.isLabelFiltered(), a.getLabel());
    let bound = a.timestamp;
    if (Math.trunc(bound) <= a.timestamp) bound = nextUpDouble(bound);
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(TS48_COLLECT_ENTRIES, [this.name, this.ttlKey],
      String(now), String(bound), TS_WHOLE_RANGE_END, String(a.getCount()), '0', String(labelMode), this.labelArg(a.getLabel(), labelMode), '1')) ?? null;
    if (res !== null) return this.decodeQuads(res);
    const count = a.getCount();
    const encLabelArg = labelMode === 1 ? this.labelArg(a.getLabel(), 1) : '';
    const rows = (await this.liveRangeJs(bound, '+inf')).filter((r) => !r.expired && r.score > a.timestamp);
    const out: TimeSeriesEntry<V>[] = [];
    for (const row of rows) {
      if (!this.labelPasses(row.u ? row.u.label : '', labelMode, encLabelArg)) continue;
      if (count > 0 && out.length >= count && row.score !== out[out.length - 1].timestamp) break;
      out.push(this.entryFromU(row.score, row.u));
    }
    return out;
  }

  /** Java 4.8 `info()`: one-round-trip snapshot of the collection. */
  async info(): Promise<TimeSeriesInfo> {
    const now = await serverNowMs(this.redis);
    const rows: any[] = (await this.lua48(TS48_INFO, [this.name, this.ttlKey, this.seqKey], String(now))) ?? null;
    if (rows !== null) {
      const tsOrNull = (v: any): number | null => {
        const s = String(v ?? '');
        return s === '' ? null : Number(s);
      };
      const issuedRaw = String(rows[6] ?? '0');
      const entriesIssued = /^-?\d+$/.test(issuedRaw) ? Number(issuedRaw) : Number.MAX_SAFE_INTEGER;
      return {
        size: Number(rows[0] ?? 0),
        totalEntries: Number(rows[1] ?? 0),
        firstTimestamp: tsOrNull(rows[2]),
        lastTimestamp: tsOrNull(rows[3]),
        memoryUsage: Number(rows[4] ?? 0),
        timeToLive: Number(rows[5] ?? -2),
        entriesIssued,
      };
    }
    const total = toNumber(await this.redis.zcard(this.name));
    let expired = 0;
    try {
      expired = toNumber(await this.redis.zcount(this.ttlKey, '-inf', String(now)));
    } catch { /* noop */ }
    const liveRows = (await this.liveRangeJs('-inf', '+inf')).filter((r) => !r.expired);
    const first = liveRows.length > 0 ? liveRows[0].score : null;
    const last = liveRows.length > 0 ? liveRows[liveRows.length - 1].score : null;
    let issued: any = null;
    try {
      issued = await this.redis.hget(this.seqKey, 'id');
    } catch { /* noop */ }
    const issuedRaw = issued === null || issued === undefined ? '0' : String(issued);
    const entriesIssued = /^-?\d+$/.test(issuedRaw) ? Number(issuedRaw) : Number.MAX_SAFE_INTEGER;
    let pttl = -2;
    try {
      pttl = toNumber(await this.redis.pttl(this.name), -2);
    } catch { /* noop */ }
    return { size: Math.max(0, total - expired), totalEntries: total, firstTimestamp: first, lastTimestamp: last, memoryUsage: 0, timeToLive: pttl, entriesIssued };
  }

  /** Java 4.8 `getAll(timestamp)`: values of every live entry at exactly
   *  `timestamp`, in insertion order (the singular `getValue` reads one). */
  async getAll(timestamp: number): Promise<V[]> {
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(TS48_GET_ALL, [this.name, this.ttlKey], String(now), String(timestamp))) ?? null;
    if (res !== null) {
      return res.map((v) => {
        try {
          return this.decodeValue(String(v));
        } catch {
          return v;
        }
      });
    }
    const rows = (await this.liveRangeJs(timestamp, timestamp)).filter((r) => !r.expired && r.u !== null);
    return rows.map((r) => {
      try {
        return this.decodeValue(r.u!.val);
      } catch {
        return r.u!.val as unknown as V;
      }
    });
  }

  /** Java 4.8 `getAllEntries(timestamp)`: entries at exactly `timestamp`. */
  async getAllEntries(timestamp: number): Promise<TimeSeriesEntry<V>[]> {
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(TS48_GET_ALL_ENTRIES, [this.name, this.ttlKey], String(now), String(timestamp))) ?? null;
    if (res !== null) return this.decodeQuads(res);
    const rows = (await this.liveRangeJs(timestamp, timestamp)).filter((r) => !r.expired);
    return rows.map((r) => this.entryFromU(r.score, r.u));
  }

  /** Java 4.8 `removeAll(timestamp)`: removes every live entry at exactly
   *  `timestamp` from both ZSETs; returns how many went. */
  async removeAll(timestamp: number): Promise<number> {
    const now = await serverNowMs(this.redis);
    const reply = await this.lua48(TS48_REMOVE_ALL, [this.name, this.ttlKey], String(now), String(timestamp));
    if (reply !== null && reply !== undefined) return toNumber(reply);
    const live = (await this.membersAtJs(timestamp, now)).filter((e) => !e.expired);
    await this.zremBoth(live.map((e) => e.member));
    return live.length;
  }

  /** Java 4.8 `getAndRemoveAll(timestamp)`: values of the live entries at
   *  `timestamp`, removing them. */
  async getAndRemoveAll(timestamp: number): Promise<V[]> {
    const entries = await this.getAndRemoveAllEntries(timestamp);
    return entries.map((e) => e.value);
  }

  /** Java 4.8 `getAndRemoveAllEntries(timestamp)`. */
  async getAndRemoveAllEntries(timestamp: number): Promise<TimeSeriesEntry<V>[]> {
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(TS48_GET_AND_REMOVE_ALL_ENTRIES, [this.name, this.ttlKey], String(now), String(timestamp))) ?? null;
    if (res !== null) return this.decodeQuads(res);
    const live = (await this.membersAtJs(timestamp, now)).filter((e) => !e.expired);
    await this.zremBoth(live.map((e) => e.member));
    return live.map((e) => this.entryFromU(timestamp, e.u));
  }

  private async rangeByLabel48(reverse: boolean, from: number, to: number, label: any, limit: number, entries: boolean): Promise<V[] | TimeSeriesEntry<V>[]> {
    if (limit < 0) return [];
    const labelMode = label === null || label === undefined ? 2 : 1;
    const script = entries ? TS48_COLLECT_ENTRIES : TS48_COLLECT_VALUES;
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(script, [this.name, this.ttlKey],
      String(now), String(from), String(to), String(limit), reverse ? '1' : '0', String(labelMode), this.labelArg(label, labelMode), '0')) ?? null;
    if (res !== null) {
      if (entries) return this.decodeQuads(res);
      return res.map((v) => {
        try {
          return this.decodeValue(String(v));
        } catch {
          return v;
        }
      });
    }
    const encLabelArg = labelMode === 1 ? this.labelArg(label, 1) : '';
    const rows = (await this.liveRangeJs(from, to)).filter((r) => !r.expired && this.labelPasses(r.u ? r.u.label : '', labelMode, encLabelArg));
    if (reverse) rows.reverse();
    if (limit > 0) rows.splice(limit);
    if (entries) return rows.map((r) => this.entryFromU(r.score, r.u));
    return rows.map((r) => this.entryFromU(r.score, r.u).value);
  }

  /** Java 4.8 `rangeByLabel(from, to, label[, limit])`: live values carrying
   *  `label` (pass `null` for the entries that carry none). */
  async rangeByLabel(from: number, to: number, label: any, limit = 0): Promise<V[]> {
    return (await this.rangeByLabel48(false, from, to, label, limit, false)) as V[];
  }

  /** Java 4.8 `rangeReversedByLabel(from, to, label[, limit])`. */
  async rangeReversedByLabel(from: number, to: number, label: any, limit = 0): Promise<V[]> {
    return (await this.rangeByLabel48(true, from, to, label, limit, false)) as V[];
  }

  /** Java 4.8 `entryRangeByLabel(from, to, label[, limit])`: entries. */
  async entryRangeByLabel(from: number, to: number, label: any, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return (await this.rangeByLabel48(false, from, to, label, limit, true)) as TimeSeriesEntry<V>[];
  }

  /** Java 4.8 `entryRangeReversedByLabel(from, to, label[, limit])`. */
  async entryRangeReversedByLabel(from: number, to: number, label: any, limit = 0): Promise<TimeSeriesEntry<V>[]> {
    return (await this.rangeByLabel48(true, from, to, label, limit, true)) as TimeSeriesEntry<V>[];
  }

  /** Java 4.8 `removeRangeByLabel(from, to, label)`: removes the live entries
   *  carrying `label` in the range; returns how many went. */
  async removeRangeByLabel(from: number, to: number, label: any): Promise<number> {
    const labelMode = label === null || label === undefined ? 2 : 1;
    const now = await serverNowMs(this.redis);
    const reply = await this.lua48(TS48_REMOVE_RANGE_BY_LABEL, [this.name, this.ttlKey],
      String(now), String(from), String(to), String(labelMode), this.labelArg(label, labelMode));
    if (reply !== null && reply !== undefined) return toNumber(reply);
    const encLabelArg = labelMode === 1 ? this.labelArg(label, 1) : '';
    const rows = (await this.liveRangeJs(from, to)).filter((r) => !r.expired && this.labelPasses(r.u ? r.u.label : '', labelMode, encLabelArg));
    await this.zremBoth(rows.map((r) => r.member));
    return rows.length;
  }

  /** Java 4.8 `labels()` / `labels(start, end)`: distinct labels of the live
   *  entries in the range, first-seen order. */
  async labels(startTimestamp?: number, endTimestamp?: number): Promise<any[]> {
    const from = startTimestamp === undefined ? TS_WHOLE_RANGE_START : String(startTimestamp);
    const to = endTimestamp === undefined ? TS_WHOLE_RANGE_END : String(endTimestamp);
    const now = await serverNowMs(this.redis);
    const res: any[] = (await this.lua48(TS48_COLLECT_LABELS, [this.name, this.ttlKey], String(now), from, to)) ?? null;
    if (res !== null) {
      return res.map((v) => {
        try {
          return this.decodeValue(String(v));
        } catch {
          return v;
        }
      });
    }
    const seen = new Set<string>();
    const out: any[] = [];
    const rows = (await this.liveRangeJs(from, to)).filter((r) => !r.expired);
    for (const row of rows) {
      const raw = row.u ? row.u.label : '';
      if (raw === '' || seen.has(raw)) continue;
      seen.add(raw);
      out.push(this.entryFromU(row.score, row.u).label);
    }
    return out;
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
    value = tonumber(value)
    if returned > 0 then
        -- Redisson 4.8 recalc branch: when returning expired permits would push
        -- the counter above rate (rate was lowered, or value drifted), recompute
        -- from the permits actually still held in the window instead of adding.
        if value + returned > rate then
            local values = redis.call('zrange', KEYS[2], 0, -1)
            local used = 0
            for i = 1, #values do
                local rnd, p = struct.unpack('Bc0I', values[i])
                used = used + p
            end
            value = rate - used
        else
            value = value + returned
        end
    end
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
    if (permits < 0) throw new Error("redi.ts: Permits amount can't be negative");
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
    // Java 4.8 release: un-acquire oldest permit members first (partial
    // remainder re-added at the original score), then raise the counter
    // (capped at rate), then refresh keepAlive TTLs.
    const s = `local rate = tonumber(ARGV[1])
local toRelease = tonumber(ARGV[2])
local values = redis.call('zrange', KEYS[2], 0, -1, 'withscores')
for i = 1, #values - 1, 2 do
    if toRelease <= 0 then break end
    local v = values[i]
    local score = values[i + 1]
    local random, p = struct.unpack('Bc0I', v)
    redis.call('zrem', KEYS[2], v)
    if p <= toRelease then
        toRelease = toRelease - p
    else
        local remained = p - toRelease
        redis.call('zadd', KEYS[2], score, struct.pack('Bc0I', string.len(random), random, remained))
        toRelease = 0
    end
end
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
redis.call('set', KEYS[1], newValue)
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
end`;
    try {
      await luaEval(this.redis, s, 3, this.valueKey(), this.permitsKey(), this.name, String(this.rate), String(permits));
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
  /** Java `init(double errorRate, long capacity)` / `init(BloomFilterInitArgs)`: BF.RESERVE. */
  async init(errorRate: number, capacity: number): Promise<void> {
    await this.reserve(capacity, errorRate);
  }
  async add(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'BF.ADD', this.name, element)) === 1;
  }
  async exists(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'BF.EXISTS', this.name, element)) === 1;
  }
  /** Java `count()`: BF.CARD — approximate number of items added. */
  async count(): Promise<number> {
    return toNumber(await tryDo(this.redis, 'BF.CARD', this.name));
  }
}

export interface TDigestMergeArgs {
  keys: string[];
  compression?: number;
  override?: boolean;
}

/** TS shape of Java's static `TDigestMergeArgs` factories. */
export const TDigestMergeArgs = {
  keys: (...keys: string[]): TDigestMergeArgs => ({ keys }),
};

export interface CuckooFilterInitArgs {
  capacity: number;
  bucketSize?: number;
  maxIterations?: number;
  expansion?: number;
}

export const CuckooFilterInitArgs = {
  capacity: (capacity: number): CuckooFilterInitArgs => ({ capacity }),
};

export interface CuckooFilterAddArgs<V = string> {
  items: V[];
  capacity?: number;
  noCreate?: boolean;
}

export const CuckooFilterAddArgs = {
  items: <V>(items: V[]): CuckooFilterAddArgs<V> => ({ items }),
};

export interface TopKInitArgs {
  topK: number;
  width?: number;
  depth?: number;
  decay?: number;
}

export const TopKInitArgs = {
  topK: (topK: number): TopKInitArgs => ({ topK }),
};

export class RCuckooFilter extends RedisDataStructure {
  async reserve(capacity: number): Promise<void> {
    await tryDo(this.redis, 'CF.RESERVE', this.name, capacity);
  }
  /** Java `init(long)` / `init(CuckooFilterInitArgs)`: CF.RESERVE. */
  async init(capacity: number): Promise<void>;
  async init(args: CuckooFilterInitArgs): Promise<void>;
  async init(a: number | CuckooFilterInitArgs): Promise<void> {
    if (typeof a === 'number') return this.reserve(a);
    const args: (string | number)[] = [this.name, a.capacity];
    if (a.bucketSize !== undefined) args.push('BUCKETSIZE', a.bucketSize);
    if (a.maxIterations !== undefined) args.push('MAXITERATIONS', a.maxIterations);
    if (a.expansion !== undefined) args.push('EXPANSION', a.expansion);
    await tryDo(this.redis, 'CF.RESERVE', ...args);
  }

  private async insert(mode: 'CF.INSERT' | 'CF.INSERTNX', args: CuckooFilterAddArgs<string>): Promise<string[]> {
    if (args.items.length === 0) return [];
    const params: (string | number)[] = [this.name];
    if (args.capacity !== undefined) params.push('CAPACITY', args.capacity);
    if (args.noCreate) params.push('NOCREATE');
    params.push('ITEMS', ...args.items.map((i) => this.encodeValue(i)));
    const r: any = await tryDo(this.redis, mode, ...params);
    const arr = (r ?? []) as any[];
    return args.items.filter((_, i) => toNumber(arr[i]) === 1);
  }

  async add(element: string): Promise<boolean>;
  async add(args: CuckooFilterAddArgs<string>): Promise<string[]>;
  async add(a: string | CuckooFilterAddArgs<string>): Promise<boolean | string[]> {
    if (typeof a === 'string') return toNumber(await tryDo(this.redis, 'CF.ADD', this.name, this.encodeValue(a))) === 1;
    return this.insert('CF.INSERT', a);
  }
  /** Java `addIfAbsent(V)` / `addIfAbsent(CuckooFilterAddArgs)`. */
  async addIfAbsent(element: string): Promise<boolean>;
  async addIfAbsent(args: CuckooFilterAddArgs<string>): Promise<string[]>;
  async addIfAbsent(a: string | CuckooFilterAddArgs<string>): Promise<boolean | string[]> {
    if (typeof a === 'string') return toNumber(await tryDo(this.redis, 'CF.ADDNX', this.name, this.encodeValue(a))) === 1;
    return this.insert('CF.INSERTNX', a);
  }
  async exists(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.EXISTS', this.name, this.encodeValue(element))) === 1;
  }
  /** Java `exists(Collection)`: CF.MEXISTS → the subset that exists. */
  async existsMany(elements: string[]): Promise<string[]> {
    if (elements.length === 0) return [];
    const r: any = await tryDo(this.redis, 'CF.MEXISTS', this.name, ...elements.map((e) => this.encodeValue(e)));
    const arr = (r ?? []) as any[];
    return elements.filter((_, i) => toNumber(arr[i]) === 1);
  }
  async remove(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.DEL', this.name, this.encodeValue(element))) === 1;
  }
  /** Java `count(V)`: CF.COUNT. */
  async count(element: string): Promise<number> {
    return toNumber(await tryDo(this.redis, 'CF.COUNT', this.name, this.encodeValue(element)));
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
   *  width/depth/decay (all three are sent together, defaults 8/7/0.9). */
  async init(k: number, width?: number, depth?: number, decay?: number): Promise<void>;
  async init(args: TopKInitArgs): Promise<void>;
  async init(a: number | TopKInitArgs, width?: number, depth?: number, decay?: number): Promise<void> {
    let k: number;
    if (typeof a === 'number') {
      k = a;
    } else {
      k = a.topK;
      if (a.width !== undefined || a.depth !== undefined || a.decay !== undefined) {
        width = a.width ?? 8;
        depth = a.depth ?? 7;
        decay = a.decay ?? 0.9;
      }
    }
    if (width === undefined) return this.reserve(k);
    const args: (string | number)[] = [this.name, k, width];
    if (depth !== undefined) args.push(depth);
    if (decay !== undefined) args.push(String(decay));
    await tryDo(this.redis, 'TOPK.RESERVE', ...args);
  }
  /** Java `add(V...)`: returns the evicted item per input (`null` when none). */
  async add(...elements: string[]): Promise<(string | null)[]> {
    if (elements.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TOPK.ADD', this.name, ...elements.map((e) => this.encodeValue(e)));
    return ((r ?? []) as any[]).map((x) => (x === null || x === undefined ? null : this.decodeValue(x)));
  }
  /** Java `incrementBy(V, int)`: TOPK.INCRBY → evicted item or null. */
  async incrementBy(element: string, increment: number): Promise<string | null>;
  /** Java `incrementBy(Map<V, Integer>)`: evicted item per entry. */
  async incrementBy(counts: Map<string, number> | Record<string, number>): Promise<(string | null)[]>;
  async incrementBy(
    a: string | Map<string, number> | Record<string, number>,
    increment?: number,
  ): Promise<string | null | (string | null)[]> {
    if (typeof a === 'string') {
      const r: any = await tryDo(this.redis, 'TOPK.INCRBY', this.name, this.encodeValue(a), increment ?? 1);
      const v = Array.isArray(r) ? r[0] : r;
      return v === null || v === undefined ? null : this.decodeValue(v);
    }
    const entries = a instanceof Map ? [...a.entries()] : Object.entries(a);
    if (entries.length === 0) return [];
    const args: (string | number)[] = [this.name];
    for (const [item, count] of entries) args.push(this.encodeValue(item), count);
    const r: any = await tryDo(this.redis, 'TOPK.INCRBY', ...args);
    return ((r ?? []) as any[]).map((x) => (x === null || x === undefined ? null : this.decodeValue(x)));
  }
  async contains(element: string): Promise<boolean>;
  async contains(elements: string[]): Promise<boolean[]>;
  async contains(a: string | string[]): Promise<boolean | boolean[]> {
    if (typeof a === 'string') {
      const r: any = await tryDo(this.redis, 'TOPK.QUERY', this.name, this.encodeValue(a));
      return toNumber(Array.isArray(r) ? r[0] : r) === 1;
    }
    if (a.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TOPK.QUERY', this.name, ...a.map((e) => this.encodeValue(e)));
    return ((r ?? []) as any[]).map((x) => toNumber(x) === 1);
  }
  async count(element: string): Promise<number>;
  async count(elements: string[]): Promise<number[]>;
  async count(a: string | string[]): Promise<number | number[]> {
    if (typeof a === 'string') {
      const r: any = await tryDo(this.redis, 'TOPK.COUNT', this.name, this.encodeValue(a));
      return toNumber(Array.isArray(r) ? r[0] : r);
    }
    if (a.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TOPK.COUNT', this.name, ...a.map((e) => this.encodeValue(e)));
    return ((r ?? []) as any[]).map((x) => toNumber(x));
  }
  async list(): Promise<string[]> {
    const r = (await tryDo(this.redis, 'TOPK.LIST', this.name)) as any[] | null;
    return ((r ?? []) as any[]).map((x) => this.decodeValue(x));
  }
  /** Java `listWithCount()`: TOPK.LIST ... WITHCOUNT → item→count. */
  async listWithCount(): Promise<Map<string, number>> {
    const r: any = await tryDo(this.redis, 'TOPK.LIST', this.name, 'WITHCOUNT');
    const out = new Map<string, number>();
    const arr = (r ?? []) as any[];
    for (let i = 0; i + 1 < arr.length; i += 2) out.set(this.decodeValue(arr[i]), Number(arr[i + 1]));
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
  async mergeWith(...names: string[]): Promise<void>;
  /** Java `mergeWith(TDigestMergeArgs)`: optional COMPRESSION/OVERRIDE. */
  async mergeWith(args: TDigestMergeArgs): Promise<void>;
  async mergeWith(a: string | TDigestMergeArgs, ...rest: string[]): Promise<void> {
    let keys: string[];
    let compression: number | undefined;
    let override = false;
    if (typeof a === 'string') {
      keys = [a, ...rest];
    } else {
      keys = a.keys;
      compression = a.compression;
      override = a.override ?? false;
    }
    const args: (string | number)[] = [this.name, keys.length, ...keys];
    if (compression !== undefined) args.push('COMPRESSION', compression);
    if (override) args.push('OVERRIDE');
    await tryDo(this.redis, 'TDIGEST.MERGE', ...args);
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

/** Java 4.8 `CountMinInitArgs`: byDimensions(width, depth) or
 *  byProbability(errorRate, probability) — the two are separate CMS commands. */
export interface CountMinInitArgs {
  byDimensions: boolean;
  width?: number;
  depth?: number;
  errorRate?: number;
  probability?: number;
}
export const CountMinInitArgs = {
  byDimensions(width: number, depth: number): CountMinInitArgs {
    return { byDimensions: true, width, depth };
  },
  byProbability(errorRate: number, probability: number): CountMinInitArgs {
    return { byDimensions: false, errorRate, probability };
  },
};
/** Java 4.8 `CountMinMergeArgs`. */
export interface CountMinMergeArgs { keys: string[]; weights?: number[] }

/** Java 4.8 `RCountMin`: Count-Min Sketch over the RedisBloom CMS commands
 *  (CMS.INITBYDIM/INITBYPROB/INCRBY/QUERY/MERGE/INFO; capability-gated). */
export class RCountMin<V = any> extends RedisDataStructure {
  async init(args: CountMinInitArgs): Promise<void> {
    if (args.byDimensions) {
      await tryDo(this.redis, 'CMS.INITBYDIM', this.name, args.width!, args.depth!);
    } else {
      await tryDo(this.redis, 'CMS.INITBYPROB', this.name, args.errorRate!, args.probability!);
    }
  }
  /** Java `add(element[, increment])` / `add(Map)`: CMS.INCRBY, returns the
   *  count after increment (per-element map for the Map form). */
  async add(element: V, increment?: number): Promise<number>;
  async add(elements: Map<V, number>): Promise<Map<V, number>>;
  async add(a: V | Map<V, number>, b?: number): Promise<number | Map<V, number>> {
    if (a instanceof Map) {
      const entries = [...a.entries()];
      if (entries.length === 0) return new Map();
      const args: (string | number)[] = ['CMS.INCRBY', this.name];
      for (const [e, inc] of entries) {
        if (inc < 0) throw new Error("redi.ts: increment can't be negative");
        args.push(this.encodeValue(e), inc);
      }
      const r = await tryDo(this.redis, ...args);
      const out = new Map<V, number>();
      entries.forEach(([e], i) => out.set(e, toNumber((r as any[])[i])));
      return out;
    }
    const inc = b ?? 1;
    if (inc < 0) throw new Error("redi.ts: increment can't be negative");
    const r = await tryDo(this.redis, 'CMS.INCRBY', this.name, this.encodeValue(a as V), inc);
    return toNumber(Array.isArray(r) ? r[0] : r);
  }
  /** Java `count(element)` / `count(Collection)`: CMS.QUERY. */
  async count(element: V): Promise<number>;
  async count(elements: V[]): Promise<Map<V, number>>;
  async count(a: V | V[]): Promise<number | Map<V, number>> {
    const list = Array.isArray(a) ? a : [a];
    if (list.length === 0) return new Map();
    const args: (string | number)[] = ['CMS.QUERY', this.name, ...list.map((e) => this.encodeValue(e))];
    const r = await tryDo(this.redis, ...args);
    if (Array.isArray(a)) {
      const out = new Map<V, number>();
      (a as V[]).forEach((e, i) => out.set(e, toNumber((r as any[])[i])));
      return out;
    }
    return toNumber(Array.isArray(r) ? r[0] : r);
  }
  /** Java 4.8 `mergeWith(CountMinMergeArgs)`: CMS.MERGE dest numkeys src [WEIGHTS]. */
  async mergeWith(args: CountMinMergeArgs): Promise<void> {
    const a: (string | number)[] = ['CMS.MERGE', this.name, args.keys.length, ...args.keys];
    if (args.weights?.length) a.push('WEIGHTS', ...args.weights);
    await tryDo(this.redis, ...a);
  }
  /** Java 4.8 `getInfo()`: CMS.INFO flat fields. */
  async getInfo(): Promise<Record<string, any>> {
    return flatRecord(await tryDo(this.redis, 'CMS.INFO', this.name));
  }
}

/** Java `GcraConfig`: maxBurst / tokensPerPeriod / period. Periods are plain
 *  millisecond numbers here (repo convention); the wire stores seconds. */
export interface GcraConfig {
  maxBurst: number;
  tokensPerPeriod: number;
  periodMs: number;
}

/** Java `GcraResult` — the 5-element GCRA reply. */
export interface GcraResult {
  limited: boolean;
  maxTokens: number;
  availableTokens: number;
  retryAfterSeconds: number;
  fullBurstAfterSeconds: number;
}

function gcraValidateRate(maxBurst: number, tokensPerPeriod: number, periodMs: number): void {
  if (maxBurst < 0) throw new Error("redi.ts: maxBurst can't be negative");
  if (tokensPerPeriod <= 0) throw new Error('redi.ts: tokensPerPeriod must be positive');
  if (!(periodMs > 0)) throw new Error('redi.ts: period must be positive');
}

/** Java `RedissonGcra.toSeconds(Duration)`: seconds, trailing zeros stripped. */
function gcraSeconds(periodMs: number): string {
  return String(periodMs / 1000);
}

export class RGcra extends RedisDataStructure {
  /** Java `getConfigName()` — suffixName(rawName, "config"). */
  private configKey: string = suffixName(this.name, 'config');

  /** Legacy convenience: one-shot GCRA call with inline limit/period.
   *  Returns true when allowed (fail-open like the native-facade defaults). */
  async acquire(limit: number, periodMs: number): Promise<boolean> {
    try {
      const r: any = await tryDo(this.redis, 'GCRA', this.name, limit, periodMs);
      return toNumber(r) === 1 || r === 'OK';
    } catch {
      return true;
    }
  }

  /** Java `trySetRate(maxBurst, tokensPerPeriod, Duration)`: HSETNX all three
   *  config fields; true only when the config was absent. Plain commands (the
   *  Java Lua is the same three HSETNX; sequential form is mock-compatible). */
  async trySetRate(maxBurst: number, tokensPerPeriod: number, periodMs: number): Promise<boolean> {
    gcraValidateRate(maxBurst, tokensPerPeriod, periodMs);
    await this.redis.hsetnx(this.configKey, 'maxBurst', String(maxBurst));
    await this.redis.hsetnx(this.configKey, 'rate', String(tokensPerPeriod));
    return toNumber(await this.redis.hsetnx(this.configKey, 'period', gcraSeconds(periodMs))) === 1;
  }

  /** Java `setRate`: overwrite the config hash and reset limiter state
   *  (DEL the raw key). */
  async setRate(maxBurst: number, tokensPerPeriod: number, periodMs: number): Promise<void> {
    gcraValidateRate(maxBurst, tokensPerPeriod, periodMs);
    await this.redis.hset(this.configKey,
      'maxBurst', String(maxBurst), 'rate', String(tokensPerPeriod), 'period', gcraSeconds(periodMs));
    await this.redis.del(this.name);
  }

  /** Java `getConfig`: HGETALL of the config hash; null when unset. */
  async getConfig(): Promise<GcraConfig | null> {
    const h: Record<string, string> = await this.redis.hgetall(this.configKey);
    if (!h || (h.maxBurst === undefined && h.rate === undefined && h.period === undefined)) return null;
    return {
      maxBurst: Number(h.maxBurst),
      tokensPerPeriod: Number(h.rate),
      periodMs: Math.round(Number(h.period) * 1000),
    };
  }

  /** Java `tryAcquire` / `tryAcquire(long tokens)`: read the config hash, then
   *  GCRA with TOKENS when tokens != 1. Throws when the rate is not set
   *  (Java Lua assert) or the server lacks GCRA. */
  async tryAcquire(tokens = 1): Promise<GcraResult> {
    if (!(tokens > 0)) throw new Error('redi.ts: tokens must be positive');
    const cfg = await this.redis.hmget(this.configKey, 'maxBurst', 'rate', 'period');
    const maxBurst = cfg[0];
    const rate = cfg[1];
    const period = cfg[2];
    if (maxBurst === null || maxBurst === undefined || rate === null || rate === undefined
      || period === null || period === undefined) {
      throw new Error('GCRA rate is not set');
    }
    const args: (string | number)[] = ['GCRA', this.name, maxBurst, rate, period];
    if (tokens !== 1) args.push('TOKENS', String(tokens));
    const r = await tryDo(this.redis, ...args);
    if (!Array.isArray(r) || r.length !== 5) {
      throw new Error(`redi.ts: unexpected GCRA reply: ${JSON.stringify(r)}`);
    }
    return {
      limited: toNumber(r[0]) === 1,
      maxTokens: toNumber(r[1]),
      availableTokens: toNumber(r[2]),
      retryAfterSeconds: toNumber(r[3]),
      fullBurstAfterSeconds: toNumber(r[4]),
    };
  }

  /** Java delete: raw key + config hash. */
  override async delete(): Promise<boolean> {
    await this.redis.del(this.name, this.configKey);
    return true;
  }

  override async unlink(): Promise<boolean> {
    await this.unlinkKeys([this.name, this.configKey]);
    return true;
  }
}

// -- Native TimeSeries (RedisTimeSeries module TS.* commands; Java 4.8) ------------------
//
// Thin binding over the module, mirroring Java's RTimeSeriesNative: a sample is
// a millisecond timestamp plus a double, labels index the whole series, and the
// wire format is the module's own (StringCodec on the Java side — no Redisson
// codec). Every TS.* command is module-gated (absent on plain Redis and on
// ioredis-mock); tests probe COMMAND INFO before use.
//
// Durations are plain millisecond numbers (repo convention), so Java
// `Duration.ofMinutes(5)` is `300000`.

/** Java `TSEncoding` (wire token == enum name). */
export type TSEncoding = 'COMPRESSED' | 'UNCOMPRESSED';

/** Java `TSDuplicatePolicy` (wire token == enum name). */
export type TSDuplicatePolicy = 'BLOCK' | 'FIRST' | 'LAST' | 'MIN' | 'MAX' | 'SUM';

/** Java `TSAggregation`; wire tokens that differ from the name are mapped in
 *  TS_AGGREGATION_TOKEN (std.p / std.s / var.p / var.s / countNaN / countAll). */
export type TSAggregation =
  | 'AVG' | 'SUM' | 'MIN' | 'MAX' | 'RANGE' | 'COUNT' | 'COUNT_NAN' | 'COUNT_ALL'
  | 'FIRST' | 'LAST' | 'STD_POPULATION' | 'STD_SAMPLE' | 'VAR_POPULATION'
  | 'VAR_SAMPLE' | 'TWA';

const TS_AGGREGATION_TOKEN: Record<TSAggregation, string> = {
  AVG: 'avg', SUM: 'sum', MIN: 'min', MAX: 'max', RANGE: 'range', COUNT: 'count',
  COUNT_NAN: 'countNaN', COUNT_ALL: 'countAll', FIRST: 'first', LAST: 'last',
  STD_POPULATION: 'std.p', STD_SAMPLE: 'std.s', VAR_POPULATION: 'var.p',
  VAR_SAMPLE: 'var.s', TWA: 'twa',
};

/** Java `TSReducer` — TSAggregation minus the tokens the server rejects as
 *  reducers (first/last/twa). */
export type TSReducer = Exclude<TSAggregation, 'FIRST' | 'LAST' | 'TWA'>;

/** Java `TSBucketTimestamp`. */
export type TSBucketTimestamp = 'START' | 'END' | 'MID';

const TS_BUCKET_TIMESTAMP_TOKEN: Record<TSBucketTimestamp, string> = {
  START: 'start', END: 'end', MID: 'mid',
};

/** One point in time: millisecond timestamp plus the values reported for it
 *  (one for a plain sample, one per aggregator / per queried series otherwise;
 *  NaN where nothing was reported). Java `TSSample`. */
export interface TSSample {
  timestamp: number;
  values: number[];
}

/** One row of a TS.MGET reply: reported labels and the last sample (null when
 *  the series is empty). Java `TSSeriesSample`. */
export interface TSSeriesSample {
  labels: Record<string, string | null>;
  sample: TSSample | null;
}

/** One row of a TS.MRANGE/TS.MREVRANGE reply. Java `TSSeriesSamples`. */
export interface TSSeriesSamples {
  labels: Record<string, string | null>;
  samples: TSSample[];
}

/** Compaction rule reported by TS.INFO. Java `TSRule` (bucketDuration in ms). */
export interface TSRule {
  destinationKey: string;
  bucketDurationMs: number;
  aggregation: TSAggregation | null;
  alignTimestamp: number;
}

/** Per-chunk detail of TS.INFO DEBUG. Java `TSChunkInfo`. */
export interface TSChunkInfo {
  startTimestamp: number;
  endTimestamp: number;
  samples: number;
  size: number;
  bytesPerSample: number;
}

/** TS.INFO snapshot. Java `TSInfo` (durations as ms numbers). */
export interface TSInfo {
  totalSamples: number;
  memoryUsage: number;
  firstTimestamp: number | null;
  lastTimestamp: number | null;
  retentionTimeMs: number;
  chunkCount: number;
  chunkSize: number;
  chunkType: TSEncoding | null;
  duplicatePolicy: TSDuplicatePolicy | null;
  labels: Record<string, string>;
  sourceKey: string | null;
  rules: TSRule[];
  ignoreMaxTimeDiffMs: number;
  ignoreMaxValueDiff: number;
  /** keySelfName/chunks are set only by getDebugInfo(). */
  keySelfName: string | null;
  chunks: TSChunkInfo[];
}

/** Shared RETENTION/ENCODING/CHUNK_SIZE/DUPLICATE_POLICY/IGNORE/LABELS block
 *  (Java `TSCreationParams`, appended by `appendCreationOptions`). */
abstract class TSCreationOptions {
  protected retentionMsValue?: number;
  protected encodingValue?: TSEncoding;
  protected chunkSizeValue?: number;
  protected duplicatePolicyValue?: TSDuplicatePolicy;
  protected ignoreMaxTimeDiffMsValue?: number;
  protected ignoreMaxValueDiffValue?: number;
  protected labelsValue?: [string, string][];

  /** Java `retention(Duration)` — ms here; 0 keeps samples forever. */
  retention(retentionMs: number): this {
    if (!Number.isInteger(retentionMs) || retentionMs < 0) {
      throw new Error(`redi.ts: retention must be a non-negative whole number of ms: ${retentionMs}`);
    }
    this.retentionMsValue = retentionMs;
    return this;
  }
  encoding(encoding: TSEncoding): this {
    this.encodingValue = encoding;
    return this;
  }
  chunkSize(chunkSize: number): this {
    this.chunkSizeValue = chunkSize;
    return this;
  }
  duplicatePolicy(duplicatePolicy: TSDuplicatePolicy): this {
    this.duplicatePolicyValue = duplicatePolicy;
    return this;
  }
  ignore(maxTimeDiffMs: number, maxValueDiff: number): this {
    this.ignoreMaxTimeDiffMsValue = maxTimeDiffMs;
    this.ignoreMaxValueDiffValue = maxValueDiff;
    return this;
  }
  labels(labels: Record<string, string>): this {
    this.labelsValue = Object.entries(labels);
    return this;
  }
  label(name: string, value: string): this {
    if (!this.labelsValue) this.labelsValue = [];
    this.labelsValue.push([name, value]);
    return this;
  }

  /** Verbatim Java `RedissonTimeSeriesNative.appendCreationOptions`.
   *  @internal wire assembly. */
  appendCreationOptions(cmdArgs: (string | number)[]): void {
    if (this.retentionMsValue !== undefined) cmdArgs.push('RETENTION', this.retentionMsValue);
    if (this.encodingValue !== undefined) cmdArgs.push('ENCODING', this.encodingValue);
    if (this.chunkSizeValue !== undefined) cmdArgs.push('CHUNK_SIZE', this.chunkSizeValue);
    if (this.duplicatePolicyValue !== undefined) cmdArgs.push('DUPLICATE_POLICY', this.duplicatePolicyValue);
    if (this.ignoreMaxTimeDiffMsValue !== undefined) {
      cmdArgs.push('IGNORE', this.ignoreMaxTimeDiffMsValue, this.ignoreMaxValueDiffValue!);
    }
    if (this.labelsValue !== undefined) {
      cmdArgs.push('LABELS');
      for (const [name, value] of this.labelsValue) cmdArgs.push(name, value);
    }
  }
}

/** Java `TSCreateArgs`. */
export class TSCreateArgs extends TSCreationOptions {
  static defaults(): TSCreateArgs {
    return new TSCreateArgs();
  }
}

/** Java `TSAlterArgs`. */
export class TSAlterArgs extends TSCreationOptions {}

/** Java `TSAddArgs`: a sample plus per-call options. */
export class TSAddArgs extends TSCreationOptions {
  private onDuplicateValue?: TSDuplicatePolicy;
  private constructor(readonly timestamp: number | null, readonly value: number) {
    super();
  }
  static sample(timestamp: number, value: number): TSAddArgs {
    return new TSAddArgs(timestamp, value);
  }
  static currentSample(value: number): TSAddArgs {
    return new TSAddArgs(null, value);
  }
  /** Per-sample policy override — distinct from the series' own (duplicatePolicy). */
  onDuplicate(duplicatePolicy: TSDuplicatePolicy): this {
    this.onDuplicateValue = duplicatePolicy;
    return this;
  }
  /** @internal wire assembly (key first, then timestamp/value, then options). */
  appendArgs(cmdArgs: (string | number)[]): void {
    cmdArgs.push(this.timestamp === null ? '*' : this.timestamp, this.value);
    if (this.onDuplicateValue !== undefined) cmdArgs.push('ON_DUPLICATE', this.onDuplicateValue);
    this.appendCreationOptions(cmdArgs);
  }
}

/** Java `TSIncrArgs`. */
export class TSIncrArgs extends TSCreationOptions {
  private timestampValue?: number;
  private constructor(readonly value: number) {
    super();
  }
  static value(value: number): TSIncrArgs {
    return new TSIncrArgs(value);
  }
  timestamp(timestamp: number): this {
    this.timestampValue = timestamp;
    return this;
  }
  /** @internal [key, value, TIMESTAMP?, ts?, creation...] */
  appendArgs(key: string): (string | number)[] {
    const cmdArgs: (string | number)[] = [key, this.value];
    if (this.timestampValue !== undefined) cmdArgs.push('TIMESTAMP', this.timestampValue);
    this.appendCreationOptions(cmdArgs);
    return cmdArgs;
  }
}

/** Shared range bound/filter/aggregation state (Java `TSBaseRangeParams`).
 *  from/to are the module's tokens: a number string, or the '-'/'+' sentinels. */
abstract class TSBaseRangeState {
  protected static readonly EARLIEST = '-';
  protected static readonly LATEST = '+';
  protected fromToken = '-';
  protected toToken = '+';
  protected latestValue = false;
  protected filterByTimestampsValue?: number[];
  protected filterByMinValueValue?: number;
  protected filterByMaxValueValue?: number;
  protected countValue?: number;
  protected alignValue?: string;
  protected bucketDurationMsValue?: number;
  protected aggregationsValue?: TSAggregation[][];
  protected bucketTimestampValue?: TSBucketTimestamp;
  protected emptyValue = false;

  setRange(from: number, to: number): void {
    this.fromToken = String(from);
    this.toToken = String(to);
  }
  setFrom(from: number): void {
    this.fromToken = String(from);
    this.toToken = '+';
  }
  setTo(to: number): void {
    this.fromToken = '-';
    this.toToken = String(to);
  }
  setAll(): void {
    this.fromToken = '-';
    this.toToken = '+';
  }
  latest(): this {
    this.latestValue = true;
    return this;
  }
  filterByTimestamp(...timestamps: number[]): this {
    this.filterByTimestampsValue = timestamps;
    return this;
  }
  filterByValue(min: number, max: number): this {
    this.filterByMinValueValue = min;
    this.filterByMaxValueValue = max;
    return this;
  }
  count(count: number): this {
    this.countValue = count;
    return this;
  }
  alignStart(): this {
    this.alignValue = '-';
    return this;
  }
  alignEnd(): this {
    this.alignValue = '+';
    return this;
  }
  align(timestamp: number): this {
    this.alignValue = String(timestamp);
    return this;
  }
  setAggregations(bucketMs: number, aggregations: TSAggregation[][]): this {
    this.bucketDurationMsValue = bucketMs;
    this.aggregationsValue = aggregations;
    return this;
  }
  bucketTimestamp(bucketTimestamp: TSBucketTimestamp): this {
    this.bucketTimestampValue = bucketTimestamp;
    return this;
  }
  empty(): this {
    this.emptyValue = true;
    return this;
  }

  /** Verbatim Java `RedissonTimeSeriesNative.appendRangeOptions` (selection
   *  goes between FILTER_BY_VALUE and COUNT).
   *  @internal wire assembly. */
  appendRangeOptions(cmdArgs: (string | number)[], selection: (string | number)[]): void {
    cmdArgs.push(this.fromToken, this.toToken);
    if (this.latestValue) cmdArgs.push('LATEST');
    if (this.filterByTimestampsValue !== undefined) {
      cmdArgs.push('FILTER_BY_TS', ...this.filterByTimestampsValue);
    }
    if (this.filterByMinValueValue !== undefined) {
      cmdArgs.push('FILTER_BY_VALUE', this.filterByMinValueValue, this.filterByMaxValueValue!);
    }
    cmdArgs.push(...selection);
    if (this.countValue !== undefined) cmdArgs.push('COUNT', this.countValue);
    if (this.alignValue !== undefined) {
      if (this.aggregationsValue === undefined) {
        throw new Error('redi.ts: align() requires aggregation() — the module rejects ALIGN on its own');
      }
      if (this.alignValue === '-' && this.fromToken === '-') {
        throw new Error('redi.ts: alignStart() requires a range with an explicit start');
      }
      if (this.alignValue === '+' && this.toToken === '+') {
        throw new Error('redi.ts: alignEnd() requires a range with an explicit end');
      }
    }
    if (this.aggregationsValue !== undefined) {
      if (this.alignValue !== undefined) cmdArgs.push('ALIGN', this.alignValue);
      cmdArgs.push('AGGREGATION');
      for (const perKey of this.aggregationsValue) {
        cmdArgs.push(perKey.map((a) => TS_AGGREGATION_TOKEN[a]).join(','));
      }
      cmdArgs.push(this.bucketDurationMsValue!);
      if (this.bucketTimestampValue !== undefined) {
        cmdArgs.push('BUCKETTIMESTAMP', TS_BUCKET_TIMESTAMP_TOKEN[this.bucketTimestampValue]);
      }
      if (this.emptyValue) cmdArgs.push('EMPTY');
    }
  }
}

/** Java `TSRangeArgs` (single series). */
export class TSRangeArgs extends TSBaseRangeState {
  static range(fromTimestamp: number, toTimestamp: number): TSRangeArgs {
    const a = new TSRangeArgs();
    a.setRange(fromTimestamp, toTimestamp);
    return a;
  }
  static from(fromTimestamp: number): TSRangeArgs {
    const a = new TSRangeArgs();
    a.setFrom(fromTimestamp);
    return a;
  }
  static to(toTimestamp: number): TSRangeArgs {
    const a = new TSRangeArgs();
    a.setTo(toTimestamp);
    return a;
  }
  static all(): TSRangeArgs {
    return new TSRangeArgs();
  }
  aggregation(bucketMs: number, ...aggregations: TSAggregation[]): this {
    return this.setAggregations(bucketMs, [aggregations]);
  }
}

/** Java `TSReadArgs`. */
export class TSReadArgs {
  private blockTimeoutMsValue?: number;
  private blockMinCountValue?: number;
  private maxCountValue?: number;
  private constructor(readonly timestamp: string) {}
  static from(timestamp: number): TSReadArgs {
    return new TSReadArgs(String(timestamp));
  }
  static fromEarliest(): TSReadArgs {
    return new TSReadArgs('-');
  }
  static fromLast(): TSReadArgs {
    return new TSReadArgs('+');
  }
  static fromNext(): TSReadArgs {
    return new TSReadArgs('$');
  }
  block(timeoutMs: number, minCount: number): this {
    if (timeoutMs < 1) {
      throw new Error('redi.ts: block timeout under 1ms would mean forever on the wire');
    }
    this.blockTimeoutMsValue = timeoutMs;
    this.blockMinCountValue = minCount;
    return this;
  }
  maxCount(maxCount: number): this {
    this.maxCountValue = maxCount;
    return this;
  }
  getBlockTimeoutMs(): number | undefined {
    return this.blockTimeoutMsValue;
  }
  getMinCount(): number | undefined {
    return this.blockMinCountValue;
  }
  getMaxCount(): number | undefined {
    return this.maxCountValue;
  }
}

/** Java `TSRuleArgs` (TS.CREATERULE). */
export class TSRuleArgs {
  private alignTimestampValue?: number;
  private constructor(
    readonly destinationKey: string,
    readonly aggregation: TSAggregation,
    readonly bucketDurationMs: number,
  ) {}
  static destination(destinationKey: string, aggregation: TSAggregation, bucketDurationMs: number): TSRuleArgs {
    return new TSRuleArgs(destinationKey, aggregation, bucketDurationMs);
  }
  alignTimestamp(timestamp: number): this {
    this.alignTimestampValue = timestamp;
    return this;
  }
  getAlignTimestamp(): number | undefined {
    return this.alignTimestampValue;
  }
}

/** Java label-selection block shared by TS.MGET / TS.MRANGE. */
function tsLabelSelection(withLabels: boolean, selectedLabels?: string[]): (string | number)[] {
  if (selectedLabels !== undefined && withLabels) {
    throw new Error('redi.ts: withLabels() and selectedLabels() are mutually exclusive');
  }
  if (selectedLabels !== undefined) {
    if (selectedLabels.length === 0) throw new Error('redi.ts: selectedLabels() requires at least one label');
    return ['SELECTED_LABELS', ...selectedLabels];
  }
  if (withLabels) return ['WITHLABELS'];
  return [];
}

function tsRequireFilters(filters: string[]): void {
  if (!filters || filters.length === 0) {
    throw new Error('redi.ts: at least one filter expression is required');
  }
}

/** Java `TSMultiGetArgs` (TS.MGET). */
export class TSMultiGetArgs {
  private latestValue = false;
  private withLabelsValue = false;
  private selectedLabelsValue?: string[];
  private constructor(readonly filters: string[]) {}
  static filter(...filters: string[]): TSMultiGetArgs {
    return new TSMultiGetArgs(filters);
  }
  latest(): this {
    this.latestValue = true;
    return this;
  }
  withLabels(): this {
    this.withLabelsValue = true;
    return this;
  }
  selectedLabels(...labels: string[]): this {
    this.selectedLabelsValue = labels;
    return this;
  }
  /** @internal */
  appendArgs(): (string | number)[] {
    tsRequireFilters(this.filters);
    const cmdArgs: (string | number)[] = [];
    if (this.latestValue) cmdArgs.push('LATEST');
    cmdArgs.push(...tsLabelSelection(this.withLabelsValue, this.selectedLabelsValue));
    cmdArgs.push('FILTER', ...this.filters);
    return cmdArgs;
  }
}

/** Java `TSMultiRangeArgs` (TS.MRANGE / TS.MREVRANGE). */
export class TSMultiRangeArgs extends TSBaseRangeState {
  private withLabelsValue = false;
  private selectedLabelsValue?: string[];
  private groupByLabelValue?: string;
  private reducerValue?: TSReducer;
  private excludeEmptyValue = false;
  private constructor(readonly filters: string[]) {
    super();
  }
  static filter(...filters: string[]): TSMultiRangeArgs {
    return new TSMultiRangeArgs(filters);
  }
  range(fromTimestamp: number, toTimestamp: number): this {
    this.setRange(fromTimestamp, toTimestamp);
    return this;
  }
  from(fromTimestamp: number): this {
    this.setFrom(fromTimestamp);
    return this;
  }
  to(toTimestamp: number): this {
    this.setTo(toTimestamp);
    return this;
  }
  all(): this {
    this.setAll();
    return this;
  }
  withLabels(): this {
    this.withLabelsValue = true;
    return this;
  }
  selectedLabels(...labels: string[]): this {
    this.selectedLabelsValue = labels;
    return this;
  }
  aggregation(bucketMs: number, ...aggregations: TSAggregation[]): this {
    return this.setAggregations(bucketMs, [aggregations]);
  }
  groupBy(label: string, reducer: TSReducer): this {
    this.groupByLabelValue = label;
    this.reducerValue = reducer;
    return this;
  }
  excludeEmpty(): this {
    this.excludeEmptyValue = true;
    return this;
  }
  /** @internal (EXCLUDEEMPTY/FILTER/GROUPBY trail appendRangeOptions). */
  appendArgs(): (string | number)[] {
    tsRequireFilters(this.filters);
    if (this.groupByLabelValue !== undefined) {
      if (this.excludeEmptyValue) {
        throw new Error('redi.ts: groupBy() and excludeEmpty() are mutually exclusive');
      }
      if (this.aggregationsValue !== undefined && this.aggregationsValue[0].length > 1) {
        throw new Error('redi.ts: groupBy() is mutually exclusive with an aggregation() of more than one aggregator');
      }
    }
    const cmdArgs: (string | number)[] = [];
    this.appendRangeOptions(cmdArgs, tsLabelSelection(this.withLabelsValue, this.selectedLabelsValue));
    if (this.excludeEmptyValue) cmdArgs.push('EXCLUDEEMPTY');
    cmdArgs.push('FILTER', ...this.filters);
    if (this.groupByLabelValue !== undefined) {
      cmdArgs.push('GROUPBY', this.groupByLabelValue, 'REDUCE', TS_AGGREGATION_TOKEN[this.reducerValue!]);
    }
    return cmdArgs;
  }
}

/** Java `TSGroupedRangeArgs` (TS.NRANGE / TS.NREVRANGE). */
export class TSGroupedRangeArgs extends TSBaseRangeState {
  private constructor(readonly keys: string[]) {
    super();
  }
  static keys(...keys: string[]): TSGroupedRangeArgs {
    return new TSGroupedRangeArgs(keys);
  }
  range(fromTimestamp: number, toTimestamp: number): this {
    this.setRange(fromTimestamp, toTimestamp);
    return this;
  }
  from(fromTimestamp: number): this {
    this.setFrom(fromTimestamp);
    return this;
  }
  to(toTimestamp: number): this {
    this.setTo(toTimestamp);
    return this;
  }
  all(): this {
    this.setAll();
    return this;
  }
  /** Same aggregation repeated for every key (server wants one spec per key). */
  aggregation(bucketMs: number, ...aggregations: TSAggregation[]): this {
    return this.setAggregations(bucketMs, this.keys.map(() => aggregations));
  }
  /** One aggregator list per key, in key order. */
  aggregations(bucketMs: number, aggregations: TSAggregation[][]): this {
    return this.setAggregations(bucketMs, aggregations);
  }
  /** @internal */
  appendArgs(): (string | number)[] {
    if (this.keys.length === 0) throw new Error('redi.ts: at least one key is required');
    if (this.aggregationsValue !== undefined && this.aggregationsValue.length !== this.keys.length) {
      throw new Error(`redi.ts: aggregations must hold one entry per key: ${this.keys.length} expected, ${this.aggregationsValue.length} given`);
    }
    const cmdArgs: (string | number)[] = [this.keys.length, ...this.keys];
    this.appendRangeOptions(cmdArgs, []);
    return cmdArgs;
  }
}

// TS.* reply decoders (RESP shapes as ioredis returns them) ------------------------------

/** @internal shared with the RBatch TS native facade. */
export function tsToSample(raw: any): TSSample | null {
  if (!raw || !Array.isArray(raw) || raw.length === 0) return null;
  const values = Array.isArray(raw[1]) ? (raw[1] as any[]) : [raw[1]];
  return { timestamp: Number(raw[0]), values: values.map((v) => Number(v)) };
}

/** @internal shared with the RBatch TS native facade. */
export function tsToSamples(raw: any): TSSample[] {
  const arr = (raw ?? []) as any[];
  return arr.map((s) => tsToSample(s)!).filter((s) => s !== null);
}

function tsToLabels(raw: any): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  for (const pair of (raw ?? []) as any[]) {
    if (Array.isArray(pair) && pair.length >= 2) out[String(pair[0])] = String(pair[1]);
  }
  return out;
}

const TS_AGGREGATION_BY_TOKEN = new Map<string, TSAggregation>(
  (Object.keys(TS_AGGREGATION_TOKEN) as TSAggregation[]).map((k) => [TS_AGGREGATION_TOKEN[k], k]),
);

/** @internal shared with the RBatch TS native facade. */
export function tsInfoDecode(flat: any): TSInfo {
  const info: TSInfo = {
    totalSamples: 0, memoryUsage: 0, firstTimestamp: null, lastTimestamp: null,
    retentionTimeMs: 0, chunkCount: 0, chunkSize: 0, chunkType: null, duplicatePolicy: null,
    labels: {}, sourceKey: null, rules: [], ignoreMaxTimeDiffMs: 0, ignoreMaxValueDiff: 0,
    keySelfName: null, chunks: [],
  };
  const parts = (flat ?? []) as any[];
  for (let i = 0; i + 1 < parts.length; i += 2) {
    const name = String(parts[i]);
    const value = parts[i + 1];
    if (value === null || value === undefined) continue;
    switch (name) {
      case 'totalSamples': info.totalSamples = Number(value); break;
      case 'memoryUsage': info.memoryUsage = Number(value); break;
      case 'firstTimestamp': info.firstTimestamp = Number(value); break;
      case 'lastTimestamp': info.lastTimestamp = Number(value); break;
      case 'retentionTime': info.retentionTimeMs = Number(value); break;
      case 'chunkCount': info.chunkCount = Number(value); break;
      case 'chunkSize': info.chunkSize = Number(value); break;
      case 'chunkType': info.chunkType = String(value).toUpperCase() as TSEncoding; break;
      case 'duplicatePolicy': info.duplicatePolicy = String(value).toUpperCase() as TSDuplicatePolicy; break;
      // TS.INFO labels never carry nulls (only SELECTED_LABELS replies do)
      case 'labels': info.labels = tsToLabels(value) as Record<string, string>; break;
      case 'sourceKey': info.sourceKey = String(value); break;
      case 'rules':
        info.rules = ((value ?? []) as any[]).map((rule: any[]) => ({
          destinationKey: String(rule[0]),
          bucketDurationMs: Number(rule[1][0]),
          aggregation: TS_AGGREGATION_BY_TOKEN.get(String(rule[1][1]).toLowerCase()) ?? null,
          alignTimestamp: Number(rule[1][2]),
        }));
        break;
      case 'ignoreMaxTimeDiff': info.ignoreMaxTimeDiffMs = Number(value); break;
      case 'ignoreMaxValDiff': info.ignoreMaxValueDiff = Number(value); break;
      case 'keySelfName': info.keySelfName = String(value); break;
      case 'Chunks':
        info.chunks = ((value ?? []) as any[]).map((chunk: any[]) => {
          const c: TSChunkInfo = { startTimestamp: 0, endTimestamp: 0, samples: 0, size: 0, bytesPerSample: 0 };
          for (let j = 0; j + 1 < chunk.length; j += 2) {
            const key = String(chunk[j]);
            if (key === 'startTimestamp') c.startTimestamp = Number(chunk[j + 1]);
            else if (key === 'endTimestamp') c.endTimestamp = Number(chunk[j + 1]);
            else if (key === 'samples') c.samples = Number(chunk[j + 1]);
            else if (key === 'size') c.size = Number(chunk[j + 1]);
            else if (key === 'bytesPerSample') c.bytesPerSample = Number(chunk[j + 1]);
          }
          return c;
        });
        break;
    }
  }
  return info;
}

/** Java 4.8 `RTimeSeriesNative`: single-series binding over the RedisTimeSeries
 *  module's TS.* commands. All methods require the module (Redis 8.0+, or the
 *  RedisTimeSeries module loaded); on clients without it they reject. */
export class RTimeSeriesNative extends RedisDataStructure {
  /** Java `create()` / `create(TSCreateArgs)`: TS.CREATE. */
  async create(args?: TSCreateArgs): Promise<void> {
    const cmdArgs: (string | number)[] = [this.name];
    args?.appendCreationOptions(cmdArgs);
    await tryDo(this.redis, 'TS.CREATE', ...cmdArgs);
  }
  /** Java `createIfAbsent(...)`: EXISTS-guarded TS.CREATE via Lua — an
   *  existing key reports false instead of an error. */
  async createIfAbsent(args?: TSCreateArgs): Promise<boolean> {
    const argv: (string | number)[] = [];
    args?.appendCreationOptions(argv);
    const s = "if redis.call('exists', KEYS[1]) == 0 then " +
      "redis.call('TS.CREATE', KEYS[1], unpack(ARGV)); return 1; end; return 0;";
    return toNumber(await luaEval(this.redis, s, 1, this.name, ...argv.map(String))) === 1;
  }
  /** Java `alter(TSAlterArgs)`: TS.ALTER (LABELS replace the whole set). */
  async alter(args: TSAlterArgs): Promise<void> {
    const cmdArgs: (string | number)[] = [this.name];
    args.appendCreationOptions(cmdArgs);
    await tryDo(this.redis, 'TS.ALTER', ...cmdArgs);
  }
  /** Java `add(long, double)` / `addCurrent(double)` / `add(TSAddArgs)`: TS.ADD. */
  async add(timestamp: number, value: number): Promise<number>;
  async add(args: TSAddArgs): Promise<number>;
  async add(a: number | TSAddArgs, value?: number): Promise<number> {
    if (typeof a === 'number') return toNumber(await tryDo(this.redis, 'TS.ADD', this.name, a, value!));
    const cmdArgs: (string | number)[] = [this.name];
    a.appendArgs(cmdArgs);
    return toNumber(await tryDo(this.redis, 'TS.ADD', ...cmdArgs));
  }
  /** Java `addCurrent(double)`: TS.ADD key * value (server-stamped). */
  async addCurrent(value: number): Promise<number> {
    return toNumber(await tryDo(this.redis, 'TS.ADD', this.name, '*', value));
  }
  /** Java `addAll(Map<Long, Double>)`: TS.MADD with this key repeated — the
   *  module refuses a key that is not already a series. */
  async addAll(samples: Map<number, number> | Record<number, number>): Promise<number[]> {
    const entries = samples instanceof Map ? [...samples.entries()] : (Object.entries(samples) as [string, number][]).map(([k, v]) => [Number(k), v]);
    if (entries.length === 0) return [];
    const cmdArgs: (string | number)[] = [];
    for (const [ts, value] of entries) cmdArgs.push(this.name, ts, value);
    const r = (await tryDo(this.redis, 'TS.MADD', ...cmdArgs)) as any[];
    return (r ?? []).map((x) => Number(x));
  }
  /** Java `incrementBy(double)` / `incrementBy(TSIncrArgs)`: TS.INCRBY. */
  async incrementBy(addend: number): Promise<number>;
  async incrementBy(args: TSIncrArgs): Promise<number>;
  async incrementBy(a: number | TSIncrArgs): Promise<number> {
    const cmdArgs = typeof a === 'number' ? [this.name, a] : a.appendArgs(this.name);
    return toNumber(await tryDo(this.redis, 'TS.INCRBY', ...cmdArgs));
  }
  /** Java `decrementBy(double)` / `decrementBy(TSIncrArgs)`: TS.DECRBY. */
  async decrementBy(subtrahend: number): Promise<number>;
  async decrementBy(args: TSIncrArgs): Promise<number>;
  async decrementBy(a: number | TSIncrArgs): Promise<number> {
    const cmdArgs = typeof a === 'number' ? [this.name, a] : a.appendArgs(this.name);
    return toNumber(await tryDo(this.redis, 'TS.DECRBY', ...cmdArgs));
  }
  /** Java `removeRange(long, long)`: TS.DEL (inclusive bounds). */
  async removeRange(fromTimestamp: number, toTimestamp: number): Promise<number> {
    return toNumber(await tryDo(this.redis, 'TS.DEL', this.name, fromTimestamp, toTimestamp));
  }
  /** Java `first()`: TS.RANGE - + COUNT 1. */
  async first(): Promise<TSSample | null> {
    const r = await tryDo(this.redis, 'TS.RANGE', this.name, '-', '+', 'COUNT', 1);
    return tsToSamples(r)[0] ?? null;
  }
  /** Java `size()`: TS.INFO totalSamples. */
  async size(): Promise<number> {
    return tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name)).totalSamples;
  }
  /** Java `firstTimestamp()`: TS.INFO firstTimestamp (0 on an empty series). */
  async firstTimestamp(): Promise<number> {
    return tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name)).firstTimestamp ?? 0;
  }
  /** Java `lastTimestamp()`: TS.INFO lastTimestamp. */
  async lastTimestamp(): Promise<number> {
    return tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name)).lastTimestamp ?? 0;
  }
  /** Java `get()`: TS.GET — the last sample, null when empty. */
  async get(): Promise<TSSample | null> {
    return tsToSample(await tryDo(this.redis, 'TS.GET', this.name));
  }
  /** Java `getLatest()`: TS.GET LATEST (compacts the open bucket). */
  async getLatest(): Promise<TSSample | null> {
    return tsToSample(await tryDo(this.redis, 'TS.GET', this.name, 'LATEST'));
  }
  /** Java `range(long, long)` / `range(TSRangeArgs)`: TS.RANGE. */
  async range(fromTimestamp: number, toTimestamp: number): Promise<TSSample[]>;
  async range(args: TSRangeArgs): Promise<TSSample[]>;
  async range(a: number | TSRangeArgs, to?: number): Promise<TSSample[]> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') cmdArgs.push(a, to!);
    else a.appendRangeOptions(cmdArgs, []);
    return tsToSamples(await tryDo(this.redis, 'TS.RANGE', ...cmdArgs));
  }
  /** Java `rangeReversed(long, long)` / `rangeReversed(TSRangeArgs)`: TS.REVRANGE. */
  async rangeReversed(fromTimestamp: number, toTimestamp: number): Promise<TSSample[]>;
  async rangeReversed(args: TSRangeArgs): Promise<TSSample[]>;
  async rangeReversed(a: number | TSRangeArgs, to?: number): Promise<TSSample[]> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') cmdArgs.push(a, to!);
    else a.appendRangeOptions(cmdArgs, []);
    return tsToSamples(await tryDo(this.redis, 'TS.REVRANGE', ...cmdArgs));
  }
  /** Java `read(long)` / `read(TSReadArgs)`: TS.READ (BLOCK holds the connection). */
  async read(timestamp: number): Promise<TSSample[]>;
  async read(args: TSReadArgs): Promise<TSSample[]>;
  async read(a: number | TSReadArgs): Promise<TSSample[]> {
    const cmdArgs: (string | number)[] = [this.name];
    if (typeof a === 'number') {
      cmdArgs.push(a);
    } else {
      cmdArgs.push(a.timestamp);
      if (a.getBlockTimeoutMs() !== undefined) cmdArgs.push('BLOCK', a.getBlockTimeoutMs()!, a.getMinCount()!);
      if (a.getMaxCount() !== undefined) cmdArgs.push('MAX_COUNT', a.getMaxCount()!);
    }
    return tsToSamples(await tryDo(this.redis, 'TS.READ', ...cmdArgs));
  }
  /** Java `createRule(TSRuleArgs)`: TS.CREATERULE (destination must exist). */
  async createRule(args: TSRuleArgs): Promise<void> {
    const cmdArgs: (string | number)[] = [
      this.name, args.destinationKey, 'AGGREGATION', TS_AGGREGATION_TOKEN[args.aggregation], args.bucketDurationMs,
    ];
    if (args.getAlignTimestamp() !== undefined) cmdArgs.push(args.getAlignTimestamp()!);
    await tryDo(this.redis, 'TS.CREATERULE', ...cmdArgs);
  }
  /** Java `deleteRule(String)`: TS.DELETERULE. */
  async deleteRule(destinationKey: string): Promise<void> {
    await tryDo(this.redis, 'TS.DELETERULE', this.name, destinationKey);
  }
  /** Java `iterator()` / `iterator(int)`: paged TS.RANGE from cursor, oldest
   *  first, `count` samples per round trip (default 10). */
  async *iterator(count = 10): AsyncGenerator<TSSample> {
    let from = 0;
    for (;;) {
      const batch = await this.range(TSRangeArgs.from(from).count(count));
      for (const s of batch) yield s;
      if (batch.length < count) return;
      from = batch[batch.length - 1].timestamp + 1;
    }
  }
  /** Java `stream()` / `stream(int)` — same walk as iterator(). */
  stream(count = 10): AsyncGenerator<TSSample> {
    return this.iterator(count);
  }
  /** Java `getLabels()`: TS.INFO labels. */
  async getLabels(): Promise<Record<string, string>> {
    const labels = tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name)).labels;
    return Object.fromEntries(Object.entries(labels).map(([k, v]) => [k, v as string]));
  }
  /** Java `getInfo()`: TS.INFO. */
  async getInfo(): Promise<TSInfo> {
    return tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name));
  }
  /** Java `getDebugInfo()`: TS.INFO DEBUG (per-chunk detail). */
  async getDebugInfo(): Promise<TSInfo> {
    return tsInfoDecode(await tryDo(this.redis, 'TS.INFO', this.name, 'DEBUG'));
  }
}

/** Java 4.8 `RTimeSeriesNatives`: TS.* operations that span several series
 *  (TS.MADD / TS.MGET / TS.MRANGE / TS.MREVRANGE / TS.NRANGE / TS.NREVRANGE /
 *  TS.QUERYINDEX / TS.QUERYLABELS). Module-gated like RTimeSeriesNative. */
export class RTimeSeriesNatives {
  constructor(private redis: Redis) {}

  /** Java `addAll(Map<String, Map<Long, Double>>)`: one TS.MADD over every
   *  key/timestamp/value triple; reply split per key by sample count. */
  async addAll(samples: Map<string, Map<number, number>> | Record<string, Record<number, number>>): Promise<Map<string, number[]>> {
    const entries = samples instanceof Map
      ? [...samples.entries()].map(([k, m]) => [k, m instanceof Map ? [...m.entries()] : Object.entries(m).map(([t, v]) => [Number(t), v] as [number, number])] as [string, [number, number][]])
      : Object.entries(samples).map(([k, rec]) => [k, Object.entries(rec).map(([t, v]) => [Number(t), v] as [number, number])] as [string, [number, number][]]);
    const out = new Map<string, number[]>();
    if (entries.length === 0) return out;
    const cmdArgs: (string | number)[] = [];
    for (const [key, list] of entries) {
      for (const [ts, value] of list) cmdArgs.push(key, ts, value);
    }
    const flat = ((await tryDo(this.redis, 'TS.MADD', ...cmdArgs)) as any[]) ?? [];
    let i = 0;
    for (const [key, list] of entries) {
      out.set(key, list.map(() => Number(flat[i++])));
    }
    return out;
  }
  /** Java `getAll(TSMultiGetArgs)`: TS.MGET — last sample per matching series. */
  async getAll(args: TSMultiGetArgs): Promise<Map<string, TSSeriesSample>> {
    const rows = ((await tryDo(this.redis, 'TS.MGET', ...args.appendArgs())) as any[]) ?? [];
    const out = new Map<string, TSSeriesSample>();
    for (const row of rows) {
      // [key, labels, sample] — sample is [] for an empty series
      out.set(String(row[0]), { labels: tsToLabels(row[1]), sample: tsToSample(row[2]) });
    }
    return out;
  }
  /** Java `range(TSMultiRangeArgs)`: TS.MRANGE. */
  async range(args: TSMultiRangeArgs): Promise<Map<string, TSSeriesSamples>> {
    return this.multiRange('TS.MRANGE', args);
  }
  /** Java `rangeReversed(TSMultiRangeArgs)`: TS.MREVRANGE. */
  async rangeReversed(args: TSMultiRangeArgs): Promise<Map<string, TSSeriesSamples>> {
    return this.multiRange('TS.MREVRANGE', args);
  }
  private async multiRange(command: 'TS.MRANGE' | 'TS.MREVRANGE', args: TSMultiRangeArgs): Promise<Map<string, TSSeriesSamples>> {
    const rows = ((await tryDo(this.redis, command, ...args.appendArgs())) as any[]) ?? [];
    const out = new Map<string, TSSeriesSamples>();
    for (const row of rows) {
      out.set(String(row[0]), { labels: tsToLabels(row[1]), samples: tsToSamples(row[2]) });
    }
    return out;
  }
  /** Java `groupedRange(TSGroupedRangeArgs)`: TS.NRANGE — one row per
   *  timestamp, values in key order (NaN where a key had no sample). */
  async groupedRange(args: TSGroupedRangeArgs): Promise<TSSample[]> {
    return tsToSamples(await tryDo(this.redis, 'TS.NRANGE', ...args.appendArgs()));
  }
  /** Java `groupedRangeReversed(TSGroupedRangeArgs)`: TS.NREVRANGE. */
  async groupedRangeReversed(args: TSGroupedRangeArgs): Promise<TSSample[]> {
    return tsToSamples(await tryDo(this.redis, 'TS.NREVRANGE', ...args.appendArgs()));
  }
  /** Java `queryIndex(String...)`: TS.QUERYINDEX. */
  async queryIndex(...filters: string[]): Promise<string[]> {
    tsRequireFilters(filters);
    const r = (await tryDo(this.redis, 'TS.QUERYINDEX', ...filters)) as any[];
    return (r ?? []).map(String);
  }
  /** Java `labelNames(String...)`: TS.QUERYLABELS LABELS [FILTER ...]. */
  async labelNames(...filters: string[]): Promise<string[]> {
    const cmdArgs: (string | number)[] = ['LABELS'];
    if (filters && filters.length > 0) cmdArgs.push('FILTER', ...filters);
    const r = (await tryDo(this.redis, 'TS.QUERYLABELS', ...cmdArgs)) as any[];
    return (r ?? []).map(String);
  }
  /** Java `labelValues(String, String...)`: TS.QUERYLABELS VALUES label [FILTER ...]. */
  async labelValues(label: string, ...filters: string[]): Promise<string[]> {
    const cmdArgs: (string | number)[] = ['VALUES', label];
    if (filters && filters.length > 0) cmdArgs.push('FILTER', ...filters);
    const r = (await tryDo(this.redis, 'TS.QUERYLABELS', ...cmdArgs)) as any[];
    return (r ?? []).map(String);
  }
}
