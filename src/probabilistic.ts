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
  async get(bit: number): Promise<boolean> {
    return ((await this.redis.getbit(this.name, bit)) as number) === 1;
  }
  async set(bit: number, value = true): Promise<boolean> {
    const prev = await this.redis.setbit(this.name, bit, value ? 1 : 0);
    return prev === 1;
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

export class RTimeSeries<V = any> extends RedisDataStructure {
  private ttlKey: string;
  private seqKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.ttlKey = prefixName('redisson__ts_ttl', name);
    this.seqKey = prefixName('redisson__ts_seq', name);
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
  private async alive(members: Buffer[], now: number): Promise<Buffer[]> {
    const out: Buffer[] = [];
    for (const m of members) {
      const score = await this.redis.zscore(this.ttlKey, m);
      if (score === null || Number(score) > now) out.push(m);
    }
    return out;
  }
  async range(from: number, to: number): Promise<TimeSeriesEntry<V>[]> {
    const now = await serverNowMs(this.redis);
    // NOTE: members carry arbitrary id bytes — must travel as Buffers.
    // ioredis string replies are UTF-8 and would corrupt them on real Redis.
    let members: Buffer[] = [];
    try {
      const raw = await this.redis.zrangebyscoreBuffer(this.name, String(from), String(to));
      members = (raw as unknown[]).map((m) => (Buffer.isBuffer(m) ? m : Buffer.from(String(m), 'latin1')));
    } catch {
      return [];
    }
    const live = await this.alive(members, now);
    const scores = await Promise.all(live.map((m) => this.redis.zscore(this.name, m)));
    return live.map((m, i) => {
      const u = unpackTimeSeriesMember(m);
      let value: V = m as unknown as V;
      let label = '';
      if (u) {
        try {
          value = this.decodeValue(u.val) as V;
        } catch {
          value = u.val as unknown as V;
        }
        // label blob is mark byte + codec-encoded label (strip mark, decode)
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
          value = this.decodeValue(m.toString('utf8')) as V;
        } catch { /* raw */ }
      }
      return { timestamp: Number(scores[i]), value, label };
    });
  }
  async get(timestamp: number): Promise<TimeSeriesEntry<V> | null> {
    const r = await this.range(timestamp, timestamp);
    return r.length === 0 ? null : r[0];
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
      const n = toNumber(await luaEval(this.redis, RATE_ACQUIRE, 2, this.valueKey(), this.permitsKey(),
        String(now), String(this.interval), String(this.rate), id, String(permits)));
      return n === 1;
    } catch {
      // mock fallback: token bucket via counter
      const key = this.valueKey();
      const cur = await this.redis.get(key);
      const avail = cur === null ? this.rate : Number(cur);
      if (avail >= permits) {
        await this.redis.set(key, String(avail - permits));
        return true;
      }
      return false;
    }
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
  async add(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.ADD', this.name, element)) === 1;
  }
  async exists(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.EXISTS', this.name, element)) === 1;
  }
  async remove(element: string): Promise<boolean> {
    return toNumber(await tryDo(this.redis, 'CF.DEL', this.name, element)) === 1;
  }
}

export class RTopK extends RedisDataStructure {
  async reserve(k: number): Promise<void> {
    await tryDo(this.redis, 'TOPK.RESERVE', this.name, k);
  }
  async add(...elements: string[]): Promise<void> {
    if (elements.length === 0) return;
    await tryDo(this.redis, 'TOPK.ADD', this.name, ...elements);
  }
  async list(): Promise<string[]> {
    return (await tryDo(this.redis, 'TOPK.LIST', this.name)) as string[];
  }
}

export class RTDigest extends RedisDataStructure {
  async create(): Promise<void> {
    await tryDo(this.redis, 'TDIGEST.CREATE', this.name);
  }
  async add(...values: number[]): Promise<void> {
    if (values.length === 0) return;
    await tryDo(this.redis, 'TDIGEST.ADD', this.name, ...values.map(String));
  }
  async quantile(...qs: number[]): Promise<number[]> {
    if (qs.length === 0) return [];
    const r: any = await tryDo(this.redis, 'TDIGEST.QUANTILE', this.name, ...qs.map(String));
    return (r as any[]).map(Number);
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
