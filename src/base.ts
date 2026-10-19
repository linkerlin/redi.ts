/**
 * Base class for all Redis-backed structures. Mirrors redi.py RedisDataStructure
 * and redi.go rObject: shared client/name/codec + RObject/RExpirable surfaces.
 */
import type { Redis } from 'ioredis';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { luaEval } from './util.js';
import { randomBytes } from 'node:crypto';

export class RedisDataStructure {
  constructor(
    protected redis: Redis,
    readonly name: string,
    protected codec: Codec = new JsonCodec(),
  ) {}

  getName(): string {
    return this.name;
  }

  // -- key-synchronizer identity ------------------------------------------

  private ownerIdCache?: string;
  private ownerWatchdogMs = 30000;

  /** Client identity used by per-key synchronizers (RMap/RSet/RMultimap
   * `getLock(key)` family). Lazily allocated for structures built outside
   * `Redisson.create`; the factory calls `attachOwner` so locks share the
   * client id (and thus interoperate with `getLock(name)`). */
  protected ownerId(): string {
    if (this.ownerIdCache === undefined) this.ownerIdCache = randomBytes(16).toString('hex');
    return this.ownerIdCache;
  }

  protected ownerWatchdog(): number {
    return this.ownerWatchdogMs;
  }

  /** @internal Redisson factory hook (client id + watchdog for key locks). */
  attachOwner(id: string, watchdogMs: number): this {
    this.ownerIdCache = id;
    this.ownerWatchdogMs = watchdogMs;
    return this;
  }

  encodeKey(key: unknown): string {
    return this.codec.encode(key);
  }
  decodeKey(raw: string | null): any {
    if (raw === null || raw === undefined) return null;
    try {
      return this.codec.decode(raw);
    } catch {
      return raw;
    }
  }
  encodeValue(v: unknown): string {
    return this.codec.encode(v);
  }
  decodeValue(v: any): any {
    if (v === null || v === undefined) return null;
    if (Array.isArray(v) && v.length === 1) v = v[0];
    if (typeof v === 'string' && v === '') return null;
    try {
      return this.codec.decode(v);
    } catch {
      return v;
    }
  }

  // -- RObject -----------------------------------------------------------

  /** Delete the structure's owned keys (main key + hash-tagged companions). */
  protected async deleteKeys(keys: string[]): Promise<boolean> {
    return ((await this.redis.del(...keys)) as number) > 0;
  }

  /** UNLINK equivalent; falls back to DEL when the server/client lacks it. */
  protected async unlinkKeys(keys: string[]): Promise<boolean> {
    try {
      const fn = (this.redis as any).unlink;
      if (typeof fn === 'function') return ((await fn.call(this.redis, ...keys)) as number) > 0;
    } catch { /* fall through to DEL */ }
    return this.deleteKeys(keys);
  }

  async delete(): Promise<boolean> {
    return this.deleteKeys([this.name]);
  }
  async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name]);
  }
  async rename(newName: string): Promise<void> {
    await this.redis.rename(this.name, newName);
    (this as any).name = newName;
  }
  async renamenx(newName: string): Promise<boolean> {
    const ok = await this.redis.renamenx(this.name, newName);
    if (ok) (this as any).name = newName;
    return ok === 1;
  }
  async isExists(): Promise<boolean> {
    return ((await this.redis.exists(this.name)) as number) > 0;
  }
  async touch(): Promise<boolean> {
    try {
      return ((await this.redis.touch(this.name)) as number) > 0;
    } catch {
      return this.isExists();
    }
  }
  async sizeInMemory(): Promise<number> {
    try {
      const n = await this.redis.memory('USAGE', this.name);
      return n === null ? 0 : Number(n);
    } catch {
      return 0;
    }
  }

  // -- RExpirable ---------------------------------------------------------
  async expire(ttlMs: number): Promise<boolean> {
    return (await this.redis.pexpire(this.name, ttlMs)) === 1;
  }
  async expireAt(timestampMs: number): Promise<boolean> {
    return (await this.redis.pexpireat(this.name, timestampMs)) === 1;
  }
  async clearExpire(): Promise<boolean> {
    return (await this.redis.persist(this.name)) === 1;
  }
  async remainTimeToLive(): Promise<number> {
    return this.redis.pttl(this.name);
  }
  async getExpireTime(): Promise<number> {
    try {
      return await this.redis.pexpiretime(this.name);
    } catch {
      return this.remainTimeToLive();
    }
  }
  protected async expireIf(ttlMs: number, mode: 'NX' | 'XX' | 'GT' | 'LT'): Promise<boolean> {
    try {
      const r = await (this.redis as any).pexpire(this.name, ttlMs, mode);
      return r === 1;
    } catch {
      // fallback Lua for old servers
      const s = `if ARGV[2] ~= '' then return redis.call('pexpire', KEYS[1], ARGV[1], ARGV[2]) end return redis.call('pexpire', KEYS[1], ARGV[1])`;
      const r = await luaEval(this.redis, s, 1, this.name, String(ttlMs), mode);
      return Number(r) === 1;
    }
  }
  async expireIfSet(ttlMs: number): Promise<boolean> {
    return this.expireIf(ttlMs, 'XX');
  }
  async expireIfNotSet(ttlMs: number): Promise<boolean> {
    return this.expireIf(ttlMs, 'NX');
  }
  async expireIfGreater(ttlMs: number): Promise<boolean> {
    return this.expireIf(ttlMs, 'GT');
  }
  async expireIfLess(ttlMs: number): Promise<boolean> {
    return this.expireIf(ttlMs, 'LT');
  }
}
