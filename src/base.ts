/**
 * Base class for all Redis-backed structures. Mirrors redi.py RedisDataStructure
 * and redi.go rObject: shared client/name/codec + RObject/RExpirable surfaces.
 */
import type { Redis } from 'ioredis';
import type { Codec } from './codec';
import { JsonCodec } from './codec';
import { luaEval } from './util';

export class RedisDataStructure {
  constructor(
    protected redis: Redis,
    readonly name: string,
    protected codec: Codec = new JsonCodec(),
  ) {}

  getName(): string {
    return this.name;
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
  async delete(): Promise<boolean> {
    return ((await this.redis.del(this.name)) as number) > 0;
  }
  async unlink(): Promise<boolean> {
    const fn = (this.redis as any).unlink;
    if (typeof fn === 'function') return ((await fn.call(this.redis, this.name)) as number) > 0;
    return this.delete();
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
      return ((await (this.redis as any).touch(this.name)) as number) > 0;
    } catch {
      return this.isExists();
    }
  }
  async sizeInMemory(): Promise<number> {
    try {
      const n = await (this.redis as any).memory('USAGE', this.name);
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
      return await (this.redis as any).pexpiretime(this.name);
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
