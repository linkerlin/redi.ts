import type { Redis } from 'ioredis';
import { Codec } from './codec';

export class RBucket<V> {
  constructor(private redis: Redis, readonly name: string, private codec: Codec) {}

  async get(): Promise<V | null> {
    const raw = await this.redis.get(this.name);
    return raw === null ? null : this.codec.decode(raw);
  }
  async set(value: V, ttlMs?: number): Promise<void> {
    if (ttlMs && ttlMs > 0) await this.redis.set(this.name, this.codec.encode(value), 'PX', ttlMs);
    else await this.redis.set(this.name, this.codec.encode(value));
  }
  async trySet(value: V): Promise<boolean> {
    return (await this.redis.set(this.name, this.codec.encode(value), 'NX')) === 'OK';
  }
  async getAndSet(value: V): Promise<V | null> {
    const raw = await this.redis.getset(this.name, this.codec.encode(value));
    return raw === null ? null : this.codec.decode(raw);
  }
  async delete(): Promise<boolean> {
    return (await this.redis.del(this.name)) > 0;
  }
  async isExists(): Promise<boolean> {
    return (await this.redis.exists(this.name)) > 0;
  }
}

export class RAtomicLong {
  constructor(private redis: Redis, readonly name: string) {}

  async get(): Promise<number> {
    return Number((await this.redis.get(this.name)) ?? 0);
  }
  async set(value: number): Promise<void> {
    await this.redis.set(this.name, String(value));
  }
  async incrementAndGet(): Promise<number> {
    return this.redis.incr(this.name);
  }
  async decrementAndGet(): Promise<number> {
    return this.redis.decr(this.name);
  }
  async addAndGet(delta: number): Promise<number> {
    return this.redis.incrby(this.name, delta);
  }
  async compareAndSet(expect: number, update: number): Promise<boolean> {
    const script =
      "local v = redis.call('get', KEYS[1]); if v == false then v = '0' end; " +
      "if tonumber(v) == tonumber(ARGV[1]) then redis.call('set', KEYS[1], ARGV[2]); return 1 else return 0 end";
    return (await this.redis.eval(script, 1, this.name, String(expect), String(update))) === 1;
  }
  async delete(): Promise<boolean> {
    return (await this.redis.del(this.name)) > 0;
  }
}

export class RMap<K, V> {
  constructor(private redis: Redis, readonly name: string, private codec: Codec) {}

  async get(key: K): Promise<V | null> {
    const raw = await this.redis.hget(this.name, this.codec.encode(key));
    return raw === null ? null : this.codec.decode(raw);
  }
  async put(key: K, value: V): Promise<V | null> {
    const old = await this.get(key);
    await this.redis.hset(this.name, this.codec.encode(key), this.codec.encode(value));
    return old;
  }
  async fastPut(key: K, value: V): Promise<boolean> {
    return (await this.redis.hset(this.name, this.codec.encode(key), this.codec.encode(value))) === 1;
  }
  async putIfAbsent(key: K, value: V): Promise<V | null> {
    const ok = await this.redis.hsetnx(this.name, this.codec.encode(key), this.codec.encode(value));
    return ok === 1 ? null : this.get(key);
  }
  async remove(key: K): Promise<V | null> {
    const old = await this.get(key);
    await this.redis.hdel(this.name, this.codec.encode(key));
    return old;
  }
  async fastRemove(...keys: K[]): Promise<number> {
    return this.redis.hdel(this.name, ...keys.map((k) => this.codec.encode(k)));
  }
  async containsKey(key: K): Promise<boolean> {
    return (await this.redis.hexists(this.name, this.codec.encode(key))) === 1;
  }
  async size(): Promise<number> {
    return this.redis.hlen(this.name);
  }
  async readAllMap(): Promise<Map<K, V>> {
    const all = await this.redis.hgetall(this.name);
    return new Map(Object.entries(all).map(([k, v]) => [this.codec.decode(k), this.codec.decode(v)]));
  }
  async delete(): Promise<boolean> {
    return (await this.redis.del(this.name)) > 0;
  }
}
