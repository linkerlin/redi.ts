import Redis, { RedisOptions } from 'ioredis';
import { randomUUID } from 'node:crypto';
import { Codec, JsonCodec } from './codec';
import { RAtomicLong, RBucket, RMap } from './objects';
import { RLock } from './lock';

export interface Config {
  /** redis:// URL or host:port */
  address?: string;
  options?: RedisOptions;
  /** an existing ioredis client (takes precedence) */
  client?: Redis;
  codec?: Codec;
  lockWatchdogTimeout?: number;
}

export class Redisson {
  readonly id = randomUUID();
  readonly redis: Redis;
  private codec: Codec;
  private watchdog: number;

  private constructor(config: Config) {
    this.redis = config.client ?? (config.address ? new Redis(config.address, config.options ?? {}) : new Redis(config.options ?? {}));
    this.codec = config.codec ?? new JsonCodec();
    this.watchdog = config.lockWatchdogTimeout ?? 30000;
  }

  static create(config: Config = {}): Redisson {
    return new Redisson(config);
  }

  getBucket<V>(name: string, codec?: Codec): RBucket<V> {
    return new RBucket<V>(this.redis, name, codec ?? this.codec);
  }
  getAtomicLong(name: string): RAtomicLong {
    return new RAtomicLong(this.redis, name);
  }
  getMap<K, V>(name: string, codec?: Codec): RMap<K, V> {
    return new RMap<K, V>(this.redis, name, codec ?? this.codec);
  }
  getLock(name: string): RLock {
    return new RLock(this.redis, name, this.id, this.watchdog);
  }
  async shutdown(): Promise<void> {
    this.redis.disconnect();
  }
}
