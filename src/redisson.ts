/**
 * Redisson client — central entry point. Wire-compatible with Java Redisson
 * (JsonJacksonCodec) + redi.py + redi.go. Single-node topology over ioredis
 * (cluster/sentinel via user-supplied client).
 */
import Redis, { RedisOptions, Cluster } from 'ioredis';
import { randomBytes } from 'node:crypto';
import { Codec, JsonCodec } from './codec.js';
import { RBucket, RBuckets } from './collections.js';
import { RAtomicLong, RAtomicDouble, RLongAdder, RDoubleAdder, RIdGenerator } from './atomic.js';
import { RMap, RSet, RList, RQueue, RDeque, RBlockingQueue, RBlockingDeque, RBoundedBlockingQueue, RScoredSortedSet, RLexSortedSet, RMaps } from './collections.js';
import { RMapCache, RMapCacheNative, RSetCache, RSetMultimap, RListMultimap, RSetMultimapCache, RListMultimapCache, RLocalCachedMap, LocalCachedMapOptions } from './caches.js';
import {
  RLock, RSpinLock, RNonReentrantLock, RFairLock, RNonReentrantFairLock,
  RReadWriteLock, RSemaphore, RCountDownLatch, RPermitExpirableSemaphore,
  RFencedLock, RMultiLock, RRedLock, BackOff, LockLike,
} from './locks.js';
import { RTopic, RPatternTopic, RShardedTopic, RReliableTopic, RStream, RDelayedQueue, RRingBuffer, RBinaryStream } from './messaging.js';
import { RBloomFilter, RBloomFilterNative, RCuckooFilter, RTopK, RTDigest, RGcra, RCountMin, RHyperLogLog, RGeo, RBitSet, RTimeSeries, RTimeSeriesNative, RTimeSeriesNatives, RRateLimiter, RateType } from './probabilistic.js';
import { RKeys, RScript, RFunction, RSearch, RJsonBucket, RJsonBuckets, RVectorSet, RPriorityQueue, RPriorityBlockingQueue, RPriorityDeque, RPriorityBlockingDeque, RTransferQueue, RArray, RCircularBuffer, RBatch, RSetMultimapCacheNative, RListMultimapCacheNative, RRedisNodes, RClientSideCaching } from './infra.js';

/**
 * Accepted client shapes: a single ioredis connection or an ioredis Cluster.
 * Cluster clients must keep all keys of one structure in the same hash slot —
 * every Redisson structure key is hash-tagged (`{name}` companions), so the
 * built-in types are cluster-safe as-is.
 */
export type RedisClient = Redis | Cluster;

/** Java `org.redisson.config.NameMapper`: maps Redisson object names to
 *  stored Redis keys (applied to every structure name at creation). */
export interface NameMapper {
  /** Applies map function to input `name` (original object name). */
  map(name: string): string;
  /** Applies unmap function to a mapped `name` to get the original name. */
  unmap(name: string): string;
}

/** Java `PrefixNameMapper`: prepends a fixed prefix unless the name already
 *  carries it; unmap strips it. */
export class PrefixNameMapper implements NameMapper {
  constructor(readonly prefix: string = '') {}
  map(name: string): string {
    if (name === null || name === undefined) return name;
    if (name.startsWith(this.prefix)) return name;
    return this.prefix + name;
  }
  unmap(name: string): string {
    if (name === null || name === undefined) return name;
    if (!name.startsWith(this.prefix)) return name;
    return name.substring(this.prefix.length);
  }
}

/** Java `NameMapper.direct()` (DefaultNameMapper): identity both ways. */
export function defaultNameMapper(): NameMapper {
  return { map: (n) => n, unmap: (n) => n };
}

export interface Config {
  /** redis:// URL or host:port */
  address?: string;
  options?: RedisOptions;
  /** an existing ioredis client — Redis or Cluster (takes precedence) */
  client?: RedisClient;
  codec?: Codec;
  lockWatchdogTimeout?: number;
  /** Java config-level `nameMapper` (Redisson 4.8): applied to every
   *  structure name in the getXxx factories. */
  nameMapper?: NameMapper;
}

export class Redisson {
  readonly id: string = randomBytes(16).toString('hex');
  readonly redis: Redis;
  private codec: Codec;
  private watchdog: number;
  private nameMapper?: NameMapper;

  private constructor(config: Config) {
    // Internals keep the `Redis` type: a Cluster exposes the same command
    // surface (verified in the live cluster suite); the field stays `Redis`
    // so every call site type-checks identically for both clients.
    this.redis = (config.client as Redis | undefined) ?? (config.address ? new Redis(config.address, config.options ?? {}) : new Redis(config.options ?? {}));
    this.codec = config.codec ?? new JsonCodec();
    this.watchdog = config.lockWatchdogTimeout ?? 30000;
    this.nameMapper = config.nameMapper;
  }

  /** Map a user-facing object name through the configured NameMapper.
   *  Companion keys derive from the mapped name inside each structure —
   *  same as Java, where the executor maps `getRawName()` before any
   *  command. RKeys pattern methods are NOT unmapped (Java limitation too). */
  private mapped(name: string): string {
    return this.nameMapper ? this.nameMapper.map(name) : name;
  }

  static create(config: Config = {}): Redisson {
    return new Redisson(config);
  }

  holderID(threadID: string | number = '1'): string {
    return `${this.id}:${threadID}`;
  }
  getId(): string {
    return this.id;
  }

  // -- buckets / atomics ----------------------------------------------------
  getBucket<V>(name: string, codec?: Codec): RBucket<V> {
    return new RBucket<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getRBucket<V>(name: string, codec?: Codec): RBucket<V> {
    return this.getBucket<V>(name, codec);
  }
  getBuckets(): RBuckets {
    return new RBuckets(this.redis, this.codec);
  }
  getBinaryStream(name: string): RBinaryStream {
    return new RBinaryStream(this.redis, this.mapped(name), this.codec);
  }
  getJsonBucket<V>(name: string, codec?: Codec): RJsonBucket<V> {
    return new RJsonBucket<V>(this.redis, this.mapped(name), codec);
  }
  getJsonBuckets(codec?: Codec): RJsonBuckets {
    return new RJsonBuckets(this.redis, codec);
  }
  getAtomicLong(name: string): RAtomicLong {
    return new RAtomicLong(this.redis, this.mapped(name));
  }
  getRAtomicLong(name: string): RAtomicLong {
    return this.getAtomicLong(name);
  }
  getAtomicDouble(name: string): RAtomicDouble {
    return new RAtomicDouble(this.redis, this.mapped(name));
  }
  getLongAdder(name: string): RLongAdder {
    return new RLongAdder(this.redis, this.mapped(name));
  }
  getDoubleAdder(name: string): RDoubleAdder {
    return new RDoubleAdder(this.redis, this.mapped(name));
  }
  getIdGenerator(name: string): RIdGenerator {
    return new RIdGenerator(this.redis, this.mapped(name));
  }

  // -- maps / sets / lists / queues ------------------------------------------
  getMap<K, V>(name: string, codec?: Codec): RMap<K, V> {
    return new RMap<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getRMap<K, V>(name: string, codec?: Codec): RMap<K, V> {
    return this.getMap<K, V>(name, codec);
  }
  getMaps<K, V>(): RMaps<K, V> {
    return new RMaps<K, V>(this.redis, this.codec);
  }
  getRMaps<K, V>(): RMaps<K, V> {
    return this.getMaps<K, V>();
  }
  getMapCache<K, V>(name: string, codec?: Codec): RMapCache<K, V> {
    return new RMapCache<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getMapCacheNative<K, V>(name: string, codec?: Codec): RMapCacheNative<K, V> {
    return new RMapCacheNative<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getLocalCachedMap<K, V>(name: string, opts: LocalCachedMapOptions = {}): RLocalCachedMap<K, V> {
    return new RLocalCachedMap<K, V>(this.redis, this.mapped(name), this.codec, opts, this.id);
  }
  getLocalCachedMapWithOptions<K, V>(name: string, opts: LocalCachedMapOptions): RLocalCachedMap<K, V> {
    return this.getLocalCachedMap<K, V>(name, opts);
  }
  getSet<V>(name: string, codec?: Codec): RSet<V> {
    return new RSet<V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getRSet<V>(name: string, codec?: Codec): RSet<V> {
    return this.getSet<V>(name, codec);
  }
  getSetCache<V>(name: string, codec?: Codec): RSetCache<V> {
    return new RSetCache<V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getList<V>(name: string, codec?: Codec): RList<V> {
    return new RList<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getRList<V>(name: string, codec?: Codec): RList<V> {
    return this.getList<V>(name, codec);
  }
  getQueue<V>(name: string, codec?: Codec): RQueue<V> {
    return new RQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getRQueue<V>(name: string, codec?: Codec): RQueue<V> {
    return this.getQueue<V>(name, codec);
  }
  getDeque<V>(name: string, codec?: Codec): RDeque<V> {
    return new RDeque<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getBlockingQueue<V>(name: string, codec?: Codec): RBlockingQueue<V> {
    return new RBlockingQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getBlockingDeque<V>(name: string, codec?: Codec): RBlockingDeque<V> {
    return new RBlockingDeque<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getBoundedBlockingQueue<V>(name: string, codec?: Codec): RBoundedBlockingQueue<V> {
    return new RBoundedBlockingQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  /**
   * Delayed queue. Java wires the background migrator (QueueTransferTask)
   * automatically at object creation; here it is opt-in — call `start()` once
   * to have expired members delivered without an explicit poll() (stop()/
   * destroy() release it, destroy() deletes nothing). Without `start()`,
   * behaviour matches 1.5.0: only poll()/peek() run a synchronous catch-up
   * migration.
   */
  getDelayedQueue<V>(name: string, codec?: Codec): RDelayedQueue<V> {
    return new RDelayedQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getTransferQueue<V>(name: string, codec?: Codec): RTransferQueue<V> {
    return new RTransferQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getQueueTransfer<V>(name: string, codec?: Codec): RTransferQueue<V> {
    return this.getTransferQueue<V>(name, codec);
  }
  getRingBuffer<V>(name: string, codec?: Codec): RRingBuffer<V> {
    return new RRingBuffer<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getScoredSortedSet<V>(name: string, codec?: Codec): RScoredSortedSet<V> {
    return new RScoredSortedSet<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getLexSortedSet(name: string): RLexSortedSet {
    return new RLexSortedSet(this.redis, this.mapped(name), this.codec);
  }
  getSetMultimap<K, V>(name: string, codec?: Codec): RSetMultimap<K, V> {
    return new RSetMultimap<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getListMultimap<K, V>(name: string, codec?: Codec): RListMultimap<K, V> {
    return new RListMultimap<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getSetMultimapCache<K, V>(name: string, codec?: Codec): RSetMultimapCache<K, V> {
    return new RSetMultimapCache<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getListMultimapCache<K, V>(name: string, codec?: Codec): RListMultimapCache<K, V> {
    return new RListMultimapCache<K, V>(this.redis, this.mapped(name), codec ?? this.codec).attachOwner(this.id, this.watchdog);
  }
  getSetMultimapCacheNative<K, V>(name: string, codec?: Codec): RSetMultimapCacheNative {
    void codec;
    return new RSetMultimapCacheNative(this.redis, this.mapped(name), this.codec);
  }
  getListMultimapCacheNative<K, V>(name: string, codec?: Codec): RListMultimapCacheNative {
    void codec;
    return new RListMultimapCacheNative(this.redis, this.mapped(name), this.codec);
  }
  getSortedSet(_name: string): never {
    throw new Error('redi.ts: RSortedSet (LIST+Comparator) is REFUSE_AS_FAKE — use getScoredSortedSet()');
  }

  // -- locks ------------------------------------------------------------------
  getLock(name: string): RLock {
    return new RLock(this.redis, this.mapped(name), this.id, this.watchdog);
  }
  getRLock(name: string): RLock {
    return this.getLock(name);
  }
  getSpinLock(name: string, backOff?: BackOff): RSpinLock {
    return new RSpinLock(this.redis, this.mapped(name), this.id, this.watchdog, backOff);
  }
  getNonReentrantLock(name: string): RNonReentrantLock {
    return new RNonReentrantLock(this.redis, this.mapped(name), this.id, this.watchdog);
  }
  getFairLock(name: string): RFairLock {
    return new RFairLock(this.redis, this.mapped(name), this.id, this.watchdog, true);
  }
  getNonReentrantFairLock(name: string): RNonReentrantFairLock {
    return new RNonReentrantFairLock(this.redis, this.mapped(name), this.id, this.watchdog);
  }
  getReadWriteLock(name: string): RReadWriteLock {
    return new RReadWriteLock(this.redis, this.mapped(name), this.id, this.watchdog);
  }
  getFencedLock(name: string): RFencedLock {
    return new RFencedLock(this.redis, this.mapped(name), this.id, this.watchdog);
  }
  getSemaphore(name: string): RSemaphore {
    return new RSemaphore(this.redis, this.mapped(name));
  }
  getPermitExpirableSemaphore(name: string): RPermitExpirableSemaphore {
    return new RPermitExpirableSemaphore(this.redis, this.mapped(name));
  }
  getCountDownLatch(name: string): RCountDownLatch {
    return new RCountDownLatch(this.redis, this.mapped(name));
  }
  getRateLimiter(name: string): RRateLimiter {
    return new RRateLimiter(this.redis, this.mapped(name), this.id);
  }
  newMultiLock(...locks: LockLike[]): RMultiLock {
    return new RMultiLock(locks);
  }
  getMultiLock(...locks: LockLike[]): RMultiLock {
    return this.newMultiLock(...locks);
  }
  newRedLock(...locks: LockLike[]): RRedLock {
    return new RRedLock(locks);
  }
  getRedLock(...locks: LockLike[]): RRedLock {
    return this.newRedLock(...locks);
  }
  /** Transactions are snapshot-rollback in Redisson, not MULTI/EXEC — refuse. */
  createTransaction(): never {
    throw new Error('redi.ts: RTransaction is REFUSE_AS_FAKE (Redisson uses operation-log rollback, not MULTI/EXEC)');
  }

  // -- messaging ----------------------------------------------------------------
  getTopic<V>(name: string, codec?: Codec): RTopic<V> {
    return new RTopic<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getPatternTopic<V>(pattern: string, codec?: Codec): RPatternTopic<V> {
    return new RPatternTopic<V>(this.redis, pattern, codec ?? this.codec);
  }
  getShardedTopic<V>(name: string, codec?: Codec): RShardedTopic<V> {
    return new RShardedTopic<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getReliableTopic<V>(name: string, codec?: Codec): RReliableTopic<V> {
    return new RReliableTopic<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getStream(name: string, codec?: Codec): RStream {
    void codec;
    return new RStream(this.redis, this.mapped(name), this.codec);
  }

  // -- observability / probabilistic ---------------------------------------------
  getGeo<V>(name: string, codec?: Codec): RGeo<V> {
    return new RGeo<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getHyperLogLog(name: string, codec?: Codec): RHyperLogLog {
    return new RHyperLogLog(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getBitSet(name: string): RBitSet {
    return new RBitSet(this.redis, this.mapped(name), this.codec);
  }
  getBloomFilter(name: string): RBloomFilter {
    return new RBloomFilter(this.redis, this.mapped(name));
  }
  getBloomFilterNative(name: string): RBloomFilterNative {
    return new RBloomFilterNative(this.redis, this.mapped(name), this.codec);
  }
  getCuckooFilter(name: string): RCuckooFilter {
    return new RCuckooFilter(this.redis, this.mapped(name), this.codec);
  }
  getTopK(name: string): RTopK {
    return new RTopK(this.redis, this.mapped(name), this.codec);
  }
  getTDigest(name: string): RTDigest {
    return new RTDigest(this.redis, this.mapped(name), this.codec);
  }
  getCountMin(name: string): RCountMin {
    return new RCountMin(this.redis, this.mapped(name), this.codec);
  }
  getGcra(name: string): RGcra {
    return new RGcra(this.redis, this.mapped(name), this.codec);
  }
  getTimeSeries<V>(name: string, codec?: Codec): RTimeSeries<V> {
    return new RTimeSeries<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  /** Java 4.8 `RTimeSeriesNative`: RedisTimeSeries module TS.* binding
   *  (module-gated; distinct from the ZSET-based RTimeSeries above). */
  getTimeSeriesNative(name: string): RTimeSeriesNative {
    return new RTimeSeriesNative(this.redis, this.mapped(name), this.codec);
  }
  getRTimeSeriesNative(name: string): RTimeSeriesNative {
    return this.getTimeSeriesNative(name);
  }
  /** Java 4.8 `RTimeSeriesNatives`: TS.* operations spanning several series. */
  getTimeSeriesNatives(): RTimeSeriesNatives {
    return new RTimeSeriesNatives(this.redis);
  }
  getRTimeSeriesNatives(): RTimeSeriesNatives {
    return this.getTimeSeriesNatives();
  }

  // -- infra ----------------------------------------------------------------------
  getKeys(): RKeys {
    return new RKeys(this.redis);
  }
  getScript(): RScript {
    return new RScript(this.redis);
  }
  getFunction(): RFunction {
    return new RFunction(this.redis);
  }
  getSearch(): RSearch {
    return new RSearch(this.redis);
  }
  getVectorSet(name: string): RVectorSet {
    return new RVectorSet(this.redis, this.mapped(name), this.codec);
  }
  getPriorityQueue<V>(name: string, codec?: Codec): RPriorityQueue<V> {
    return new RPriorityQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getPriorityBlockingQueue<V>(name: string, codec?: Codec): RPriorityBlockingQueue<V> {
    return new RPriorityBlockingQueue<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getPriorityDeque<V>(name: string, codec?: Codec): RPriorityDeque<V> {
    return new RPriorityDeque<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getPriorityBlockingDeque<V>(name: string, codec?: Codec): RPriorityBlockingDeque<V> {
    return new RPriorityBlockingDeque<V>(this.redis, this.mapped(name), codec ?? this.codec);
  }
  getArray<V>(name: string): RArray<V> {
    return new RArray<V>(this.redis, this.mapped(name), this.codec);
  }
  getCircularBuffer<V>(name: string): RCircularBuffer<V> {
    return new RCircularBuffer<V>(this.redis, this.mapped(name), this.codec);
  }
  createBatch(): RBatch {
    return new RBatch(this.redis);
  }
  getRedisNodes(): RRedisNodes {
    return new RRedisNodes(this.redis);
  }
  getClientSideCaching(): RClientSideCaching {
    return new RClientSideCaching(this.redis);
  }

  // -- PRO-only stubs (refuse, matching redi.go) ------------------------------------
  getReliableQueue(_name: string): never {
    throw new Error('redi.ts: RReliableQueue is Redisson PRO-only');
  }
  getBitVectorStore(_name: string): never {
    throw new Error('redi.ts: RBitVectorStore is Redisson PRO-only');
  }
  getLocalCachedMapCache(_name: string): never {
    throw new Error('redi.ts: RLocalCachedMapCache is Redisson PRO-only');
  }
  getReliablePubSubTopic(_name: string): never {
    throw new Error('redi.ts: RReliablePubSubTopic is Redisson PRO-only');
  }
  getReliableFanout(_name: string): never {
    throw new Error('redi.ts: RReliableFanout is Redisson PRO-only');
  }
  getLeasedMap(_name: string): never {
    throw new Error('redi.ts: RLeasedMap is Redisson PRO-only');
  }

  async shutdown(): Promise<void> {
    try {
      this.redis.disconnect();
    } catch { /* noop */ }
  }
}

export type { RateType };
