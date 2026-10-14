# redi.ts — 用 TypeScript 把 Redisson 原样搬过来

[English](README_EN.md) | **中文**

你在 Java 里用 Redisson 往 Redis 放了一把锁。换到 TypeScript，这把锁还得能锁上、解开。键怎么摆、Lua 怎么算、值怎么编码，都不能另起一套。

redi.ts 就是干这个的。它跟 **Java Redisson**（`JsonJacksonCodec`，默认把类型写进 JSON）、**redi.py**、**redi.go** 用的是同一套东西：同样的键布局，同样的 Lua，同样的值编码。一边写进去的数据，另外两边读得出来。

标准是 Redisson Java 源码。Lua 照 `redi.go` 搬。那套脚本已经对着 Redisson 4.6.1 对过。

## 安装

```sh
npm install redi.ts ioredis
```

## 先跑一个

```ts
import { Redisson } from 'redi.ts';

const redisson = Redisson.create({ address: 'redis://127.0.0.1:6379' });

// 桶。JSON 编码，对象带 @class，跟 Redisson 一样
await redisson.getBucket('any').set({ n: 1 });

// 可重入锁：一个 HASH，频道 redisson_lock__channel:{name}，外加一条看门狗
const lock = redisson.getLock('myLock');
await lock.lock();
try { /* ... */ } finally { await lock.unlock(); }

// Map：Redis 里是 HASH，字段是 JSON
await redisson.getMap('m').put('k', 'v');

// 限流、信号量、门闩、布隆过滤器、Stream、发布订阅……
await redisson.getRateLimiter('api').setRate('OVERALL', 10, 1000);

await redisson.shutdown();
```

手里已经有客户端（集群、哨兵或 TLS）？接上就行：

```ts
import Redis from 'ioredis';
const redis = new Redis.Cluster([{ host: '127.0.0.1', port: 7000 }]);
const redisson = Redisson.create({ client: redis as any });
```

## 有哪些结构

六十多种，按你拿它来干什么分组。

- 锁：`RLock`、`Fair`、`ReadWrite`、`Spin`、`NonReentrant`、`Fenced`、`Multi`、`Red`
- 映射：`RMap`、`MapCache`（含 Native）、`LocalCachedMap`
- 集合：`RSet`、`SetCache`
- 列表和队列：`RList`、`RQueue`、`RDeque`（含阻塞、有界）
- 排序：`RScoredSortedSet`；`RLexSortedSet`（成员不编码，原样放）
- 单个值：`RBucket`、`RBuckets`、`RBinaryStream`、`RJsonBucket`
- 数字：`RAtomicLong`、`Double`；`RLongAdder`、`RDoubleAdder`；`RIdGenerator`
- 许可和限流：`RSemaphore`、`RPermitExpirableSemaphore`、`RCountDownLatch`、`RRateLimiter`
- 发布订阅：`RTopic`、`RPatternTopic`、`RShardedTopic`、`RReliableTopic`（走 Stream，字段名 `m`）
- 流：`RStream`（字段名是 JSON）
- 延迟队列：`RDelayedQueue`（一个 ZSET 加一个 LIST，前缀 `Bc0Lc0`）
- 环形缓冲：`RRingBuffer`
- 地理位置：`RGeo`（成员是 JSON）
- 去重计数：`RHyperLogLog`
- 位图：`RBitSet`（位序跟 Redis 原生一致）
- 时间序列：`RTimeSeries`（`BBc0Lc0Lc0`）
- 布隆过滤器：`RBloomFilter`（HighwayHash-128，测试向量对过）
- 键、脚本、函数、搜索：`RKeys`、`RScript`、`RFunction`、`RSearch`、`RVectorSet`
- 优先级队列（`GO_ONLY`，ZSET 加分数）、`RTransferQueue`（`GO_ONLY`）、`RBatch`，……

细账在 [COMPATIBILITY.md](COMPATIBILITY.md)。

有些东西故意不做，跟 redi.go 一样。`RSortedSet` 要 LIST 再加比较器。`RTransaction` 要快照回滚。`LiveObject`、reactive / Rx 不做。`RRemoteService` 和 `RExecutorService` 的线上协议也不做。PRO 才有的 `RReliableQueue`、`RLocalCachedMapCache`、`RReliablePubSubTopic`、`RBitVectorStore`，这里没有。

## 值是怎么写进去的

看你 `set` 进去的 `{ n: 1 }`。

数字、字符串、布尔，能裸着就裸着。整数一旦超出 int32，就包成 `["java.lang.Long", v]`。对象多一个字段 `@class: java.util.LinkedHashMap`。数组写成 `["java.util.ArrayList", [...]]`，里面再套对象或数组，照样这么包。

读的时候，这些包装全部剥掉。所以 redi.py 写下的裸 JSON，还有普通字符串，这边都读得出来。

`StringCodec` 不包装，就是原始字符串。`RBinaryStream` 和 `RLexSortedSet` 不走这套编码。

## 怎么验

```sh
npm test    # vitest：线格式向量 + mock 功能 + 跟 Python 实机互通（互通本身不用 Redis）
npm run build
```

`test/interop.test.ts` 会拉起 `python`，用 `C:/GitHub/redi.py` 对三件事：编码输出是不是一样，HighwayHash 的测试向量对不对，multimap 的内部 id 是不是同一个。

本机 `127.0.0.1:6379` 有一个真 Redis 的话，mock 那套也会换上真客户端再跑一遍：传给 `Redisson.create({ client })` 就行。
