# redi.ts — 用 TypeScript 把 Redisson 原样搬过来

[English](README_EN.md) | **中文**

你在 Java 里用 Redisson 往 Redis 放了一把锁。换到 TypeScript，这把锁还得能锁上、解开。键怎么摆、Lua 怎么算、值怎么编码，都不能另起一套。

redi.ts 就是干这个的。它跟 **Java Redisson**（`JsonJacksonCodec`，默认把类型写进 JSON）、**redi.py**、**redi.go** 用的是同一套东西：同样的键布局，同样的 Lua，同样的值编码。一边写进去的数据，另外两边读得出来。

标准是 Redisson Java 源码。Lua 照 `redi.go` 搬。那套脚本已经对着 Redisson 4.6.1 对过。

## 安装

```sh
npm install redi.ts ioredis
```

双格式发布：ESM 走 `import { Redisson } from 'redi.ts'`，CommonJS 走 `const { Redisson } = require('redi.ts')`——`exports` 分别指向 `dist/esm` 和 `dist/cjs`，两边跑出来的 wire 输出一致（`npm run pack-smoke` 验证）。

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

// 延迟队列：start() 后按 deadline 后台迁移（Java QueueTransferTask 同款调度），
// poll()/peek() 不 start() 也能手动补迁移
const dq = redisson.getDelayedQueue('dq');
await dq.start();
await dq.offer('later', 5000);

await redisson.shutdown();
```

手里已经有客户端（集群、哨兵或 TLS）？接上就行：

```ts
import Redis from 'ioredis';
const redis = new Redis.Cluster([{ host: '127.0.0.1', port: 7000 }]);
const redisson = Redisson.create({ client: redis });
```

集群下键得同槽。本库所有伴生键都带 hash-tag：`prefixName`/`suffixName` 把裸名包成 `{name}`，与裸名恰好同槽，所以上面这些结构在 Cluster 上开箱即用；`test/live-cluster.test.ts` 对着 3 主节点实测过（`scripts/cluster-up.sh` 起集群）。

## 有哪些结构

六十多种，按你拿它来干什么分组。

- 锁：`RLock`（含 `lockInterruptibly` + AbortSignal）、`Fair`、`ReadWrite`（读锁伴生 `:rwlock_timeout:<n>` 键，与 Java 混部安全）、`Spin`、`NonReentrant`、`Fenced`（`tryLockAndGetToken` 非阻塞/等待两种语义）、`Multi`、`Red`；以及 Key 级同步器（`map.getLock(key)`/`getFairLock`/`getReadWriteLock`/`getSemaphore`/`getCountDownLatch`/`getPermitExpirableSemaphore`，伴生命名与 Java 逐字节一致）
- 映射：`RMap`（另含 `putAll(batchSize)`、`HRANDFIELD`/`HSTRLEN`、惰性 HSCAN 迭代器、`keySet/values/entrySet` 视图、三参 `replace`、put/incr/remove 字段监听）、`MapCache`（含 Native；`expireEntries`/`getWithTTLOnly`/`getMaxSize`、`compute/computeIfAbsent`、Native 支持绝对 `Date` 过期、`fastPut/fastPutIfAbsent/putIfExist/fastPutIfExist`、结构化 `PutArgs`（HSETEX KEEPTTL/PX/PXAT）、批量 `remainTimeToLive/clearExpire`、字段监听与条件批量）、`LocalCachedMap`（`maxSize` 下 LRU/LFU 淘汰、`getCachedMap()` 本地快照、重连策略 NONE/CLEAR/LOAD、过期事件策略）
- 集合：`RSet`（`countIntersection` 含 LIMIT、sadd/srem/spop/store 键空间监听、惰性 `iterator`/`stream`）、`SetCache`（TTL/空闲淘汰、zadd/zrem 监听）
- 列表和队列：`RList`、`RQueue`（`poll(count)`/`indexOf`/`pollLastAndOfferFirstTo`）、`RDeque`（变参 `addFirst/addLast`、`*IfExists`、`poll(count)`、`move`；含阻塞、有界）；阻塞队列支持 `pollFromAny`/`pollFirstFromAny`/`pollLastFromAny`/`pollLastFromAnyWithName`（BLPOP/BRPOP/BLMPOP）与 `move`/`moveWithTimeout`/`pollLastAndOfferFirstTo`（LMOVE/BLMOVE），以及 `subscribeOnElements`/`unsubscribe` 元素订阅
- 排序：`RScoredSortedSet`（分数区间正反向、`addScore` 家族、`addIf*` 与 `addAll*` 条件批量、`random`/`randomEntries`、阻塞 `pollFirst/Last` 与 `FromAny`（BZPOP/BZMPOP）、`iterator`/`stream`/`entriesIterator`、按分数/排名删除，区间读取走 Redis 6.2+ `ZRANGE … BYSCORE`）；`RLexSortedSet`（成员不编码，原样放）
- 单个值：`RBucket`、`RBuckets`（NX/XX 条件批量写，Redis 8 `MSETEX`）、`RBinaryStream`、`RJsonBucket`（文档/路径读写、NX/XX/CAS、字符串/数组/数字操作、`*Multi` 多匹配族与多路径 `get`、merge/keys/type/toggle）、`RJsonBuckets`（MSET/MGET 批量）
- 数字：`RAtomicLong`、`Double`（`getAndIncrement`/`Decrement`、`setIfLess`/`Greater`、`getAndDelete`/`compareAndDelete`（含 `< <= > >= == ~=` 条件）、`IncrementArgs` 边界/饱和/TTL 策略、incrby/incrbyfloat 键空间监听）；`RLongAdder`、`RDoubleAdder`；`RIdGenerator`
- 许可和限流：`RSemaphore`、`RPermitExpirableSemaphore`、`RCountDownLatch`、`RRateLimiter`（`keepAlive` 在每次成功获准的 Lua 内顺延）
- 发布订阅：`RTopic`、`RPatternTopic`、`RShardedTopic`、`RReliableTopic`（走 Stream，字段名 `m`；每实例单组、publish 返回订阅者数、自动 XTRIM）；管理面 `getChannelNames`/`countListeners`/`countSubscribers`/`getActiveTopics`（Pattern）、`StatusListener`（订阅/退订回调）、`removeListener(handler)` 按回调删除
- 流：`RStream`（字段名是 JSON；组/claim/pending/nack、`fastClaim`/`fastAutoClaim`（JUSTID）、`readMany`/`readGroupMany`（支持每流独立 group）、`addListener`（keyspace xadd/xdel/xgroup-*））
- 延迟队列：`RDelayedQueue`（一个 ZSET 加一个 LIST，前缀 `Bc0Lc0`；`readAll`/`poll(count)`、负延迟校验、`pollLastAndOfferFirstTo`、无参 `remove()`）
- 环形缓冲：`RRingBuffer`
- 地理位置：`RGeo`（成员是 JSON；`GeoSearchArgs` 半径/矩形搜索、距离与坐标结果、`GEOSEARCHSTORE` 落库、`addIfExists`）
- 去重计数：`RHyperLogLog`
- 位图：`RBitSet`（位序跟 Redis 原生一致；`BITFIELD` 类型化读写、批量 `get/set`/`asBitSet`、Redis 8 `BITOP` 的 DIFF/DIFF1/ANDOR/ONE）
- 时间序列：`RTimeSeries`（`BBc0Lc0Lc0`；`addAll`、`getEntry/getValue`、`first/last` 与 `pollFirst/Last` 全族、正/反向 `range`/`entryRange`（可 limit）、`getAndRemove`、`remove/removeRange`、`stream/iterator` 分页取值、`addListener`（keyspace zadd/zrem），TTL 惰性过滤）
- 布隆过滤器：`RBloomFilter`（HighwayHash-128，测试向量对过）
- 概率结构：`RCuckooFilter`（addIfAbsent/existsMany/count/info）、`RTopK`（丢弃项/incrementBy/listWithCount）、`RTDigest`（min/max/cdf/rank/byRank/merge/info/reset，Redis 8 模块）
- 键、脚本、函数、搜索：`RKeys`（expire/expireAt 单/多键、clearExpire、remainTimeToLive、getSlot、move、migrate/MigrateArgs、远程 copy、swapdb、并行 flush、`addListener/removeListener` 全局 new/set/expired/del 事件、unlinkByPattern、`getKeysStream` 惰性扫描、`getKeysWithLimit`、`getKeysByPattern`）、`RScript`（含 `scriptKill`/`scriptFlush`）、`RFunction`（list/stats/dump/restore/kill）、`RSearch`（结构化/原始 schema 建索引（含 VECTOR FLAT/HNSW/SVS-VAMANA）、索引 info/别名/排序/params/聚合/同义词/词典/config/spellcheck、`hybridSearch` KNN/RANGE + RRF/LINEAR、`profileSearch/profileAggregate`、全文聚合游标、`alter`）、`RVectorSet`（VADD 高级参数、VEMB/VEMB RAW/VINFO/VLINKS WITHSCORES/VRANDMEMBER/VRANGE、VSIM 名称/分数/属性、属性读写、`iterator`/`stream`）
- 优先级队列（`GO_ONLY`，ZSET 加分数）、`RTransferQueue`（`GO_ONLY`）、`RBatch`（13 类类型化门面），……

细账在 [COMPATIBILITY.md](COMPATIBILITY.md)。

有些东西故意不做，跟 redi.go 一样。`RSortedSet` 要 LIST 再加比较器。`RTransaction` 要快照回滚。`LiveObject`、reactive / Rx 不做。`RRemoteService` 和 `RExecutorService` 的线上协议也不做。PRO 才有的 `RReliableQueue`、`RLocalCachedMapCache`、`RReliablePubSubTopic`、`RBitVectorStore`，这里没有。

## 值是怎么写进去的

看你 `set` 进去的 `{ n: 1 }`。

数字、字符串、布尔，能裸着就裸着。整数一旦超出 int32，就包成 `["java.lang.Long", v]`。对象多一个字段 `@class: java.util.LinkedHashMap`。数组写成 `["java.util.ArrayList", [...]]`，里面再套对象或数组，照样这么包。

读的时候，这些包装全部剥掉。所以 redi.py 写下的裸 JSON，还有普通字符串，这边都读得出来。

`StringCodec` 不包装，就是原始字符串。`RBinaryStream` 和 `RLexSortedSet` 不走这套编码。

## 怎么验

```sh
npm test             # vitest：线格式向量 + mock 功能 + 跟 Python/Java 实机互通
npm run build        # 双格式产物：dist/cjs + dist/esm
npm run pack-smoke   # npm pack 后装进临时消费者，验证 CJS/ESM 都能解析并写出同一 wire 输出
```

`test/interop.test.ts` 会拉起 `python`，用 `C:/GitHub/redi.py` 对三件事：编码输出是不是一样，HighwayHash 的测试向量对不对，multimap 的内部 id 是不是同一个。

本机 `127.0.0.1:6379` 有一个真 Redis 的话，mock 那套也会换上真客户端再跑一遍：传给 `Redisson.create({ client })` 就行。缺席的命令（比如 Redis 8.6 没有 `SUNIONCARD`/`XNACK`）按 `COMMAND INFO` 能力门控、打日志 SKIP，不会静默过。

集群测试 `test/live-cluster.test.ts` 需要一个本机 3 主节点集群：WSL 里跑 `wsl -e bash scripts/cluster-up.sh`（起在 7100-7102，`scripts/cluster-down.sh` 收尾），Windows 侧自动经端口转发可达；找不到集群时用例打警告跳过。
