# redi.ts vs Redisson 差距分析（Gap Analysis）

> 日期：2026-10-13；v2.19.0（2026-10-20）阶段 41：RQueue/RDeque 4.6.1 剩余面（poll(count)/indexOf/RPOPLPUSH/变参 add/*IfExists/LMOVE）。基线：`redisson-4.6.1.jar`（jar 实测为准）+ 当前主干源码。
> 方法：4 路并行调研（锁/集合/消息/数据基建），关键断言逐条源码 + jar + 实证复核。
> 状态：P0 修完 5 项（PES-hex、setPermits 删除、tryInit 对齐、RateLimiter tag+hex member、SpinLock BackOff；latch 经查是按请求幂等键，跨语言无影响），见 CHANGELOG 1.8.1/1.8.2。

## §0 已证伪的误报（不再跟踪）

| # | 原断言 | 结论 |
|---|---|---|
| 1 | RFairLock/SpinLock 在 4.6.1 已移除（redi.py 文档 #14） | **错**。jar 实测 `RedissonFairLock`、`RedissonSpinLock`、`getFairLock`、`getSpinLock(name, BackOff)` 全存在。Python 文档那条是错的。 |
| 2 | RIdGenerator 跨语言重号（每轮 N+1 个 id） | **误报**。`RedissonIdGenerator.handleIdRequests` 逐行推演 + alloc=5 交错 8 次实测（`5000,5005,5010,5006,5015,5007,5020,5008`，8/8 唯一）：每轮恰好 N 个，连续无重叠。 |
| 3 | RMapCache 无 Lua 是缺陷 | 取舍，不是遗漏（Buffer 传输与 Lua 传参互斥，见 AGENTS.md:7）。代价是容量驱逐非原子，已声明。 |
| 4 | SpinLock unlock-latch 致 Java 自旋者收不到 TS 解锁 | **错**。latch（`redisson_unlock_latch:{name}:<requestId>`，`RedissonBaseLock:212`）是按请求 id 的幂等键，acquire 路径从不读它——跨语言无影响。残留真差距仅 BackOff 不可配（已补，见下）。 |

## §1 P0：混部必现（已修 ✅，1.8.1–1.8.2；外加 spin HASH 互斥 live 验证）

| # | 问题 | 证据 | 修复 |
|---|---|---|---|
| P0-1 | PES permitId 是 latin1 随机串，经字符串 `luaEval` 在真 Redis 上 UTF-8 必损坏；多许可 `join(',')`，id 含 `0x2C` 即切分污染；与 Java UUID 文本域不同，跨语言 release 必然 miss | `locks.ts` 前 1077/1081 | id 改 hex 文本；`tryAcquire` 一律返 `string[]`，`releaseAll(string[])` 不变 |
| P0-2 | `RSemaphore.setPermits` 为 TS 自创（4.6.1 javap 确认无此方法）：裸 `SET` 不 publish，等待者饿死 + 脏总数 | `locks.ts` 前 961-963，零调用处 | 删除（Breaking；无调用者） |
| P0-5 | `RIdGenerator.tryInit` 要求双 `setnx` 成功；Java 只返回第二个（K1 存在 + K2 缺失时 Java 回 true） | `atomic.ts` 前 286-296 vs `RedissonIdGenerator.tryInitAsync` | 逐字对齐 Java（含疑似上游 bug，wire 兼容要求 bug-compatible） |
| P0-3r | SpinLock 重试间隔不可配（TS 硬编码 10ms；Java 默认指数 1→128ms ×2+jitter，可配 Constant） | `RedissonSpinLock` + `LockOptions:51-59,206-207` vs `locks.ts` 前 RSpinLock | `BackOff`/`BackOffPolicy` + `constantBackOff`/`exponentialBackOff`/`defaultBackOff`，`RSpinLock` 构造器第 5 参 + `getSpinLock(name, backOff?)`（latch 经查无需移植，见 §0-4） |
| P0-4a | RateLimiter 直接构造 `clientTag=''` 致 PER_CLIENT 后缀退化；随机 member latin1 字符串传参 | `probabilistic.ts` 前 485/547 | 默认 per-process tag（工厂仍传 client id）；member 改 hex 文本 |

## §2 P1：影响移植的 API 缺口

按面积排序。命名精确到 Java 签名。

**`RScoredSortedSet`**：交并差已补 ✅ (1.8.4：`union/unionWith/readUnion/readUnionEntries/intersection/intersectionWith/readIntersection/readIntersectionEntries/countIntersection/countIntersectionLimit/diff/readDiff/readDiffEntries`，store 版“只算 others 存入 self”与 Java 逐字对齐，live 验证 WEIGHTS 位置）；分数族已补 ✅ (1.5.0：`firstScore/lastScore`、`addScore/addScoreAndGetRank/addScoreAndGetRevRank/addAndGetRank/addAndGetRevRank`（单 Lua ZADD+rank）、`count`、`entryRange`（索引+分数）、`valueRangeReversed/entryRangeReversed`（分数 4/6 参与索引 2 参；Java 语义 startScore=下界，命令 `ZRANGE max min BYSCORE REV`）、`removeRangeByRank/removeRangeByScore`；`rank/revRank` 回 `number | null` 与 Java 一致）；`addIfAbsent`(NX)/`addIfExists`(XX CH)/`addIfLess`(LT CH)/`addIfGreater`(GT CH) ✅ (2.2.0)；`addAll*` 条件批量、`random`/`randomEntries`、`firstEntry`/`lastEntry`、非阻塞 `pollFirst/LastEntry` 与阻塞 `pollFirst/LastWithTimeout`、`pollFirst/LastFromAny`（BZPOPMIN/MAX、BZMPOP）✅ (2.3.0)；区间读取改用 `ZRANGE ... BYSCORE [REV]` ✅ (2.3.0，弃用 `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`)；惰性 `iterator/stream/entriesIterator`（ZSCAN）✅ (2.16.0)。

**`RSet`**：store 交并差已补 ✅ (1.8.8)，且 **v2.0.0 修正为 Java 4.6.1 裸键形状**（`SUNIONSTORE/SINTERSTORE/SDIFFSTORE` + `dest others…`，无 numkeys、self 仅作目标且不入输入——此前多传 numkeys 并把 self 当输入，静默算错）；read 变体 self 作首输入的语义不变；`addAllCounted/removeAllCounted/containsEach` ✅ (2.5.0)；`iterator/distributedIterator/stream` 惰性 SSCAN ✅ (2.16.0)；`countIntersection`（含 LIMIT）与键空间监听（sadd/srem/spop/sinterstore/sunionstore/sdiffstore）✅ (2.30.0)；`countUnion` 使用 SUNIONCARD（Redis 8.10+，本机 8.6 不可用）；`countDiff`/`countUnionApprox` 仅存在于新版源码、不在 4.6.1 jar，记为版本外。

**`RStream`**：组运维 ✅ (1.8.6) + 读/写全选项 ✅ (1.8.7：`addWithOptions`/`readGroupWith`/`nack`/`ackWithDelete`，参数顺序与源码逐字对齐)；`fastClaim`/`fastAutoClaim`（JUSTID）与 `readMany`/`readGroupMany`（多流 XREAD/XREADGROUP）✅ (2.5.0/2.10.0)；`addListener(ObjectListener)` ✅ (2.10.0，keyspace `xadd|xdel|xgroup-*`，需服务端 `notify-keyspace-events` K+E；未配置时订阅静默)；`readGroupMany` 每流独立 group/consumer ✅ (2.11.0，按 group+consumer 分桶各发一条 XREADGROUP)。

**`RMap`**：`put`/单键 `remove`/`putIfAbsent` 已改为单 Lua 原子旧值返回 ✅ (1.8.3)；`getLockByKey` 六件套 ✅ (1.4.0，命名与 Java 4.6.1 逐字节一致)；惰性 HSCAN 已补 ✅ (1.5.0：`keysIterator/valuesIterator/entriesIterator(pattern, count)` 异步迭代器、`randomKeys/randomEntries`（HRANDFIELD [WITHVALUES]）、`valueSize`（HSTRLEN））；物化视图 `keySet/values/entrySet(pattern?, count?)`、`readAllEntrySet` 与三参 `replace` ✅ (2.6.0)；仍缺 `putAllWithTTL`（在 `RMapCache.putAll`）、`remove` 联合返回的类型收窄（TS 以重载返回 `V|null|boolean`）。

**`RMapCache`**：`expireEntry`（单条目，拒已过期）、`expireEntries`（批量）、`remainTimeToLive`（`-2/-1/ms`）、`getWithTTLOnly`（不刷新 idle/access）、`getMaxSize`/`setMaxSize`、`putAll`（TTL 批量）、`compute`/`computeIfAbsent`（per-key 锁 + TTL）、事件订阅双向 ✅ (1.4.0/2.7.0/2.9.0)；`RLeasedMap` 归 PRO（开源 jar 无 `RedissonLeasedMap` 实现），不再作为开源缺口。

**`RMapCacheNative`**（现有 4 个 vs ~38）：`putIfAbsent(key, value, ttl|Date)`、`putIfAllKeysExist/Absent`（全字段条件 Lua + 逐字段 TTL）、`fastPut`/`fastPutIfAbsent`/`putIfExist`/`fastPutIfExist`（relative/absolute TTL）、`expireEntry(IfNotSet/IfGreater/IfLess)`（毫秒/Date）、`expireEntries(IfNotSet/IfGreater/IfLess)` 批量、`clearExpire` + `remainTimeToLive`（`-2` 缺失）✅ (1.4.0/2.12.0/2.18.0/2.20.0；`fastPutIfExist` 为 TS 扩展，4.6.1 jar 仅到 `fastPutIfAbsent`)；结构化 `PutArgs`（HSETEX KEEPTTL/PX/PXAT、FXX/FNX）与 `compute/computeIfAbsent` ✅ (2.29.0；TS 的 Instant 重载以绝对 `Date` 表达)；4.6.1 Native 写面已收口。注：Java 4.6.1 对 HPERSIST `-2` 回 `null`，TS 统一 `false`（boolean 契约）。

**`RBlockingQueue/Deque`**：`pollFromAny`/`pollFromAnyWithName`（BLPOP）、`pollFirstFromAny`/`pollLastFromAny`（BLMPOP + COUNT）、`pollLastAndOfferFirstTo`/`takeLastAndOfferFirstTo`（BLMOVE）、deque `move`/`moveWithTimeout`（LMOVE/BLMOVE）✅ (2.1.0)；基类 `RQueue/RDeque` 的 `poll(count)`/`indexOf`/RPOPLPUSH/变参 add/`*IfExists`/`pollFirst|Last(count)`/`move` ✅ (2.19.0)；亚秒超时/`timeoutMs<=0` 语义、`take` 独立连接、`drainTo` 原子化均已在 v1.2.0/v1.3.0 修正 ✅。`subscribeOnElements/unsubscribe` 元素订阅 ✅ (2.23.0，持续 take + 异步背压 + 取消断开 BLPOP 连接；有界队列走容量感知 poll)。`pollLastFromAnyWithName`（BRPOP + 源队列名）✅ (2.26.0)；RBlockingQueue/Deque 4.6.1 面已收口（`QueueMoveElementsArgs` 仅存在于新版源码，4.6.1 jar 无此类，记为版本外）。

**`RLock` 面**：`lockInterruptibly` ✅ (2.7.0，以 AbortSignal 模拟线程中断：等待中 abort 抛 `AbortError` 且不获取锁)；`RWLock` 读锁 `:rwlock_timeout:<n>` 伴生键 ✅ (2.8.0，逐次获取创建、解锁按剩余键重算 HASH TTL、看门狗同步续期——修复 Java 释放读锁可能误删仍有其他读者的 HASH)；`tryLock` 四重载分解（TS 三参合一）仍为 TS 形态、`*Async` 命名、`newCondition`、`RObservable.addListener` 未做；`RWLock`/`FairLock` view 的 `forceUnlock/isLocked/isHeldByThread/getHoldCount` ✅ (1.4.0)，唤醒常量对齐 Java `LockPubSub` ✅；仍缺 `CommonOptions`；`FencedLock` 的 `tryLockAndGetToken`（非阻塞/等待）✅ (2.1.0，live 与 Java token 序列互通）；仍存语义差异：TS `lockAndGetToken` 是 try-only，Java 默认阻塞（需阻塞时请用 `lockWaitAndGetToken`）；`RNonReentrantLock` 默认放行 vs Java 抛；`MultiLock` 接受 `LockLike`（`RFairLock`/RW 视图可混装）✅ (1.4.0)，仍缺批锁变体、返回类型非 `RLock`；`RedLock`：复核 4.6.1 jar/源码后确认 **RedissonRedLock 无 drift 扣减**（继承 MultiLock，末尾统一按完整 lease 重设 TTL），TS 现行实现与其一致，原“缺 drift”条目不成立。

**`RTopic` 系管理面**：`getChannelNames/getPatternNames/getActiveTopics`、`countListeners`（本地监听器数）与 `countSubscribers`（`PUBSUB NUMSUB`；分片 `SHARDNUMSUB` 带回退）✅ (2.5.0)；`StatusListener`（onSubscribe/onUnsubscribe，`removeListener` 同时处理消息/状态两类句柄）✅ (2.9.0)；`removeListener(handler)` 按回调句柄删除 ✅ (2.11.0)；`Class<M>` 类型门控为 Java 特有（TS codec 负责解码，无运行时类），记录为设计差异；`RReliableTopic` 已对齐 4.6.1：每实例单组、`publish` 返回组数、心跳 ZSET 清理过期组、按最早组位点自动 XTRIM、`delete()` 同时删 stream+timeout ✅ (2.17.0)；仍缺可靠主题的 `Class<M>` 门控与 `RReliableFanout`（PRO）。

**`RBitSet`**：已补 ✅ (1.5.0)：`BITFIELD` 全族（`BitFieldArgs`/`BitOffset.bit/index`/`overflow`，OVERFLOW FAIL 回 `null`）、类型化读写（signed/unsigned 的 get/set/increment + `getByte/Short/Integer/Long` 快捷）、`or/and/xor/not` 回字节长、Redis 8 `BITOP DIFF/DIFF1/ANDOR/ONE`、`size()`（STRLEN×8）与 `length()`（最高置位 + 1）区分、`setRange`/`clear(from,to)` 为 `to` 排他；批量 `get(...bits)`/`set(long[]|Set,boolean)`/区间 `set(from,to,value)`、`asBitSet()` ✅ (2.12.0)。

**`RGeo`**：已补 ✅ (1.5.0)：`search(GeoSearchArgs)` 全族（from lon-lat/member、radius/box、order、count/countAny）、`searchWithDistance/Position`、`storeSearchTo/storeSortedSearchTo`、`addIfExists`（单成员 Lua geopos→geoadd；变参 GEOADD XX 只计新增故恒 0）；命令选择与 4.6.1 jar 逐条对齐（半径→`GEORADIUS_RO`/`GEORADIUSBYMEMBER_RO`、Box→`GEOSEARCH`、存储→`GEORADIUS ... STOREDIST`/`GEOSEARCHSTORE`；4.6.1 无 polygon，新版才有）；未继承 `RScoredSortedSet`。

**`RTimeSeries`**：`addAll`、`getEntry/getValue/getAndRemove(Entry)`、`first/last`（值/Entry/Timestamp/count 变体）、`pollFirst/Last`（值/Entry/count）、`range/rangeReversed/entryRange/entryRangeReversed`（limit）、`remove/removeRange` ✅ (2.4.0，读侧按 TTL ZSET 惰性过滤；label 形状兼容 n=2/3/4)；`stream/iterator` 分页取值 ✅ (2.6.0)；`addListener` ✅ (2.10.0，ScoredSortedSet zadd/zrem keyspace，需 K+E)。

**`RKeys`**：`expire` 单/多键、`expireAt` 单/多键、`clearExpire`、`remainTimeToLive`、`getSlot`（客户端 CRC16-XMODEM，hash-tag 感知；Java 走 `CLUSTER KEYSLOT`）、`move`、`unlinkByPattern` ✅ (2.2.0)；`getKeysStream` 惰性 SCAN 生成器 ✅ (2.11.0)；`getKeysWithLimit`（有/无 pattern）与 `getKeysByPattern` ✅ (2.26.0)；`migrate`（单键与 `MigrateArgs` 多键/COPY/REPLACE/AUTH2）、远程 `copy`（MIGRATE COPY，保留本地 COPY 重载）、`swapdb`、`flushdbParallel/flushallParallel` ✅ (2.24.0)；`addListener/removeListener` 全局键空间事件 ✅ (2.25.0：new/set/expired/del；按 Redis 客户端共享 hub，onFlush 需 RESP3，本客户端明确抛错)；RKeys 4.6.1 面已收口（`time` 在 4.6.1 接口中不存在，原 GAP 条目为误报）。

**`RScript`**：`scriptKill/scriptFlush` ✅ (2.12.0)；`mode` 参数保留在签名中但 ioredis 不复刻 Redisson 的 READ_ONLY/READ_WRITE 路由（单节点无影响；集群路由由 ioredis 处理）；`STRING/MAPVALUELIST` 精确 cast 未做（返回通用 EVAL 结构）。

**`RFunction`**：`kill/list/stats/dump/restore/restoreAndReplace/restoreAfterFlush` ✅ (2.12.0)；库名由调用方显式传入（Java 的 `libraryName` 字段读取未复刻）、`FunctionMode`/`FunctionResult` 精确 cast 未做；`load()` 无 REPLACE 时不再发送空参数（2.12.0 修复）。

**`RBuckets`**：`setIfAllKeysExist/setIfAllKeysAbsent` ✅ (2.2.0，`MSETEX … NX|XX [PX ms]`，Redis 8+；无该命令时显式报错而非降级)。

**`RSearch`**：索引 `getIndexes/hasIndex/info/dropIndexAndDocuments`、别名三件套、增强 `search`（sort/return/inKeys/filter/params/dialect/timeout）、`aggregate`（load/groupBy/reduce/apply/sort/limit/rawArgs）、`updateSynonyms/dumpSynonyms`、`addDict/delDict/dumpDict`、`getConfig/setConfig`、`spellcheck` ✅ (2.15.0，Redis 8 内置 RediSearch；缺失时能力门控)；`hybridSearch`（KNN/RANGE + RRF/LINEAR）、`profileSearch/profileAggregate`、聚合游标 `readCursor/delCursor`、`alter` ✅ (2.22.0)；`IndexOptions/FieldIndex` 结构化建索引与 `alter` ✅ (2.27.0：TEXT/TAG/NUMERIC/GEO/GEOSHAPE/VECTOR FLAT|HNSW；TS 用普通对象而非 Java builder 类；INDEXEMPTY/INDEXMISSING 需在 SORTABLE 前)；仍缺 SVS-VAMANA 向量参数与 VectorAddArgs 高级对象。注：本机 Redis 8.6 不支持 `YIELD_DISTANCE_AS`，且 FT.HYBRID RANGE 过滤偏宽；参数透传与 Java 4.6.1 一致。

**`RJsonBucket`**：文档/路径 `get/set/delete`、`setIfAbsent/trySet/setIfExists`、原子 `compareAndSet`（root/路径，`$` 期望值按 RedisJSON match array 包装）、`getAndSet/getAndDelete`、`stringSize/stringAppend`、`arrayAppend/Index/Insert/Size/Pop/PollFirst/PollLast/Trim`、`incrementAndGet`、`merge/clear/countKeys/getKeys/toggle/getType` ✅ (2.13.0；默认纯 JSON，可选 Codec；Redis 8.6 内置 JSON 模块，缺失时能力门控)；`RJsonBuckets.set/get/getWithPath`（MSET/MGET）✅ (2.18.0)；`*Multi` 多匹配族与多路径 `get(codec, paths...)` ✅ (2.21.0；并修正两参 `arrayIndex` 的全数组搜索)；仍缺 `FPHAType`（本机 RedisJSON 不支持 `JSON.SET … FPHA`，版本相关）。

**`RVectorSet`**：`size/dimensions/getVector/contains/random/range/getNeighbors/getInfo/getAttributes/setAttributes` ✅ (2.12.0，Redis 8 VEMB/VDIM/VRANDMEMBER/VRANGE/VLINKS/VINFO/VGETATTR/VSETATTR)；`add(VectorAddArgs)`（REDUCE/CAS/Q8/BIN/EF/SETATTR/M）、`getRawVector`、`getNeighborEntries`（VLINKS WITHSCORES）、`getSimilar/getSimilarEntries/getSimilarEntriesWithAttributes`（元素/向量 + COUNT/EPSILON/EF/FILTER/FILTER-EF/TRUTH/NOTHREAD）✅ (2.28.0)；`iterator/stream`（VRANGE 分批、`(` 排他续页）✅ (2.33.0)；RVectorSet 4.6.1 面已收口。

**概率三件套**：`RTDigest`（create(compression)/mergeWith/min/max/quantile/cdf/trimmedMean/rank/revRank/byRank/byRevRank/info/reset）✅ (2.14.0)、`RCuckooFilter`（init/addIfAbsent/existsMany/count/info）✅ (2.14.0)、`RTopK`（init 参数/add 丢弃项/incrementBy/contains/count/list/listWithCount/info）✅ (2.14.0)；仍缺 `TDigestMergeArgs`、`CuckooFilterAddArgs`/`TopKInitArgs` 的高级参数对象与批量元素重载。

**`RAtomic*`**：`getAndIncrement/getAndDecrement`、`getAndDelete`、`setIfLess/setIfGreater`、Double `compareAndDelete` ✅ (2.2.0)；`addListener/removeListener`（Long incrby、Double incrby+incrbyfloat）✅ (2.31.0)；仍缺 `IncrementArgs`（增量配置，后端 INCREX 非标准 Redis 命令）；`getAndAdd` 浮点回算与超 `2^53` 的 `compareAndSet` 已在 v1.3.0 修复 ✅（Long 字符串 CAS / BigInt，Double 单 Lua 旧值）。

**`RLocalCachedMap`**：`timeToLiveMs/maxIdleMs` 已在近端缓存兑现 ✅ (1.1.0)；`maxSize` 下 LRU/LFU 淘汰与 `getCachedMap()` 本地快照 ✅ (2.1.0)；`preloadCache(count)`（HSCAN 限量）✅ (2.16.0)；`loader/writer` 不移植（外部存储语义，对 Redis wire 无影响）——决策记录；仍缺重连策略、Caffeine、过期事件策略。

**`RRateLimiter`**：`keepAliveTime` 续期 ✅ (2.1.0，acquire 脚本内 `PEXPIRE` config/value/permits；keepAlive=0 时 value/permits 继承 config HASH 的 TTL，与 Java 4.6.1 一致)。

**`RMultimapCache`**：可选周期清扫 ✅ (2.1.0，`enablePeriodicEviction(intervalMs)` / `evictExpired()` / `disablePeriodicEviction()`；默认仍为 `checkExpiry` 惰性)。

**`RDelayedQueue`**：`size()` 已对齐 Java（staging LLEN）✅、`delete()` 不再误删目标队列 ✅ (1.1.0)；**后台迁移调度器 ✅ (2.0.0)**：`start()/stop()/destroy()`（Java `QueueTransferTask`/`QueueTransferService` 移植——channel 订阅走 `duplicate()` 连接、deadline 定时器 >10ms 定时否则立即、5s 错误重试、迁移 Lua 返回下一表头 deadline 重臂；`destroy()` = 摘除任务不删数据），单机 + 集群实测；`readAll/poll(count)` ✅ (2.12.0)、负延迟校验、`add`/单参 `offer`/`indexOf` 拒绝、无参 `remove()`、`pollLastAndOfferFirstTo` ✅ (2.32.0)；4.6.1 面已收口（`move` 仅存在于新版源码，记为版本外）。

**`RRingBuffer`**：缺 `readNewest/readOldest/peekLast/addAll`；`delete()` override 已补 ✅ (1.1.0，容量 key 与 LIST 一并清理；`clear()` 保持 Java 只删 LIST)。

**`RBatch`**：类型覆盖已补 13 类 ✅ (2.1.0：原 7 类 + `BatchDeque/AtomicDouble/BitSet/Geo/Stream/MapCacheNative`，写排队、读在 `execute()` 后 resolve，对齐 RFuture；`BatchAtomicDouble.getAndAdd` 单 Lua 精确旧值、native map-cache `HPEXPIRE FIELDS` 字段 TTL；Lua 版 ops 走 live 测，mock pipeline 无 `eval`)；仍存读回占位非即时值；pub/sub 不可进 batch（`duplicate()` 真实连接）。

## §3 P2（统一三刀，见报告正文）

① 时间全收敛 `ms:number`（保持）；② 不存在一律 `null`（`rank/revRank/getScore` 的 `-1` 是唯三例外，文档置顶）；③ 联合返回拆分。另 `getConfig` 多 `keepAliveMs`、`release` 回 number、`containsAll` 回 number、`RList.add` 回长度等，一律 COMPATIBILITY 标“名同义异”。

## §4 明确不做（保持）

`RSortedSet-Comparator`（无跨语言表示）、`RTransaction`（快照回滚≠MULTI）、`LiveObject`、`reactive/Rx`、`Executor/Remote` 真协议、`RTransferQueue` 握手（同名不同物）、PRO 四件套。另建议记名（4.6.1 无，本属版本外）：`RCountMin`、`RTimeSeriesNative`、`RReliablePubSubTopic`、`RLocalCachedMapCache`。**设计差异（非缺口）**：`CommonOptions`/`InvocationOptions` 不移植——TS 工厂直接接收名称，调用超时/重试归 ioredis；`Class<M>` 监听类型门控不适用——TS codec 负责解码，无运行时类；`RKeys.time` 在 4.6.1 接口中不存在（原 GAP 误报）。

## §5 路线图

- P0 剩余：RateLimiter 配置重读（`getConfig` 已是 fresh-read；acquire 路径沿用实例缓存，与 Java 实例字段行为大体一致，决策保持，见 `COMPATIBILITY.md`）。
- P1 第一批（已完成）：stream claim/pending/info ✅ (1.8.6) + add/read opts/nack/ackdel ✅ (1.8.7)；`RMap.put` 原子 Lua ✅ (1.8.3)；zset 交并差 ✅ (1.8.4)；`RBatch` 决策 ✅ (1.8.5)；`RSet` store 交并差 ✅ (1.8.8，v2.0.0 修正为 Java 裸键形状)。
- **阶段 16（已完成 ✅，v1.1.0）**：伴生键 `delete()/unlink()` 全量闭环、`RDelayedQueue` size/delete 对齐、`RLocalCachedMap` TTL/idle 兑现；验证见 `test/redisson.test.ts` + `test/live-phase16.test.ts`。
- **阶段 17（已完成 ✅，v1.2.0）**：`RBoundedBlockingQueue` 信号量协议（DECRBY+RPUSH / INCRBY+PUBLISH、阻塞 put/timed offer）、阻塞命令独立连接与浮点秒超时、Topic 订阅生命周期；验证见 `test/live-phase17.test.ts`。
- **阶段 18（已完成 ✅，v1.3.0）**：`JsonCodec` BigInt 无损、`RAtomicLong` 字符串精确 CAS、`RAtomicDouble.getAndAdd` 单 Lua、`RBlockingQueue.drainTo` 原子提取；验证见 `test/wire.test.ts` + `test/live-phase18.test.ts`。
- **阶段 19（已完成 ✅，v1.4.0）**：Key 级同步器家族（6 种，命名与 Java 4.6.1 逐字节一致）、RW 视图补齐与 `LockPubSub` 唤醒常量对齐、`RMultiLock/RRedLock` 接受 `LockLike`、`RMapCache` `putAll/expireEntry/remainTimeToLive` 与双向事件监听、`RMapCacheNative` 条件过期/HPERSIST；验证见 `test/wire.test.ts` + `test/redisson.test.ts` + `test/live-phase19.test.ts`。
- **阶段 20（已完成 ✅，v1.5.0）**：`TsProbe` REPL 常驻化（全量 live 测试 673s → 56.9s）、`RMap` HSCAN 异步迭代器 + `HRANDFIELD`/`HSTRLEN`、`RScoredSortedSet` 分数族与反向区间/分数删除、`RBitSet` `BITFIELD` 与 Redis 8 `BITOP` 族、`RGeo` `GeoSearchArgs` 族与 `GEOSEARCHSTORE`；验证见 `test/redisson.test.ts` + `test/live-phase20.test.ts`。
- **阶段 21（已完成 ✅，v2.0.0）**：ESM+CJS 双格式构建与 `pack-smoke` 验收、`Config.client` 接受 `Redis | Cluster`（导出 `RedisClient`）及约 87 处内部 cast 清理、`RDelayedQueue` 后台迁移器（Java `QueueTransferTask` 移植，单机+集群实测）、`RSet` store wire 修复（4.6.1 裸键形状）、缺失命令能力门控（`COMMAND INFO` + SKIP 日志，`XNACK`/`SUNIONCARD` 在 Redis 8.10.2 实测通过）、3 主节点集群实测（`test/live-cluster.test.ts`）；验证见 `test/redisson.test.ts` + `test/live-phase21.test.ts` + `test/live-cluster.test.ts`。
- **阶段 22（已完成 ✅，v2.1.0）**：`RFencedLock.tryLockAndGetToken`（4.6.1 非阻塞/等待语义，Java token 序列互通）、阻塞队列/双端队列 `pollFromAny`/BLMPOP/LMOVE/BLMOVE 全族、`RRateLimiter` keepAlive 脚本内续期（keepAlive=0 时继承 config TTL）、`RLocalCachedMap` LRU/LFU 淘汰 + `getCachedMap()` 视图、`RMultimapCache` 可选周期清扫、`RBatch` 门面扩至 13 类；验证见 `test/redisson.test.ts` + `test/live-phase22.test.ts`（5/5，含 Java 4.6.1 交叉断言）。
- **阶段 23（已完成 ✅，v2.2.0）**：`RKeys` 运维族（expire/expireAt 单+多键、clearExpire、remainTimeToLive、getSlot、move、unlinkByPattern）、`RAtomicLong/Double` 标量余项（getAndIncrement/Decrement、setIfLess/Greater、getAndDelete、Double compareAndDelete）、`RBuckets` 条件写（MSETEX NX/XX）、`RScoredSortedSet` addIfAbsent/Exists/Less/Greater；验证见 `test/redisson.test.ts` + `test/live-phase23.test.ts`（4/4；含 Java 读回、live cluster `CLUSTER KEYSLOT` 对照、MOVE 跨 db）。
- **阶段 24（已完成 ✅，v2.3.0）**：`RScoredSortedSet` 收口——`addAll*` 条件批量（NX/XX/GT/LT CH）、`random`/`randomEntries`、`firstEntry`/`lastEntry`、非阻塞 `pollFirst/LastEntry`、阻塞 `pollFirst/LastWithTimeout`（BZPOPMIN/MAX）与 `pollFirst/LastFromAny`（BZMPOP）、区间读取改 `ZRANGE … BYSCORE [REV]`；验证见 `test/redisson.test.ts` + `test/live-phase24.test.ts`（2/2；含 Java 分数读回与真实阻塞唤醒）。
- **阶段 25（已完成 ✅，v2.4.0）**：`RTimeSeries` 读写全族（addAll、getEntry/getValue、first/last/poll 全族、正反向 range/entryRange（limit）、getAndRemove、remove/removeRange、TTL 惰性过滤、label n=2/3/4 兼容）；验证见 `test/redisson.test.ts` + `test/live-phase25.test.ts`（2/2，含 Java ts-range/ts-add-label 往返）。
- **阶段 26（已完成 ✅，v2.5.0）**：`RTopic/RPatternTopic/RShardedTopic` 管理面（channel/pattern names、countListeners、countSubscribers、getActiveTopics）、`RStream.fastClaim/fastAutoClaim`（JUSTID）与 `readMany`（多流 XREAD）、`RSet.addAllCounted/removeAllCounted/containsEach`；验证见 `test/redisson.test.ts` + `test/live-phase26.test.ts`（2/2，live 订阅计数/active topics、XCLAIM/XAUTOCLAIM JUSTID、多流读）。
- **阶段 27（已完成 ✅，v2.6.0）**：`RMap` 物化视图 `keySet/values/entrySet(pattern?, count?)`、`readAllEntrySet` 与三参 `replace`；`RTimeSeries.stream/iterator` 分页产出值；验证见 `test/redisson.test.ts` + `test/live-phase27.test.ts`（2/2，视图 vs Java HSCAN、条件 replace/remove Java 读回、stream/iterator vs ts-range）。
- **阶段 28（已完成 ✅，v2.7.0）**：`RLock.lockInterruptibly`（AbortSignal；等待中 abort → `AbortError`）、`RMapCache.expireEntries/getWithTTLOnly/getMaxSize`；RedLock drift 条目经 4.6.1 源码复核后更正为不适用；验证见 `test/redisson.test.ts` + `test/live-phase28.test.ts`（2/2，双客户端争锁中断、TTL ZSET/only-read/max-size）。
- **阶段 29（已完成 ✅，v2.8.0）**：`RReadWriteLock` 读锁 `:rwlock_timeout:<n>` 伴生键协议（逐次获取 SET+PX、解锁删除并重算 HASH TTL、看门狗续期），修复 Java 释放读锁可能误删仍有其他读者的 HASH；验证见 `test/redisson.test.ts` + `test/live-phase29.test.ts`（1/1，Java 持有读锁 + TS 读锁混部存活）。
- **阶段 30（已完成 ✅，v2.9.0）**：`RMapCache.compute/computeIfAbsent`（per-key 分布式锁 + TTL + null 删除语义；并发 compute 串行化）与 `RTopic/RShardedTopic` `StatusListener`（订阅/退订回调，removeListener/removeAllListeners 统一管理）；验证见 `test/redisson.test.ts` + `test/live-phase30.test.ts`（2/2，含 Java `mc-get` 读回与实时状态回调）。
- **阶段 31（已完成 ✅，v2.10.0）**：`RStream/RTimeSeries.addListener(ObjectListener)`（keyspace 通知：stream 六事件、timeseries zadd/zrem；依赖 `notify-keyspace-events` K+E）与 `RStream.readGroupMany`（单条 XREADGROUP 多流读）；验证见 `test/redisson.test.ts` + `test/live-phase31.test.ts`（2/2，六事件/退订静默/多流 group 读）。
- **阶段 32（已完成 ✅，v2.11.0）**：`RKeys.getKeysStream/getKeysStreamByPattern` 惰性 SCAN、Topic `removeListener(handler)` 按回调删除、`RStream.readGroupMany` 每流独立 group/consumer（按 group+consumer 分桶）；CommonOptions/Class<M> 记录为设计差异；验证见 `test/redisson.test.ts` + `test/live-phase32.test.ts`（2/2）。
- **阶段 33（已完成 ✅，v2.12.0）**：最终审计——RScript `scriptKill/scriptFlush`、RFunction `kill/list/stats/dump/restore(+变体)`（修复 load 空参数）、RVectorSet 深层命令、RBitSet 批量/`asBitSet`、RMapCacheNative 绝对 `Date`（HPEXPIREAT）、RDelayedQueue `readAll/poll(count)`；同时剔除误报并归档设计差异；验证见 `test/redisson.test.ts` + `test/live-phase33.test.ts`（3/3）。
- **阶段 34（已完成 ✅，v2.12.1）**：稳定化收尾——连续三次全量 246 项全绿（82.15s / 71.40s / 71.62s，无重试/阈值调整）、README/COMPATIBILITY/GAP 一致性校对、RSearch/RedisJSON 模块能力门控说明、PRO/设计差异清单定稿。
- **阶段 35（已完成 ✅，v2.13.0）**：`RJsonBucket` 全族（文档/路径读写、NX/XX/CAS、getAndSet/Delete、字符串/数组/数字操作、merge/keys/type/toggle/clear；默认纯 JSON + 可选 Codec），新增 Java `json-set/json-get` 探针做双向验证；验证见 `test/live-phase35.test.ts`（1/1，Redis 8.6 内置 JSON 模块）。
- **阶段 36（已完成 ✅，v2.14.0）**：`RTDigest`（create/merge/min/max/quantile/cdf/trimmedMean/rank/revRank/byRank/byRevRank/info/reset）、`RCuckooFilter`（init/addIfAbsent/existsMany/count/info）、`RTopK`（init 参数/add 丢弃项/incrementBy/listWithCount/info）；验证见 `test/live-phase36.test.ts`（3/3，Redis 8 模块能力门控）。
- **阶段 37（已完成 ✅，v2.15.0）**：`RSearch` 扩展（索引 info/别名/增强 search/aggregate/同义词/词典/config/spellcheck）；验证见 `test/live-phase37.test.ts`（1/1，Redis 8 内置 RediSearch）。
- **阶段 38（已完成 ✅，v2.16.0）**：`RSet` 惰性 `iterator/distributedIterator/stream`（SSCAN）、`RScoredSortedSet` `iterator/stream/entriesIterator`（ZSCAN）、`RLocalCachedMap.preloadCache(count)`；验证见 `test/redisson.test.ts` + `test/live-phase38.test.ts`（1/1，编码前缀 MATCH + 限量预载）。
- **阶段 39（已完成 ✅，v2.17.0）**：`RReliableTopic` 对齐 4.6.1——每实例单组（监听器共享）、`publish` 返回组数、心跳 ZSET 清理过期组、按最早组位点自动 XTRIM、最后监听器移除时销毁组；验证见 `test/live-phase39.test.ts`（1/1）与既有 phase3/17 回归。
- **阶段 40（已完成 ✅，v2.18.0）**：`RJsonBuckets.set/get/getWithPath`（MSET/MGET）与 `RMapCacheNative.putIfAllKeysExist/Absent`、`expireEntries(IfNotSet/IfGreater/IfLess)` 批量；FPHA 因本机 RedisJSON 不支持而记录为版本相关缺口；验证见 `test/redisson.test.ts` + `test/live-phase40.test.ts`（2/2）。
- **阶段 41（已完成 ✅，v2.19.0）**：`RQueue.poll(count)/indexOf/pollLastAndOfferFirstTo`、`RDeque` 变参 `addFirst/addLast`、`addFirstIfExists/addLastIfExists`、`pollFirst/pollLast(count)`、`move`（LMOVE），有界队列 `poll(count)` 保持容量记账；验证见 `test/redisson.test.ts` + `test/live-phase41.test.ts`（1/1）。
- **后续纵深研究与分阶段演进路线图**：详见根目录 [演进方案.md](../演进方案.md)。
- 不做：§4。
