# redi.ts vs Redisson 差距分析（Gap Analysis）

> 日期：2026-10-13；v2.5.0（2026-10-19）更新阶段 26 收口项（Topic 管理面、RStream JUSTID/多流读、RSet counted/containsEach）。基线：`redisson-4.6.1.jar`（jar 实测为准）+ 当前主干源码。
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

**`RScoredSortedSet`**：交并差已补 ✅ (1.8.4：`union/unionWith/readUnion/readUnionEntries/intersection/intersectionWith/readIntersection/readIntersectionEntries/countIntersection/countIntersectionLimit/diff/readDiff/readDiffEntries`，store 版“只算 others 存入 self”与 Java 逐字对齐，live 验证 WEIGHTS 位置）；分数族已补 ✅ (1.5.0：`firstScore/lastScore`、`addScore/addScoreAndGetRank/addScoreAndGetRevRank/addAndGetRank/addAndGetRevRank`（单 Lua ZADD+rank）、`count`、`entryRange`（索引+分数）、`valueRangeReversed/entryRangeReversed`（分数 4/6 参与索引 2 参；Java 语义 startScore=下界，命令 `ZRANGE max min BYSCORE REV`）、`removeRangeByRank/removeRangeByScore`；`rank/revRank` 回 `number | null` 与 Java 一致）；`addIfAbsent`(NX)/`addIfExists`(XX CH)/`addIfLess`(LT CH)/`addIfGreater`(GT CH) ✅ (2.2.0)；`addAll*` 条件批量、`random`/`randomEntries`、`firstEntry`/`lastEntry`、非阻塞 `pollFirst/LastEntry` 与阻塞 `pollFirst/LastWithTimeout`、`pollFirst/LastFromAny`（BZPOPMIN/MAX、BZMPOP）✅ (2.3.0)；区间读取改用 `ZRANGE ... BYSCORE [REV]` ✅ (2.3.0，弃用 `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`)；仍缺 `iterator/stream` 专用视图（ZSCAN/ZRANGE 迭代语义）。

**`RSet`**：store 交并差已补 ✅ (1.8.8)，且 **v2.0.0 修正为 Java 4.6.1 裸键形状**（`SUNIONSTORE/SINTERSTORE/SDIFFSTORE` + `dest others…`，无 numkeys、self 仅作目标且不入输入——此前多传 numkeys 并把 self 当输入，静默算错）；read 变体 self 作首输入的语义不变；`addAllCounted/removeAllCounted/containsEach` ✅ (2.5.0)；仍缺 count 族完整面：`countIntersection/countDiff/countUnion` 的 limit 变体与 `countUnionApprox`（Java 形状 = `SINTERCARD/SDIFFCARD/SUNIONCARD numkeys self …names [APPROX] [LIMIT n]`，Redis 8.10+ 能力门控）、`iterator/distributedIterator/stream` 视图。

**`RStream`**：组运维 ✅ (1.8.6) + 读/写全选项 ✅ (1.8.7：`addWithOptions`/`readGroupWith`/`nack`/`ackWithDelete`，参数顺序与源码逐字对齐)；`fastClaim`/`fastAutoClaim`（JUSTID）与 `readMany`（单 XREAD 多流）✅ (2.5.0)；仍缺 `addListener`。

**`RMap`**：`put`/单键 `remove`/`putIfAbsent` 已改为单 Lua 原子旧值返回 ✅ (1.8.3)；`getLockByKey` 六件套 ✅ (1.4.0，命名与 Java 4.6.1 逐字节一致)；惰性 HSCAN 已补 ✅ (1.5.0：`keysIterator/valuesIterator/entriesIterator(pattern, count)` 异步迭代器、`randomKeys/randomEntries`（HRANDFIELD [WITHVALUES]）、`valueSize`（HSTRLEN））；缺 Java 形态的 `keySet/values/entrySet(pattern, count)` Set/Collection 视图（迭代器已覆盖大 key 扫描需求）、`putAllWithTTL`（在 `RMapCache.putAll`）、`replace(K, old, new)`（TS 叫 `replaceWithCheck`，须改名/重载）、`remove` 联合返回须收窄。

**`RMapCache`**：`expireEntry`（单条目，拒已过期）、`remainTimeToLive`（`-2/-1/ms`）、`putAll`（TTL 批量）、事件订阅双向 ✅ (1.4.0)；仍缺 `expireEntries` 批量、`getWithTTLOnly`（TS `get` 必刷新，无等价）、`compute*`、`RLeasedMap` 全族、`getMaxSize`。

**`RMapCacheNative`**（现有 4 个 vs ~38）：`putIfAbsent(key, value, ttl)`、`expireEntry(IfNotSet/IfGreater/IfLess)` + `clearExpire` + `remainTimeToLive`（`-2` 缺失）✅ (1.4.0)；仍缺条件写全族其余变体、`Instant` 绝对过期、`PutArgs` 批量、`compute*`。注：Java 4.6.1 对 HPERSIST `-2` 回 `null`，TS 统一 `false`（boolean 契约）。

**`RBlockingQueue/Deque`**：`pollFromAny`/`pollFromAnyWithName`（BLPOP）、`pollFirstFromAny`/`pollLastFromAny`（BLMPOP + COUNT）、`pollLastAndOfferFirstTo`/`takeLastAndOfferFirstTo`（BLMOVE）、deque `move`/`moveWithTimeout`（LMOVE/BLMOVE）✅ (2.1.0)；亚秒超时/`timeoutMs<=0` 语义、`take` 独立连接、`drainTo` 原子化均已在 v1.2.0/v1.3.0 修正 ✅。仍缺 Java 4.6.1 的 `pollLastFromAnyWithName(Duration, names...)` 与 `subscribeOnElements/unsubscribe` 元素订阅。

**`RLock` 面**：`lockInterruptibly` 全族、`tryLock` 四重载分解（TS 三参合一）、`*Async` 命名、`newCondition`、`RObservable.addListener`；`RWLock`/`FairLock` view 的 `forceUnlock/isLocked/isHeldByThread/getHoldCount` ✅ (1.4.0)，唤醒常量对齐 Java `LockPubSub` ✅；仍缺 `rwlock_timeout` 续期键 + `CommonOptions`；`FencedLock` 的 `tryLockAndGetToken`（非阻塞/等待）✅ (2.1.0，live 与 Java token 序列互通）；仍存语义差异：TS `lockAndGetToken` 是 try-only，Java 默认阻塞（需阻塞时请用 `lockWaitAndGetToken`）；`RNonReentrantLock` 默认放行 vs Java 抛；`MultiLock` 接受 `LockLike`（`RFairLock`/RW 视图可混装）✅ (1.4.0)，仍缺批锁变体、返回类型非 `RLock`；`RedLock` 缺 drift 扣减。

**`RTopic` 系管理面**：`getChannelNames/getPatternNames/getActiveTopics`、`countListeners`（本地监听器数）与 `countSubscribers`（`PUBSUB NUMSUB`；分片 `SHARDNUMSUB` 带回退）✅ (2.5.0)；仍缺 `StatusListener` 回调、`Class<M>` 类型门控、按实例删除；`RReliableTopic.publish` 应回订阅者数（TS `void`）、无过期组清理/XTRIM（流无限增长）、`delete()` 多删了流、每监听器一组 vs Java 每实例一组。

**`RBitSet`**：已补 ✅ (1.5.0)：`BITFIELD` 全族（`BitFieldArgs`/`BitOffset.bit/index`/`overflow`，OVERFLOW FAIL 回 `null`）、类型化读写（signed/unsigned 的 get/set/increment + `getByte/Short/Integer/Long` 快捷）、`or/and/xor/not` 回字节长、Redis 8 `BITOP DIFF/DIFF1/ANDOR/ONE`、`size()`（STRLEN×8）与 `length()`（最高置位 + 1，4.6.1 bitpos Lua 等价）区分、`setRange`/`clear(from,to)` 为 `to` 排他；仍缺 `get(long...)`/`set(long[], boolean)` 批读批写、`set(BitSet)`/`toBitSet()`。

**`RGeo`**：已补 ✅ (1.5.0)：`search(GeoSearchArgs)` 全族（from lon-lat/member、radius/box、order、count/countAny）、`searchWithDistance/Position`、`storeSearchTo/storeSortedSearchTo`、`addIfExists`（单成员 Lua geopos→geoadd；变参 GEOADD XX 只计新增故恒 0）；命令选择与 4.6.1 jar 逐条对齐（半径→`GEORADIUS_RO`/`GEORADIUSBYMEMBER_RO`、Box→`GEOSEARCH`、存储→`GEORADIUS ... STOREDIST`/`GEOSEARCHSTORE`；4.6.1 无 polygon，新版才有）；未继承 `RScoredSortedSet`。

**`RTimeSeries`**：`addAll`、`getEntry/getValue/getAndRemove(Entry)`、`first/last`（值/Entry/Timestamp/count 变体）、`pollFirst/Last`（值/Entry/count）、`range/rangeReversed/entryRange/entryRangeReversed`（limit）、`remove/removeRange` ✅ (2.4.0，读侧按 TTL ZSET 惰性过滤；label 形状兼容 n=2/3/4)；仍缺 `stream()/iterator(int)` 视图与 `addListener`。

**`RKeys`**：`expire` 单/多键、`expireAt` 单/多键、`clearExpire`、`remainTimeToLive`、`getSlot`（客户端 CRC16-XMODEM，hash-tag 感知；Java 走 `CLUSTER KEYSLOT`）、`move`、`unlinkByPattern` ✅ (2.2.0)；`copy` 已有；仍缺 `migrate`/`MigrateArgs`、`getKeysStream*`、`time`、`swapdb`、`flushdbParallel/flushallParallel`、`addListener/removeListener`。

**`RScript`**：丢 `mode`（集群读写路由）、缺 `STRING/MAPVALUELIST` 精确 cast、`scriptKill/scriptFlush`。

**`RFunction`**：缺 `libraryName`（跨语言库名分叉）、`dump/restore/kill/list/stats`。

**`RBuckets`**：`setIfAllKeysExist/setIfAllKeysAbsent` ✅ (2.2.0，`MSETEX … NX|XX [PX ms]`，Redis 8+；无该命令时显式报错而非降级)。

**`RJsonBucket`**：`JSON.SET/GET/DEL` 子集 vs ~40 方法（path 系列、`array*`、`compareAndSet` 全缺）。

**`RVectorSet`**：缺 attrs/neighbors/random/range/iterator（`add(VectorAddArgs)` 高级版缺）。

**概率三件套**：`RTDigest`（缺 merge/min/max/rank/info 等 10+）、`RCuckooFilter`（缺 init 参数/`addIfAbsent`/批量）、`RTopK`（`add` 丢 dropped 项）。

**`RAtomic*`**：`getAndIncrement/getAndDecrement`、`getAndDelete`、`setIfLess/setIfGreater`、Double `compareAndDelete` ✅ (2.2.0)；仍缺 `IncrementArgs`（增量配置）与 `addListener`；`getAndAdd` 浮点回算与超 `2^53` 的 `compareAndSet` 已在 v1.3.0 修复 ✅（Long 字符串 CAS / BigInt，Double 单 Lua 旧值）。

**`RLocalCachedMap`**：`timeToLiveMs/maxIdleMs` 已在近端缓存兑现 ✅ (1.1.0)；`maxSize` 下 LRU/LFU 淘汰与 `getCachedMap()` 本地快照 ✅ (2.1.0)；仍缺 `preloadCache(count)`、重连策略、Caffeine、`loader/writer`、过期事件策略。

**`RRateLimiter`**：`keepAliveTime` 续期 ✅ (2.1.0，acquire 脚本内 `PEXPIRE` config/value/permits；keepAlive=0 时 value/permits 继承 config HASH 的 TTL，与 Java 4.6.1 一致)。

**`RMultimapCache`**：可选周期清扫 ✅ (2.1.0，`enablePeriodicEviction(intervalMs)` / `evictExpired()` / `disablePeriodicEviction()`；默认仍为 `checkExpiry` 惰性)。

**`RDelayedQueue`**：`size()` 已对齐 Java（staging LLEN）✅、`delete()` 不再误删目标队列 ✅ (1.1.0)；**后台迁移调度器 ✅ (2.0.0)**：`start()/stop()/destroy()`（Java `QueueTransferTask`/`QueueTransferService` 移植——channel 订阅走 `duplicate()` 连接、deadline 定时器 >10ms 定时否则立即、5s 错误重试、迁移 Lua 返回下一表头 deadline 重臂；`destroy()` = 摘除任务不删数据），单机 + 集群实测；仍缺 `readAll/poll(limit)/move/addListener`、负延迟校验。

**`RRingBuffer`**：缺 `readNewest/readOldest/peekLast/addAll`；`delete()` override 已补 ✅ (1.1.0，容量 key 与 LIST 一并清理；`clear()` 保持 Java 只删 LIST)。

**`RBatch`**：类型覆盖已补 13 类 ✅ (2.1.0：原 7 类 + `BatchDeque/AtomicDouble/BitSet/Geo/Stream/MapCacheNative`，写排队、读在 `execute()` 后 resolve，对齐 RFuture；`BatchAtomicDouble.getAndAdd` 单 Lua 精确旧值、native map-cache `HPEXPIRE FIELDS` 字段 TTL；Lua 版 ops 走 live 测，mock pipeline 无 `eval`)；仍存读回占位非即时值；pub/sub 不可进 batch（`duplicate()` 真实连接）。

## §3 P2（统一三刀，见报告正文）

① 时间全收敛 `ms:number`（保持）；② 不存在一律 `null`（`rank/revRank/getScore` 的 `-1` 是唯三例外，文档置顶）；③ 联合返回拆分。另 `getConfig` 多 `keepAliveMs`、`release` 回 number、`containsAll` 回 number、`RList.add` 回长度等，一律 COMPATIBILITY 标“名同义异”。

## §4 明确不做（保持）

`RSortedSet-Comparator`（无跨语言表示）、`RTransaction`（快照回滚≠MULTI）、`LiveObject`、`reactive/Rx`、`Executor/Remote` 真协议、`RTransferQueue` 握手（同名不同物）、PRO 四件套。另建议记名（4.6.1 无，本属版本外）：`RCountMin`、`RTimeSeriesNative`、`RReliablePubSubTopic`、`RLocalCachedMapCache`。

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
- **后续纵深研究与分阶段演进路线图**：详见根目录 [演进方案.md](../演进方案.md)。
- 不做：§4。
