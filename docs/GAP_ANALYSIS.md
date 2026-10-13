# redi.ts vs Redisson 差距分析（Gap Analysis）

> 日期：2026-10-13。基线：`redisson-4.6.1.jar`（jar 实测为准）+ 当前主干源码。
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

**`RScoredSortedSet`**：交并差已补 ✅ (1.8.4：`union/unionWith/readUnion/readUnionEntries/intersection/intersectionWith/readIntersection/readIntersectionEntries/countIntersection/countIntersectionLimit/diff/readDiff/readDiffEntries`，store 版“只算 others 存入 self”与 Java 逐字对齐，live 验证 WEIGHTS 位置）；仍缺 `entryRange/valueRange` 反向/分页全族、`addIfExists/Less/Greater`、`addScore` 系列、`first/lastScore`、`random`、`pollFirst/Last` 阻塞版及 `FromAny`（`BZMPOP`）、`ZRANGESTORE`、迭代器；`rank` 回 `-1` 而 Java 回 `null`（移植判空必改）；`rangeByScore` 调已废弃 `ZRANGEBYSCORE`。

**`RStream`**：缺组运维（`removeGroup/createConsumer/removeConsumer/updateGroupMessageId`）、pending 全套（`claim/autoClaim/fastClaim/pendingRange/listPending/getPendingInfo`）、`readGroup` 完整版（`NOACK/claim`）、`range/rangeReversed` 精确版、`add` 完整版（`MINID`/`=`严格/`LIMIT`/`NOMKSTREAM`/幂等）、`ack(StreamAckArgs)` + `nack`、`getInfo/listGroups/listConsumers`、`addListener`。

**`RMap`**：`put`/单键 `remove`/`putIfAbsent` 已改为单 Lua 原子旧值返回 ✅ (1.8.3)；缺 `keySet/values/entrySet` 惰性批视图（只有全量快照，大 key OOM）、`getLockByKey` 六件套、`putAllWithTTL`（在 `RMapCache.putAll`）、`randomKeys/randomEntries`、`valueSize`、`replace(K,old,new)`（TS 叫 `replaceWithCheck`，须改名）、`remove` 联合返回须收窄。

**`RMapCache`**：缺 `expireEntry/expireEntries` 族、`getWithTTLOnly`（TS `get` 必刷新，无等价）、`remainTimeToLive`、`compute*`、`RLeasedMap` 全族、`putAll`（TTL 批量）、`getMaxSize`、事件订阅（只发不收）。

**`RMapCacheNative`**（现有 4 个 vs ~38）：缺条件写全族、`Instant` 绝对过期、`expireEntry*` 八件套、`PutArgs` 批量、`compute*`；`remainTTL` 把“不支持”与“无过期”都回 `-1`。

**`RBlockingQueue/Deque`**：`pollFromAny` 全族（`BLMPOP`）、`pollLastAndOfferFirstTo`（`BLMOVE`）全缺；亚秒超时被 `ceil(ms/1000)` 放大到整秒，且 `timeoutMs<=0` 反转为无限阻塞；`take` 占整条连接（须 `duplicate()`）；`drainTo` 非原子（并发尾部追加可丢）。

**`RLock` 面**：`lockInterruptibly` 全族、`tryLock` 四重载分解（TS 三参合一）、`*Async` 命名、`newCondition`、`RObservable.addListener`；`RWLock`/`FairLock` view 缺 `forceUnlock/isLocked/getHoldCount` 等 + `rwlock_timeout` 续期键 + `CommonOptions`；`FencedLock` 缺 `tryLockAndGetToken` 四重载（且 TS `lockAndGetToken` 是 try-only，Java 默认阻塞——默认语义反转）；`RNonReentrantLock` 默认放行 vs Java 抛；`MultiLock` 缺批锁变体、返回类型非 `RLock`（不可嵌套，`RFairLock` 非 `RLock` 子类故不能混装）；`RedLock` 缺 drift 扣减。

**`RTopic` 系管理面**：`getChannelNames/getPatternNames/getActiveTopics`、`StatusListener`、`countListeners`（vs `PUBSUB NUMSUB` 语义不同）、按实例删除、`Class<M>` 类型门控；`RShardedTopic.addListener` 无 subs 管理恒返 1；`RReliableTopic.publish` 应回订阅者数（TS `void`）、无过期组清理/XTRIM（流无限增长）、`delete()` 多删了流、每监听器一组 vs Java 每实例一组。

**`RBitSet`**：`BITFIELD` 全族 + 类型化读写全缺；`or/and/xor/not` 应回字节长；缺 Redis 8 `BITOP DIFF/DIFF1/ANDOR/ONE`；`size` vs `cardinality` 未区分。

**`RGeo`**：`search(GeoSearchArgs)`（from/member/radius/box/polygon/order/count）、`searchWithDistance/Position`、`storeSearchTo/storeSortedSearchTo`、`addIfExists` 全缺；未继承 `RScoredSortedSet`。

**`RTimeSeries`**：`add(TimeSeriesAddArgs)`（label+TTL 全参）、`addAll/addIfAbsent/addOrReplace`、`first/last/entryRange/rangeReversed` 全族、label 过滤读、聚合、`info` 全缺。

**`RKeys`**：`move/migrate/copy`(跨节点)/`expire`(多key)/`clearExpire/remainTimeToLive/getSlot/getKeysStream/unlinkByPattern/time/swapdb/并行flush/监听` 缺。

**`RScript`**：丢 `mode`（集群读写路由）、缺 `STRING/MAPVALUELIST` 精确 cast、`scriptKill/scriptFlush`。

**`RFunction`**：缺 `libraryName`（跨语言库名分叉）、`dump/restore/kill/list/stats`。

**`RBuckets`**：缺 `setIfAllKeysExist/setIfAllKeysAbsent`（带 TTL 条件批量）。

**`RJsonBucket`**：`JSON.SET/GET/DEL` 子集 vs ~40 方法（path 系列、`array*`、`compareAndSet` 全缺）。

**`RVectorSet`**：缺 attrs/neighbors/random/range/iterator（`add(VectorAddArgs)` 高级版缺）。

**概率三件套**：`RTDigest`（缺 merge/min/max/rank/info 等 10+）、`RCuckooFilter`（缺 init 参数/`addIfAbsent`/批量）、`RTopK`（`add` 丢 dropped 项）。

**`RAtomic*`**：Double 缺 `increment/decrement/getAndDelete/compareAndDelete`；两者缺 `setIfLess/setIfGreater/IncrementArgs/addListener`；`getAndAdd` 浮点回算有 `0.1+0.2` 类误差；超 `2^53` 的 `compareAndSet` 失真。

**`RLocalCachedMap`**：`timeToLiveMs/maxIdleMs` 声明了但实际硬编码 60s（未使用）；缺 `preloadCache(count)`、本地缓存视图、重连策略、Caffeine、`loader/writer`、过期事件策略。

**`RDelayedQueue`**：缺 `destroy/readAll/poll(limit)/move/addListener`；`size()` 与 Java 含义相反（TS=已到期未消费，Java=staging 未到期，混部断言必岔）；`delete()` 多删目标队列；负延迟不校验。

**`RRingBuffer`**：缺 `readNewest/readOldest/peekLast/addAll`；无 `delete()` override 致容量 key 泄漏。

**`RBatch`**：类型覆盖为 0（Java ~35 个 `getXxxAsync`），读回占位非即时值。

## §3 P2（统一三刀，见报告正文）

① 时间全收敛 `ms:number`（保持）；② 不存在一律 `null`（`rank/revRank/getScore` 的 `-1` 是唯三例外，文档置顶）；③ 联合返回拆分。另 `getConfig` 多 `keepAliveMs`、`release` 回 number、`containsAll` 回 number、`RList.add` 回长度等，一律 COMPATIBILITY 标“名同义异”。

## §4 明确不做（保持）

`RSortedSet-Comparator`（无跨语言表示）、`RTransaction`（快照回滚≠MULTI）、`LiveObject`、`reactive/Rx`、`Executor/Remote` 真协议、`RTransferQueue` 握手（同名不同物）、PRO 四件套。另建议记名（4.6.1 无，本属版本外）：`RCountMin`、`RTimeSeriesNative`、`RReliablePubSubTopic`、`RLocalCachedMapCache`。

## §5 路线图

- P0 剩余：RateLimiter 配置重读（`getConfig` 已是 fresh-read；acquire 路径沿用实例缓存，与 Java 实例字段行为大体一致，待决策是否加 refresh）。
- P1 按文件：stream claim/pending/info；`RMap.put` 原子 Lua ✅ (1.8.3)；zset 交并差 ✅ (1.8.4)；`RBatch` 决策（补类型或文档锁死）。
- 不做：§4。
