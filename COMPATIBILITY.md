# COMPATIBILITY — redi.ts ↔ Redisson / redi.py / redi.go

> Normative: Java Redisson sources (`C:/GitHub/redisson`, 4.6.x) + `JsonJacksonCodec`.
> Direct port baseline: `redi.go` Lua scripts (verified against Redisson 4.6.1);
> `redi.py` used for codec/hash cross-checks (note: it writes bare JSON objects,
> which Java default typing rejects — redi.ts follows redi.go and wraps them).

## Wire facts (verified)

1. **JsonJacksonCodec default typing**: bare `{...}`/`[...]` rejected by Java.
   Encode wraps maps (`@class: java.util.LinkedHashMap`) and arrays
   (`["java.util.ArrayList",[...]]`), longs (`["java.lang.Long",v]`); decode
   strips all wrappers and any `@class`. `bigint` encodes losslessly;
   `JsonCodec({bigIntForUnsafeLong:true})` decodes unsafe integer literals as
   `BigInt` (default keeps `number`). `test/wire.test.ts` + live
   `test/interop.test.ts` (python) guard this.
2. **HighwayHash-128**: BigInt port of `redipy/highway_hash.py` (= Redisson
   `org.redisson.misc.HighwayHash` fixed KEY). Vectors verified live against
   python: `hello`, empty, 32/33-byte, `test-key-123`, 50-byte — all exact.
   Bloom bit derivation (`h1 & 2^63-1`, alternate `+h2/+h1`) and multimap
   internal id (`BE pack + base64 RawStd, no padding`) verified live.
3. **Companion keys** always hash-tagged: `prefixName` → `prefix:{name}`,
   `suffixName` → `{name}:suffix` (cluster same-slot).
4. **Binary members** (`MapCache <dQ`, `RateLimiter Bc0I`, `DelayedQueue Bc0Lc0`,
   `TimeSeries BBc0Lc0Lc0`) are little-endian `struct.pack` parity via
   `Buffer.writeDoubleLE/writeBigUInt64LE/writeUInt32LE`, transported as
   `latin1` (never UTF-8).
5. **Permit ids** are fresh `randomBytes(16)` per acquire (ZADD dedup collapse
   otherwise); server time comes from `TIME` (no local skew).
6. **Blocking commands never share the main connection**: `BLPOP`/`BRPOP`/
   `BZPOPMIN`/`XREADGROUP BLOCK` run on `duplicate()` connections, otherwise
   the TCP connection cannot carry lock-watchdog renewals. Timeouts are Redis
   6+ float seconds; `<= 0` means non-blocking.

## Status matrix

| Structure | Redis layout | Status |
|-----------|--------------|--------|
| RLock / Spin / NonReentrant | HASH `{name}` field=`uuid:thread`, channel `redisson_lock__channel:{name}`; Lua acquire/renew/unlock/force verbatim; Spin 默认指数退避 1→128ms×2+jitter（`BackOff` 可配，`getSpinLock(name, backOff?)`）；`lockInterruptibly(lease?, signal?, threadId?)` 以 AbortSignal 模拟线程中断（等待中 abort → `AbortError`，不获取锁）；unlock-latch 为按请求幂等键，跨语言无影响 | WIRE_OK + live mutual exclusion both directions ✅ + live phase 28 (contended abort ✅) |
| RFairLock / NonReentrantFair | + LIST `redisson_lock_queue:{name}` + ZSET `redisson_lock_timeout:{name}`, per-waiter `channel:field`, FIFO Lua verbatim; `delete()/unlink()` clears all three keys + local watchdog timers | WIRE_OK + live phase 16 (capacity/keys re-init ✅) |
| RReadWriteLock | single HASH + `mode`, `redisson_rwlock:{name}`; unlock/forceUnlock wake constants follow Java `LockPubSub` (read → `UNLOCK_MESSAGE` 0 wake-one, write → `READ_UNLOCK_MESSAGE` 1 wake-all), downgrade parity; read locks create per-acquisition `<tagged>:<field>:rwlock_timeout:<n>` keys (SET 1 + PX), delete them on unlock and recompute the HASH TTL from the remaining keys' max PTTL; watchdog renews every timeout key + HASH; views: `isLocked` (read: `mode=='read' or (mode=='write' and hlen>2)`), `isHeldByThread`, `getHoldCount`, `forceUnlock` | WIRE_OK + live phase 3 (互斥双向+共享读） + live phase 19 (视图/强解 ✅) + live phase 29 (Java 读锁与 TS 读锁混部存活 ✅) |
| RFencedLock | RLock HASH + `redisson_lock_token:{name}` (`INCR` incl. re-entry, returns `{-1,token}`); `tryLockAndGetToken(waitTimeMs?, leaseTimeMs?, threadId?)` follows 4.6.1 (omitted wait = non-blocking, otherwise waits); `delete()` clears token so the next acquire restarts at 1 | WIRE_OK + live phase 3 (token 跨语言单调） + live phase 16 (token reset ✅) + live phase 22 (TS/Java token 序列 n→n+1→n+2 ✅) |
| RMultiLock / RRedLock | client-side orchestration (quorum `n/2+1`, lease amplification); accepts any `LockLike` (`RLock`/`RFairLock`/RW views) | WIRE_OK + live phase 9（全有或全无+回滚，多数派 2/3 ✅） |
| RSemaphore | STRING + `redisson_sc:{name}` Lua | WIRE_OK + live shared-permit ✅ |
| RCountDownLatch | STRING + `redisson_countdownlatch__channel__{name}` (`1` set / `0` zero) | WIRE_OK (count shared, live read ✅) |
| RPermitExpirableSemaphore | STRING + ZSET `{name}:timeout` + `redisson_sc:{name}`, lazy purge; permit ids are hex text (Java-UUID domain), `tryAcquire` returns `string[]`; `delete()` removes the timeout ZSET | WIRE_OK + live phase 3/8 (共享许可池/多许可 ✅） + live phase 16 (zero residue ✅) |
| RMap / RList / RQueue / RDeque / Blocking(+Bounded) | bare name + JSON values (map keys JSON); Bounded uses `redisson_bqs:{name}` as available permits: atomic DECRBY+RPUSH / INCRBY+PUBLISH, `put`/timed `offer` block, `remainingCapacity()`; BLPOP/BRPOP use dedicated connections + float seconds (0 = non-blocking); `drainTo` extracts with one atomic Lua; blocking queue/deque also expose `pollFromAny`/`pollFromAnyWithName` (BLPOP), `pollFirstFromAny`/`pollLastFromAny` (BLMPOP + COUNT), `pollLastFromAnyWithName` (BRPOP + source name), `pollLastAndOfferFirstTo`/`takeLastAndOfferFirstTo` (BLMOVE), deque `move`/`moveWithTimeout` (LMOVE/BLMOVE); base RQueue/RDeque add `poll(count)`, `indexOf`, non-blocking `pollLastAndOfferFirstTo` (RPOPLPUSH), variadic `addFirst/addLast`, `addFirstIfExists`/`addLastIfExists` (LPUSHX/RPUSHX), `pollFirst/pollLast(count)`, `move` (LMOVE); blocking queues/deques add `subscribeOnElements`/`unsubscribe` (continuous take, async back-pressure, BLPOP connection abort on cancel; bounded goes through capacity-aware poll); RMap 另有 `valueSize`（HSTRLEN）、`randomKeys`/`randomEntries`（HRANDFIELD [WITHVALUES]）、`keysIterator`/`valuesIterator`/`entriesIterator(pattern, count)` 惰性 HSCAN 异步迭代器、物化视图 `keySet/values/entrySet(pattern?, count?)`、`readAllEntrySet`、三参 `replace(key, old, new)` | WIRE_OK + live bidirectional ✅ + live phase 4/7/16/17/18 + live phase 20 ✅ + live phase 22 (真实 BLPOP/BLMPOP/BLMOVE 唤醒 ✅) + live phase 27 (视图 vs Java HSCAN / 条件 replace/remove ✅) + live phase 41（RPOPLPUSH/变参/LPUSHX/LMOVE ✅） + live phase 45（元素订阅/即时取消/有界容量 ✅） + live phase 48（BRPOP+源名/限量扫描 ✅） |
| RSet | set ops: store variants write into SELF like Java (self IS an input; old read-only names now `readUnion/readDiff/readIntersection`), explicit numkeys (`as any`: typings can't express it), `countUnion` via `.call`; `countIntersection` with optional LIMIT (SINTERCARD, SMEMBERS fallback); `addListener`/`removeListener` for sadd/srem/spop/sinterstore/sunionstore/sdiffstore; `addAllCounted`/`removeAllCounted`/`containsEach`; lazy `iterator(pattern?, count?)`/`distributedIterator`/`stream` (SSCAN); key synchronizers shared with RMap (see below) | WIRE_OK + live bidirectional ✅ + live phase 15 (union ✅; inter/diff gated — local msys2 SINTERSTORE/SDIFFSTORE broken, raw-RESP verified) + live phase 38 (SSCAN ✅) + live phase 52（SINTERCARD/监听 ✅） |
| Key synchronizers (RMap/RSet/MapCache/SetCache/Multimap) | `getLock`/`getFairLock`/`getReadWriteLock`/`getSemaphore`/`getCountDownLatch`/`getPermitExpirableSemaphore`: companion = `suffixName(name, hash128toBase64(encode(key)) + ":" + {lock\|fairlock\|rw_lock\|semaphore\|countdownlatch\|permitexpirablesemaphore})`; owners inherit the client id/watchdog | WIRE_OK + live phase 19（6/6 命名与 Java 4.6.1 逐字节一致、Key 锁双向互斥 ✅） |
| RScoredSortedSet | ZSET + JSON members; union/intersection/diff store+read+count (store 版只算 others 存入 self，WEIGHTS/AGGREGATE/LIMIT 位置对齐）；分数族：`firstScore`/`lastScore`、`firstEntry`/`lastEntry`、`addScore`/`addScoreAndGetRank`/`addScoreAndGetRevRank`/`addAndGetRank`/`addAndGetRevRank`、`count`、`entryRange`（索引+分数）、`valueRangeReversed`/`entryRangeReversed`（分数 4/6 参与索引 2 参；Java 语义 `startScore`=下界，命令 `ZRANGE max min BYSCORE REV`）、`removeRangeByRank`/`removeRangeByScore`；`addIfAbsent`(NX)/`addIfExists`(XX CH)/`addIfLess`(LT CH)/`addIfGreater`(GT CH) 与 `addAll*` 条件批量；`random`/`randomEntries`（ZRANDMEMBER [WITHSCORES]）；非阻塞 `pollFirst/LastEntry`（ZPOPMIN/MAX）与阻塞 `pollFirst/LastWithTimeout`、`pollFirst/LastFromAny`（BZPOPMIN/MAX、BZMPOP，独立连接+浮点秒）；惰性 `iterator`/`stream`/`entriesIterator`（ZSCAN）；`rank`/`revRank` 回 `number \| null`（Java 语义） | WIRE_OK + live bidirectional ✅ + live phase 11 ✅ + live phase 20 ✅ + live phase 23 (GT/LT/XX 分数 Java 读回 ✅) + live phase 24 (addAll/random/BZPOP/BZMPOP ✅) + live phase 38 (ZSCAN ✅) |
| RLexSortedSet | ZSET score 0, **raw** members (no codec) | WIRE_OK + live bidirectional ✅ |
| RBucket / RBuckets / RBinaryStream | STRING (+ms TTL, CAS Lua); RBuckets conditional multi-set uses Java's `MSETEX numkeys k v… NX\|XX [PX ms]` (Redis 8+; without MSETEX it fails loudly), values codec-encoded | WIRE_OK + live bidirectional ✅ + live phase 23 (MSETEX NX/XX + Java read ✅) |
| RAtomicLong / RAtomicDouble | decimal strings, `INCRBY(FLOAT)`; Long CAS compares exact decimal strings (no Lua double), `LongValue = number/bigint` accepted, unsafe results return BigInt, `getAndAdd` single Lua; Double `getAndAdd` returns the stored old value; `getAndIncrement`/`getAndDecrement`/`getAndDelete` and `setIfLess`/`setIfGreater` (single Lua from redi.go) on both; Double `compareAndDelete` | WIRE_OK + live bidirectional ✅ + live phase 18 (Long.MAX CAS ✅) + live phase 23 (scalar remainder ✅) |
| RLongAdder / RDoubleAdder | `{name}:adder-topic` + `{name}:<id>:counter` + `{name}:<id>:semaphore` hash-tag 三元组；sum 只返回 GETDEL（请求者自 flush 已包含在内）；订阅先挂 handler 后 await ack | WIRE_OK + live phase 3 |
| RIdGenerator | counter + `{name}:allocation` interval cache | WIRE_OK |
| RMapCache | HASH packed `<dQ` + `redisson__timeout__set:{name}`/`redisson__idle__set:{name}`/`last_access`/`redisson_options` + `redisson_map_cache_*:{name}` events（明令 +Buffer 传输：Lua 传参无法承载二进制，容量淘汰为尽力而为；`updated` 为 `[key,new,old]` 三段）; `putAll(mapping,ttl,maxIdle)` per-key、`expireEntry`（拒已过期条目、重写 maxIdle 打包）、`expireEntries` 批量、`remainTimeToLive`（`-2` 缺失/已过期、`-1` 无过期）、`getWithTTLOnly`（不刷新 idle/access）、`getMaxSize`/`setMaxSize`、`compute`/`computeIfAbsent`（per-key 锁 + TTL）; `addListener`/`removeListener` 走独立连接 + `messageBuffer`（u64LE 长度前缀字节保真），写 `has-listeners=1` 使 Java 写端也发布，事件负载以 Buffer 发送; `unlink()` clears all companions | WIRE_OK + live bidirectional ✅ + live phase 6（四类事件进 Java 监听 ✅） + live phase 19（双向事件 ✅、过期语义 ✅） + live phase 28（批量 TTL/only-read/max-size ✅） + live phase 30（compute + Java mc-get ✅） |
| RMapCacheNative | plain HASH + `HPEXPIRE`/`HPEXPIREAT` + `HPERSIST`/`HPTTL` `FIELDS` (≥7.4); `put`/`putIfAbsent`/`fastPut`/`fastPutIfAbsent`/`putIfExist`/`fastPutIfExist`（`fastPutIfExist` 为 TS 扩展；4.6.1 jar 仅到 `fastPutIfAbsent`） 接受毫秒或 **绝对 `Date`**（HPEXPIRE/HPEXPIREAT）、`putIfAllKeysExist`/`putIfAllKeysAbsent`（全字段条件 Lua，含逐字段 TTL；结构化 PutArgs 走 HSETEX FXX/FNX）、`putAll`（HSETEX KEEPTTL/PX/PXAT，回退逐字段）、`compute`/`computeIfAbsent`（per-key 分布式锁 + 字段 TTL）、`expireEntry(IfNotSet/IfGreater/IfLess)`、`expireEntries(IfNotSet/IfGreater/IfLess)` 批量、`clearExpire`（HPERSIST）、`remainTimeToLive`（HPTTL，`-2` 缺失）；4.6.1 Java 对 HPERSIST `-2` 返回 `null`，TS 统一 `false` | NATIVE_OK + live phase 6（TTL 跨语言过期 ✅） + live phase 19（条件过期/HPERSIST/putIfAbsent 双向 ✅） + live phase 33（Date/HPEXPIREAT ✅） + live phase 40（条件批量/批量过期 + Java 读 ✅） + live phase 42（fastPut 家族 + 字段 TTL/Java 读 ✅） + live phase 51（HSETEX 结构化批量/compute ✅） |
| RSetCache | ZSET(abs expiry/`MaxInt64`) + `redisson__idle__set:{name}` durations + `{name}:redisson_touch` last-touch (TS extension — Redisson has TTL-only SetCache; touch key is Java-invisible, presence stays cross-readable); `delete()/clear()` removes idle + touch | WIRE_OK + live bidirectional ✅ + live phase 5 (idle expiry ✅) + live phase 16 (zero residue ✅) |
| RMultimap (Set/List) | HASH field→id + `{name}:{id}` collections, Highway internal ids; `delete()/clear()` reads the HASH first and removes every child collection | WIRE_OK + live phase 16 (Java reads empty after delete ✅) |
| RSet/ListMultimapCache (+Native) | + per-key TTL ZSET / `HPEXPIRE`; `delete()/clear()`/`unlink()` also removes the TTL ZSET and (Native) child collections; opt-in `enablePeriodicEviction(intervalMs)` / `evictExpired()` sweeps child + HASH field + TTL member (default lazy) | WIRE_OK / NATIVE_OK + live phase 16 (zero residue ✅) + live phase 22 (周期清扫 ✅) |
| RLocalCachedMap | data = RMap wire (live phase 9 Java 直读/直写 ✅); invalidation = local JSON broadcast, UPDATE 带值 / INVALIDATE 语义均有对照测试; near-cache honours `timeToLiveMs`/`maxIdleMs`/`maxSize` + `evictionPolicy` LRU/LFU (entries track expiry, last access and hit count; local reads await subscription ack); `getCachedMap()` snapshot; bounded `preloadCache(count)` (HSCAN) ✅ (2.16.0); Java binary protocol not ported — disable near-cache for mixed deploys; `loader/writer` deliberately not ported (external-store semantics, no wire impact) | PARTIAL + live phase 16 (TTL/idle ✅) + mock phase 22 (LRU/LFU + view ✅) + live phase 38 (preload count ✅) |
| RTopic / RPatternTopic / RShardedTopic | `PUBLISH`/`PSUBSCRIBE`/`SSUBSCRIBE` + JSON（分片投递走 `smessage` 事件）; handlers attached before subscribe; Sharded saves conn and supports removeListener/removeAllListeners; management: `getChannelNames`, `countListeners` (local), `countSubscribers` (`PUBSUB NUMSUB`; sharded `SHARDNUMSUB` with fallbacks), Pattern `getPatternNames`/`getActiveTopics` (`PUBSUB CHANNELS`), `StatusListener` (onSubscribe/onUnsubscribe; removeListener handles both listener kinds), `removeListener(handler)` by callback identity | WIRE_OK / NATIVE_OK + live phase 4/5/17（Topic/Sharded/Pattern 双向 + lifecycle ✅） + live phase 26（counts/active topics ✅） + live phase 30（status callbacks ✅） + live phase 32（handler removal ✅） |
| RReliableTopic | STREAM field **`m`** + one subscriber group per client instance (all listeners share it; created at `0`, destroyed when the last listener leaves) + `{name}:timeout` heartbeat ZSET (120s; expired groups destroyed, stream trimmed to the earliest group's last-delivered id, Java 4.6.1 maintenance Lua port); `publish` returns the group count (XADD + XINFO GROUPS); XREADGROUP BLOCK on a dedicated connection; `unlink()` clears stream+timeout | WIRE_OK + live phase 3/17 (双向投递） + live phase 39 (单实例单组/publish 计数/自动 trim ✅) |
| RStream | STREAM, field names **and** values JSON-encoded; group mgmt + pending/claim/autoClaim + info/list + add/read opts + nack/ackdel; `fastClaim`/`fastAutoClaim` (XCLAIM/XAUTOCLAIM JUSTID → ids), `readMany`/`readGroupMany` (one XREAD/XREADGROUP over several streams; per-stream groups supported) and `addListener(ObjectListener)` via `__keyevent@*:xadd|xdel|xgroup-*` (requires `notify-keyspace-events` K+E); blocking `readGroupWith({blockMs})` uses a dedicated connection | WIRE_OK + live bidirectional ✅ + live phase 13/14/17 ✅ + live phase 26（JUSTID/多流读 ✅） + live phase 31（keyspace 六事件/多流 group 读 ✅; keyspace 未配置时无事件） + live phase 32（每流独立 group ✅） |
| RDelayedQueue | ZSET `redisson_delay_queue_timeout:{name}` + LIST `redisson_delay_queue:{name}` (`Bc0Lc0`) + channel（offer id 走 Buffer 保真；`size()` = staging LLEN，与 Java 一致；`targetSize()` = 目标队列；`readAll()`/`poll(count)` 目标队列；`delete()` 只删 staging+timeout，保留目标队列；消费侧迁移由 DelayedQueue 包装类调度：`start()` 起 Java `QueueTransferTask` 同款后台迁移（channel 订阅走 `duplicate()` 连接 + deadline 定时器 + 5s 错误重试 + 迁移 Lua 返回下一表头 deadline），`stop()` 释放、`destroy()` = 摘除任务不删数据；未 `start()` 时 `poll()`/`peek()` 同步补迁移） | WIRE_OK + live phase 3 (双向迁移） + live phase 16 (size/delete ✅) + live phase 21 (后台迁移端到端 ✅) + live cluster (集群下迁移器 ✅) + mock phase 33（readAll/poll(count)） |
| RRingBuffer | LIST + `redisson_rb:{name}` capacity; `delete()` clears LIST+capacity, `clear()` clears LIST only (Java 4.6.1 parity) | WIRE_OK + live phase 16 (capacity re-init ✅) |
| RGeo | GEOADD, members JSON-encoded; `GeoSearchArgs` 全族（`from(lon,lat)`/`from(member)`、`radius`/`box`、`order`、`count`/`countAny`）、`search`/`searchWithDistance`/`searchWithPosition`、`storeSearchTo`/`storeSortedSearchTo`、`addIfExists`（单成员 Lua geopos→geoadd；变参 GEOADD XX 只计新增故恒 0）。命令选择对齐 4.6.1 jar：半径→`GEORADIUS_RO`/`GEORADIUSBYMEMBER_RO`，Box→`GEOSEARCH`，存储→`GEORADIUS ... STOREDIST`/`GEOSEARCHSTORE` | WIRE_OK + live bidirectional ✅ + live phase 20 ✅ |
| RHyperLogLog | PFADD/PFCOUNT/MERGE, members JSON-encoded | NATIVE_OK + live bidirectional ✅ |
| RBitSet | native MSB-first numbering, no reversal; `BITFIELD` 全族（`BitFieldArgs`/`BitOffset.bit/index`/`overflow`，OVERFLOW FAIL 回 `null`）、类型化 get/set/increment（signed/unsigned + Byte/Short/Integer/Long 快捷）、批量 `get(...bits)`/`set(long[]\|Set, boolean)`/区间 `set(from,to)`/`asBitSet()`、`size()`（STRLEN×8）与 `length()`（最高置位 + 1，4.6.1 bitpos Lua 等价）区分、`or/and/xor/not` 回字节长、Redis 8 `diff`/`diffInverse`/`andOr`/`setExclusive`（`BITOP DIFF/DIFF1/ANDOR/ONE`）、`setRange`/`clear(from,to)` 为 `to` 排他 | WIRE_OK + live phase 20 ✅（BITFIELD/BITOP 双向 + Redis 8.6 语义实测） + mock phase 33（批量/视图） |
| RTimeSeries | ZSET packed `BBc0Lc0Lc0` + `redisson__ts_ttl:{name}` + `redisson__ts_seq:{name}` (4.6.1 首字节 `2/3` 无 label 前缀；读侧按 n==4 新形状标志位、n==2 无 label、其余整段兼容；成员须走 Buffer 传输，UTF-8 会破坏随机 id 字节）；`addAll`（单 Lua、序列号+双 ZSET）、`getEntry`/`getValue`、`first/last` 与 `pollFirst/Last` 全族、`range/rangeReversed` 与 `entryRange/entryRangeReversed`（limit）、`getAndRemove(Entry)`、`remove/removeRange`，读侧按 TTL ZSET 惰性过滤；`stream/iterator` 按页产出值；`addListener(ObjectListener)` via `__keyevent@*:zadd|zrem`（需 `notify-keyspace-events` K+E）；`unlink()` clears ttl/seq | WIRE_OK + live phase 3 + live phase 6（label 双向） + live phase 25（全族/Java 往返 ✅） + live phase 27（stream/iterator ✅） + live phase 31（keyspace add/remove ✅） |
| RBloomFilter | bitmap + `{name}:config` HASH, HighwayHash-128; `unlink()` clears both | WIRE_OK + live bidirectional ✅ |
| BF/CF/TOPK/TDIGEST/GCRA | `BF.*`/`CF.*`/`TOPK.*`/`TDIGEST.*`/`GCRA` native; CF: init/add/remove/addIfAbsent/exists/existsMany (MEXISTS)/count/info; TOPK: init(k,width,depth,decay), add→dropped items, incrementBy, contains/count/list/listWithCount/info; TDIGEST: create(compression)/add/mergeWith/min/max/quantile/cdf/trimmedMean/rank/revRank/byRank/byRevRank/info/reset | NATIVE_OK + live phase 36（TDigest / Cuckoo / TopK ✅） |
| RRateLimiter | HASH config + `{name}:value` + ZSET `{name}:permits` (`Bc0I`, hex-text members) OVERALL/PER_CLIENT; instance caches config after first load (`getConfig` always fresh-reads); successful acquire renews `keepAliveTime` on config/value/permits inside the Lua (and companions inherit a config TTL when keepAlive is 0); `unlink()` clears value/permits | WIRE_OK + live shared-window ✅ + live phase 22 (TS/Java 双向续期 ✅) |
| RKeys / RScript / RFunction / RSearch / RJsonBucket / RVectorSet / RBatch / RMaps / RArray / RCircularBuffer | native commands / pipeline (RBatch: 13 typed facades — bucket/map/list/set/queue/deque/atomicLong/atomicDouble/bitset/geo/stream/mapCacheNative/scoredSortedSet, reads resolve on execute); RKeys: `PEXPIRE`/`PEXPIREAT` single + multi-key, `PERSIST`/`PTTL`, `MOVE`, `MIGRATE` single/MigrateArgs (COPY/REPLACE/AUTH/AUTH2, multiple KEYS), remote `copy` via MIGRATE COPY, `SWAPDB`, `FLUSHDB`/`FLUSHALL ASYNC`, global `addListener/removeListener` (keyevent new/set/expired/del; RESP2 rejects flush-only listeners), `SCAN`+`UNLINK`, `getSlot` computed client-side (CRC16-XMODEM, hash-tag aware; Java delegates to `CLUSTER KEYSLOT`), lazy `getKeysStream(pattern?, count?)`, `getKeysWithLimit([pattern,] limit)` and `getKeysByPattern(pattern, count?)`; RScript: eval/evalSha/load/exists/`scriptKill`/`scriptFlush`; RFunction: load/delete/flush/`kill`/`list`/`stats`/`dump`/`restore(+Replace/AfterFlush)` + FCALL(_RO); RJsonBucket (Redis 8 JSON): document/path set/get/delete, NX/XX, atomic CAS/getAndSet/getAndDelete, string/array/number ops incl. all 4.6.1 `*Multi` match lists and multi-path `get(codec, paths...)` (two-arg `arrayIndex` searches the whole array), merge/keys/type/toggle/clear (plain JSON by default; optional Codec); RJsonBuckets: MSET/MGET + path reads; RSearch (RediSearch): FT.CREATE with raw tokens or structured SearchFieldIndex/SearchIndexOptions (TEXT/TAG/NUMERIC/GEO/GEOSHAPE/VECTOR FLAT|HNSW, ON/PREFIX/FILTER/LANGUAGE/SCORE/NOOFFSETS/STOPWORDS/SKIPINITIALSCAN/TEMPORARY/PAYLOAD_FIELD), FT.ALTER structured/raw, FT._LIST/info, aliases, enhanced FT.SEARCH (sort/return/inKeys/filter/params/dialect/timeout), FT.AGGREGATE (load/groupBy/reduce/apply/sort/limit/rawArgs/WITHCURSOR) + FT.CURSOR READ/DEL, FT.HYBRID (KNN/RANGE, RRF/LINEAR, load/group/apply/filter/params/timeout), FT.PROFILE SEARCH/AGGREGATE (LIMITED), synonyms (SYNUPDATE/SYNDUMP), dictionaries (DICTADD/DICTDEL/DICTDUMP), FT.CONFIG and FT.SPELLCHECK; RVectorSet: VADD (element+vector or VectorAddArgs: REDUCE/CAS/Q8/BIN/EF/SETATTR/M), VSIM by element/vector with COUNT/EPSILON/EF/FILTER/FILTER-EF/TRUTH/NOTHREAD and WITHSCORES/WITHATTRIBS, VCARD/VDIM/VEMB(+RAW)/VLINKS(+WITHSCORES)/VINFO/VRANDMEMBER/VRANGE/VGETATTR/VSETATTR | NATIVE_OK + live phase 5/6（Keys/Script/Buckets/Vector/Function/Batch ✅） + live phase 12（batch Lua ops + 跨语言读 ✅） + live phase 22（batch geo/bitset/HPEXPIRE ✅） + live phase 23（keys ops / slots vs 集群节点 / MOVE db9 ✅） + live phase 32（lazy key stream ✅） + live phase 33（SCRIPT/FUNCTION/VectorSet ✅） + live phase 35（RJsonBucket + Java 双向 ✅） + live phase 37（RSearch ✅） + live phase 40（RJsonBuckets 批量 ✅） + live phase 43（RJsonBucket `*Multi`/多路径 ✅） + live phase 44（hybrid/profile/游标/alter ✅） + live phase 46（migrate/远程 copy/swapdb/并行 flush ✅） + live phase 47（RKeys 键空间事件 ✅） + live phase 48（限量/模式扫描 ✅） + live phase 49（结构化索引 ✅） + live phase 50（VectorSet 深层 ✅） |
| RPriority* / RTransferQueue | ZSET+score / queue migration (**GO_ONLY**, not Java Comparator/RemoteService) | GO_ONLY |
| RSortedSet / RTransaction / RRemoteService / RExecutorService / LiveObject / reactive | — | REFUSE_AS_FAKE |
| PRO-only (ReliableQueue/LocalCachedMapCache/ReliablePubSubTopic/BitVectorStore/LeasedMap) | — | PRO (throw) |
| RClientSideCaching | thin pass-through (no Java EvictionPolicy) | PARTIAL |

## Verification

- `npm test`: 280 tests / 55 files — `wire` (codec/names/hash-vectors/struct/map-cache events),
  mock functional (incl. phase-21 delayed-queue migrator lifecycle, phase-22
  §3 leftovers, phase-23 keys/atomic remainder, phase-24 zset remainder,
  phase-25 time-series family, phase-26 topic/set surfaces, phase-27
  map views/streaming, phase-28 interruptible lock/map-cache remainder,
  phase-29 rwlock_timeout, phase-30 map-cache compute/status listeners,
  phase-31 keyspace listeners/multi-stream group reads, phase-32 API polish,
  phase-33 bitset batch/delayed-queue reads, phase-35 RJsonBucket, phase-36
  probabilistic deep APIs, phase-37 RSearch, phase-38 lazy iterators,
  phase-39 reliable-topic semantics, phase-40 JSON/native-map batches,
  phase-41 queue/deque surface, phase-42 native-map fast writes, phase-43
  JSON multi-match, phase-45 queue element subscriptions, phase-48
  last-from-any/limited scans, phase-51 native-map PutArgs/compute and
  phase-52 set count/listeners), `interop`
  (live python codec/hash), `live-interop`
  (bidirectional vs Redisson 4.6.1: 20 Java→TS reads + 28 TS→Java reads,
  all PASS), `live-sync` (mutual exclusion both directions, shared semaphore
  permits, shared rate-limiter window), `live-phase3` (rwlock/fenced/delayed/
  topic/ts/adder/pes, 8/8 PASS), `live-phase4` (topic/stream-group/multimap/
  bq/ring/idgen/batch/mc-ttl, 8/8 PASS), `live-phase5` (pattern/sharded/
  blocking/latch/geo/bits/hll/cache/reenter/keys/vector, 9/9 PASS),
  `live-phase6` (mc-events/stream-ack/dq-remove/rate-release/ts-label/bitop/
  hll/mcnative/buckets/fn/order, 8/8 PASS), `live-phase7` (bstream/fanout/
  mc-maxsize/mmcache/zset+/trim/fn-reload/bbq-take/dq-fifo, 9/9 PASS),
  `live-phase8` (lcm/deque-list-set/atomicdouble-bucket-cas/pes-multi/
  rate-update/downgrade/watchdog/zset-drain, 8/8 PASS),
  `live-phase9` (multilock-redlock/lcm-strategies/cache-paths/misc + slot
  parity + mock contention, 7/7 PASS),
  `live-phase10` (bdeque/batch/bucket-ttl/mc-combo/sc-idle/destroy, 6/6 PASS),
  `live-phase11` (zset union/intersection/diff, 2/2 PASS),
  `live-phase12` (batch Lua ops + cross-language reads, 2/2 PASS),
  `live-phase13` (stream groups/pending/claim, 2/2 PASS),
  `live-phase14` (stream add/read opts, nack, ackdel, 2/2 PASS),
  `live-phase15` (plain-set store ops; `SOPSTORE self others…` bare-key shape
  verified against 4.6.1; `countUnion` gated on `SUNIONCARD`, 2/2 PASS),
  `live-phase16` (companion-key cleanup / DQ size+delete / LCM TTL-idle, 7/7 PASS),
  `live-phase17` (bounded semaphore / blocking isolation / topic lifecycle, 9/9 PASS),
  `live-phase18` (BigInt buckets / exact CAS / AtomicDouble / drainTo, 4/4 PASS),
  `live-phase19` (six key-synchronizer names vs Java / key-lock exclusion both
  ways / MapCache events both ways / conditional expiry + HPERSIST + putIfAbsent, 5/5 PASS),
  `live-phase21` (delayed-queue background migrator: channel re-arm on an
  earlier head, stop halts, destroy keeps data, Java `list-read` cross-check, 1/1 PASS),
  `live-phase22` (fenced token sequence TS→Java→TS / real BLPOP+BLMPOP+BLMOVE
  wake-ups / keepAlive renewal by TS and Java / periodic multimap sweep / batch
  geo+HPEXPIRE wire, 5/5 PASS),
  `live-phase23` (RKeys expire family + MOVE to db 9 + slots vs a live cluster
  node / MSETEX NX+XX read by Java / atomic exactness / ZADD GT/LT scores, 4/4 PASS),
  `live-phase24` (zset addAll NX/XX/GT/LT + Java score reads / ZRANDMEMBER /
  BZPOPMIN+BZPOPMAX+BZMPOP wake-ups, 2/2 PASS),
  `live-phase25` (time-series addAll + Java ts-range/ts-add-label round-trip /
  poll+getAndRemove / removeRange / TTL expiry filtering, 2/2 PASS),
  `live-phase26` (live listener/subscriber counts + active topics / XCLAIM and
  XAUTOCLAIM JUSTID / multi-stream XREAD, 2/2 PASS),
  `live-phase27` (RMap views vs Java HSCAN / conditional replace/remove read by
  Java / TimeSeries stream+iterator vs ts-range, 2/2 PASS),
  `live-phase28` (contended lockInterruptibly abort across two clients /
  MapCache expireEntries TTL ZSET / TTL-only reads / max-size persistence, 2/2 PASS),
  `live-phase29` (Java-held read lock survives a TS read acquire/release with
  `rwlock_timeout` keys intact, then disappears on Java release, 1/1 PASS),
  `live-phase30` (concurrent RMapCache.compute serialization + Java mc-get /
  status listener subscribe-unsubscribe callbacks, 2/2 PASS),
  `live-phase31` (six stream keyspace events + removed-listener silence /
  TimeSeries zadd+zrem events / multi-stream XREADGROUP, 2/2 PASS),
  `live-phase32` (lazy key stream / removeListener(handler) silence /
  per-stream group XREADGROUP, 2/2 PASS),
  `live-phase33` (SCRIPT FLUSH/KILL, FUNCTION list/stats/dump/restore, Redis 8
  VectorSet deep API, RMapCacheNative Date/HPEXPIREAT, 3/3 PASS),
  `live-phase35` (RJsonBucket document/path/NX-XX/CAS/string-array-number ops
  + Java JacksonCodec round-trip, 1/1 PASS),
  `live-phase36` (TDigest min/max/cdf/rank/merge/info/reset, CuckooFilter
  addIfAbsent/existsMany/count/info, TopK dropped items/listWithCount, 3/3 PASS),
  `live-phase37` (RSearch index/search/sort/params/alias/aggregate/synonyms/
  dicts/config/drop-with-docs, 1/1 PASS),
  `live-phase38` (SSCAN/ZSCAN encoded-prefix MATCH + bounded preloadCache, 1/1 PASS),
  `live-phase39` (reliable topic: one group per instance, publish count 0/1/2,
  automatic trim and per-instance teardown, 1/1 PASS),
  `live-phase40` (RJsonBuckets MSET/MGET + path reads; native MapCache
  conditional batches/Date expiry with Java map reads, 2/2 PASS),
  `live-phase41` (RPOPLPUSH, variadic adds, LPUSHX/RPUSHX, LMOVE, count polls,
  1/1 PASS),
  `live-phase42` (native MapCache fastPut/fastPutIfAbsent/putIfExist/
  fastPutIfExist with field TTLs and Java reads, 1/1 PASS),
  `live-phase43` (RJsonBucket `*Multi` match lists, multi-path get and
  arrayIndex boundaries, 1/1 PASS),
  `live-phase44` (FT.HYBRID KNN/RANGE + RRF/LINEAR, FT.PROFILE search/
  aggregate, FT.CURSOR READ/DEL, FT.ALTER and alias search, 1/1 PASS),
  `live-phase45` (element subscriptions: order/back-pressure, prompt BLPOP
  cancellation, deque and bounded-capacity delivery, 1/1 PASS),
  `live-phase46` (second Redis: single/multi-key MIGRATE, remote COPY,
  COPY+REPLACE, SWAPDB, FLUSHDB/FLUSHALL ASYNC, 1/1 PASS),
  `live-phase47` (RKeys global new/set/expired/del events, removal through a
  fresh facade, 1/1 PASS),
  `live-phase48` (BRPOP with source name/priority/negative timeout, deque,
  getKeysWithLimit/getKeysByPattern, 1/1 PASS),
  `live-phase49` (structured index schema with TEXT/TAG/NUMERIC/GEO/FLAT+HNSW
  vectors, aliases, filters, KNN and structured ALTER, 1/1 PASS),
  `live-phase50` (VectorSet add args/CAS/raw vectors/scored neighbors and
  attribute-aware VSIM, 1/1 PASS),
  `live-phase51` (native MapCache HSETEX KEEPTTL/PX/PXAT, FXX/FNX batches,
  compute TTL, Java map reads, 1/1 PASS),
  `live-phase52` (RSet SINTERCARD with LIMIT and sadd/srem/spop/store keyspace
  events with removal silence, 1/1 PASS),
  `live-cluster` (3-master cluster on 7100-7102: bucket/map/atomic/lock with a
  second client contending through slot routing, MapCache TTL + events over the
  cluster bus, delayed-queue migrator end-to-end, rate limiter + topic, 4/4 PASS).
- Flake audit (v2.12.1): three consecutive full-suite runs green —
  **246/246 tests / 37 files** at 82.15 s, 71.40 s and 71.62 s; no retries or
  threshold changes were needed (spread is machine load, not flakiness).
- Packaging: `npm run build` emits `dist/cjs` + `dist/esm` (two `tsc` passes,
  per-dir `package.json` stubs); `npm run pack-smoke` packs the tarball,
  installs it into a temp consumer and asserts `require`/`import` both resolve
  and produce identical wire output.
- Cluster keys: every companion key is hash-tagged (`prefixName`/`suffixName`),
  so all multi-key Lua/EVAL calls stay single-slot; `live-cluster` runs with an
  ioredis `Cluster` as the Redisson client (`Config.client` accepts
  `Redis | Cluster`). Commands missing on the local server are capability-gated
  via `COMMAND INFO` and logged as SKIP (`XNACK`/`SUNIONCARD` verified live on
  Redis 8.10.2).
- Byte-level: HighwayHash vectors + multimap ids asserted equal to
  `redi.py` in-process; Lua scripts ported verbatim from `redi.go`
  (Redisson 4.6.1 verified).
- Live-Redis suites need Redis at `127.0.0.1:6379` (else skip) and, for the
  Java directions, `mvn` + `interop/` probe (else fail loudly, never silent).

  探针 verdict 键是 JsonJacksonCodec 写入的 bucket——TS 侧读须走 `getBucket().get()`
  （裸 `GET` 会拿到带引号的 JSON 字符串，对照断言会假失败）。
