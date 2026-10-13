## 1.8.5 — 2026-10-13

RBatch decision closed (see `docs/GAP_ANALYSIS.md` §2): typed facades over
deferred promises instead of doc-lock — `BatchBucket/Map/List/Set/Queue/
AtomicLong/ScoredSortedSet` (writes queue, reads resolve on `execute()`,
RFuture semantics incl. never-await-pre-execute, `discard()` rejects);
Lua-backed ops covered live (mock pipelines lack `eval`).

## 1.8.4 — 2026-10-13

P1 biggest gap closed (see `docs/GAP_ANALYSIS.md` §2):

- `RScoredSortedSet` 交并差全族：`union/unionWith/readUnion/readUnionEntries/`
  `intersection/intersectionWith/readIntersection/readIntersectionEntries/`
  `countIntersection/countIntersectionLimit/diff/readDiff/readDiffEntries`
  （store 版“只算 others 存入 self”、read 版含 self 的参数顺序与 Java 逐字对齐；
  live phase 11 验证 WEIGHTS 位置与聚合分数，双向 2/2 PASS）。

P1 quick win (see `docs/GAP_ANALYSIS.md` §2 `RMap`):

- `RMap.put` / single-key `remove` / `putIfAbsent`: GET+HSET(HDEL) multi-RTT
  rewritten as single-Lua atomic old-value return (same pattern as existing
  `putIfExists`; zero wire change, mock-safe — same `hget/hset/hsetnx`
  commands the mock already runs).
- `RRateLimiter` acquire-path config caching documented in matrix
  (instance-cached after first load, `getConfig` always fresh — mirrors
  Java instance fields; decision: keep, P0-4 tail closed).

## 1.8.2 — 2026-10-13

P0 follow-ups (see `docs/GAP_ANALYSIS.md` §1):

- `RSpinLock`: `BackOff`/`BackOffPolicy` + `constantBackOff`/`exponentialBackOff`
  (defaults 1→128ms ×2 + jitter, mirrored exactly incl. sticky cap) +
  `getSpinLock(name, backOff?)`; default retry now exponential, was fixed 10ms.
  Verified: unlock-latch is per-request idempotency, never read on acquire —
  no cross-language effect, not ported.
- `RRateLimiter`: hex-text permit members; per-process fallback tag for
  direct construction (factory already passes client id); live spin HASH
  exclusion both directions (new test).

## 1.8.1 — 2026-10-13

P0 gap fixes (see `docs/GAP_ANALYSIS.md` §1):

- `RPermitExpirableSemaphore`: permit ids switched from latin1 to hex text
  (same domain as Java UUIDs; latin1 corrupts on the UTF-8 wire and breaks
  cross-language id lookup); `tryAcquire`/`acquire` now always return
  `string[]` (was `string | null` / comma-joined — Breaking).
- `RSemaphore.setPermits` removed (was TS-invented bare `SET` without
  publish: starved waiters + dirty totals; Redisson has no such method).
- `RIdGenerator.tryInit` aligned to Java exactly (returns second `setnx`
  only — bug-compatible by design).

## 1.8.0 — 2026-10-12

Phase 10 live interop (93 → 99 tests green, 6/6 PASS first run):

- BlockingDeque 双向交接；batch Set/Atomic 写 Java 可见；Bucket TTL；
  MapCache TTL+maxIdle 组合；SetCache idle（presence 共享、过期 TS 侧，
  4.6.1 本无 idle 概念）；MapCache `destroy()` 清数据 + companion。

## 1.7.1 — 2026-10-12

- Fix real bug: `RSetCache` maxIdle never expired anything (`contains`
  unconditionally renewed; without last-touch tracking expiry is
  unrepresentable). Now deadline-tracked via `{name}:redisson_touch`
  (Java-invisible, presence stays cross-readable), enforced + refreshed on
  `contains`, swept in `size`/`readAll`, cleaned on `remove`/`purge`.
  Redisson itself has no SetCache-idle concept (TTL-only, verified in
  4.6.1 + current sources) — ours is a documented extension.

## 1.7.0 — 2026-10-12

Phase 9 live interop (86 → 93 tests green, 5/7 first run → 7/7):

- MultiLock 全有或全无 + 回滚（Java 持有一把时 TS 侧失败，被回滚的另一把仍可获取）；
  RedLock 多数派（Java 占一把，TS 凭另两把达成 2/3）。
- LocalCachedMap：UPDATE 广播带值（杀后端仍命中）vs INVALIDATE（杀后端即 miss）
  对照；数据层 = RMap wire（Java 直读/直写）。
- MapCache/SetCache 快捷路径、zset/map/atomic/bucket/keys/scriptSHA/vector 杂项互读；
  CRC16 槽位静态回归（25 对 companion 键）；mock 20 路并发唯一获胜。
- 源码修复：`RKeys.copy` 空字符串参数 bug（同 trim 一类）；LCM 先挂 handler
  后 subscribe（同 adder 一类）。
- 测试卫生：监听类就绪一律 NUMSUB 等待（固定 sleep 赌 JVM 启动必输）。

## 1.6.0 — 2026-10-12

Phase 8 live interop (78 → 86 tests green, 8/8 PASS first run):

- LocalCachedMap 数据层 = RMap wire（Java 直读）+ TS↔TS 失效广播；
  Deque/List/Set 位置操作（pollFirst/Last、addAt/set/trim/indexOf、SMOVE）；
  AtomicDouble 精度（INCRBYFLOAT 0.1+0.2）；Bucket/Map CAS；
  PES 多许可 + 租约更新；Rate updateRate + availablePermits 互读；
  RW 同线程 write→read 降级；RLock watchdog 35s 续期；zset pollFirst、
  drainTo、stream deleteEntries。
- 探针 +8 读命令（sc-contains、bucket-get、deque/list/set-read、
  atomicdouble-add/get、lmm-get），全部先冒烟。

## 1.5.0 — 2026-10-12

Phase 5/6/7 live interop (52 → 78 tests green):

- Phase 5：Pattern/Sharded Topic 双向（含 `smessage` 事件修复）、BlockingQueue
  交接、Latch 开门、Geo/BitSet/HLL 互读、SetCache TTL、MapCache maxIdle 刷新、
  MapCacheNative、重入计数/Keys/Script、VectorSet 共享、ListMultimap 保序。
- Phase 6：MapCache 四类事件进 Java 监听、Stream pending/ack、DQ 外条目删除、
  Rate 双向 release、TS label/4.6.1 形状/TTL、BITOP/HLL merge、Native 过期、
  Buckets、Redis Functions、ReliableTopic 保序。
- Phase 7：BinaryStream 原始字节、ReliableTopic 双组 fanout（双向）、MapCache
  maxSize 淘汰、MultimapCache 过期、zset/map/atomic 高级操作互读、Stream
  trim（`~` parity + exact）、Function 热替换、Bounded 阻塞 take、DQ 按
  deadline 先后投递。
- 源码修复：`RShardedTopic` 加 `smessage` 监听；MapCache `updated` 事件补 old
  段；TimeSeries 写侧切 4.6.1 形状（n=2/3 无前缀 label）读侧实现 DECODE_LABEL；
  `RStream.trim(exact)` 空字符串参数 bug。
- 探针教训：`mcnative-put` 参数守卫差一位吞 TTL；Function 须 `WRITE` 模式
  （`FCALL_RO` 拒无 no-writes 标记函数）；监听类探针一律 Redis 键握手就绪；
  MultimapCache 过期须经 cache 读（plain 读按设计忽略 TTL）；`MAXLEN ~`
  小流不裁（Java 默认亦然，parity 即正确）；多 offer 须同 JVM（冷启间隔
  淹没小 delay 的相对顺序）。
- 测试卫生：Multimap 清理必须 `clear()` 先行（经 HASH 反查集合；顺序反了会
  静默 no-op，确定性 id 导致残留复用——Java 同行为）。

## 1.4.0 — 2026-10-12

Phase 4 live interop (topic/stream/multimap/bq/ring/idgen/batch/mc-ttl):

- Topic 双向投递（PUBSUB NUMSUB 就绪后发布）；Stream 消费组跨语言读
  （`StreamCreateGroupArgs.makeStream`，空流建组须 MKSTREAM，id 默认 NEWEST）；
  SetMultimap 内部 id 字节级一致；BoundedBlockingQueue/RingBuffer 容量共享；
  IdGenerator 区间交错无冲突；Batch 双向；MapCache 单条目 TTL 跨语言过期。
- 修复测试基建：companion 键清理改精确 `DEL`（Redis glob 的 `{a,b}` 是交替
  语法，`SCAN MATCH` 会漏删花括号键）；长驻探针发布前等订阅就绪；
  探针 verdict 键是 JsonJacksonCodec bucket，TS 读须解码（裸 GET 会带引号）。

## 1.3.0 — 2026-10-12

Phase 3 live interop vs Redisson 4.6.1 (all PASS, 8/8):

- `RReadWriteLock` 读写互斥双向 + 共享读；`RFencedLock` token 跨语言单调递增；
  `RDelayedQueue` 双向迁移；`RReliableTopic` 双向投递；
  `RTimeSeries` 精确时间戳双向；`RLongAdder` 跨语言 flush 求和；
  `RPermitExpirableSemaphore` 共享许可池。
- 修三处真 bug：adder 键缺 hash-tag 且 sum 重复计本地值（对齐 redi.go）；
  struct-packed 成员/值走 Buffer 传输（UTF-8 会破坏随机 id/double 字节，
  MapCache/TimeSeries/DelayedQueue/BinaryStream/BitSet 全面切换）；
  MapCache 改明令实现（Lua 传参带不了二进制）。
- 探针：`dq-poll` 先建 DelayedQueue 包装类（否则迁移任务不调度）；
  `longadder-cohort` 常驻 JVM；长驻探针 verdict 双写 Redis 键 + stdout。
- Total: 44 tests green.

## 1.2.0 — 2026-10-12

Live interop vs Java Redisson 4.6.1 (local Redis 8.10.2):

- New `interop/` Maven probe (`redits-java-probe`, Redisson 4.6.1 +
  `JsonJacksonCodec`; plain Maven, no Spring Boot — interop is about wire
  format, not framework integration).
- `test/live-interop.test.ts`: Java→TS 20 reads + TS→Java 28 reads, all PASS
  (bucket variants incl. `Long`/`ArrayList`/nested-map, atomics, map, set,
  list, queue, deque, zset, lex-raw, blocking queue, bloom, mapcache,
  setcache, latch, semaphore, HLL, geo, stream).
- `test/live-sync.test.ts`: RLock mutual exclusion both directions,
  semaphore shared permits, rate-limiter shared window — all PASS.
- Total: 36 tests green. Next: RWLock/fenced/delayed-queue/reliable-topic
  cross-language timing suites.

## 1.1.0 — 2026-10-12

Full 1:1 TypeScript port of Redisson (Java normative, `redi.go` Lua baseline):

- Codec: `JsonCodec` with `@class`/`ArrayList`/`Long` wrapping + recursive
  unwrapping (`StringCodec`, `ByteArrayCodec` for raw paths).
- `util`: hash-tag names, server `TIME`, `HighwayHash-128` (vectors verified
  live vs `redi.py`), multimap ids, `<dQ`/`Bc0I`/`Bc0Lc0`/`BBc0Lc0Lc0` packs.
- Locks: `RLock`/`Spin`/`NonReentrant`/`Fair`/`NonReentrantFair`/`ReadWrite`/
  `Fenced`/`Multi`/`RedLock`, `Semaphore`, `CountDownLatch`,
  `PermitExpirableSemaphore` — Lua verbatim, watchdog `lease/3`, pub/sub wake.
- Collections, caches, messaging, probabilistic, infra: 60+ factories on
  `Redisson` with Redisson-style + `R`-prefixed aliases.
- Tests: 30 green (`wire` + mock functional + live python interop).

## 1.0.0 — earlier

Early `RBucket`/`RAtomicLong`/`RMap`/`RLock` preview on `ioredis-mock`.
