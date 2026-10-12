# COMPATIBILITY — redi.ts ↔ Redisson / redi.py / redi.go

> Normative: Java Redisson sources (`C:/GitHub/redisson`, 4.6.x) + `JsonJacksonCodec`.
> Direct port baseline: `redi.go` Lua scripts (verified against Redisson 4.6.1);
> `redi.py` used for codec/hash cross-checks (note: it writes bare JSON objects,
> which Java default typing rejects — redi.ts follows redi.go and wraps them).

## Wire facts (verified)

1. **JsonJacksonCodec default typing**: bare `{...}`/`[...]` rejected by Java.
   Encode wraps maps (`@class: java.util.LinkedHashMap`) and arrays
   (`["java.util.ArrayList",[...]]`), longs (`["java.lang.Long",v]`); decode
   strips all wrappers and any `@class`. `test/wire.test.ts` + live
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

## Status matrix

| Structure | Redis layout | Status |
|-----------|--------------|--------|
| RLock / Spin / NonReentrant | HASH `{name}` field=`uuid:thread`, channel `redisson_lock__channel:{name}`; Lua acquire/renew/unlock/force verbatim | WIRE_OK + live mutual exclusion both directions ✅ |
| RFairLock / NonReentrantFair | + LIST `redisson_lock_queue:{name}` + ZSET `redisson_lock_timeout:{name}`, per-waiter `channel:field`, FIFO Lua verbatim | WIRE_OK |
| RReadWriteLock | single HASH + `mode`, `redisson_rwlock:{name}` (read→`1`, write→`0`), downgrade parity | WIRE_OK + live phase 3 (互斥双向+共享读） |
| RFencedLock | RLock HASH + `redisson_lock_token:{name}` (`INCR` incl. re-entry, returns `{-1,token}`) | WIRE_OK + live phase 3 (token 跨语言单调） |
| RMultiLock / RRedLock | client-side orchestration (quorum `n/2+1`, lease amplification) | WIRE_OK + live phase 9（全有或全无+回滚，多数派 2/3 ✅） |
| RSemaphore | STRING + `redisson_sc:{name}` Lua | WIRE_OK + live shared-permit ✅ |
| RCountDownLatch | STRING + `redisson_countdownlatch__channel__{name}` (`1` set / `0` zero) | WIRE_OK (count shared, live read ✅) |
| RPermitExpirableSemaphore | STRING + ZSET `{name}:timeout` + `redisson_sc:{name}`, lazy purge | WIRE_OK + live phase 3 (共享许可池） |
| RMap / RList / RSet / RQueue / RDeque / Blocking(+Bounded) | bare name + JSON values (map keys JSON) | WIRE_OK + live bidirectional ✅ |
| RScoredSortedSet | ZSET + JSON members | WIRE_OK + live bidirectional ✅ |
| RLexSortedSet | ZSET score 0, **raw** members (no codec) | WIRE_OK + live bidirectional ✅ |
| RBucket / RBuckets / RBinaryStream | STRING (+ms TTL, CAS Lua) | WIRE_OK + live bidirectional ✅ |
| RAtomicLong / RAtomicDouble | decimal strings, `INCRBY(FLOAT)` | WIRE_OK + live bidirectional ✅ |
| RLongAdder / RDoubleAdder | `{name}:adder-topic` + `{name}:<id>:counter` + `{name}:<id>:semaphore` hash-tag 三元组；sum 只返回 GETDEL（请求者自 flush 已包含在内）；订阅先挂 handler 后 await ack | WIRE_OK + live phase 3 |
| RIdGenerator | counter + `{name}:allocation` interval cache | WIRE_OK |
| RMapCache | HASH packed `<dQ` + `redisson__timeout__set:{name}`/`redisson__idle__set:{name}`/`last_access`/`redisson_options` + `redisson_map_cache_*:{name}` events（明令 +Buffer 传输：Lua 传参无法承载二进制，容量淘汰为尽力而为；`updated` 为 `[key,new,old]` 三段） | WIRE_OK + live bidirectional ✅ + live phase 6（四类事件进 Java 监听 ✅） |
| RMapCacheNative | plain HASH + `HPEXPIRE` (≥7.4) | NATIVE_OK + live phase 6（TTL 跨语言过期 ✅） |
| RSetCache | ZSET(abs expiry/`MaxInt64`) + `redisson__idle__set:{name}` durations | WIRE_OK + live bidirectional ✅ |
| RMultimap (Set/List) | HASH field→id + `{name}:{id}` collections, Highway internal ids | WIRE_OK |
| RSet/ListMultimapCache (+Native) | + per-key TTL ZSET / `HPEXPIRE` | WIRE_OK / NATIVE_OK |
| RLocalCachedMap | data = RMap wire (live phase 9 Java 直读/直写 ✅); invalidation = local JSON broadcast, UPDATE 带值 / INVALIDATE 语义均有对照测试 (Java binary protocol not ported — disable near-cache for mixed deploys) | PARTIAL |
| RTopic / RPatternTopic / RShardedTopic | `PUBLISH`/`PSUBSCRIBE`/`SSUBSCRIBE` + JSON（分片投递走 `smessage` 事件） | WIRE_OK / NATIVE_OK + live phase 4/5（Topic/Sharded/Pattern 双向 ✅） |
| RReliableTopic | STREAM field **`m`** + per-subscriber group + `{name}:timeout` | WIRE_OK + live phase 3 (双向投递） |
| RStream | STREAM, field names **and** values JSON-encoded | WIRE_OK + live bidirectional ✅ |
| RDelayedQueue | ZSET `redisson_delay_queue_timeout:{name}` + LIST `redisson_delay_queue:{name}` (`Bc0Lc0`) + channel（offer id 走 Buffer 保真；消费侧迁移须由 DelayedQueue 包装类调度，直接 poll 目标队列不会迁移） | WIRE_OK + live phase 3 (双向迁移） |
| RRingBuffer | LIST + `redisson_rb:{name}` capacity | WIRE_OK |
| RGeo | GEOADD/SEARCH, members JSON-encoded | WIRE_OK + live bidirectional ✅ |
| RHyperLogLog | PFADD/PFCOUNT/MERGE, members JSON-encoded | NATIVE_OK + live bidirectional ✅ |
| RBitSet | native MSB-first numbering, no reversal | WIRE_OK |
| RTimeSeries | ZSET packed `BBc0Lc0Lc0` + `redisson__ts_ttl:{name}` + `redisson__ts_seq:{name}` (4.6.1 首字节 `2` 无 label；unpack 与 n/t 无关故双向兼容；成员须走 Buffer 传输，UTF-8 会破坏随机 id 字节） | WIRE_OK + live phase 3 |
| RBloomFilter | bitmap + `{name}:config` HASH, HighwayHash-128 | WIRE_OK + live bidirectional ✅ |
| BF/CF/TOPK/TDIGEST/GCRA | `BF.*`/`CF.*`/`TOPK.*`/`TDIGEST.*`/`GCRA` native | NATIVE_OK |
| RRateLimiter | HASH config + `{name}:value` + ZSET `{name}:permits` (`Bc0I`) OVERALL/PER_CLIENT | WIRE_OK + live shared-window ✅ |
| RKeys / RScript / RFunction / RSearch / RJsonBucket / RVectorSet / RBatch / RMaps / RArray / RCircularBuffer | native commands / pipeline | NATIVE_OK + live phase 5/6（Keys/Script/Buckets/Vector/Function/Batch ✅；RSearch/JSON 需模块，本地无） |
| RPriority* / RTransferQueue | ZSET+score / queue migration (**GO_ONLY**, not Java Comparator/RemoteService) | GO_ONLY |
| RSortedSet / RTransaction / RRemoteService / RExecutorService / LiveObject / reactive | — | REFUSE_AS_FAKE |
| PRO-only (ReliableQueue/LocalCachedMapCache/ReliablePubSubTopic/BitVectorStore) | — | PRO (throw) |
| RClientSideCaching | thin pass-through (no Java EvictionPolicy) | PARTIAL |

## Verification

- `npm test`: 93 tests — `wire` (codec/names/hash-vectors/struct),
  mock functional, `interop` (live python codec/hash), `live-interop`
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
  parity + mock contention, 7/7 PASS).

  探针 verdict 键是 JsonJacksonCodec 写入的 bucket——TS 侧读须走 `getBucket().get()`
  （裸 `GET` 会拿到带引号的 JSON 字符串，对照断言会假失败）。
- Byte-level: HighwayHash vectors + multimap ids asserted equal to
  `redi.py` in-process; Lua scripts ported verbatim from `redi.go`
  (Redisson 4.6.1 verified).
- Live-Redis suites need Redis at `127.0.0.1:6379` (else skip) and, for the
  Java directions, `mvn` + `interop/` probe (else fail loudly, never silent).
