## 2.32.0 — 2026-10-21

Phase 54: RDelayedQueue 4.6.1 surface. Full suite:
**284 tests / 57 files**.

- `offer(value, delayMs)` now rejects negative delays
  (`Delay can't be negative`).
- `add(value)` and single-argument `offer(value)` throw the Java
  "Use 'offer' method with timeout param" error; `indexOf` throws
  UnsupportedOperationException.
- `remove()` (no argument) polls the destination or throws
  NoSuchElementException; `remove(value)` still removes staged members.
- `pollLastAndOfferFirstTo(dest)`: atomic staging RPOP + timeout ZREM +
  destination LPUSH (Lua, Buffer-safe), decodes the moved value.
- Tests: 1 mock (validation/remove) + `test/live-phase54.test.ts` 1/1
  (negative delay, staged move to destination, pending/size updates,
  ready-after-delay poll, empty remove throws).

## 2.31.0 — 2026-10-21

Phase 53: atomic keyspace listeners. Full suite:
**282 tests / 56 files**.

- **`RAtomicLong.addListener` / `removeListener`**: `onChange` → `incrby`
  keyspace events.
- **`RAtomicDouble.addListener` / `removeListener`**: subscribes to both
  `incrby` and `incrbyfloat` (Redis emits the latter for INCRBYFLOAT; Java
  maps only `incrby`, which would miss addAndGet).
- Tests: 1 mock (registration/removal) + `test/live-phase53.test.ts` 1/1
  (incrby + incrbyfloat delivery, removal silence; config serialized with the
  shared keyspace test lock).

## 2.30.0 — 2026-10-21

Phase 52: RSet countIntersection + keyspace listeners. Full suite:
**280 tests / 55 files**.

- **`countIntersection(...names)`** and **`countIntersection(limit, ...names)`**:
  `SINTERCARD numkeys self names [LIMIT n]` (limit 0 = unlimited), with a
  client-side SMEMBERS fallback for mocks/older servers.
- **`addListener` / `removeListener`** on `RSet`: `onAdd` → `sadd`,
  `onRemove` → `srem`, `onRandomRemove` → `spop`, and `onStore` subscribed to
  `sinterstore`/`sunionstore`/`sdiffstore`. The keyspace hub now supports one
  callback mapped to multiple event patterns.
- Tests: 1 mock (count intersection/limit/empty, listener registration) +
  `test/live-phase52.test.ts` 1/1 (SINTERCARD + LIMIT, all four set events,
  removal silence; config serialized with the shared keyspace test lock).

## 2.29.0 — 2026-10-21

Phase 51: RMapCacheNative structured batches + compute. Full suite:
**278 tests / 54 files**.

- **`NativePutArgs`** (TS shape of Java `PutArgs`): `entries`, `keepTTL`,
  `timeToLiveMs`, `expireAt`.
- **`putAll`**: one atomic `HSETEX` with KEEPTTL/PX/PXAT (per-field HSET
  fallback on older servers/mock).
- **`putIfAllKeysExist` / `putIfAllKeysAbsent`**: structured PutArgs via
  `HSETEX FXX/FNX` (Lua fallback preserved for plain Map + TTL).
- **`compute` / `computeIfAbsent`**: per-key distributed lock, double-checked
  reads, async callbacks, null deletes/writes skipped, field TTL applied via
  fastPut/putIfAbsent.
- Tests: 1 mock (structured batches, conditional paths, compute null/delete
  and concurrent computeIfAbsent executes the mapper once) +
  `test/live-phase51.test.ts` 1/1 (HSETEX TTL/PXAT/KEEPTTL, FXX/FNX atomicity,
  compute TTL, Java `map-get`; HSETEX capability gate).

## 2.28.0 — 2026-10-21

Phase 50: RVectorSet deep API. Full suite:
**276 tests / 53 files**.

- **`add(VectorAddArgs)`** (in addition to `add(element, vector)`): REDUCE, CAS,
  quantization (NOQUANT/Q8/BIN), EF, SETATTR with a per-call codec and M.
- **`getRawVector(element)`**: `VEMB ... RAW` returning raw parts with binary
  payloads preserved (Buffer).
- **`getNeighborEntries(element)`**: `VLINKS ... WITHSCORES` as
  `{ value, score }`; `getNeighbors` now unwraps the nested VLINKS reply.
- **`getSimilar` / `getSimilarEntries` / `getSimilarEntriesWithAttributes`**:
  `VSIM` by element or vector (VALUES/FP32) with COUNT/EPSILON/EF/FILTER/
  FILTER-EF/TRUTH/NOTHREAD and WITHSCORES/WITHATTRIBS parsing.
- `setAttributes` accepts an optional codec.
- Tests: `test/live-phase50.test.ts` 1/1 (attributes, CAS, raw vector, scored
  neighbors, filtered/similar names-scores-attributes, Q8/REDUCE/M options;
  capability-gated on VADD).

## 2.27.0 — 2026-10-21

Phase 49: structured RSearch index definitions. Full suite:
**275 tests / 52 files**.

- **`RSearch.createIndex`** now accepts either raw schema tokens (unchanged)
  or structured `SearchFieldIndex` objects with `SearchIndexOptions`
  (TS shape of Java `IndexOptions`/`FieldIndex`).
- Field types: TEXT (AS/NOSTEM/PHONETIC/WEIGHT/WITHSUFFIXTRIE/SORTABLE/UNF/
  NOINDEX/INDEXEMPTY/INDEXMISSING), TAG (CASESENSITIVE/SEPARATOR/...), NUMERIC,
  GEO, GEOSHAPE, and VECTOR with FLAT or HNSW (TYPE/DIM/DISTANCE_METRIC/
  INITIAL_CAP/BLOCK_SIZE/M/EF_CONSTRUCTION/EF_RUNTIME/EPSILON).
- Index options: ON, PREFIX, FILTER, LANGUAGE, LANGUAGE_FIELD, SCORE,
  SCORE_FIELD, PAYLOAD_FIELD, MAXTEXTFIELDS, TEMPORARY, NOOFFSETS, NOHL,
  NOFIELDS, NOFREQS, STOPWORDS (including empty) and SKIPINITIALSCAN.
- **`RSearch.alter`** accepts structured fields too (one `ADD` per field).
- Redis grammar note: INDEXEMPTY/INDEXMISSING must precede SORTABLE; the
  builder emits that order even though the newer Java source emits them last.
  SVS-VAMANA vectors are not built yet.
- Tests: `test/live-phase49.test.ts` 1/1 (structured HASH index with aliases,
  filter/stopwords/no-offsets, tag case sensitivity, numeric/geo queries,
  sort by alias, FLAT+HNSW KNN, FT.INFO and structured ALTER).

## 2.26.0 — 2026-10-21

Phase 48: last-from-any with source name + limited key scans. Full suite:
**274 tests / 51 files**.

- **`RBlockingQueue` / `RBlockingDeque`**:
  `pollLastFromAnyWithName(timeoutMs, ...queueNames)` — BRPOP over this queue
  plus the given ones, returning `{ key, value }`; negative timeouts return
  null without scanning, `null` blocks forever.
- **`RKeys`**: `getKeysWithLimit(limit)` / `getKeysWithLimit(pattern, limit)`
  (SCAN stops after the limit, `limit <= 0` → empty) and
  `getKeysByPattern(pattern[, count])`; both share the SCAN helper with
  `getKeys`.
- Note: `QueueMoveElementsArgs`/`RQueue.move` belong to newer Redisson
  sources and are absent from the 4.6.1 jar, so they are recorded as
  version-out rather than implemented.
- Tests: 2 mock + `test/live-phase48.test.ts` 1/1 (BRPOP priority/source
  name/negative + empty timeout, deque path, limited/pattern scans).
- Stabilization: the two keyspace-config tests (phase 31/47) now serialize
  their `notify-keyspace-events` sections with a Redis test lock, removing a
  parallel-restore race that could drop `new`/`set` events.

## 2.25.0 — 2026-10-21

Phase 47: RKeys global keyspace listeners. Full suite:
**271 tests / 50 files**.

- **`RKeys.addListener` / `removeListener`**: Java 4.6.1 mappings
  `onNew` → `__keyevent@*:new`, `onSet` → `:set`, `onExpired` → `:expired`,
  `onDeleted` → `:del`. Callbacks receive the actual key name (global hub, no
  per-structure filtering).
- The listener hub is shared per Redis client, so removal works through any
  `getKeys()` facade. Unknown ids are ignored; ids are idempotent on removal.
- `onFlush` throws the Java RESP3 error (`protocol ... RESP3`) because this
  client negotiates RESP2; unregistered/empty listeners throw
  `Unsupported ObjectListener`.
- Tests: 1 mock + `test/live-phase47.test.ts` 1/1 (new/set/expired/del with
  `KEAn`, removal through a fresh facade, config saved/restored; logs/skips
  when `CONFIG SET` or the `n` flag is unavailable).

## 2.24.0 — 2026-10-21

Phase 46: RKeys migrate/database operations. Full suite:
**269 tests / 49 files**.

- **`migrate(name, host, port, database, timeoutMs)`**: single-key MIGRATE.
- **`migrate(MigrateArgs)`**: multiple `KEYS`, optional `COPY`/`REPLACE`,
  `AUTH` or `AUTH2` credentials.
- **`copy(name, host, port, database, timeoutMs)`**: remote MIGRATE ... COPY
  (source retained); the existing local `copy(src, dest, replace?)` overload
  is unchanged and selected by the boolean third argument.
- **`swapdb(db1, db2)`**, **`flushdbParallel()`** / **`flushallParallel()`**
  (`FLUSHDB`/`FLUSHALL ASYNC`).
- Tests: `test/live-phase46.test.ts` 1/1 with a second Redis on 6380
  (`REDIS_MIGRATE_URL` overrides): single/multi-key migrate, remote copy,
  COPY+REPLACE, SWAPDB 10/11, FLUSHDB ASYNC on db12 and FLUSHALL ASYNC on the
  target; migration assertions log-skip when the second server is absent.

## 2.23.0 — 2026-10-21

Phase 45: blocking-queue element subscriptions. Full suite:
**268 tests / 48 files**.

- **`RBlockingQueue` / `RBlockingDeque`**: `subscribeOnElements(consumer)` and
  `unsubscribe(listenerId)` (Java `ElementsSubscribeService` semantics):
  continuous take → consumer → take again, async consumers provide
  back-pressure, consumer failures retry after one second, registering the
  same consumer twice throws, unknown unsubscribe ids are ignored.
- Cancellation aborts the dedicated BLPOP/BRPOP connection, so
  `unsubscribe` stops a blocked listener immediately instead of waiting for
  the next element. Mock clients without dedicated connections fall back to
  signal-aware polling.
- **`RBoundedBlockingQueue`**: subscriptions run through the capacity-aware
  poll, releasing one permit per delivered element.
- Tests: 2 mock + `test/live-phase45.test.ts` 1/1 (order, back-pressure,
  prompt cancellation leaving a later element queued, deque delivery, bounded
  capacity restoration).

## 2.22.0 — 2026-10-20

Phase 44: RSearch 4.6.1 hybrid/profile/cursor/alter. Full suite:
**265 tests / 47 files**.

- **`hybridSearch(index, params)`**: FT.HYBRID with SEARCH + VSIM (KNN with
  `K`/`EF_RUNTIME`/`YIELD_DISTANCE_AS`/`SHARD_K_RATIO`, or RANGE with
  `RADIUS`/`EPSILON`), RRF/LINEAR combine, `load`, `groupBy`/`reduce`, `apply`,
  `filter`, `sortBy`/`noSort`, `limit`, `params` (Buffer-safe) and `timeoutMs`.
  Returns `{ total, results, warnings, executionTime }`.
- **`profileSearch` / `profileAggregate`**: FT.PROFILE ... QUERY with optional
  `LIMITED`; returns the inner result plus the flat profile map
  (`Shards`/`Coordinator`).
- **Cursors**: `aggregate(..., { withCursor })` now returns the cursor id, and
  `readCursor(index, id[, count])` / `delCursor` wrap FT.CURSOR READ/DEL.
- **`alter(index, schema, skipInitialScan?)`**: FT.ALTER ... SCHEMA ADD with
  the same raw schema tokens as `createIndex`.
- Redis 8.6 rejects zero-argument `COMBINE RRF/LINEAR` (Java 4.6.1 sends
  `RRF 0`); TS omits COMBINE when no options are set, or materialises the
  documented defaults when only a score alias is requested.
- Tests: `test/live-phase44.test.ts` 1/1 (hybrid KNN/LINEAR + RRF/RANGE,
  profiles, cursors, alter; capability-gated).

## 2.21.0 — 2026-10-20

Phase 43: RJsonBucket 4.6.1 multi-match surface. Full suite:
**264 tests / 46 files**.

- **`RJsonBucket`**: all `*Multi` variants — `stringSizeMulti`,
  `stringAppendMulti`, `arrayAppendMulti`, `arrayIndexMulti` (with and without
  `[start, stop)`), `arrayInsertMulti`, `arraySizeMulti`, `arrayPopMulti`,
  `arrayPollFirstMulti`, `arrayPollLastMulti`, `arrayTrimMulti`,
  `incrementAndGetMulti`, `countKeysMulti`, `getKeysMulti` (nested key lists)
  and `toggleMulti` (nulls preserved for non-boolean matches).
- **Multi-path read**: `get(codec, ...paths)` mirrors Java's
  `JSON.GET key path...` and decodes with a per-call codec (RESP2 behaviour:
  no paths returns the document).
- **Fix**: the two-argument `arrayIndex(path, value)` now omits start/stop so
  Redis searches the whole array; previously it sent `0,0` and only checked
  the first element. The four-argument overload still sends `[start, stop)`.
- Tests: `test/live-phase43.test.ts` 1/1 (JSON capability-gated; all Multi
  variants + multi-path get + arrayIndex boundaries).

## 2.20.0 — 2026-10-20

Phase 42: RMapCacheNative fast-write family. Full suite:
**263 tests / 45 files**.

- **`RMapCacheNative`**: `fastPut(key, value, ttl|Date)` (HSET without reading
  the old value; returns whether the field was newly added), `fastPutIfAbsent`
  (HSETNX / atomic Lua with per-field TTL), `putIfExist` (write only when the
  field exists, returns the previous value), `fastPutIfExist` (boolean form;
  TS extension — the 4.6.1 jar stops at `fastPutIfAbsent`). All accept relative
  milliseconds or an absolute `Date` (HPEXPIRE/HPEXPIREAT), with JS fallbacks
  on servers without the field-expiry family.
- Refactored the shared `applyFieldExpiry` helper used by `put` and the fast
  paths.
- Tests: 1 mock + `test/live-phase42.test.ts` 1/1 (existence guards, relative
  and absolute field TTLs, Java `map-get` reads).

## 2.19.0 — 2026-10-20

Phase 41: RQueue/RDeque 4.6.1 surface. Full suite:
**261 tests / 44 files**.

- **`RQueue`**: `poll(count)` overload (LPOP with count, fallback loop),
  `indexOf(value)` (encoded comparison) and `pollLastAndOfferFirstTo(dest)`
  (non-blocking RPOPLPUSH; the blocking queue/deque overloads with a timeout
  remain available).
- **`RDeque`**: variadic `addFirst(...)` / `addLast(...)` returning the new
  length, `addFirstIfExists` / `addLastIfExists` (LPUSHX/RPUSHX with
  fallback), `pollFirst(count)` / `pollLast(count)` and `move(dest, from, to)`
  (LMOVE).
- **`RBoundedBlockingQueue`**: `poll(count)` overload preserving capacity
  accounting (loops through the semaphore-aware single poll).
- Tests: 2 mock + `test/live-phase41.test.ts` 1/1 (RPOPLPUSH, variadic
  writes, LPUSHX/RPUSHX on absent lists, LMOVE, count polls).

## 2.18.0 — 2026-10-20

Phase 40: JSON bucket batches + native map-cache conditionals. Full suite:
**258 tests / 43 files**.

- **`RJsonBuckets`**: `set(Map, path?)` (JSON.MSET, default path `.`),
  `get(...keys)` / `getWithPath(path, ...keys)` (JSON.MGET; null replies are
  omitted from the result map) and `bucket(name)` now forwards an optional
  codec.
- **`RMapCacheNative`**: `putIfAllKeysExist` / `putIfAllKeysAbsent`
  (atomic all-fields condition via Lua: HEXISTS guards then HSET +
  HPEXPIRE/HPEXPIREAT), `expireEntries(keys, ttl|Date[, mode])` and
  `expireEntriesIfNotSet/IfGreater/IfLess` batch helpers.
- **FPHA note**: Java 4.6.1 `FPHAType` (BF16/FP16/FP32/FP64) is not supported
  by the local RedisJSON build (`JSON.SET … FPHA …` → syntax error); recorded
  as a version-dependent gap instead of silently emulating it.
- Tests: 1 mock + `test/live-phase40.test.ts` 2/2 (JSON.MSET/MGET round-trip
  and path reads; conditional batch + Date expiry with Java `map-get` reads).

## 2.17.0 — 2026-10-20

Phase 39: RReliableTopic 4.6.1 semantics. Full suite:
**255 tests / 42 files**.

- **One subscriber group per client instance** (Java `RedissonReliableTopic`):
  all local listeners share one XGROUP created at `0`; `subscriberId` is
  generated once per instance. `countListeners()` reports local listeners.
- **`publish` returns the subscriber count** (number of groups) with Java's
  exact Lua: `XADD` then `XINFO GROUPS` and `#groups` — 0 when nobody is
  subscribed.
- **Heartbeat + trim**: each poll cycle ZADDs the subscriber's timeout score
  (`now + 120s`), destroys groups whose timeout expired, and trims the stream
  to the earliest group's last-delivered id (4.6.1 maintenance Lua port);
  acknowledged entries leave the stream automatically.
- **Lifecycle**: removing the last local listener destroys the group and
  removes the heartbeat; `removeAllListeners`, `delete` and `unlink` do the
  same. Existing phase 3/17 reliable-topic interop tests stay green.
- Tests: `test/live-phase39.test.ts` 1/1 (publish counts 0/1/2, shared vs
  separate groups, trim to empty, per-instance teardown).

## 2.16.0 — 2026-10-20

Phase 38: lazy iterators + preload bound. Full suite:
**254 tests / 41 files**.

- **`RSet`**: `iterator(pattern?, count?)` (lazy SSCAN), plus
  `distributedIterator` and `stream` aliases. MATCH applies to encoded
  member names (same convention as HSCAN/ZSCAN).
- **`RScoredSortedSet`**: `iterator(pattern?, count?)` / `stream` over values
  and `entriesIterator(pattern?, count?)` over `{value, score}` via ZSCAN
  (mock falls back to a full range for `*`).
- **`RLocalCachedMap.preloadCache(count?)`**: `count > 0` fills at most that
  many entries with HSCAN; omitted/0 keeps the previous full-preload
  behaviour.
- Tests: 2 mock + `test/live-phase38.test.ts` 1/1 (SSCAN/ZSCAN with encoded
  prefix MATCH, preload count vs full).

## 2.15.0 — 2026-10-20

Phase 37: RSearch expansion (RediSearch in Redis 8). Full suite:
**251 tests / 40 files**.

- **Index metadata**: `getIndexes` (FT._LIST), `hasIndex`, `info` (FT.INFO
  flat record), `dropIndexAndDocuments` (DROPINDEX DD).
- **Aliases**: `addAlias` / `delAlias` / `updateAlias`
  (FT.ALIASADD/ALIASDEL/ALIASUPDATE).
- **Enhanced search**: `sortBy`/`sortAsc`, `returnFields`, `inKeys`, raw
  `FILTER`, query `params` + `dialect`, `timeoutMs`, `noContent`, `limit`
  (same RESP2/RESP3 result parsing as before).
- **Aggregation**: `aggregate(index, query, {load, groupBy, reduce, apply,
  sortBy, limit, params, dialect, rawArgs})` → `{ total, rows }`
  (FT.AGGREGATE).
- **Synonyms / dictionaries**: `updateSynonyms` (FT.SYNUPDATE),
  `dumpSynonyms` (SYNDUMP → term→groups), `addDict`/`delDict`/`dumpDict`.
- **Config / spellcheck**: `getConfig`/`setConfig` (FT.CONFIG) and
  `spellcheck` (FT.SPELLCHECK → term→suggestions·score).
- Redis 8 ships the search module; the live test capability-gates on
  `COMMAND INFO FT.CREATE` and skips with a log otherwise.
- Tests: `test/live-phase37.test.ts` 1/1 (index/search/sort/params/alias/
  aggregate/synonyms/dicts/config/drop-with-docs).

## 2.14.0 — 2026-10-20

Phase 36: probabilistic deep APIs. Full suite: **250 tests / 39 files**.

- **`RTDigest`**: `create(compression?)` (COMPRESSION), `mergeWith(...names)`
  (TDIGEST.MERGE), `getMin`/`getMax`, `cumulativeProbability` (CDF),
  `trimmedMean`, `rank`/`revRank`, `byRank`/`byRevRank`, `getInfo` (flat
  Compression/Capacity/weights records) and `reset`.
- **`RCuckooFilter`**: `init(capacity)` alias (CF.RESERVE), `addIfAbsent`
  (CF.ADDNX), `existsMany` (CF.MEXISTS → present subset), `count` (CF.COUNT)
  and `getInfo` (CF.INFO), on top of add/exists/remove.
- **`RTopK`**: `init(k[, width, depth, decay])`, `add(...)` now returns the
  evicted item per input (null when none), `incrementBy` (TOPK.INCRBY, array
  unwrap), `contains`/`count`, `listWithCount` (WITHCOUNT → Map) and `getInfo`.
- Redis 8 modules supply all commands; live tests capability-gate on
  `COMMAND INFO` and skip with a log when unavailable.
- Tests: `test/live-phase36.test.ts` 3/3 (TDigest statistics+merge+reset,
  CuckooFilter counts/info, TopK dropped items/listWithCount).

## 2.13.0 — 2026-10-20

Phase 35: RJsonBucket (Redis 8 built-in JSON module). Full suite:
**247 tests / 38 files green** (new live suite).

- **Document/path API**: `set(value, path?, {nx, xx})`, `get(path?)`
  (no path = whole document; `$` paths return RedisJSON match arrays),
  `delete`, `setIfAbsent`/`trySet`/`setIfExists` (root or explicit path),
  atomic `compareAndSet` (root/path, `$` expectations compared against the
  match array like Java's path CAS), `getAndSet` and `getAndDelete` (root/path
  Lua scripts).
- **String/array/number ops**: `stringSize`/`stringAppend`, `arrayAppend`/
  `arrayIndex`/`arrayInsert`/`arraySize`/`arrayPop`/`arrayPollFirst`/
  `arrayPollLast`/`arrayTrim`, `incrementAndGet` (JSON.NUMINCRBY),
  `merge`, `clear`, `countKeys` (OBJLEN), `getKeys` (OBJKEYS), `toggle`,
  `getType`.
- **Wire note**: plain JSON is the default (matches Java `JacksonCodec`, which
  writes `{"a":1}` without `@class`); an optional `Codec` can be supplied to
  the factory for typed documents. Java 4.6.1 `json-set`/`json-get` probes
  (added to TsProbe, `JacksonCodec<Map>`) verify both read directions.
- Redis 8.6 ships the JSON module; the live test capability-gates on
  `COMMAND INFO JSON.SET` and skips with a log otherwise.
- Tests: `test/live-phase35.test.ts` 1/1 (full API + Java round-trip).

## 2.12.1 — 2026-10-20

Phase 34: stabilization & documentation audit (no behavior changes).

- **Flake audit**: three consecutive full-suite runs, all green —
  **246/246 tests · 37 files** at 82.15 s, 71.40 s and 71.62 s (including the
  live Java/Redis/Cluster suites). No retries or threshold changes were
  needed; previous phases' 60–90 s spread is load variance, not flakiness.
- **Docs consistency**: README_EN test-count comment (188 → 246); PRO-only
  lists now include `RLeasedMap` (README_EN/COMPATIBILITY/AGENTS); MapCache
  native absolute `Date` expiry and RBitSet batch/`asBitSet` surfaced in the
  feature lists; RSearch/RedisJSON module dependency documented as
  capability-gated (missing module → command error/test skip, never silent).
- **Final boundary list**: design differences (CommonOptions/Class<M>/
  LocalCachedMap loader-writer) and PRO-only structures are now recorded in
  GAP §4 and the compatibility matrix; no remaining §3 roadmap items.

## 2.12.0 — 2026-10-20

Phase 33: final audit + remaining real gaps. Full suite:
**246 tests / 37 files green in ~82 s**.

- **`RBitSet`**: batch `get(...bits)` (pipelined GETBIT), `set(number[]|Set,
  value)` and `set(from, to, value)` range overload, `asBitSet()` (pipelined
  GETBIT so mock and real servers agree on ordering).
- **`RFunction`**: `kill`, `list([libraryName])` (typed library info),
  `stats`, `dump` (buffer), `restore` / `restoreAndReplace` /
  `restoreAfterFlush`; fixed `load()` sending an empty argument when
  `replace=false`.
- **`RVectorSet`** (Redis 8): `size`, `dimensions` (VDIM), `getVector` (VEMB),
  `contains`, `random`/`random(count)` (VRANDMEMBER), `range` (VRANGE;
  count is positional in 8.6), `getNeighbors` (VLINKS), `getInfo` (VINFO),
  `getAttributes`/`setAttributes` (VGETATTR/VSETATTR + codec).
- **`RMapCacheNative`**: absolute field expiry — `put`/`putIfAbsent`/
  `expireEntry*` accept `Date` and use HPEXPIREAT.
- **`RDelayedQueue`**: `readAll()` (destination snapshot after catch-up) and
  `poll(count)` overload.
- **GAP audit**: removed false positives — `RSet` countUnionApprox/countDiff
  and zset `ZRANGESTORE` do not exist in 4.6.1; `RDelayedQueue` `move`/
  `addListener` are not in the 4.6.1 interface; `RLeasedMap` has no
  open-source implementation (PRO-only); `CommonOptions`/`Class<M>` are design
  differences (factories take names; codec decodes). `RLocalCachedMap`
  loader/writer is a documented no-port decision (external-store semantics,
  no wire impact).
- Tests: 2 mock + `test/live-phase33.test.ts` 3/3 (script flush/kill,
  function dump/restore/list/stats, VectorSet deep API, absolute Date expiry).

## 2.11.0 — 2026-10-20

Phase 32: API polish. Full suite: **241 tests / 36 files green in ~79 s**.

- **`RKeys.getKeysStream(pattern?, count?)` / `getKeysStreamByPattern`** —
  lazy `AsyncGenerator<string>` over SCAN (no full key materialization).
- **`RTopic` / `RShardedTopic` / `RPatternTopic` `removeListener(handler)`** —
  Java's `removeListener(MessageListener)` overload: removes the registered
  entries whose callback is the given function (ids still supported).
- **`RStream.readGroupMany` per-stream groups** — new overload
  `readGroupMany({ stream: { group, consumer, id? } }, count?)`: streams are
  bucketed by group+consumer and read with one XREADGROUP per bucket, matching
  Java's per-stream `StreamMultiReadGroupArgs` semantics.
- Design notes recorded in GAP/COMPATIBILITY: `CommonOptions` is not ported
  (TS factories take names directly; ioredis owns invocation options);
  `Class<M>` type gating is unnecessary (codec decodes; TS has no runtime
  classes).
- Tests: 2 mock + `test/live-phase32.test.ts` 2/2 (lazy key stream,
  removed-handler silence, per-stream group reads incl. empty second pass).

## 2.10.0 — 2026-10-20

Phase 31: keyspace ObjectListener + multi-stream group read. Full suite:
**237 tests / 35 files green in ~81 s**.

- **`RStream.addListener(ObjectListener)`** — Java's keyspace-notification
  surface: subscribes `__keyevent@*:xadd|xdel|xgroup-create|xgroup-destroy|
  xgroup-createconsumer|xgroup-delconsumer` over a duplicate connection,
  filters events by this stream name and invokes `onAdd`/`onRemove`/
  `onCreateGroup`/`onRemoveGroup`/`onCreateConsumer`/`onRemoveConsumer`.
  `removeListener(id)` / `removeAllListeners()` unsubscribes cleanly.
- **`RTimeSeries.addListener(ObjectListener)`** — `zadd`/`zrem` keyevents on
  the series key (Java uses ScoredSortedSet listeners for its ZSET backing).
- **`RStream.readGroupMany(group, consumer, streams, count)`** — one
  `XREADGROUP GROUP … COUNT … STREAMS k1 k2 … id1 id2 …` returning
  `Map<stream, StreamEntry[]>`.
- Server dependency (same as Java): keyspace notifications require
  `notify-keyspace-events` with K+E (e.g. `KEA`). The live test saves/restores
  the setting and skips with a log when `CONFIG SET` is refused; mock only
  verifies listener lifecycle (no events are emitted).
- Tests: 1 mock + `test/live-phase31.test.ts` 2/2 (all six stream events,
  removed-listener silence, TimeSeries add/remove events, multi-stream
  XREADGROUP incl. empty second read).

## 2.9.0 — 2026-10-20

Phase 30: map-cache compute + topic status listeners. Full suite:
**234 tests / 34 files green in ~88 s**.

- **`RMapCache.compute(key, ttlMs, fn)`** — per-key distributed lock
  (`getLock(key)` with a unique per-call thread id so concurrent computes in
  one Node process serialize), read, apply, then `fastPut(..., ttl)` or remove
  when the function returns `null` (Java `BiFunction` semantics).
- **`RMapCache.computeIfAbsent(key, ttlMs, fn)`** — same locking, returns the
  existing value when present, otherwise `putIfAbsent` and returns the winner.
  Results are Java-readable (`mc-get` cross-check) and carry per-entry TTL.
- **`RTopic`/`RShardedTopic` StatusListener** — `addStatusListener({onSubscribe,
  onUnsubscribe})`: fires `onSubscribe(channel)` on registration and
  `onUnsubscribe(channel)` on removal; `removeListener(id)` removes either
  message or status listeners, `countListeners()` counts both,
  `removeAllListeners()` clears both.
- Tests: 2 mock + `test/live-phase30.test.ts` 2/2 (concurrent compute
  serialization + Java `mc-get`; status callbacks live).

## 2.8.0 — 2026-10-20

Phase 29: `rwlock_timeout` protocol (read-lock interop fix). Full suite:
**230 tests / 33 files green in ~69 s**.

- **`RReadWriteLock` read locks** now create Java's per-acquisition companion
  keys `<tagged-name>:<clientId>:<threadId>:rwlock_timeout:<n>` on every
  acquire/re-entry (SET 1 + PX lease), delete the released acquisition's key on
  unlock, and recompute the lock HASH TTL from the remaining timeout keys
  (`max PTTL`) instead of blindly re-applying the lease. The watchdog renews
  every timeout key together with the HASH.
- **Why it matters**: the Java 4.6.1 read-unlock script scans those keys to
  decide whether other readers still hold the lock. Without them, a Java
  release could delete the whole HASH while another reader (e.g. a TS client)
  still holds it. Live phase 29 reproduces the mixed scenario: Java holds a
  read lock, TS acquires/releases a second read lock, and the Java-held HASH
  plus its timeout key must survive (then vanish after Java's own release).
- **Mock note**: ioredis-mock's Lua renders numeric concatenation as `2.0`
  (real Redis Lua gives `2`); the mock test asserts key patterns, while the
  live test pins the real `:1` suffix.
- Tests: 1 mock + `test/live-phase29.test.ts` 1/1 (reentrant/second-holder TTL
  recomputation; mixed Java+TS read lock survival).

## 2.7.0 — 2026-10-19

Phase 28: interruptible lock + map-cache remainder. Full suite:
**228 tests / 32 files green in ~72 s**.

- **`RLock.lockInterruptibly(leaseTimeMs?, signal?, threadId?)`** — Java
  `lockInterruptibly()` analogue: blocks until acquired, but an `AbortSignal`
  aborts the wait with an `AbortError` (TS has no thread interruption). A
  pre-aborted signal never acquires; holders are unaffected.
- **`RMapCache`**: `expireEntries(keys, ttlMs, maxIdleMs?)` (batch expiry,
  returns updated count), `getWithTTLOnly(key)` (decode without refreshing
  idle/access bookkeeping), `getMaxSize()` (reads `redisson_options`
  max-size). `setMaxSize(size, mode?)` was already present.
- Note: `RLock` drift was re-audited against the 4.6.1 jar — `RedissonRedLock`
  inherits `RedissonMultiLock` and performs no drift subtraction (it re-applies
  the full lease at the end); the TS implementation already mirrors that, so
  no code change was needed. GAP wording corrected.
- Tests: 2 mock + `test/live-phase28.test.ts` 2/2 (contended abort across two
  clients; TTL ZSET deadlines from `expireEntries`, TTL-only reads,
  max-size persistence).

## 2.6.0 — 2026-10-19

Phase 27: map views + time-series streaming. Full suite:
**224 tests / 31 files green in ~68 s**.

- **`RMap`**: materialized Java views `keySet()`, `values()`, `entrySet()` with
  the `(count)` and `(pattern, count)` overloads (backed by the lazy HSCAN
  iterators; MATCH applies to encoded field names, same as Java), plus
  `readAllEntrySet()`. `replace(key, value)` and the Java three-argument
  `replace(key, oldValue, newValue)` are now both available
  (`replaceWithCheck` kept as an alias).
- **`RTimeSeries`**: `stream(count?)` and `iterator(count?)` async generators
  over values in ascending timestamp order, page-by-page (index ZRANGE,
  TTL-filtered, `count` = page size); no full-series materialization.
- Tests: 2 mock + `test/live-phase27.test.ts` 2/2 (views/pattern vs Java HSCAN,
  conditional replace/remove Java reads, stream/iterator vs Java ts-range).

## 2.5.0 — 2026-10-19

Phase 26: remaining topic/stream/collection surfaces. Full suite:
**220 tests / 30 files green in ~66 s**.

- **`RTopic`**: `getChannelNames()` and `countListeners()` (local listeners;
  `countSubscribers()` already used PUBSUB NUMSUB). **`RPatternTopic`**:
  `getPatternNames()` and `getActiveTopics()` (PUBSUB CHANNELS matching the
  pattern). **`RShardedTopic`**: `getChannelNames()`, `countListeners()` and
  `countSubscribers()` (PUBSUB SHARDNUMSUB, NUMSUB fallback, then local count).
- **`RStream`**: `fastClaim` (XCLAIM … JUSTID → id list), `fastAutoClaim`
  (XAUTOCLAIM … JUSTID → cursor + ids) and `readMany` (one `XREAD COUNT …
  STREAMS k1 k2 … id1 id2 …` returning `Map<stream, entries>`).
- **`RSet`**: `addAllCounted` / `removeAllCounted` (SADD/SREM counts) and
  `containsEach` (SMISMEMBER → subset that IS present), matching Java method
  names.
- Tests: 2 mock + `test/live-phase26.test.ts` 2/2 (live listener/subscriber
  counts, active topics, XCLAIM/XAUTOCLAIM JUSTID, multi-stream XREAD).

## 2.4.0 — 2026-10-19

Phase 25: `RTimeSeries` read/delete family. Full suite: **216 tests / 29
files green in ~68 s**.

- **Writes**: `addAll(entries, ttlMs?)` — one atomic Lua pass (sequence ids +
  data ZSET + TTL ZSET, same 4.6.1 member shape as `add`); mock falls back to
  per-entry `add`.
- **Reads**: `getEntry` / `getValue` (Java `get` shape), `first` / `last`
  (+`Entry`/`Timestamp`/`Entries`/count overloads), `range` / `rangeReversed`
  with an optional limit, `entryRange` / `entryRangeReversed` aliases.
  All reads filter expired entries via `redisson__ts_ttl` without deleting
  them (Java lazy semantics).
- **Removals**: `getAndRemove` / `getAndRemoveEntry` (atomic Lua, label shape
  branching for n=2/3/4), `remove(timestamp)` (both ZSETs), `removeRange`
  (expiry-aware Lua, live entries only), `pollFirst` / `pollLast`
  (+`Entry`/`Entries`/count overloads, atomic Lua).
- **Label compatibility**: new Lua readers branch on the outer type byte —
  n==4 reads the mark byte (newer Java), n==2 is unlabeled, n==3 is the
  unprefixed codec-encoded label written by 4.6.1. Java label round-trips in
  both directions are covered live.
- Tests: 2 mock + `test/live-phase25.test.ts` 2/2 (addAll + Java `ts-range` /
  `ts-add-label` round-trip, poll/getAndRemove, removeRange, TTL expiry
  filtering).

## 2.3.0 — 2026-10-19

Phase 24: `RScoredSortedSet` remainder. Full suite: **212 tests / 28 files
green in ~68 s**.

- **`addAll` family**: `addAll(Map<V, Number>)`, `addAllIfAbsent` (NX CH),
  `addAllIfExist` (XX CH), `addAllIfGreater` (GT CH), `addAllIfLess` (LT CH) —
  return the changed/added count, scores cross-checked through Java.
- **Reads**: `firstEntry()` / `lastEntry()` (WITHSCORES), `random()` /
  `random(count)` (ZRANDMEMBER, negative count allows repeats), `randomEntries
  (count)` (ZRANDMEMBER ... WITHSCORES → member→score).
- **Non-blocking pops**: `pollFirstEntry()` / `pollLastEntry()` (ZPOPMIN/MAX).
  **Blocking**: `pollFirstWithTimeout(ms)` / `pollLastWithTimeout(ms)`
  (BZPOPMIN/BZPOPMAX) and `pollFirstFromAny` / `pollLastFromAny` (BZMPOP
  MIN/MAX) returning the source key + member + score; `null` blocks forever,
  `<= 0` is non-blocking, dedicated connection + float-second timeouts, polling
  fallback for mock.
- **Modern range commands**: `rangeByScore`, score-based `entryRange`,
  `valueRangeReversed` and `entryRangeReversed` now send
  `ZRANGE ... BYSCORE [REV]` instead of the deprecated
  `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE` (same results; mock implements BYSCORE).
- Tests: 3 mock + `test/live-phase24.test.ts` 2/2 (GT/LT/NX/XX addAll + Java
  score reads, ZRANDMEMBER membership, BZPOPMIN/BZPOPMAX/BZMPOP wake-ups).

## 2.2.0 — 2026-10-19

Phase 23: operations & scalar remainder. Full suite: **207 tests / 27 files
green in ~64 s**.

- **`RKeys`**: `expire(key, ttlMs)` now has the Java multi-key overload
  (`expire(ttlMs, ...keys)` → count), same for `expireAt`; added
  `clearExpire` (PERSIST), `remainTimeToLive` (PTTL, `-2`/`-1`/ms),
  `getSlot` (client-side CRC16-XMODEM, hash-tag aware — Java delegates to
  `CLUSTER KEYSLOT`), `move(key, db)` (MOVE), `unlinkByPattern`
  (SCAN + UNLINK with DELETE fallback).
- **`RAtomicLong` / `RAtomicDouble`**: `getAndIncrement` / `getAndDecrement`,
  Long/Double `setIfLess` / `setIfGreater` (single Lua, redi.go scripts),
  Double `incrementAndGet` / `decrementAndGet` / `getAndDelete` /
  `compareAndDelete`.
- **`RBuckets`**: `setIfAllKeysExist` / `setIfAllKeysAbsent` via `MSETEX …
  NX|XX [PX ms]` (Redis 8+, same command as Java 4.6.1); without MSETEX the
  call fails loudly with a capability message instead of degrading.
- **`RScoredSortedSet`**: `addIfExists` (ZADD XX CH), `addIfLess` (LT CH),
  `addIfGreater` (GT CH) alongside the existing `addIfAbsent` (NX) — exact
  Java 4.6.1 command shapes; scores cross-checked through Java reads.
- Tests: 4 mock regressions + `test/live-phase23.test.ts` 4/4 (expire family /
  MOVE to db 9 / slots vs a live cluster node's `CLUSTER KEYSLOT` / MSETEX
  NX+XX written by TS and read by Java / atomic exactness incl. `0.1+0.2` on
  the wire / ZADD GT/LT scores read back by Java).
- Test infra: the resident-probe recompile now uses a shell command
  (`execSync`) — Node ≥ 20.12 refuses to `execFile` a `.cmd` without a shell
  (EINVAL), which surfaced the first time TsProbe changed on this Node.

## 2.1.0 — 2026-10-19

Phase 22: closes every remaining row of 演进方案 §3 (API depth). Full suite:
**199 tests / 26 files green in ~65 s**.

- **`RFencedLock.tryLockAndGetToken(waitTimeMs?, leaseTimeMs?, threadId?)`** —
  Java 4.6.1 shape (omitted/zero `waitTimeMs` = single non-blocking attempt,
  otherwise wait up to the deadline). Token sequence cross-checked with the
  `fenced-token` Java probe: TS `n` → Java `n+1` → TS `n+2`.
- **`RBlockingQueue` / `RBlockingDeque` blocking ops**:
  `pollFromAny` / `pollFromAnyWithName` (BLPOP over this queue + names in
  argument order), `pollFirstFromAny` / `pollLastFromAny` (BLMPOP + `COUNT`,
  result keyed by source queue), `pollLastAndOfferFirstTo` /
  `takeLastAndOfferFirstTo` (BLMOVE RIGHT→LEFT); deque `move` (LMOVE) and
  `moveWithTimeout` (BLMOVE). Blocking paths use `duplicate()` + float-second
  Redis timeouts; unsupported mock commands fall back to polling. `timeout 0`
  is a non-blocking scan, `null` blocks forever.
- **`RRateLimiter` keepAlive**: renewal moved into the 4.6.1 acquire script —
  a successful acquire PEXPIREs config/value/permits to `keepAliveTime`;
  with keepAlive 0 but a TTL on the config HASH, companions inherit that TTL
  (same `else` branch as Java). Verified by interleaving TS and Java acquires:
  both renew the shared keys.
- **`RLocalCachedMap`**: `evictionPolicy: 'LRU' | 'LFU'` is now actually
  enforced under `maxSize` (LRU = oldest `lastAccess`; LFU = fewest accesses,
  ties by oldest access). `getCachedMap()` returns a snapshot of the live
  near-cache with decoded keys and stale entries dropped.
- **`RMultimapCache`**: opt-in `enablePeriodicEviction(intervalMs)` /
  `disablePeriodicEviction()` plus public `evictExpired()` — removes child
  collection + HASH field + TTL ZSET member in one sweep. Default stays lazy.
- **`RBatch`**: six new typed facades (13 total): `getDeque`,
  `getAtomicDouble`, `getBitSet`, `getGeo`, `getStream`, `getMapCacheNative`.
  `BatchAtomicDouble.getAndAdd` returns the float-exact old value from one Lua
  script; native map-cache `put`/`fastPut` support field TTL via
  `HPEXPIRE ... FIELDS`.
- Tests: 6 mock regressions + `test/live-phase22.test.ts` 5/5 (fenced token
  sequence vs Java, real BLPOP/BLMPOP/BLMOVE wake-ups, keepAlive renewal by
  both implementations, periodic multimap sweep without a read, batch wire
  incl. GEO and `HPEXPIRE`/`HPTTL`). `live-interop` gained a 60 s `beforeAll`
  timeout so JVM cold-start jitter cannot flake the full parallel suite.

## 2.0.0 — 2026-10-19

Phase 21: modern packaging (ESM + CJS), a real Redis Cluster test pass, and
the delayed-queue background migrator. Full suite: **188 tests / 25 files
green in ~60 s**.

- Packaging: dual-format build (`tsc` ×2) — `dist/cjs` (CommonJS) +
  `dist/esm` (ES modules with `.js` specifiers), per-directory
  `package.json` stubs, `exports` map in `package.json` (`scripts/build.mjs`
  orchestrates). `npm run pack-smoke` packages the tarball, installs it into
  a temp consumer and asserts both `require` and `import` resolve the right
  build and produce identical wire output.
- ioredis typing: `Config.client` now accepts `Redis | Cluster` (exported
  `RedisClient` type). ~87 internal `(client as any).cmd(...)` casts dropped
  so typed calls catch arity/shape mistakes at compile time — the cast-hiding
  was what let the RSet store bug through (below).
- **`RSet` wire fix** (latent since 1.0.0): `union/intersection/diff` store
  variants sent `SOPSTORE dest numkeys self ...` (ioredis varargs treat the
  trailing keys as a flat argv; there is no numkeys and `self` is only the
  *destination*, never an input). Shape is now Java 4.6.1's bare-key form
  `SOPSTORE self others...` — verified by black-box probe against the 4.6.1
  jar (`sinterstore self other1 other2`, `sdiffstore` likewise). Read
  variants keep `self` as first input (that part was correct). Mock + live
  phase-15 tests now use discriminating seeds so a wrong shape cannot pass.
- `RDelayedQueue`: opt-in background migrator ported from Java
  `QueueTransferTask`/`QueueTransferService` — `start()` subscribes to the
  queue channel over a `duplicate()` connection and arms a deadline timer,
  `stop()`/`destroy()` release it (Java `destroy()` = detach only, data
  survives). Scheduling semantics from 4.6.1: push immediately when the next
  deadline is ≤ 10 ms away, otherwise `setTimeout` (timers `unref()`ed); the
  migration Lua now returns the next head deadline (Java's `return v[2]`
  tail) so each push re-arms; errors retry after 5 s. The push-on-subscribe
  catch-up is done explicitly because ioredis `Cluster` re-emits `message`
  but not `subscribe`. `poll()`/`peek()` keep their synchronous catch-up
  migration, so behaviour is unchanged without `start()`. ioredis-mock
  degrades to a no-op (offers already land in the destination there).
- Live tests: phase-14 `nack` (`XNACK`) and phase-15 `countUnion`
  (`SUNIONCARD`) are now capability-gated via `COMMAND INFO` with a logged
  SKIP on older servers (Redis 8.6 lacks both); both verified live against
  Redis 8.10.2 with `REDIS_URL` — the previous "2 environment failures" are
  gone, full suite fully green.
- Cluster: `scripts/cluster-up.sh` / `cluster-down.sh` (WSL, 3 masters on
  ports 7100-7102) + `test/live-cluster.test.ts` (4 tests) running an
  ioredis `Cluster` as the Redisson client — locks with a second client
  contending through slot routing, MapCache TTL/bookkeeping + listener
  events over the cluster bus, the delayed-queue migrator end-to-end, rate
  limiter and topic pub/sub. All keys stay in one slot via the
  `prefixName`/`suffixName` hash-tag rule. Skips with a logged warning when
  no cluster is reachable.

## 1.5.0 — 2026-10-19

Phase 20: collection/bitmap depth APIs plus a resident Java probe REPL
(full live suite 673 s → ~57 s).

- Test infra: `TsProbe` gained a resident `repl` mode (`TsRepl`) so one JVM
  serves every probe call (~5 ms per command instead of an `mvn exec:java`
  boot); all live suites migrated to `test/helpers/java-probe.ts`. Full
  `npm test` went from 673 s to ~57 s with the same semantic waits kept.
- `RMap`: `valueSize` (HSTRLEN), `randomKeys`/`randomEntries` (HRANDFIELD
  [WITHVALUES]) and lazy HSCAN iterators
  `keysIterator`/`valuesIterator`/`entriesIterator(pattern, count)`.
- `RScoredSortedSet`: `firstScore`/`lastScore`, `addScore` (ZINCRBY),
  `addScoreAndGetRank`/`addScoreAndGetRevRank`, `addAndGetRank`/
  `addAndGetRevRank` (single Lua ZADD+rank), `count` (ZCOUNT), `entryRange`
  (index and score forms), `valueRangeReversed`/`entryRangeReversed` (index
  2-arg form and score 4/6-arg form; Java semantics: `startScore` is the
  lower bound and the command is `ZREVRANGEBYSCORE key end start`),
  `removeRangeByRank`/`removeRangeByScore`; `rank`/`revRank` now return
  `number | null` like Java (previously `-1`).
- `RBitSet`: `BitFieldArgs` builder (`get`/`set`/`increment` × signed/
  unsigned, `BitOffset.bit/index`, `overflow`) with `bitField()` returning
  `(number | null)[]` (`null` on OVERFLOW FAIL); typed helpers
  `getSigned`/`setSigned`/`incrementAndGetSigned` and unsigned variants plus
  `getByte/Short/Integer/Long` shortcuts; `size()` = STRLEN×8 and `length()`
  matches the 4.6.1 bitpos Lua (highest set bit + 1 — the in-byte scan
  direction was fixed); `or/and/xor/not` return the result byte length;
  Redis 8 `diff`/`diffInverse`/`andOr`/`setExclusive`
  (`BITOP DIFF/DIFF1/ANDOR/ONE`); `setRange`/`clear(from, to)` are
  `to`-exclusive like Java.
- `RGeo`: `GeoSearchArgs` builder (`from(lon,lat)` / `from(member)`,
  `radius`/`box`, `order`, `count`/`countAny`), `search(args)`,
  `searchWithDistance`, `searchWithPosition`, `storeSearchTo` /
  `storeSortedSearchTo`, single-member `addIfExists` (Lua geopos→geoadd) and
  `addIfExists(GeoEntry...)` (GEOADD XX; Redis only counts added elements, so
  the reply is 0 even when a member moves — tests assert the effect).
  Command selection mirrors Java 4.6.1 (verified via javap): radius →
  `GEORADIUS_RO`/`GEORADIUSBYMEMBER_RO`, box → `GEOSEARCH`, stores →
  `GEORADIUS ... STOREDIST` / `GEOSEARCHSTORE`. 4.6.1 has no polygon search
  (newer versions only), so the ported surface is complete for 4.6.1.
- Verification: 2 new mock tests (map iterators + HRANDFIELD/HSTRLEN;
  scored-set score ops + reversed ranges incl. `entryRange` index form), a
  `length()` regression assertion in the mock bitset test, and
  `live-phase20.test.ts` (4 tests, Redis + Java 4.6.1 probes, both
  directions: map views/random, zset score family/deletion, BITFIELD/BITOP
  incl. the DIFF family, Geo search/stores/XX). Full suite: 182 tests,
  180 passed, 2 known environment failures (`xnack`, `SUNIONCARD` missing on
  local Redis 8.6), 56.9 s.

## 1.4.0 — 2026-10-19

Phase 19: key synchronizers, RW-lock views and map-cache depth APIs.

- New `RedisKeyedStructure` base (`src/synchronizers.ts`): `RMap`, `RSet`,
  `RMapCache(+Native)`, `RSetCache` and the multimaps gain the Java
  `getLock`/`getFairLock`/`getReadWriteLock`/`getSemaphore`/
  `getCountDownLatch`/`getPermitExpirableSemaphore` family. Companion names
  follow Java `suffixName(name, hash128toBase64(encode(key)) + ":" + suffix)`
  with suffixes `lock`/`fairlock`/`rw_lock`/`semaphore`/`countdownlatch`/
  `permitexpirablesemaphore`; verified byte-identical with the Java 4.6.1
  probe. Synchronizers created through a `Redisson` factory inherit the
  client owner id/watchdog (`attachOwner`).
- `RReadWriteLock` views implement `isLocked` (read view: Java
  `mode=='read' or (mode=='write' and hlen>2)`), `isHeldByThread`,
  `getHoldCount` and `forceUnlock`. Unlock/forceUnlock wake-up publishes now
  match Java `LockPubSub`: read → `UNLOCK_MESSAGE` (0, wake one), write →
  `READ_UNLOCK_MESSAGE` (1, wake all) — previously inverted (TS and redi.go).
- `RMultiLock`/`RRedLock` accept any `LockLike` (`tryLock`/`unlock`), so
  `RFairLock` and read/write views can join multi-lock/red-lock orchestration.
- `RMapCache`: `putAll(mapping, ttl, maxIdle)`, `expireEntry` (refuses
  already-expired entries, repacks maxIdle), `remainTimeToLive` (`-2`
  missing/expired, `-1` no expiry, else ms) and entry listeners
  (`addListener`/`removeListener`/`removeAllListeners`). Listeners subscribe
  on a dedicated connection through `messageBuffer` (u64LE length prefixes
  survive binary-safe transport) and set the Java `has-listeners` flag so
  Java writers publish too; published payloads are sent as Buffers because
  ioredis encodes string args as UTF-8.
- `RMapCacheNative`: `putIfAbsent(key, value, ttl)` (base `hsetnx`/`hget`
  script when no TTL; 4.6.1 guard script + `HPEXPIRE ... FIELDS 1` when a TTL
  is given — an existing entry keeps its value and TTL),
  `expireEntry(IfNotSet/IfGreater/IfLess)` (HPEXPIRE
  `[NX|GT|LT] FIELDS 1`), `clearExpire` (HPERSIST `FIELDS 1`) and
  `remainTimeToLive` over HPTTL. Java 4.6.1 answers `null` for HPERSIST `-2`
  (field already gone); TS keeps the `boolean` contract and returns `false`.
- Verification: 3 new wire tests (event codec round-trip/truncation, Java
  golden hash vector), 7 new mock tests + `live-phase19.test.ts` (5 tests,
  Redis + Java 4.6.1: six companion names, key-lock exclusion both ways,
  MapCache events both ways, conditional expiry/HPERSIST both ways).

## 1.3.0 — 2026-10-19

Phase 18: 64-bit precision and atomic-operation hardening.

- `JsonCodec` encodes `bigint` directly as `["java.lang.Long",<decimal>]`
  (including nested objects/arrays) without `Number()` rounding. New optional
  `JsonCodecOptions.bigIntForUnsafeLong` makes `decode` return `BigInt` for
  integer literals outside ±(2^53-1); default stays `number`.
- `RAtomicLong` CAS now compares decimal strings exactly (Java 4.6.1 script,
  including missing-key/expect=0). `LongValue = number | bigint` is accepted by
  set/getAndSet/getAndAdd/addAndGet/compareAndSet/compareAndDelete;
  `getAndAdd`/`addAndGet` use a single Lua round-trip and `get` returns `BigInt`
  for values beyond the safe range.
- `RAtomicDouble.getAndAdd` uses one Lua script (GET old + INCRBYFLOAT) instead
  of client-side `newValue - delta`, removing float tail drift.
- `RBlockingQueue.drainTo` extracts via one atomic Lua `LRANGE`+`LTRIM/DEL`,
  so concurrent tail `RPUSH` is not lost; values are then offered to the
  target queue through its codec. Bounded queues keep their permit-restoring
  Lua variant.
- Verification: 9 new wire/mock tests + `live-phase18.test.ts` (4 tests, Redis
  + Java 4.6.1: Long.MAX/MIN buckets, exact string CAS, AtomicDouble old value,
  drainTo visible in Java; mock includes concurrent tail-appends).

## 1.2.0 — 2026-10-19

Phase 17: bounded-queue semaphore protocol, blocking-connection isolation and
topic subscription lifecycle.

- `RBoundedBlockingQueue` now treats `redisson_bqs:{name}` as available
  permits (Java semantics): `trySetCapacity` SET+PUBLISH; `offer` atomically
  DECRBY+RPUSH; `poll`/`take`/`pollWithTimeout`/`remove`/`clear`/`drainTo`
  INCRBY+PUBLISH `redisson_sc:redisson_bqs:{name}`. `put` and
  `offer(value, timeoutMs)` block until a consumer releases capacity.
  `remainingCapacity()` is the real value; `getCapacity()` is an alias.
  Offering without a capacity now throws like Java (`assert`).
- Blocking reads use dedicated connections (`duplicateRedis` /
  `withDedicatedRedis`) so `BLPOP`/`BRPOP`/`BZPOPMIN`/`XREADGROUP BLOCK`
  (including `RStream.readGroupWith({blockMs})`) cannot stall lock-watchdog
  renewals or normal commands. Timeouts use Redis 6+ float seconds;
  `timeoutMs <= 0` is non-blocking; mock clients without blocking commands
  fall back to short polling. Duplicated connections wait for ioredis's INFO
  ready-check before subscribing (avoids subscriber-mode errors).
- Topics: `RTopic`/`RPatternTopic` attach handlers before SUBSCRIBE;
  `RShardedTopic` stores its subscription connection/handlers and gains
  `removeListener`/`removeAllListeners`; `RReliableTopic` consumes on a
  dedicated connection and disconnects it on removal.
- Verification: 6 new mock tests + `live-phase17.test.ts` (9 tests, Redis +
  Java 4.6.1: shared permits, Java consumer wakes TS put, Java put waits for
  TS poll, BLPOP/BZPOPMIN/XREADGROUP isolation including blocking stream
  reads, topic lifecycle).

## 1.1.0 — 2026-10-18

Phase 16: lifecycle / companion-key hardening.

- `delete()`/`unlink()` now close companion keys instead of deleting only the
  main key: `RFairLock` (queue + timeout ZSET), `RFencedLock` (token),
  `RPermitExpirableSemaphore` (`{name}:timeout`), `RSetCache` (idle ZSET +
  touch HASH), `RBoundedBlockingQueue` (`redisson_bqs`), `RRingBuffer`
  (`redisson_rb`), and `RMultimap`/`RMultimapCache` + native variants (child
  collections + TTL ZSET; HASH is read before deletion).
- `unlink()` now clears companions for structures whose `delete()` already did:
  `RMapCache` (timeout/idle/access/options), `RBloomFilter` (config),
  `RTimeSeries` (ttl/seq), `RRateLimiter` (value/permits), and
  `RReliableTopic` (stream/timeout).
- `RRingBuffer.clear()` keeps Java 4.6.1 semantics (LIST only); `delete()`
  removes capacity too.
- `RDelayedQueue.size()` now reads the staging LIST like Java
  (`redisson_delay_queue:{name}`), not the destination queue; `targetSize()`
  exposes the latter. `delete()` deletes staging + timeout only and preserves
  the caller-owned destination queue.
- `RLocalCachedMap` honours `timeToLiveMs`/`maxIdleMs` in the near cache
  (entries track absolute expiry + last access); `UPDATE` broadcasts use the
  configured TTL. Local reads now wait for the invalidation subscription ack,
  which closes a constructor-time race that could leave a stale entry after a
  concurrent writer (phase 8 LCM test regression). LRU/LFU + cache view remain
  phase 19.
- Verification: 11 new mock lifecycle tests + `live-phase16.test.ts` (7 tests,
  Redis + Java 4.6.1: capacity re-init for BQ/Ring, DQ size/delete, multimap
  cleanup, fenced-token reset, PES/SetCache/FairLock zero residue,
  LocalCachedMap TTL/idle).

## 1.0.0 — 2026-10-14

First stable release. TypeScript 1:1 port of Redisson, wire-compatible with
Java Redisson (`JsonJacksonCodec`, default typing), redi.py and redi.go:
identical Redis key layouts, Lua algorithms and value encoding.

- 60+ structures: locks (`RLock`/`Fair`/`ReadWrite`/`Spin`/`NonReentrant`/
  `Fenced`/`Multi`/`Red`), maps + caches (+Native, LocalCachedMap),
  sets/lists/queues/deques (+Blocking/Bounded), sorted sets (scored + lex),
  multimaps (+Cache/Native), buckets, atomics/adders/generators,
  semaphores/CountdownLatch/rate limiters, topics (plain/pattern/sharded/reliable),
  streams (+consumer groups/pending/claim/trim), delayed queues, ring buffers,
  geo/HLL/bitset/bloom (+native probabilistic), timeseries, keys/script/
  functions/search/vectors/batch (+7 typed batch facades).
- 115 tests green: wire vectors, mock functional, live python interop, and
  live bidirectional suites vs Redisson 4.6.1 (see COMPATIBILITY.md).
- Honest non-goals (documented, same as redi.go): `RSortedSet`-Comparator,
  `RTransaction` snapshot rollback, `LiveObject`, reactive/Rx,
  `RExecutorService`/`RRemoteService` wire protocol, PRO-only structures.
- Prior 1.1.0–1.8.9 entries below were same-day development iterations;
  1.0.0 is the first public release, superseding all of them.

## 1.8.9 — 2026-10-14

Test hygiene + environment findings (no src changes except noted):

- phase12: atomic key added to cleanup (CAS asserts exact values; cross-run
  residue flipped results — caught in full15).
- `countUnion` via `.call` (absent from ioredis typings, like XACKDEL).
- Full-suite note: 2/115 flakes under 18-worker parallel load are JVM
  cold-start starvation (Maven exec with no verdict line), both green in
  isolation. Timing-sensitive asserts keep ≥5s margins per AGENTS.md:10.

## 1.8.8 — 2026-10-14

P1 plain-set set-ops (see `docs/GAP_ANALYSIS.md` §2):

- `RSet.union/diff/intersection` renamed to store-into-self semantics
  (self IS an input — opposite of scored-sorted-set; old read-only methods
  are now `readUnion/readDiff/readIntersection` — Breaking renames);
  command shape `SOPSTORE self (1+N) self others...` with explicit numkeys
  (ioredis typings can't express it, hence `as any`; without numkeys correct
  servers reject); `countUnion` via `.call` (absent from typings like XACKDEL).
- Environment finding: local msys2 Redis 8.10.2 returns 0/[] for
  SINTERSTORE/SDIFFSTORE (raw-RESP verified, reads fine) — inter/diff live
  asserts capability-gated, run on healthy servers (CI official image).

P1 stream remainder (see `docs/GAP_ANALYSIS.md` §2):

- `RStream.addWithOptions/readGroupWith/nack/ackWithDelete` (arg order verbatim
  vs RedissonStream: NOMKSTREAM, IDMP/IDMPAUTO, MAXLEN/MINID [~], LIMIT, id;
  GROUP g c [COUNT] [BLOCK] [CLAIM] [NOACK]; XNACK/XACKDEL; live phase 14
  2/2 PASS incl. XNACK-delivery-count semantics + `.call` fallback for
  untyped ioredis methods).
- Remaining: `fastClaim/fastAutoClaim` variants, multi-stream reads, listeners.

## 1.8.6 — 2026-10-13

P1 stream batch (see `docs/GAP_ANALYSIS.md` §2):

- `RStream` 组运维 + pending/claim 全套：`removeGroup/createConsumer/`
  `removeConsumer/updateGroupMessageId/getPendingInfo/listPending/`
  `pendingRange/claim/autoClaim/getInfo/listGroups/listConsumers`
  （live phase 13 双向 2/2 PASS：SETID 回拨重投、跨语言 claim 交接、
  autoClaim 整组接管）。剩余：`readGroup` 完整版、`add` 精确裁剪、`nack`。

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
