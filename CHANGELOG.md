## 2.54.1 — 2026-10-25

Round 34 of the Java-4.8.0 bidirectional evidence suite (docs+test
only): blocking-wake paths and lock TTL semantics the earlier rounds
did not cover, plus a TTL-slack hardening in the round-30 suite.

Round 34 — `live-interop48ah` (probe commands `bd48-offerfirst/
offerlast/bgpoll/bgresult`, `lock48-trylocklease`): blocking-wake
paths and lock TTL semantics the earlier rounds did not cover.

- RBlockingDeque: all four end × direction blocking wakes — TS parks
  on BLPOP/BRPOP and a Java offer at the OPPOSITE end wakes it (an
  empty deque's single element pops from either side); a Java
  background pollFirst/pollLast(timeout, MS) is woken by TS
  addFirst/addLast.
- RLock lease expiry: a 500ms lease is rejected by the other language
  inside its window and releases to it after expiry, both directions
  (TS lease → Java; Java tryLock(0, 500, MS) → TS).
- RLock watchdog: TS `lock()` renews past a full watchdog cycle
  (30s/3) — TTL stays ~30s vs the ~17.5s unrenewed floor, Java
  `isLocked` holds throughout, TS `unlock()` lets Java acquire
  immediately.
- RStream: TS blocking `readGroupWith({ blockMs })` (dedicated
  connection) parks on XREADGROUP BLOCK until a Java XADD arrives;
  the entry lands in Java's PEL for the TS consumer.
- Test-side hardening: `live-interop48ad`'s four 60s TTL upper
  bounds had no clock-rounding slack and flaked at 60008ms under
  full-suite parallel load — +500ms per law 10.
- 4/4 PASS (one test-side API-name fix: TS RDeque inserts are
  addFirst/addLast, not offerFirst/Last); full 36-file interop
  regression: 115/115 TWICE consecutively after the slack fix; build
  green.

## 2.54.6 — 2026-10-25

Round 43 of the Java-4.8.0 evidence suite (docs+test only): RGeo
addIfExists (GEOADD XX) moves members cross-language; RRateLimiter
PER_CLIENT budgets isolated per client (complements the OVERALL
direction); RBitSet getByte/setByte/incrementAndGetByte — offsets are
BIT offsets (both languages pass them to BITFIELD i8), cross-language
agreement on the same field.

Round 43 — `live-interop48aq` (probe commands `geo48-addifexists`,
`rate48-setratepc`, `bs48-getbyte/setbyte`): three conditional /
byte-level families.

- RGeo `addIfExists` (GEOADD XX): updating an EXISTING member returns
  true and MOVES it (cross-language dist shrinks); an absent member is
  a no-op returning false (still addable afterwards); TS addIfExists
  moves a Java-written member the same way.
- RRateLimiter `RateType.PER_CLIENT`: budgets are isolated per client —
  Java exhausting its own budget (true,true,false) leaves the TS side
  untouched (also true,true,false) within the same window; complements
  round 48e's shared-OVERALL direction.
- RBitSet byte ops SEMANTICS: `getByte/setByte/incrementAndGetByte`
  offsets are BIT offsets, not byte offsets — both languages pass them
  straight to BITFIELD GET/SET/INCRBY i8 ("byte 1" lives at offset 8);
  cross-language set/read/increment agree on the same field.
- 3/3 PASS (one expectation fix after the bit-offset discovery);
  full 45-file interop regression: 138/138 TWICE consecutively; build
  green.

## 2.54.5 — 2026-10-25

Round 42 of the Java-4.8.0 evidence suite (docs+test only): RTimeSeries
remove family both ways, RRingBuffer readNewest/readOldest (slices come
back in ring order, not newest-first), RStream readMany two-stream
reads both directions.

Round 42 — `live-interop48ap` (probe commands `ts48-getandremove/
remove/removerange`, `rb48-add/readnewest/readoldest`,
`stream48-readmany`): three read/remove families.

- RTimeSeries (ZSET family) removal: Java `getAndRemove(ts)` /
  `remove(ts)` on TS-added entries (value decodes exactly — integer-ish
  doubles print without the trailing .0, so the test pins true
  fractions) and TS `getAndRemove`/`removeRange` shrink Java's 4.8-args
  series to zero.
- RRingBuffer read family SEMANTICS: `readNewest(n)`/`readOldest(n)`
  return their SLICE in ring (chronological) order — readNewest(2) on a
  wrapped [three,four,five] is four,five, NOT five-first; Java and TS
  agree on the same ring both directions.
- RStream `readMany`: TS two-stream XREAD returns both Java-written
  streams; Java's `StreamMultiReadArgs.greaterThan(id1, name2, id2)`
  two-stream read returns TS entries. (The builder has no single-arg
  form — only the 3/5-arg static overloads.)
- 3/3 PASS (two test-side expectation fixes: Double printing, ring
  slice order); full 44-file interop regression: 135/135 TWICE
  consecutively; build green.

## 2.54.4 — 2026-10-25

Round 41 of the Java-4.8.0 evidence suite (docs+test only): RSearch
synonym groups shared both ways (FT.SYNUPDATE/FT.SYNDUMP; SYNDUMP
needs an existing index) and RBlockingQueue subscribeOnElements
continuous cross-language drain with unsubscribe stopping the pump.

Round 41 — `live-interop48ao` (probe commands `fts48-synupdate/syndump`,
`bqs48-subscribe/seen/unsubscribe`): search synonyms and continuous
queue consumption.

- RSearch synonyms: Java `updateSynonyms(index, groupId, terms...)` →
  TS `dumpSynonyms` reads term→groups; TS updates a second group → Java
  dumps the merged view (dog → [grp1, grp2]). FT.SYNUPDATE requires the
  index to exist — the test creates it first via `search48-create`.
- RBlockingQueue `subscribeOnElements`: continuous cross-language drain
  — a Java background subscriber (take() loop) consumes every TS offer
  one at a time in order and `unsubscribe` stops the pump; a TS
  subscriber drains Java's offers the same way (queue length back to 0).
- 2/2 PASS; full 43-file interop regression: 132/132 TWICE
  consecutively; build green.

## 2.54.3 — 2026-10-25

Rounds 36-40 of the Java-4.8.0 evidence suite (docs+test only; no
library changes): RVectorSet + RJsonBuckets first cross-language
evidence (36); blocking-lock wake / BinaryStream raw bytes / stream
trim (37); BLMPOP multi-queue wakes + addPermits accounting + the
deque pollFirstFromAny ignores-names quirk (38); MapCache idle expiry
/ HLL algebra / fair-lock mixed FIFO (39); MapCache event channels
with the Java-4.8 addListener zero-subscription quirk (40).

Round 36 — `live-interop48aj` (probe commands `vs48-add/sim/size/contains/
dim`, `jb48-set/get`): two module surfaces whose prior evidence was
TS-only phase suites.

- RVectorSet: TS `add(element, vector)` builds the set (DIM fixed by the
  first vector) — Java `contains`/`size`/`dimensions`/
  `getSimilar(element, count)` see the identical elements and rank the
  nearest cluster first; a Java `VectorAddArgs.element().vector()` add is
  visible to TS `contains`/`size` and TS `sim` ranks it with the cluster.
  Gated on the vector-set module (skip with a logged line).
- RJsonBuckets: Java `set(Map)` / `get(keys)` with 4.8's JacksonCodec —
  DISCOVERY: `getJsonBuckets()` REQUIRES a `JsonCodec` argument, and
  `JsonJacksonCodec` is NOT one (the Json* API moved to the new
  codec hierarchy; `JacksonCodec(Object.class)` is the plain-JSON
  implementor) — TS plain-JSON docs read identically both ways.
- 2/2 PASS first try; full 38-file interop regression: 120/120 TWICE
  consecutively; build green.

## Unreleased

Round 37 — `live-interop48ak` (probe commands `lock48-bglock/bgresult`,
`bs48-set/get`, `stream48-trim`): blocking-lock wakes, binary-stream
bytes, and stream trim — paths the earlier rounds did not cover.

- RLock BLOCKING `lock()` (not tryLock) woken across languages both
  ways: a TS `lock()` parked behind a Java holder completes on Java
  `unlock()`; a Java background blocking `lock()` behind a TS holder
  completes on TS `unlock()`.
- RBinaryStream: RBinaryStream extends `RBucket<byte[]>` — plain
  get/set carries raw bytes; TS `set(Buffer)` with non-UTF8-safe
  latin1 bytes reaches Java byte-identical (length proves the high
  bytes; the probe's line protocol prints them lossily), and Java
  `set(byte[])` reads back through TS `get()`/`getRange()`/`size()`.
- RStream `trim(MAXLEN n)`: Java trims a TS-built stream and vice
  versa — removed counts and final sizes agree on both sides (Java's
  `StreamTrimArgs.maxLen()` builder requires a terminal `noLimit()`).
- 3/3 PASS (one test-side display fix: probe stdout renders high
  bytes as `?`, so assertions compare length + printable segments);
  full 39-file interop regression: 123/123 TWICE consecutively; build
  green.

## Unreleased

Round 38 — `live-interop48al` (probe commands `blm48-bgpollfirstany/
bgresult`, `sem48-addpermits`): multi-queue BLMPOP wakes and semaphore
permit accounting.

- BLMPOP: TS `pollLastFromAnyWithName` (BRPOP with name, two queues)
  parked is woken by a Java offer into the SECOND queue and reports
  the source; a Java background BLMPOP `pollFirstFromAny(Duration,
  count, names...)` is woken by a TS offer into the second queue.
- Java-side DISCOVERY: `RedissonBlockingDeque.pollFirstFromAny(long,
  TimeUnit, names...)` IGNORES the extra names — its pollFromAnyAsync
  parks on self only (source line 121); the real multi-queue BLMPOP
  is the queue `pollFirstFromAny(Duration, count, names...)` count
  variant. Documented in the test + probe comment.
- RSemaphore `addPermits(delta)` accounting across languages: TS
  grant / Java acquire+release / TS addPermits(+3) / Java
  addPermits(-2) — `availablePermits` agrees on both sides at every
  step and a cross-language acquire succeeds.
- 2/2 PASS; full 40-file interop regression: 125/125 TWICE
  consecutively; build green.

## Unreleased

Round 39 — `live-interop48am` (probe commands `mci48-put`,
`hll48-merge`, `hll48-countwith`): idle expiry, HLL set algebra, and
strict fair-lock FIFO — three wire behaviors the earlier rounds did
not cover.

- RMapCache IDLE expiry (the `redisson__idle__set:{name}` ZSET, a
  distinct wire structure from the TTL set): TS
  `put(k, v, 60s, idle 3s)` → Java reads; Java's 6-arg
  `put(K,V,ttl,MS,maxIdle,MS)` → TS reads; past the idle window both
  entries vanish on BOTH sides (gone-after, law 10) with the TTL far
  from due — the idle clock, not the TTL, drove the expiry.
- RHyperLogLog algebra: TS `countWith` over a Java-built register and
  Java `countWith` over a TS-built one both report |{1,2} ∪ {3,4}| = 4;
  Java `mergeWith` combines them into a third register counting 4 on
  both sides.
- RFairLock STRICT FIFO through mixed-language waiters: Java holder,
  TS waiter A, Java bg waiter B, TS waiter C queue in that order;
  serial unlocks hand the lock down the queue A → B → C (order array
  asserts A before C, bgwaitacquired proves B in between).
- 3/3 PASS (two test-side fixes: TS waiters need DISTINCT threadIds —
  the shared default 1 makes them one re-entrant owner — and unlocks
  must carry the matching threadId); full 41-file interop regression:
  128/128 TWICE consecutively; build green.

## Unreleased

Round 40 — `live-interop48an` (probe commands `mce48-listen/poll/
rmlistener`, `mc48-remove`): RMapCache entry events — the last untouched
wire surface (`redisson_map_cache_{created,updated,removed}:{name}`
topics).

- Java writer → TS listener, full dispatch: TS `addListener({ onCreated,
  onUpdated, onRemoved })` receives all three kinds from Java's put/
  re-put/remove with key/value/oldValue decoded; registering sets the
  Java `has-listeners` option so Java writers publish.
- TS publish side verified at BYTE level: a raw subscriber on the three
  channels sees u64LE-length-prefixed frames with codec-encoded parts
  ([key,value] / [key,value,old] / [key,old]) — the exact framing of
  Java's own writer (struct.pack('Lc0Lc0',...) in its put/remove Lua).
- Java-side DISCOVERY (4.8 quirk): `RMapCache.addListener` registers
  NOTHING observable in this standalone-RESP2 setup — NUMSUB/CHANNELS/
  SHARDCHANNELS/NUMPAT all zero and the listener misses even its OWN
  writes (plain RTopic in the same probe self-delivers fine, so the
  probe's pubsub is healthy). Plus one registration serves exactly ONE
  kind (instanceof first-match; Removed is checked first) and the bare
  `MapEntryListener` is rejected with "Wrong listener type" — the probe
  keeps a four-interface listener class. TS→Java listener delivery is
  therefore not testable here; the byte-level check is TS's evidence.
- 2/2 PASS; full 42-file interop regression: 130/130 TWICE
  consecutively; build green.

## 2.54.2 — 2026-10-25

Round 35 — `live-interop48ai` (probe commands `geo48-store/storesorted`,
`lock48-unlock`, `lock48-watchdog`): geo store family, the Java-side
watchdog, and lock re-entry counts — plus one live-caught library fix.

- **`RGeo.storeSearchTo` FIX**: the radius/member paths of
  `GEORADIUS … STORE`/`GEORADIUSBYMEMBER … STORE` were missing the
  literal `STORE` token — the destination key landed as a bare
  positional arg and Redis rejected the command (`ERR syntax error`).
  The box path (`GEOSEARCHSTORE`, dest positional) and `STOREDIST`
  (own token) were correct, which is why phase 20 never caught it:
  its radius case used the sorted variant only. Fixed in
  `buildStore`; phase 20 still 4/4.
- RGeo store family bidirectional: TS/Java plain `STORE` destinations
  keep the members' geohash-integer scores (~3.48e15, not 0) — both
  languages read the identical set+scores; `STOREDIST` destinations
  are distance-scored ZSETs readable as scored entries in either
  language (palermo < near-west < catania from the Palermo origin,
  200km radius).
- Java-side watchdog: Java no-arg `lock()` (its own 30s/3 renewal)
  stays held across a full renewal cycle — TS observes the TTL back
  near 30s (unrenewed floor ~17.5s) and acquires only after the Java
  `unlock()` (round 34 proved the TS-side watchdog; this closes the
  mirror).
- RLock re-entry count: two Java re-entries (HASH field count 2) gate
  TS through BOTH unlocks — the count, not hash presence, controls
  the lock.
- 3/3 PASS (three test-side expectation fixes: geohash scores ≠ 0,
  member/distance ordering, geo dest is a ZSET); full 37-file interop
  regression: 118/118 TWICE consecutively; build green.

## 2.54.0 — 2026-10-24

Interop-caught wire fixes + the complete Java-4.8.0 bidirectional e2e
evidence suite: 33 rounds, 33 new `live-interop48*` files, 111
cross-language cases green TWICE against redis 8.10.2 (probe commands
javap'd against the 4.8.0 jar per law 11). Four library fixes the Java
side caught:

- **codec**: `@class` is now written FIRST in encoded objects (Java
  default-typing order) — reads are order-agnostic, but byte-comparing
  ops (`RBucket.compareAndSet`'s Lua `GET == ARGV`) make the position a
  wire fact.
- **RSetCache**: no-TTL entries score `Long.MAX/100 − now` (Java's
  ZRANGEBYSCORE ceiling in `readAll`); a MaxInt64 score made TS-written
  entries invisible to Java (bytecode-verified 4.6.1 + 4.8.0).
- **RPermitExpirableSemaphore**: the timeout-ZSET member is the hex
  permit id DECODED to 16 raw bytes (Java `ByteBufUtil.decodeHexDump`) —
  hex text broke cross-language release-by-id.
- **RCuckooFilter/RTopK** items travel codec-encoded (Java
  `encode(element)`); **RBloomFilterNative** gains `init(errorRate,
  capacity)` (Java `init(double, long)`).

Round 1 — `live-interop48` (probabilistic/ring family), first Java-side
e2e: bidirectional vs Java Redisson **4.8.0** via new `TsProbe48`
commands `cf48-*`/`topk48-*`/`td48-*`/`cb48-*`, all shapes javap'd
against the 4.8.0 jar:

- **`RCuckooFilter`/`RTopK` items now travel codec-encoded** (Java
  `encode(element)` → JSON-quoted strings), returns codec-decoded —
  previously raw strings, so a TS `add('x')` was invisible to Java
  `exists("x")` and vice versa (TS-only phase 36/66 suites couldn't see
  it: both sides of those tests used the same raw convention). `RCountMin`
  already encoded correctly (phase74-proven) — now all three agree.
- `RTDigest` (raw doubles) and `RCircularBuffer` (codec-encoded AR.* ring,
  shared `redisson_acb:{name}` settings key) needed no fix; the new suite
  proves both directions: TS rings 4 through capacity 3 ⇄ Java reads
  `two,three,four` and rings `five` back.
- Gated per command (`CF.RESERVE`/`TOPK.RESERVE`/`TDIGEST.CREATE`/`ARSET`)
  + Java probe availability; needs redis ≥8.8 for the AR case
  (`REDIS_URL=redis://127.0.0.1:6390`). 4/4 PASS on redis 8.10.2.

Round 2 — `live-interop48b` (probe commands `gcra48-*`/`tsn48-*`/
`maps48-*`/`set48-*`/`q48-*`, shapes javap'd against the 4.8.0 jar),
first Java-side e2e for the 4.8 OSS surfaces phase 69/71/72 had covered
TS-only:

- **RGcra**: TS-configured `{name}:config` hash read back as Java
  `GcraConfig` (period s↔ms) and vice versa; a shared burst-2 limiter
  enforced across languages (TS and Java alternate `tryAcquire` down to
  `limited=true`), incl. the `TOKENS n` path. Gated on the `GCRA`
  command — **not shipped in OSS redis:8** (verified 8.10.2: 669
  commands, none rate-limit-ish), so the case activates only on
  GCRA-capable builds; the wire shape itself was already Java-Lua-proven
  in phase72.
- **RTimeSeriesNative**: raw-double TS.* series shared both directions —
  TS adds two samples, Java reads size/firstTs/lastTs/range and appends a
  third, TS sees all three. No codec either side (Java StringCodec).
- **RMaps** mass set: one TS `RMaps.set` of two maps read back through
  Java `getMap` field-by-field; Java `RMaps.set` read back through TS
  `getMap` (codec HASH parity).
- **RSet countDiff/countUnionApprox** (SDIFFCARD/SUNIONCARD APPROX):
  identical cardinalities (5/15) from both languages on the same sets.
- **RQueue.move** (LMOVEM): TS bulk-moves 2 (decoded returns), Java moves
  1 more, both ends verified from both languages.
- 5/5 PASS on redis 8.10.2 (GCRA case gated-skip with log); regression
  across interop48/phase69/71/72/74/wire/mock 169/169.

Rounds 3–4 — `live-interop48c` (RAtomicLong args + RSearch) and
`live-interop48d` (RAtomicDouble args + PES), probe commands
`atomic48-*`/`atomicd48-*`/`search48-*`/`pes48-*`, shapes javap'd against
the 4.8.0 jar:

- **PES permit members fixed to raw bytes** — the round's second real wire
  bug: Java ZADDs the permit as the hex id decoded to 16 raw bytes
  (`ByteBufUtil.decodeHexDump`, confirmed in the 4.6.1 AND 4.8 bytecode);
  TS stored the 32-char hex text, so a permit acquired in one language
  was unreleasable in the other (both directions). All TS member
  writes/lookups now go through `Buffer.from(id, 'hex')` (real Redis:
  raw bytes; ioredis-mock serializes Buffer eval args symmetrically, so
  its internally-symmetric paths keep working). e2e: TS acquires → Java
  releases by id → availability observed on both sides, plus reverse and
  cross-language `updateLeaseTime`.
- `incrementAndGet(LongIncrementArgs)`/`(DoubleIncrementArgs)`: TS's
  Lua-CAS emulation proven equivalent to Java's native `INCREX`
  point-by-point — out-of-bounds without `saturate` rejected (value
  unchanged, returns current), with `saturate` clamps; both directions,
  long and double. `compareAndDelete(CompareAndDeleteArgs)` six ops agree
  cross-language.
- **RSearch** first Java-side e2e: TS-built index (TEXT/NUMERIC/TAG over
  plain HASH docs) queried by Java (`*`, numeric range, tag), Java-built
  index queried by TS incl. doc field read-back. Probe reads use
  StringCodec — plain HASH field values are not JSON.
- 3/3 + 2/2 PASS on redis 8.10.2; regression incl. PES phases 3/8/16/19,
  mock + wire + interop48/48b: 191/191.

Round 5 — `live-interop48e` (probe commands `bfn48-*`/`rate48-*`/
`q48-offer`): RBloomFilterNative, blocking queue move and rate-limiter
permit recycling.

- **RBloomFilterNative**: elements are RAW strings on both sides (Java
  StringCodec — deliberately NOT codec-encoded, the opposite of
  CF/TopK); e2e shares the bits in both directions (TS reserve+add ⇄
  Java exists/count). Also closes a 1:1 API gap: TS gained
  `count()` (BF.CARD) and `init(errorRate, capacity)` matching Java's
  `init(double, long)`.
- **BLMOVEM**: TS consumer blocks on a dedicated connection with
  `exactly(2)` — the move completes only after Java feeds BOTH elements
  (`COUNT 2` alone wakes on the first push and moves what is there;
  EXACTLY is the wait-for-all selector).
- **RRateLimiter**: permits acquired in TS, recycled by `release()`,
  immediately acquireable by Java `tryAcquire` — the release/recalc
  branch phase68 covered TS-only, now cross-language.
- 3/3 PASS; regression mock + wire + interop48–48e + phase22: 171/171.

Round 6 — `live-interop48f`: TimeSeries labels and RJsonBucket deep
paths — the two subtlest wire surfaces left (probe commands
`ts48-addlabel`/`ts48-rangebylabel`/`json48-*`).

- **TimeSeries labels (law 12 in action)**: TS's codec-encoded label
  blob (flag-2/3 members) is found by Java 4.8's `entryRangeByLabel`,
  which also filters out unlabeled members; Java 4.8's own
  `add(ts, v, label)` is found by TS `rangeByLabel`. Each direction
  uses a separate series key — the e2e re-confirmed live why law 12
  forbids mixing families in one series: Java 4.8's old-signature add
  runs the seq-HASH Lua while TS's old API keeps seq STRING
  (WRONGTYPE without the guard).
- **RJsonBucket deep paths**: TS document write read by Java through
  single-path queries (RedisJSON returns the match array — probe decodes
  with JacksonCodec<List>, Map fails on `[2]`); Java `set(path, v)` and
  `setIfAbsent(path, v)` read precisely by TS (`$.b.c` → `[3]`); NX
  verdicts agree on occupied paths in both languages.
- 2/2 PASS; regression incl. phase25/27/35/43 + mock + wire + all six
  interop suites: 174/174.

Round 7 — `live-interop48g` (probe commands `search48-agg`/`lock48-*`):
aggregation, blocking deque takes and cross-language force-unlock.

- **FT.AGGREGATE**: TS-built TAG index aggregated by Java
  (`GROUPBY @cat REDUCE COUNT 0` — `GroupBy.fieldNames` wants the `@`
  property syntax, the probe adds it) and a Java-built index aggregated
  by TS raw REDUCE tokens — identical group rows (fruit=3/berry=2 and
  fruit=2/berry=1) from both languages.
- **RBlockingDeque**: TS `takeFirst`/`takeLast` block on dedicated
  connections; Java `q48-offer` (RPUSH) feeds them — the head-pop and
  tail-pop both wake and return, queue drained to empty.
- **RLock.forceUnlock**: a watchdog-held TS lock force-released by Java
  (TS re-acquires), and a Java-held lock force-released by TS (Java's
  isLocked flips) — the disaster-recovery path, cross-language.
- 3/3 PASS; regression incl. phase3/5/45 + mock + wire + all seven
  interop suites: 189/189.

Round 8 — `live-interop48h` (probe commands `sem48-*`): multi-queue
blocking polls, semaphore drain and the BLMOVE relay.

- **pollFromAny/pollFirstFromAny**: TS consumer blocks across multiple
  queues (BLPOP over [q1,q2] / BLMPOP COUNT 2); Java feeds one of them;
  the single pop resolves from the fed queue, the batch pop drains the
  loaded queue only (atomic on one key).
- **RSemaphore**: permits configured by TS are drained to zero by Java
  `drainPermits()`; TS `release()` restores cross-language visibility
  (Java `tryAcquire` consumes it); reverse direction with Java-configured
  permits drained by TS.
- **pollLastAndOfferFirstTo** (BLMOVE RIGHT→LEFT): blocking relay fires
  on the first push; with elements resident the TAIL relays and is
  LPUSHed to the destination head (Java reads [r3, r1] after two relays).
- 3/3 PASS; regression mock + wire + all eight interop suites + phase3:
  182/182.

Round 9 — `live-interop48i` (probe commands `bfl48-*`/`lex48-*`): the
hash-based bloom filter, lex set and JSON array ops.

- **RBloomFilter (HighwayHash-128)** — the Lua/hash one, distinct from
  round 5's native `BF.*` filter: cross-language contains proves the
  HighwayHash-128 port lands on identical bit positions (TS init+add ⇄
  Java contains, both directions; config key is the hash-tag
  `{name}:config`).
- **RLexSortedSet**: members are RAW strings both sides (Java
  StringCodec); Java `range` takes ELEMENT bounds and adds the `[`
  brackets itself (passing raw lex syntax like `-`/`+` corrupts them);
  TS ranges with its own lex-syntax bounds — full order, rank and count
  agree cross-language.
- **RJsonBucket array/string ops** (phase43 TS-only until now): TS
  `arrayAppend` observed by Java (`$.arr` → `[[1,2,3]]`); Java path
  writes consumed by TS `arrayIndex`/`arrayPop`/`arraySize`; `stringAppend`
  growth visible to Java.
- 3/3 PASS; regression mock + wire + the original live-interop fixtures
  + all nine interop suites: 179/179.

Round 10 — `live-interop48j` (probe commands `latch48-*`/`rw48-*`):
cross-language coordination.

- **RCountDownLatch**: TS `await` polls the shared counter while Java
  `countDown()`s it to zero — the one-shot barrier opens for the TS
  awaiter; reverse setup with a Java-armed latch released by TS.
- **RReadWriteLock**: a TS-held READ lock lets Java read-acquire
  (shared) while denying its write-acquire; a Java-held WRITE lock
  blocks both TS ends; every transition flips exclusivity correctly.
- 2/2 PASS; regression mock + wire + phase3 + all ten interop suites:
  187/187.

Round 11 — `live-interop48k` (probe commands `bkt48-*`/`tsn48-*`/
`vec48-*`): bucket object CAS, native multi-series MRANGE and the
vector set.

- **`@class` now written FIRST in encoded objects** — the round's real
  wire bug (4th of this effort): Java's JsonJacksonCodec default typing
  emits `{"@class":"java.util.LinkedHashMap","v":1}` while TS emitted
  the `@class` LAST. Reads are order-agnostic (every fixture passed for
  years), but byte-comparing ops exposed it — Java's
  `RBucket.compareAndSet` on a TS-written object always failed. TS now
  emits `@class` first (`javaTypeWrap`); 220/220 regression incl. the
  python interop and the 4.6.1 fixtures.
- **RBucket.compareAndSet on objects**: expect/update maps CAS in both
  languages; stale expects rejected.
- **RTimeSeriesNatives MRANGE with FILTER** (phase71 TS-only): TS-created
  labeled native series found by Java's filter (others excluded), and
  Java-created labeled series found by TS's `TSMultiRangeArgs.filter`.
- **RVectorSet** (deep 4.8 surface, only 4.6.1 basics covered before):
  TS-added vectors similarity-searched by Java (`getSimilar`);
  Java-added vector with JSON attributes read back by TS
  (`sim`/`getAttributes`); TS `setAttributes` visible to Java.
- 3/3 PASS on redis 8.10.2.

Round 12 — `live-interop48l`: wake paths and expiry args.

- **PES blocked acquire woken cross-language**: a TS `acquire()` parked on
  the last permit resumes when Java `release(permitId)` frees it and
  publishes the `redisson_sc:{name}` wake — the blocked side receives a
  fresh permit id.
- **RArray inserts** (phase73 TS-only before): TS `ARINSERT` appends read
  positionally by Java; Java's append observed via TS `range` (explicit
  bounds — TS `range()` rejects negative indexes).
- **INCREX `timeToLive`**: TS's Lua-CAS PX and Java's native
  `INCREX ... PX` both leave live TTLs on the shared key, mutually
  readable; a TS renewal with a shorter window shortens it (PTTL
  verified raw).
- 3/3 PASS; regression incl. phase73 + mock + wire + all twelve
  interop suites: 190/190.

Round 13 — `live-interop48m`: CONCURRENT cross-language contention (all
earlier suites alternated operations sequentially).

- **Atomic INCR race**: 50 TS + 50 Java increments in flight together →
  exactly 100, read identically by both.
- **Queue drain race**: 20 preloaded items consumed by concurrent TS and
  Java polls → every item delivered exactly once (union = full set, no
  duplicates, queue empty).
- **Semaphore no-oversell**: 1 permit raced by both languages → exactly
  one holder, none available, regardless of the winner.
- Flake hunted down and fixed in the round-12 PES wake case: the 6390
  redis (WSL docker) **jumps its clock forward** under parallel load,
  expiring short leases mid-test and skewing counters (observed
  counter=4/zset=[]). 10-minute leases make any realistic jump
  irrelevant; lesson folded into AGENTS law 10.
- 3/3 PASS; full 15-file interop regression under parallel load:
  188/188.

Round 14 — `live-interop48n`: TRI-LATERAL (TS + Java 4.6.1 + Java 4.8.0
in one worker, both resident JVMs) and Unicode payloads.

- **Rolling upgrade relay**: one counter relayed TS → 4.6.1 set → 4.8.0
  increment → read back identically by all three; one queue fed by three
  producers (one per runtime), drained in order by TS.
- **Version-mixed lock handoff**: a lock held by TS denies 4.6.1
  `tryLock`, is observed `locked` by 4.8.0, force-released BY 4.8.0 and
  re-acquired by 4.6.1 — the lock wire is stable across Java versions
  (the rolling-upgrade disaster-recovery path).
- **Unicode**: CJK/emoji/accented bucket fields and queue members cross
  languages intact. Java verification goes through boolean
  field-match commands (`bkt48-expectfield`/`q48-expectpoll`) so values
  never echo through stdout (host-charset-sensitive); Java-written CJK
  (UTF-8 stdin args) read exactly by TS.
- 3/3 PASS; full 16-file interop regression: 191/191.

Round 15 — `live-interop48o`: TTL-driven CRASH RECOVERY and large
payloads (probe commands `bstream48-stat`/`bstream48-gen`).

- **Lock lease expiry**: a TS holder that never unlocks (explicit short
  lease, no watchdog — a crashed client) is recovered purely by expiry:
  Java 4.6.1 `tryLock` goes CONTENDED → ACQUIRED across the window.
- **PES permit expiry**: an unreleased permit returns to the pool by
  lease expiry and Java acquires it ("gone-after" direction only — see
  the clock note).
- **DelayedQueue firing**: the 4.6.1 probe offers with a delay (its
  resident JVM schedules the transfer); TS's destination poll receives
  the item after the deadline.
- **Large payloads**: 128KB of random bytes written by TS verified by
  Java via len+sum stats; 64KB of deterministic pattern written by Java
  verified byte-for-byte (0 mismatches) by TS.
- **Clock finding documented (AGENTS law 10)**: TS scores leases with
  server TIME while Java PES/DQ writes and lazily purges with its
  CLIENT wall clock — with the WSL-vs-host skew exceeding the lease,
  cross-language "not before deadline" assertions are inherently flaky;
  such cases assert only the gone-after direction.
- 4/4 PASS; full 17-file interop regression: 195/195 (one load-sensitive
  first-run blip did not reproduce).

Round 16 — `live-interop48p`: numeric precision boundaries, the last
untested codec risk area (probe commands `bkt48-setlong`/
`atomic48-setlong`).

- **Unsafe longs (|v| > 2^53)**: the wire carries the exact decimal in
  the `["java.lang.Long",v]` wrapper. Java reads TS-written 2^53+1
  EXACTLY (and −2^53−5); Java-written values read back exactly by TS
  through the opt-in `JsonCodec({ bigIntForUnsafeLong: true })`. (The
  default codec keeps its documented lossy number contract on DECODE;
  encoding was verified lossless — the raw stored bytes carry the full
  literal either way.)
- **int32 boundary**: 2147483647 stays a bare literal, 2147483648 gets
  the Long wrapper — wire shapes verified raw, values exact in both
  directions.
- **RAtomicLong** stores raw decimal strings: unsafe values exact in
  both directions with the plain API (TS parses unsafe as BigInt).
  **Doubles** round-trip bit-for-bit (Java's minimal `Double.toString`
  agrees with TS's serialization).
- 3/3 PASS; full 19-file interop regression incl. the python codec
  cross-checks: 203/203.

Round 17 — `live-interop48q` (probe commands `search48-alias`/
`search48-dropalias`): watchdog renewal, shared bloom init, aliases.

- **Lock watchdog renewal**: a TS `lock()` with NO explicit lease
  (watchdogTimeout 8s) stays locked at 12s — 1.5× the first lease, so
  renewal is the only way the key exists — observed by Java's
  `isLocked` at both ends; `unlock` flips it everywhere.
- **Bloom shared initialization** (round 9 proved each side's OWN init;
  this closes the loop): Java ADDS bits into a TS-initialized filter by
  reading TS's `{name}:config` (same hashIterations/bitSize) and vice
  versa — counts stay exact on both sides.
- **RSearch aliases**: a TS `addAlias` resolves Java searches through
  the alias; a Java-dropped-and-re-added alias resolves TS searches.
- 3/3 PASS; full 19-file interop regression: 201/201 (one load-sensitive
  first-run blip did not reproduce).

Round 18 — `live-interop48r`: the final structural gaps.

- **RBoundedBlockingQueue across languages**: capacity configured by TS
  (`trySetCapacity(2)`, companion `redisson_bqs:{name}`) gates Java's
  blocking `put` — the third put parks (its result key shows
  `BQ_PUT_STARTED`) until a TS `take` frees the slot and completes it
  (`BQ_PUT_OK`); drain restores the remaining capacity to 2. (The
  capacity key stores the REMAINING counter, not the max —
  `getCapacity()` is a remainingCapacity alias.)
- **PES multi-permit atomicity**: TS `tryAcquire(lease, 2)` consumes two
  permits in one Lua; Java sees exactly one remaining, takes it, and
  TS's next 2-permit attempt is rejected whole (no partial grant).
  Cleanup uses the hash-tag `{name}:timeout` form (law 4 — plain-form
  deletes leak members across runs).
- **RJsonBucket terminal ops**: TS `merge` read by Java as the merged
  shape; TS `toggle` flip seen by Java; Java path-writes consumed by TS
  `arraySize`/`clear` (cleared empty array reads back as the match
  array `[[]]`).
- 3/3 PASS; full 20-file interop regression: 204/204 (one first-run
  blip did not reproduce).

Round 19 — `live-interop48s` (probe command `sem48-release`): the last
distinct blocking code paths.

- **`RBlockingQueue.take()` forever-block**: TS parks on a dedicated
  connection with NO timeout parameter; a Java offer wakes it (30s
  safety race fails loudly instead of hanging on a missed wake). Every
  earlier round used the timeout variants.
- **`RSemaphore.acquire()` wake loop**: TS blocks on the last permit
  held by Java; Java's `release()` publishes the `redisson_sc` wake and
  the TS acquire resolves holding the permit — the plain-semaphore wake
  path, previously proven only for PES (round 12).
- 2/2 PASS; full 21-file interop regression: 206/206 (one first-run
  blip did not reproduce).

Round 20 — `live-interop48t` (probe commands `flock48-*`, `topic48-*`,
`mc48-*`): three surfaces never exercised cross-language before.

- **RFairLock FIFO queue, both wake directions**: Java holder → TS
  `lock()` queued → Java unlock wakes TS; and TS holder → Java
  background `tryLock(wait, lease)` waiter → TS unlock wakes Java.
  Queue member / HASH field / wake-channel suffix are `id:threadId`
  on both sides, so the wake resolving IS the wire proof. Plus
  `tryLock()` immediate-fail against a Java holder.
- **RTopic pub/sub payload**: channel is the raw name (no prefix —
  Java wraps it in ChannelName directly). TS listener decodes Java's
  JsonJacksonCodec payload and vice versa; PUBLISH's receiver count
  sees the other side's subscriber (exactly 1).
- **RMapCache per-entry TTL**: Java `put(k, v, ttl)` → TS get unpacks
  Java's packed value + deadline ZSET; TS `put(k, v, ttlMs)` → Java
  get unpacks TS's packing while alive. Expiry asserted gone-after
  only (law 10). First live proof of the MapCache packed layout
  against 4.8.
- Test-side lesson: `getTopic()` returns a fresh instance each call —
  `removeListener` on a new instance no-ops and the old subscription
  leaks (PUBLISH counted 2 receivers). Hold one instance.
- 5/5 PASS; full 22-file interop regression: 69/69 (one first-run
  blip in 48l did not reproduce).

Round 21 — `live-interop48u` (probe commands `zset48-*`, `ring48-*`,
`sc48-*`): the ZSET / ring / set-cache families had thorough 4.6.1-probe
coverage but no 4.8.0-jar evidence until now.

- **Real wire bug fixed (`RSetCache` no-TTL score)**: TS scored no-TTL
  entries `MaxInt64`, but Java scores them
  `92233720368547758L - now` (Long.MAX_VALUE/100 — bytecode-verified
  identical on 4.6.1 AND 4.8.0) and `readAll`'s ZRANGEBYSCORE ceiling
  is that same constant, so TS-written no-TTL entries were INVISIBLE
  to Java `readAll` (contains/size passed — only readAll exposes it;
  the 4.6.1 probe never tested a TS no-TTL entry, so it went
  unnoticed). `src/caches.ts` now writes Java's formula (BigInt for
  the >2^53 constant); the sc48 test asserts the fixed direction
  (Java readAll reads back a TS no-TTL entry) plus TTL gone-after on
  both sides.
- RScoredSortedSet: codec members + scores read back both directions;
  rank/first/last/pollFirst/addScore/count agreement; GT conditionals
  agree cross-language.
- RRingBuffer: capacity trim round-trips through both writers
  (`redisson_rb:{name}` settings key identical).
- 4/4 PASS; SetCache regressions (mock 154 + live phase 5/16/62/72
  20/20) green; full 23-file interop regression: 73/73 (one first-run
  blip did not reproduce; rerun twice clean).

Round 22 — `live-interop48v` (probe commands `stream48-*`, 10 new):
RStream, the biggest family still without 4.8.0-jar evidence.

- **add/read both directions**: field names AND values are
  codec-encoded on the wire; TS group-read decodes Java's entries and
  vice versa (`fields={temp=19}` from a TS-written entry), ids agree.
- **PEL is cross-language**: a Java consumer's unacked reads appear in
  TS `listPending` (consumer name and all); TS `claim` (XCLAIM, min
  idle 0) + `ack` shrinks Java's PEL; Java `ack` closes it — both
  sides report empty.
- **XAUTOCLAIM takeover**: TS-consumer pending entries claimed by
  Java's `autoClaim` (min-idle 0); ownership flip visible from TS
  `listPending` before TS acks everything clean.
- 3/3 PASS first try; full 24-file interop regression: 76/76 (48l's
  PES-wake case blipped twice under full load — solo + two full reruns
  all green, same documented clock-jump class; no code change needed).

Round 23 — `live-interop48w` (probe commands `bits48-*`, `hll48-*`,
`geo48-*`, `idgen48-*`): the four smaller families the 4.6.1 probe had
covered but the 4.8.0 jar had zero evidence for.

- RBitSet: single bits, BITFIELD 64-bit segments and BITOP OR agree
  both ways. Lesson: BITFIELD windows are MSB-first (value 5 at bit
  offset 64 → set bits {125,127} in LSB numbering), while SETBIT bits
  are LSB-numbered — both sides consistent, the test's first draft
  assumed little-endian.
- RHyperLogLog: shared register counts identically (5 → 7 through
  both writers). RGeo: dist/pos/search agree both directions on codec
  members. RIdGenerator: block allocation is `[old, old+alloc)` on
  both sides (Java's Lua returns the OLD counter as block start);
  each fresh Java instance burns a whole block — exact sequence
  1000/1010/1020… asserted, no duplicates.
- **48l's chronic PES-wake flake ROOT-CAUSED and fixed**: the PES
  timeout ZSET is `suffixName`-braced (`{name}:timeout`) but 48l/48d/
  48o cleanup deleted the UNBRACED `${name}:timeout` — a silent no-op
  that leaked stale permits across runs; any full-suite run spaced
  >10 min from a leak had its purge recover them, inflating
  `availablePermits` (observed as counter=2). Cleanup keys now use the
  braced form; leaked keys purged from 6390.
- **48o's DQ test latent bug exposed by that fix**: `poll(12000)` is
  the non-blocking COUNT overload, not a timed wait — it used to pass
  by draining leftover transfers from the leaked staging keys. Now
  uses `pollWithTimeout(12000)` (blocking) and passes on clean state.
- 4/4 PASS; full 25-file regression 80/80 TWICE consecutively — the
  48l flake is gone with its root cause.

Round 24 — `live-interop48x` (probe commands `adder48-*`, `mm48-*`,
`lmm48-*`, `map48-random-entries`): the last three families without
4.8.0-jar evidence.

- RLongAdder cohort flush chain, the subtlest wire in the library:
  Java cohort `add(100)` → TS `add(20)` + `sum()` (merge) = 120 →
  signal → Java cohort `sum()` = 120, both exact. Per-client
  `{name}:{id}:counter` keys are random per client — cleanup uses a
  `*name*` pattern sweep (no braces in glob).
- RMultimap: Highway-derived collection ids must match cross-language
  or `getAll` misses — SET and LIST variants, both directions; cleanup
  via `delete()` per law 9 (child collections are invisible to
  patterns and ids are deterministic).
- RMap `randomEntries`: HRANDFIELD subsets from both writers stay
  inside the shared key space.
- 3/3 PASS first try; full 26-file interop regression: 83/83 TWICE
  consecutively. With this round every structure family in
  COMPATIBILITY.md has live bidirectional evidence against the 4.8.0
  jar.

Round 25 — `live-interop48y` (probe commands `batch48-run`,
`list48-range`, `buckets48-*`, `script48-*`): the execution layer —
RBatch, RScript, RBuckets — joins the 4.8.0 evidence set (structure
families were closed in round 24).

- RBatch both directions: TS queues codec-encoded bucket/map/list/
  atomicLong writes and executes; Java reads each back. Java's batch
  facades expose ONLY the `*Async` methods (RFutures settle on
  `execute()` — compile-time discovery); TS's per-op Deferreds decode
  on execute the same way (incrementAndGet → 1, add → 2 asserted).
- RScript asymmetry documented, not "fixed": Java eval encodes ARGV
  through the codec (`"v"` with quotes) and decodes VALUE replies
  through it; TS eval passes values raw and returns raw replies. The
  round trip works because each side reads through its own codec
  (Java script-put → TS bucket get; TS bucket set → Java script-get
  VALUE). Script bodies are space-free constants duplicated
  byte-identically in the probe (args split on spaces, law 11). Sha
  sharing goes both ways: TS scriptLoad → Java scriptExists + evalsha;
  Java scriptLoad → TS scriptExists + evalSha (raw reply, JSON-decoded
  in the test).
- RBuckets MSET/MGET both directions.
- 3/3 PASS first try; full 27-file interop regression: 86/86 TWICE
  consecutively; build green.

Round 26 — `live-interop48z` (probe commands `sharded48-*`,
`buckets48-casexist/casabsent`, `bq48-bgtake/bgresult`,
`deque48-push/pop`): the queue/topic/deque edges the earlier rounds
skipped.

- RShardedTopic (4.8: extends RTopic, wire is SPUBLISH/SSUBSCRIBE —
  TS delivers via the `smessage` event): codec payloads decode both
  directions and TS sees the SPUBLISH receiver count (publish → 1
  while Java listens).
- RBuckets conditional MSETEX (XX/NX) all-or-nothing both directions:
  a failed XX writes NEITHER key (pre-existing TS value intact);
  Java's `SetArgs.entries(m).timeToLive(...)` lands codec values + TTL
  on both keys (PTTL bounded, read by TS); TS `setIfAllKeysAbsent/
  setIfAllKeysExist` verified by Java readback.
- RBlockingQueue wake both ways: TS `take()` parked on BLPOP woken by
  Java's plain RPUSH offer; a Java background take() (daemon thread +
  latch statics, REPL stays responsive) woken by a TS offer.
- RDeque both-ends ops both directions (the no-arg `pollFirst/
  pollLast` are inherited from `java.util.Deque` in 4.8 — javap of the
  interface alone doesn't show them).
- 4/4 PASS first try; full 28-file interop regression: 90/90 TWICE
  consecutively; build green.

Round 27 — `live-interop48aa` (probe commands `latch48-bgwait/
bgresult`, `sem48-bgacquire/bgresult`, `pattern48-listen/poll`,
`mmc48-put/expirekey/getall`): blocked waits woken cross-language and
the last two pub/sub / multimap shapes without 4.8 evidence.

- RCountDownLatch wake both ways: TS `await()` parked on a count of 2
  resolves when Java counts down twice; a Java background await() (the
  no-arg variant is VOID in 4.8 — probe uses the bounded 120s one)
  resolves on TS's countdowns.
- RSemaphore wake both ways on the shared `redisson_sc:{name}`
  channel: TS `acquire()` parked at 0 permits woken by Java release();
  a Java background acquire() woken by a TS release(1).
- RPatternTopic: Java's PatternMessageListener is 3-arg (pattern,
  channel, message) — TS receives concrete channel + decoded payload
  both directions; publish receiver count visible to TS.
- RSetMultimapCache TTL: the RMultimapCache interface has NO
  put-with-TTL (both languages) — per-key expiry is `expireKey` only;
  Java values visible to TS, TS values to Java, empty after expiry
  (gone-after, law 10). Cache-variant read method is `get(key)`
  returning an array, not `getAll`.
- 4/4 PASS (one test-side `getAll`→`get` fix); full 29-file interop
  regression: 94/94 TWICE consecutively; build green.

Round 28 — `live-interop48ab` (probe commands `lock48-trylock`,
`map48-putifabsent/replace3/removeval/putall`): mutual exclusion and
conditional writes — the semantics behind wire shapes earlier rounds
only probed non-blockingly.

- RLock mutex both ways: a TS-held lock rejects Java's tryLock(0) and
  vice versa (Java locks with a 10s lease); Java forceUnlock releases
  the other side's view and TS can lock again. Companion of the round
  20 FairLock FIFO/wake tests — plain-lock exclusion was unproven
  against 4.8 until now.
- RReadWriteLock full semantics across languages: Java read held → TS
  read tryLock succeeds (shared) while TS write tryLock fails; after
  all readers release, TS writer holds and Java's write tryLock fails;
  after TS releases Java can write. Writer exclusion is symmetric.
- RMap conditional CAS chain alternating sides: putIfAbsent sees the
  other's value as `prev`, 3-arg replace accepts only the exact old
  value on both sides, remove(key,value) rejects stale expectations,
  putIfAbsent creates after delete.
- RMap lazy HSCAN iterator: 12 Java-written fields fully collected at
  COUNT 5 (multi-round scan), then a TS put read back by Java.
- 3/3 PASS (one assertion-string typo fixed); full 30-file interop
  regression: 97/97 TWICE consecutively; build green.

Round 29 — `live-interop48ac` (probe commands `rtopic48-*`,
`set48-addall/readall/union`, `zset48-addpairs/union/dump`): reliable
delivery and set algebra.

- RReliableTopic both directions: XADD field `m` + one subscriber group
  per client, payloads codec JSON through XREADGROUP. publish() returns
  the live group count (0 on a groupless topic, 1 across languages).
  Consumed-by-all entries are DELETED (Java size() = 0 after delivery
  is correct semantics); groupless entries stay (size = 1).
- **Set store semantics CLARIFIED by e2e** (no library change — TS
  already matched): Java `union(names)` overwrites SELF with
  union(others) — the SUNIONSTORE args[0] is only the DESTINATION, not
  an input (my first bytecode reading mistook it for an input; clean
  probe repro: s1={a,b,c} union s2={d,b,e} → s1={b,d,e}, count=3, for
  4.6.1 AND 4.8.0 — bytecode-identical). TS's pinned behavior was
  right; the COMPATIBILITY row's "(self IS an input)" note was
  misleading and is now corrected. read* variants DO read self as
  first input (fresh s4={f,x} readUnion(s1,s2) → {b,d,e,f,x}, s4
  untouched).
- RScoredSortedSet algebra: Java `union(Aggregate, String...)` MAX
  merge read by TS, TS `unionWith({names, aggregate: 'SUM'})` read by
  Java — member scores agree both ways (Java prints whole doubles as
  "1.0").
- 3/3 PASS (test-side expectation fixes: SUNIONSTORE destination
  semantics, consumed-entry deletion, `add` returns boolean); full
  31-file interop regression: 100/100 TWICE consecutively; build green.

Round 30 — `live-interop48ad` (probe commands `mc48-compute/
expireentry/rttl`, `mcn48-put/get/rttl`, `bits48-and/xor`,
`list48-set/get`): TTL-deep map-cache surfaces and small operators.

- RMapCache deep ops: TS `computeIfAbsent(key, ttlMs, fn)` (per-key
  lock + TTL) read by Java; Java `computeIfAbsent(K, Duration, fn)`
  read by TS; per-key `remainTimeToLive(K)` cross-read both ways;
  `getWithTTLOnly` returns without refreshing; `expireEntry`
  shortens a TTL set by the other side (bounds asserted both views,
  small clock-rounding slack for the WSL host).
- RMapCacheNative (7.4+ HSETEX/HPEXPIRE) — zero 4.8 evidence until
  now: Java `put(K, V, Duration)` ↔ TS `put(key, value, ttlMs)`,
  values and per-field HPTTL read back on both sides.
- **BITOP self-as-input lesson (test-side, no library change)**:
  Java's `and/xor` build `BITOP <op> self self others...` — the
  destination is ALSO a source, so AND into a fresh (zero) key yields
  all zeros; seed the destination with OR first. And the `or/and/xor`
  return value is the result's BYTE LENGTH, not popcount — round 23's
  "len=" wording on `bits48-or` was the honest one.
- Grab-bag: TS Bucket `getAndSet`/`getAndDelete` with Java readback;
  RList index ops both ways (TS `set`/`trim` ↔ Java single-index
  `get`, Java `set` ↔ TS `get`).
- 3/3 PASS after two test-side semantic fixes (BITOP seeding, TTL
  rounding slack); full 32-file interop regression: 103/103 TWICE
  consecutively; build green.

Round 31 — `live-interop48ae` (probe commands `exp48-rttl/expire/
expireat/persist`, `zset48-bgpoll/bgresult`, `mcn48-putifabsent`):
TTL-management primitives and blocking sorted-set polls — the last
unproven surfaces outside RFunction.

- RExpirable primitives full chain: TS `expire(ms)` → Java
  `remainTimeToLive()` bounded; Java `expire(ms, MS)` → TS bound; TS
  `expireAt(epochMs)` → Java bound; Java persist is `clearExpire()`
  (RExpirable has NO persist() in 4.8 — it lives on RObject, found at
  compile time) → TS reads -1; TS `clearExpire()` → Java reads -1.
- RScoredSortedSet blocking polls both ways: TS `pollFirstWithTimeout`
  parked on BZPOPMIN woken by Java `add` (ScoredEntry carries value +
  score 1.5); a Java background `pollFirst(60, SECONDS)` woken by a TS
  add.
- RMapCacheNative conditionals: Java `putIfAbsent(K, V, Duration)`
  creates a fresh field; TS putIfAbsent sees the existing value and
  rejects; TS putIfAbsent with TTL lands where Java reads value +
  HPTTL.
- 3/3 PASS first try; full 33-file interop regression: 106/106 TWICE
  consecutively; build green.

Round 32 — `live-interop48af` (probe commands `fn48-load/callwrite/
callread/fnexists/listlibs`): RFunction FUNCTION LOAD / FCALL — the
last execution-layer surface without 4.8.0-jar bidirectional evidence.

- TS `FUNCTION LOAD` (full payload incl. shebang) → Java `FCALL` runs
  the TS-registered write function; Java's vararg VALUES are
  codec-encoded (bytecode: `encode(Collection, codec)` on
  RedissonFuction.callAsync — impl class is literally
  `org.redisson.RedissonFuction`, typo and all), so TS's bucket decode
  sees the clean string; Java `FCALL_RO` runs the TS no-writes read
  function → codec-decoded VALUE.
- Java `loadAndReplace(libName, BODY)` → TS `FCALL`/`FCALL_RO` run the
  Java-registered functions. Java builds the shebang line ITSELF
  (`"#!lua name=" + lib + " \n" + body`) — probe passes body-only.
  TS reads the raw bulk reply (stored codec bytes, quotes included —
  `JSON.parse` to compare), same asymmetry as RScript.
- `FUNCTION LIST` both ways: Java `list(lib)` function names sorted;
  TS `list()`/`list(lib)` library + function discovery; Java
  `list()` all-libraries listing sees each side's library.
- 2/2 PASS after one test-side ordering fix (per-library listlibs
  assertions split — test 1's cleanup deletes its lib before test 2
  runs); full 34-file interop regression: 108/108 TWICE consecutively;
  build green.

Round 33 — `live-interop48ag` (probe commands `dq48-offer/read`,
`mlk48-trylock/unlock/bgtrylock/bgresult/bgrelease`, `rlk48-trylock/
unlock`, `fnc48-trylockget/unlock/gettoken/delete`): delayed delivery
and composite/fenced locking — three surfaces whose cross-language
evidence predated the 4.8 jar rounds.

- RDelayedQueue wire holds vs 4.8: Java `offer(v, 400ms, MS)` STAGES a
  struct-packed (`Bc0Lc0`) member that TS `size()`/`pendingSize()`/
  `contains()` unpack (payload decodes identically); TS `poll()` runs
  the sync catch-up migration and consumes it. Reverse: TS `offer` +
  started migrator delivers into the destination where Java polls.
- RMultiLock all-or-nothing across languages (Java-held triple rejects
  TS MultiLock + single lock; TS-held triple parks a Java background
  `tryLock(6s, MS)` until TS `unlock()` wakes it); RRedLock quorum —
  TS holding 1 of 3 still lets Java win (2 ≥ quorum), a TS RedLock at
  1 of its targets fails AND rolls back the one it took.
- RFencedLock DISCOVERY: Java 4.8's NO-ARG `tryLockAndGetToken()` is
  bytecode-identical to `lockAndGetToken()` (both route to
  waitTime=-1/leaseTime=-1) and BLOCKS on contention until an unlock
  wake — a 180s live hang; the probe uses the bounded
  `(waitMs, MS)` variant. TS's no-arg variant is the single
  non-blocking attempt its interface contract promises (documented
  divergence, client-side semantics only — the wire counter is shared
  and monotonic across languages, and TS `delete()` resets it to 0).
- Java multi-lock unlock lesson: the background thread holds on ITS
  thread id — a foreign thread's `unlock()` throws, so the bg task
  holds until `mlk48-bgrelease` asks it to release itself.
- 3/3 PASS (two live-caught fixes: bg-thread unlock ownership, and
  delete() = false on a fresh name); full 35-file interop regression:
  111/111 TWICE consecutively; build green.

## 2.53.2 — 2026-10-23

`stream`/`iterator` pages join the server-side collect (companion to
2.53.1): `TS_COLLECT_PAGE` returns the live entries of an index window in
one round trip, leading with the raw member count (live + expired) so the
lazy cursor still advances correctly; same `anyExpired` fast path, same
decoded-triples reply, same client-side fallback for no-`struct` Lua.
A 100-member page drops from 101 round trips to 1.

## 2.53.1 — 2026-10-23

TimeSeries read path: server-side collection (one round trip), Java
`COLLECT_RANGE` shape — 44× faster windowed reads.

- `range`/`entryRange`/`rangeReversed` (and everything routed through
  them: `getEntry`/`getValue`/`first`/`last`/poll fallbacks) now collect
  the window in ONE Lua (`TS_COLLECT_LIVE`) with Java's `anyExpired` fast
  path: a single ZCOUNT up front skips the per-member expiry checks when
  nothing has expired; members are struct-unpacked inside the script so
  raw binary never crosses the wire as UTF-8 (reply = decoded
  `[score, value, label]` triples, `decodeTriples` unchanged). The
  historical client-side loop stays as the fallback for Lua environments
  without `struct` (ioredis-mock).
- Measured on the bench oracle (redis 8.10.2, 64-entry window):
  `entryRange` **52.7 ms → 1.2–1.7 ms**; docs/BENCHMARK.md refreshed.
- Bug caught by the mock while porting: the reverse branch must use
  `zrevrangebyscore` (swapped `zrangebyscore` bounds return empty on real
  Redis and silently succeed empty on the mock).
- Tests: existing phase25/27/70/74 suites cover both paths (mock takes
  the fallback, live takes the Lua); no new cases needed — 149 mock +
  24 TimeSeries live re-verified against 8.10.2.

## 2.53.0 — 2026-10-23

Phase 74: closing the evidence loop on "100% Java Redisson 4.8.x" —
bidirectional interop against the released **4.8.0 jar** (not just the
4.6.1 baseline), cluster co-slotting for the 4.8-era companion keys, and a
set of hardening fixes the new surfaces flushed out.

- **TS ↔ Java Redisson 4.8.0 bidirectional probes** (`interop/pom48.xml` +
  `TsProbe48`, a self-contained resident REPL against the released jar):
  TimeSeries `add(TimeSeriesAddArgs)` family — Java reads TS-written
  flag-4 members and vice versa, both writers draw from the ONE
  `redisson__ts_seq:{name}` HASH counter (identical companion names
  verified in RedissonTimeSeries 4.8 source), member bytes asserted
  flag-4 with distinct ids; `RArray` — cross-language get/set with the
  raw stored bytes proven to be the JSON-quoted codec value;
  `RCountMin` — one shared CMS sketch. `test/helpers/java-probe.ts`
  refactored into `createJavaProbe(opts)` (existing 4.6.1 exports
  unchanged; `java-probe48.ts` is the second instance). New CI job
  `java-interop-48` runs `live-phase74` on redis:8.
- **`RTimeSeries` mixed-family guard** (AGENTS law 12 enforcement): the
  seq key type (STRING = legacy 4.6.1 family, HASH = 4.8 family) is
  checked once per family per instance; a cross-family write throws a
  clear error instead of corrupting members / surfacing a raw WRONGTYPE.
- **`RMaps` cluster fix (real bug)**: pipeline `exec()` never rejects for
  per-command errors — on an ioredis Cluster the error object sits inside
  the results, so `set()`/`createImport()` silently wrote NOTHING when
  HIMPORT was missing (or CROSSSLOT). Per-command errors are now checked;
  unknown-HIMPORT/CROSSSLOT falls back to the cluster-safe plain-HASH
  path (HIMPORT's fieldset key has no hash-tag — Java parity — so the
  fast path is effectively single-node, documented).
- **`live-cluster-phase74`**: server-side `CLUSTER KEYSLOT` proves every
  4.8-era companion (`redisson__ts_ttl/seq`, `redisson_acb`,
  `redisson_set_cache_expired`) co-slots with its bare name; TimeSeries
  4.8 family, RCountMin, RMaps and (AR-gated) RCircularBuffer operate
  through a real 3-master cluster client.
- **CI flake root cause**: the java-interop job now runs the ~75 live
  suites **serialized** (`--no-file-parallelism`) — one Redis hammered by
  73 parallel files was the source of the phase9/37/44 load flakes.
- **Local dev oracle**: `scripts/redis88-up.sh` — WSL-docker redis:8
  (≥8.8, AR./CMS./TS.*/HIMPORT/GCRA) on 6390 without touching the managed
  6379; `REDIS_URL=redis://127.0.0.1:6390 npm test` runs the whole suite
  on the new surface locally (phase5/6/10 TTL-interop margins assume
  Java-probe and Redis share a clock — fine on CI, keep in mind for
  cross-VM setups).
- **`live-interop` env fix**: the fixture JVM (`mvn exec` TsProbe) now
  inherits REDIS_HOST/REDIS_PORT mapped from REDIS_URL — it used to write
  fixtures to the default 6379 regardless of the suite's target.
- Tooling: `typedoc` API reference (`npm run docs` → docs/api,
  gitignored) and `npm run bench` (`scripts/bench.mjs`, sequential
  ops/sec microbenchmark → docs/BENCHMARK.md); ioredis-mock upstream TIME
  fix prepared under docs/upstream/ (patch + issue text, not submitted).
- Tests: +1 mock (mixed-family guard), `live-phase74` (3 suites, all
  gated: probe boot / ARSET / CMS), `live-cluster-phase74` (1 suite).
  Local validation: serialized live sweep against redis 8.10.2 green —
  75 files / 207 tests (mid-run failures traced to a concurrent benchmark
  on the same instance plus a 100ms-margin rate-limiter seed that
  host↔server clock skew could eat; the stale-permit seed is now 5s old).

## 2.52.0 — 2026-10-23

Phase 73: the final 4.8-audit items — full `RArray` (Redis 8.8+ `AR.*`
ARRAY type) and `RCircularBuffer` surfaces plus the two `RBatch` module
facades. Classes moved from `infra.ts` to `src/arrays.ts` (public API
unchanged; old `set/get/len/add/readAll` signatures kept).

- **`RArray` full surface** (Java 4.8 `RedissonArray`, ~30 methods over
  `ARSET/ARGET/ARMGET/ARMSET/ARGETRANGE/ARLEN/ARINSERT/ARRING/ARNEXT/ARSEEK/
  ARDEL/ARDELRANGE/ARCOUNT/ARSCAN/ARINFO/ARLASTITEMS/ARGREP` etc.):
  single + variadic `get`, `isSet`, async-generator `iterator`,
  `set(index, v…)` / `set(Map)` (ARSET/ARMSET), `delete(indexes…)`,
  `deleteRange(s)`, `count()`, `countMatches`, `length`, `range`, `scan`
  (flat + limit), `insert`, `ring`, `next`, `seek`, `lastItems(+Reversed)`,
  `getInfo`/`getFullInfo` (ARINFO [FULL], flat and nested reply forms both
  accepted), `grep`/`grepEntries` (`ArrayGrepArgs`: EXACT/MATCH values
  codec-encoded, GLOB/RE raw patterns, AND/OR, LIMIT, NOCASE), and the
  `AROP` aggregate family `sum/min/max/bitAnd/bitOr/bitXor`. Values ride
  the instance codec exactly like Java `encode()`.
- **`RCircularBuffer` full surface** (Java 4.8 `RedissonCircularBuffer`):
  capacity in the hash-tagged `redisson_acb:{name}` settings key (LongCodec
  plain digits — SETNX/SET/GET), Lua ports **verbatim** (ARING ring writes
  in 4000-chunks, ARNEXT peek, AROP MATCH/SUM/aggregate ops): `trySetCapacity`
  /`setCapacity`/`capacity`/`remainingCapacity`, `add`/`addAll`, `set`,
  `get`, `lastItems(+Reversed)`, `range`, `readAll`, `size`, `isFull`,
  `isEmpty`, `peekFirst`/`peekLast`, `count`/`contains`, `average`,
  `sum/min/max` (whole-buffer + range forms), `bitAnd/bitOr/bitXor`,
  `clear`.
- **`RBatch` module facades** (Java `RedissonBatch.getCountMin/
  getTimeSeriesNative`): `BatchCountMin` (CMS.INITBYDIM/INITBYPROB/INCRBY/
  QUERY/INFO) and `BatchTimeSeriesNative` (TS.CREATE/ADD/GET/RANGE/
  REVRANGE/INFO) queue the same commands the standalone classes issue;
  reads resolve on `execute()`. RBatch now has 15 typed facades.
- Tests: `live-phase73.test.ts` (+5: RArray two suites, RCircularBuffer,
  both batch facades — every suite COMMAND INFO-gated on `ARSET`/`ARRING`/
  `CMS.`/`TS.` with logged skip), +5 mock builder tests (`wire.test.ts`:
  ArrayGrepArgs predicate stacking/wire tokens). CI-driven fixes folded in:
  retention-safe dynamic timestamps in `live-phase71` (newer TimeSeries
  modules trim out-of-retention samples on write) + the RESP2-flat
  `TS.RANGE` value shape; `ARLASTITEMS` slot semantics (last n slots by
  index, holes included); ARGREP predicates compare stored bytes so grep
  needs `StringCodec` (same as Java's own test — documented on
  `ArrayGrepArgs`); **ioredis-mock's `TIME` mixes wall-clock seconds with
  `process.hrtime` sub-seconds (two uncorrelated clocks — the value can
  jump ±999ms between calls), silently breaking every `serverNowMs` TTL
  comparison at ~4% flake rate**; `mockClient()` now serves both fields
  from one `Date` call (30/30 green after the fix); phase44 alter-visibility
  retry under 73-file parallel load. First fully green CI run including
  live AR.*/CMS.*/TS.* validation: 39883266177.

## 2.51.0 — 2026-10-23

Phase 72: the last two Redisson 4.7/4.8 items — `SetExpiredListener` and
`NameMapper` — plus the RGcra config surface and a final 4.8-gap audit.

- **`SetExpiredListener` for `RSetCache`** (Java 4.7): `addListener({ onExpired(value) })`
  starts a ref-counted, `unref()`ed 5s poll (Java EvictionScheduler
  minCleanUpDelay ballpark) plus a dedicated subscription on
  `redisson_set_cache_expired:{name}` (Java channel name verbatim), alive only
  while an expired-listener is registered. Each sweep claims expired members
  zrem-first (at-most-once across concurrent pollers, including Java's own
  scheduler), drops idle/touch metadata and PUBLISHes `u64LE(len) +
  codec-encoded member` — byte-identical to the Java SetCacheEvictionTask Lua
  (`struct.pack('Lc0', ...)`) and SetCacheEventCodec (readLongLE + value
  decoder), so Java listeners on non-Windows servers decode TS events and vice
  versa. `pollExpiry()` stays public for tests / manual sweeps; the read-path
  `purge()` still evicts silently without events (same as Java read paths).
  Mock-safe: without pub/sub the sweep dispatches locally.
- **`NameMapper` / `PrefixNameMapper`** (Java 4.8): config-level
  `Redisson.create({ nameMapper })` maps every structure name inside the getXxx
  factories (65 sites, mechanical `mapped(name)`); companion keys derive from
  the mapped name — Java parity. `PrefixNameMapper(prefix)` never double-prefixes
  and `unmap` round-trips; `getPatternTopic` patterns and `RKeys` methods are
  not mapped/unmapped (documented limitation, matching Java users' primary use).
- **`RGcra` config surface** (Java 4.7): `trySetRate`/`setRate`/`getConfig` over
  the `{name}:config` hash (StringCodec fields `maxBurst`/`rate`/`period`;
  periods ms↔seconds with Java `toSeconds` shape), `tryAcquire([tokens])`
  returning the decoded 5-element `GcraResult` (limited/maxTokens/
  availableTokens/retryAfterSeconds/fullBurstAfterSeconds; TOKENS arg when
  tokens≠1), the Java Lua assert message when the rate is unset, and `delete()`
  clearing raw + config keys. The legacy `acquire(limit, periodMs)` stays.
- Tests: +3 mock (expired dispatch + stop-after-remove, prefix mapping +
  companion/lock/topic keys, Gcra config hash shape) and
  `live-phase72.test.ts` (+3: TS-only expired delivery with a raw
  `messageBuffer` subscriber asserting the u64LE wire payload, PrefixNameMapper
  against live keys, RGcra gated by COMMAND INFO — local Redis 8.6.0 ships no
  GCRA command, so the gated-skip path ran).
- Final 4.6.1→4.8.0 api audit: everything else portable is either already
  covered (phases 68-71), module-gated (full AR.* surface of RArray /
  RCircularBuffer; RBatch `getCountMin`/`getTimeSeriesNative` wrappers), PRO/AI,
  or Java-framework-only (codecs, Valkey AZ affinity, StorageMemoryUsageListener,
  Spring/Hibernate). Post-4.8.0 HEAD adds only `RBatch.getMap(name, MapOptions)`.

## 2.50.0 — 2026-10-23

Phase 71: Redisson 4.8 OSS additions — RTimeSeriesNative(+Natives) and RMaps.

- **`RTimeSeriesNative`** (new structure, Java 4.8): thin binding over the
  RedisTimeSeries module's `TS.*` commands — distinct from the ZSET-based
  `RTimeSeries` (no codec; a sample is a ms timestamp + double, labels index
  the series). `create/createIfAbsent` (EXISTS-guarded Lua, creation options
  RETENTION/ENCODING/CHUNK_SIZE/DUPLICATE_POLICY/IGNORE/LABELS via
  `TSCreateArgs`), `alter`, `add(ts,v)`/`addCurrent`/`add(TSAddArgs)` with
  ON_DUPLICATE, `addAll` (TS.MADD), `incrementBy`/`decrementBy`
  (`TSIncrArgs` TIMESTAMP + creation options), `removeRange`, `first`,
  `size`/`firstTimestamp`/`lastTimestamp` (TS.INFO projections), `get`/
  `getLatest` (TS.GET [LATEST]), `range`/`rangeReversed` (longs or
  `TSRangeArgs`: LATEST/FILTER_BY_TS/FILTER_BY_VALUE/COUNT/ALIGN/AGGREGATION
  multi-agg comma join/BUCKETTIMESTAMP/EMPTY with Java's align validations),
  `read` (TS.READ BLOCK/MAX_COUNT), `createRule`/`deleteRule`, paged
  `iterator`/`stream`, `getLabels`/`getInfo`/`getDebugInfo`.
- **`RTimeSeriesNatives`** (new, Java 4.8): cross-series TS.* — `addAll`
  (TS.MADD, reply split per key by sample count), `getAll` (TS.MGET),
  `range`/`rangeReversed` (TS.MRANGE/MREVRANGE incl. GROUPBY/REDUCE and
  EXCLUDEEMPTY with Java's mutual-exclusion checks), `groupedRange(+Reversed)`
  (TS.NRANGE/NREVRANGE), `queryIndex`, `labelNames`/`labelValues`
  (TS.QUERYLABELS). Arg classes `TSMultiGetArgs`/`TSMultiRangeArgs`/
  `TSGroupedRangeArgs` follow the Java factories 1:1. Everything is
  module-gated: no TS.* command is issued on plain Redis or ioredis-mock.
- **`RMaps`** (rebuilt, Java 4.8): mass writes of same-shaped Map objects —
  `set(maps[, batchSize])` groups objects by their encoded (byte-sorted)
  field set, `createImport(MapsImportArgs.fields(...).batchSize(n))` returns
  an `RMapsImport` session (`add(name, values...)` positional, buffer
  auto-flush at batchSize, `flush()`, `getImportedCount()`). Write path
  prefers Java's `HIMPORT PREPARE/SET` with the identical fieldsetName
  (`rs%08x` of HighwayHash-128 over length-prefixed encoded fields); servers
  without HIMPORT (plain Redis; ioredis-mock pipelines) are detected once and
  permanently use the pipelined DEL+HSET fallback, which stores a plain HASH
  with RMap's exact JSON field/value encoding — Java `RMap` reads it
  directly. Replaces the older infra.ts RMaps stand-in (setMap/readAll);
  factories `getMaps()`/`getRMaps()`.
- Factories: `getTimeSeriesNative`/`getRTimeSeriesNative`,
  `getTimeSeriesNatives`/`getRTimeSeriesNatives`, `getMaps`/`getRMaps`.
- Tests: +3 mock (RMaps fallback writes read back through RMap, import
  batching/guards, TS-native args wire shapes vs Java's
  appendCreationOptions/appendRangeOptions), +3 live
  (`live-phase71.test.ts`; TS.* groups gated per command via COMMAND INFO,
  RMaps raw-HASH wire checks — local Redis offline, exercised by CI's
  redis:8 job).

## 2.49.0 — 2026-10-23

Phase 70 (part 2): RStream read 4.7 params.

- `StreamReadGroupOptions` gains `maxCount` / `maxSize` (Java 4.7
  `maxCount()`/`maxSize()` — XREADGROUP MAXCOUNT/MAXSIZE, Redis 8.x; sent
  only when set, so older servers are unaffected until the option is used).
- `readMany(streams, {count, maxCount, maxSize})` options overload
  (XREAD COUNT/MAXCOUNT/MAXSIZE); the numeric-count form is unchanged.
- Mock suite 137/137 (wire 29 + redisson 108).

## 2.48.0 — 2026-10-23

Phase 70: RTimeSeries 4.8 expansion (full port of RedissonTimeSeries 4.8,
Lua verbatim). Full suite **334 tests / 73 files green with live Redis**.
Also fixes the CI java-interop job (see 05455e7 below).

- **Add family** (write the 4.8-native member shape: flag byte 4 +
  server-side zero-padded monotonic id via the seq HASH; legacy
  add/addAll keep the 4.6.1 flag-2/3 shape — do not mix families on one
  series): `add(TimeSeriesAddArgs)` with retention trimming,
  `addIfAbsent` (expired slots cleared and reused), `addOrReplace`,
  `addIfLess`/`addIfGreater` (fold policies), `addAndSum`, `addAndGet`
  (running total in the seq HASH with out-of-order increments landing at
  the higher timestamp).
- **Reads/aggregation/info**: `aggregate(TimeSeriesAggregationArgs)` →
  `TimeSeriesBucket[]` (one-Lua pass, Welford variance, 12 aggregations,
  label/value filters, alignment), `readTail(TimeSeriesReadArgs)` (count
  is a floor, never ends inside a timestamp), `info()` → `TimeSeriesInfo`,
  `getAll`/`getAllEntries`/`removeAll`/`getAndRemoveAll(+Entries)`,
  `rangeByLabel`/`rangeReversedByLabel`/`entryRangeByLabel`/
  `entryRangeReversedByLabel`/`removeRangeByLabel`, `labels()`.
- Args follow the repo factory style (`TimeSeriesAddArgs.entry(ts, v)` →
  `.label/.ttl/.retention`, `TimeSeriesAggregationArgs.between`,
  `TimeSeriesReadArgs.after`); readers parse both member shapes (flag 2/3
  legacy and flag 4).
- Mock safety: a `lua48()` gate routes mock clients (no Lua `struct`)
  to faithful client-side JS equivalents.
- **CI fix** (`05455e7`): the java probe classpath hardcoded the Windows
  `;` separator — on the Linux runner every probe test failed with
  ClassNotFoundException since the job was introduced; now joined with
  `path.delimiter`. live-phase69's CMS surface real-verified locally
  (SDIFFCARD/SUNIONCARD/LMOVEM still 8.10-gated — CI's redis:8 covers).
- Tests: +8 mock, +9 live (`live-phase70.test.ts` — flag-4 raw-member byte
  check, expired-slot reuse, aggregate filters on the real Lua path).

## 2.47.0 — 2026-10-23

Phase 69: upstream 4.8 OSS surface (part 1). Full suite:
**317 tests / 72 files** (local Redis offline at release time — live-phase69
cases are command-gated and exercised by CI's redis:8 service job).

- **`RCountMin`** (new structure, Java 4.8): Count-Min Sketch over the
  RedisBloom CMS commands — `init(CountMinInitArgs.byDimensions|byProbability)`
  (CMS.INITBYDIM/INITBYPROB), `add(element[, incr])` / `add(Map)` (CMS.INCRBY,
  negative increments rejected), `count(element|Collection)` (CMS.QUERY),
  `mergeWith(CountMinMergeArgs)` (CMS.MERGE + WEIGHTS), `getInfo()` (CMS.INFO).
  Factory `getCountMin(name)`.
- `RSet.countUnionApprox([limit,] names...)` (SUNIONCARD APPROX) and
  `countDiff([limit,] names...)` (SDIFFCARD) — Java 4.7.
- `RQueue.move(QueueMoveElementsArgs)` (LMOVEM; `to(dest).count(n)/.exactly(n)
  .bulk()/.oneByOne()`) and blocking `RBlockingQueue.move(timeoutMs, args)`
  (BLMOVEM, dedicated connection) — Java 4.7. RDeque/RBlockingDeque `move`
  signatures widened to overloads so both the LMOVE and LMOVEM forms coexist.
- `RRingBuffer.addAll(values)` — Java 4.8 bulk add (RPUSH 5000-chunks + LTRIM
  overflow in one Lua; mock fallback path).
- Tests: +2 mock (args shapes, negative guards, ring addAll), +2 live
  (`live-phase69`, CMS/SDIFFCARD/SUNIONCARD/LMOVEM all command-gated).
- Remaining 4.8 surface tracked for phase 70+: RTimeSeries 30+ method
  expansion, RTimeSeriesNative(+Natives), RMaps, RStream read/readGroup
  maxCount/maxSize, SetExpiredListener, PrefixNameMapper.

## 2.46.0 — 2026-10-23

Phase 68: upstream Redisson 4.7/4.8 interop alignment (local checkout
4.8.1-SNAPSHOT as source; 4.6.1 wire compat unchanged). Full suite:
**313 tests / 71 files** (live Redis incl. TS↔Java shared rate window).

- **`RRateLimiter` release/acquire Lua realigned with Java** (largest
  divergence found): `release()` now un-acquires permit members oldest-first
  (partial remainder re-added at the original score) instead of only bumping
  the counter, and the acquire script gains the 4.8 recalc branch — when
  returning expired permits would push the counter above rate, value is
  recomputed from the permits actually held in the window. Negative permits
  rejected (upstream #7383). Live drift scenario asserts available = 2 where
  the old shape yielded 7.
- `renamenx` (RObject + RKeys): EXISTS-guarded — a missing source returns
  false instead of an error (Java 4.8).
- `RFairLock` tryLock/lockWait: ambiguous response timeouts now run the
  waiter cleanup (`redisson_lock_queue`/`redisson_lock_timeout`) and return
  false instead of leaving zombie waiters (upstream #5019).
- New APIs from 4.7/4.8: `RKeys.time()` (server TIME as
  {seconds, microseconds}), `RRingBuffer.readNewest/readOldest/peekLast`,
  `RSearch.getAliases(index)` (FT.ALIASLIST passthrough — throws where the
  server lacks the command; gated in tests).
- Audited and NOT affected (verified against our shapes): RSetCache no-TTL
  visibility (we score MaxInt64), RMapCache putIfAbsent maxIdle=0 (guarded
  zrem), RSearch.hasIndex (FT._LIST based), RMap.entrySet().contains (tuple
  arrays), RBinaryStream read position (no client cursor), RTimeSeries
  same-timestamp reads (already filter-then-limit).
- Kept 4.6.1-parity limitation: RTimeSeries writes stay on the flag-2/3
  member shape — 4.8's flag-4 server-side monotonic ids would break 4.6.1
  readers (AGENTS.md law 12); >1e17 timestamps share Redis double-score
  precision limits with 4.6.1.
- Tests: +3 mock, +2 live (`test/live-phase68.test.ts`).

## 2.45.2 — 2026-10-22

Review follow-ups (2): duplication collapse, no behavior change. Full suite
**308 tests / 70 files green with live Redis** (pattern-topic once flaked
under 70-way parallel load, passes isolated and on rerun).

- `locks.ts`: five copies of the listener trio collapsed —
  `LockObservable` base (owns the `KeyspaceListenerHub` field + delegating
  `addListener`/`removeListener`/`newCondition`) behind
  RLock/RFairLock/RWBase; `UnsupportedLockSurface` base (throwing trio)
  behind RMultiLock/RRedLock.
- `infra.ts`: `pushCommonFlags` extracts the identical
  INDEXMISSING/SORTABLE/NOINDEX tail from the TEXT/TAG/NUMERIC/GEO schema
  cases (GEOSHAPE flag order differs, VECTOR has none — both left alone).
- `collections.ts` RBlockingQueue/RBlockingDeque method pairs deliberately
  NOT deduped: 3-line interface plumbing across sibling classes (mirrors
  Java's own sibling layout); extraction would only trade duplication for
  forwarding.

## 2.45.1 — 2026-10-22

Review follow-ups from the two-axis audit of the full 2-week history
(root → 2.45.0). No wire-format changes.

- PRO-only refusal completed: `getLocalCachedMapCache` /
  `getReliablePubSubTopic` / `getReliableFanout` / `getLeasedMap` factory
  stubs now throw like the existing two (previously missing → raw
  `TypeError`). Verified against the 4.6.1 OSS jar: all six PRO interfaces
  exist there, only the implementations are PRO.
- Fallback catches narrowed: `RMapCacheNative.putAll` (HSETEX→HSET) and
  `RSet.countIntersection` (SINTERCARD→SMEMBERS) now re-throw unless the
  error is an unsupported-command failure (`isUnsupportedCommand` moved to
  `util.ts` and shared) — WRONGTYPE-class errors no longer silently take
  the slow path. Mock fallbacks unaffected (`call is not a function`
  matches the guard).
- Live tests log their skip when Redis is unreachable (AGENTS.md 带日志
  skip，绝不静默过): one `console.warn` per live-*.test.ts file (66 files).
- Docs: GAP_ANALYSIS §4 corrected — `RReliablePubSubTopic` /
  `RLocalCachedMapCache` are 4.6.1-present interfaces with PRO impls, not
  "版本外" (only `RCountMin` / `RTimeSeriesNative` are truly absent from
  the jar); README / COMPATIBILITY / AGENTS PRO lists completed with
  `RLeasedMap` / `RReliableFanout`.
- Tests: +1 mock (`PRO-only factory refusal`, all six stubs).

## 2.45.0 — 2026-10-22

Phase 67: bucket/zset listeners + MultiLock rejection. Full suite:
**307 tests / 70 files**.

- `RBucket.addListener` / `removeListener`: `onSet` → `set`, plus
  `onExpired`/`onDeleted`.
- `RScoredSortedSet.addListener`: `onAdd` → `zadd`, `onRemove` → `zrem`,
  `onIncrement` → `zincr`, `onStore` → `zunionstore`/`zinterstore`/`zdiffstore`.
- `RMultiLock` / `RRedLock` `addListener`/`removeListener` now throw
  UnsupportedOperationException, matching Java 4.6.1.
- Tests: 1 mock + `test/live-phase67.test.ts` 1/1 (bucket set/expired/del,
  zset add/incr/remove/store, removal; shared keyspace config lock).

## 2.44.0 — 2026-10-21

Phase 66: probabilistic advanced argument objects. Full suite:
**305 tests / 69 files**.

- **`TDigestMergeArgs`** (keys/compression/override) accepted by
  `RTDigest.mergeWith` alongside the variadic form.
- **`CuckooFilterInitArgs`** (capacity/bucketSize/maxIterations/expansion) and
  **`CuckooFilterAddArgs`** (items/capacity/noCreate) for
  `init`/`add`/`addIfAbsent` (CF.INSERT/CF.INSERTNX; returns the added subset).
- **`TopKInitArgs`** (topK/width/depth/decay, defaults 8/7/0.9) plus batch
  `incrementBy(Map)`, `contains(list)`, `count(list)`.
- Tests: `test/live-phase66.test.ts` 1/1 (TDigest merge replace semantics,
  Cuckoo init/add/insertnx, TopK init/batches; command-gated).

## 2.43.0 — 2026-10-21

Phase 65: lock keyspace listeners. Full suite:
**304 tests / 68 files**.

- `RLock`, `RFairLock` and RW read/write views expose
  `addListener` / `removeListener` mapped to `expired`/`del` keyspace events,
  matching Java's inherited `RedissonObject.addListener` (ExpiredObjectListener
  / DeletedObjectListener).
- Tests: 1 mock (registration/removal on all lock types) +
  `test/live-phase65.test.ts` 1/1 (lease expiry → expired, unlock → deleted,
  removal silence; config uses the shared lock).

## 2.42.0 — 2026-10-21

Phase 64: local cached map expiration events. Full suite:
**302 tests / 67 files**.

- `LocalCachedMapOptions.expirationEventPolicy`:
  `DONT_SUBSCRIBE` / `SUBSCRIBE_WITH_KEYEVENT_PATTERN` (default) /
  `SUBSCRIBE_WITH_KEYSPACE_CHANNEL`.
- In keyevent mode the invalidation connection PSUBSCRIBEs `expired` and
  `hexpired`; any event for the map key clears the near cache (Redis 8.6
  keyevent carries only the key name, so field-level expiry invalidates the
  whole local map entry set for that key).
- Reconnection re-subscribes the expiration patterns as well.
- Tests: 1 mock (both policies) + `test/live-phase64.test.ts` 1/1 (expiry
  clears the cache; DONT_SUBSCRIBE keeps it; keyspace config uses the shared
  lock).
- Test stabilization: SVS KNN assertions check the result set rather than
  approximate ranking order (SVS may return equidistant/small-set results in
  any order).

## 2.41.0 — 2026-10-21

Phase 63: local cached map reconnection strategies. Full suite:
**300 tests / 66 files**.

- `LocalCachedMapOptions.reconnectionStrategy` (`NONE`/`CLEAR`/`LOAD`).
  After the invalidation subscription reconnects, `CLEAR` drops the near
  cache and `LOAD` refreshes every locally cached field from Redis (an
  equivalent, stronger form of Java's invalidation-log replay).
- Initial connection does not trigger a strategy; only real reconnects do.
- Tests: 1 mock (all three strategies, cache snapshot) +
  `test/live-phase63.test.ts` 1/1 (forced disconnect/reconnect: CLEAR empties,
  LOAD observes a write missed while disconnected).

## 2.40.0 — 2026-10-21

Phase 62: RSetCache listeners + newCondition parity. Full suite:
**298 tests / 65 files**.

- `RSetCache.addListener` / `removeListener`: `onAdd` → `zadd`,
  `onRemove` → `zrem` via keyspace notifications; `SetExpiredListener` is not
  implemented (Java uses a local expiry scheduler).
- `newCondition()` now exists on `RLock`, `RFairLock`, RW read/write views,
  `RMultiLock` and `RRedLock`, throwing `UnsupportedOperationException` — the
  exact Java 4.6.1 behavior (RedissonBaseLock has a TODO).
- Tests: 1 mock (SetCache registration + newCondition everywhere) +
  `test/live-phase62.test.ts` 1/1 (zadd/zrem delivery, removal silence,
  condition errors; config uses the shared lock).

## 2.39.0 — 2026-10-21

Phase 61: IncrementArgs via Lua-CAS. Full suite:
**296 tests / 64 files**.

- `RAtomicLong.incrementAndGet(LongIncrementArgs)` and
  `RAtomicDouble.incrementAndGet(DoubleIncrementArgs)`: `by`, `lowerBound`,
  `upperBound`, `saturate`, `timeToLiveMs`, `expireAt`, `persist`,
  `expireIfNotSet` (ENX semantics: keep the old TTL).
- Vanilla Redis has no INCREX/DECREX, so this is a compare-and-set loop with
  server-side Lua: exact BigInt arithmetic for Long, existing TTL preserved by
  default, PERSIST/PX/PXAT applied atomically in the CAS.
- Out-of-bounds without SATURATE returns the current value and leaves value +
  TTL untouched (Java docs semantics).
- Tests: 1 mock (bounds/saturation, BigInt, 10 concurrent CAS increments,
  Double) + `test/live-phase61.test.ts` 1/1 (20 concurrent increments, BigInt
  2^53+1, TTL/ENX/PERSIST/PXAT, Double bounds).
- Fix found during regression: Redis `VLINKS` returns one neighbor array per
  HNSW layer; `getNeighbors`/`getNeighborEntries` now flatten all layers like
  Java instead of reading only layer 0.
- Test stabilization: the Java reliable-topic fanout probe deduplicates group
  deliveries (Redisson can occasionally deliver the same group twice), while
  still requiring both G1 and G2.

## 2.38.0 — 2026-10-21

Phase 60: conditional compareAndDelete. Full suite:
**294 tests / 63 files**.

- `RAtomicLong` / `RAtomicDouble.compareAndDelete(CompareAndDeleteArgs)`:
  atomic `<`, `<=`, `>`, `>=`, `==`, `~=` threshold checks followed by DEL
  (single Lua, same shape as Java 4.6.1).
- `CompareAndDeleteArgs` exposes the Java factory helpers
  (`less`, `lessOrEqual`, `greater`, `greaterOrEqual`, `equal`, `notEqual`);
  the existing equality overload (`compareAndDelete(value)`) is unchanged.
- Tests: 1 mock + `test/live-phase60.test.ts` 1/1 (all operators, mismatch
  leaves the value, Long and Double equality).

## 2.37.0 — 2026-10-21

Phase 59: map field listeners. Full suite:
**292 tests / 62 files**.

- **`RMap.addListener` / `removeListener`**: `onPut` → `hset`,
  `onIncrement` → `hincrbyfloat`, `onRemove` → `hdel`.
- **`RMapCacheNative.addListener`** adds `onExpired` → `hexpired` and
  `onClearExpire` → `hpersist` on top of put/incr/remove.
- Redis 8.6 emits only `__keyevent` (no `__subkeyevent`), so callbacks get
  `field = null` — the same fallback Java applies when no subkey event
  arrives.
- `RMap.addAndGet` stays on the exact-decimal Lua path (HSET), so it emits
  `hset` rather than `hincrbyfloat`; raw HINCRBYFLOAT events still reach
  `onIncrement`.
- Tests: 1 mock (registration/removal on both maps) +
  `test/live-phase59.test.ts` 1/1 (put/incr/remove, native expired/clear,
  removal silence; keyspace config uses the shared lock).

## 2.36.0 — 2026-10-21

Phase 58: batch field TTLs for native map caches. Full suite:
**290 tests / 61 files**.

- `RMapCacheNative.remainTimeToLive(keys[])`: one `HPTTL ... FIELDS n`
  returning `Map<K, number>` (`-2` missing, `-1` no TTL); per-key fallback for
  mocks/older servers.
- `RMapCacheNative.clearExpire(keys[])`: one `HPERSIST ... FIELDS n`
  returning `Map<K, boolean>`; single-key and whole-HASH forms unchanged.
- Tests: 1 mock (keyed maps, empty arrays) + `test/live-phase58.test.ts` 1/1
  (HPTTL values, HPERSIST clearing, missing keys, empty batches).

## 2.35.0 — 2026-10-21

Phase 57: SVS-VAMANA structured search indexes. Full suite:
**288 tests / 60 files**.

- `SearchVectorField.algorithm` accepts `SVS-VAMANA`; parameters
  COMPRESSION (LVQ8/LVQ4/LVQ4x4/LVQ4x8/LeanVec4x8/LeanVec8x8),
  CONSTRUCTION_WINDOW_SIZE, GRAPH_MAX_DEGREE, SEARCH_WINDOW_SIZE, EPSILON,
  TRAINING_THRESHOLD and LEANVEC_DIM.
- Tests: `test/live-phase57.test.ts` 1/1 (plain SVS KNN plus a compressed
  field with all build options; FT.INFO). Redis requires
  TRAINING_THRESHOLD ≥ 1024, documented.

## 2.34.0 — 2026-10-21

Phase 56: RMap batched putAll. Full suite:
**287 tests / 59 files**.

- `RMap.putAll(map[, batchSize])`: HSET in field batches (default 1000,
  Java 4.6.1's overload); `batchSize` must be a positive integer.
- Tests: 1 mock (5 fields batch 2, Map input, validation, empty map) +
  `test/live-phase56.test.ts` 1/1 (field batching, Java `map-get` read).

## 2.33.0 — 2026-10-21

Phase 55: RVectorSet iterator/stream. Full suite:
**285 tests / 58 files**.

- `RVectorSet.iterator(batchSize = 10)` pages with `VRANGE start + count`,
  resuming at `(` + last element (exclusive) exactly like Java
  `RedissonVectorSet`; `stream()` yields the same lazy sequence.
- Tests: `test/live-phase55.test.ts` 1/1 (non-alphabetical insertion order,
  batch sizes 2 and 4, empty set; capability-gated on VADD).

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
