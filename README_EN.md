# redi.ts — TypeScript 1:1 Port of Redisson

[中文](README.md) | **English**

Wire-compatible with **Java Redisson** (`JsonJacksonCodec`, default typing),
**redi.py** (Python) and **redi.go** (Go): identical Redis key layouts, identical
Lua algorithms, identical value encoding. Data written by one client is readable
by the others. The Java sources in `C:/GitHub/redisson` are normative; the
`redi.go` Lua scripts (verified against Redisson 4.6.1) are the direct port baseline.

## Install

```sh
npm install redi.ts ioredis
```

Dual publish: ESM via `import { Redisson } from 'redi.ts'` and CommonJS via
`const { Redisson } = require('redi.ts')` — the `exports` map points to
`dist/esm` and `dist/cjs` respectively and both produce identical wire output
(verified by `npm run pack-smoke`).

## Quick Start

```ts
import { Redisson } from 'redi.ts';

const redisson = Redisson.create({ address: 'redis://127.0.0.1:6379' });

// Bucket (JSON codec, @class-wrapped like Redisson)
await redisson.getBucket('any').set({ n: 1 });

// Reentrant lock (HASH + redisson_lock__channel:{name} + watchdog)
const lock = redisson.getLock('myLock');
await lock.lock();
try { /* ... */ } finally { await lock.unlock(); }

// Map (HASH, JSON-encoded fields)
await redisson.getMap('m').put('k', 'v');

// Rate limiter / semaphore / latch / bloom / stream / topic / ...
await redisson.getRateLimiter('api').setRate('OVERALL', 10, 1000);

// Delayed queue: start() migrates expired members in the background on a
// deadline timer (Java QueueTransferTask scheduling); poll()/peek() still
// work without start()
const dq = redisson.getDelayedQueue('dq');
await dq.start();
await dq.offer('later', 5000);

await redisson.shutdown();
```

With an existing client (cluster / sentinel / TLS):

```ts
import Redis from 'ioredis';
const redis = new Redis.Cluster([{ host: '127.0.0.1', port: 7000 }]);
const redisson = Redisson.create({ client: redis });
```

On a cluster all keys of a structure must share one slot. Every companion key
here carries a hash tag — `prefixName`/`suffixName` wrap a bare name as
`{name}`, which hashes exactly like `name` itself — so the structures above
work on a Cluster out of the box; `test/live-cluster.test.ts` exercises a real
3-master cluster (`scripts/cluster-up.sh`).

## Coverage

60+ structures: locks (`RLock`/`Fair`/`ReadWrite`/`Spin`/`NonReentrant`/`Fenced`
with `tryLockAndGetToken` wait/non-blocking forms/`Multi`/`Red`, plus Java-style
key synchronizers through
`map.getLock(key)`/`getFairLock`/`getReadWriteLock`/`getSemaphore`/
`getCountDownLatch`/`getPermitExpirableSemaphore` with byte-identical companion
names), `RMap` (lazy HSCAN iterators, `HRANDFIELD`/`HSTRLEN`)/
`MapCache`(+Native)/`LocalCachedMap` (LRU/LFU under `maxSize`, `getCachedMap()`
view), `RSet`/`SetCache`,
`RList`/`RQueue`/`RDeque`(+Blocking/Bounded; `pollFromAny`/BLMPOP,
`move`/`moveWithTimeout`/LMOVE/BLMOVE), `RScoredSortedSet` (score
ranges incl. reversed forms, `addScore` family, deletion by score/rank),
`RLexSortedSet` (raw members), `RBucket`/`RBuckets`/`RBinaryStream`/`RJsonBucket`,
`RAtomicLong`/`Double`, `RLongAdder`/`RDoubleAdder`, `RIdGenerator`,
`RSemaphore`/`RPermitExpirableSemaphore`/`RCountDownLatch`/`RRateLimiter`
(keepAlive renewed inside the acquire Lua),
`RTopic`/`RPatternTopic`/`RShardedTopic`/`RReliableTopic` (Stream, field `m`),
`RStream` (JSON field names), `RDelayedQueue` (dual ZSET+LIST, `Bc0Lc0`),
`RRingBuffer`, `RGeo` (JSON members; `GeoSearchArgs` radius/box search,
distance/position results, `GEOSEARCHSTORE`), `RHyperLogLog`, `RBitSet`
(native bit order; `BITFIELD`, Redis 8 `BITOP` DIFF/DIFF1/ANDOR/ONE),
`RTimeSeries` (`BBc0Lc0Lc0`), `RBloomFilter` (HighwayHash-128, verified vectors),
`RKeys`/`RScript`/`RFunction`/`RSearch`/`RVectorSet`, priority queues
(`GO_ONLY`, ZSET+score), `RTransferQueue` (`GO_ONLY`), `RBatch` (13 typed
facades), …
See [COMPATIBILITY.md](COMPATIBILITY.md).

Honest non-goals (same as redi.go): `RSortedSet` (LIST+Comparator),
`RTransaction` (snapshot rollback), `LiveObject`, reactive/Rx, `RRemoteService`/
`RExecutorService` wire protocol, PRO-only (`RReliableQueue`,
`RLocalCachedMapCache`, `RReliablePubSubTopic`, `RBitVectorStore`).

## Codec

`JsonCodec` (default): scalars stay bare; integers outside int32 range become
`["java.lang.Long", v]`; objects gain `@class: java.util.LinkedHashMap`;
arrays become `["java.util.ArrayList", [...]]` (recursive). Decoding strips all
wrappers, so bare JSON from redi.py and raw strings also read fine.
`StringCodec` is raw-string; `RBinaryStream`/`RLexSortedSet` bypass the codec.

## Tests

```sh
npm test             # 188 tests: wire vectors, mock functional, live python
                     # interop, live bidirectional suites vs Redisson 4.6.1
npm run build        # dual output: dist/cjs + dist/esm
npm run pack-smoke   # npm pack → temp consumer: CJS and ESM both resolve
                     # and emit identical wire output
```

- `test/interop.test.ts` spawns `python` with `C:/GitHub/redi.py` to assert
  identical codec outputs, HighwayHash vectors and multimap internal ids
  (no Redis needed).
- `test/live-*.test.ts` need a real Redis at `127.0.0.1:6379` (skip otherwise)
  and the Maven probe in `interop/` (Redisson 4.6.1 + `JsonJacksonCodec`):
  data structures both directions, lock mutual exclusion, semaphore/rate-limiter
  sharing, RWLock/fenced/delayed/reliable-topic/timeseries/adder/PES,
  topic/stream-groups/multimaps/queues/generators/batch/TTL, pattern/sharded
  topics, geo/bitset/HLL/caches/vectors, bounded-queue permits and blocking
  connection isolation, companion-key lifecycle, MapCache events, functions
  and more. Commands missing on the local server (`SUNIONCARD`/`XNACK` on
  Redis 8.6) are capability-gated via `COMMAND INFO` and skipped with a log
  line — never silently passed.
- `test/live-cluster.test.ts` needs a local 3-master cluster; start it from
  WSL with `wsl -e bash scripts/cluster-up.sh` (ports 7100-7102, tear down
  with `scripts/cluster-down.sh`). It logs a warning and skips when the
  cluster is unreachable.
  See [interop/README.md](interop/README.md) and [COMPATIBILITY.md](COMPATIBILITY.md).
