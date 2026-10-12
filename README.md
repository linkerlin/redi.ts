# redi.ts — TypeScript 1:1 port of Redisson

[English](README_EN.md) | 中文

Wire-compatible with **Java Redisson** (`JsonJacksonCodec`, default typing),
**redi.py** (Python) and **redi.go** (Go): same Redis key layouts, same Lua
algorithms, same value encoding. Data written by one client is readable by the
others. Java sources in `C:/GitHub/redisson` are normative; `redi.go` Lua
(verified against Redisson 4.6.1) is the direct port baseline.

## Install

```sh
npm install redi.ts ioredis
```

## Quick start

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

await redisson.shutdown();
```

With an existing client (cluster / sentinel / TLS):

```ts
import Redis from 'ioredis';
const redis = new Redis.Cluster([{ host: '127.0.0.1', port: 7000 }]);
const redisson = Redisson.create({ client: redis as any });
```

## Coverage

60+ structures: locks (`RLock`/`Fair`/`ReadWrite`/`Spin`/`NonReentrant`/`Fenced`/
`Multi`/`Red`), `RMap`/`MapCache`(+Native)/`LocalCachedMap`, `RSet`/`SetCache`,
`RList`/`RQueue`/`RDeque`(+Blocking/Bounded), `RScoredSortedSet`,
`RLexSortedSet` (raw members), `RBucket`/`RBuckets`/`RBinaryStream`/`RJsonBucket`,
`RAtomicLong`/`Double`, `RLongAdder`/`RDoubleAdder`, `RIdGenerator`,
`RSemaphore`/`RPermitExpirableSemaphore`/`RCountDownLatch`/`RRateLimiter`,
`RTopic`/`RPatternTopic`/`RShardedTopic`/`RReliableTopic` (Stream, field `m`),
`RStream` (JSON field names), `RDelayedQueue` (dual ZSET+LIST, `Bc0Lc0`),
`RRingBuffer`, `RGeo` (JSON members), `RHyperLogLog`, `RBitSet` (native bit order),
`RTimeSeries` (`BBc0Lc0Lc0`), `RBloomFilter` (HighwayHash-128, verified vectors),
`RKeys`/`RScript`/`RFunction`/`RSearch`/`RVectorSet`, priority queues
(`GO_ONLY`, ZSET+score), `RTransferQueue` (`GO_ONLY`), `RBatch`, …
See [COMPATIBILITY.md](COMPATIBILITY.md).

Honest non-goals (same as redi.go): `RSortedSet` (LIST+Comparator),
`RTransaction` (snapshot rollback), `LiveObject`, reactive/Rx, `RRemoteService`/
`RExecutorService` wire protocol, PRO-only (`RReliableQueue`,
`RLocalCachedMapCache`, `RReliablePubSubTopic`, `RBitVectorStore`).

## Codec

`JsonCodec` (default): scalars bare; integers outside int32 →
`["java.lang.Long", v]`; objects gain `@class: java.util.LinkedHashMap`;
arrays become `["java.util.ArrayList", [...]]` (recursive). Decode strips all
wrappers, so bare JSON from redi.py and raw strings also read fine.
`StringCodec` is raw-string; `RBinaryStream`/`RLexSortedSet` bypass the codec.

## Tests

```sh
npm test    # vitest: wire vectors + mock functional + live python interop (no Redis needed for interop)
npm run build
```

`test/interop.test.ts` spawns `python` with `C:/GitHub/redi.py` to assert
identical codec outputs, HighwayHash vectors and multimap internal ids.
With a real Redis at `127.0.0.1:6379`, the mock suites also run against it by
passing a real client to `Redisson.create({ client })`.
