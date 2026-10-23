# Benchmark

The table below comes from `npm run bench` (`scripts/bench.mjs`), a
**sequential-latency microbenchmark**: every operation is one awaited
round trip from a single Node client, so `ops/sec` here is simply the
reciprocal of per-op latency — it is *not* a throughput comparison with
Java Redisson (whose Netty pipeline, thread model and batching change the
picture entirely). Use it to spot relative wire costs between structures
and regressions over time on the same machine, nothing more.

RTimeSeries `entryRange` is deliberately included with its real per-entry
cost: the Java-compatible read path checks each returned member's TTL
individually, so a 64-entry range costs 64 extra sequential round trips
(~52 ms below). RArray requires Redis >= 8.8 (`ARSET`); the script skips
it with a log line when the server lacks the command.

| environment | |
|---|---|
| date | 2026-10-23T12:39:18.766Z |
| os | win32 10.0.26200 (x64) |
| cpu | 13th Gen Intel(R) Core(TM) i9-13900HX x 32 |
| node | v26.8.1 |
| redis | 8.10.2 |
| REDIS_URL | redis://127.0.0.1:6390 |
| OPS | 2000 (sequential awaits per op) |

| structure | op | ops/sec | avg latency µs |
|---|---|---:|---:|
| RBucket | set | 2817 | 355.0 |
| RBucket | get | 3319 | 301.3 |
| RMap | put | 2955 | 338.4 |
| RMap | get | 3165 | 315.9 |
| RLock | lock+unlock | 1097 | 911.9 |
| RQueue | offer | 3135 | 319.0 |
| RQueue | poll | 2847 | 351.2 |
| RTimeSeries | add(entry) | 1427 | 700.8 |
| RTimeSeries | entryRange | 19 | 52697.0 |
| RArray | set | 1661 | 602.1 |
| RArray | get | 2034 | 491.8 |
