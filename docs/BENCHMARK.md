# Benchmark

The table below comes from `npm run bench` (`scripts/bench.mjs`), a
**sequential-latency microbenchmark**: every operation is one awaited
round trip from a single Node client, so `ops/sec` here is simply the
reciprocal of per-op latency — it is *not* a throughput comparison with
Java Redisson (whose Netty pipeline, thread model and batching change the
picture entirely). Use it to spot relative wire costs between structures
and regressions over time on the same machine, nothing more.

RTimeSeries `entryRange` collects the window **server-side in one Lua**
(Java `COLLECT_RANGE` shape, with the `anyExpired` fast path) — 64-entry
windows measure ~1.7 ms below. Before 2.53.1 the TTL check ran per member
client-side (64 extra sequential round trips, ~52 ms on this machine).
RArray requires Redis >= 8.8 (`ARSET`); the script skips it with a log
line when the server lacks the command.

| environment | |
|---|---|
| date | 2026-10-23 (after 2.53.1 server-side collect) |
| os | win32 10.0.26200 (x64) |
| cpu | 13th Gen Intel(R) Core(TM) i9-13900HX x 32 |
| node | v26.8.1 |
| redis | 8.10.2 |
| REDIS_URL | redis://127.0.0.1:6390 |
| OPS | 2000 (sequential awaits per op) |

| structure | op | ops/sec | avg latency µs |
|---|---|---:|---:|
| RBucket | set | 2963 | 337.5 |
| RBucket | get | 2347 | 426.0 |
| RMap | put | 2498 | 400.3 |
| RMap | get | 2543 | 393.2 |
| RLock | lock+unlock | 1151 | 868.9 |
| RQueue | offer | 1866 | 535.9 |
| RQueue | poll | 2804 | 356.6 |
| RTimeSeries | add(entry) | 1091 | 916.6 |
| RTimeSeries | entryRange | 577 | 1732.3 |
| RArray | set | 2313 | 432.4 |
| RArray | get | 1742 | 574.0 |
