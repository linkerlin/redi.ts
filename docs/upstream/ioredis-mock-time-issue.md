# Ready-to-paste GitHub issue for stipsan/ioredis-mock

> Not submitted — kept here for review. Companion patch: `ioredis-mock-time-fix.patch`.

**Title:** `time()` mixes wall-clock seconds with `process.hrtime()` sub-seconds, producing jumps of up to ±999 ms

## Body

### Summary

`time()` composes its reply from two unrelated clocks: the **seconds** field
comes from `new Date().getTime()` (wall clock) while the **microseconds**
field comes from `process.hrtime()[1]` (monotonic clock relative to process
start). The two clocks' second-boundaries are uncorrelated, so any client
that recombines the reply into milliseconds (`s * 1000 + us / 1000` — what
ioredis clients, Redisson-style TTL math and most tooling do) gets a value
that is offset from real time by an arbitrary constant 0–999 ms **and steps
by ±~1000 ms** whenever one clock rolls over a second before the other. The
composed "server time" can even run backwards. This silently breaks TTL /
expiry logic that trusts `TIME` in tests (we hit this in CI: fixed- TTL
entries appeared to expire early or late at random, depending on process
start phase).

### Reproduction

```js
import Mock from 'ioredis-mock'

const redis = new Mock()
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

for (let i = 0; i < 14; i++) {
  const [s, us] = await redis.time()
  const composedMs = Number(s) * 1000 + Number(us) / 1000
  const wallMs = Date.now()
  console.log('composed', composedMs.toFixed(1), 'Date.now()', wallMs,
              'skew', (composedMs - wallMs).toFixed(1))
  await sleep(120)
}
redis.disconnect()
```

Actual output (ioredis-mock 8.13.1, the skew flips between two values ~1000 ms
apart, and `composed` goes backwards, e.g. `...432976.7 -> ...432102.4`):

```
composed 1792759342463.2 Date.now() 1792759341673 skew 790.2
composed 1792759342599.1 Date.now() 1792759341809 skew 790.1
composed 1792759342724.5 Date.now() 1792759341934 skew 790.5
composed 1792759342849.7 Date.now() 1792759342060 skew 789.7
composed 1792759342976.7 Date.now() 1792759342187 skew 789.7
composed 1792759342102.4 Date.now() 1792759342312 skew -209.6   <- -999.8 ms step, time went backwards
composed 1792759342228.3 Date.now() 1792759342438 skew -209.7
composed 1792759343352.2 Date.now() 1792759342562 skew 790.2    <- +999.8 ms step
composed 1792759343477.8 Date.now() 1792759342688 skew 789.8
composed 1792759343605.0 Date.now() 1792759342815 skew 790.0
composed 1792759343730.7 Date.now() 1792759342941 skew 789.7
composed 1792759343856.4 Date.now() 1792759343066 skew 790.4
composed 1792759343983.9 Date.now() 1792759343194 skew 789.9
composed 1792759343108.9 Date.now() 1792759343319 skew -210.1   <- -999.9 ms step
```

### Expected behavior

`composed` stays within one rounding unit of `Date.now()`: a single-digit-ms
skew that never steps. `TIME` should behave like the real command — one
consistent point in time split across both fields:

```
composed 1792759342463.0 Date.now() 1792759342463 skew 0.0
composed 1792759342583.0 Date.now() 1792759342583 skew 0.0
...
```

### Cause

`src/commands/time.js`:

```js
export function time() {
  return [
    `${Math.round(new Date().getTime() / 1000)}`,        // wall clock
    `${Math.round(process.hrtime()[1] / 1000)}`,         // monotonic, unrelated
  ]
}
```

### Suggested fix

Derive both fields from **one** `Date` call — milliseconds of the same
instant become the microseconds field (resolution drops to 1 ms, which is
what a mock reasonably provides, and both fields now describe the same point
in time):

```diff
 export function time() {
-  return [
-    `${Math.round(new Date().getTime() / 1000)}`,
-    `${Math.round(process.hrtime()[1] / 1000)}`,
-  ]
+  const now = new Date().getTime()
+  const seconds = Math.floor(now / 1000)
+  const microseconds = (now % 1000) * 1000
+  return [`${seconds}`, `${microseconds}`]
 }
```

Reply shape is unchanged (array of two strings), `timeBuffer()` keeps
working, and the composed millisecond value tracks `Date.now()` exactly.
Full patch: `ioredis-mock-time-fix.patch` (applies to `src/commands/time.js`
on v8.13.1 / current `main`).

### Environment

- ioredis-mock: 8.13.1
- Node: v26.8.1 (any version — the clocks are uncorrelated on all of them)
- OS: Windows 11 10.0.26200 (platform-independent)
