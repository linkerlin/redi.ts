/**
 * Sequential-latency microbenchmark for a handful of core structures
 * (RBucket / RMap / RLock / RQueue / RTimeSeries / RArray) against a live
 * Redis. Each op is awaited in sequence — numbers measure round-trip latency
 * of a single client, NOT server throughput.
 *
 * Usage:
 *   npm run build                       # bench drives ./dist/esm
 *   REDIS_URL=redis://127.0.0.1:6379 OPS=2000 npm run bench
 *
 * Env:
 *   REDIS_URL  default redis://127.0.0.1:6379 (exits with a message when
 *              unreachable — never hangs)
 *   OPS        iterations per op, default 2000
 *
 * Keys live under the `TsBench:` namespace and are removed by exact-name DEL
 * at the end (companion keys are hash-tagged, so pattern cleanup would miss
 * them — iron law 9).
 */
import { existsSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import os from 'node:os';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distEntry = path.join(root, 'dist', 'esm', 'index.js');
if (!existsSync(distEntry)) {
  console.error(`bench: ${distEntry} not found — run \`npm run build\` first.`);
  process.exit(1);
}

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const OPS = Number.parseInt(process.env.OPS || '2000', 10) || 2000;

const NS = 'TsBench:';
/** Every key this script can create (main + deterministic companions). */
const CREATED_KEYS = [
  `${NS}bucket`,
  `${NS}map`,
  `${NS}lock`,
  `${NS}queue`,
  `${NS}ts`,
  `redisson__ts_ttl:{${NS}ts}`,
  `redisson__ts_seq:{${NS}ts}`,
  `${NS}array`,
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// -- connectivity probe: fail fast with a message instead of hanging --------
const { default: Redis } = await import('ioredis');
const probe = new Redis(REDIS_URL, {
  lazyConnect: true,
  connectTimeout: 3000,
  maxRetriesPerRequest: 0,
  retryStrategy: () => null,
});
probe.on('error', () => undefined); // surfaced via the connect() rejection below

let infoServer;
try {
  await Promise.race([probe.connect(), sleep(5000).then(() => { throw new Error('timeout'); })]);
  infoServer = await probe.info('server');
} catch {
  console.error(`bench: Redis at ${REDIS_URL} is unreachable — start it (or point REDIS_URL at a live server) and retry.`);
  try { probe.disconnect(); } catch { /* noop */ }
  process.exit(1);
}
const redisVersion = (infoServer.match(/redis_version:(\S+)/) || [])[1] || 'unknown';

// RArray needs Redis >= 8.8 (ARSET); gate via COMMAND INFO like the live suites.
async function hasArset() {
  try {
    const info = await probe.call('COMMAND', 'INFO', 'arset');
    return Array.isArray(info) && info.length > 0 && Array.isArray(info[0]);
  } catch {
    return false;
  }
}
const arsetOk = await hasArset();
if (!arsetOk) {
  console.log('bench: ARSET unavailable (server < 8.8) — skipping RArray set/get');
}

const { Redisson, TimeSeriesAddArgs } = await import(pathToFileURL(distEntry).href);
const redisson = Redisson.create({ address: REDIS_URL });

// -- measured loops ----------------------------------------------------------
const rows = [];
async function measure(structure, op, fn) {
  for (let i = 0; i < Math.min(50, OPS); i++) await fn(i); // warm-up
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < OPS; i++) await fn(i);
  const sec = Number(process.hrtime.bigint() - t0) / 1e9;
  rows.push({ structure, op, opsPerSec: OPS / sec, avgUs: (sec / OPS) * 1e6 });
}

const bucket = redisson.getBucket(`${NS}bucket`);
await measure('RBucket', 'set', (i) => bucket.set(`v${i}`));
await measure('RBucket', 'get', () => bucket.get());

const map = redisson.getMap(`${NS}map`);
await measure('RMap', 'put', (i) => map.put(`k${i % 64}`, `v${i}`));
await measure('RMap', 'get', (i) => map.get(`k${i % 64}`));

const lock = redisson.getLock(`${NS}lock`);
await measure('RLock', 'lock+unlock', async () => {
  await lock.lock();
  await lock.unlock();
});

const queue = redisson.getQueue(`${NS}queue`);
await measure('RQueue', 'offer', (i) => queue.offer(`v${i}`));
await measure('RQueue', 'poll', () => queue.poll());

// 4.8-family TimeSeries API: add(TimeSeriesAddArgs.entry) / entryRange.
// Single family on one series (iron law 12); fixed 64-entry read window.
const ts = redisson.getTimeSeries(`${NS}ts`);
const tsBase = Date.now();
await measure('RTimeSeries', 'add(entry)', (i) => ts.add(TimeSeriesAddArgs.entry(tsBase + i, i)));
await measure('RTimeSeries', 'entryRange', () => ts.entryRange(tsBase, tsBase + 63));

if (arsetOk) {
  const array = redisson.getArray(`${NS}array`);
  await measure('RArray', 'set', (i) => array.set(0, `v${i}`));
  await measure('RArray', 'get', () => array.get(0));
}

// -- cleanup: exact-name DEL only (no SCAN patterns; companions are tagged) --
await probe.del(...CREATED_KEYS);
const leftover = await probe.exists(...CREATED_KEYS);
if (leftover !== 0) {
  console.error(`bench: cleanup failed — ${leftover} bench key(s) still present`);
}

// -- report ------------------------------------------------------------------
const cpu = os.cpus()[0]?.model || 'unknown';
console.log(`| environment | |`);
console.log(`|---|---|`);
console.log(`| date | ${new Date().toISOString()} |`);
console.log(`| os | ${os.platform()} ${os.release()} (${os.arch()}) |`);
console.log(`| cpu | ${cpu.trim()} x ${os.cpus().length} |`);
console.log(`| node | ${process.version} |`);
console.log(`| redis | ${redisVersion} |`);
console.log(`| REDIS_URL | ${REDIS_URL} |`);
console.log(`| OPS | ${OPS} (sequential awaits per op) |`);
console.log('');
console.log('| structure | op | ops/sec | avg latency \u00b5s |');
console.log('|---|---|---:|---:|');
for (const r of rows) {
  console.log(`| ${r.structure} | ${r.op} | ${r.opsPerSec.toFixed(0)} | ${r.avgUs.toFixed(1)} |`);
}

await redisson.shutdown();
probe.disconnect();
process.exit(leftover !== 0 ? 1 : 0);
