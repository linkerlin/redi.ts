/**
 * Shared wire utilities: hash-tag names, HighwayHash-128, struct pack/unpack,
 * random ids, server time, sleep. All layouts are little-endian to match
 * Redis Lua struct.pack on x86_64 (verified against Redisson 4.6.1).
 */
import type { Redis } from 'ioredis';
import { randomBytes } from 'node:crypto';

export function prefixName(prefix: string, name: string): string {
  if (name.includes('{')) return `${prefix}:${name}`;
  return `${prefix}:{${name}}`;
}

export function suffixName(name: string, suffix: string): string {
  if (name.includes('{')) return `${name}:${suffix}`;
  return `{${name}}:${suffix}`;
}

export function randomHex(nBytes = 16): string {
  return randomBytes(nBytes).toString('hex');
}

export function randomLatin1(nBytes: number): string {
  return randomBytes(nBytes).toString('latin1');
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export class LockNotHeldError extends Error {
  constructor(msg = 'attempt to unlock lock, not locked by current thread') {
    super(msg);
    this.name = 'LockNotHeldError';
  }
}

export class NonReentrantError extends Error {
  constructor(msg = 'NON_REENTRANT_REACQUIRE') {
    super(msg);
    this.name = 'NonReentrantError';
  }
}

export async function serverNowMs(redis: Redis): Promise<number> {
  const t = await redis.time();
  // ioredis TIME returns [seconds, microseconds] as strings
  if (Array.isArray(t) && t.length >= 2) {
    return Number(t[0]) * 1000 + Math.floor(Number(t[1]) / 1000);
  }
  return Date.now();
}

/** Duplicate a connection (pub/sub, blocking commands). Returns null when the
 * client cannot duplicate (ioredis-mock or a closed client). */
export async function duplicateRedis(redis: Redis): Promise<Redis | null> {
  try {
    const Dup = (redis as any).duplicate?.bind(redis);
    if (!Dup) return null;
    const conn = Dup() as Redis;
    // ioredis performs an INFO ready-check on connect. Subscribing before it
    // finishes puts the socket in subscriber mode and makes INFO fail with
    // "Connection in subscriber mode, only subscriber commands may be used".
    (conn as any).on?.('error', () => undefined);
    if ((conn as any).status === 'wait') {
      try { await (conn as any).connect?.(); } catch { /* handled by ready wait */ }
    }
    if ((conn as any).status && (conn as any).status !== 'ready') {
      await new Promise<void>((resolve) => {
        let done = false;
        const finish = () => {
          if (done) return;
          done = true;
          clearTimeout(timer);
          try {
            (conn as any).off?.('ready', finish);
            (conn as any).off?.('error', finish);
          } catch { /* noop */ }
          resolve();
        };
        const timer = setTimeout(finish, 5000);
        (conn as any).once?.('ready', finish);
        (conn as any).once?.('error', finish);
      });
    }
    return conn;
  } catch {
    return null;
  }
}

/** Run `fn` on a dedicated connection when possible, then disconnect it.
 * Blocking commands (BLPOP/BRPOP/BZPOPMIN/XREADGROUP BLOCK) must never share
 * the main connection: while blocked, ioredis cannot send lock watchdog
 * renewals on that TCP connection. Falls back to the main connection when
 * duplication is unavailable (mock). */
export async function withDedicatedRedis<T>(
  redis: Redis,
  fn: (conn: Redis, dedicated: boolean) => Promise<T>,
): Promise<T> {
  const conn = await duplicateRedis(redis);
  if (!conn) return fn(redis, false);
  try {
    return await fn(conn, true);
  } finally {
    try { (conn as any).disconnect?.(); } catch { /* noop */ }
  }
}

/** Evaluate a Lua script. Returns null when Lua returns nil/false. Buffer args
 * pass through as raw bytes (required for binary-packed members on real Redis;
 * note: ioredis-mock cannot handle Buffer args — keep Lua args as strings in
 * mock-covered paths). */
export async function luaEval(
  redis: Redis,
  script: string,
  numKeys: number,
  ...keysAndArgs: (string | number | Buffer)[]
): Promise<any> {
  const args = keysAndArgs.map((a) => (Buffer.isBuffer(a) ? a : String(a)));
  return redis.eval(script, numKeys, ...args);
}

export function toNumber(v: any, dflt = 0): number {
  if (v === null || v === undefined || v === false) return dflt;
  const n = Number(v);
  return Number.isNaN(n) ? dflt : n;
}

// ---------------------------------------------------------------------------
// HighwayHash-128 (Redisson fixed KEY). BigInt-based, ported from
// redi.py highway_hash.py / Go minio/highwayhash with Redisson KEY.
// ---------------------------------------------------------------------------

const MASK64 = 0xffffffffffffffffn;

const HH_KEY = [0x9e3779b97f4a7c15n, 0xf39cc0605cedc834n, 0x1082276bf3a27251n, 0xf86c6a11d0c18e95n];
const HH_MUL0 = [0xdbe6d5d5fe4cce2fn, 0xa4093822299f31d0n, 0x13198a2e03707344n, 0x243f6a8885a308d3n];
const HH_MUL1 = [0x3bd39e10cb0ef593n, 0xc0acf169b5f18a8cn, 0xbe5466cf34e90c6cn, 0x452821e638d01377n];

function rot32(x: bigint): bigint {
  return (((x >> 32n) | (x << 32n)) & MASK64);
}

function zipperMerge0(v1: bigint, v0: bigint): bigint {
  return (
    ((((v0 & 0xff000000n) | (v1 & 0xff00000000n)) >> 24n) |
      (((v0 & 0xff0000000000n) | (v1 & 0xff000000000000n)) >> 16n) |
      (v0 & 0xff0000n) |
      ((v0 & 0xff00n) << 32n) |
      ((v1 & 0xff00000000000000n) >> 8n) |
      ((v0 << 56n) & MASK64)) &
    MASK64
  );
}

function zipperMerge1(v1: bigint, v0: bigint): bigint {
  return (
    ((((v1 & 0xff000000n) | (v0 & 0xff00000000n)) >> 24n) |
      (v1 & 0xff0000n) |
      ((v1 & 0xff0000000000n) >> 16n) |
      ((v1 & 0xff00n) << 24n) |
      ((v0 & 0xff000000000000n) >> 8n) |
      ((v1 & 0xffn) << 48n) |
      (v0 & 0xff00000000000000n)) &
    MASK64
  );
}

interface HHState {
  v0: bigint[];
  v1: bigint[];
  mul0: bigint[];
  mul1: bigint[];
}

function hhUpdate(s: HHState, a: bigint[]): void {
  for (let i = 0; i < 4; i++) s.v1[i] = (s.v1[i] + s.mul0[i] + a[i]) & MASK64;
  for (let i = 0; i < 4; i++) {
    s.mul0[i] ^= ((s.v1[i] & 0xffffffffn) * (s.v0[i] >> 32n)) & MASK64;
    s.v0[i] = (s.v0[i] + s.mul1[i]) & MASK64;
    s.mul1[i] ^= ((s.v0[i] & 0xffffffffn) * (s.v1[i] >> 32n)) & MASK64;
  }
  s.v0[0] = (s.v0[0] + zipperMerge0(s.v1[1], s.v1[0])) & MASK64;
  s.v0[1] = (s.v0[1] + zipperMerge1(s.v1[1], s.v1[0])) & MASK64;
  s.v0[2] = (s.v0[2] + zipperMerge0(s.v1[3], s.v1[2])) & MASK64;
  s.v0[3] = (s.v0[3] + zipperMerge1(s.v1[3], s.v1[2])) & MASK64;
  s.v1[0] = (s.v1[0] + zipperMerge0(s.v0[1], s.v0[0])) & MASK64;
  s.v1[1] = (s.v1[1] + zipperMerge1(s.v0[1], s.v0[0])) & MASK64;
  s.v1[2] = (s.v1[2] + zipperMerge0(s.v0[3], s.v0[2])) & MASK64;
  s.v1[3] = (s.v1[3] + zipperMerge1(s.v0[3], s.v0[2])) & MASK64;
}

function hhPermute(s: HHState): void {
  hhUpdate(s, [rot32(s.v0[2]), rot32(s.v0[3]), rot32(s.v0[0]), rot32(s.v0[1])]);
}

function readU64LE(buf: Buffer, off: number): bigint {
  return buf.readBigUInt64LE(off);
}

/** HighwayHash-128 with Redisson fixed key. Returns [h1, h2] (LE halves). */
export function highwayHash128(data: Buffer | string): [bigint, bigint] {
  const buf = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
  const s: HHState = {
    v0: HH_MUL0.map((m, i) => (m ^ HH_KEY[i]) & MASK64),
    v1: HH_MUL1.map((m, i) => (m ^ rot32(HH_KEY[i])) & MASK64),
    mul0: [...HH_MUL0],
    mul1: [...HH_MUL1],
  };
  let pos = 0;
  const len = buf.length;
  while (pos + 32 <= len) {
    hhUpdate(s, [readU64LE(buf, pos), readU64LE(buf, pos + 8), readU64LE(buf, pos + 16), readU64LE(buf, pos + 24)]);
    pos += 32;
  }
  const mod32 = len & 31;
  if (mod32 !== 0) {
    // updateRemainder (port of highway_hash.py :: update_remainder)
    for (let i = 0; i < 4; i++) s.v0[i] = (s.v0[i] + (BigInt(mod32) * 0x100000000n + BigInt(mod32))) & MASK64;
    // _rotate32_by(mod32, v1): rotate each 32-bit half left by count
    const count = mod32;
    for (let i = 0; i < 4; i++) {
      const v = s.v1[i];
      const half0 = v & 0xffffffffn;
      const half1 = (v >> 32n) & 0xffffffffn;
      const c = BigInt(count);
      const r0 = c === 0n ? half0 : (((half0 << c) & 0xffffffffn) | (half0 >> (32n - c)));
      const r1 = c === 0n ? half1 : (((half1 << c) & 0xffffffffn) | (half1 >> (32n - c)));
      s.v1[i] = (r1 << 32n) | r0;
    }
    const packet = Buffer.alloc(32, 0);
    const remainder = mod32 & ~3;
    for (let i = 0; i < remainder; i++) packet[i] = buf[pos + i];
    if (mod32 & 16) {
      for (let i = 0; i < 4; i++) packet[28 + i] = buf[pos + remainder + (mod32 & 3) - 4 + i];
    } else {
      const mod4 = mod32 & 3;
      if (mod4 !== 0) {
        packet[16] = buf[pos + remainder];
        packet[17] = buf[pos + remainder + (mod4 >> 1)];
        packet[18] = buf[pos + remainder + mod4 - 1];
      }
    }
    hhUpdate(s, [readU64LE(packet, 0), readU64LE(packet, 8), readU64LE(packet, 16), readU64LE(packet, 24)]);
  }
  for (let i = 0; i < 6; i++) hhPermute(s);
  const h1 = (s.v0[0] + s.mul0[0] + s.v1[2] + s.mul1[2]) & MASK64;
  const h2 = (s.v0[1] + s.mul0[1] + s.v1[3] + s.mul1[3]) & MASK64;
  return [h1, h2];
}

/** HighwayHash-128 of the encoded payload, BE-packed + base64 (no padding) —
 * exact Java `Hash.hash128toBase64(ByteBuf)` used by multimap internal ids and
 * by key-synchronizer companion names. */
export function hash128toBase64(encodedContent: string): string {
  const [h1, h2] = highwayHash128(Buffer.from(encodedContent, 'utf8'));
  const buf = Buffer.alloc(16);
  buf.writeBigUInt64BE(h1, 0);
  buf.writeBigUInt64BE(h2, 8);
  return buf.toString('base64').replace(/=+$/, '');
}

/** Multimap internal id == Hash.hash128toBase64 (historical name). */
export const multimapInternalId = hash128toBase64;

/** Companion key for RMap/RSet/RMultimap key synchronizers
 * (RedissonObject.getLockByMapKey / getLockByValue):
 *   suffixName(name, hash128toBase64(encode(key)) + ":" + suffix) */
export function keySynchronizerName(name: string, encodedKey: string, suffix: string): string {
  return suffixName(name, `${hash128toBase64(encodedKey)}:${suffix}`);
}

/** Bloom bit index: h starts at h1, alternately adding h2/h1, masked to 63 bits. */
export function bloomBitIndex(h1: bigint, h2: bigint, iteration: number, size: bigint): bigint {
  const M = 0x7fffffffffffffffn;
  let h = h1 & M;
  for (let i = 0; i < iteration; i++) {
    h = (h + (i % 2 === 0 ? h2 : h1)) & M;
  }
  return (h & M) % size;
}

export function optimalBloomBits(n: number, p: number): bigint {
  return BigInt(Math.trunc((-n * Math.log(p)) / (Math.log(2) * Math.log(2))));
}

export function optimalBloomIters(n: number, m: number): number {
  const v = Math.floor((m / n) * Math.log(2) + 0.5);
  return Math.max(1, Math.trunc(v));
}

// ---------------------------------------------------------------------------
// Struct pack/unpack helpers (little-endian, latin1透传)
// ---------------------------------------------------------------------------

/** RMapCache value: doubleLE(maxIdle) + u64LE(len) + codec bytes */
export function packMapCacheValue(codecStr: string, maxIdleMs = 0): string {
  const b = Buffer.from(codecStr, 'utf8');
  const out = Buffer.alloc(16 + b.length);
  out.writeDoubleLE(maxIdleMs, 0);
  out.writeBigUInt64LE(BigInt(b.length), 8);
  b.copy(out, 16);
  return out.toString('latin1');
}

export function unpackMapCacheValue(raw: string | Buffer): { maxIdle: number; codec: string; ok: boolean } {
  const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw, 'latin1');
  const asText = Buffer.isBuffer(raw) ? raw.toString('latin1') : raw;
  if (buf.length < 16) return { maxIdle: 0, codec: asText, ok: false };
  const len = Number(buf.readBigUInt64LE(8));
  if (len !== buf.length - 16) {
    // bare JSON written by RMap (or foreign bytes): expose as text
    return { maxIdle: 0, codec: buf.toString('utf8'), ok: false };
  }
  return { maxIdle: buf.readDoubleLE(0), codec: buf.subarray(16).toString('utf8'), ok: true };
}

/** RRateLimiter permits member: [u8 len=16][16B id][u32LE permits] (Lua writes 0x00 pad? Go wire is 21-22B; use exact struct.pack Bc0I) */
export function packRatePermit(idLatin1: string, permits: number): string {
  const id = Buffer.from(idLatin1, 'latin1');
  // struct.pack('Bc0I', len, id, permits): B + c0(len bytes) + I(LE u32)
  const out = Buffer.alloc(1 + id.length + 4);
  out[0] = id.length;
  id.copy(out, 1);
  out.writeUInt32LE(permits, 1 + id.length);
  return out.toString('latin1');
}

/** Unpack delayed-queue member: [u8 idLen][id][u64LE encLen][enc] */
export function unpackDelayedMember(rawLatin1: string | Buffer): { id: Buffer; enc: string } | null {
  const m = Buffer.isBuffer(rawLatin1) ? rawLatin1 : Buffer.from(rawLatin1, 'latin1');
  if (m.length < 1) return null;
  const idLen = m[0];
  if (m.length < 1 + idLen + 8) return null;
  const encLen = Number(m.readBigUInt64LE(1 + idLen));
  const s = 1 + idLen + 8;
  if (m.length < s + encLen) return null;
  return { id: m.subarray(1, 1 + idLen), enc: m.subarray(s, s + encLen).toString('utf8') };
}

/** Pack RTimeSeries member in 4.6.1 shape (readable by old AND new Java):
 * n = 2/3 with the label UNPREFIXED (codec-encoded label bytes as-is, empty
 * when absent). Newer n=4+flag shape breaks 4.6.1 label reads; the reverse
 * (4.6.1 shape under newer Java) is explicitly supported by DECODE_LABEL.
 * `encLabel` must already be codec-encoded (or '' when absent). */
export function packTimeSeriesMember(idStr: string, codecStr: string, encLabel: string): string {
  const ib = Buffer.from(idStr, 'utf8');
  const vb = Buffer.from(codecStr, 'utf8');
  const lb = Buffer.from(encLabel, 'utf8');
  const n = encLabel ? 3 : 2;
  const out = Buffer.alloc(1 + 1 + ib.length + 8 + vb.length + 8 + lb.length);
  let o = 0;
  out[o++] = n;
  out[o++] = ib.length;
  ib.copy(out, o);
  o += ib.length;
  out.writeBigUInt64LE(BigInt(vb.length), o);
  o += 8;
  vb.copy(out, o);
  o += vb.length;
  out.writeBigUInt64LE(BigInt(lb.length), o);
  o += 8;
  lb.copy(out, o);
  return out.toString('latin1');
}

/** Unpack RTimeSeries member → {val, label} with label still CODEC-ENCODED
 * (caller decodes; '' when absent). Accepts raw Buffer (exact bytes) or
 * latin1 string. Implements Java DECODE_LABEL branching:
 * n==4 → first label byte is a flag (2 = absent, else strip it);
 * n==2 → absent; otherwise (4.6.1 n==3) the label is unprefixed. */
export function unpackTimeSeriesMember(rawLatin1: string | Buffer): { val: string; label: string } | null {
  const m = Buffer.isBuffer(rawLatin1) ? rawLatin1 : Buffer.from(rawLatin1, 'latin1');
  try {
    if (m.length < 2) return null;
    const n = m[0];
    const idLen = m[1];
    let o = 2 + idLen;
    if (m.length < o + 8) return null;
    const valLen = Number(m.readBigUInt64LE(o));
    o += 8;
    if (m.length < o + valLen + 8) return null;
    const val = m.subarray(o, o + valLen).toString('utf8');
    o += valLen;
    const lblLen = Number(m.readBigUInt64LE(o));
    o += 8;
    if (m.length < o + lblLen) return null;
    const lbl = m.subarray(o, o + lblLen);
    let label = '';
    if (n === 4) {
      if (lbl.length === 0 || lbl[0] === 2) label = '';
      else label = lbl.subarray(1).toString('utf8');
    } else if (n === 2) {
      label = '';
    } else {
      label = lbl.toString('utf8');
    }
    return { val, label };
  } catch {
    return null;
  }
}

/** Encode map-cache event segment: u64LE(len)+bytes repeated. */
export function encodeMapCacheEvent(parts: string[]): string {
  const bufs = parts.map((p) => {
    const b = Buffer.from(p, 'utf8');
    const h = Buffer.alloc(8);
    h.writeBigUInt64LE(BigInt(b.length), 0);
    return Buffer.concat([h, b]);
  });
  return Buffer.concat(bufs).toString('latin1');
}

/** Decode map-cache event segments (inverse of encodeMapCacheEvent).
 * Returns the raw codec-encoded segments; a truncated tail is dropped. */
export function decodeMapCacheEvent(raw: string | Buffer): string[] {
  const m = Buffer.isBuffer(raw) ? raw : Buffer.from(raw, 'latin1');
  const parts: string[] = [];
  let o = 0;
  while (o + 8 <= m.length) {
    const len = Number(m.readBigUInt64LE(o));
    o += 8;
    if (len < 0 || o + len > m.length) break;
    parts.push(m.subarray(o, o + len).toString('utf8'));
    o += len;
  }
  return parts;
}
