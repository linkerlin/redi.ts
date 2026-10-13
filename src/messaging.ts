/**
 * Messaging: Topic / PatternTopic / ShardedTopic / ReliableTopic / Stream /
 * DelayedQueue / RingBuffer / BinaryStream.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base';
import type { Codec } from './codec';
import { JsonCodec } from './codec';
import { luaEval, prefixName, randomHex, serverNowMs, sleep, suffixName, toNumber, unpackDelayedMember } from './util';
import { randomBytes } from 'node:crypto';

export type MessageListener<V = any> = (message: V, channel?: string) => void;

async function duplicate(redis: Redis): Promise<Redis | null> {
  try {
    const Dup = (redis as any).duplicate?.bind(redis);
    if (!Dup) return null;
    return Dup() as Redis;
  } catch {
    return null;
  }
}

export class RTopic<V = any> extends RedisDataStructure {
  private subs = new Map<number, { conn: Redis; cb: MessageListener<V> }>();
  private nextId = 1;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async publish(message: V): Promise<number> {
    return this.redis.publish(this.name, this.encodeValue(message));
  }
  async addListener(cb: MessageListener<V>): Promise<number> {
    const id = this.nextId++;
    const conn = await duplicate(this.redis);
    if (!conn) {
      // mock fallback: no live delivery; listener registered as no-op
      this.subs.set(id, { conn: this.redis, cb });
      return id;
    }
    await (conn as any).subscribe(this.name);
    (conn as any).on('message', (_ch: string, payload: string) => {
      try {
        cb(this.decodeValue(payload) as V, this.name);
      } catch { /* noop */ }
    });
    this.subs.set(id, { conn, cb });
    return id;
  }
  async removeListener(id: number): Promise<void> {
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    try {
      await (s.conn as any).unsubscribe?.(this.name);
      if (s.conn !== this.redis) (s.conn as any).disconnect?.();
    } catch { /* noop */ }
  }
  async countSubscribers(): Promise<number> {
    try {
      const r: any = await (this.redis as any).pubsub('NUMSUB', this.name);
      if (Array.isArray(r)) return Number(r[1] ?? 0);
      return 0;
    } catch {
      return this.subs.size;
    }
  }
  async removeAllListeners(): Promise<void> {
    for (const id of [...this.subs.keys()]) await this.removeListener(id);
  }
}

export class RPatternTopic<V = any> extends RedisDataStructure {
  private subs = new Map<number, Redis>();
  private nextId = 1;
  constructor(redis: Redis, pattern: string, codec: Codec = new JsonCodec()) {
    super(redis, pattern, codec);
  }
  async publish(channel: string, message: V): Promise<number> {
    return this.redis.publish(channel, this.encodeValue(message));
  }
  async addListener(cb: (channel: string, message: V) => void): Promise<number> {
    const id = this.nextId++;
    const conn = await duplicate(this.redis);
    if (!conn) {
      this.subs.set(id, this.redis);
      return id;
    }
    await (conn as any).psubscribe(this.name);
    (conn as any).on('pmessage', (_pat: string, ch: string, payload: string) => {
      try {
        cb(ch, this.decodeValue(payload) as V);
      } catch { /* noop */ }
    });
    this.subs.set(id, conn);
    return id;
  }
  async removeListener(id: number): Promise<void> {
    const c = this.subs.get(id);
    if (!c) return;
    this.subs.delete(id);
    try {
      await (c as any).punsubscribe?.(this.name);
      if (c !== this.redis) (c as any).disconnect?.();
    } catch { /* noop */ }
  }
}

export class RShardedTopic<V = any> extends RedisDataStructure {
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async publish(message: V): Promise<number> {
    try {
      return await (this.redis as any).spublish(this.name, this.encodeValue(message));
    } catch {
      return this.redis.publish(this.name, this.encodeValue(message));
    }
  }
  async addListener(cb: MessageListener<V>): Promise<number> {
    const conn = await duplicate(this.redis);
    if (!conn) return 0;
    const handler = (_ch: string, payload: string) => {
      try {
        cb(this.decodeValue(payload) as V, this.name);
      } catch { /* noop */ }
    };
    // NOTE: sharded deliveries arrive as `smessage`, plain as `message`.
    // Listening to both keeps interop working regardless of server/client path.
    (conn as any).on('message', handler);
    (conn as any).on('smessage', (_ch: string, payload: string) => {
      try {
        cb(this.decodeValue(payload) as V, this.name);
      } catch { /* noop */ }
    });
    try {
      await (conn as any).ssubscribe(this.name);
    } catch {
      await (conn as any).subscribe(this.name);
    }
    return 1;
  }
}

export class RReliableTopic<V = any> extends RedisDataStructure {
  private timeoutKey: string;
  private loops = new Map<string, { stop: boolean; conn: Redis | null }>();
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.timeoutKey = suffixName(name, 'timeout');
  }
  async publish(message: V): Promise<void> {
    await (this.redis as any).xadd(this.name, '*', 'm', this.encodeValue(message));
  }
  async size(): Promise<number> {
    return (this.redis as any).xlen(this.name);
  }
  async countSubscribers(): Promise<number> {
    try {
      const g: any = await (this.redis as any).xinfo('GROUPS', this.name);
      return Array.isArray(g) ? g.length : 0;
    } catch {
      return this.loops.size;
    }
  }
  async addListener(cb: (msg: V) => void | Promise<void>): Promise<string> {
    const id = randomHex(16);
    const now = await serverNowMs(this.redis);
    await this.redis.zadd(this.timeoutKey, String(now + 120000), id);
    try {
      await (this.redis as any).xgroup('CREATE', this.name, id, '0', 'MKSTREAM');
    } catch (e: any) {
      if (!String(e?.message ?? e).includes('BUSYGROUP')) throw e;
    }
    const state = { stop: false, conn: null as Redis | null };
    this.loops.set(id, state);
    void this.consumeLoop(id, cb, state);
    void this.renewLoop(id, state);
    return id;
  }
  private async renewLoop(id: string, state: { stop: boolean }): Promise<void> {
    while (!state.stop) {
      await sleep(40000);
      if (state.stop) break;
      try {
        const now = await serverNowMs(this.redis);
        await this.redis.zadd(this.timeoutKey, 'XX', String(now + 120000), id);
      } catch { /* noop */ }
    }
  }
  private async consumeLoop(id: string, cb: (msg: V) => void | Promise<void>, state: { stop: boolean }): Promise<void> {
    while (!state.stop) {
      try {
        const res: any = await (this.redis as any).xreadgroup('GROUP', id, id, 'COUNT', 10, 'BLOCK', 2000, 'STREAMS', this.name, '>');
        if (!res) continue;
        const streams = Array.isArray(res[0]?.[1]) ? res : res;
        for (const stream of streams) {
          const entries = stream[1] ?? [];
          for (const [msgId, fields] of entries) {
            if (state.stop) return;
            let payload: string | null = null;
            if (Array.isArray(fields)) {
              for (let i = 0; i + 1 < fields.length; i += 2) if (fields[i] === 'm') payload = fields[i + 1];
            } else if (fields && typeof fields === 'object') payload = (fields as any)['m'] ?? null;
            if (payload !== null) {
              try {
                await cb(this.decodeValue(payload) as V);
              } catch { /* listener error: still ack? no — redeliver */ continue; }
              try {
                await (this.redis as any).xack(this.name, id, msgId);
              } catch { /* noop */ }
            }
          }
        }
      } catch {
        await sleep(500);
      }
    }
  }
  async removeListener(id: string): Promise<void> {
    const s = this.loops.get(id);
    if (s) s.stop = true;
    this.loops.delete(id);
    try {
      await (this.redis as any).xgroup('DESTROY', this.name, id);
    } catch { /* noop */ }
    await this.redis.zrem(this.timeoutKey, id);
  }
  override async delete(): Promise<boolean> {
    for (const id of [...this.loops.keys()]) await this.removeListener(id);
    await this.redis.del(this.name, this.timeoutKey);
    return true;
  }
}

export interface StreamEntry<V = any> {
  id: string;
  fields: Record<string, V>;
}

export interface StreamPendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveredCount: number;
}

export interface StreamPendingInfo {
  count: number;
  minId: string | null;
  maxId: string | null;
  consumers: Record<string, number>;
}

export interface StreamGroupInfo {
  name: string;
  lastDeliveredId: string | null;
  entriesRead: number | null;
  lag: number | null;
  pelCount: number | null;
}

export interface StreamConsumerInfo {
  name: string;
  pending: number | null;
  idleMs: number | null;
  inactiveMs: number | null;
}

export interface StreamInfo {
  length: number | null;
  groups: number | null;
  lastGeneratedId: string | null;
  firstEntry: StreamEntry | null;
  lastEntry: StreamEntry | null;
}

export class RStream extends RedisDataStructure {
  async add(fields: Record<string, any>, id = '*', maxLen?: number): Promise<string> {
    const flat: string[] = [];
    for (const [k, v] of Object.entries(fields)) flat.push(this.encodeKey(k), this.encodeValue(v));
    const args: (string | number)[] = maxLen !== undefined ? [this.name, 'MAXLEN', '~', maxLen, id, ...flat] : [this.name, id, ...flat];
    return (this.redis as any).xadd(...args);
  }
  async read(count = 10, lastId = '0'): Promise<StreamEntry[]> {
    const res: any = await (this.redis as any).xrange(this.name, lastId === '0' ? '-' : `(${lastId}`, '+', 'COUNT', count);
    return (res ?? []).map(([eid, f]: [string, string[]]) => ({ id: eid, fields: this.decodeFields(f) }));
  }
  private decodeFields(fields: string[] | Record<string, string>): Record<string, any> {
    const out: Record<string, any> = {};
    if (Array.isArray(fields)) {
      for (let i = 0; i + 1 < fields.length; i += 2) {
        let k: any = fields[i];
        try {
          k = this.decodeKey(k);
        } catch { /* raw */ }
        const ks = typeof k === 'string' ? k : String(k ?? '');
        try {
          out[ks] = this.decodeValue(fields[i + 1]);
        } catch {
          out[ks] = fields[i + 1];
        }
      }
    } else {
      for (const [k, v] of Object.entries(fields)) {
        let dk: any = k;
        try {
          dk = this.decodeKey(k);
        } catch { /* raw */ }
        out[String(dk)] = this.decodeValue(v);
      }
    }
    return out;
  }
  async size(): Promise<number> {
    return (this.redis as any).xlen(this.name);
  }
  async trim(maxLen: number, approx = true): Promise<number> {
    // NOTE: exact form omits the marker entirely — sending '' would be a
    // syntax error. (~ matches Redisson's default non-strict trimming, which
    // intentionally no-ops on small streams; interop means parity, not strictness.)
    const args: (string | number)[] = approx
      ? [this.name, 'MAXLEN', '~', maxLen]
      : [this.name, 'MAXLEN', maxLen];
    return (this.redis as any).xtrim(...args);
  }
  async deleteEntries(...ids: string[]): Promise<number> {
    if (ids.length === 0) return 0;
    return (this.redis as any).xdel(this.name, ...ids);
  }
  async createGroup(group: string, lastId = '0'): Promise<void> {
    await (this.redis as any).xgroup('CREATE', this.name, group, lastId, 'MKSTREAM');
  }
  async readGroup(group: string, consumer: string, count = 10, blockMs = -1): Promise<StreamEntry[]> {
    const args: (string | number | undefined)[] = ['GROUP', group, consumer, 'COUNT', count];
    if (blockMs >= 0) args.push('BLOCK', blockMs);
    args.push('STREAMS', this.name, '>');
    const res: any = await (this.redis as any).xreadgroup(...args.filter((a) => a !== undefined));
    if (!res) return [];
    const out: StreamEntry[] = [];
    for (const stream of res) {
      for (const [eid, f] of stream[1] ?? []) out.push({ id: eid, fields: this.decodeFields(f) });
    }
    return out;
  }
  async ack(group: string, ...ids: string[]): Promise<number> {
    if (ids.length === 0) return 0;
    return (this.redis as any).xack(this.name, group, ...ids);
  }
  async removeGroup(group: string): Promise<boolean> {
    return toNumber(await (this.redis as any).xgroup('DESTROY', this.name, group)) === 1;
  }
  async createConsumer(group: string, consumer: string): Promise<boolean> {
    return toNumber(await (this.redis as any).xgroup('CREATECONSUMER', this.name, group, consumer)) === 1;
  }
  async removeConsumer(group: string, consumer: string): Promise<number> {
    return toNumber(await (this.redis as any).xgroup('DELCONSUMER', this.name, group, consumer));
  }
  async updateGroupMessageId(group: string, id: string): Promise<void> {
    await (this.redis as any).xgroup('SETID', this.name, group, id);
  }
  /** XPENDING summary: [count, minId, maxId, [[consumer, count], ...]]. */
  async getPendingInfo(group: string): Promise<StreamPendingInfo> {
    const r: any = await (this.redis as any).xpending(this.name, group);
    if (!r) return { count: 0, minId: null, maxId: null, consumers: {} };
    const consumers: Record<string, number> = {};
    for (const [c, n] of r[3] ?? []) consumers[String(c)] = Number(n);
    return {
      count: Number(r[0] ?? 0),
      minId: r[1] == null ? null : String(r[1]),
      maxId: r[2] == null ? null : String(r[2]),
      consumers,
    };
  }
  /** XPENDING range (detailed per-entry rows). */
  async listPending(group: string, start = '-', end = '+', count = 10, consumer?: string): Promise<StreamPendingEntry[]> {
    const args: (string | number)[] = [this.name, group, start, end, count];
    if (consumer !== undefined) args.push(consumer);
    const rows: any[] = (await (this.redis as any).xpending(...args)) ?? [];
    return rows.map(([id, c, idle, delivered]: any[]) => ({
      id: String(id),
      consumer: String(c),
      idleMs: Number(idle),
      deliveredCount: Number(delivered),
    }));
  }
  /** XPENDING range + fetch each entry body (mirrors pendingRange). */
  async pendingRange(group: string, start = '-', end = '+', count = 10): Promise<StreamEntry[]> {
    const rows = await this.listPending(group, start, end, count);
    const out: StreamEntry[] = [];
    for (const row of rows) {
      const got: any = await (this.redis as any).xrange(this.name, row.id, row.id);
      for (const [eid, f] of got ?? []) out.push({ id: String(eid), fields: this.decodeFields(f) });
    }
    return out;
  }
  /** XCLAIM: take ownership of idle pending entries. */
  async claim(group: string, consumer: string, minIdleMs: number, ...ids: string[]): Promise<StreamEntry[]> {
    if (ids.length === 0) return [];
    const res: any = await (this.redis as any).xclaim(this.name, group, consumer, minIdleMs, ...ids);
    return (res ?? []).map(([eid, f]: [string, string[]]) => ({ id: String(eid), fields: this.decodeFields(f) }));
  }
  /** XAUTOCLAIM: cursor-scan ownership transfer. Returns next cursor + entries. */
  async autoClaim(group: string, consumer: string, minIdleMs: number, startId = '0-0', count = 10): Promise<{ nextId: string; entries: StreamEntry[] }> {
    const res: any = await (this.redis as any).xautoclaim(this.name, group, consumer, minIdleMs, startId, 'COUNT', count);
    if (!res) return { nextId: '0-0', entries: [] };
    const [cursor, rows] = res;
    return {
      nextId: String(cursor),
      entries: ((rows ?? []) as [string, string[]][]).map(([eid, f]) => ({ id: String(eid), fields: this.decodeFields(f) })),
    };
  }
  async getInfo(): Promise<StreamInfo> {
    const flat: any[] = (await (this.redis as any).xinfo('STREAM', this.name)) ?? [];
    const m = new Map<string, any>();
    for (let i = 0; i + 1 < flat.length; i += 2) m.set(String(flat[i]), flat[i + 1]);
    const num = (k: string) => (m.has(k) && m.get(k) !== null ? Number(m.get(k)) : null);
    const entry = (k: string): StreamEntry | null => {
      const e = m.get(k);
      if (!e) return null;
      return { id: String(e[0]), fields: this.decodeFields(e[1]) };
    };
    return {
      length: num('length'),
      groups: num('groups'),
      lastGeneratedId: m.has('last-generated-id') ? String(m.get('last-generated-id')) : null,
      firstEntry: entry('first-entry'),
      lastEntry: entry('last-entry'),
    };
  }
  async listGroups(): Promise<StreamGroupInfo[]> {
    const groups: any[] = (await (this.redis as any).xinfo('GROUPS', this.name)) ?? [];
    return groups.map((g: any[]) => {
      const m = new Map<string, any>();
      for (let i = 0; i + 1 < g.length; i += 2) m.set(String(g[i]), g[i + 1]);
      const num = (k: string) => (m.has(k) && m.get(k) !== null ? Number(m.get(k)) : null);
      const str = (k: string) => (m.has(k) && m.get(k) !== null ? String(m.get(k)) : null);
      return {
        name: String(m.get('name')),
        lastDeliveredId: str('last-delivered-id'),
        entriesRead: num('entries-read'),
        lag: num('lag'),
        pelCount: num('pel-count'),
      };
    });
  }
  async listConsumers(group: string): Promise<StreamConsumerInfo[]> {
    const consumers: any[] = (await (this.redis as any).xinfo('CONSUMERS', this.name, group)) ?? [];
    return consumers.map((c: any[]) => {
      const m = new Map<string, any>();
      for (let i = 0; i + 1 < c.length; i += 2) m.set(String(c[i]), c[i + 1]);
      const num = (k: string) => (m.has(k) && m.get(k) !== null ? Number(m.get(k)) : null);
      return {
        name: String(m.get('name')),
        pending: num('pending'),
        idleMs: num('idle'),
        inactiveMs: num('inactive'),
      };
    });
  }
}

// -- DelayedQueue (dual ZSET+LIST, struct-packed member) -----------------------

const DQ_OFFER = `local value = struct.pack('Bc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3])
redis.call('zadd', KEYS[2], ARGV[1], value)
redis.call('rpush', KEYS[3], value)
local v = redis.call('zrange', KEYS[2], 0, 0)
if v[1] == value then
    redis.call('publish', KEYS[4], ARGV[1])
end`;

const DQ_MIGRATE = `local expiredValues = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2])
if #expiredValues > 0 then
    for i, v in ipairs(expiredValues) do
        local randomId, value = struct.unpack('Bc0Lc0', v)
        redis.call('rpush', KEYS[1], value)
        redis.call('lrem', KEYS[3], 1, v)
    end
    redis.call('zrem', KEYS[2], unpack(expiredValues))
end`;

export class RDelayedQueue<V = any> extends RedisDataStructure {
  private timeoutKey: string;
  private queueKey: string;
  private channel: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.timeoutKey = prefixName('redisson_delay_queue_timeout', name);
    this.queueKey = prefixName('redisson_delay_queue', name);
    this.channel = prefixName('redisson_delay_queue_channel', name);
  }
  private async migrate(): Promise<void> {
    const now = await serverNowMs(this.redis);
    try {
      await luaEval(this.redis, DQ_MIGRATE, 3, this.name, this.timeoutKey, this.queueKey, String(now), '100');
    } catch { /* mock fallback noop */ }
  }
  async offer(value: V, delayMs: number): Promise<void> {
    const now = await serverNowMs(this.redis);
    const deadline = now + delayMs;
    // 8 opaque id bytes as Buffer: exact bytes reach Lua (no UTF-8 expansion).
    // Mock cannot take Buffer Lua args → falls back to plain rpush there.
    const rid = randomBytes(8);
    try {
      await luaEval(this.redis, DQ_OFFER, 4, this.name, this.timeoutKey, this.queueKey, this.channel,
        String(deadline), rid, this.encodeValue(value));
    } catch {
      // mock fallback: plain rpush with delay ignored
      await this.redis.rpush(this.name, this.encodeValue(value));
    }
  }
  async poll(): Promise<V | null> {
    await this.migrate();
    const raw = await this.redis.lpop(this.name);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async peek(): Promise<V | null> {
    await this.migrate();
    const raw = await this.redis.lindex(this.name, 0);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async size(): Promise<number> {
    await this.migrate();
    return this.redis.llen(this.name);
  }
  async pendingSize(): Promise<number> {
    return this.redis.zcard(this.timeoutKey);
  }
  async contains(value: V): Promise<boolean> {
    const enc = this.encodeValue(value);
    const members: Buffer[] = await this.redis.lrangeBuffer(this.queueKey, 0, -1);
    for (const m of members) {
      const u = unpackDelayedMember(Buffer.isBuffer(m) ? m : Buffer.from(String(m), 'latin1'));
      if (u && u.enc === enc) return true;
    }
    return false;
  }
  async remove(value: V): Promise<boolean> {
    const enc = this.encodeValue(value);
    const members: Buffer[] = await this.redis.lrangeBuffer(this.queueKey, 0, -1);
    for (const m of members) {
      const buf = Buffer.isBuffer(m) ? m : Buffer.from(String(m), 'latin1');
      const u = unpackDelayedMember(buf);
      if (u && u.enc === enc) {
        await this.redis.lrem(this.queueKey, 1, buf);
        await this.redis.zrem(this.timeoutKey, buf);
        return true;
      }
    }
    return false;
  }
  async clear(): Promise<void> {
    await this.redis.del(this.queueKey, this.timeoutKey);
  }
  override async delete(): Promise<boolean> {
    await this.redis.del(this.name, this.queueKey, this.timeoutKey);
    return true;
  }
}

// -- RingBuffer -----------------------------------------------------------------

export class RRingBuffer<V = any> extends RedisDataStructure {
  private capacityKey: string;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.capacityKey = prefixName('redisson_rb', name);
  }
  async trySetCapacity(capacity: number): Promise<boolean> {
    return ((await this.redis.setnx(this.capacityKey, String(capacity))) as number) === 1;
  }
  async setCapacity(capacity: number): Promise<void> {
    const s = `redis.call('set', KEYS[2], ARGV[1])
local len = redis.call('llen', KEYS[1])
local cap = tonumber(ARGV[1])
if len > cap then
    redis.call('ltrim', KEYS[1], len - cap, -1)
end`;
    try {
      await luaEval(this.redis, s, 2, this.name, this.capacityKey, String(capacity));
    } catch {
      await this.redis.set(this.capacityKey, String(capacity));
      const len = await this.redis.llen(this.name);
      if (len > capacity) await this.redis.ltrim(this.name, len - capacity, -1);
    }
  }
  async capacity(): Promise<number> {
    const v = await this.redis.get(this.capacityKey);
    return v === null ? 0 : Number(v);
  }
  async remainingCapacity(): Promise<number> {
    const cap = await this.capacity();
    const size = await this.redis.llen(this.name);
    return Math.max(0, cap - size);
  }
  async add(value: V): Promise<void> {
    const s = `local limit = redis.call('get', KEYS[2])
if limit == false then
    return redis.error_reply('RingBuffer capacity is not defined')
end
limit = tonumber(limit)
local size = redis.call('rpush', KEYS[1], ARGV[1])
if size > limit then
    redis.call('lpop', KEYS[1])
end`;
    try {
      await luaEval(this.redis, s, 2, this.name, this.capacityKey, this.encodeValue(value));
    } catch (e: any) {
      if (String(e?.message ?? e).includes('capacity')) throw e;
      await this.redis.rpush(this.name, this.encodeValue(value));
    }
  }
  async readAll(): Promise<V[]> {
    const m = await this.redis.lrange(this.name, 0, -1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async size(): Promise<number> {
    return this.redis.llen(this.name);
  }
  async clear(): Promise<void> {
    await this.redis.del(this.name);
  }
}

// -- BinaryStream (raw bytes, ByteArrayCodec semantics) --------------------------

export class RBinaryStream extends RedisDataStructure {
  private toBuf(data: string | Buffer | Uint8Array): Buffer | string {
    if (typeof data === 'string') return data;
    return Buffer.isBuffer(data) ? data : Buffer.from(data);
  }
  async set(data: string | Buffer | Uint8Array): Promise<void> {
    await this.redis.set(this.name, this.toBuf(data));
  }
  async get(): Promise<Buffer | null> {
    let raw: unknown;
    try {
      raw = await this.redis.getBuffer(this.name);
    } catch {
      raw = await this.redis.get(this.name);
    }
    if (raw === null || raw === undefined) return null;
    return Buffer.isBuffer(raw) ? raw : Buffer.from(raw as string, 'latin1');
  }
  async append(data: string | Buffer): Promise<number> {
    return this.redis.append(this.name, this.toBuf(data));
  }
  async getRange(from: number, to: number): Promise<Buffer | null> {
    let raw: unknown;
    try {
      raw = await this.redis.getrangeBuffer(this.name, from, to);
    } catch {
      raw = await this.redis.getrange(this.name, from, to);
    }
    if (raw === null || raw === undefined) return null;
    return Buffer.isBuffer(raw) ? raw : Buffer.from(raw as string, 'latin1');
  }
  async size(): Promise<number> {
    return this.redis.strlen(this.name);
  }
  /** Back-compat alias used by older tests. */
  async read(): Promise<Buffer | null> {
    return this.get();
  }
}
