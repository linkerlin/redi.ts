/**
 * Messaging: Topic / PatternTopic / ShardedTopic / ReliableTopic / Stream /
 * DelayedQueue / RingBuffer / BinaryStream.
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import type { Codec } from './codec.js';
import { JsonCodec } from './codec.js';
import { duplicateRedis, luaEval, prefixName, randomHex, serverNowMs, sleep, suffixName, toNumber, unpackDelayedMember, withDedicatedRedis } from './util.js';
import { randomBytes } from 'node:crypto';
import { KeyspaceListenerHub, type ObjectListener } from './listeners.js';

export type MessageListener<V = any> = (message: V, channel?: string) => void;

/** Java `StatusListener`: subscription lifecycle callbacks (local). */
export interface StatusListener {
  onSubscribe(channel: string): void;
  onUnsubscribe(channel: string): void;
}

export class RTopic<V = any> extends RedisDataStructure {
  private subs = new Map<number, { conn: Redis; cb: MessageListener<V>; handler: (ch: string, payload: string) => void }>();
  private statusSubs = new Map<number, StatusListener>();
  private nextId = 1;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
  }
  async publish(message: V): Promise<number> {
    return this.redis.publish(this.name, this.encodeValue(message));
  }
  async addListener(cb: MessageListener<V>): Promise<number> {
    const id = this.nextId++;
    const conn = await duplicateRedis(this.redis);
    if (!conn) {
      // mock fallback: no live delivery; listener registered as no-op
      this.subs.set(id, { conn: this.redis, cb, handler: () => undefined });
      return id;
    }
    const handler = (_ch: string, payload: string) => {
      try {
        cb(this.decodeValue(payload) as V, this.name);
      } catch { /* noop */ }
    };
    // Attach before SUBSCRIBE: a message published between the ack and the
    // handler registration would otherwise be dropped.
    (conn as any).on('message', handler);
    try {
      await conn.subscribe(this.name);
    } catch (e) {
      try {
        (conn as any).off?.('message', handler);
        (conn as any).disconnect?.();
      } catch { /* noop */ }
      throw e;
    }
    this.subs.set(id, { conn, cb, handler });
    return id;
  }
  async removeListener(idOrCb: number | MessageListener<V>): Promise<void> {
    if (typeof idOrCb !== 'number') {
      for (const [id, s] of [...this.subs.entries()]) {
        if (s.cb === idOrCb) await this.removeListener(id);
      }
      return;
    }
    const id = idOrCb;
    const status = this.statusSubs.get(id);
    if (status) {
      this.statusSubs.delete(id);
      try { status.onUnsubscribe(this.name); } catch { /* noop */ }
      return;
    }
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    try {
      (s.conn as any).off?.('message', s.handler);
      await (s.conn as any).unsubscribe?.(this.name);
      if (s.conn !== this.redis) (s.conn as any).disconnect?.();
    } catch { /* noop */ }
  }
  async countSubscribers(): Promise<number> {
    try {
      const r: any = await this.redis.pubsub('NUMSUB', this.name);
      if (Array.isArray(r)) return Number(r[1] ?? 0);
      return 0;
    } catch {
      return this.subs.size;
    }
  }
  /** Java `getChannelNames()`: the underlying pub/sub channel(s). */
  async getChannelNames(): Promise<string[]> {
    return [this.name];
  }
  /** Java `countListeners()`: listeners registered on this instance. */
  async countListeners(): Promise<number> {
    return this.subs.size + this.statusSubs.size;
  }
  /** Java `addListener(StatusListener)`: local subscribe/unsubscribe callbacks.
   *  Registration reports the current subscription immediately. */
  async addStatusListener(listener: StatusListener): Promise<number> {
    const id = this.nextId++;
    this.statusSubs.set(id, listener);
    try { listener.onSubscribe(this.name); } catch { /* noop */ }
    return id;
  }
  async removeAllListeners(): Promise<void> {
    for (const id of [...this.statusSubs.keys(), ...this.subs.keys()]) await this.removeListener(id);
  }
}

export class RPatternTopic<V = any> extends RedisDataStructure {
  private subs = new Map<number, { conn: Redis; cb: (channel: string, message: V) => void; handler: (pat: string, ch: string, payload: string) => void }>();
  private nextId = 1;
  constructor(redis: Redis, pattern: string, codec: Codec = new JsonCodec()) {
    super(redis, pattern, codec);
  }
  async publish(channel: string, message: V): Promise<number> {
    return this.redis.publish(channel, this.encodeValue(message));
  }
  async addListener(cb: (channel: string, message: V) => void): Promise<number> {
    const id = this.nextId++;
    const conn = await duplicateRedis(this.redis);
    if (!conn) {
      this.subs.set(id, { conn: this.redis, cb, handler: () => undefined });
      return id;
    }
    const handler = (_pat: string, ch: string, payload: string) => {
      try {
        cb(ch, this.decodeValue(payload) as V);
      } catch { /* noop */ }
    };
    (conn as any).on('pmessage', handler);
    try {
      await conn.psubscribe(this.name);
    } catch (e) {
      try {
        (conn as any).off?.('pmessage', handler);
        (conn as any).disconnect?.();
      } catch { /* noop */ }
      throw e;
    }
    this.subs.set(id, { conn, cb, handler });
    return id;
  }
  async removeListener(idOrCb: number | ((channel: string, message: V) => void)): Promise<void> {
    if (typeof idOrCb !== 'number') {
      for (const [id, s] of [...this.subs.entries()]) {
        if (s.cb === idOrCb) await this.removeListener(id);
      }
      return;
    }
    const id = idOrCb;
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    try {
      (s.conn as any).off?.('pmessage', s.handler);
      await (s.conn as any).punsubscribe?.(this.name);
      if (s.conn !== this.redis) (s.conn as any).disconnect?.();
    } catch { /* noop */ }
  }
  async removeAllListeners(): Promise<void> {
    for (const id of [...this.subs.keys()]) await this.removeListener(id);
  }
  /** Java `getPatternNames()`. */
  async getPatternNames(): Promise<string[]> {
    return [this.name];
  }
  /** Java `getActiveTopics()`: PUBSUB CHANNELS matching this pattern. */
  async getActiveTopics(): Promise<string[]> {
    try {
      const r: any = await (this.redis as any).pubsub('CHANNELS', this.name);
      return Array.isArray(r) ? r.map(String) : [];
    } catch {
      return [];
    }
  }
}

export class RShardedTopic<V = any> extends RedisDataStructure {
  private subs = new Map<number, { conn: Redis; cb: MessageListener<V>; handler: (ch: string, payload: string) => void; mode: 'sharded' | 'plain' }>();
  private statusSubs = new Map<number, StatusListener>();
  private nextId = 1;
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
    const id = this.nextId++;
    const conn = await duplicateRedis(this.redis);
    if (!conn) return 0;
    const handler = (_ch: string, payload: string) => {
      try {
        cb(this.decodeValue(payload) as V, this.name);
      } catch { /* noop */ }
    };
    // NOTE: sharded deliveries arrive as `smessage`, plain as `message`.
    // Listening to both keeps interop working regardless of server/client path.
    (conn as any).on('message', handler);
    (conn as any).on('smessage', handler);
    let mode: 'sharded' | 'plain' = 'sharded';
    try {
      await conn.ssubscribe(this.name);
    } catch {
      try {
        await conn.subscribe(this.name);
        mode = 'plain';
      } catch (e) {
        try {
          (conn as any).off?.('message', handler);
          (conn as any).off?.('smessage', handler);
          (conn as any).disconnect?.();
        } catch { /* noop */ }
        throw e;
      }
    }
    this.subs.set(id, { conn, cb, handler, mode });
    return id;
  }
  async removeListener(idOrCb: number | MessageListener<V>): Promise<void> {
    if (typeof idOrCb !== 'number') {
      for (const [id, s] of [...this.subs.entries()]) {
        if (s.cb === idOrCb) await this.removeListener(id);
      }
      return;
    }
    const id = idOrCb;
    const status = this.statusSubs.get(id);
    if (status) {
      this.statusSubs.delete(id);
      try { status.onUnsubscribe(this.name); } catch { /* noop */ }
      return;
    }
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    try {
      (s.conn as any).off?.('message', s.handler);
      (s.conn as any).off?.('smessage', s.handler);
      if (s.mode === 'sharded') await (s.conn as any).sunsubscribe?.(this.name);
      else await (s.conn as any).unsubscribe?.(this.name);
      (s.conn as any).disconnect?.();
    } catch { /* noop */ }
  }
  async removeAllListeners(): Promise<void> {
    for (const id of [...this.statusSubs.keys(), ...this.subs.keys()]) await this.removeListener(id);
  }
  /** Java `getChannelNames()` (sharded topic uses the same channel name). */
  async getChannelNames(): Promise<string[]> {
    return [this.name];
  }
  /** Java `countListeners()`: listeners registered on this instance. */
  async countListeners(): Promise<number> {
    return this.subs.size + this.statusSubs.size;
  }
  /** Java `addListener(StatusListener)`. */
  async addStatusListener(listener: StatusListener): Promise<number> {
    const id = this.nextId++;
    this.statusSubs.set(id, listener);
    try { listener.onSubscribe(this.name); } catch { /* noop */ }
    return id;
  }
  /** Java `countSubscribers()`: PUBSUB SHARDNUMSUB, falling back to NUMSUB. */
  async countSubscribers(): Promise<number> {
    try {
      const r: any = await (this.redis as any).pubsub('SHARDNUMSUB', this.name);
      if (Array.isArray(r) && r.length >= 2) return Number(r[1] ?? 0);
    } catch { /* fall through */ }
    try {
      const r: any = await this.redis.pubsub('NUMSUB', this.name);
      if (Array.isArray(r) && r.length >= 2 && Number(r[1]) > 0) return Number(r[1]);
    } catch { /* fall through */ }
    return this.subs.size;
  }
}

const RT_MAINTENANCE = `local expired = redis.call('zrangebyscore', KEYS[2], 0, tonumber(ARGV[2]) - 1)
for i, v in ipairs(expired) do
    redis.call('xgroup', 'destroy', KEYS[1], v)
    redis.call('zrem', KEYS[2], v)
end
local r = redis.call('zscore', KEYS[2], ARGV[1])
local score = 92233720368547758
local groups = redis.call('xinfo', 'groups', KEYS[1])
for i, v in ipairs(groups) do
    local id1, id2 = string.match(v[8], '(.*)%-(.*)')
    score = math.min(tonumber(id1), score)
end
score = tostring(score) .. '-0'
local range = redis.call('xrange', KEYS[1], score, '+')
if #range == 0 or (#range == 1 and range[1][1] == score) then
    redis.call('xtrim', KEYS[1], 'maxlen', 0)
else
    redis.call('xtrim', KEYS[1], 'maxlen', #range)
end
return r ~= false`;

// Java RedissonReliableTopic: one subscriber group per client instance; all
// local listeners share it. publish() returns the number of groups (XINFO
// GROUPS), the heartbeat ZSET destroys stale groups, and the stream is trimmed
// to the earliest group's last-delivered id after each poll.
export class RReliableTopic<V = any> extends RedisDataStructure {
  private timeoutKey: string;
  private subscriberId = randomHex(16);
  private listeners = new Map<string, (msg: V) => void | Promise<void>>();
  private subscribed = false;
  private conn: Redis | null = null;
  private loopGen = 0;
  constructor(redis: Redis, name: string, codec: Codec = new JsonCodec()) {
    super(redis, name, codec);
    this.timeoutKey = suffixName(name, 'timeout');
  }
  async publish(message: V): Promise<number> {
    const s = `redis.call('xadd', KEYS[1], '*', 'm', ARGV[1])
local groups = redis.call('xinfo', 'groups', KEYS[1])
return #groups`;
    try {
      return toNumber(await luaEval(this.redis, s, 1, this.name, this.encodeValue(message)));
    } catch {
      // mock fallback: no XINFO in Lua; a single local subscriber is implied
      await this.redis.xadd(this.name, '*', 'm', this.encodeValue(message));
      return this.subscribed ? 1 : 0;
    }
  }
  async size(): Promise<number> {
    return this.redis.xlen(this.name);
  }
  async countSubscribers(): Promise<number> {
    try {
      const g: any = await this.redis.xinfo('GROUPS', this.name);
      return Array.isArray(g) ? g.length : 0;
    } catch {
      return this.subscribed ? 1 : 0;
    }
  }
  async countListeners(): Promise<number> {
    return this.listeners.size;
  }
  async addListener(cb: (msg: V) => void | Promise<void>): Promise<string> {
    const id = randomHex(16);
    this.listeners.set(id, cb);
    if (!this.subscribed) {
      this.subscribed = true;
      try {
        await this.redis.zadd(this.timeoutKey, String(Date.now() + 120000), this.subscriberId);
        try {
          await this.redis.xgroup('CREATE', this.name, this.subscriberId, '0', 'MKSTREAM');
        } catch (e: any) {
          if (!String(e?.message ?? e).includes('BUSYGROUP')) throw e;
        }
        const gen = ++this.loopGen;
        void this.consumeLoop(gen);
      } catch (e) {
        this.subscribed = false;
        this.listeners.delete(id);
        throw e;
      }
    }
    return id;
  }
  private closeConn(): void {
    try {
      if (this.conn && this.conn !== this.redis) (this.conn as any).disconnect?.();
    } catch { /* noop */ }
    this.conn = null;
  }
  private async consumeLoop(gen: number): Promise<void> {
    while (this.subscribed && gen === this.loopGen) {
      try {
        if (!this.conn) this.conn = (await duplicateRedis(this.redis)) ?? this.redis;
        if (!this.subscribed || gen !== this.loopGen) break;
        const conn = this.conn;
        const now = Date.now();
        // heartbeat + stale-group cleanup + trim (Java renewExpiration + update)
        try {
          await this.redis.zadd(this.timeoutKey, String(now + 120000), this.subscriberId);
          await luaEval(this.redis, RT_MAINTENANCE, 2, this.name, this.timeoutKey,
            this.subscriberId, String(now));
        } catch { /* maintenance is best effort */ }
        const res: any = await conn.xreadgroup(
          'GROUP', this.subscriberId, 'consumer', 'COUNT', 10, 'BLOCK', 2000, 'STREAMS', this.name, '>',
        );
        if (!res) continue;
        for (const stream of res as any[]) {
          for (const [msgId, fields] of (stream[1] ?? []) as [string, any][]) {
            if (!this.subscribed || gen !== this.loopGen) return;
            let payload: string | null = null;
            if (Array.isArray(fields)) {
              for (let i = 0; i + 1 < fields.length; i += 2) if (fields[i] === 'm') payload = fields[i + 1];
            } else if (fields && typeof fields === 'object') {
              payload = (fields as any)['m'] ?? null;
            }
            if (payload === null) continue;
            let msg: V;
            try {
              msg = this.decodeValue(payload) as V;
            } catch {
              msg = payload as unknown as V;
            }
            for (const cb of [...this.listeners.values()]) {
              try {
                await cb(msg);
              } catch { /* listener errors must not block delivery */ }
            }
            try {
              await this.redis.xack(this.name, this.subscriberId, msgId);
            } catch { /* noop */ }
          }
        }
      } catch (e) {
        if (!this.subscribed || gen !== this.loopGen) break;
        if (String((e as any)?.message ?? e).includes('NOGROUP')) {
          this.subscribed = false;
          break;
        }
        await sleep(1000);
      }
    }
    if (gen === this.loopGen) this.closeConn();
  }
  async removeListener(id: string): Promise<void> {
    this.listeners.delete(id);
    if (this.listeners.size === 0) await this.unsubscribe();
  }
  async removeAllListeners(): Promise<void> {
    this.listeners.clear();
    await this.unsubscribe();
  }
  private async unsubscribe(): Promise<void> {
    if (!this.subscribed) return;
    this.subscribed = false;
    this.loopGen += 1;
    this.closeConn();
    try {
      await this.redis.xgroup('DESTROY', this.name, this.subscriberId);
    } catch { /* noop */ }
    try {
      await this.redis.zrem(this.timeoutKey, this.subscriberId);
    } catch { /* noop */ }
  }
  override async delete(): Promise<boolean> {
    await this.unsubscribe();
    await this.redis.del(this.name, this.timeoutKey);
    return true;
  }
  override async unlink(): Promise<boolean> {
    await this.unsubscribe();
    await this.unlinkKeys([this.name, this.timeoutKey]);
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

export interface StreamEntry<V = any> {
  id: string;
  fields: Record<string, V>;
}

/** Trim strategy for XADD (mirrors StreamTrimArgs subset). */
export interface StreamTrimOptions {
  maxLen?: number;
  minId?: string;
  /** default false → '~' marker (Redisson default non-strict) */
  strict?: boolean;
  limit?: number;
}

/** Full XADD options (mirrors StreamAddArgs subset). */
export interface StreamAddOptions extends StreamTrimOptions {
  id?: string;
  noMakeStream?: boolean;
  idempotentProducerId?: string;
  idempotentId?: string;
}

/** Full XREADGROUP options (mirrors StreamReadGroupArgs subset). */
export interface StreamReadGroupOptions {
  count?: number;
  /** Java 4.7 `maxCount()` — XREADGROUP MAXCOUNT (Redis 8.x). */
  maxCount?: number;
  /** Java 4.7 `maxSize()` — XREADGROUP MAXSIZE (Redis 8.x). */
  maxSize?: number;
  blockMs?: number;
  noAck?: boolean;
  /** min-idle-time CLAIM takeover, ms */
  claimIdleMs?: number;
}

export type StreamNackMode = 'SILENT' | 'FAIL' | 'FATAL';
export type StreamAckRefPolicy = 'KEEPREF' | 'DELREF' | 'ACKED';

/** Per-stream group spec for `RStream.readGroupMany`. */
export interface StreamGroupReadSpec {
  group: string;
  consumer: string;
  /** Last-seen id; defaults to `>` (new messages). */
  id?: string;
}

export class RStream extends RedisDataStructure {
  private keyspaceHub: KeyspaceListenerHub | null = null;
  private hub(): KeyspaceListenerHub {
    if (this.keyspaceHub === null) {
      this.keyspaceHub = new KeyspaceListenerHub(this.redis, this.name, {
        onAdd: 'xadd',
        onRemove: 'xdel',
        onCreateGroup: 'xgroup-create',
        onRemoveGroup: 'xgroup-destroy',
        onCreateConsumer: 'xgroup-createconsumer',
        onRemoveConsumer: 'xgroup-delconsumer',
      });
    }
    return this.keyspaceHub;
  }
  /** Java `addListener(ObjectListener)`: keyspace notifications
   *  (`__keyevent@*:xadd|xdel|xgroup-*`), filtered to this stream. Requires
   *  server `notify-keyspace-events` (K+E, e.g. "KEA"). */
  async addListener(listener: ObjectListener): Promise<number> {
    return this.hub().add(listener);
  }
  async removeListener(id: number): Promise<void> {
    return this.hub().remove(id);
  }
  async removeAllListeners(): Promise<void> {
    return this.hub().removeAll();
  }
  async add(fields: Record<string, any>, id = '*', maxLen?: number): Promise<string> {
    // Legacy path always creates the stream, so the id is never null.
    return (await this.addWithOptions(fields, { id, ...(maxLen !== undefined ? { maxLen } : {}) })) as string;
  }
  async addWithOptions(fields: Record<string, any>, opts: StreamAddOptions = {}): Promise<string | null> {
    // Order mirrors RedissonStream.addCustomAsync exactly: NOMKSTREAM,
    // IDMP/IDMPAUTO, MAXLEN [~], MINID [~], LIMIT, id, fields.
    const args: (string | number)[] = [this.name];
    if (opts.noMakeStream) args.push('NOMKSTREAM');
    if (opts.idempotentProducerId !== undefined && opts.idempotentId !== undefined) {
      args.push('IDMP', opts.idempotentProducerId, opts.idempotentId);
    } else if (opts.idempotentProducerId !== undefined) {
      args.push('IDMPAUTO', opts.idempotentProducerId);
    }
    if (opts.maxLen !== undefined && opts.maxLen > 0) {
      args.push('MAXLEN');
      if (!opts.strict) args.push('~');
      args.push(opts.maxLen);
    }
    if (opts.minId !== undefined) {
      args.push('MINID');
      if (!opts.strict) args.push('~');
      args.push(opts.minId);
    }
    if (opts.limit !== undefined && opts.limit > 0) args.push('LIMIT', opts.limit);
    const flat: string[] = [];
    for (const [k, v] of Object.entries(fields)) flat.push(this.encodeKey(k), this.encodeValue(v));
    args.push(opts.id ?? '*', ...flat);
    return (this.redis as any).xadd(...args);
  }
  async read(count = 10, lastId = '0'): Promise<StreamEntry[]> {
    const res: any = await this.redis.xrange(this.name, lastId === '0' ? '-' : `(${lastId}`, '+', 'COUNT', count);
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
    return this.redis.xlen(this.name);
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
  /** Java `trimNonStrict(StreamTrimArgs)` with maxLen — XTRIM MAXLEN ~ n. */
  async trimNonStrict(maxLen: number): Promise<number> {
    return this.trim(maxLen, true);
  }
  async deleteEntries(...ids: string[]): Promise<number> {
    if (ids.length === 0) return 0;
    return this.redis.xdel(this.name, ...ids);
  }
  async createGroup(group: string, lastId = '0'): Promise<void> {
    await this.redis.xgroup('CREATE', this.name, group, lastId, 'MKSTREAM');
  }
  async readGroup(group: string, consumer: string, count = 10, blockMs = -1): Promise<StreamEntry[]> {
    return this.readGroupWith(group, consumer, { count, ...(blockMs >= 0 ? { blockMs } : {}) });
  }
  /**
   * Full XREADGROUP (mirrors StreamReadGroupArgs subset): GROUP g c
   * [COUNT n] [BLOCK ms] [CLAIM ms] [NOACK] STREAMS key >.
   * Default read position is '>' (never delivered), like Redisson.
   */
  async readGroupWith(group: string, consumer: string, opts: StreamReadGroupOptions = {}): Promise<StreamEntry[]> {
    const args: (string | number)[] = ['GROUP', group, consumer];
    if (opts.count !== undefined && opts.count > 0) args.push('COUNT', opts.count);
    if (opts.maxCount !== undefined && opts.maxCount > 0) args.push('MAXCOUNT', opts.maxCount);
    if (opts.maxSize !== undefined && opts.maxSize > 0) args.push('MAXSIZE', opts.maxSize);
    if (opts.blockMs !== undefined && opts.blockMs >= 0) args.push('BLOCK', opts.blockMs);
    if (opts.claimIdleMs !== undefined && opts.claimIdleMs > 0) args.push('CLAIM', opts.claimIdleMs);
    if (opts.noAck) args.push('NOACK');
    args.push('STREAMS', this.name, '>');
    // A blocking XREADGROUP occupies the connection; keep the main one free
    // for lock watchdogs and ordinary commands.
    const res: any = opts.blockMs !== undefined && opts.blockMs >= 0
      ? await withDedicatedRedis(this.redis, (conn) => (conn as any).xreadgroup(...args))
      : await (this.redis as any).xreadgroup(...args);
    if (!res) return [];
    const out: StreamEntry[] = [];
    for (const stream of res) {
      for (const [eid, f] of stream[1] ?? []) out.push({ id: String(eid), fields: this.decodeFields(f) });
    }
    return out;
  }
  async ack(group: string, ...ids: string[]): Promise<number> {
    if (ids.length === 0) return 0;
    return this.redis.xack(this.name, group, ...ids);
  }
  /**
   * Conditional ack-and-delete (XACKDEL, Redis 8.2+):
   * XACKDEL key group [refpolicy] IDS n ids... Returns per-id status codes.
   */
  async ackWithDelete(group: string, refPolicy: StreamAckRefPolicy, ...ids: string[]): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    if (ids.length === 0) return out;
    const res: any[] = (await (this.redis as any).call('XACKDEL', this.name, group, refPolicy, 'IDS', ids.length, ...ids)) ?? [];
    ids.forEach((id, i) => out.set(id, Number(res[i] ?? -1)));
    return out;
  }
  /**
   * Requeue pending entries (XNACK, Redis 8.8+):
   * XNACK key group mode IDS n ids... [RETRYCOUNT n] [FORCE].
   */
  async nack(group: string, mode: StreamNackMode, ids: string[], retryCount?: number, force?: boolean): Promise<number> {
    if (ids.length === 0) return 0;
    const args: (string | number)[] = [this.name, group, mode, 'IDS', ids.length, ...ids];
    if (retryCount !== undefined) args.push('RETRYCOUNT', retryCount);
    if (force) args.push('FORCE');
    return toNumber(await (this.redis as any).xnack(...args));
  }
  async removeGroup(group: string): Promise<boolean> {
    return toNumber(await this.redis.xgroup('DESTROY', this.name, group)) === 1;
  }
  async createConsumer(group: string, consumer: string): Promise<boolean> {
    return toNumber(await this.redis.xgroup('CREATECONSUMER', this.name, group, consumer)) === 1;
  }
  async removeConsumer(group: string, consumer: string): Promise<number> {
    return toNumber(await this.redis.xgroup('DELCONSUMER', this.name, group, consumer));
  }
  async updateGroupMessageId(group: string, id: string): Promise<void> {
    await this.redis.xgroup('SETID', this.name, group, id);
  }
  /** XPENDING summary: [count, minId, maxId, [[consumer, count], ...]]. */
  async getPendingInfo(group: string): Promise<StreamPendingInfo> {
    const r: any = await this.redis.xpending(this.name, group);
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
      const got: any = await this.redis.xrange(this.name, row.id, row.id);
      for (const [eid, f] of got ?? []) out.push({ id: String(eid), fields: this.decodeFields(f) });
    }
    return out;
  }
  /** XCLAIM: take ownership of idle pending entries. */
  async claim(group: string, consumer: string, minIdleMs: number, ...ids: string[]): Promise<StreamEntry[]> {
    if (ids.length === 0) return [];
    const res: any = await this.redis.xclaim(this.name, group, consumer, minIdleMs, ...ids);
    return (res ?? []).map(([eid, f]: [string, string[]]) => ({ id: String(eid), fields: this.decodeFields(f) }));
  }
  /** XAUTOCLAIM: cursor-scan ownership transfer. Returns next cursor + entries. */
  async autoClaim(group: string, consumer: string, minIdleMs: number, startId = '0-0', count = 10): Promise<{ nextId: string; entries: StreamEntry[] }> {
    const res: any = await this.redis.xautoclaim(this.name, group, consumer, minIdleMs, startId, 'COUNT', count);
    if (!res) return { nextId: '0-0', entries: [] };
    const [cursor, rows] = res;
    return {
      nextId: String(cursor),
      entries: ((rows ?? []) as [string, string[]][]).map(([eid, f]) => ({ id: String(eid), fields: this.decodeFields(f) })),
    };
  }
  /** Java `fastClaim`: XCLAIM ... JUSTID — ids only, no field payload. */
  async fastClaim(group: string, consumer: string, minIdleMs: number, ...ids: string[]): Promise<string[]> {
    if (ids.length === 0) return [];
    const res: any = await (this.redis as any).xclaim(this.name, group, consumer, minIdleMs, ...ids, 'JUSTID');
    return (res ?? []).map((id: any) => String(id));
  }
  /** Java `fastAutoClaim`: XAUTOCLAIM ... JUSTID — cursor + claimed ids. */
  async fastAutoClaim(group: string, consumer: string, minIdleMs: number, startId = '0-0', count = 10): Promise<{ nextId: string; ids: string[] }> {
    const res: any = await (this.redis as any).xautoclaim(this.name, group, consumer, minIdleMs, startId, 'COUNT', count, 'JUSTID');
    if (!res) return { nextId: '0-0', ids: [] };
    const [cursor, ids] = res;
    return { nextId: String(cursor), ids: ((ids ?? []) as any[]).map((x) => String(x)) };
  }
  /** Java `read(StreamMultiReadArgs)`: one XREAD over several streams.
   *  `streams` maps stream name → last seen id (use `0` for the beginning). */
  async readMany(streams: Record<string, string>, count: number): Promise<Map<string, StreamEntry[]>>;
  /** Java 4.7 `read(StreamMultiReadArgs)` — XREAD with COUNT/MAXCOUNT/MAXSIZE. */
  async readMany(streams: Record<string, string>, opts: { count?: number; maxCount?: number; maxSize?: number }): Promise<Map<string, StreamEntry[]>>;
  async readMany(streams: Record<string, string>, a: number | { count?: number; maxCount?: number; maxSize?: number } = 10): Promise<Map<string, StreamEntry[]>> {
    const names = Object.keys(streams);
    const out = new Map<string, StreamEntry[]>();
    if (names.length === 0) return out;
    const opts = typeof a === 'number' ? { count: a } : a;
    const args: (string | number)[] = [];
    if (opts.count !== undefined && opts.count > 0) args.push('COUNT', opts.count);
    if (opts.maxCount !== undefined && opts.maxCount > 0) args.push('MAXCOUNT', opts.maxCount);
    if (opts.maxSize !== undefined && opts.maxSize > 0) args.push('MAXSIZE', opts.maxSize);
    args.push('STREAMS', ...names, ...names.map((n) => streams[n]));
    const res: any = await (this.redis as any).xread(...args);
    for (const [name, rows] of (res ?? []) as [string, [string, string[]][]][]) {
      out.set(String(name), ((rows ?? []) as [string, string[]][]).map(([eid, f]) => ({ id: String(eid), fields: this.decodeFields(f) })));
    }
    return out;
  }
  /** Java `readGroup(StreamMultiReadGroupArgs)`: one XREADGROUP across streams
   *  sharing the same group/consumer (`streams` maps name → last id). */
  async readGroupMany(group: string, consumer: string, streams: Record<string, string>, count?: number): Promise<Map<string, StreamEntry[]>>;
  /** Per-stream group/consumer variant (Java allows a different group per
   *  stream); streams are bucketed by group+consumer and read in one
   *  XREADGROUP per bucket. */
  async readGroupMany(specs: Record<string, StreamGroupReadSpec>, count?: number): Promise<Map<string, StreamEntry[]>>;
  async readGroupMany(
    a: string | Record<string, StreamGroupReadSpec>,
    b?: string | number,
    c?: Record<string, string>,
    d = 10,
  ): Promise<Map<string, StreamEntry[]>> {
    let specs: Record<string, StreamGroupReadSpec>;
    let count: number;
    if (typeof a === 'string') {
      count = d;
      specs = {};
      for (const [name, id] of Object.entries(c ?? {})) {
        specs[name] = { group: a, consumer: String(b), id };
      }
    } else {
      count = typeof b === 'number' ? b : 10;
      specs = a;
    }
    const buckets = new Map<string, { group: string; consumer: string; names: string[]; ids: string[] }>();
    for (const [name, s] of Object.entries(specs)) {
      const key = `${s.group}\u0000${s.consumer}`;
      let bucket = buckets.get(key);
      if (!bucket) {
        bucket = { group: s.group, consumer: s.consumer, names: [], ids: [] };
        buckets.set(key, bucket);
      }
      bucket.names.push(name);
      bucket.ids.push(s.id ?? '>');
    }
    const out = new Map<string, StreamEntry[]>();
    for (const bucket of buckets.values()) {
      const res: any = await (this.redis as any).xreadgroup(
        'GROUP', bucket.group, bucket.consumer, 'COUNT', count, 'STREAMS', ...bucket.names, ...bucket.ids,
      );
      for (const [name, rows] of (res ?? []) as [string, [string, string[]][]][]) {
        out.set(String(name), ((rows ?? []) as [string, string[]][]).map(([eid, f]) => ({ id: String(eid), fields: this.decodeFields(f) })));
      }
    }
    return out;
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
end
-- get startTime from scheduler queue head task (QueueTransferTask.pushTaskAsync)
local v = redis.call('zrange', KEYS[2], 0, 0, 'WITHSCORES')
if v[1] ~= nil then
    return v[2]
end
return nil`;

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
  private async migrateOnce(): Promise<number | null> {
    const now = await serverNowMs(this.redis);
    const next = await luaEval(this.redis, DQ_MIGRATE, 3, this.name, this.timeoutKey, this.queueKey, String(now), '100');
    return next === null || next === undefined ? null : Number(next);
  }
  private async migrate(): Promise<void> {
    try {
      await this.migrateOnce();
    } catch { /* mock fallback noop */ }
  }

  // -- background migrator (port of Java QueueTransferTask + QueueTransferService) --
  //
  // Java starts one shared transfer task per queue name when the object is
  // created; redi.ts keeps it opt-in: start() once, stop()/destroy() to
  // release the extra connection. poll()/peek() still run a synchronous
  // catch-up migration so delayed delivery also works without the task.
  private transferConn: Redis | null = null;
  private transferHandler: ((channel: string, message: string) => void) | null = null;
  private transferOnSubscribe: ((channel: string) => void) | null = null;
  private deadlineTimer: ReturnType<typeof setTimeout> | null = null;
  private scheduleGen = 0;
  private started = false;
  private destroyed = false;

  /** Subscribe to the queue channel and pump expired members in the
   * background. Idempotent. Mock (no duplicate connection): no-op — offers
   * already land in the destination queue there. */
  async start(): Promise<void> {
    if (this.started || this.destroyed) return;
    this.started = true;
    const conn = await duplicateRedis(this.redis);
    if (!conn || !this.started) {
      try { (conn as any)?.disconnect?.(); } catch { /* noop */ }
      return;
    }
    this.transferConn = conn;
    const onMessage = (channel: string, message: string) => {
      if (channel !== this.channel) return;
      const deadline = Number(message);
      if (Number.isFinite(deadline)) void this.scheduleTask(deadline);
    };
    // Java BaseStatusListener.onSubscribe: catch up right after (re)subscribe
    const onSubscribe = (channel: string) => {
      if (channel === this.channel) void this.pushTask();
    };
    this.transferHandler = onMessage;
    this.transferOnSubscribe = onSubscribe;
    (conn as any).on?.('message', onMessage);
    (conn as any).on?.('subscribe', onSubscribe);
    try {
      await conn.subscribe(this.channel);
    } catch (e) {
      await this.stop();
      throw e;
    }
    // Java BaseStatusListener.onSubscribe → pushTask: catch up on members that
    // expired while the migrator was not running. Done explicitly (not only via
    // the 'subscribe' event) because ioredis Cluster does not re-emit it.
    void this.pushTask();
  }

  /** Cancel the deadline timer and drop the subscription. Idempotent. */
  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    this.scheduleGen++;
    if (this.deadlineTimer) {
      clearTimeout(this.deadlineTimer);
      this.deadlineTimer = null;
    }
    const conn = this.transferConn;
    this.transferConn = null;
    if (conn) {
      try {
        if (this.transferHandler) (conn as any).off?.('message', this.transferHandler);
        if (this.transferOnSubscribe) (conn as any).off?.('subscribe', this.transferOnSubscribe);
        await (conn as any).unsubscribe?.(this.channel);
      } catch { /* noop */ }
      try { (conn as any).disconnect?.(); } catch { /* noop */ }
    }
    this.transferHandler = null;
    this.transferOnSubscribe = null;
  }

  /** Java destroy(): detach the transfer task and its listeners only — the
   * destination queue and staged members survive (use delete() for keys). */
  async destroy(): Promise<void> {
    this.destroyed = true;
    await this.stop();
  }

  // Java QueueTransferTask.scheduleTask: >10ms → arm a timer, else push now
  private async scheduleTask(deadline: number | null): Promise<void> {
    if (!this.started || deadline === null || !Number.isFinite(deadline)) return;
    const gen = ++this.scheduleGen;
    if (this.deadlineTimer) {
      clearTimeout(this.deadlineTimer);
      this.deadlineTimer = null;
    }
    let now: number;
    try {
      now = await serverNowMs(this.redis);
    } catch {
      now = Date.now();
    }
    if (this.scheduleGen !== gen || !this.started) return;
    const delay = deadline - now;
    if (delay > 10) {
      const t = setTimeout(() => {
        this.deadlineTimer = null;
        void this.pushTask();
      }, delay);
      (t as any).unref?.();
      this.deadlineTimer = t;
    } else {
      await this.pushTask();
    }
  }

  // Java QueueTransferTask.pushTask: migrate, then follow the next head
  // deadline; on error retry after 5s.
  private async pushTask(): Promise<void> {
    if (!this.started) return;
    let next: number | null;
    try {
      next = await this.migrateOnce();
    } catch {
      void this.scheduleTask(Date.now() + 5000);
      return;
    }
    if (next !== null) void this.scheduleTask(next);
  }
  /** Java `add(V)`: unsupported on a delayed queue — use timed `offer`. */
  async add(_value: V): Promise<boolean> {
    throw new Error("Use 'offer' method with timeout param");
  }
  /** Java `offer(V)`: unsupported without a delay. */
  async offer(value: V): Promise<void>;
  async offer(value: V, delayMs: number): Promise<void>;
  async offer(value: V, delayMs?: number): Promise<void> {
    if (delayMs === undefined) throw new Error("Use 'offer' method with timeout param");
    if (delayMs < 0) throw new Error("Delay can't be negative");
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
  async poll(): Promise<V | null>;
  async poll(count: number): Promise<V[]>;
  async poll(count?: number): Promise<V | null | V[]> {
    await this.migrate();
    if (count === undefined) {
      const raw = await this.redis.lpop(this.name);
      return raw === null ? null : (this.decodeValue(raw) as V);
    }
    if (count <= 0) return [];
    let arr: string[];
    try {
      const raw: any = await this.redis.lpop(this.name, count);
      arr = raw === null || raw === undefined ? [] : Array.isArray(raw) ? raw : [String(raw)];
    } catch {
      arr = [];
      for (let i = 0; i < count; i++) {
        const v = await this.redis.lpop(this.name);
        if (v === null) break;
        arr.push(v);
      }
    }
    return arr.map((x) => this.decodeValue(x) as V);
  }
  /** Java `RQueue.readAll()`: destination snapshot after catch-up migration. */
  async readAll(): Promise<V[]> {
    await this.migrate();
    const m = await this.redis.lrange(this.name, 0, -1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  async peek(): Promise<V | null> {
    await this.migrate();
    const raw = await this.redis.lindex(this.name, 0);
    return raw === null ? null : (this.decodeValue(raw) as V);
  }
  async size(): Promise<number> {
    // Java RedissonDelayedQueue.size() = staging LIST length (not yet
    // migrated), NOT the destination queue length.
    return this.redis.llen(this.queueKey);
  }
  /** Destination queue length (TS helper; Java's RBlockingQueue.size()). */
  async targetSize(): Promise<number> {
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
  /** Java `Queue.remove()`: poll or throw NoSuchElementException. */
  async remove(): Promise<V>;
  /** Java `Queue.remove(Object)`: remove a staged member. */
  async remove(value: V): Promise<boolean>;
  async remove(value?: V): Promise<V | boolean> {
    if (arguments.length === 0) {
      const v = await this.poll();
      if (v === null) throw new Error('NoSuchElementException');
      return v;
    }
    const enc = this.encodeValue(value as V);
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
  /** Java `indexOf(V)`: unsupported on a delayed queue. */
  async indexOf(_value: V): Promise<number> {
    throw new Error('UnsupportedOperationException');
  }
  /** Java `pollLastAndOfferFirstTo(dest)`: atomically move the staging-list
   *  tail (removing its timeout entry) to the destination head. */
  async pollLastAndOfferFirstTo(dest: string): Promise<V | null> {
    const lua = `local v = redis.call('rpop', KEYS[1])
if v == false then return nil end
redis.call('zrem', KEYS[2], v)
local _, value = struct.unpack('Bc0Lc0', v)
redis.call('lpush', KEYS[3], value)
return value`;
    try {
      const raw = await luaEval(this.redis, lua, 3, this.queueKey, this.timeoutKey, dest);
      return raw === null || raw === undefined ? null : (this.decodeValue(String(raw)) as V);
    } catch {
      const raw: any = (this.redis as any).rpopBuffer
        ? await (this.redis as any).rpopBuffer(this.queueKey)
        : await this.redis.rpop(this.queueKey);
      if (raw === null || raw === undefined) return null;
      const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(String(raw), 'latin1');
      await this.redis.zrem(this.timeoutKey, buf);
      const u = unpackDelayedMember(buf);
      const payload = u ? u.enc : buf.toString('utf8');
      await this.redis.lpush(dest, payload);
      return this.decodeValue(payload) as V;
    }
  }
  async clear(): Promise<void> {
    await this.redis.del(this.queueKey, this.timeoutKey);
  }
  // Java deletes ONLY the staging LIST and timeout ZSET; the destination
  // queue (`this.name`) belongs to the caller and must survive.
  override async delete(): Promise<boolean> {
    return this.deleteKeys([this.queueKey, this.timeoutKey]);
  }
  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.queueKey, this.timeoutKey]);
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
  /** Java 4.7 `readNewest(count)`: the `count` most recently added elements. */
  async readNewest(count: number): Promise<V[]> {
    if (count <= 0) return [];
    const m = await this.redis.lrange(this.name, -count, -1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  /** Java 4.7 `readOldest(count)`: the `count` oldest elements. */
  async readOldest(count: number): Promise<V[]> {
    if (count <= 0) return [];
    const m = await this.redis.lrange(this.name, 0, count - 1);
    return m.map((x) => this.decodeValue(x) as V);
  }
  /** Java 4.7 `peekLast()`: newest element without removing it. */
  async peekLast(): Promise<V | null> {
    const m = await this.redis.lrange(this.name, -1, -1);
    return m.length === 0 ? null : (this.decodeValue(m[0]) as V);
  }
  /** Java 4.8 `addAll(Collection)`: bulk RPUSH in 5000-chunks, then LTRIM the
   *  overflow (requires capacity to be set). */
  async addAll(values: V[]): Promise<boolean> {
    if (values.length === 0) return false;
    const enc = values.map((v) => this.encodeValue(v));
    const s = `local limit = redis.call('get', KEYS[2])
assert(limit ~= false, 'RingBuffer capacity is not defined')
local size = 0
for i = 1, #ARGV, 5000 do
    size = redis.call('rpush', KEYS[1], unpack(ARGV, i, math.min(i + 4999, #ARGV)))
end
local extraSize = size - tonumber(limit)
if extraSize > 0 then
    redis.call('ltrim', KEYS[1], extraSize, -1)
end
return 1`;
    try {
      await luaEval(this.redis, s, 2, this.name, this.capacityKey, ...enc);
    } catch (e) {
      if (String((e as any)?.message ?? e).includes('capacity')) throw e;
      // mock fallback
      const cap = await this.capacity();
      if (cap === null) throw new Error('RingBuffer capacity is not defined');
      await this.redis.rpush(this.name, ...enc);
      const len = await this.redis.llen(this.name);
      if (len > cap) await this.redis.ltrim(this.name, len - cap, -1);
    }
    return true;
  }
  async size(): Promise<number> {
    return this.redis.llen(this.name);
  }
  async clear(): Promise<void> {
    // Java RedissonRingBuffer.clear() deletes only the LIST; capacity
    // survives by design. delete() (below) removes both.
    await this.redis.del(this.name);
  }

  override async delete(): Promise<boolean> {
    return this.deleteKeys([this.name, this.capacityKey]);
  }

  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name, this.capacityKey]);
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
