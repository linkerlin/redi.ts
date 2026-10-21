/**
 * Java `ObjectListener` surface backed by Redis keyspace notifications.
 *
 * Java's RedissonStream/RedissonTimeSeries subscribe to
 * `__keyevent@*:<event>` patterns and filter messages by key name. The server
 * must have `notify-keyspace-events` enabled with K and E (e.g. "KEA");
 * without it subscriptions are silent — the same server-side dependency as
 * Java. Listener callbacks receive the object's key name.
 */
import type { Redis } from 'ioredis';
import { duplicateRedis } from './util.js';

export interface ObjectListener {
  onAdd?(key: string): void;
  onRemove?(key: string, field?: string | null): void;
  onCreateGroup?(key: string): void;
  onRemoveGroup?(key: string): void;
  onCreateConsumer?(key: string): void;
  onRemoveConsumer?(key: string): void;
  /** RKeys: `new` key event (requires the `n` notify flag). */
  onNew?(key: string): void;
  /** RKeys: `set` event. */
  onSet?(key: string): void;
  /** RKeys: `expired` event. */
  onExpired?(key: string): void;
  /** RKeys: `del` event. */
  onDeleted?(key: string): void;
  /** RKeys: flush event (RESP3 only in Java 4.6.1). */
  onFlush?(address: string): void;
  /** RSet: SPOP event. */
  onRandomRemove?(key: string): void;
  /** RSet: SINTERSTORE/SUNIONSTORE/SDIFFSTORE event. */
  onStore?(key: string): void;
  /** Atomic structures: INCRBY/INCRBYFLOAT event. */
  onChange?(key: string): void;
  /** RMap: HSET event (field is null when only keyevent is available). */
  onPut?(key: string, field: string | null): void;
  /** RMap: HINCRBYFLOAT event. */
  onIncrement?(key: string, field: string | null): void;
  /** RMapCacheNative: HEXPIRED event. */
  onExpired?(key: string, field?: string | null): void;
  /** RMapCacheNative: HPERSIST event. */
  onClearExpire?(key: string, field: string | null): void;
}

interface KeyspaceSub {
  conn: Redis;
  patterns: string[];
  handlers: ((pat: string, ch: string, msg: string) => void)[];
}

/** One hub per structure instance; event names map callbacks to keyevent types. */
export class KeyspaceListenerHub {
  private subs = new Map<number, KeyspaceSub>();
  private nextId = 1;
  constructor(
    private redis: Redis,
    private name: string,
    private events: Partial<Record<keyof ObjectListener, string | string[]>>,
    /** false for the global RKeys hub: deliver the message key, not `name`. */
    private filterName = true,
  ) {}

  async add(listener: ObjectListener): Promise<number> {
    const entries = (Object.entries(this.events) as [keyof ObjectListener, string | string[]][])
      .filter(([kind]) => typeof listener[kind] === 'function');
    if (entries.length === 0) return 0;
    const id = this.nextId++;
    const conn = await duplicateRedis(this.redis);
    if (!conn) {
      // mock: no keyspace delivery, but registration must remain removable
      this.subs.set(id, { conn: this.redis, patterns: [], handlers: [] });
      return id;
    }
    const sub: KeyspaceSub = { conn, patterns: [], handlers: [] };
    try {
      for (const [kind, event] of entries) {
        for (const eventName of (Array.isArray(event) ? event : [event])) {
          const pattern = `__keyevent@*:${eventName}`;
          const handler = (pat: string, _ch: string, msg: string) => {
            if (pat !== pattern) return;
            if (this.filterName && msg !== this.name) return;
            try {
              // Field-aware listeners receive null unless a subkey event exists
              // (Redis 8.6 emits only keyevent; Java falls back the same way).
              (listener[kind] as (key: string, field?: string | null) => void)
                .call(listener, this.filterName ? this.name : msg, null);
            } catch { /* user callback */ }
          };
          // Attach before PSUBSCRIBE so no event in the gap is lost.
          (conn as any).on('pmessage', handler);
          sub.handlers.push(handler);
          await (conn as any).psubscribe(pattern);
          sub.patterns.push(pattern);
        }
      }
    } catch (e) {
      try {
        for (const h of sub.handlers) (conn as any).off?.('pmessage', h);
        (conn as any).disconnect?.();
      } catch { /* noop */ }
      throw e;
    }
    this.subs.set(id, sub);
    return id;
  }

  async remove(id: number): Promise<void> {
    const s = this.subs.get(id);
    if (!s) return;
    this.subs.delete(id);
    try {
      for (const h of s.handlers) (s.conn as any).off?.('pmessage', h);
      for (const p of s.patterns) await (s.conn as any).punsubscribe?.(p);
      if (s.conn !== this.redis) (s.conn as any).disconnect?.();
    } catch { /* noop */ }
  }

  async removeAll(): Promise<void> {
    for (const id of [...this.subs.keys()]) await this.remove(id);
  }
}

const sharedKeysHubs = new WeakMap<Redis, KeyspaceListenerHub>();

/** One global RKeys hub per Redis client (getKeys() returns new facades). */
export function sharedKeysListenerHub(redis: Redis): KeyspaceListenerHub {
  let hub = sharedKeysHubs.get(redis);
  if (!hub) {
    hub = new KeyspaceListenerHub(redis, '*', {
      onNew: 'new',
      onSet: 'set',
      onExpired: 'expired',
      onDeleted: 'del',
    }, false);
    sharedKeysHubs.set(redis, hub);
  }
  return hub;
}
