/**
 * Distributed locks — wire-compatible with Redisson (Java) + redi.go/redi.py.
 *
 * Key layouts (cluster hash-tagged):
 *  RLock: HASH {name}, field=<clientId>:<threadId>, channel redisson_lock__channel:{name}
 *  RFairLock: + LIST redisson_lock_queue:{name} + ZSET redisson_lock_timeout:{name}
 *  RReadWriteLock: single HASH {name} with mode/read/write fields, channel redisson_rwlock:{name}
 *  RSemaphore: STRING {name} + channel redisson_sc:{name}
 *  RCountDownLatch: STRING {name} + channel redisson_countdownlatch__channel__{name}
 *  RPermitExpirableSemaphore: STRING {name} + ZSET {name}:timeout + channel redisson_sc:{name}
 *  RFencedLock: RLock HASH + STRING redisson_lock_token:{name}
 *
 * Lua scripts are verbatim ports of redi.go (verified against Redisson 4.6.1).
 */
import type { Redis } from 'ioredis';
import { RedisDataStructure } from './base.js';
import { LockNotHeldError, NonReentrantError, luaEval, prefixName, serverNowMs, sleep, suffixName, toNumber } from './util.js';
import { randomBytes } from 'node:crypto';

export const UNLOCK_MSG = '0';

// -- Lua (verbatim from redi.go, Redisson 4.6.1) ------------------------------

export const LOCK_ACQUIRE_SCRIPT = `if (redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return redis.call('pttl', KEYS[1])`;

export const LOCK_RENEW_SCRIPT = `if (redis.call('hexists', KEYS[1], ARGV[1]) == 1) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 1
end
return 0`;

export const LOCK_UNLOCK_SCRIPT = `if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return counter
end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[1])
return 0`;

export const LOCK_FORCE_UNLOCK_SCRIPT = `if (redis.call('del', KEYS[1]) == 1) then
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;

const NON_REENTRANT_ACQUIRE = `if (redis.call('exists', KEYS[1]) == 0) then
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return redis.call('pttl', KEYS[1])`;

const NON_REENTRANT_UNLOCK = `if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return -1
end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[1])
return 0`;

const FENCED_ACQUIRE = `if (redis.call('exists', KEYS[1]) == 0) or (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    local token = redis.call('incr', KEYS[2])
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return {-1, token}
end
return {redis.call('pttl', KEYS[1]), -1}`;

const FAIR_TRY_SCRIPT = `while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[3]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0)
        and ((redis.call('exists', KEYS[2]) == 0)
        or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
    redis.call('lpop', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
    local keys = redis.call('zrange', KEYS[3], 0, -1)
    for i = 1, #keys, 1 do
        redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i])
    end
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    if ARGV[5] == '0' then
        return ARGV[6]
    end
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
return 1`;

const FAIR_ACQUIRE_SCRIPT = `while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[4]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0)
        and ((redis.call('exists', KEYS[2]) == 0)
        or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then
    redis.call('lpop', KEYS[2])
    redis.call('zrem', KEYS[3], ARGV[2])
    local keys = redis.call('zrange', KEYS[3], 0, -1)
    for i = 1, #keys, 1 do
        redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i])
    end
    redis.call('hset', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    if ARGV[5] == '0' then
        return ARGV[6]
    end
    redis.call('hincrby', KEYS[1], ARGV[2], 1)
    redis.call('pexpire', KEYS[1], ARGV[1])
    return nil
end
local timeout = redis.call('zscore', KEYS[3], ARGV[2])
if timeout ~= false then
    local ttl = redis.call('pttl', KEYS[1])
    return math.max(0, ttl)
end
local lastThreadId = redis.call('lindex', KEYS[2], -1)
local ttl
if lastThreadId ~= false and lastThreadId ~= ARGV[2]
        and redis.call('zscore', KEYS[3], lastThreadId) ~= false then
    ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4])
else
    ttl = redis.call('pttl', KEYS[1])
end
local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4])
if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then
    redis.call('rpush', KEYS[2], ARGV[2])
end
return ttl`;

const FAIR_FAIL_SCRIPT = `local queue = redis.call('lrange', KEYS[1], 0, -1)
local i = 1
while i <= #queue and queue[i] ~= ARGV[1] do
    i = i + 1
end
i = i + 1
while i <= #queue do
    redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i])
    i = i + 1
end
redis.call('zrem', KEYS[2], ARGV[1])
redis.call('lrem', KEYS[1], 0, ARGV[1])`;

const FAIR_UNLOCK_SCRIPT = `while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[4]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('exists', KEYS[1]) == 0) then
    local nextThreadId = redis.call('lindex', KEYS[2], 0)
    if nextThreadId ~= false then
        redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
    end
    return 1
end
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return nil
end
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1)
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2])
    return 0
end
redis.call('del', KEYS[1])
local nextThreadId = redis.call('lindex', KEYS[2], 0)
if nextThreadId ~= false then
    redis.call(ARGV[5], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
end
return 1`;

const FAIR_FORCE_SCRIPT = `while true do
    local firstThreadId2 = redis.call('lindex', KEYS[2], 0)
    if firstThreadId2 == false then
        break
    end
    local timeout = redis.call('zscore', KEYS[3], firstThreadId2)
    if timeout ~= false and tonumber(timeout) <= tonumber(ARGV[2]) then
        redis.call('zrem', KEYS[3], firstThreadId2)
        redis.call('lpop', KEYS[2])
    else
        break
    end
end
if (redis.call('del', KEYS[1]) == 1) then
    local nextThreadId = redis.call('lindex', KEYS[2], 0)
    if nextThreadId ~= false then
        redis.call(ARGV[3], KEYS[4] .. ':' .. nextThreadId, ARGV[1])
    end
    return 1
end
return 0`;

const RW_READ_ACQUIRE = `local lease = tonumber(ARGV[3])
local function markRead(ind)
    local key = KEYS[2] .. ':' .. ind
    redis.call('set', key, 1)
    redis.call('pexpire', key, lease)
end
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    local ind = redis.call('hincrby', KEYS[1], ARGV[1], 1)
    markRead(ind)
    local remain = redis.call('pttl', KEYS[1])
    redis.call('pexpire', KEYS[1], math.max(remain, lease))
    return nil
end
if redis.call('exists', KEYS[1]) == 0 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    redis.call('hset', KEYS[1], 'mode', 'read')
    markRead(1)
    redis.call('pexpire', KEYS[1], lease)
    return nil
end
local mode = redis.call('hget', KEYS[1], 'mode')
if mode == 'read' or (mode == 'read-write' and redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('hset', KEYS[1], 'mode', 'read-write')
    local ind = redis.call('hincrby', KEYS[1], ARGV[1], 1)
    markRead(ind)
    local remain = redis.call('pttl', KEYS[1])
    redis.call('pexpire', KEYS[1], math.max(remain, lease))
    return nil
end
if mode == 'write' and redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    local ind = redis.call('hincrby', KEYS[1], ARGV[1], 1)
    markRead(ind)
    local remain = redis.call('pttl', KEYS[1])
    redis.call('pexpire', KEYS[1], math.max(remain, lease))
    return nil
end
return redis.call('pttl', KEYS[1])`;

const RW_WRITE_ACQUIRE = `local mode = redis.call('hget', KEYS[1], 'mode')
if mode == false then
    redis.call('hset', KEYS[1], 'mode', 'write')
    redis.call('hset', KEYS[1], ARGV[1], 1)
    redis.call('pexpire', KEYS[1], ARGV[3])
    return nil
end
if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
    redis.call('hincrby', KEYS[1], ARGV[1], 1)
    local cur = redis.call('pttl', KEYS[1])
    if cur > tonumber(ARGV[3]) then
        redis.call('pexpire', KEYS[1], cur)
    else
        redis.call('pexpire', KEYS[1], ARGV[3])
    end
    return nil
end
return redis.call('pttl', KEYS[1])`;

const RW_READ_RENEW = `local counter = tonumber(redis.call('hget', KEYS[1], ARGV[1]))
if counter == false then return 0 end
for i = counter, 1, -1 do
    redis.call('pexpire', KEYS[2] .. ':' .. ARGV[1] .. ':rwlock_timeout:' .. i, ARGV[2])
end
redis.call('pexpire', KEYS[1], ARGV[2])
return 1`;

const RW_READ_UNLOCK = `if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[1], -1)
if counter == 0 then
    redis.call('hdel', KEYS[1], ARGV[1])
end
redis.call('del', KEYS[3] .. ':' .. (counter + 1))
local function maxRemain()
    local maxr = -3
    local fields = redis.call('hkeys', KEYS[1])
    for _, key in ipairs(fields) do
        local c = tonumber(redis.call('hget', KEYS[1], key))
        if c ~= nil then
            for i = c, 1, -1 do
                local r = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i)
                if r > maxr then maxr = r end
            end
        end
    end
    return maxr
end
if counter > 0 then
    local mr = maxRemain()
    if mr > 0 then redis.call('pexpire', KEYS[1], mr) end
    return counter
end
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    local keys = redis.call('hkeys', KEYS[1])
    local readers = false
    for _, k in ipairs(keys) do
        if k ~= 'mode' and k ~= ARGV[2] then readers = true break end
    end
    if readers then
        redis.call('hset', KEYS[1], 'mode', 'read-write')
    else
        redis.call('hset', KEYS[1], 'mode', 'write')
    end
    local mr = maxRemain()
    if mr > 0 then redis.call('pexpire', KEYS[1], mr) end
    return 0
end
if redis.call('hlen', KEYS[1]) > 1 then
    local mr = maxRemain()
    if mr > 0 then
        redis.call('hset', KEYS[1], 'mode', 'read')
        redis.call('pexpire', KEYS[1], mr)
        return 0
    end
    -- legacy holders without :rwlock_timeout keys: keep the lock alive
    local ttl = redis.call('pttl', KEYS[1])
    redis.call('hset', KEYS[1], 'mode', 'read')
    if ttl > 0 then redis.call('pexpire', KEYS[1], ttl) end
    return 0
end
redis.call('del', KEYS[1])
redis.call('publish', KEYS[2], ARGV[4])
return 0`;

const RW_WRITE_UNLOCK = `if redis.call('hexists', KEYS[1], ARGV[1]) == 0 then
    return -1
end
local counter = redis.call('hincrby', KEYS[1], ARGV[1], -1)
if counter > 0 then
    redis.call('pexpire', KEYS[1], ARGV[3])
    return counter
end
redis.call('hdel', KEYS[1], ARGV[1])
if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then
    redis.call('hset', KEYS[1], 'mode', 'read')
    return 0
end
local keys = redis.call('hkeys', KEYS[1])
local any = false
for _, k in ipairs(keys) do
    if k ~= 'mode' then
        any = true
        break
    end
end
if any then
    redis.call('hset', KEYS[1], 'mode', 'read')
else
    redis.call('del', KEYS[1])
    redis.call('publish', KEYS[2], ARGV[4])
end
return 0`;

const SEM_ACQUIRE = `local value = redis.call('get', KEYS[1])
if (value ~= false and tonumber(value) >= tonumber(ARGV[1])) then
    redis.call('decrby', KEYS[1], ARGV[1])
    return 1
end
return 0`;

const SEM_RELEASE = `local value = redis.call('incrby', KEYS[1], ARGV[1])
redis.call('publish', KEYS[2], ARGV[1])
return value`;

const SEM_DRAIN = `local value = redis.call('get', KEYS[1])
if value == false then
    return 0
end
redis.call('set', KEYS[1], 0)
return value`;

const SEM_TRYSET = `if redis.call('get', KEYS[1]) == false then
    redis.call('set', KEYS[1], ARGV[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;

const SEM_TRYSET_TTL = `if redis.call('get', KEYS[1]) == false then
    redis.call('set', KEYS[1], ARGV[1], 'px', ARGV[2])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;

const CDL_COUNTDOWN = `local v = redis.call('decr', KEYS[1])
if v <= 0 then
    redis.call('del', KEYS[1])
end
if v == 0 then
    redis.call('publish', KEYS[2], ARGV[1])
end
return v`;

const CDL_SET = `if redis.call('exists', KEYS[1]) == 0 then
    redis.call('set', KEYS[1], ARGV[2])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
else
    return 0
end`;

const PES_ACQUIRE = `local key = KEYS[1]
local permits_key = KEYS[2]
local permits = tonumber(ARGV[1])
local lease_ms = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local expired = redis.call('zrangebyscore', permits_key, 0, now)
if #expired > 0 then
    redis.call('zremrangebyscore', permits_key, 0, now)
    redis.call('incrby', key, #expired)
end
local available = tonumber(redis.call('get', key) or '0')
if available < permits then
    return 0
end
redis.call('decrby', key, permits)
for i = 4, #ARGV do
    redis.call('zadd', permits_key, now + lease_ms, ARGV[i])
end
return 1`;

const PES_RELEASE = `local removed = redis.call('zrem', KEYS[2], ARGV[1])
if removed == 1 then
    redis.call('incrby', KEYS[1], 1)
    redis.call('publish', KEYS[3], ARGV[1])
    return 1
end
return 0`;

const PES_PURGE = `local expired = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1])
if #expired > 0 then
    redis.call('zremrangebyscore', KEYS[2], 0, ARGV[1])
    redis.call('incrby', KEYS[1], #expired)
end
return #expired`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function threadField(clientId: string, threadId: number | string): string {
  return `${clientId}:${threadId}`;
}

function abortError(): Error {
  const e = new Error('The operation was aborted');
  e.name = 'AbortError';
  return e;
}

/** Wait for a wake-up, rejecting with AbortError when `signal` fires first. */
async function waitForWakeAbortable(redis: Redis, channel: string, timeoutMs: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return waitForWake(redis, channel, timeoutMs);
  if (signal.aborted) throw abortError();
  let onAbort: (() => void) | null = null;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(abortError());
    signal.addEventListener('abort', onAbort, { once: true });
  });
  try {
    await Promise.race([waitForWake(redis, channel, timeoutMs), aborted]);
  } finally {
    if (onAbort) signal.removeEventListener('abort', onAbort);
  }
}

/** Subscribe once and wait for a single wake-up or timeout. */
async function waitForWake(redis: Redis, channel: string, timeoutMs: number): Promise<void> {
  let sub: Redis | null = null;
  try {
    const Dup = (redis as any).duplicate?.bind(redis);
    sub = Dup ? Dup() : redis;
    if (sub === redis) {
      await sleep(Math.min(timeoutMs, 100));
      return;
    }
    await (sub as any).subscribe(channel);
    await Promise.race([
      new Promise<void>((resolve) => {
        const onMsg = () => {
          try {
            (sub as any).off?.('message', onMsg);
          } catch { /* noop */ }
          resolve();
        };
        (sub as any).on('message', onMsg);
        setTimeout(resolve, timeoutMs);
      }),
      sleep(timeoutMs),
    ]);
  } catch {
    await sleep(Math.min(timeoutMs, 100));
  } finally {
    try {
      await (sub as any)?.unsubscribe?.(channel);
      if (sub && sub !== redis) (sub as any)?.disconnect?.();
    } catch { /* noop */ }
  }
}

// ---------------------------------------------------------------------------
// RLock
// ---------------------------------------------------------------------------

export class RLock extends RedisDataStructure {
  protected channel: string;
  protected clientId: string;
  protected watchdogMs: number;
  private timers = new Map<string, any>();

  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000) {
    super(redis, name);
    this.channel = prefixName('redisson_lock__channel', name);
    this.clientId = clientId;
    this.watchdogMs = watchdogMs;
  }

  getChannel(): string {
    return this.channel;
  }
  protected field(threadId: number | string = 1): string {
    return threadField(this.clientId, threadId);
  }

  protected async tryAcquire(leaseMs: number, threadId: number | string): Promise<number | null> {
    const r = await luaEval(this.redis, LOCK_ACQUIRE_SCRIPT, 1, this.name, String(leaseMs), this.field(threadId));
    return r === null || r === undefined ? null : Number(r);
  }

  protected startWatchdog(threadId: number | string): void {
    const f = this.field(threadId);
    this.stopWatchdog(threadId);
    const t = setInterval(() => {
      luaEval(this.redis, LOCK_RENEW_SCRIPT, 1, this.name, f, String(this.watchdogMs))
        .then((r) => {
          if (toNumber(r) === 0) this.stopWatchdog(threadId);
        })
        .catch(() => undefined);
    }, Math.max(1, Math.floor(this.watchdogMs / 3)));
    (t as any).unref?.();
    this.timers.set(f, t);
  }

  protected stopWatchdog(threadId: number | string): void {
    const f = this.field(threadId);
    const t = this.timers.get(f);
    if (t) clearInterval(t);
    this.timers.delete(f);
  }

  protected stopAllWatchdogs(): void {
    for (const t of this.timers.values()) clearInterval(t);
    this.timers.clear();
  }

  /** Acquire; leaseTimeMs <= 0 / undefined enables the watchdog. */
  async lock(leaseTimeMs?: number, threadId: number | string = 1): Promise<void> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    for (;;) {
      const ttl = await this.tryAcquire(lease, threadId);
      if (ttl === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return;
      }
      // contended: wait for wake-up with 1s fallback (Redisson parity)
      await waitForWake(this.redis, this.channel, Math.min(Math.max(ttl, 1), 1000));
    }
  }

  async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const deadline = Date.now() + waitTimeMs;
    const ttl = await this.tryAcquire(lease, threadId);
    if (ttl === null) {
      if (useWatchdog) this.startWatchdog(threadId);
      return true;
    }
    if (waitTimeMs <= 0) return false;
    for (;;) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) return false;
      await waitForWake(this.redis, this.channel, Math.min(remaining, 1000));
      if (Date.now() >= deadline) return false;
      const t2 = await this.tryAcquire(lease, threadId);
      if (t2 === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return true;
      }
      if (Date.now() >= deadline) return false;
    }
  }

  /** Java `lockInterruptibly()`: acquire, but give up with an `AbortError`
   *  when `signal` aborts while waiting (TS has no thread interruption, so an
   *  AbortSignal plays that role). Already-acquired locks are unaffected. */
  async lockInterruptibly(leaseTimeMs?: number, signal?: AbortSignal, threadId: number | string = 1): Promise<void> {
    if (signal?.aborted) throw abortError();
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    for (;;) {
      if (signal?.aborted) throw abortError();
      const ttl = await this.tryAcquire(lease, threadId);
      if (ttl === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return;
      }
      await waitForWakeAbortable(this.redis, this.channel, Math.min(Math.max(ttl, 1), 1000), signal);
    }
  }

  async unlock(threadId: number | string = 1): Promise<void> {
    const res = await luaEval(this.redis, LOCK_UNLOCK_SCRIPT, 2, this.name, this.channel, UNLOCK_MSG, String(this.watchdogMs), this.field(threadId));
    if (res === null || res === undefined || toNumber(res) < 0) {
      throw new LockNotHeldError(`attempt to unlock lock, not locked by current thread by node id: ${this.clientId} thread-id: ${threadId}`);
    }
    if (toNumber(res) === 0) this.stopWatchdog(threadId);
  }

  async forceUnlock(): Promise<boolean> {
    const r = await luaEval(this.redis, LOCK_FORCE_UNLOCK_SCRIPT, 2, this.name, this.channel, UNLOCK_MSG);
    this.stopAllWatchdogs();
    return toNumber(r) === 1;
  }

  async isLocked(): Promise<boolean> {
    return this.isExists();
  }
  async isHeldByThread(threadId: number | string = 1): Promise<boolean> {
    return ((await this.redis.hexists(this.name, this.field(threadId))) as number) === 1;
  }
  async getHoldCount(threadId: number | string = 1): Promise<number> {
    const v = await this.redis.hget(this.name, this.field(threadId));
    return v === null ? 0 : Number(v);
  }
  /** Back-compat alias. */
  async isHeldByCurrentThread(threadId: number | string = 1): Promise<boolean> {
    return this.isHeldByThread(threadId);
  }
  /** Java 4.6.1 `newCondition()` is explicitly unimplemented. */
  newCondition(): never {
    throw new Error('UnsupportedOperationException');
  }
}

// ---------------------------------------------------------------------------
// RSpinLock — same wire, 10ms spin, no pub/sub
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// BackOff (mirrors org.redisson.api.LockOptions): retry sleep policies for
// RSpinLock. Defaults match Java: exponential, initial 1ms, max 128ms, ×2
// with jitter (nextSleep*multiplier + random(fails)).
// ---------------------------------------------------------------------------

export interface BackOffPolicy {
  next(): number;
}

export interface BackOff {
  create(): BackOffPolicy;
}

export function constantBackOff(delayMs = 64): BackOff {
  if (delayMs <= 0) throw new Error('delay should be positive');
  return { create: () => ({ next: () => delayMs }) };
}

export function exponentialBackOff(initialMs = 1, maxMs = 128, multiplier = 2): BackOff {
  if (initialMs <= 0 || maxMs <= 0) throw new Error('delays should be positive');
  if (multiplier <= 0) throw new Error('multiplier should be positive');
  return {
    // Mirrors ExponentialBackOffPolicy.getNextSleepPeriod exactly (sticky cap,
    // jitter added AFTER returning the current period).
    create: () => {
      let nextSleep = initialMs;
      let fails = 0;
      return {
        next: () => {
          if (nextSleep >= maxMs) return maxMs;
          const result = nextSleep;
          fails += 1;
          nextSleep = Math.min(maxMs, nextSleep * multiplier + Math.floor(Math.random() * fails));
          return Math.max(1, Math.floor(result));
        },
      };
    },
  };
}

export function defaultBackOff(): BackOff {
  return exponentialBackOff();
}

export class RSpinLock extends RLock {
  private backOff: BackOff;
  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000, backOff?: BackOff) {
    super(redis, name, clientId, watchdogMs);
    this.backOff = backOff ?? defaultBackOff();
  }
  override async lock(leaseTimeMs?: number, threadId: number | string = 1): Promise<void> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const policy = this.backOff.create();
    for (;;) {
      const ttl = await this.tryAcquire(lease, threadId);
      if (ttl === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return;
      }
      await sleep(policy.next());
    }
  }
  override async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const deadline = Date.now() + waitTimeMs;
    const policy = this.backOff.create();
    for (;;) {
      const ttl = await this.tryAcquire(lease, threadId);
      if (ttl === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return true;
      }
      if (Date.now() >= deadline) return false;
      await sleep(Math.min(policy.next(), Math.max(1, deadline - Date.now())));
    }
  }
}

// ---------------------------------------------------------------------------
// RNonReentrantLock — count fixed at 1, direct DEL on unlock
// ---------------------------------------------------------------------------

export class RNonReentrantLock extends RLock {
  override async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    // Non-reentrant: same holder re-entry is contention (Go parity) — caller
    // may opt into throwing via tryLockStrict.
    return super.tryLock(waitTimeMs, leaseTimeMs, threadId);
  }
  async tryLockStrict(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const held = await this.isHeldByThread(threadId);
    if (held) throw new NonReentrantError();
    return super.tryLock(waitTimeMs, leaseTimeMs, threadId);
  }
  protected override async tryAcquire(leaseMs: number, threadId: number | string): Promise<number | null> {
    const r = await luaEval(this.redis, NON_REENTRANT_ACQUIRE, 1, this.name, String(leaseMs), this.field(threadId));
    return r === null || r === undefined ? null : Number(r);
  }
  override async unlock(threadId: number | string = 1): Promise<void> {
    const res = await luaEval(this.redis, NON_REENTRANT_UNLOCK, 2, this.name, this.channel, UNLOCK_MSG, String(this.watchdogMs), this.field(threadId));
    if (res === null || res === undefined || toNumber(res) < 0) throw new LockNotHeldError();
    this.stopWatchdog(threadId);
  }
}

// ---------------------------------------------------------------------------
// RFairLock
// ---------------------------------------------------------------------------

const FAIR_WAIT_MS = 5 * 60 * 1000;
const NON_REENTRANT_MARKER = 'redi_non_reentrant';

export class RFairLock extends RedisDataStructure {
  protected channelPrefix: string;
  protected queueKey: string;
  protected timeoutKey: string;
  protected clientId: string;
  protected watchdogMs: number;
  private timers = new Map<string, any>();
  protected reentrant: boolean;

  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000, reentrant = true) {
    super(redis, name);
    this.channelPrefix = prefixName('redisson_lock__channel', name);
    this.queueKey = prefixName('redisson_lock_queue', name);
    this.timeoutKey = prefixName('redisson_lock_timeout', name);
    this.clientId = clientId;
    this.watchdogMs = watchdogMs;
    this.reentrant = reentrant;
  }

  protected field(threadId: number | string = 1): string {
    return threadField(this.clientId, threadId);
  }
  protected waitChannel(threadId: number | string): string {
    return `${this.channelPrefix}:${this.field(threadId)}`;
  }

  private async runTryOnly(leaseMs: number, threadId: number | string, nowMs: number): Promise<any> {
    return luaEval(this.redis, FAIR_TRY_SCRIPT, 3, this.name, this.queueKey, this.timeoutKey,
      String(leaseMs), this.field(threadId), String(nowMs), String(FAIR_WAIT_MS),
      this.reentrant ? '1' : '0', NON_REENTRANT_MARKER);
  }

  private async runAcquire(leaseMs: number, threadId: number | string, nowMs: number): Promise<any> {
    return luaEval(this.redis, FAIR_ACQUIRE_SCRIPT, 3, this.name, this.queueKey, this.timeoutKey,
      String(leaseMs), this.field(threadId), String(FAIR_WAIT_MS), String(nowMs),
      this.reentrant ? '1' : '0', NON_REENTRANT_MARKER);
  }

  private checkReentrant(res: any): void {
    if (typeof res === 'string' && res === NON_REENTRANT_MARKER) throw new NonReentrantError();
  }

  async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const now = await serverNowMs(this.redis);
    const first = await this.runTryOnly(lease, threadId, now);
    this.checkReentrant(first);
    if (first === null || first === undefined) {
      if (useWatchdog) this.startWatchdog(threadId);
      return true;
    }
    if (waitTimeMs <= 0) return false;
    return this.lockWait(waitTimeMs, lease, useWatchdog, threadId);
  }

  async lock(leaseTimeMs?: number, threadId: number | string = 1): Promise<void> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const now = await serverNowMs(this.redis);
    const first = await this.runAcquire(lease, threadId, now);
    this.checkReentrant(first);
    if (first === null || first === undefined) {
      if (useWatchdog) this.startWatchdog(threadId);
      return;
    }
    await this.lockWait(Infinity, lease, useWatchdog, threadId);
  }

  private async lockWait(waitMs: number, lease: number, useWatchdog: boolean, threadId: number | string): Promise<boolean> {
    const deadline = waitMs === Infinity ? Infinity : Date.now() + waitMs;
    for (;;) {
      const now = await serverNowMs(this.redis);
      const r = await this.runAcquire(lease, threadId, now);
      this.checkReentrant(r);
      if (r === null || r === undefined) {
        if (useWatchdog) this.startWatchdog(threadId);
        return true;
      }
      if (deadline !== Infinity && Date.now() >= deadline) {
        await this.cleanupWaiter(threadId);
        return false;
      }
      const ttl = toNumber(r, 100);
      const remain = deadline === Infinity ? 1000 : Math.min(Math.max(ttl, 1), deadline - Date.now(), 1000);
      if (remain <= 0) {
        await this.cleanupWaiter(threadId);
        return false;
      }
      await waitForWake(this.redis, this.waitChannel(threadId), Math.max(1, remain));
    }
  }

  private async cleanupWaiter(threadId: number | string): Promise<void> {
    try {
      await luaEval(this.redis, FAIR_FAIL_SCRIPT, 2, this.queueKey, this.timeoutKey, this.field(threadId), String(FAIR_WAIT_MS));
    } catch { /* noop */ }
  }

  async unlock(threadId: number | string = 1): Promise<void> {
    const now = await serverNowMs(this.redis);
    const r = await luaEval(this.redis, FAIR_UNLOCK_SCRIPT, 4, this.name, this.queueKey, this.timeoutKey, this.channelPrefix,
      UNLOCK_MSG, String(this.watchdogMs), this.field(threadId), String(now), 'publish');
    if (r === null || r === undefined) throw new LockNotHeldError();
    if (toNumber(r) === 1 || toNumber(r) === 0) this.stopWatchdog(threadId);
  }

  async forceUnlock(): Promise<boolean> {
    const now = await serverNowMs(this.redis);
    const r = await luaEval(this.redis, FAIR_FORCE_SCRIPT, 4, this.name, this.queueKey, this.timeoutKey, this.channelPrefix,
      UNLOCK_MSG, String(now), 'publish');
    this.stopAllWatchdogs();
    return toNumber(r) === 1;
  }

  async isLocked(): Promise<boolean> {
    return this.isExists();
  }

  // Java RedissonFairLock.deleteAsync() removes the lock HASH plus the FIFO
  // queue and timeout ZSET. Leaving those behind would make the next client
  // inherit stale waiters (and `trySet*` lifecycle checks fail).
  override async delete(): Promise<boolean> {
    this.stopAllWatchdogs();
    return this.deleteKeys([this.name, this.queueKey, this.timeoutKey]);
  }

  override async unlink(): Promise<boolean> {
    this.stopAllWatchdogs();
    return this.unlinkKeys([this.name, this.queueKey, this.timeoutKey]);
  }

  private startWatchdog(threadId: number | string): void {
    const f = this.field(threadId);
    this.stopWatchdog(threadId);
    const t = setInterval(() => {
      luaEval(this.redis, LOCK_RENEW_SCRIPT, 1, this.name, f, String(this.watchdogMs))
        .then((r) => {
          if (toNumber(r) === 0) this.stopWatchdog(threadId);
        })
        .catch(() => undefined);
    }, Math.max(1, Math.floor(this.watchdogMs / 3)));
    (t as any).unref?.();
    this.timers.set(f, t);
  }
  private stopWatchdog(threadId: number | string): void {
    const t = this.timers.get(this.field(threadId));
    if (t) clearInterval(t);
    this.timers.delete(this.field(threadId));
  }
  private stopAllWatchdogs(): void {
    for (const t of this.timers.values()) clearInterval(t);
    this.timers.clear();
  }
  /** Java 4.6.1 `newCondition()` is explicitly unimplemented. */
  newCondition(): never {
    throw new Error('UnsupportedOperationException');
  }
}

export class RNonReentrantFairLock extends RFairLock {
  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000) {
    super(redis, name, clientId, watchdogMs, false);
  }
}

// ---------------------------------------------------------------------------
// RReadWriteLock
// ---------------------------------------------------------------------------

export class RReadWriteLock extends RedisDataStructure {
  readonly readLock: RReadLockView;
  readonly writeLock: RWriteLockView;
  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000) {
    super(redis, name);
    const channel = prefixName('redisson_rwlock', name);
    this.readLock = new RReadLockView(redis, name, clientId, watchdogMs, channel);
    this.writeLock = new RWriteLockView(redis, name, clientId, watchdogMs, channel);
  }
  getReadLock(): RReadLockView {
    return this.readLock;
  }
  getWriteLock(): RWriteLockView {
    return this.writeLock;
  }
}

class RWBase extends RedisDataStructure {
  protected clientId: string;
  protected watchdogMs: number;
  protected channel: string;
  private timers = new Map<string, any>();
  constructor(redis: Redis, name: string, clientId: string, watchdogMs: number, channel: string) {
    super(redis, name);
    this.clientId = clientId;
    this.watchdogMs = watchdogMs;
    this.channel = channel;
  }
  protected primary(tid: number | string): string {
    return `${this.clientId}:${tid}`;
  }
  protected startWatchdog(tid: number | string): void {
    const f = this.primary(tid);
    this.stopWatchdog(tid);
    const t = setInterval(() => {
      this.renew(f)
        .then((r) => {
          if (toNumber(r) === 0) this.stopWatchdog(tid);
        })
        .catch(() => undefined);
    }, Math.max(1, Math.floor(this.watchdogMs / 3)));
    (t as any).unref?.();
    this.timers.set(f, t);
  }
  /** Renewal hook; read locks touch the :rwlock_timeout keys as well. */
  protected async renew(field: string): Promise<number> {
    return toNumber(await luaEval(this.redis, LOCK_RENEW_SCRIPT, 1, this.name, field, String(this.watchdogMs)));
  }
  protected stopWatchdog(tid: number | string): void {
    const t = this.timers.get(this.primary(tid));
    if (t) clearInterval(t);
    this.timers.delete(this.primary(tid));
  }
  protected stopAllWatchdogs(): void {
    for (const t of this.timers.values()) clearInterval(t);
    this.timers.clear();
  }
  protected async waitLoop(fn: () => Promise<number | null>, waitMs: number): Promise<boolean> {
    if (waitMs <= 0) {
      return (await fn()) === null;
    }
    const deadline = Date.now() + waitMs;
    for (;;) {
      const r = await fn();
      if (r === null) return true;
      const remain = deadline - Date.now();
      if (remain <= 0) return false;
      await waitForWake(this.redis, this.channel, Math.min(remain, 1000));
      if (Date.now() >= deadline) return false;
    }
  }
  /** Java 4.6.1 read/write lock `newCondition()` is unimplemented. */
  newCondition(): never {
    throw new Error('UnsupportedOperationException');
  }
}

export class RReadLockView extends RWBase {
  protected override async renew(field: string): Promise<number> {
    const tagged = this.name.includes('{') ? this.name : `{${this.name}}`;
    return toNumber(await luaEval(this.redis, RW_READ_RENEW, 2, this.name, tagged, field, String(this.watchdogMs)));
  }
  async lock(leaseTimeMs?: number, threadId: number | string = 1): Promise<void> {
    const ok = await this.tryLock(Infinity, leaseTimeMs, threadId);
    if (!ok) throw new Error('read lock acquisition failed');
  }
  async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const read = this.primary(threadId);
    const write = `${read}:write`;
    const timeoutPrefix = `${suffixName(this.name, read)}:rwlock_timeout`;
    const fn = async () => {
      const r = await luaEval(this.redis, RW_READ_ACQUIRE, 2, this.name, timeoutPrefix, read, write, String(lease));
      return r === null || r === undefined ? null : Number(r);
    };
    const first = await fn();
    if (first === null) {
      if (useWatchdog) this.startWatchdog(threadId);
      if (waitTimeMs === Infinity || waitTimeMs > 0) {
        // already acquired
      }
      return true;
    }
    if (waitTimeMs !== Infinity && waitTimeMs <= 0) return false;
    const ok = await this.waitLoop(fn, waitTimeMs === Infinity ? 24 * 365 * 3600 * 1000 : waitTimeMs);
    if (ok && useWatchdog) this.startWatchdog(threadId);
    return ok;
  }
  async unlock(threadId: number | string = 1): Promise<void> {
    const read = this.primary(threadId);
    const write = `${read}:write`;
    const tagged = this.name.includes('{') ? this.name : `{${this.name}}`;
    const timeoutPrefix = `${suffixName(this.name, read)}:rwlock_timeout`;
    // Java LockPubSub: read release publishes UNLOCK_MESSAGE (0, wakes one
    // waiter); write release publishes READ_UNLOCK_MESSAGE (1, wakes all).
    const r = await luaEval(this.redis, RW_READ_UNLOCK, 4, this.name, this.channel, timeoutPrefix, tagged, read, write, String(this.watchdogMs), '0');
    if (r === null || r === undefined || toNumber(r) < 0) throw new LockNotHeldError();
    if (toNumber(r) === 0) {
      const stillRead = toNumber(await this.redis.hexists(this.name, read)) === 1;
      const stillWrite = toNumber(await this.redis.hexists(this.name, write)) === 1;
      if (!stillRead && !stillWrite) this.stopWatchdog(threadId);
    }
  }

  // -- Java RedissonReadLock views (mode read | write + extra read fields) ----

  async isLocked(): Promise<boolean> {
    const script = `local mode = redis.call('hget', KEYS[1], 'mode')
if (mode == 'read') or (mode == 'read-write') or (mode == 'write' and redis.call('hlen', KEYS[1]) > 2) then
    return 1
end
return 0`;
    return toNumber(await luaEval(this.redis, script, 1, this.name)) === 1;
  }
  async isHeldByThread(threadId: number | string = 1): Promise<boolean> {
    return toNumber(await this.redis.hexists(this.name, this.primary(threadId))) === 1;
  }
  async getHoldCount(threadId: number | string = 1): Promise<number> {
    const v = await this.redis.hget(this.name, this.primary(threadId));
    return v === null ? 0 : Number(v);
  }
  async forceUnlock(): Promise<boolean> {
    const script = `if redis.call('hget', KEYS[1], 'mode') == 'read' then
    redis.call('del', KEYS[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;
    const ok = toNumber(await luaEval(this.redis, script, 2, this.name, this.channel, '0')) === 1;
    if (ok) this.stopAllWatchdogs();
    return ok;
  }
}

export class RWriteLockView extends RWBase {
  async lock(leaseTimeMs?: number, threadId: number | string = 1): Promise<void> {
    const ok = await this.tryLock(Infinity, leaseTimeMs, threadId);
    if (!ok) throw new Error('write lock acquisition failed');
  }
  async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId: number | string = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const write = `${this.primary(threadId)}:write`;
    const read = this.primary(threadId);
    const fn = async () => {
      const r = await luaEval(this.redis, RW_WRITE_ACQUIRE, 1, this.name, write, read, String(lease));
      return r === null || r === undefined ? null : Number(r);
    };
    const first = await fn();
    if (first === null) {
      if (useWatchdog) this.startWatchdog(threadId);
      return true;
    }
    if (waitTimeMs !== Infinity && waitTimeMs <= 0) return false;
    const ok = await this.waitLoop(fn, waitTimeMs === Infinity ? 24 * 365 * 3600 * 1000 : waitTimeMs);
    if (ok && useWatchdog) this.startWatchdog(threadId);
    return ok;
  }
  async unlock(threadId: number | string = 1): Promise<void> {
    const write = `${this.primary(threadId)}:write`;
    const read = this.primary(threadId);
    // Java write release publishes READ_UNLOCK_MESSAGE (1, wakes all waiters).
    const r = await luaEval(this.redis, RW_WRITE_UNLOCK, 2, this.name, this.channel, write, read, String(this.watchdogMs), '1');
    if (r === null || r === undefined || toNumber(r) < 0) throw new LockNotHeldError();
    if (toNumber(r) === 0) this.stopWatchdog(threadId);
  }

  // -- Java RedissonWriteLock views (mode write only; field has :write) -------

  async isLocked(): Promise<boolean> {
    const mode = await this.redis.hget(this.name, 'mode');
    return mode === 'write' || mode === 'read-write';
  }
  async isHeldByThread(threadId: number | string = 1): Promise<boolean> {
    return toNumber(await this.redis.hexists(this.name, `${this.primary(threadId)}:write`)) === 1;
  }
  async getHoldCount(threadId: number | string = 1): Promise<number> {
    const v = await this.redis.hget(this.name, `${this.primary(threadId)}:write`);
    return v === null ? 0 : Number(v);
  }
  async forceUnlock(): Promise<boolean> {
    const script = `if redis.call('hget', KEYS[1], 'mode') == 'write' then
    redis.call('del', KEYS[1])
    redis.call('publish', KEYS[2], ARGV[1])
    return 1
end
return 0`;
    const ok = toNumber(await luaEval(this.redis, script, 2, this.name, this.channel, '1')) === 1;
    if (ok) this.stopAllWatchdogs();
    return ok;
  }
}

// ---------------------------------------------------------------------------
// RSemaphore
// ---------------------------------------------------------------------------

export class RSemaphore extends RedisDataStructure {
  protected channel: string;
  constructor(redis: Redis, name: string) {
    super(redis, name);
    this.channel = prefixName('redisson_sc', name);
  }
  async trySetPermits(permits: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, SEM_TRYSET, 2, this.name, this.channel, String(permits))) === 1;
  }
  async trySetPermitsWithTTL(permits: number, ttlMs: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, SEM_TRYSET_TTL, 2, this.name, this.channel, String(permits), String(ttlMs))) === 1;
  }
  // NOTE: no setPermits here by design — Redisson has only trySetPermits /
  // addPermits. A bare SET would overwrite the counter without publish,
  // starving waiters and dirtying the total. Use trySetPermits/addPermits.
  async addPermits(delta: number): Promise<void> {
    await this.redis.incrby(this.name, delta);
  }
  async availablePermits(): Promise<number> {
    const v = await this.redis.get(this.name);
    return v === null ? 0 : Number(v);
  }
  async tryAcquire(permits = 1): Promise<boolean> {
    return toNumber(await luaEval(this.redis, SEM_ACQUIRE, 1, this.name, String(permits))) === 1;
  }
  async acquire(permits = 1): Promise<void> {
    for (;;) {
      if (await this.tryAcquire(permits)) return;
      await waitForWake(this.redis, this.channel, 1000);
    }
  }
  async tryAcquireWait(permits: number, waitMs: number): Promise<boolean> {
    const deadline = Date.now() + waitMs;
    for (;;) {
      if (await this.tryAcquire(permits)) return true;
      if (Date.now() >= deadline) return false;
      await waitForWake(this.redis, this.channel, Math.min(1000, Math.max(1, deadline - Date.now())));
    }
  }
  async release(permits = 1): Promise<number> {
    return toNumber(await luaEval(this.redis, SEM_RELEASE, 2, this.name, this.channel, String(permits)));
  }
  async drainPermits(): Promise<number> {
    return toNumber(await luaEval(this.redis, SEM_DRAIN, 1, this.name));
  }
}

// ---------------------------------------------------------------------------
// RCountDownLatch
// ---------------------------------------------------------------------------

export class RCountDownLatch extends RedisDataStructure {
  protected channel: string;
  constructor(redis: Redis, name: string) {
    super(redis, name);
    this.channel = `redisson_countdownlatch__channel__{${name}}`;
  }
  async trySetCount(count: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, CDL_SET, 2, this.name, this.channel, '1', String(count))) === 1;
  }
  async getCount(): Promise<number> {
    const v = await this.redis.get(this.name);
    if (v === null) return 0;
    return Math.max(0, Number(v));
  }
  async countDown(): Promise<number> {
    return toNumber(await luaEval(this.redis, CDL_COUNTDOWN, 2, this.name, this.channel, '0'));
  }
  async await(waitMs?: number): Promise<boolean> {
    if ((await this.getCount()) <= 0) return true;
    if (waitMs !== undefined && waitMs <= 0) return false;
    const deadline = waitMs === undefined ? Infinity : Date.now() + waitMs;
    for (;;) {
      if ((await this.getCount()) <= 0) return true;
      if (deadline !== Infinity && Date.now() >= deadline) return false;
      const remain = deadline === Infinity ? 200 : Math.min(200, deadline - Date.now());
      await waitForWake(this.redis, this.channel, Math.max(1, remain));
    }
  }
}

// ---------------------------------------------------------------------------
// RPermitExpirableSemaphore
// ---------------------------------------------------------------------------

export class RPermitExpirableSemaphore extends RedisDataStructure {
  protected timeoutKey: string;
  protected channel: string;
  constructor(redis: Redis, name: string) {
    super(redis, name);
    this.timeoutKey = suffixName(name, 'timeout');
    this.channel = prefixName('redisson_sc', name);
  }
  private async purge(): Promise<number> {
    const now = await serverNowMs(this.redis);
    return toNumber(await luaEval(this.redis, PES_PURGE, 2, this.name, this.timeoutKey, String(now)));
  }
  async trySetPermits(permits: number): Promise<boolean> {
    return toNumber(await luaEval(this.redis, SEM_TRYSET, 2, this.name, this.channel, String(permits))) === 1;
  }
  async setPermits(permits: number): Promise<void> {
    await this.purge();
    const cur = await this.redis.get(this.name);
    const acquired = await this.redis.zcard(this.timeoutKey);
    const total = (cur === null ? 0 : Number(cur)) + acquired;
    if (total !== permits) {
      await this.redis.incrby(this.name, permits - total);
      await this.redis.publish(this.channel, String(permits));
    }
  }
  async addPermits(delta: number): Promise<void> {
    const cur = await this.redis.get(this.name);
    const base = cur === null ? 0 : Number(cur);
    await this.redis.set(this.name, String(base + delta));
    if (delta > 0) await this.redis.publish(this.channel, String(delta));
  }
  async availablePermits(): Promise<number> {
    await this.purge();
    const v = await this.redis.get(this.name);
    return v === null ? 0 : Number(v);
  }
  async acquiredPermits(): Promise<number> {
    await this.purge();
    return this.redis.zcard(this.timeoutKey);
  }
  async tryAcquire(leaseMs: number, permits = 1): Promise<string[] | null> {
    const now = await serverNowMs(this.redis);
    // hex text ids (same domain as Java UUID strings): latin1 would corrupt
    // on the UTF-8 wire and break cross-language release/lookup by id.
    const ids: string[] = [];
    for (let i = 0; i < permits; i++) ids.push(randomBytes(16).toString('hex'));
    const r = await luaEval(this.redis, PES_ACQUIRE, 2, this.name, this.timeoutKey,
      String(permits), String(leaseMs), String(now), ...ids);
    if (toNumber(r) !== 1) return null;
    return ids;
  }
  async acquire(leaseMs: number, permits = 1): Promise<string[]> {
    for (;;) {
      const ids = await this.tryAcquire(leaseMs, permits);
      if (ids !== null) return ids;
      await waitForWake(this.redis, this.channel, 1000);
    }
  }
  async release(permitId: string): Promise<boolean> {
    return toNumber(await luaEval(this.redis, PES_RELEASE, 3, this.name, this.timeoutKey, this.channel, permitId)) === 1;
  }
  async releaseAll(permitIds: string[]): Promise<number> {
    if (permitIds.length === 0) return 0;
    const s = `local removed = 0
for i = 1, #ARGV do
    removed = removed + redis.call('zrem', KEYS[2], ARGV[i])
end
if removed > 0 then
    redis.call('incrby', KEYS[1], removed)
    redis.call('publish', KEYS[3], removed)
end
return removed`;
    return toNumber(await luaEval(this.redis, s, 3, this.name, this.timeoutKey, this.channel, ...permitIds));
  }
  async updateLeaseTime(permitId: string, leaseMs: number): Promise<boolean> {
    const score = await this.redis.zscore(this.timeoutKey, permitId);
    if (score === null) return false;
    const now = await serverNowMs(this.redis);
    await this.redis.zadd(this.timeoutKey, 'XX', String(now + leaseMs), permitId);
    return true;
  }
  async getLeaseTime(permitId: string): Promise<number> {
    const score = await this.redis.zscore(this.timeoutKey, permitId);
    if (score === null) return -1;
    const now = await serverNowMs(this.redis);
    return Math.max(0, Number(score) - now);
  }

  // Java deletes both the permit counter and the acquired-permit timeout ZSET.
  override async delete(): Promise<boolean> {
    return this.deleteKeys([this.name, this.timeoutKey]);
  }

  override async unlink(): Promise<boolean> {
    return this.unlinkKeys([this.name, this.timeoutKey]);
  }
}

// ---------------------------------------------------------------------------
// RFencedLock
// ---------------------------------------------------------------------------

export class RFencedLock extends RLock {
  private tokenKey: string;
  constructor(redis: Redis, name: string, clientId: string, watchdogMs = 30000) {
    super(redis, name, clientId, watchdogMs);
    this.tokenKey = prefixName('redisson_lock_token', name);
  }
  /** Acquire and return fencing token. Returns null when contended (use *_wait variants to block). */
  async lockAndGetToken(leaseTimeMs?: number, threadId: number | string = 1): Promise<number | null> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const r = await luaEval(this.redis, FENCED_ACQUIRE, 2, this.name, this.tokenKey, String(lease), this.field(threadId));
    if (Array.isArray(r) && toNumber(r[0]) === -1) {
      if (useWatchdog) this.startWatchdog(threadId);
      return toNumber(r[1]);
    }
    return null;
  }
  async lockWaitAndGetToken(waitMs: number, leaseTimeMs?: number, threadId: number | string = 1): Promise<number | null> {
    const first = await this.lockAndGetToken(leaseTimeMs, threadId);
    if (first !== null) return first;
    if (waitMs <= 0) return null;
    const deadline = Date.now() + waitMs;
    for (;;) {
      const remain = deadline - Date.now();
      if (remain <= 0) return null;
      await waitForWake(this.redis, this.channel, Math.min(remain, 1000));
      const t = await this.lockAndGetToken(leaseTimeMs, threadId);
      if (t !== null) return t;
    }
  }
  /** Java `RFencedLock.tryLockAndGetToken(...)`: without `waitTimeMs` it is a
   *  single non-blocking attempt (null when contended); with it, waits up to
   *  `waitTimeMs`. Returns the fencing token, or null on timeout. */
  async tryLockAndGetToken(waitTimeMs?: number, leaseTimeMs?: number, threadId: number | string = 1): Promise<number | null> {
    if (waitTimeMs === undefined || waitTimeMs <= 0) return this.lockAndGetToken(leaseTimeMs, threadId);
    return this.lockWaitAndGetToken(waitTimeMs, leaseTimeMs, threadId);
  }
  async getToken(): Promise<number> {
    const v = await this.redis.get(this.tokenKey);
    return v === null ? 0 : Number(v);
  }

  // Java deletes the RLock HASH plus the fencing-token counter. Without this
  // a recreated lock would continue from the previous token sequence.
  override async delete(): Promise<boolean> {
    this.stopAllWatchdogs();
    return this.deleteKeys([this.name, this.tokenKey]);
  }

  override async unlink(): Promise<boolean> {
    this.stopAllWatchdogs();
    return this.unlinkKeys([this.name, this.tokenKey]);
  }
}

// ---------------------------------------------------------------------------
// RMultiLock / RRedLock (client-side orchestration, no new keys)
// ---------------------------------------------------------------------------

/** Structural lock contract accepted by RMultiLock/RRedLock: RLock, RFairLock,
 *  fenced locks and the RReadWriteLock views all satisfy it. */
export interface LockLike {
  tryLock(waitTimeMs?: number, leaseTimeMs?: number, threadId?: number | string): Promise<boolean>;
  unlock(threadId?: number | string): Promise<void>;
}

export class RMultiLock {
  constructor(private locks: LockLike[]) {}
  async tryLock(threadId: number | string = 1, leaseMs = 30000): Promise<boolean> {
    const acquired: LockLike[] = [];
    for (const l of this.locks) {
      if (await l.tryLock(0, leaseMs, threadId)) acquired.push(l);
      else {
        for (const a of acquired) {
          try {
            await a.unlock(threadId);
          } catch { /* rollback best-effort */ }
        }
        return false;
      }
    }
    return true;
  }
  async lock(threadId: number | string = 1, leaseMs = 30000, waitMs = 60000): Promise<void> {
    const deadline = Date.now() + waitMs;
    for (;;) {
      if (await this.tryLock(threadId, leaseMs)) return;
      if (Date.now() >= deadline) throw new Error('redi: multilock acquisition failed');
      await sleep(50);
    }
  }
  async unlock(threadId: number | string = 1): Promise<void> {
    let first: unknown = null;
    for (const l of this.locks) {
      try {
        await l.unlock(threadId);
      } catch (e) {
        first ??= e;
      }
    }
    if (first) throw first;
  }
  /** Java 4.6.1 MultiLock `newCondition()` throws. */
  newCondition(): never {
    throw new Error('UnsupportedOperationException');
  }
}

export class RRedLock {
  constructor(private locks: LockLike[]) {}
  private quorum(): number {
    return Math.floor(this.locks.length / 2) + 1;
  }
  async tryLock(leaseMs = 30000, threadId: number | string = 1): Promise<boolean> {
    let n = 0;
    const acquired: LockLike[] = [];
    for (const l of this.locks) {
      try {
        if (await l.tryLock(0, leaseMs, threadId)) {
          n++;
          acquired.push(l);
        }
      } catch { /* count as failure */ }
    }
    if (n >= this.quorum()) return true;
    for (const a of acquired) {
      try {
        await a.unlock(threadId);
      } catch { /* noop */ }
    }
    return false;
  }
  async lock(waitMs = 60000, leaseMs = 30000, threadId: number | string = 1): Promise<void> {
    const deadline = Date.now() + waitMs;
    const attemptTTL = waitMs > 0 && leaseMs > 0 ? 2 * waitMs : leaseMs;
    for (;;) {
      let n = 0;
      const acquired: LockLike[] = [];
      for (const l of this.locks) {
        const remain = deadline - Date.now();
        if (remain <= 0) break;
        try {
          if (await l.tryLock(Math.max(1, Math.floor(remain / this.locks.length)), attemptTTL, threadId)) {
            n++;
            acquired.push(l);
          }
        } catch { /* noop */ }
      }
      if (n >= this.quorum()) {
        if (attemptTTL !== leaseMs) {
          for (const a of acquired) {
            try {
              await (a as any).redis.pexpire((a as any).name, leaseMs);
            } catch { /* noop */ }
          }
        }
        return;
      }
      for (const a of acquired) {
        try {
          await a.unlock(threadId);
        } catch { /* noop */ }
      }
      if (Date.now() >= deadline) throw new Error('redi: redlock acquisition failed');
      await sleep(50);
    }
  }
  async unlock(threadId: number | string = 1): Promise<void> {
    for (const l of this.locks) {
      try {
        await l.unlock(threadId);
      } catch {
        // ignore not-held (quorum rollback parity)
      }
    }
  }
  /** Java 4.6.1 RedLock `newCondition()` throws. */
  newCondition(): never {
    throw new Error('UnsupportedOperationException');
  }
}
