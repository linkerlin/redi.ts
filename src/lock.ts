import type { Redis } from 'ioredis';

const LOCK_SCRIPT =
  "if (redis.call('exists', KEYS[1]) == 0) then redis.call('hincrby', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return nil; end; " +
  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then redis.call('hincrby', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return nil; end; " +
  "return redis.call('pttl', KEYS[1]);";

const UNLOCK_SCRIPT =
  "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then return nil; end; " +
  "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
  "if (counter > 0) then redis.call('pexpire', KEYS[1], ARGV[2]); return 0; " +
  "else redis.call('del', KEYS[1]); redis.call(ARGV[4], KEYS[2], ARGV[1]); return 1; end; return nil;";

const RENEW_SCRIPT =
  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then redis.call('pexpire', KEYS[1], ARGV[1]); return 1; end; return 0;";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Reentrant lock using Redisson's wire format: a hash at `name` whose field is
 * `<clientId>:<threadId>`, so it interoperates with Redisson/redi.py/redi.go.
 */
export class RLock {
  private timers = new Map<string, NodeJS.Timeout>();

  constructor(
    private redis: Redis,
    readonly name: string,
    private clientId: string,
    private watchdogMs = 30000,
  ) {}

  private get channel(): string {
    return `redisson_lock__channel:{${this.name}}`;
  }
  private field(threadId: number): string {
    return `${this.clientId}:${threadId}`;
  }

  private async tryAcquire(leaseMs: number, threadId: number): Promise<number | null> {
    const ttl = (await this.redis.eval(LOCK_SCRIPT, 1, this.name, String(leaseMs), this.field(threadId))) as number | null;
    return ttl == null ? null : Number(ttl);
  }

  private startWatchdog(threadId: number): void {
    const f = this.field(threadId);
    this.timers.get(f) && clearInterval(this.timers.get(f));
    const t = setInterval(() => {
      this.redis
        .eval(RENEW_SCRIPT, 1, this.name, String(this.watchdogMs), f)
        .then((r) => r === 0 && this.stopWatchdog(threadId))
        .catch(() => undefined);
    }, this.watchdogMs / 3);
    t.unref();
    this.timers.set(f, t);
  }

  private stopWatchdog(threadId: number): void {
    const f = this.field(threadId);
    clearInterval(this.timers.get(f));
    this.timers.delete(f);
  }

  /** Acquire; leaseTime <= 0 / undefined enables the watchdog. */
  async lock(leaseTimeMs?: number, threadId = 1): Promise<void> {
    while (!(await this.tryLock(Infinity, leaseTimeMs, threadId))) {
      /* retry */
    }
  }

  async tryLock(waitTimeMs = 0, leaseTimeMs?: number, threadId = 1): Promise<boolean> {
    const useWatchdog = !leaseTimeMs || leaseTimeMs <= 0;
    const lease = useWatchdog ? this.watchdogMs : leaseTimeMs!;
    const deadline = Date.now() + waitTimeMs;
    for (;;) {
      const ttl = await this.tryAcquire(lease, threadId);
      if (ttl === null) {
        if (useWatchdog) this.startWatchdog(threadId);
        return true;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) return false;
      await sleep(Math.min(Math.max(ttl, 1), remaining, 100));
    }
  }

  async unlock(threadId = 1): Promise<void> {
    const res = await this.redis.eval(
      UNLOCK_SCRIPT, 2, this.name, this.channel, '0', String(this.watchdogMs), this.field(threadId), 'publish',
    );
    if (res == null) {
      throw new Error(
        `attempt to unlock lock, not locked by current thread by node id: ${this.clientId} thread-id: ${threadId}`,
      );
    }
    if (res === 1) this.stopWatchdog(threadId);
  }

  async isLocked(): Promise<boolean> {
    return (await this.redis.exists(this.name)) > 0;
  }
  async isHeldByThread(threadId = 1): Promise<boolean> {
    return (await this.redis.hexists(this.name, this.field(threadId))) === 1;
  }
  async forceUnlock(): Promise<boolean> {
    const r = await this.redis.eval(
      "if (redis.call('del', KEYS[1]) == 1) then redis.call('publish', KEYS[2], ARGV[1]); return 1 else return 0 end",
      2, this.name, this.channel, '0',
    );
    return r === 1;
  }
}
