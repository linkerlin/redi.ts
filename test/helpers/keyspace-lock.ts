/** Serialize test sections that mutate the server-global
 *  `notify-keyspace-events` config (vitest runs files in parallel). */
import type { Redis } from 'ioredis';
import { randomBytes } from 'node:crypto';

const LOCK_KEY = 'TsJavaT:test:keyspace-config-lock';
const LOCK_MS = 120000;

export async function acquireKeyspaceConfigLock(redis: Redis, timeoutMs = LOCK_MS): Promise<() => Promise<void>> {
  const token = randomBytes(8).toString('hex');
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await redis.set(LOCK_KEY, token, 'PX', LOCK_MS, 'NX');
    if (ok === 'OK') {
      return async () => {
        try {
          await redis.eval(
            `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
            1, LOCK_KEY, token,
          );
        } catch { /* noop */ }
      };
    }
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error('timeout waiting for keyspace config test lock');
}
