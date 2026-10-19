/**
 * Per-key synchronizers — Redisson `RMap`/`RSet`/`RMultimap` `getLock(key)`
 * family (plus `getFairLock` / `getReadWriteLock` / `getSemaphore` /
 * `getCountDownLatch` / `getPermitExpirableSemaphore`).
 *
 * Companion names are derived exactly like RedissonObject:
 *   suffixName(structureName, Hash.hash128toBase64(encode(key)) + ":" + suffix)
 * (Map → encodeMapKey == value encoder; Set → encode(value).)
 * The hash-tag wraps the structure name, so every companion key lands in the
 * same cluster slot as the structure itself.
 */
import { RedisDataStructure } from './base.js';
import { keySynchronizerName } from './util.js';
import {
  RLock,
  RFairLock,
  RReadWriteLock,
  RSemaphore,
  RCountDownLatch,
  RPermitExpirableSemaphore,
} from './locks.js';

export abstract class RedisKeyedStructure<K = any> extends RedisDataStructure {
  /** Companion name for a key synchronizer (see module docs). */
  protected synchronizerName(key: K, suffix: string): string {
    return keySynchronizerName(this.name, this.encodeKey(key), suffix);
  }

  /** Reentrant lock associated with a map key / set value. */
  getLock(key: K): RLock {
    return new RLock(this.redis, this.synchronizerName(key, 'lock'), this.ownerId(), this.ownerWatchdog());
  }

  /** Fair lock associated with a map key / set value. */
  getFairLock(key: K): RFairLock {
    return new RFairLock(this.redis, this.synchronizerName(key, 'fairlock'), this.ownerId(), this.ownerWatchdog(), true);
  }

  /** Read-write lock associated with a map key / set value. */
  getReadWriteLock(key: K): RReadWriteLock {
    return new RReadWriteLock(this.redis, this.synchronizerName(key, 'rw_lock'), this.ownerId(), this.ownerWatchdog());
  }

  /** Counting semaphore associated with a map key / set value. */
  getSemaphore(key: K): RSemaphore {
    return new RSemaphore(this.redis, this.synchronizerName(key, 'semaphore'));
  }

  /** Count-down latch associated with a map key / set value. */
  getCountDownLatch(key: K): RCountDownLatch {
    return new RCountDownLatch(this.redis, this.synchronizerName(key, 'countdownlatch'));
  }

  /** Permit-expirable semaphore associated with a map key / set value. */
  getPermitExpirableSemaphore(key: K): RPermitExpirableSemaphore {
    return new RPermitExpirableSemaphore(this.redis, this.synchronizerName(key, 'permitexpirablesemaphore'));
  }
}
