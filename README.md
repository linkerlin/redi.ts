# redi.ts
A 1:1 Clone for using Redisson in TypeScript!

## Status

Early implementation on top of `ioredis`, using Redisson's Redis wire formats so data
is shared with Redisson (Java), redi.py and redi.go:

- `RBucket`, `RAtomicLong`, `RMap` (codecs: `JsonCodec`, `StringCodec`)
- `RLock` – reentrant lock using Redisson's Lua scripts, `<clientId>:<threadId>` hash fields,
  `redisson_lock__channel:{name}` and watchdog renewal.

```ts
import { Redisson } from 'redi.ts';
const redisson = Redisson.create({ address: 'redis://127.0.0.1:6379' });
const lock = redisson.getLock('myLock');
await lock.lock();
try { /* ... */ } finally { await lock.unlock(); }
await redisson.shutdown();
```

`npm test` runs the tests (against `ioredis-mock`). More Redisson objects are planned.
