import { describe, it, expect } from 'vitest';
import RedisMock from 'ioredis-mock';
import { Redisson, StringCodec } from '../src';

const make = () => Redisson.create({ client: new RedisMock() as any });

describe('Redisson', () => {
  it('bucket', async () => {
    const r = make();
    const b = r.getBucket<{ a: number }>('b');
    await b.set({ a: 1 });
    expect(await b.get()).toEqual({ a: 1 });
    expect(await b.trySet({ a: 2 })).toBe(false);
    await r.shutdown();
  });
  it('string codec is raw', async () => {
    const r = make();
    await r.getBucket<string>('s', new StringCodec()).set('hi');
    expect(await r.redis.get('s')).toBe('hi');
  });
  it('atomic long', async () => {
    const r = make();
    const a = r.getAtomicLong('n');
    expect(await a.incrementAndGet()).toBe(1);
    expect(await a.addAndGet(5)).toBe(6);
    expect(await a.compareAndSet(6, 10)).toBe(true);
    expect(await a.get()).toBe(10);
  });
  it('map', async () => {
    const r = make();
    const m = r.getMap<string, number>('m');
    await m.put('x', 1);
    expect(await m.get('x')).toBe(1);
    expect(await m.size()).toBe(1);
    expect(await m.remove('x')).toBe(1);
  });
  it('lock uses redisson layout and is reentrant', async () => {
    const r = make();
    const l = r.getLock('lk');
    await l.lock(5000);
    expect(await r.redis.hget('lk', `${r.id}:1`)).toBe('1');
    expect(await l.tryLock(0, 5000)).toBe(true);
    expect(await r.redis.hget('lk', `${r.id}:1`)).toBe('2');
    await l.unlock();
    await l.unlock();
    expect(await l.isLocked()).toBe(false);
    await expect(l.unlock()).rejects.toThrow();
    await l.lock(5000, 1);
    expect(await l.tryLock(0, 5000, 2)).toBe(false);
    await l.unlock(1);
  });
});
