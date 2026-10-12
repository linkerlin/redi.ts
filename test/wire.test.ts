import { describe, it, expect } from 'vitest';
import { JsonCodec, StringCodec, javaTypeWrap, unwrap } from '../src/codec';
import { prefixName, suffixName, highwayHash128, multimapInternalId, bloomBitIndex, packMapCacheValue, unpackMapCacheValue, packTimeSeriesMember, unpackTimeSeriesMember, unpackDelayedMember } from '../src/util';

describe('codec (JsonJacksonCodec default typing)', () => {
  const c = new JsonCodec();
  it('scalars stay bare', () => {
    expect(c.encode('hello')).toBe('"hello"');
    expect(c.encode(42)).toBe('42');
    expect(c.encode(3.14)).toBe('3.14');
    expect(c.encode(true)).toBe('true');
    expect(c.encode(null)).toBe('null');
  });
  it('long wraps outside int32', () => {
    expect(c.encode(5000000000)).toBe('["java.lang.Long",5000000000]');
    expect(c.encode(-5000000000)).toBe('["java.lang.Long",-5000000000]');
    expect(c.encode(2147483647)).toBe('2147483647');
    expect(c.decode('["java.lang.Long",5000000000]')).toBe(5000000000);
  });
  it('maps get @class, arrays get ArrayList wrapper', () => {
    const enc = c.encode({ n: 1 });
    expect(enc).toContain('@class');
    expect(enc).toContain('java.util.LinkedHashMap');
    expect(c.decode(enc)).toEqual({ n: 1 });
    const a = c.encode([1, 'x']);
    expect(a).toContain('java.util.ArrayList');
    expect(c.decode(a)).toEqual([1, 'x']);
  });
  it('nested wrappers unwrap recursively', () => {
    expect(c.decode('["java.util.ArrayList",[1,2]]')).toEqual([1, 2]);
    expect(c.decode('{"@class":"com.foo.Bar","x":1}')).toEqual({ x: 1 });
    expect(c.decode('not json {{{')).toBe('not json {{{');
    expect(c.decode(null)).toBe(null);
    expect(c.decode('')).toBe(null);
  });
  it('string codec is raw', () => {
    const s = new StringCodec();
    expect(s.encode('hi')).toBe('hi');
    expect(s.decode('hi')).toBe('hi');
  });
  it('matches redi.py bare-scalar outputs', () => {
    // redi.py: {"n":1} bare, [1] bare — we decode both fine (superset compat)
    expect(c.decode('{"n": 1}')).toEqual({ n: 1 });
    expect(c.decode('[1]')).toEqual([1]);
  });
});

describe('names (cluster hash-tag)', () => {
  it('prefix/suffix', () => {
    expect(prefixName('redisson_lock__channel', 'mylock')).toBe('redisson_lock__channel:{mylock}');
    expect(prefixName('redisson_lock_queue', 'mylock')).toBe('redisson_lock_queue:{mylock}');
    expect(suffixName('mylock', 'timeout')).toBe('{mylock}:timeout');
    expect(suffixName('mylock', 'value')).toBe('{mylock}:value');
    // already-tagged names pass through
    expect(prefixName('p', '{a}b')).toBe('p:{a}b');
    expect(suffixName('{a}b', 's')).toBe('{a}b:s');
  });
});

describe('highwayhash-128 (vs redi.py vectors)', () => {
  it('hello + empty match python', () => {
    expect(highwayHash128(Buffer.from('hello')).map(String)).toEqual(['9210372222324082145', '8139643753120311257']);
    expect(highwayHash128(Buffer.alloc(0)).map(String)).toEqual(['13234629882879058795', '12886175836324478118']);
  });
  it('32/33B + keys match python', () => {
    expect(highwayHash128(Buffer.alloc(32, 'a')).map(String)).toEqual(['17388512156953207854', '13842850530778299461']);
    expect(highwayHash128(Buffer.alloc(33, 'x')).map(String)).toEqual(['8114271344423122041', '2643270870814505735']);
    expect(highwayHash128(Buffer.from('test-key-123')).map(String)).toEqual(['8692500668595188017', '7566105125251088020']);
  });
  it('multimap id is base64-raw BE (no padding)', () => {
    const id = multimapInternalId('"hello"');
    expect(id).not.toContain('=');
    expect(Buffer.from(id, 'base64').length).toBe(16);
    // stable
    expect(multimapInternalId('"hello"')).toBe(id);
  });
  it('bloom bit index is deterministic in-range', () => {
    const [h1, h2] = highwayHash128(Buffer.from('"x"', 'utf8'));
    const b = bloomBitIndex(h1, h2, 0, 7298n);
    expect(b >= 0n && b < 7298n).toBe(true);
  });
});

describe('struct packs (little-endian wire)', () => {
  it('mapcache round-trips', () => {
    const p = packMapCacheValue('"v"', 123.5);
    const u = unpackMapCacheValue(p);
    expect(u.ok).toBe(true);
    expect(u.codec).toBe('"v"');
    expect(u.maxIdle).toBeCloseTo(123.5);
    expect(unpackMapCacheValue('{"a":1}').ok).toBe(false);
  });
  it('timeseries round-trips (4.6.1 shape: n=2/3, unprefixed label)', () => {
    const m = packTimeSeriesMember('00000000000000000007', '"v"', '"lbl"');
    const mb = Buffer.from(m, 'latin1');
    expect(mb[0]).toBe(3); // labeled marker, no mark byte inside blob
    const u = unpackTimeSeriesMember(m)!;
    expect(u.val).toBe('"v"');
    expect(u.label).toBe('"lbl"');
    const m2 = packTimeSeriesMember('00000000000000000007', '"v"', '');
    expect(Buffer.from(m2, 'latin1')[0]).toBe(2);
    expect(unpackTimeSeriesMember(m2)!.label).toBe('');
    // newer n=4 shape still decodes (DECODE_LABEL branch)
    const m4 = '\x04\x14' + '12345678901234567890' + '\x03\x00\x00\x00\x00\x00\x00\x00"v"'
      + '\x06\x00\x00\x00\x00\x00\x00\x00\x03"lbl"';
    const u4 = unpackTimeSeriesMember(m4)!;
    expect(u4.val).toBe('"v"');
    expect(u4.label).toBe('"lbl"');
  });
  it('delayed member rejects garbage', () => {
    expect(unpackDelayedMember('')).toBe(null);
    expect(unpackDelayedMember('\x08abc')).toBe(null);
  });
});
