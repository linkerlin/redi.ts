import { describe, it, expect } from 'vitest';
import { JsonCodec, StringCodec, javaTypeWrap, unwrap } from '../src/codec';
import { prefixName, suffixName, highwayHash128, multimapInternalId, bloomBitIndex, packMapCacheValue, unpackMapCacheValue, packTimeSeriesMember, unpackTimeSeriesMember, unpackDelayedMember, encodeMapCacheEvent, decodeMapCacheEvent, keySynchronizerName, hash128toBase64 } from '../src/util';
import { ArrayGrepArgs, RCircularBuffer, buildGrepArgs, decodeArrayEntries, decodeArrayInfo, decodeArrayFullInfo } from '../src/arrays';

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

describe('JsonCodec BigInt (64-bit lossless)', () => {
  const c = new JsonCodec({ bigIntForUnsafeLong: true });

  it('encodes BigInt without Number() rounding', () => {
    expect(c.encode(9223372036854775807n)).toBe('["java.lang.Long",9223372036854775807]');
    expect(c.encode(-9223372036854775808n)).toBe('["java.lang.Long",-9223372036854775808]');
    expect(c.encode([1n, 9223372036854775807n])).toBe(
      '["java.util.ArrayList",[["java.lang.Long",1],["java.lang.Long",9223372036854775807]]]',
    );
    expect(c.encode({ id: 9223372036854775807n })).toContain('["java.lang.Long",9223372036854775807]');
  });

  it('decodes unsafe integer literals as BigInt', () => {
    expect(c.decode('9223372036854775807')).toBe(9223372036854775807n);
    expect(c.decode('["java.lang.Long",9223372036854775807]')).toBe(9223372036854775807n);
    expect(c.decode('{"id":9223372036854775807}')).toEqual({ id: 9223372036854775807n });
    expect(c.decode('9223372036854775807.5')).toBe(9223372036854775807.5);
    expect(c.decode('1e20')).toBe(1e20);
  });

  it('keeps safe integers, strings and exponents on the number contract', () => {
    expect(c.decode('5000000000')).toBe(5000000000);
    expect(c.decode('["java.lang.Long",5000000000]')).toBe(5000000000);
    expect(c.decode('"9223372036854775807"')).toBe('9223372036854775807');
    expect(new JsonCodec().decode('9223372036854775807')).toBe(9223372036854776000);
    expect(new JsonCodec().decode('9223372036854775807')).not.toBe(9223372036854775807n);
  });

  it('round-trips BigInt through nested structures', () => {
    const v = { id: 18446744073709551615n, list: [9223372036854775807n, 7] };
    expect(c.decode(c.encode(v))).toEqual(v);
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

describe('map cache events (u64LE length-prefixed segments)', () => {
  it('round-trips created/updated/removed payloads', () => {
    const msg = encodeMapCacheEvent(['"k"', '"v1"']);
    const b = Buffer.from(msg, 'latin1');
    expect(b.readBigUInt64LE(0)).toBe(3n);
    expect(decodeMapCacheEvent(b)).toEqual(['"k"', '"v1"']);
    expect(decodeMapCacheEvent(encodeMapCacheEvent(['"k"', '"v2"', '"v1"']))).toEqual(['"k"', '"v2"', '"v1"']);
    // JSON escapes stay inside the segment bytes
    expect(decodeMapCacheEvent(encodeMapCacheEvent(['"k"', '"a\\"b"']))).toEqual(['"k"', '"a\\"b"']);
  });
  it('drops a truncated tail instead of throwing', () => {
    const full = encodeMapCacheEvent(['"k"', '"v"']);
    const cut = Buffer.from(full, 'latin1').subarray(0, full.length - 2);
    expect(decodeMapCacheEvent(cut)).toEqual(['"k"']);
    expect(decodeMapCacheEvent('')).toEqual([]);
  });
});

describe('key synchronizer names (Java RMap/RSet getLock family)', () => {
  it('matches the Java 4.6.1 probe vector for k1', () => {
    // TsProbe: map-lock-name TsJavaT:p19:probe k1 (JsonJacksonCodec) →
    // {TsJavaT:p19:probe}:vBdUTRe0AVL6cuMJeT8lIw:{lock|fairlock|rw_lock|...}
    const h = hash128toBase64('"k1"');
    expect(h).toBe('vBdUTRe0AVL6cuMJeT8lIw');
    expect(keySynchronizerName('TsJavaT:p19:probe', '"k1"', 'lock'))
      .toBe(`{TsJavaT:p19:probe}:${h}:lock`);
    expect(keySynchronizerName('TsJavaT:p19:probe', '"k1"', 'rw_lock'))
      .toBe(`{TsJavaT:p19:probe}:${h}:rw_lock`);
    expect(keySynchronizerName('TsJavaT:p19:probe', '"k1"', 'permitexpirablesemaphore'))
      .toBe(`{TsJavaT:p19:probe}:${h}:permitexpirablesemaphore`);
  });
});

describe('array grep args + ARINFO/ARSCAN decoders (Java ArrayGrepParams shapes)', () => {
  const enc = (v: any) => JSON.stringify(v); // scalar-strategy stand-in for codec encode
  it('grep argv order: predicates, AND/OR, LIMIT, WITHVALUES, NOCASE', () => {
    const args = ArrayGrepArgs.exact('v1').withGlob('a*').withRegex('^b').or().limit(5).noCase();
    expect(buildGrepArgs('k', '-', '+', args, enc, false))
      .toEqual(['k', '-', '+', 'EXACT', '"v1"', 'GLOB', 'a*', 'RE', '^b', 'OR', 'LIMIT', 5, 'NOCASE']);
    expect(buildGrepArgs('k', 0, 9, ArrayGrepArgs.match('m').and(), enc, true))
      .toEqual(['k', 0, 9, 'MATCH', '"m"', 'AND', 'WITHVALUES']);
  });
  it('grep builders validate like Java', () => {
    expect(() => buildGrepArgs('k', '-', '+', new ArrayGrepArgs(), enc, false)).toThrow(/predicate/);
    expect(() => ArrayGrepArgs.exact('x').limit(0)).toThrow(/positive/);
    expect(() => ArrayGrepArgs.glob(null as any)).toThrow(/null/);
  });
  it('ArrayEntry decode: flat RESP2 pairs and nested form', () => {
    const jdec = (x: any) => (x === null ? null : JSON.parse(x as string));
    expect(decodeArrayEntries([0, '"a"', 2, '"b"'], jdec))
      .toEqual([{ index: 0, value: 'a' }, { index: 2, value: 'b' }]);
    expect(decodeArrayEntries([[0, '"a"'], [2, null]], jdec))
      .toEqual([{ index: 0, value: 'a' }, { index: 2, value: null }]);
    expect(decodeArrayEntries(null, jdec)).toEqual([]);
  });
  it('ArrayInfo / ArrayFullInfo decode from flat ARINFO replies', () => {
    const flat = ['count', 3, 'len', 11, 'next-insert-index', 2, 'slices', 1, 'directory-size', 8,
      'super-dir-entries', 2, 'slice-size', 64];
    expect(decodeArrayInfo(flat)).toEqual({ count: 3, length: 11, nextInsertIndex: 2, slices: 1,
      directorySize: 8, superDirectoryEntries: 2, sliceSize: 64 });
    const full = [...flat, 'dense-slices', 1, 'sparse-slices', 0, 'avg-dense-size', '42.5',
      'avg-dense-fill', '0.9', 'avg-sparse-size', '3.25'];
    const decoded = decodeArrayFullInfo(full);
    expect(decoded.count).toBe(3);
    expect(decoded.denseSlices).toBe(1);
    expect(decoded.sparseSlices).toBe(0);
    expect(decoded.averageDenseSize).toBeCloseTo(42.5, 10);
    expect(decoded.averageDenseFill).toBeCloseTo(0.9, 10);
    expect(decoded.averageSparseSize).toBeCloseTo(3.25, 10);
  });
  it('circular buffer settings key is hash-tagged prefixName', () => {
    const cb = new RCircularBuffer({} as any, 'cb');
    expect(cb.settingsName).toBe('redisson_acb:{cb}');
    const tagged = new RCircularBuffer({} as any, '{tenant}:cb');
    expect(tagged.settingsName).toBe('redisson_acb:{tenant}:cb');
  });
});
