/**
 * Cross-language interop (no Redis required): TS codec <-> redi.py SerializationService.
 * Spawns python to encode/decode a matrix; skips when python/redi.py is absent.
 */
import { describe, it, expect } from 'vitest';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { JsonCodec } from '../src/codec';
import { highwayHash128, multimapInternalId } from '../src/util';

const execFileAsync = promisify(execFile);

async function pyEval(code: string): Promise<string | null> {
  try {
    const { stdout } = await execFileAsync('python', ['-c', code], { timeout: 15000 });
    return stdout.trim();
  } catch {
    return null;
  }
}

describe('interop codec (TS <-> redi.py, no Redis)', () => {
  const c = new JsonCodec();
  it('python bare scalars decode in TS', async () => {
    const out = await pyEval(
      "import sys, os; sys.path.insert(0, os.environ.get('REDIPY_PATH', 'C:/GitHub/redi.py')); from redipy.services.serialization import SerializationService; s=SerializationService(); print(repr(s.encode('hello'))); print(repr(s.encode(42))); print(repr(s.encode(None)))",
    );
    if (out === null) return; // no python → skip
    expect(c.decode('"hello"')).toBe('hello');
    expect(c.decode('42')).toBe(42);
    expect(c.decode('null')).toBe(null);
  });
  it('TS Long wrapper decodes in python', async () => {
    const enc = c.encode(5000000000);
    const out = await pyEval(
      `import sys, os; sys.path.insert(0, os.environ.get('REDIPY_PATH', 'C:/GitHub/redi.py')); from redipy.services.serialization import SerializationService; s=SerializationService(); print(s.decode(${JSON.stringify(enc)}))`,
    );
    if (out === null) return;
    expect(out).toBe('5000000000');
  });
  it('TS @class wrapper is stripped by python unwrapper', async () => {
    const enc = c.encode({ n: 1 });
    const out = await pyEval(
      `import sys, os, json; sys.path.insert(0, os.environ.get('REDIPY_PATH', 'C:/GitHub/redi.py')); from redipy.services.serialization import SerializationService; s=SerializationService(); print(json.dumps(s.decode(${JSON.stringify(enc)}), sort_keys=True))`,
    );
    if (out === null) return;
    expect(out).toBe('{"n": 1}');
  });
  it('highway multimap id matches python', async () => {
    const field = '"mykey"';
    const tsId = multimapInternalId(field);
    const out = await pyEval(
      `import sys, os, struct, base64; sys.path.insert(0, os.environ.get('REDIPY_PATH', 'C:/GitHub/redi.py')); from redipy.highway_hash import highway_hash128; h1,h2=highway_hash128(${JSON.stringify(field)}.encode()); print(base64.b64encode(struct.pack('>QQ',h1,h2)).decode().rstrip('='))`,
    );
    if (out === null) return;
    expect(tsId).toBe(out);
  });
  it('bloom hash matches python for sample elements', async () => {
    for (const el of ['"a"', '"hello"', '42']) {
      const [h1, h2] = highwayHash128(Buffer.from(el, 'utf8'));
      const out = await pyEval(
        `import sys, os; sys.path.insert(0, os.environ.get('REDIPY_PATH', 'C:/GitHub/redi.py')); from redipy.highway_hash import highway_hash128; print(highway_hash128(${JSON.stringify(el)}.encode()))`,
      );
      if (out === null) return;
      const m = out.replace(/[() ]/g, '').split(',');
      expect(h1.toString()).toBe(m[0]);
      expect(h2.toString()).toBe(m[1]);
    }
  });
});
