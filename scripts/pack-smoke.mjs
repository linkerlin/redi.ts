/**
 * Acceptance smoke for phase 21 item ①: pack the tarball, install it into a
 * throwaway consumer, and load it BOTH ways — `require('redi.ts')` (CJS) and
 * `import 'redi.ts'` (ESM) — asserting the codec still emits the frozen Java
 * wire format from the packed artifact.
 *
 * Usage: npm run pack-smoke
 */
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const work = process.env.REDI_SMOKE_DIR ?? path.join(os.tmpdir(), 'redi-pack-smoke');
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';

const run = (cmd, args, cwd = root) => {
  // Windows: .cmd shims (npm) need a shell; real executables (node) must NOT
  // go through one — their paths contain spaces ("C:\Program Files\...").
  // Quoting the whole command string avoids DEP0190 (args + shell).
  const needsShell = process.platform === 'win32' && cmd.toLowerCase().endsWith('.cmd');
  const [file, argv] = needsShell
    ? [`"${cmd}" ${args.map((a) => (a.includes(' ') ? `"${a}"` : a)).join(' ')}`, []]
    : [cmd, args];
  const r = spawnSync(file, argv, { cwd, encoding: 'utf8', shell: needsShell });
  if (r.status !== 0) {
    throw new Error(`${cmd} ${args.join(' ')} failed (${r.status})\n${r.stdout}\n${r.stderr}`);
  }
  return r.stdout;
};

// expected wire strings (frozen against Java Redisson JsonJacksonCodec)
const WIRE_OBJ = '{"a":1,"@class":"java.util.LinkedHashMap"}';
const WIRE_LONG = '["java.lang.Long",9223372036854775807]';

const SMOKE_BODY = `
assert.strictEqual(typeof m.Redisson, 'function');
assert.strictEqual(typeof m.Redisson.create, 'function');
for (const name of ['RMap', 'RLock', 'RDelayedQueue', 'RSet', 'RScoredSortedSet', 'RBitSet', 'JsonCodec']) {
  assert.ok(m[name], 'missing export ' + name);
}
const c = new m.JsonCodec();
assert.strictEqual(c.encode({ a: 1 }), ${JSON.stringify(WIRE_OBJ)});
assert.strictEqual(c.encode(9223372036854775807n), ${JSON.stringify(WIRE_LONG)});
assert.deepStrictEqual(c.decode(${JSON.stringify(WIRE_OBJ)}), { a: 1 });
`;

const CJS_SMOKE = `
const m = require('redi.ts');
const assert = require('node:assert');
${SMOKE_BODY}
console.log('CJS OK ->', require.resolve('redi.ts'));
`;

const ESM_SMOKE = `
import * as m from 'redi.ts';
import assert from 'node:assert';
${SMOKE_BODY}
console.log('ESM OK ->', import.meta.resolve ? import.meta.resolve('redi.ts') : '(resolved)');
`;

rmSync(work, { recursive: true, force: true });
mkdirSync(work, { recursive: true });
writeFileSync(path.join(work, 'package.json'), JSON.stringify({ name: 'redi-pack-smoke', private: true }, null, 2) + '\n');
writeFileSync(path.join(work, 'smoke.cjs'), CJS_SMOKE);
writeFileSync(path.join(work, 'smoke.mjs'), ESM_SMOKE);

try {
  // build explicitly, pack without lifecycle noise (prepack stays wired for
  // real publishes; npm pack prints the tarball name as its last line)
  run(npm, ['run', 'build']);
  run(npm, ['pack', '--ignore-scripts', '--pack-destination', work]);
  const tgz = readdirSync(work)
    .filter((f) => f.endsWith('.tgz'))
    .map((f) => path.join(work, f))[0];
  if (!tgz) throw new Error('npm pack produced no tarball');
  console.log(`packed: ${path.basename(tgz)} (${statSync(tgz).size} bytes)`);

  run(npm, ['install', '--no-save', '--no-audit', '--no-fund', '--prefer-offline', tgz], work);

  // guard: the INSTALLED copy must carry both module formats + stubs
  const pkgDir = path.join(work, 'node_modules', 'redi.ts');
  for (const need of [
    'dist/cjs/package.json',
    'dist/esm/package.json',
    'dist/cjs/index.js',
    'dist/esm/index.js',
    'dist/cjs/index.d.ts',
    'dist/esm/index.d.ts',
    'package.json',
  ]) {
    if (!existsSync(path.join(pkgDir, need))) throw new Error(`installed package missing ${need}`);
  }

  console.log(run(process.execPath, ['smoke.cjs'], work).trim());
  console.log(run(process.execPath, ['smoke.mjs'], work).trim());
  console.log('PACK SMOKE OK');
} catch (e) {
  console.error(`PACK SMOKE FAILED (work dir kept: ${work})`);
  console.error(e.message ?? e);
  process.exitCode = 1;
} finally {
  if (!process.exitCode) rmSync(work, { recursive: true, force: true });
}
