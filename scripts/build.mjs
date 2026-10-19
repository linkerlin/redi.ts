/**
 * Dual-format build: dist/cjs (CommonJS) + dist/esm (ESM).
 *
 * The package keeps no root "type" field; each output directory carries its
 * own package.json stub so Node resolves dist/esm/*.js as ESM and
 * dist/cjs/*.js as CommonJS. Sources use explicit `.js` import specifiers,
 * which both tsc passes resolve back to the .ts sources and Node resolves to
 * the emitted files at runtime.
 */
import { execFileSync } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tscBin = path.join(root, 'node_modules', 'typescript', 'bin', 'tsc');

rmSync(path.join(root, 'dist'), { recursive: true, force: true });

const tsc = (project) =>
  execFileSync(process.execPath, [tscBin, '-p', project], { stdio: 'inherit', cwd: root });

tsc('tsconfig.json'); // CommonJS -> dist/cjs
tsc('tsconfig.esm.json'); // ESM -> dist/esm

mkdirSync(path.join(root, 'dist', 'cjs'), { recursive: true });
mkdirSync(path.join(root, 'dist', 'esm'), { recursive: true });
writeFileSync(path.join(root, 'dist', 'cjs', 'package.json'), JSON.stringify({ type: 'commonjs' }, null, 2) + '\n');
writeFileSync(path.join(root, 'dist', 'esm', 'package.json'), JSON.stringify({ type: 'module' }, null, 2) + '\n');

console.log('build: dist/cjs (commonjs) + dist/esm (module) ready');
