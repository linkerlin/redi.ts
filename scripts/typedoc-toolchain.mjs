/**
 * predocs bootstrap: TypeDoc (0.28.x) drives the classic TypeScript compiler
 * API, which the repo-wide `typescript@7` (native, Go) compiler no longer
 * ships. The lockfile-pinned alias dependency `typedoc-typescript`
 * (npm:typescript@5) provides a classic-API compiler; this script exposes it
 * to TypeDoc as a nested `node_modules/typedoc/node_modules/typescript`
 * junction, without touching the build toolchain at the root.
 *
 * npm cannot express that nesting in package.json (peers dedupe to the root,
 * and routine `npm install` prunes nested extras), so this script re-creates
 * the junction on demand — `npm run docs` self-heals after any reinstall.
 * Run from the `predocs` npm lifecycle hook.
 */
import { existsSync, mkdirSync, readFileSync, rmSync, symlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = path.join(root, 'node_modules', 'typedoc-typescript');
const nested = path.join(root, 'node_modules', 'typedoc', 'node_modules', 'typescript');

function hasClassicApi(dir) {
  try {
    const pkg = JSON.parse(readFileSync(path.join(dir, 'package.json'), 'utf8'));
    return Number(pkg.version.split('.')[0]) < 7 && existsSync(path.join(dir, 'lib', 'typescript.js'));
  } catch {
    return false;
  }
}

if (!hasClassicApi(source)) {
  console.error('predocs: node_modules/typedoc-typescript missing — run `npm install` first');
  process.exit(1);
}

if (hasClassicApi(nested)) {
  process.exit(0); // already provisioned
}

rmSync(nested, { recursive: true, force: true });
mkdirSync(path.dirname(nested), { recursive: true });
try {
  // 'junction' needs no privilege on Windows; ignored on POSIX.
  symlinkSync(path.relative(path.dirname(nested), source), nested, 'junction');
} catch (err) {
  console.error(`predocs: could not link ${nested} -> ${source}: ${err.message}`);
  process.exit(1);
}

if (!hasClassicApi(nested)) {
  console.error('predocs: junction created but the classic TS API is still unreachable');
  process.exit(1);
}
