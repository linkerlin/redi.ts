/**
 * Resident Java probe (TsProbe `repl` mode) for the live interop suites.
 *
 * One JVM per vitest worker instead of one `mvn exec:java` per call: the
 * probe client boots once, every command then round-trips over stdin/stdout
 * in a few milliseconds. Protocol (see interop .../TsRepl.java):
 *
 *   -> "<id> <command> <args...>"
 *   <- "<id> OUT <line>" ... "<id> DONE" | "<id> ERROR <msg>"
 *
 * Long/blocking commands (lock holds, listeners) run on their own worker
 * thread inside the JVM, so `probeBg` starts them without stalling other
 * probes; `await` the returned promise to collect their output lines.
 *
 * The classpath (interop/target/cp.txt) and classes are compiled on demand
 * via Maven once per worker; afterwards the helper spawns `java -cp ...`
 * directly, skipping Maven/JVM per call.
 */
import { spawn, execSync, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const INTEROP = path.join(ROOT, 'interop');
const CLASSES = path.join(INTEROP, 'target', 'classes');
const CP_FILE = path.join(INTEROP, 'target', 'cp.txt');
const MVN =
  process.env.MVN ?? (process.platform === 'win32' ? 'C:\\javas\\apache-maven-3.9.6-bin\\apache-maven-3.9.6\\bin\\mvn.cmd' : 'mvn');
const JAVA = process.env.JAVA ?? 'java';
const PROBE_CLASS_FILE = path.join(CLASSES, 'io', 'github', 'redits', 'interop', 'TsProbe.class');

const mtime = (p: string): number => {
  try {
    return statSync(p).mtimeMs;
  } catch {
    return 0;
  }
};

/** Ensures classes/classpath are up to date (once per worker). */
let ensured: Promise<void> | null = null;
function ensureBuilt(): Promise<void> {
  if (ensured) return ensured;
  ensured = (async () => {
    const srcMax = Math.max(
      mtime(path.join(INTEROP, 'pom.xml')),
      mtime(path.join(INTEROP, 'src', 'main', 'java', 'io', 'github', 'redits', 'interop', 'TsProbe.java')),
      mtime(path.join(INTEROP, 'src', 'main', 'java', 'io', 'github', 'redits', 'interop', 'TsRepl.java')),
      mtime(path.join(INTEROP, 'src', 'main', 'java', 'io', 'github', 'redits', 'interop', 'TsFixtures.java')),
    );
    const stale = !existsSync(PROBE_CLASS_FILE) || mtime(PROBE_CLASS_FILE) < srcMax || !existsSync(CP_FILE);
    if (!stale) return;
    // execSync (not execFileSync): Node >= 20.12 refuses to spawn .cmd/.bat
    // without a shell on Windows (EINVAL), and a shell command avoids the
    // DEP0190 args+shell warning. Paths here contain no spaces.
    const cmd = `"${MVN}" -f "${path.join(INTEROP, 'pom.xml')}" -q compile dependency:build-classpath "-Dmdep.outputFile=target/cp.txt"`;
    execSync(cmd, {
      cwd: ROOT,
      stdio: 'pipe',
      timeout: 600000,
    });
  })();
  return ensured;
}

interface Pending {
  out: string[];
  resolve: (lines: string[]) => void;
  reject: (e: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

let child: ChildProcessWithoutNullStreams | null = null;
let booting: Promise<void> | null = null;
let nextId = 1;
let stdoutBuf = '';
let stderrTail: string[] = [];
const pending = new Map<number, Pending>();

function redisEnv(): Record<string, string> {
  const url = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
  try {
    const u = new URL(url);
    return { REDIS_HOST: u.hostname, REDIS_PORT: u.port || '6379' };
  } catch {
    return {};
  }
}

function handleLine(line: string): void {
  const m = /^(\d+) (OUT|DONE|ERROR)(?: (.*))?$/.exec(line);
  if (!m) return; // raw noise (SLF4J, unknown threads) — ignore
  const id = Number(m[1]);
  const kind = m[2];
  const p = pending.get(id);
  if (!p) return;
  if (kind === 'OUT') {
    p.out.push(m[3] ?? '');
  } else if (kind === 'DONE') {
    pending.delete(id);
    if (p.timer) clearTimeout(p.timer);
    p.resolve(p.out);
  } else {
    pending.delete(id);
    if (p.timer) clearTimeout(p.timer);
    p.reject(new Error(`java probe "${id}" failed: ${m[3] ?? ''}`));
  }
}

async function boot(): Promise<void> {
  if (child && !child.killed) return;
  if (booting) return booting;
  booting = (async () => {
    await ensureBuilt();
    // path.delimiter: ';' on Windows, ':' on POSIX — cp.txt is generated with
    // the platform-native separator too, so this line is what broke CI's
    // linux runner (a hardcoded ';' made the whole classpath one bad entry).
    const cp = [CLASSES, existsSync(CP_FILE) ? readFileSync(CP_FILE, 'utf8').trim() : '']
      .filter((x) => x.length > 0)
      .join(path.delimiter);
    const proc = spawn(JAVA, ['-Dorg.slf4j.simpleLogger.defaultLogLevel=warn', '-cp', cp, 'io.github.redits.interop.TsProbe', 'repl'], {
      cwd: ROOT,
      env: { ...process.env, ...redisEnv() },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    child = proc;
    stderrTail = [];
    stdoutBuf = '';
    let ready = false;
    let readyResolve: (() => void) | null = null;
    let readyReject: ((e: Error) => void) | null = null;
    const readyPromise = new Promise<void>((resolve, reject) => {
      readyResolve = resolve;
      readyReject = reject;
      setTimeout(() => reject(new Error(`java probe boot timed out; stderr:\n${stderrTail.join('')}`)), 120000).unref?.();
    });
    proc.stdout.on('data', (d: Buffer) => {
      stdoutBuf += d.toString();
      let idx: number;
      while ((idx = stdoutBuf.indexOf('\n')) >= 0) {
        const line = stdoutBuf.slice(0, idx).replace(/\r$/, '');
        stdoutBuf = stdoutBuf.slice(idx + 1);
        if (line === 'REPL_READY') {
          ready = true;
          readyResolve?.();
          continue;
        }
        if (ready) handleLine(line);
      }
    });
    proc.stderr.on('data', (d: Buffer) => {
      stderrTail.push(d.toString());
      if (stderrTail.length > 40) stderrTail.shift();
    });
    proc.on('exit', () => {
      for (const [id, p] of pending) {
        if (p.timer) clearTimeout(p.timer);
        p.reject(new Error(`java probe exited (id ${id}); stderr tail:\n${stderrTail.join('')}`));
      }
      pending.clear();
      readyReject?.(new Error(`java probe failed to start; stderr:\n${stderrTail.join('')}`));
      if (child === proc) child = null;
      booting = null;
    });
    await readyPromise;
  })();
  try {
    await booting;
  } finally {
    booting = null;
  }
}

/** Sends a command, resolves with all of its stdout lines when it finishes. */
export async function probeLines(cmd: string, timeoutMs = 180000): Promise<string[]> {
  await boot();
  const id = nextId++;
  const p = child!;
  return await new Promise<string[]>((resolve, reject) => {
    pending.set(id, { out: [], resolve, reject });
    const entry = pending.get(id)!;
    entry.timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`java probe timeout after ${timeoutMs}ms: ${cmd}`));
    }, timeoutMs);
    p.stdin.write(`${id} ${cmd}\n`);
  });
}

/** One-shot command: resolves with its last stdout line ('' when silent). */
export async function probe(cmd: string, timeoutMs = 180000): Promise<string> {
  const lines = await probeLines(cmd, timeoutMs);
  return lines.length > 0 ? lines[lines.length - 1] : '';
}

/**
 * Starts a long/blocking command and resolves with its lines when it
 * finishes. `const hold = probeBg(...)` runs the command in the background —
 * the resident probe keeps answering other commands meanwhile.
 */
export function probeBg(cmd: string, timeoutMs = 240000): Promise<string[]> {
  return probeLines(cmd, timeoutMs);
}

/** Stops the resident probe (also happens on process exit). */
export function stopProbe(): void {
  try {
    child?.kill();
  } catch {
    /* already gone */
  }
  child = null;
}

process.once('exit', stopProbe);
