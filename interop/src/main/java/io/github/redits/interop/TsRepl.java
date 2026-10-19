package io.github.redits.interop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.redisson.api.RedissonClient;

/**
 * Resident REPL for the live-interop suites: one RedissonClient for the whole
 * JVM, commands read from stdin, each command runs on its own worker thread so
 * blocking probe commands (locks held for seconds, event listeners) never
 * stall the pipe.
 *
 * Protocol (line oriented, ids chosen by the caller):
 * <pre>
 *   -&gt; "&lt;id&gt; &lt;command&gt; &lt;args...&gt;"
 *   &lt;- "&lt;id&gt; OUT &lt;line&gt;"      zero or more, the command's stdout lines
 *   &lt;- "&lt;id&gt; DONE" | "&lt;id&gt; ERROR &lt;message&gt;"
 * </pre>
 * The REPL prints {@code REPL_READY} once the client is up, answers
 * {@code PING} with {@code PONG}, and exits on {@code QUIT} or stdin EOF.
 * Lines printed by threads the REPL does not know (SLF4J noise before the
 * swap, Redisson internals) pass through without an id prefix.
 */
final class TsRepl {

    private static final Map<Thread, String> OWNER = new ConcurrentHashMap<>();
    private static volatile PrintStream REAL = System.out;
    private static final java.util.List<Thread> WORKERS = new java.util.concurrent.CopyOnWriteArrayList<>();

    private TsRepl() {
    }

    static void serve(RedissonClient redisson, TsFixtures fx, String ns) throws Exception {
        REAL = System.out;
        System.setOut(new MuxStream(REAL));
        REAL.println("REPL_READY");
        REAL.flush();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if ("QUIT".equals(trimmed)) {
                break;
            }
            if ("PING".equals(trimmed)) {
                REAL.println("PONG");
                REAL.flush();
                continue;
            }
            int sp = trimmed.indexOf(' ');
            String id = sp < 0 ? trimmed : trimmed.substring(0, sp);
            List<String> argv = splitArgs(sp < 0 ? "" : trimmed.substring(sp + 1));
            if (argv.isEmpty()) {
                REAL.println(id + " ERROR empty command");
                REAL.flush();
                continue;
            }
            String[] args = argv.toArray(new String[0]);
            Thread worker = new Thread(() -> {
                try {
                    TsProbe.run(args[0].toLowerCase(), args, ns, redisson, fx);
                    emit(id, "DONE");
                } catch (Throwable e) {
                    e.printStackTrace(System.err);
                    emit(id, "ERROR " + e);
                }
            }, "probe-" + id);
            OWNER.put(worker, id);
            WORKERS.add(worker);
            worker.start();
        }
        // EOF: let in-flight commands finish before the client shuts down,
        // mirroring the one-shot CLI semantics.
        for (Thread worker : WORKERS) {
            worker.join();
        }
    }

    private static void emit(String id, String text) {
        REAL.println(id + " " + text);
        REAL.flush();
    }

    /** Splits a command line on spaces, honouring "..." and '...' groups. */
    static List<String> splitArgs(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    cur.append(c);
                }
            } else if (c == '"' || c == '\'') {
                quote = c;
            } else if (c == ' ') {
                if (cur.length() > 0) {
                    out.add(cur.toString());
                    cur.setLength(0);
                }
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) {
            out.add(cur.toString());
        }
        return out;
    }

    /** Routes println calls to "&lt;owner-id&gt; OUT &lt;line&gt;" so commands
     *  running concurrently on their own worker threads stay separable. */
    static final class MuxStream extends PrintStream {
        MuxStream(PrintStream real) {
            super(real, true);
        }

        @Override
        public void println(String s) {
            String id = OWNER.get(Thread.currentThread());
            if (id == null) {
                super.println(s);
            } else {
                super.println(id + " OUT " + (s == null ? "null" : s));
            }
        }

        @Override
        public void println(Object o) {
            println(String.valueOf(o));
        }

        @Override
        public void println(long l) {
            println(String.valueOf(l));
        }

        @Override
        public void println(int i) {
            println(String.valueOf(i));
        }

        @Override
        public void println(boolean b) {
            println(String.valueOf(b));
        }

        @Override
        public void println() {
            println("");
        }
    }
}
