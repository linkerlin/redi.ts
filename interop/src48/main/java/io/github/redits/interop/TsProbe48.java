package io.github.redits.interop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.redisson.Redisson;
import org.redisson.api.RArray;
import org.redisson.api.RCountMin;
import org.redisson.api.RTimeSeries;
import org.redisson.api.RedissonClient;
import org.redisson.api.TimeSeriesEntry;
import org.redisson.api.countmin.CountMinInitArgs;
import org.redisson.api.ts.TimeSeriesAddArgs;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

/**
 * Redisson <b>4.8.0</b> resident probe for the 4.8-only surfaces. Same wire
 * protocol as TsProbe's repl mode but fully self-contained (every command is
 * synchronous request/response, so no worker-thread mux is needed):
 *
 * <pre>
 *   -&gt; "&lt;id&gt; &lt;command&gt; &lt;args...&gt;"
 *   &lt;- "&lt;id&gt; OUT &lt;line&gt;" ... "&lt;id&gt; DONE" | "&lt;id&gt; ERROR &lt;msg&gt;"
 * </pre>
 *
 * Commands (Java 4.8 shapes verified against the local 4.8.1-SNAPSHOT source,
 * compiled against the released 4.8.0 jar — AGENTS.md law 11):
 * <ul>
 *   <li>{@code ts48-add name ts value} — RTimeSeries.add(TimeSeriesAddArgs.entry(ts, value));
 *       writes the 4.8 flag-4 member shape</li>
 *   <li>{@code ts48-entryrange name from to} — entryRange, prints "ts=value" lines</li>
 *   <li>{@code arr48-set name index value} / {@code arr48-get name index} / {@code arr48-len name}</li>
 *   <li>{@code cms48-init name width depth} / {@code cms48-add name item increment} /
 *       {@code cms48-count name item}</li>
 * </ul>
 */
public final class TsProbe48 {

    public static void main(String[] args) throws Exception {
        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        String port = System.getenv().getOrDefault("REDIS_PORT", "6379");

        Config config = new Config();
        config.setCodec(new JsonJacksonCodec());
        config.useSingleServer().setAddress("redis://" + host + ":" + port);
        config.useSingleServer().setPingConnectionInterval(30000);

        RedissonClient redisson = Redisson.create(config);
        System.out.println("REPL_READY");
        System.out.flush();
        try {
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
                String[] p = trimmed.split(" ");
                int id;
                try {
                    id = Integer.parseInt(p[0]);
                } catch (NumberFormatException e) {
                    continue; // raw noise from other threads
                }
                try {
                    List<String> out = dispatch(redisson, p[1], Arrays.copyOfRange(p, 2, p.length));
                    for (String o : out) {
                        System.out.println(id + " OUT " + o);
                    }
                    System.out.println(id + " DONE");
                } catch (Exception e) {
                    String msg = e.getMessage();
                    if (msg == null) {
                        msg = e.getClass().getSimpleName();
                    }
                    System.out.println(id + " ERROR " + msg.replace('\n', ' '));
                }
                System.out.flush();
            }
        } finally {
            redisson.shutdown();
        }
    }

    private static List<String> dispatch(RedissonClient redisson, String cmd, String[] a) {
        List<String> out = new ArrayList<>();
        switch (cmd) {
            case "ts48-add" -> {
                RTimeSeries<Double, String> ts = redisson.getTimeSeries(a[0]);
                boolean added = ts.add(TimeSeriesAddArgs.entry(Long.parseLong(a[1]), Double.parseDouble(a[2])));
                out.add("added=" + added);
            }
            case "ts48-entryrange" -> {
                RTimeSeries<Double, String> ts = redisson.getTimeSeries(a[0]);
                for (TimeSeriesEntry<Double, String> e : ts.entryRange(Long.parseLong(a[1]), Long.parseLong(a[2]))) {
                    out.add(e.getTimestamp() + "=" + e.getValue());
                }
            }
            case "arr48-set" -> {
                RArray<String> arr = redisson.getArray(a[0]);
                out.add("set=" + arr.set(Long.parseLong(a[1]), a[2]));
            }
            case "arr48-get" -> {
                RArray<String> arr = redisson.getArray(a[0]);
                String v = arr.get(Long.parseLong(a[1]));
                out.add("value=" + v);
            }
            case "arr48-len" -> {
                out.add("len=" + redisson.getArray(a[0]).length());
            }
            case "cms48-init" -> {
                redisson.<String>getCountMin(a[0]).init(CountMinInitArgs.dimensions(Long.parseLong(a[1]), Long.parseLong(a[2])));
                out.add("init=ok");
            }
            case "cms48-add" -> {
                long c = redisson.<String>getCountMin(a[0]).add(a[1], Long.parseLong(a[2]));
                out.add("count=" + c);
            }
            case "cms48-count" -> {
                out.add("count=" + redisson.<String>getCountMin(a[0]).count(a[1]));
            }
            default -> throw new IllegalArgumentException("unknown command: " + cmd);
        }
        return out;
    }

    private TsProbe48() {
    }
}
