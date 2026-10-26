package io.github.redits.interop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.redisson.Redisson;
import org.redisson.api.GcraConfig;
import org.redisson.api.GcraResult;
import org.redisson.api.RArray;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBloomFilterNative;
import org.redisson.api.RCircularBuffer;
import org.redisson.api.RCountMin;
import org.redisson.api.RCuckooFilter;
import org.redisson.api.RGcra;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RLock;
import org.redisson.api.RMaps;
import org.redisson.api.RQueue;
import org.redisson.api.RTopic;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RSearch;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSet;
import org.redisson.api.RRingBuffer;
import org.redisson.api.RSetCache;
import org.redisson.api.RTDigest;
import org.redisson.api.RTimeSeries;
import org.redisson.api.RTimeSeriesNative;
import org.redisson.api.RTopK;
import org.redisson.api.RedissonClient;
import org.redisson.api.TimeSeriesEntry;
import org.redisson.api.atomic.CompareAndDeleteArgs;
import org.redisson.api.atomic.LongIncrementArgs;
import org.redisson.api.countmin.CountMinInitArgs;
import org.redisson.api.queue.QueueMoveElementsArgs;
import org.redisson.api.search.aggregate.AggregationOptions;
import org.redisson.api.search.aggregate.AggregationResult;
import org.redisson.api.search.aggregate.GroupBy;
import org.redisson.api.search.aggregate.Reducer;
import org.redisson.api.search.index.FieldIndex;
import org.redisson.api.search.index.IndexOptions;
import org.redisson.api.search.query.Document;
import org.redisson.api.search.query.QueryOptions;
import org.redisson.api.search.query.SearchResult;
import org.redisson.api.ts.TimeSeriesAddArgs;
import org.redisson.api.tsnative.TSSample;
import org.redisson.client.codec.StringCodec;
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
 *   <li>{@code cf48-init name capacity} / {@code cf48-add name item} / {@code cf48-addnx name item} /
 *       {@code cf48-exists name item} / {@code cf48-count name item} / {@code cf48-remove name item}</li>
 *   <li>{@code topk48-init name k} / {@code topk48-add name item} / {@code topk48-incrby name item n} /
 *       {@code topk48-contains name item} / {@code topk48-count name item} / {@code topk48-list name} /
 *       {@code topk48-listwc name} (item=count lines)</li>
 *   <li>{@code td48-create name [compression]} / {@code td48-add name v...} /
 *       {@code td48-min name} / {@code td48-max name} / {@code td48-quantile name q} /
 *       {@code td48-rank name v} / {@code td48-cdf name v}</li>
 *   <li>{@code cb48-setcap name capacity} / {@code cb48-capacity name} / {@code cb48-add name value} /
 *       {@code cb48-size name} / {@code cb48-read name} / {@code cb48-last name count}</li>
 *   <li>{@code gcra48-setrate name burst rate periodSec} / {@code gcra48-config name} (burst=/rate=/periodSec= lines) /
 *       {@code gcra48-tryacquire name [tokens]} (single line, limited= maxTokens= available= retryAfter= fullBurst=)</li>
 *   <li>{@code tsn48-add name ts value} / {@code tsn48-range name from to} (ts=value lines) /
 *       {@code tsn48-size name} / {@code tsn48-firstts name} / {@code tsn48-lastts name}</li>
 *   <li>{@code maps48-set name k v [k2 v2 ...]} / {@code map48-get name k}</li>
 *   <li>{@code set48-countdiff name others...} / {@code set48-countunion name others...}</li>
 *   <li>{@code q48-move src dest count} (moved= lines) / {@code q48-read name} (value= lines)</li>
 *   <li>{@code atomic48-get name} / {@code atomic48-incrargs name by lbound|- ubound|- saturate0|1} /
 *       {@code atomic48-cadd name op threshold} (op: &lt; &lt;= &gt; &gt;= == ~=)</li>
 *   <li>{@code search48-create idx prefix} / {@code search48-drop idx} / {@code search48-dropdocs idx} /
 *       {@code search48-hset key f v ...} / {@code search48-count idx query...} (total= + sorted doc= lines)</li>
 *   <li>{@code atomicd48-get name} / {@code atomicd48-incrargs name by lbound|- ubound|- saturate0|1} /
 *       {@code atomicd48-cadd name op threshold}</li>
 *   <li>{@code pes48-tryset name permits} / {@code pes48-tryacquire name leaseMs} / {@code pes48-release name permitId} /
 *       {@code pes48-available name}</li>
 *   <li>{@code bfn48-init name errorRate capacity} / {@code bfn48-add name element} /
 *       {@code bfn48-exists name element} / {@code bfn48-count name}</li>
 *   <li>{@code rate48-setrate name rate intervalSec} / {@code rate48-try name} / {@code rate48-release name} /
 *       {@code rate48-available name}</li>
 *   <li>{@code q48-offer name value} (feeds TS-side BLMOVEM blocking consumers)</li>
 *   <li>{@code ts48-addlabel name ts value label} (old 3-arg add, flag-2/3 shape) /
 *       {@code ts48-rangebylabel name from to label} (ts=value lines)</li>
 *   <li>{@code json48-setdoc name json} / {@code json48-setpath name path json} /
 *       {@code json48-setnx name path json} / {@code json48-get name path} (JacksonCodec&lt;Map&gt;)</li>
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
                    // walk to the root cause — Redisson wraps server errors
                    // ("Unexpected exception while processing command") that alone say nothing
                    Throwable root = e;
                    while (root.getCause() != null) {
                        root = root.getCause();
                    }
                    String msg = root.getMessage();
                    if (msg == null) {
                        msg = root.getClass().getSimpleName();
                    }
                    System.out.println(id + " ERROR " + msg.replace('\n', ' '));
                }
                System.out.flush();
            }
        } finally {
            redisson.shutdown();
        }
    }

    // background fair-lock waiter + topic listener state (single dispatch
    // thread; the REPL blocks on bgwaitacquired/bgjoin only AFTER the wake
    // already happened, so these latches never deadlock the loop)
    private static CountDownLatch fair48Latch = new CountDownLatch(1);
    private static String fair48Acquired = null;
    private static CountDownLatch fair48Go = new CountDownLatch(1);
    private static CountDownLatch fair48Done = new CountDownLatch(1);
    private static String fair48Result = null;
    private static CountDownLatch topic48Latch = new CountDownLatch(1);
    private static String topic48Msg = null;
    private static CountDownLatch sharded48Latch = new CountDownLatch(1);
    private static String sharded48Msg = null;
    private static CountDownLatch bq48Latch = new CountDownLatch(1);
    private static String bq48Result = null;
    private static CountDownLatch latch48BgLatch = new CountDownLatch(1);
    private static String latch48BgResult = null;
    private static CountDownLatch sem48BgLatch = new CountDownLatch(1);
    private static String sem48BgResult = null;
    private static CountDownLatch pattern48Latch = new CountDownLatch(1);
    private static String pattern48Msg = null;
    private static CountDownLatch rt48Latch = new CountDownLatch(1);
    private static String rt48Msg = null;
    private static String rt48Id = null;
    private static CountDownLatch zs48BgLatch = new CountDownLatch(1);
    private static String zs48BgResult = null;
    private static CountDownLatch mlk48BgLatch = new CountDownLatch(1);
    private static String mlk48BgResult = null;
    private static CountDownLatch bd48BgLatch = new CountDownLatch(1);
    private static String bd48BgResult = null;
    private static CountDownLatch lk48BgLatch = new CountDownLatch(1);
    private static String lk48BgResult = null;
    private static CountDownLatch mce48Latch = new CountDownLatch(1);
    private static String mce48Result = null;
    private static int mce48Id = -1;
    private static final java.util.List<String> bqs48Seen = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private static int bqs48Id = -1;
    private static final java.util.List<String> bd48FirstSeen = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private static final java.util.List<String> bd48LastSeen = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private static int bd48FirstId = -1;
    private static int bd48LastId = -1;
    private static CountDownLatch mlk48BgRelease = new CountDownLatch(1);
    private static CountDownLatch mlk48BgDone = new CountDownLatch(1);

    /** MapCache entry listener implementing all four kinds (Java dispatches
     *  by instanceof on the sub-interfaces; a bare MapEntryListener throws
     *  "Wrong listener type"). */
    private static class MceAllListener implements
            org.redisson.api.map.event.EntryCreatedListener<String, String>,
            org.redisson.api.map.event.EntryUpdatedListener<String, String>,
            org.redisson.api.map.event.EntryRemovedListener<String, String>,
            org.redisson.api.map.event.EntryExpiredListener<String, String> {
        private void record(String kind, org.redisson.api.map.event.EntryEvent<String, String> e) {
            mce48Result = "kind=" + kind + " key=" + e.getKey() + " value=" + e.getValue();
            mce48Latch.countDown();
        }
        public void onCreated(org.redisson.api.map.event.EntryEvent<String, String> e) {
            record("ENTRY_CREATED", e);
        }
        public void onUpdated(org.redisson.api.map.event.EntryEvent<String, String> e) {
            record("ENTRY_UPDATED", e);
        }
        public void onRemoved(org.redisson.api.map.event.EntryEvent<String, String> e) {
            record("ENTRY_REMOVED", e);
        }
        public void onExpired(org.redisson.api.map.event.EntryEvent<String, String> e) {
            record("ENTRY_EXPIRED", e);
        }
    }

    private static List<String> dispatch(RedissonClient redisson, String cmd, String[] a) throws Exception {
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
            case "arr48-insert" -> {
                // arr48-insert <name> <value> — ARINSERT appends, returns the index
                long at = redisson.<String>getArray(a[0]).insert(a[1]);
                out.add("at=" + at);
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
            // --- CuckooFilter (CF.*) — items travel codec-encoded (encode(element)) ---
            case "cf48-init" -> {
                redisson.<String>getCuckooFilter(a[0]).init(Long.parseLong(a[1]));
                out.add("init=ok");
            }
            case "cf48-add" -> {
                out.add("added=" + redisson.<String>getCuckooFilter(a[0]).add(a[1]));
            }
            case "cf48-addnx" -> {
                out.add("added=" + redisson.<String>getCuckooFilter(a[0]).addIfAbsent(a[1]));
            }
            case "cf48-exists" -> {
                out.add("exists=" + redisson.<String>getCuckooFilter(a[0]).exists(a[1]));
            }
            case "cf48-count" -> {
                out.add("count=" + redisson.<String>getCuckooFilter(a[0]).count(a[1]));
            }
            case "cf48-remove" -> {
                out.add("removed=" + redisson.<String>getCuckooFilter(a[0]).remove(a[1]));
            }
            // --- TopK (TOPK.*) — items codec-encoded, returns codec-decoded ---
            case "topk48-init" -> {
                redisson.<String>getTopK(a[0]).init(Integer.parseInt(a[1]));
                out.add("init=ok");
            }
            case "topk48-add" -> {
                String evicted = redisson.<String>getTopK(a[0]).add(a[1]);
                out.add("evicted=" + evicted);
            }
            case "topk48-incrby" -> {
                String evicted = redisson.<String>getTopK(a[0]).incrementBy(a[1], Integer.parseInt(a[2]));
                out.add("evicted=" + evicted);
            }
            case "topk48-contains" -> {
                out.add("contains=" + redisson.<String>getTopK(a[0]).contains(a[1]));
            }
            case "topk48-count" -> {
                out.add("count=" + redisson.<String>getTopK(a[0]).count(a[1]));
            }
            case "topk48-list" -> {
                for (String item : redisson.<String>getTopK(a[0]).list()) {
                    out.add("item=" + item);
                }
            }
            case "topk48-listwc" -> {
                for (java.util.Map.Entry<String, Long> e : redisson.<String>getTopK(a[0]).listWithCount().entrySet()) {
                    out.add(e.getKey() + "=" + e.getValue());
                }
            }
            // --- TDigest (TDIGEST.*) — raw doubles, no codec ---
            case "td48-create" -> {
                if (a.length > 1) {
                    redisson.getTDigest(a[0]).create(Integer.parseInt(a[1]));
                } else {
                    redisson.getTDigest(a[0]).create();
                }
                out.add("create=ok");
            }
            case "td48-add" -> {
                RTDigest td = redisson.getTDigest(a[0]);
                double[] vals = new double[a.length - 1];
                for (int i = 1; i < a.length; i++) {
                    vals[i - 1] = Double.parseDouble(a[i]);
                }
                td.add(vals);
                out.add("added=" + vals.length);
            }
            case "td48-min" -> {
                out.add("min=" + redisson.getTDigest(a[0]).getMin());
            }
            case "td48-max" -> {
                out.add("max=" + redisson.getTDigest(a[0]).getMax());
            }
            case "td48-quantile" -> {
                out.add("quantile=" + redisson.getTDigest(a[0]).quantile(Double.parseDouble(a[1])).get(0));
            }
            case "td48-rank" -> {
                out.add("rank=" + redisson.getTDigest(a[0]).rank(Double.parseDouble(a[1])).get(0));
            }
            case "td48-cdf" -> {
                out.add("cdf=" + redisson.getTDigest(a[0]).cumulativeProbability(Double.parseDouble(a[1])).get(0));
            }
            // --- CircularBuffer (AR.* ring) — values codec-encoded ---
            case "cb48-setcap" -> {
                out.add("set=" + redisson.<String>getCircularBuffer(a[0]).trySetCapacity(Integer.parseInt(a[1])));
            }
            case "cb48-capacity" -> {
                out.add("capacity=" + redisson.<String>getCircularBuffer(a[0]).capacity());
            }
            case "cb48-add" -> {
                out.add("added=" + redisson.<String>getCircularBuffer(a[0]).add(a[1]));
            }
            case "cb48-size" -> {
                out.add("size=" + redisson.<String>getCircularBuffer(a[0]).size());
            }
            case "cb48-read" -> {
                for (String v : redisson.<String>getCircularBuffer(a[0]).readAll()) {
                    out.add("value=" + v);
                }
            }
            case "cb48-last" -> {
                for (String v : redisson.<String>getCircularBuffer(a[0]).lastItems(Integer.parseInt(a[1]), false)) {
                    out.add("value=" + v);
                }
            }
            // --- RGcra: {name}:config hash + GCRA command inside Java's Lua ---
            case "gcra48-setrate" -> {
                boolean ok = redisson.getGcra(a[0]).trySetRate(
                        Long.parseLong(a[1]), Long.parseLong(a[2]), java.time.Duration.ofSeconds(Long.parseLong(a[3])));
                out.add("set=" + ok);
            }
            case "gcra48-config" -> {
                GcraConfig c = redisson.getGcra(a[0]).getConfig();
                if (c == null) {
                    out.add("config=null");
                } else {
                    out.add("burst=" + c.getMaxBurst());
                    out.add("rate=" + c.getTokensPerPeriod());
                    out.add("periodSec=" + c.getPeriod().getSeconds());
                }
            }
            case "gcra48-tryacquire" -> {
                RGcra g = redisson.getGcra(a[0]);
                GcraResult r = a.length > 2 ? g.tryAcquire(Long.parseLong(a[2])) : g.tryAcquire();
                out.add("limited=" + r.isLimited() + " maxTokens=" + r.getMaxTokens()
                        + " available=" + r.getAvailableTokens()
                        + " retryAfter=" + r.getRetryAfterSeconds()
                        + " fullBurst=" + r.getFullBurstAfterSeconds());
            }
            // --- RTimeSeriesNative (TS.* module, raw doubles — no codec) ---
            case "tsn48-add" -> {
                long ts = redisson.getTimeSeriesNative(a[0]).add(Long.parseLong(a[1]), Double.parseDouble(a[2]));
                out.add("ts=" + ts);
            }
            case "tsn48-range" -> {
                RTimeSeriesNative tsn = redisson.getTimeSeriesNative(a[0]);
                for (TSSample s : tsn.range(Long.parseLong(a[1]), Long.parseLong(a[2]))) {
                    out.add(s.getTimestamp() + "=" + s.getValue());
                }
            }
            case "tsn48-size" -> {
                out.add("size=" + redisson.getTimeSeriesNative(a[0]).size());
            }
            case "tsn48-firstts" -> {
                out.add("firstTs=" + redisson.getTimeSeriesNative(a[0]).firstTimestamp());
            }
            case "tsn48-lastts" -> {
                out.add("lastTs=" + redisson.getTimeSeriesNative(a[0]).lastTimestamp());
            }
            // --- RMaps mass set (codec HASHes) + plain map read-back ---
            case "maps48-set" -> {
                java.util.Map<String, String> entries = new java.util.LinkedHashMap<>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    entries.put(a[i], a[i + 1]);
                }
                java.util.Map<String, java.util.Map<String, String>> maps = new java.util.LinkedHashMap<>();
                maps.put(a[0], entries);
                redisson.<String, String>getMaps().set(maps);
                out.add("set=" + entries.size());
            }
            case "map48-get" -> {
                out.add("value=" + redisson.<String, String>getMap(a[0]).get(a[1]));
            }
            // --- RSet approximate cardinalities (SDIFFCARD/SUNIONCARD APPROX) ---
            case "set48-countdiff" -> {
                out.add("count=" + redisson.<String>getSet(a[0]).countDiff(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            case "set48-countunion" -> {
                out.add("count=" + redisson.<String>getSet(a[0]).countUnionApprox(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            // --- RQueue.move (LMOVEM) ---
            case "q48-move" -> {
                java.util.List<String> moved = redisson.<String>getQueue(a[0])
                        .move(QueueMoveElementsArgs.to(a[1]).count(Integer.parseInt(a[2])).bulk());
                for (String v : moved) {
                    out.add("moved=" + v);
                }
            }
            case "q48-read" -> {
                for (String v : redisson.<String>getQueue(a[0]).readAll()) {
                    out.add("value=" + v);
                }
            }
            case "q48-poll" -> {
                String v = redisson.<String>getQueue(a[0]).poll();
                out.add("value=" + v);
            }
            case "q48-expectpoll" -> {
                // q48-expectpoll <name> <expected> — poll and compare; ASCII-safe output
                String v = redisson.<String>getQueue(a[0]).poll();
                out.add("match=" + java.util.Objects.equals(v, a[1]));
            }
            // --- RBinaryStream: large-payload integrity stats ---
            case "bstream48-stat" -> {
                byte[] v = redisson.getBinaryStream(a[0]).get();
                if (v == null) {
                    out.add("len=0 sum=0");
                } else {
                    long sum = 0;
                    for (byte b : v) {
                        sum += (b & 0xffL);
                    }
                    out.add("len=" + v.length + " sum=" + sum);
                }
            }
            case "bstream48-gen" -> {
                // bstream48-gen <name> <size> <seed> — deterministic pattern bytes
                int size = Integer.parseInt(a[1]);
                int seed = Integer.parseInt(a[2]);
                byte[] data = new byte[size];
                for (int i = 0; i < size; i++) {
                    data[i] = (byte) ((i * 31 + seed) & 0xff);
                }
                redisson.getBinaryStream(a[0]).set(data);
                out.add("gen=" + size);
            }
            // --- RAtomicLong 4.8 args family (Java: native INCREX / CAD Lua) ---
            case "atomic48-get" -> {
                out.add("value=" + redisson.getAtomicLong(a[0]).get());
            }
            case "atomic48-setlong" -> {
                redisson.getAtomicLong(a[0]).set(Long.parseLong(a[1]));
                out.add("set=ok");
            }
            case "atomic48-incrargs" -> {
                // atomic48-incrargs <name> <by> <lbound|-> <ubound|-> <saturate 0|1> [pxMs]
                LongIncrementArgs args = LongIncrementArgs.by(Long.parseLong(a[1]));
                if (!"-".equals(a[2])) {
                    args = args.lowerBound(Long.parseLong(a[2]));
                }
                if (!"-".equals(a[3])) {
                    args = args.upperBound(Long.parseLong(a[3]));
                }
                if ("1".equals(a[4])) {
                    args = args.saturate();
                }
                if (a.length > 5) {
                    args = args.timeToLive(java.time.Duration.ofMillis(Long.parseLong(a[5])));
                }
                out.add("value=" + redisson.getAtomicLong(a[0]).incrementAndGet(args));
            }
            case "atomic48-cadd" -> {
                // atomic48-cadd <name> <op: < <= > >= == ~> <threshold> — raw Lua tokens
                CompareAndDeleteArgs cond;
                switch (a[1]) {
                    case "<" -> cond = CompareAndDeleteArgs.less(Long.parseLong(a[2]));
                    case "<=" -> cond = CompareAndDeleteArgs.lessOrEqual(Long.parseLong(a[2]));
                    case ">" -> cond = CompareAndDeleteArgs.greater(Long.parseLong(a[2]));
                    case ">=" -> cond = CompareAndDeleteArgs.greaterOrEqual(Long.parseLong(a[2]));
                    case "==" -> cond = CompareAndDeleteArgs.equal(Long.parseLong(a[2]));
                    case "~=" -> cond = CompareAndDeleteArgs.notEqual(Long.parseLong(a[2]));
                    default -> throw new IllegalArgumentException("bad op: " + a[1]);
                }
                out.add("deleted=" + redisson.getAtomicLong(a[0]).compareAndDelete(cond));
            }
            // --- RSearch (FT.*) — docs are plain HASH fields under a prefix ---
            case "search48-create" -> {
                RSearch s = redisson.getSearch();
                s.createIndex(a[0], IndexOptions.defaults().prefix(a[1]),
                        FieldIndex.text("title"), FieldIndex.numeric("price"), FieldIndex.tag("cat"));
                out.add("create=ok");
            }
            case "search48-drop" -> {
                redisson.getSearch().dropIndex(a[0]);
                out.add("drop=ok");
            }
            case "search48-dropdocs" -> {
                redisson.getSearch().dropIndexAndDocuments(a[0]);
                out.add("drop=ok");
            }
            case "search48-hset" -> {
                // search48-hset <key> <f> <v> [<f> <v> ...] — raw HASH doc (StringCodec)
                java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    m.put(a[i], a[i + 1]);
                }
                redisson.<String, String>getMap(a[0], StringCodec.INSTANCE).putAll(m);
                out.add("hset=" + m.size());
            }
            case "search48-count" -> {
                // everything after the index name (joined with spaces) is the query;
                // StringCodec: doc field values are plain HASH strings, not JSON
                String query = String.join(" ", Arrays.copyOfRange(a, 1, a.length));
                SearchResult res = redisson.getSearch(StringCodec.INSTANCE).search(
                        a[0], query, QueryOptions.defaults().limit(0, 10));
                out.add("total=" + res.getTotal());
                java.util.List<String> ids = new ArrayList<>();
                for (Document d : res.getDocuments()) {
                    ids.add("doc=" + d.getId());
                }
                java.util.Collections.sort(ids);
                out.addAll(ids);
            }
            // --- RAtomicDouble args family (native INCREX double / CAD Lua) ---
            case "atomicd48-get" -> {
                out.add("value=" + redisson.getAtomicDouble(a[0]).get());
            }
            case "atomicd48-incrargs" -> {
                // atomicd48-incrargs <name> <by> <lbound|-> <ubound|-> <saturate 0|1>
                org.redisson.api.atomic.DoubleIncrementArgs args =
                        org.redisson.api.atomic.DoubleIncrementArgs.by(Double.parseDouble(a[1]));
                if (!"-".equals(a[2])) {
                    args = args.lowerBound(Double.parseDouble(a[2]));
                }
                if (!"-".equals(a[3])) {
                    args = args.upperBound(Double.parseDouble(a[3]));
                }
                if ("1".equals(a[4])) {
                    args = args.saturate();
                }
                out.add("value=" + redisson.getAtomicDouble(a[0]).incrementAndGet(args));
            }
            case "atomicd48-cadd" -> {
                out.add("deleted=" + redisson.getAtomicDouble(a[0]).compareAndDelete(
                        caddArgs(a[1], Double.parseDouble(a[2]))));
            }
            // --- RPermitExpirableSemaphore: hex ids, raw-byte ZSET members ---
            case "pes48-tryset" -> {
                out.add("set=" + redisson.getPermitExpirableSemaphore(a[0]).trySetPermits(Integer.parseInt(a[1])));
            }
            case "pes48-tryacquire" -> {
                String id = redisson.getPermitExpirableSemaphore(a[0])
                        .tryAcquire(0, Long.parseLong(a[1]), java.util.concurrent.TimeUnit.MILLISECONDS);
                out.add("permit=" + id);
            }
            case "pes48-release" -> {
                // Java release(String) is void — availability is the observable
                redisson.getPermitExpirableSemaphore(a[0]).release(a[1]);
                out.add("released=ok");
            }
            case "pes48-available" -> {
                out.add("available=" + redisson.getPermitExpirableSemaphore(a[0]).availablePermits());
            }
            // --- RBloomFilterNative (BF.*, StringCodec — elements raw, no codec) ---
            case "bfn48-init" -> {
                redisson.<String>getBloomFilterNative(a[0]).init(Double.parseDouble(a[1]), Long.parseLong(a[2]));
                out.add("init=ok");
            }
            case "bfn48-add" -> {
                out.add("added=" + redisson.<String>getBloomFilterNative(a[0]).add(a[1]));
            }
            case "bfn48-exists" -> {
                out.add("exists=" + redisson.<String>getBloomFilterNative(a[0]).exists(a[1]));
            }
            case "bfn48-count" -> {
                out.add("count=" + redisson.<String>getBloomFilterNative(a[0]).count());
            }
            // --- RRateLimiter: cross-language permit recycling ---
            case "rate48-setrate" -> {
                boolean ok = redisson.getRateLimiter(a[0]).trySetRate(
                        RateType.OVERALL, Long.parseLong(a[1]),
                        java.time.Duration.ofSeconds(Long.parseLong(a[2])));
                out.add("set=" + ok);
            }
            case "rate48-try" -> {
                out.add("allowed=" + redisson.getRateLimiter(a[0]).tryAcquire());
            }
            case "rate48-release" -> {
                redisson.getRateLimiter(a[0]).release(1); // no no-arg overload in 4.8
                out.add("released=ok");
            }
            case "rate48-available" -> {
                out.add("available=" + redisson.getRateLimiter(a[0]).availablePermits());
            }
            // --- queue offer (feeding TS-side blocking consumers) ---
            case "q48-offer" -> {
                out.add("offered=" + redisson.<String>getQueue(a[0]).offer(a[1]));
            }
            // --- RTimeSeries label queries (ZSET TimeSeries, codec-encoded labels) ---
            case "ts48-addlabel" -> {
                // old 3-arg add — flag-2/3 member shape with an encoded label
                RTimeSeries<String, String> ts = redisson.getTimeSeries(a[0]);
                ts.add(Long.parseLong(a[1]), a[2], a[3]);
                out.add("added=ok");
            }
            case "ts48-rangebylabel" -> {
                RTimeSeries<String, String> ts = redisson.getTimeSeries(a[0]);
                for (TimeSeriesEntry<String, String> e
                        : ts.entryRangeByLabel(Long.parseLong(a[1]), Long.parseLong(a[2]), a[3])) {
                    out.add(e.getTimestamp() + "=" + e.getValue());
                }
            }
            // --- RJsonBucket deep paths (JacksonCodec<Map>) ---
            case "json48-setdoc" -> {
                org.redisson.codec.JacksonCodec<java.util.Map> jc =
                        new org.redisson.codec.JacksonCodec<>(java.util.Map.class);
                redisson.<java.util.Map>getJsonBucket(a[0], jc).set(MAPPER.readValue(a[1], java.util.Map.class));
                out.add("set=ok");
            }
            case "json48-setpath" -> {
                org.redisson.codec.JacksonCodec<java.util.Map> jc =
                        new org.redisson.codec.JacksonCodec<>(java.util.Map.class);
                redisson.<java.util.Map>getJsonBucket(a[0], jc).set(a[1], MAPPER.readValue(a[2], java.util.Map.class));
                out.add("set=ok");
            }
            case "json48-setnx" -> {
                org.redisson.codec.JacksonCodec<java.util.Map> jc =
                        new org.redisson.codec.JacksonCodec<>(java.util.Map.class);
                boolean ok = redisson.<java.util.Map>getJsonBucket(a[0], jc)
                        .setIfAbsent(a[1], MAPPER.readValue(a[2], java.util.Map.class));
                out.add("setnx=" + ok);
            }
            case "json48-get" -> {
                // JSON.GET with a path returns the RedisJSON match ARRAY ([2]),
                // so the decode side must be a List codec, not Map
                org.redisson.codec.JacksonCodec<java.util.List> jl =
                        new org.redisson.codec.JacksonCodec<>(java.util.List.class);
                Object v = redisson.<java.util.List>getJsonBucket(a[0], jl).get(jl, a[1]);
                out.add("json=" + MAPPER.writeValueAsString(v));
            }
            // --- FT.AGGREGATE: GROUPBY <field> REDUCE COUNT 0 over '*' ---
            case "search48-agg" -> {
                // fieldNames wants the property syntax (@cat) — add it when missing
                String field = a[1].startsWith("@") || a[1].startsWith("$") ? a[1] : "@" + a[1];
                AggregationResult res = redisson.getSearch(StringCodec.INSTANCE).aggregate(
                        a[0], "*", AggregationOptions.defaults()
                                .groupBy(GroupBy.fieldNames(field).reducers(Reducer.count())));
                out.add("total=" + res.getTotal());
                java.util.List<String> rows = new ArrayList<>();
                for (java.util.Map<String, Object> m : res.getAttributes()) {
                    java.util.List<String> es = new ArrayList<>();
                    for (java.util.Map.Entry<String, Object> e : m.entrySet()) {
                        es.add(e.getKey() + "=" + e.getValue());
                    }
                    java.util.Collections.sort(es);
                    rows.add("row=" + String.join(";", es));
                }
                java.util.Collections.sort(rows);
                out.addAll(rows);
            }
            // --- RLock cross-language force-unlock ---
            case "lock48-lock" -> {
                redisson.getLock(a[0]).lock(Long.parseLong(a[1]), java.util.concurrent.TimeUnit.MILLISECONDS);
                out.add("locked=true");
            }
            case "lock48-islocked" -> {
                out.add("locked=" + redisson.getLock(a[0]).isLocked());
            }
            case "lock48-forceunlock" -> {
                out.add("forced=" + redisson.getLock(a[0]).forceUnlock());
            }
            // --- RSemaphore: shared permits + cross-language drain ---
            case "sem48-tryset" -> {
                out.add("set=" + redisson.getSemaphore(a[0]).trySetPermits(Integer.parseInt(a[1])));
            }
            case "sem48-available" -> {
                out.add("available=" + redisson.getSemaphore(a[0]).availablePermits());
            }
            case "sem48-acquire" -> {
                out.add("acquired=" + redisson.getSemaphore(a[0]).tryAcquire());
            }
            case "sem48-drain" -> {
                out.add("drained=" + redisson.getSemaphore(a[0]).drainPermits());
            }
            case "sem48-release" -> {
                redisson.getSemaphore(a[0]).release();
                out.add("released=ok");
            }
            // --- RBloomFilter (hash/Lua-based, NOT the native BF.* one) ---
            case "bfl48-init" -> {
                out.add("init=" + redisson.<String>getBloomFilter(a[0])
                        .tryInit(Long.parseLong(a[1]), Double.parseDouble(a[2])));
            }
            case "bfl48-add" -> {
                out.add("added=" + redisson.<String>getBloomFilter(a[0]).add(a[1]));
            }
            case "bfl48-contains" -> {
                out.add("contains=" + redisson.<String>getBloomFilter(a[0]).contains(a[1]));
            }
            // --- RLexSortedSet ---
            case "lex48-add" -> {
                out.add("added=" + redisson.getLexSortedSet(a[0]).add(a[1]));
            }
            case "lex48-range" -> {
                // lex48-range <name> <min> <max> — inclusive bounds, e.g. [a [d
                for (String v : redisson.getLexSortedSet(a[0]).range(a[1], true, a[2], true)) {
                    out.add("v=" + v);
                }
            }
            case "lex48-contains" -> {
                out.add("contains=" + redisson.getLexSortedSet(a[0]).contains(a[1]));
            }
            // --- RCountDownLatch: cross-language countdown ---
            case "latch48-set" -> {
                out.add("set=" + redisson.getCountDownLatch(a[0]).trySetCount(Long.parseLong(a[1])));
            }
            case "latch48-count" -> {
                out.add("count=" + redisson.getCountDownLatch(a[0]).getCount());
            }
            case "latch48-countdown" -> {
                redisson.getCountDownLatch(a[0]).countDown();
                out.add("down=ok");
            }
            // --- RReadWriteLock: cross-language read-share / write-exclude ---
            case "rw48-readtry" -> {
                out.add("got=" + redisson.getReadWriteLock(a[0]).readLock()
                        .tryLock(0, java.util.concurrent.TimeUnit.MILLISECONDS));
            }
            case "rw48-writetry" -> {
                out.add("got=" + redisson.getReadWriteLock(a[0]).writeLock()
                        .tryLock(0, java.util.concurrent.TimeUnit.MILLISECONDS));
            }
            case "rw48-readunlock" -> {
                redisson.getReadWriteLock(a[0]).readLock().unlock();
                out.add("unlocked=ok");
            }
            case "rw48-writeunlock" -> {
                redisson.getReadWriteLock(a[0]).writeLock().unlock();
                out.add("unlocked=ok");
            }
            // --- RBucket object CAS (JacksonCodec<Map>) ---
            case "bkt48-set" -> {
                redisson.<java.util.Map>getBucket(a[0], new JsonJacksonCodec()).set(MAPPER.readValue(a[1], java.util.Map.class));
                out.add("set=ok");
            }
            case "bkt48-get" -> {
                Object v = redisson.<java.util.Map>getBucket(a[0], new JsonJacksonCodec()).get();
                out.add("json=" + MAPPER.writeValueAsString(v));
            }
            case "bkt48-cas" -> {
                boolean ok = redisson.<java.util.Map>getBucket(a[0], new JsonJacksonCodec())
                        .compareAndSet(MAPPER.readValue(a[1], java.util.Map.class),
                                       MAPPER.readValue(a[2], java.util.Map.class));
                out.add("cas=" + ok);
            }
            case "bkt48-expectfield" -> {
                // bkt48-expectfield <name> <field> <expected> — boolean match;
                // avoids echoing non-ASCII through stdout (charset-sensitive)
                java.util.Map<?, ?> v = redisson.<java.util.Map>getBucket(a[0], new JsonJacksonCodec()).get();
                Object f = v == null ? null : v.get(a[1]);
                out.add("match=" + String.valueOf(f).equals(a[2]));
            }
            case "bkt48-setlong" -> {
                // bare Long value (default typing wraps it on the wire)
                redisson.<Long>getBucket(a[0], new JsonJacksonCodec()).set(Long.parseLong(a[1]));
                out.add("set=ok");
            }
            // --- RTimeSeriesNatives multi-series MRANGE with FILTER ---
            case "tsn48-create" -> {
                // tsn48-create <name> <k=v> [k2=v2 ...]
                org.redisson.api.tsnative.TSCreateArgs args = org.redisson.api.tsnative.TSCreateArgs.defaults();
                for (int i = 1; i < a.length; i++) {
                    String[] kv = a[i].split("=", 2);
                    args = args.label(kv[0], kv[1]);
                }
                redisson.getTimeSeriesNative(a[0]).create(args);
                out.add("create=ok");
            }
            case "tsn48-mrange" -> {
                // tsn48-mrange <filter...> — every token is a filter (no name arg)
                java.util.Map<String, org.redisson.api.tsnative.TSSeriesSamples> m =
                        redisson.getTimeSeriesNatives().range(
                                org.redisson.api.tsnative.TSMultiRangeArgs.filter(a));
                java.util.List<String> lines = new ArrayList<>();
                for (java.util.Map.Entry<String, org.redisson.api.tsnative.TSSeriesSamples> e : m.entrySet()) {
                    for (TSSample s : e.getValue().getSamples()) {
                        lines.add("series=" + e.getKey() + " " + s.getTimestamp() + "=" + s.getValue());
                    }
                }
                java.util.Collections.sort(lines);
                out.addAll(lines);
            }
            // --- RVectorSet: VADD/VSIM/attributes ---
            case "vec48-add" -> {
                // vec48-add <name> <element> <v1,v2,...> [attrJson]
                String[] parts = a[2].split(",");
                java.lang.Double[] vec = new java.lang.Double[parts.length];
                for (int i = 0; i < parts.length; i++) {
                    vec[i] = java.lang.Double.parseDouble(parts[i]);
                }
                org.redisson.api.vector.VectorAddArgs va =
                        org.redisson.api.vector.VectorAddArgs.element(a[1]).vector(vec);
                if (a.length > 3) {
                    va = va.attributes(MAPPER.readValue(a[3], java.util.Map.class), jsonMapCodec());
                }
                out.add("added=" + redisson.getVectorSet(a[0]).add(va));
            }
            case "vec48-sim" -> {
                for (String s : redisson.getVectorSet(a[0]).getSimilar(
                        org.redisson.api.vector.VectorSimilarArgs.element(a[1])
                                .count(Integer.parseInt(a[2])))) {
                    out.add("sim=" + s);
                }
            }
            case "vec48-attr" -> {
                Object attr = redisson.getVectorSet(a[0]).getAttributes(a[1], java.util.Map.class);
                out.add("json=" + MAPPER.writeValueAsString(attr));
            }
            // --- FT.ALIASADD / FT.ALIASDEL ---
            case "search48-alias" -> {
                redisson.getSearch(StringCodec.INSTANCE).addAlias(a[0], a[1]);
                out.add("alias=ok");
            }
            case "search48-dropalias" -> {
                redisson.getSearch(StringCodec.INSTANCE).delAlias(a[0]);
                out.add("alias=dropped");
            }
            // --- RFairLock (RedissonFairLock): cross-language FIFO queue wake.
            // Queue member + HASH field + wake channel suffix are all
            // id:threadId on BOTH sides (RedissonBaseLock.getLockName).
            case "flock48-lock" -> {
                redisson.getFairLock(a[0]).lock();
                out.add("locked=ok");
            }
            case "flock48-unlock" -> {
                redisson.getFairLock(a[0]).unlock();
                out.add("unlocked=ok");
            }
            case "flock48-bglock" -> {
                // bg thread: tryLock(waitMs, leaseMs, MS), signal, hold until
                // bggo, then unlock — lets the TS holder's unlock() wake Java.
                long waitMs = Long.parseLong(a[1]);
                long leaseMs = Long.parseLong(a[2]);
                fair48Latch = new CountDownLatch(1);
                fair48Go = new CountDownLatch(1);
                fair48Done = new CountDownLatch(1);
                fair48Result = null;
                fair48Acquired = null;
                RLock fl = redisson.getFairLock(a[0]);
                Thread t = new Thread(() -> {
                    try {
                        boolean ok = fl.tryLock(waitMs, leaseMs, TimeUnit.MILLISECONDS);
                        // bgwaitacquired reads THIS (never mutated after signal);
                        // fair48Result later gains " released=1" for bgjoin.
                        fair48Acquired = "acquired=" + ok;
                        fair48Result = "acquired=" + ok;
                        fair48Latch.countDown();
                        if (ok) {
                            fair48Go.await(30, TimeUnit.SECONDS);
                            fl.unlock();
                            fair48Result += " released=1";
                        }
                    } catch (Exception e) {
                        Throwable root = e;
                        while (root.getCause() != null) {
                            root = root.getCause();
                        }
                        fair48Result = "error=" + root;
                    }
                    fair48Done.countDown();
                });
                t.setDaemon(true);
                t.start();
                out.add("started");
            }
            case "flock48-bgwaitacquired" -> {
                boolean r = fair48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? fair48Acquired : "timeout");
            }
            case "flock48-bggo" -> {
                fair48Go.countDown();
                out.add("go=ok");
            }
            case "flock48-bgjoin" -> {
                boolean r = fair48Done.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? fair48Result : "timeout");
            }
            // --- RTopic: cross-language pub/sub payload (channel = raw name,
            // no prefix — RedissonTopic wraps name in ChannelName directly).
            case "topic48-listen" -> {
                topic48Latch = new CountDownLatch(1);
                topic48Msg = null;
                RTopic topic = redisson.getTopic(a[0]);
                int lid = topic.addListener(String.class, (ch, msg) -> {
                    topic48Msg = msg;
                    topic48Latch.countDown();
                });
                out.add("listening=" + lid);
            }
            case "topic48-pub" -> {
                redisson.getTopic(a[0]).publish(a[1]);
                out.add("published=ok");
            }
            case "topic48-poll" -> {
                boolean r = topic48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? "msg=" + topic48Msg : "timeout");
            }
            // --- RMapCache: per-entry TTL, cross-language packed layout ---
            case "mc48-putttl" -> {
                redisson.getMapCache(a[0]).put(a[1], a[2], Long.parseLong(a[3]), TimeUnit.MILLISECONDS);
                out.add("put=ok");
            }
            case "mc48-remove" -> {
                Object prev = redisson.getMapCache(a[0]).remove(a[1]);
                out.add("removed=" + (prev != null));
            }
            case "mc48-get" -> {
                Object v = redisson.getMapCache(a[0]).get(a[1]);
                out.add(v == null ? "value=null" : "value=" + v);
            }
            // --- RScoredSortedSet (ZSET + codec members) ---
            case "zset48-add" -> {
                out.add("added=" + redisson.<String>getScoredSortedSet(a[0])
                        .add(Double.parseDouble(a[1]), a[2]));
            }
            case "zset48-addmap" -> {
                // zset48-addmap name score member [score member ...]
                java.util.Map<String, Double> m = new java.util.LinkedHashMap<>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    m.put(a[i + 1], Double.parseDouble(a[i]));
                }
                out.add("added=" + redisson.<String>getScoredSortedSet(a[0]).addAll(m));
            }
            case "zset48-readall" -> {
                java.util.List<String> vs = new ArrayList<>(redisson.<String>getScoredSortedSet(a[0]).readAll());
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("value=" + v);
                }
            }
            case "zset48-score" -> {
                Double s = redisson.<String>getScoredSortedSet(a[0]).getScore(a[1]);
                out.add("score=" + (s == null ? "null" : s));
            }
            case "zset48-firstlast" -> {
                RScoredSortedSet<String> z = redisson.getScoredSortedSet(a[0]);
                out.add("first=" + z.first() + " last=" + z.last());
            }
            case "zset48-rank" -> {
                Integer r = redisson.<String>getScoredSortedSet(a[0]).rank(a[1]);
                out.add("rank=" + (r == null ? "null" : r));
            }
            case "zset48-pollfirst" -> {
                out.add("polled=" + redisson.<String>getScoredSortedSet(a[0]).pollFirst());
            }
            case "zset48-addscore" -> {
                Double s = redisson.<String>getScoredSortedSet(a[0]).addScore(a[1], Double.parseDouble(a[2]));
                out.add("score=" + (s == null ? "null" : s));
            }
            case "zset48-count" -> {
                // zset48-count name min max — inclusive bounds
                out.add("count=" + redisson.<String>getScoredSortedSet(a[0])
                        .count(Double.parseDouble(a[1]), true, Double.parseDouble(a[2]), true));
            }
            // --- RRingBuffer (LIST + redisson_rb settings, head/tail trim) ---
            case "ring48-setcap" -> {
                out.add("set=" + redisson.<String>getRingBuffer(a[0]).trySetCapacity(Integer.parseInt(a[1])));
            }
            case "ring48-add" -> {
                redisson.<String>getRingBuffer(a[0]).add(a[1]);
                out.add("added=ok");
            }
            case "ring48-readnewest" -> {
                for (String v : redisson.<String>getRingBuffer(a[0]).readNewest(Integer.parseInt(a[1]))) {
                    out.add("value=" + v);
                }
            }
            case "ring48-peeklast" -> {
                out.add("last=" + redisson.<String>getRingBuffer(a[0]).peekLast());
            }
            case "ring48-stat" -> {
                RRingBuffer<String> rb = redisson.getRingBuffer(a[0]);
                out.add("size=" + rb.size() + " capacity=" + rb.capacity()
                        + " remaining=" + rb.remainingCapacity());
            }
            // --- RSetCache (ZSET expiry + idle ZSET). ttlMs 0 = no TTL:
            // Java scores it 92233720368547758L - now (readAll's ceiling is
            // the same constant — see the TS-side fix note in CHANGELOG).
            case "sc48-add" -> {
                out.add("added=" + redisson.<String>getSetCache(a[0])
                        .add(a[1], Long.parseLong(a[2]), TimeUnit.MILLISECONDS));
            }
            case "sc48-readall" -> {
                java.util.List<String> vs = new ArrayList<>(redisson.<String>getSetCache(a[0]).readAll());
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("value=" + v);
                }
            }
            case "sc48-contains" -> {
                out.add("contains=" + redisson.<String>getSetCache(a[0]).contains(a[1]));
            }
            case "sc48-size" -> {
                out.add("size=" + redisson.<String>getSetCache(a[0]).size());
            }
            // --- RStream: field names AND values codec-encoded ---
            case "stream48-add" -> {
                var f = new java.util.LinkedHashMap<String, String>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    f.put(a[i], a[i + 1]);
                }
                var id = redisson.<String, String>getStream(a[0])
                        .add(org.redisson.api.stream.StreamAddArgs.entries(f));
                out.add("id=" + id);
            }
            case "stream48-create-group" -> {
                redisson.<String, String>getStream(a[0]).createGroup(
                        org.redisson.api.stream.StreamCreateGroupArgs.name(a[1]).makeStream());
                out.add("group=ok");
            }
            case "stream48-read-group" -> {
                var r = redisson.<String, String>getStream(a[0]).readGroup(a[1], a[2],
                        org.redisson.api.stream.StreamReadGroupArgs
                                .greaterThan(org.redisson.api.stream.StreamMessageId.NEVER_DELIVERED)
                                .count(Integer.parseInt(a[3])));
                for (var e : r.entrySet()) {
                    out.add("id=" + e.getKey() + " fields=" + e.getValue());
                }
            }
            case "stream48-range" -> {
                var r = redisson.<String, String>getStream(a[0]).range(
                        org.redisson.api.stream.StreamMessageId.MIN, org.redisson.api.stream.StreamMessageId.MAX);
                for (var e : r.entrySet()) {
                    out.add("id=" + e.getKey() + " fields=" + e.getValue());
                }
            }
            case "stream48-pending" -> {
                var p = redisson.<String, String>getStream(a[0]).listPending(a[1],
                        org.redisson.api.stream.StreamMessageId.MIN,
                        org.redisson.api.stream.StreamMessageId.MAX, 100);
                for (var e : p) {
                    out.add("id=" + e.getId() + " consumer=" + e.getConsumerName());
                }
            }
            case "stream48-claim" -> {
                String[] parts = a[4].split("-");
                var m = redisson.<String, String>getStream(a[0]).claim(a[1], a[2], Long.parseLong(a[3]),
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                        new org.redisson.api.stream.StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1])));
                for (var e : m.entrySet()) {
                    out.add("id=" + e.getKey() + " fields=" + e.getValue());
                }
            }
            case "stream48-autoclaim" -> {
                var r = redisson.<String, String>getStream(a[0]).autoClaim(a[1], a[2], Long.parseLong(a[3]),
                        java.util.concurrent.TimeUnit.MILLISECONDS,
                        org.redisson.api.stream.StreamMessageId.MIN, 10);
                for (var e : r.getMessages().entrySet()) {
                    out.add("id=" + e.getKey() + " fields=" + e.getValue());
                }
            }
            case "stream48-ack" -> {
                String[] parts = a[2].split("-");
                long n = redisson.<String, String>getStream(a[0]).ack(a[1],
                        new org.redisson.api.stream.StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1])));
                out.add("acked=" + n);
            }
            case "stream48-size" -> {
                out.add("size=" + redisson.<String, String>getStream(a[0]).size());
            }
            case "stream48-groups" -> {
                var names = new ArrayList<String>();
                for (var g : redisson.<String, String>getStream(a[0]).listGroups()) {
                    names.add(g.getName());
                }
                java.util.Collections.sort(names);
                out.add("groups=" + names);
            }
            // --- RBitSet (raw SETBIT/GETBIT/BITOP domain) ---
            case "bits48-set" -> {
                redisson.getBitSet(a[0]).set(Long.parseLong(a[1]), Boolean.parseBoolean(a[2]));
                out.add("set=ok");
            }
            case "bits48-get" -> {
                out.add("bit=" + redisson.getBitSet(a[0]).get(Long.parseLong(a[1])));
            }
            case "bits48-card" -> {
                out.add("card=" + redisson.getBitSet(a[0]).cardinality());
            }
            case "bits48-or" -> {
                // bits48-or dest src... — BITOP OR into dest, returns byte length
                out.add("len=" + redisson.getBitSet(a[0]).or(Arrays.copyOfRange(a, 1, a.length)));
            }
            case "bits48-long-set" -> {
                out.add("old=" + redisson.getBitSet(a[0]).setLong(Long.parseLong(a[1]), Long.parseLong(a[2])));
            }
            case "bits48-long-get" -> {
                out.add("value=" + redisson.getBitSet(a[0]).getLong(Long.parseLong(a[1])));
            }
            // --- RHyperLogLog (PFADD/PFCOUNT on codec-encoded elements) ---
            case "hll48-add" -> {
                redisson.<Integer>getHyperLogLog(a[0]).add(Integer.parseInt(a[1]));
                out.add("added=ok");
            }
            case "hll48-count" -> {
                out.add("count=" + redisson.<Integer>getHyperLogLog(a[0]).count());
            }
            // --- RGeo (ZSET + geohash, codec members) ---
            case "geo48-add" -> {
                redisson.<String>getGeo(a[0]).add(Double.parseDouble(a[1]), Double.parseDouble(a[2]), a[3]);
                out.add("added=ok");
            }
            case "geo48-dist" -> {
                Double d = redisson.<String>getGeo(a[0]).dist(a[1], a[2], org.redisson.api.geo.GeoUnit.METERS);
                out.add("dist=" + (d == null ? "null" : d.longValue()));
            }
            case "geo48-search" -> {
                // geo48-search name lon lat radiusMeters — sorted member lines
                var r = redisson.<String>getGeo(a[0]).search(
                        org.redisson.api.geo.GeoSearchArgs.from(Double.parseDouble(a[1]), Double.parseDouble(a[2]))
                                .radius(Double.parseDouble(a[3]), org.redisson.api.geo.GeoUnit.METERS));
                java.util.Collections.sort(r);
                for (String m : r) {
                    out.add("member=" + m);
                }
            }
            case "geo48-pos" -> {
                var p = redisson.<String>getGeo(a[0]).pos(a[1]);
                out.add("pos=" + (p.get(0) == null ? "null"
                        : Math.round(p.get(0).getLongitude() * 1000) / 1000.0 + ","
                          + Math.round(p.get(0).getLatitude() * 1000) / 1000.0));
            }
            // --- RIdGenerator (counter + allocation cache; each getIdGenerator()
            // call is a fresh instance claiming a whole [old, old+alloc) block) ---
            case "idgen48-init" -> {
                out.add("init=" + redisson.getIdGenerator(a[0]).tryInit(Long.parseLong(a[1]), Long.parseLong(a[2])));
            }
            case "idgen48-next" -> {
                out.add("next=" + redisson.getIdGenerator(a[0]).nextId());
            }
            // --- RLongAdder: cross-language flush via the adder-topic. sum()
            // reads ALL {name}:*:counter keys (Lua), each client's subscriber
            // merges the others' published deltas into its own counter.
            case "adder48-add" -> {
                redisson.getLongAdder(a[0]).add(Long.parseLong(a[1]));
                out.add("added=ok");
            }
            case "adder48-sum" -> {
                out.add("sum=" + redisson.getLongAdder(a[0]).sum());
            }
            case "adder48-cohort" -> {
                // adder48-cohort name delta signal — add, mark ready, wait for
                // the signal key (TS added+summed), then sum() (same JVM keeps
                // its local buffer + subscriber alive across both sums)
                String signal = a[2];
                String readyKey = signal + ":ready";
                String resultKey = signal + ":result";
                var adder = redisson.getLongAdder(a[0]);
                adder.add(Long.parseLong(a[1]));
                redisson.<String>getBucket(readyKey).set("1");
                long deadline = System.currentTimeMillis() + 90000;
                boolean seen = false;
                while (System.currentTimeMillis() < deadline) {
                    if (redisson.<String>getBucket(signal).isExists()) {
                        seen = true;
                        break;
                    }
                    Thread.sleep(200);
                }
                String verdict = seen ? "sum=" + adder.sum() : "nosignal";
                try {
                    redisson.<String>getBucket(resultKey).set(verdict);
                } catch (Exception ignored) {
                }
                out.add(verdict);
            }
            // --- RMultimap: HASH field→id (Highway) + {name}:{id} collections;
            // the id derivation must match across languages or getAll misses
            case "mm48-put" -> {
                out.add("put=" + redisson.<String, String>getSetMultimap(a[0]).put(a[1], a[2]));
            }
            case "mm48-getall" -> {
                var vs = new ArrayList<>(redisson.<String, String>getSetMultimap(a[0]).getAll(a[1]));
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("value=" + v);
                }
            }
            case "mm48-keyset" -> {
                var ks = new ArrayList<>(redisson.<String, String>getSetMultimap(a[0]).keySet());
                java.util.Collections.sort(ks);
                out.add("keys=" + ks);
            }
            case "lmm48-put" -> {
                out.add("put=" + redisson.<String, String>getListMultimap(a[0]).put(a[1], a[2]));
            }
            case "lmm48-getall" -> {
                for (String v : redisson.<String, String>getListMultimap(a[0]).getAll(a[1])) {
                    out.add("value=" + v); // LIST order matters
                }
            }
            // --- RMap random entries (HRANDFIELD) ---
            case "map48-random-entries" -> {
                var es = redisson.<String, String>getMap(a[0]).randomEntries(Integer.parseInt(a[1]));
                var lines = new ArrayList<String>();
                for (var e : es.entrySet()) {
                    lines.add(e.getKey() + "=" + e.getValue());
                }
                java.util.Collections.sort(lines);
                for (String l : lines) {
                    out.add("entry=" + l);
                }
            }
            // --- Round 25: execution layer — RBatch / RScript / RBuckets ---
            case "batch48-run" -> {
                // a[0]=bucket a[1]=map a[2]=list a[3]=atomic — batch facades
                // expose only the *Async methods (RFutures resolve on execute)
                var batch = redisson.createBatch();
                batch.getBucket(a[0]).setAsync("java-batch-v");
                batch.getMap(a[1]).putAsync("jf", "java-batch-fv");
                batch.getList(a[2]).addAllAsync(java.util.List.of("java-li1", "java-li2"));
                batch.getAtomicLong(a[3]).incrementAndGetAsync();
                var res = batch.execute();
                out.add("responses=" + res.getResponses().size());
            }
            case "list48-range" -> {
                for (String s : redisson.<String>getList(a[0]).readAll()) {
                    out.add("item=" + s);
                }
            }
            case "buckets48-set" -> {
                var m = new java.util.LinkedHashMap<String, String>();
                m.put(a[0], a[1]);
                m.put(a[2], a[3]);
                redisson.getBuckets().set(m);
                out.add("OK");
            }
            case "buckets48-get" -> {
                var m = redisson.getBuckets().get(a[0], a[1]);
                var keys = new ArrayList<String>(m.keySet());
                java.util.Collections.sort(keys);
                for (String k : keys) {
                    out.add(k + "=" + m.get(k));
                }
            }
            // Script bodies are hardcoded on both sides (probe args split on
            // spaces): TS test constants must stay byte-identical.
            case "script48-put" -> {
                Object v = redisson.getScript().eval(org.redisson.api.RScript.Mode.READ_WRITE,
                        "redis.call('set',KEYS[1],ARGV[1])return 1",
                        org.redisson.api.RScript.ReturnType.LONG, List.of(a[0]), a[1]);
                out.add("value=" + v);
            }
            case "script48-get" -> {
                Object v = redisson.getScript().eval(org.redisson.api.RScript.Mode.READ_ONLY,
                        "return redis.call('get',KEYS[1])",
                        org.redisson.api.RScript.ReturnType.VALUE, List.of(a[0]));
                out.add("value=" + v);
            }
            case "script48-exists" -> {
                out.add("exists=" + redisson.getScript().scriptExists(a[0]).get(0));
            }
            case "script48-load" -> {
                out.add("sha=" + redisson.getScript().scriptLoad("return redis.call('get',KEYS[1])"));
            }
            case "script48-evalsha" -> {
                Object v = redisson.getScript().evalSha(org.redisson.api.RScript.Mode.READ_WRITE,
                        a[0], org.redisson.api.RScript.ReturnType.LONG, List.of(a[1]), a[2]);
                out.add("value=" + v);
            }
            // --- Round 26: sharded topic / conditional buckets / blocking
            // wake / deque both-ends (RShardedTopic extends RTopic in 4.8) ---
            case "sharded48-listen" -> {
                sharded48Latch = new CountDownLatch(1);
                sharded48Msg = null;
                RTopic topic = redisson.getShardedTopic(a[0]);
                int lid = topic.addListener(String.class, (ch, msg) -> {
                    sharded48Msg = msg;
                    sharded48Latch.countDown();
                });
                out.add("listening=" + lid);
            }
            case "sharded48-pub" -> {
                redisson.getShardedTopic(a[0]).publish(a[1]);
                out.add("published=ok");
            }
            case "sharded48-poll" -> {
                boolean r = sharded48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? "msg=" + sharded48Msg : "timeout");
            }
            // MSETEX XX / NX — all-or-nothing multi-set over existing/absent keys
            case "buckets48-casexist" -> {
                var m = new java.util.LinkedHashMap<String, String>();
                m.put(a[0], a[1]);
                m.put(a[2], a[3]);
                var args = org.redisson.api.bucket.SetArgs.entries(m);
                if (a.length > 4) {
                    args = args.timeToLive(java.time.Duration.ofSeconds(Long.parseLong(a[4])));
                }
                out.add("ok=" + redisson.getBuckets().setIfAllKeysExist(args));
            }
            case "buckets48-casabsent" -> {
                var m = new java.util.LinkedHashMap<String, String>();
                m.put(a[0], a[1]);
                m.put(a[2], a[3]);
                out.add("ok=" + redisson.getBuckets().setIfAllKeysAbsent(
                        org.redisson.api.bucket.SetArgs.entries(m)));
            }
            // background TAKE: a daemon thread blocks on BLPOP so the REPL
            // stays responsive; TS's offer is the wake source
            case "bq48-bgtake" -> {
                bq48Latch = new CountDownLatch(1);
                bq48Result = null;
                String name = a[0];
                Thread t = new Thread(() -> {
                    try {
                        bq48Result = "value=" + redisson.<String>getBlockingQueue(name).take();
                    } catch (Exception e) {
                        bq48Result = "error=" + e.getMessage();
                    }
                    bq48Latch.countDown();
                });
                t.setDaemon(true);
                t.start();
                Thread.sleep(300); // let the BLPOP arm before the waker runs
                out.add("armed");
            }
            case "bq48-bgresult" -> {
                boolean r = bq48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? bq48Result : "timeout");
            }
            case "deque48-push" -> {
                // a[0]=name a[1]=first a[2]=last
                redisson.<String>getDeque(a[0]).addFirst(a[1]);
                redisson.<String>getDeque(a[0]).addLast(a[2]);
                out.add("OK");
            }
            case "deque48-pop" -> {
                String f = redisson.<String>getDeque(a[0]).pollFirst();
                String l = redisson.<String>getDeque(a[0]).pollLast();
                out.add("first=" + f + ";last=" + l);
            }
            // --- Round 27: latch/semaphore blocked waits woken cross-language,
            // RPatternTopic, RSetMultimapCache TTL (put() has no TTL in the
            // RMultimapCache interface — per-key expiry is expireKey only) ---
            case "latch48-bgwait" -> {
                latch48BgLatch = new CountDownLatch(1);
                latch48BgResult = null;
                String name = a[0];
                Thread t = new Thread(() -> {
                    try {
                        // no-arg await() is void in 4.8 — bounded variant returns boolean
                        latch48BgResult = "ok=" + redisson.getCountDownLatch(name).await(120, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        latch48BgResult = "error=" + e.getMessage();
                    }
                    latch48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                Thread.sleep(300);
                out.add("armed");
            }
            case "latch48-bgresult" -> {
                boolean r = latch48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? latch48BgResult : "timeout");
            }
            case "sem48-bgacquire" -> {
                sem48BgLatch = new CountDownLatch(1);
                sem48BgResult = null;
                String name = a[0];
                Thread t = new Thread(() -> {
                    try {
                        redisson.getSemaphore(name).acquire();
                        sem48BgResult = "acquired";
                    } catch (Exception e) {
                        sem48BgResult = "error=" + e.getMessage();
                    }
                    sem48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                Thread.sleep(300);
                out.add("armed");
            }
            case "sem48-bgresult" -> {
                boolean r = sem48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? sem48BgResult : "timeout");
            }
            case "pattern48-listen" -> {
                pattern48Latch = new CountDownLatch(1);
                pattern48Msg = null;
                redisson.getPatternTopic(a[0]).addListener(String.class, (pat, ch, msg) -> {
                    pattern48Msg = ch + "|" + msg;
                    pattern48Latch.countDown();
                });
                out.add("listening=ok");
            }
            case "pattern48-poll" -> {
                boolean r = pattern48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? "msg=" + pattern48Msg : "timeout");
            }
            case "mmc48-put" -> {
                out.add("put=" + redisson.<String, String>getSetMultimapCache(a[0]).put(a[1], a[2]));
            }
            case "mmc48-expirekey" -> {
                out.add("ok=" + redisson.<String, String>getSetMultimapCache(a[0])
                        .expireKey(a[1], Long.parseLong(a[2]), TimeUnit.MILLISECONDS));
            }
            case "mmc48-getall" -> {
                var vs = new ArrayList<>(redisson.<String, String>getSetMultimapCache(a[0]).getAll(a[1]));
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("value=" + v);
                }
            }
            // --- Round 28: lock mutex / map conditional CAS ops ---
            case "lock48-trylock" -> {
                out.add("trylock=" + redisson.getLock(a[0]).tryLock(0, java.util.concurrent.TimeUnit.MILLISECONDS));
            }
            case "map48-putifabsent" -> {
                Object prev = redisson.<String, String>getMap(a[0]).putIfAbsent(a[1], a[2]);
                out.add("prev=" + prev);
            }
            case "map48-replace3" -> {
                out.add("ok=" + redisson.<String, String>getMap(a[0]).replace(a[1], a[2], a[3]));
            }
            case "map48-removeval" -> {
                out.add("ok=" + redisson.<String, String>getMap(a[0]).remove(a[1], a[2]));
            }
            case "map48-putall" -> {
                var m = new java.util.LinkedHashMap<String, String>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    m.put(a[i], a[i + 1]);
                }
                redisson.<String, String>getMap(a[0]).putAll(m);
                out.add("OK");
            }
            // --- Round 29: reliable topic / set algebra / scored-set algebra ---
            case "rtopic48-listen" -> {
                rt48Latch = new CountDownLatch(1);
                rt48Msg = null;
                rt48Id = redisson.getReliableTopic(a[0]).addListener(String.class, (ch, msg) -> {
                    rt48Msg = msg;
                    rt48Latch.countDown();
                });
                out.add("listening=" + rt48Id);
            }
            case "rtopic48-pub" -> {
                out.add("published=" + redisson.getReliableTopic(a[0]).publish(a[1]));
            }
            case "rtopic48-poll" -> {
                boolean r = rt48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? "msg=" + rt48Msg : "timeout");
            }
            case "rtopic48-rmlistener" -> {
                redisson.getReliableTopic(a[0]).removeListener(rt48Id);
                out.add("removed=ok");
            }
            case "rtopic48-size" -> {
                out.add("size=" + redisson.getReliableTopic(a[0]).size());
            }
            case "set48-addall" -> {
                redisson.<String>getSet(a[0]).addAll(java.util.Arrays.asList(java.util.Arrays.copyOfRange(a, 1, a.length)));
                out.add("OK");
            }
            case "set48-readall" -> {
                var vs = new ArrayList<>(redisson.<String>getSet(a[0]).readAll());
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("value=" + v);
                }
            }
            case "set48-union" -> {
                // SUNIONSTORE self others... — self is only the destination
                out.add("count=" + redisson.<String>getSet(a[0]).union(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            case "zset48-addpairs" -> {
                var m = new java.util.LinkedHashMap<String, Double>();
                for (int i = 1; i + 1 < a.length; i += 2) {
                    m.put(a[i], Double.parseDouble(a[i + 1]));
                }
                out.add("added=" + redisson.<String>getScoredSortedSet(a[0]).addAll(m));
            }
            case "zset48-union" -> {
                var agg = org.redisson.api.RScoredSortedSet.Aggregate.valueOf(a[1]);
                out.add("count=" + redisson.<String>getScoredSortedSet(a[0]).union(agg, java.util.Arrays.copyOfRange(a, 2, a.length)));
            }
            case "zset48-dump" -> {
                var zset = redisson.<String>getScoredSortedSet(a[0]);
                var vs = new ArrayList<>(zset.readAll());
                java.util.Collections.sort(vs);
                for (String v : vs) {
                    out.add("entry=" + v + "@" + zset.getScore(v));
                }
            }
            // --- Round 30: MapCache deep ops / MapCacheNative / grab-bag ---
            case "mc48-compute" -> {
                Object v = redisson.<String, String>getMapCache(a[0]).computeIfAbsent(a[1],
                        java.time.Duration.ofMillis(Long.parseLong(a[2])), k -> a[3]);
                out.add("value=" + v);
            }
            case "mc48-expireentry" -> {
                out.add("ok=" + redisson.<String, String>getMapCache(a[0]).expireEntry(a[1],
                        java.time.Duration.ofMillis(Long.parseLong(a[2])), java.time.Duration.ZERO));
            }
            case "mc48-rttl" -> {
                out.add("ttl=" + redisson.<String, String>getMapCache(a[0]).remainTimeToLive(a[1]));
            }
            case "mcn48-put" -> {
                redisson.<String, String>getMapCacheNative(a[0]).put(a[1], a[2],
                        java.time.Duration.ofMillis(Long.parseLong(a[3])));
                out.add("put=ok");
            }
            case "mcn48-get" -> {
                out.add("value=" + redisson.<String, String>getMapCacheNative(a[0]).get(a[1]));
            }
            case "mcn48-rttl" -> {
                out.add("ttl=" + redisson.<String, String>getMapCacheNative(a[0]).remainTimeToLive(a[1]));
            }
            case "bits48-and" -> {
                out.add("card=" + redisson.getBitSet(a[0]).and(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            case "bits48-xor" -> {
                out.add("card=" + redisson.getBitSet(a[0]).xor(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            case "list48-set" -> {
                redisson.<String>getList(a[0]).set(Integer.parseInt(a[1]), a[2]);
                out.add("OK");
            }
            case "list48-get" -> {
                // RList extends java.util.List — single-index get returns V
                out.add("value=" + redisson.<String>getList(a[0]).get(Integer.parseInt(a[1])));
            }
            // --- Round 31: RExpirable primitives / zset blocking poll /
            // MapCacheNative conditionals ---
            case "exp48-rttl" -> {
                out.add("ttl=" + redisson.getBucket(a[0]).remainTimeToLive());
            }
            case "exp48-expire" -> {
                out.add("ok=" + redisson.getBucket(a[0]).expire(Long.parseLong(a[1]), TimeUnit.MILLISECONDS));
            }
            case "exp48-expireat" -> {
                out.add("ok=" + redisson.getBucket(a[0]).expireAt(Long.parseLong(a[1])));
            }
            case "exp48-persist" -> {
                // RExpirable has no persist() — clearExpire() (RObject) is it
                out.add("ok=" + redisson.getBucket(a[0]).clearExpire());
            }
            case "zset48-bgpoll" -> {
                zs48BgLatch = new CountDownLatch(1);
                zs48BgResult = null;
                String name = a[0];
                Thread t = new Thread(() -> {
                    try {
                        zs48BgResult = "value=" + redisson.<String>getScoredSortedSet(name).pollFirst(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        zs48BgResult = "error=" + e.getMessage();
                    }
                    zs48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                Thread.sleep(300);
                out.add("armed");
            }
            case "zset48-bgresult" -> {
                boolean r = zs48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? zs48BgResult : "timeout");
            }
            case "mcn48-putifabsent" -> {
                Object prev = redisson.<String, String>getMapCacheNative(a[0]).putIfAbsent(a[1], a[2],
                        java.time.Duration.ofMillis(Long.parseLong(a[3])));
                out.add("prev=" + prev);
            }
            // --- Round 32: RFunction cross-language ---
            case "fn48-load" -> {
                // loadAndReplace(libName, BODY) — Java builds the shebang line
                // itself ("#!lua name=<lib> \n"); body hardcoded (probe args
                // split on spaces)
                String body = "redis.register_function('p31jset', function(keys, args) redis.call('set', keys[1], args[1]) return 1 end)\n"
                        + "redis.register_function{function_name='p31jget', callback=function(keys, args) return redis.call('get', keys[1]) end, flags={'no-writes'}}";
                redisson.getFunction().loadAndReplace(a[0], body);
                out.add("loaded=ok");
            }
            case "fn48-callwrite" -> {
                Object v = redisson.getFunction().call(org.redisson.api.FunctionMode.WRITE, a[0],
                        org.redisson.api.FunctionResult.LONG, List.of(a[1]), a[2]);
                out.add("value=" + v);
            }
            case "fn48-callread" -> {
                Object v = redisson.getFunction().call(org.redisson.api.FunctionMode.READ, a[0],
                        org.redisson.api.FunctionResult.VALUE, List.of(a[1]));
                out.add("value=" + v);
            }
            case "fn48-fnexists" -> {
                var libs = redisson.getFunction().list(a[0]);
                if (libs.isEmpty()) {
                    out.add("found=false");
                } else {
                    var fns = new ArrayList<String>();
                    for (var f : libs.get(0).getFunctions()) {
                        fns.add(f.getName());
                    }
                    java.util.Collections.sort(fns);
                    out.add("fns=" + String.join(",", fns));
                }
            }
            case "fn48-listlibs" -> {
                var names = new ArrayList<String>();
                for (var l : redisson.getFunction().list()) {
                    names.add(l.getName());
                }
                java.util.Collections.sort(names);
                for (String n : names) {
                    out.add("lib=" + n);
                }
            }
            // --- Round 33: RDelayedQueue / RMultiLock / RRedLock / RFencedLock ---
            case "dq48-offer" -> {
                // offer(value, delay, MS) — a fresh delayed-queue object per
                // command; destroy() detaches ITS transfer task so it cannot
                // migrate other sides' staged members mid-test
                org.redisson.api.RDelayedQueue<String> dq =
                        redisson.getDelayedQueue(redisson.getQueue(a[0]));
                dq.offer(a[2], Long.parseLong(a[1]), TimeUnit.MILLISECONDS);
                dq.destroy();
                out.add("offered=ok");
            }
            case "dq48-read" -> {
                // destination queue poll (the migrated member surfaces here)
                Object v = redisson.getQueue(a[0]).poll();
                out.add("value=" + v);
            }
            case "mlk48-trylock" -> {
                org.redisson.api.RLock ml = redisson.getMultiLock(
                        redisson.getLock(a[0]), redisson.getLock(a[1]), redisson.getLock(a[2]));
                out.add("locked=" + ml.tryLock());
            }
            case "mlk48-unlock" -> {
                org.redisson.api.RLock ml = redisson.getMultiLock(
                        redisson.getLock(a[0]), redisson.getLock(a[1]), redisson.getLock(a[2]));
                ml.unlock();
                out.add("unlocked=ok");
            }
            case "mlk48-bgtrylock" -> {
                // daemon thread parks tryLock(waitMs, MS) on the triple; it
                // HOLDS the acquired locks on its own thread until
                // mlk48-bgrelease (a foreign thread's unlock throws)
                long waitMs = Long.parseLong(a[3]);
                mlk48BgLatch = new CountDownLatch(1);
                mlk48BgRelease = new CountDownLatch(1);
                mlk48BgDone = new CountDownLatch(1);
                mlk48BgResult = null;
                Thread t = new Thread(() -> {
                    try {
                        org.redisson.api.RLock ml = redisson.getMultiLock(
                                redisson.getLock(a[0]), redisson.getLock(a[1]), redisson.getLock(a[2]));
                        boolean ok = ml.tryLock(waitMs, TimeUnit.MILLISECONDS);
                        mlk48BgResult = "locked=" + ok;
                        mlk48BgLatch.countDown();
                        if (ok) {
                            mlk48BgRelease.await(20, TimeUnit.SECONDS);
                            ml.unlock();
                        }
                        mlk48BgDone.countDown();
                    } catch (Exception e) {
                        mlk48BgResult = "error=" + e.getClass().getSimpleName();
                        mlk48BgLatch.countDown();
                        mlk48BgDone.countDown();
                    }
                });
                t.setDaemon(true);
                t.start();
                out.add("armed");
            }
            case "mlk48-bgresult" -> {
                boolean r = mlk48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? mlk48BgResult : "timeout");
            }
            case "mlk48-bgrelease" -> {
                mlk48BgRelease.countDown();
                boolean r = mlk48BgDone.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? "released=ok" : "timeout");
            }
            case "rlk48-trylock" -> {
                // RedLock: quorum = 2 of 3 — inner failures up to 1 tolerated
                org.redisson.api.RLock rl = redisson.getRedLock(
                        redisson.getLock(a[0]), redisson.getLock(a[1]), redisson.getLock(a[2]));
                out.add("locked=" + rl.tryLock());
            }
            case "rlk48-unlock" -> {
                org.redisson.api.RLock rl = redisson.getRedLock(
                        redisson.getLock(a[0]), redisson.getLock(a[1]), redisson.getLock(a[2]));
                rl.unlock();
                out.add("unlocked=ok");
            }
            case "fnc48-trylockget" -> {
                // BOUNDED try only: the no-arg tryLockAndGetToken() is
                // bytecode-identical to lockAndGetToken() in 4.8 (both route
                // to (-1, -1)) and BLOCKS until an unlock wake — never call
                // it against a held lock (180s live hang, round 33)
                Long tok = redisson.getFencedLock(a[0])
                        .tryLockAndGetToken(Long.parseLong(a[1]), TimeUnit.MILLISECONDS);
                out.add("token=" + tok);
            }
            case "fnc48-unlock" -> {
                redisson.getFencedLock(a[0]).unlock();
                out.add("unlocked=ok");
            }
            case "fnc48-gettoken" -> {
                out.add("token=" + redisson.getFencedLock(a[0]).getToken());
            }
            case "fnc48-delete" -> {
                // delete() lives on RObject, not on the 4.8 RFencedLock
                // interface (RExpirable is off the RObject chain) — impl cast
                org.redisson.RedissonFencedLock fl =
                        (org.redisson.RedissonFencedLock) redisson.getFencedLock(a[0]);
                out.add("deleted=" + fl.delete());
            }
            // --- Round 34: RBlockingDeque ends / RLock lease + watchdog / stream blocking read ---
            case "bd48-offerfirst" -> {
                out.add("offered=" + redisson.<String>getBlockingDeque(a[0]).offerFirst(a[1]));
            }
            case "bd48-offerlast" -> {
                out.add("offered=" + redisson.<String>getBlockingDeque(a[0]).offerLast(a[1]));
            }
            case "bd48-bgpoll" -> {
                // a[1]=first|last, a[2]=waitMs — daemon thread parks on that end
                String side = a[1];
                long waitMs = Long.parseLong(a[2]);
                bd48BgLatch = new CountDownLatch(1);
                bd48BgResult = null;
                Thread t = new Thread(() -> {
                    try {
                        var bd = redisson.<String>getBlockingDeque(a[0]);
                        Object v = side.equals("first")
                                ? bd.pollFirst(waitMs, TimeUnit.MILLISECONDS)
                                : bd.pollLast(waitMs, TimeUnit.MILLISECONDS);
                        bd48BgResult = "value=" + v;
                    } catch (Exception e) {
                        bd48BgResult = "error=" + e.getClass().getSimpleName();
                    }
                    bd48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                out.add("armed");
            }
            case "bd48-bgresult" -> {
                boolean r = bd48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? bd48BgResult : "timeout");
            }
            case "lock48-trylocklease" -> {
                // tryLock(0, leaseMs, MS) — bounded lease, no watchdog
                boolean ok = redisson.getLock(a[0]).tryLock(0, Long.parseLong(a[1]), TimeUnit.MILLISECONDS);
                out.add("locked=" + ok);
            }
            // --- Round 35: RGeo store family / Java watchdog / re-entry count ---
            case "geo48-store" -> {
                // geo48-store name dest lon lat radiusMeters — plain STORE (no scores)
                long n = redisson.<String>getGeo(a[0]).storeSearchTo(a[1],
                        org.redisson.api.geo.GeoSearchArgs.from(Double.parseDouble(a[2]), Double.parseDouble(a[3]))
                                .radius(Double.parseDouble(a[4]), org.redisson.api.geo.GeoUnit.METERS));
                out.add("stored=" + n);
            }
            case "geo48-storesorted" -> {
                // STOREDIST — dest ZSET scored by distance
                long n = redisson.<String>getGeo(a[0]).storeSortedSearchTo(a[1],
                        org.redisson.api.geo.GeoSearchArgs.from(Double.parseDouble(a[2]), Double.parseDouble(a[3]))
                                .radius(Double.parseDouble(a[4]), org.redisson.api.geo.GeoUnit.METERS));
                out.add("stored=" + n);
            }
            case "lock48-unlock" -> {
                redisson.getLock(a[0]).unlock();
                out.add("unlocked=ok");
            }
            case "lock48-watchdog" -> {
                // no-arg lock(): no lease → Java's own watchdog renews every 30s/3
                redisson.getLock(a[0]).lock();
                out.add("locked=true");
            }
            // --- Round 36: RVectorSet / RJsonBuckets ---
            case "vs48-add" -> {
                // vs48-add name element dim1,dim2,dim3 — comma-joined doubles (probe args split on spaces)
                String[] parts = a[2].split(",");
                java.util.List<java.lang.Double> vec = new java.util.ArrayList<>();
                for (String p : parts) vec.add(Double.parseDouble(p));
                boolean ok = redisson.<String>getVectorSet(a[0])
                        .add(org.redisson.api.vector.VectorAddArgs.element(a[1]).vector(vec.toArray(new Double[0])));
                out.add("added=" + ok);
            }
            case "vs48-sim" -> {
                var hits = redisson.<String>getVectorSet(a[0])
                        .getSimilar(org.redisson.api.vector.VectorSimilarArgs.element(a[1]).count(Integer.parseInt(a[2])));
                out.add("hits=" + String.join(",", hits));
            }
            case "vs48-size" -> {
                out.add("size=" + redisson.<String>getVectorSet(a[0]).size());
            }
            case "vs48-contains" -> {
                out.add("contains=" + redisson.<String>getVectorSet(a[0]).contains(a[1]));
            }
            case "vs48-dim" -> {
                out.add("dim=" + redisson.<String>getVectorSet(a[0]).dimensions());
            }
            case "jb48-set" -> {
                // jb48-set key1 val1 key2 val2 ... — codec plain JSON docs
                var m = new java.util.LinkedHashMap<String, Object>();
                for (int i = 0; i + 1 < a.length; i += 2) m.put(a[i], a[i + 1]);
                redisson.getJsonBuckets(new org.redisson.codec.JacksonCodec<>(Object.class)).set(m);
                out.add("set=ok");
            }
            case "jb48-get" -> {
                var keys = new ArrayList<String>();
                for (int i = 0; i < a.length; i++) keys.add(a[i]);
                var m = redisson.<String>getJsonBuckets(new org.redisson.codec.JacksonCodec<>(Object.class)).get(keys.toArray(new String[0]));
                for (int i = 0; i < a.length; i++) out.add("k=" + a[i] + " v=" + m.get(a[i]));
            }
            // --- Round 37: blocking lock() wake / RBinaryStream bytes / stream trim ---
            case "lock48-bglock" -> {
                // daemon thread parks in blocking lock() until someone unlocks
                lk48BgLatch = new CountDownLatch(1);
                lk48BgResult = null;
                Thread t = new Thread(() -> {
                    try {
                        redisson.getLock(a[0]).lock();
                        lk48BgResult = "locked=ok";
                    } catch (Exception e) {
                        lk48BgResult = "error=" + e.getClass().getSimpleName();
                    }
                    lk48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                out.add("armed");
            }
            case "lock48-bgresult" -> {
                boolean r = lk48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? lk48BgResult : "timeout");
            }
            case "bs48-set" -> {
                // RBinaryStream extends RBucket<byte[]> — plain set/get works
                redisson.getBinaryStream(a[0]).set(a[1].getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
                out.add("set=ok");
            }
            case "bs48-get" -> {
                byte[] b = redisson.getBinaryStream(a[0]).get();
                out.add("len=" + (b == null ? -1 : b.length)
                        + " text=" + (b == null ? "" : new String(b, java.nio.charset.StandardCharsets.ISO_8859_1)));
            }
            case "stream48-trim" -> {
                // trim(MAXLEN n) — the builder needs a terminal (noLimit)
                long removed = redisson.<String, String>getStream(a[0]).trim(
                        org.redisson.api.stream.StreamTrimArgs.maxLen(Integer.parseInt(a[1])).noLimit());
                out.add("removed=" + removed);
            }
            // --- Round 38: BLMPOP multi-queue / semaphore addPermits ---
            case "blm48-bgpollfirstany" -> {
                // daemon thread parks on pollFirstFromAny(waitMs, MS, names...)
                // — a[1..] are the queue names, a[0] the wait in ms
                long waitMs = Long.parseLong(a[0]);
                String[] names = java.util.Arrays.copyOfRange(a, 1, a.length);
                bd48BgLatch = new CountDownLatch(1);
                bd48BgResult = null;
                Thread t = new Thread(() -> {
                    try {
                        // NOTE: RedissonBlockingDeque.pollFirstFromAny IGNORES the
                        // extra names (parks on self only — source line 121); the
                        // real multi-queue BLMPOP is the queue count variant
                        var m = redisson.<String>getBlockingQueue(names[0])
                                .pollFirstFromAny(java.time.Duration.ofMillis(waitMs), 1, names);
                        String v = m.isEmpty() ? null : m.values().iterator().next().get(0);
                        bd48BgResult = "value=" + v;
                    } catch (Exception e) {
                        bd48BgResult = "error=" + e.getClass().getSimpleName();
                    }
                    bd48BgLatch.countDown();
                });
                t.setDaemon(true);
                t.start();
                out.add("armed");
            }
            case "blm48-bgresult" -> {
                boolean r = bd48BgLatch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? bd48BgResult : "timeout");
            }
            case "sem48-addpermits" -> {
                redisson.getSemaphore(a[0]).addPermits(Integer.parseInt(a[1]));
                out.add("added=ok");
            }
            // --- Round 39: MapCache idle expiry / HLL merge ---
            case "mci48-put" -> {
                // put(K, V, ttl, MS, maxIdle, MS) — the idle variant
                redisson.getMapCache(a[0]).put(a[1], a[2],
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS,
                        Long.parseLong(a[4]), TimeUnit.MILLISECONDS);
                out.add("put=ok");
            }
            case "hll48-merge" -> {
                redisson.<Integer>getHyperLogLog(a[0]).mergeWith(java.util.Arrays.copyOfRange(a, 1, a.length));
                out.add("merged=ok");
            }
            case "hll48-countwith" -> {
                long c = redisson.<Integer>getHyperLogLog(a[0])
                        .countWith(java.util.Arrays.copyOfRange(a, 1, a.length));
                out.add("count=" + c);
            }
            // --- Round 40: RMapCache entry events (redisson_map_cache_* topics) ---
            case "mce48-listen" -> {
                mce48Latch = new CountDownLatch(1);
                mce48Result = null;
                org.redisson.api.RMapCache<String, String> mc = redisson.getMapCache(a[0]);
                // Java dispatches by instanceof on the four sub-interfaces —
                // the bare MapEntryListener is rejected ("Wrong listener
                // type"); the static listener implements ALL four so every
                // kind reaches us
                mce48Id = mc.addListener(new MceAllListener());
                out.add("listening=" + mce48Id);
            }
            case "mce48-poll" -> {
                // fresh latch first — wait for the NEXT event, not a stale one
                mce48Latch = new CountDownLatch(1);
                boolean r = mce48Latch.await(Long.parseLong(a[0]), TimeUnit.MILLISECONDS);
                out.add(r ? mce48Result : "timeout");
            }
            case "mce48-rmlistener" -> {
                redisson.getMapCache(a[0]).removeListener(mce48Id);
                out.add("removed=ok");
            }
            // --- Round 41: search synonyms / queue continuous subscription ---
            case "fts48-synupdate" -> {
                // updateSynonyms(index, groupId, terms...)
                redisson.getSearch().updateSynonyms(a[0], a[1],
                        java.util.Arrays.copyOfRange(a, 2, a.length));
                out.add("syn=ok");
            }
            case "fts48-syndump" -> {
                var d = redisson.getSearch().dumpSynonyms(a[0]);
                var terms = new ArrayList<String>(d.keySet());
                java.util.Collections.sort(terms);
                for (String t : terms) {
                    out.add("term=" + t + " groups=" + d.get(t));
                }
            }
            case "bqs48-subscribe" -> {
                // continuous consume: take() loop delivering to the list
                bqs48Seen.clear();
                bqs48Id = redisson.<String>getBlockingQueue(a[0])
                        .subscribeOnElements((java.util.function.Consumer<String>) v -> bqs48Seen.add(String.valueOf(v)));
                out.add("subscribed=" + bqs48Id);
            }
            case "bqs48-seen" -> {
                synchronized (bqs48Seen) {
                    out.add("n=" + bqs48Seen.size() + " items=" + String.join(",", bqs48Seen));
                }
            }
            case "bqs48-unsubscribe" -> {
                redisson.<String>getBlockingQueue(a[0]).unsubscribe(bqs48Id);
                out.add("unsub=ok");
            }
            // --- Round 49: RBlockingDeque first/last element subscriptions ---
            case "bd48-subfirst" -> {
                bd48FirstSeen.clear();
                bd48FirstId = redisson.<String>getBlockingDeque(a[0])
                        .subscribeOnFirstElements((java.util.function.Consumer<String>) v -> bd48FirstSeen.add(String.valueOf(v)));
                out.add("subscribed=" + bd48FirstId);
            }
            case "bd48-sublast" -> {
                bd48LastSeen.clear();
                bd48LastId = redisson.<String>getBlockingDeque(a[0])
                        .subscribeOnLastElements((java.util.function.Consumer<String>) v -> bd48LastSeen.add(String.valueOf(v)));
                out.add("subscribed=" + bd48LastId);
            }
            case "bd48-seen" -> {
                synchronized (bd48FirstSeen) {
                    synchronized (bd48LastSeen) {
                        out.add("first=" + String.join(",", bd48FirstSeen) + " last=" + String.join(",", bd48LastSeen));
                    }
                }
            }
            case "bd48-unsubscribe" -> {
                redisson.<String>getBlockingDeque(a[0]).unsubscribe(bd48FirstId);
                redisson.<String>getBlockingDeque(a[0]).unsubscribe(bd48LastId);
                out.add("unsub=ok");
            }
            // --- Round 49: RDoubleAdder cross-language flush ---
            case "adder48-dcohort" -> {
                // adder48-dcohort name delta signal — same contract as the long
                // cohort: add locally, mark ready, wait for the TS signal, sum()
                String signal = a[2];
                String readyKey = signal + ":ready";
                String resultKey = signal + ":result";
                var adder = redisson.getDoubleAdder(a[0]);
                adder.add(Double.parseDouble(a[1]));
                redisson.<String>getBucket(readyKey).set("1");
                long deadline = System.currentTimeMillis() + 90000;
                boolean seen = false;
                while (System.currentTimeMillis() < deadline) {
                    if (redisson.<String>getBucket(signal).isExists()) {
                        seen = true;
                        break;
                    }
                    Thread.sleep(200);
                }
                String verdict = seen ? "sum=" + adder.sum() : "nosignal";
                try {
                    redisson.<String>getBucket(resultKey).set(verdict);
                } catch (Exception ignored) {
                }
                out.add(verdict);
            }
            case "adder48-dadd" -> {
                redisson.getDoubleAdder(a[0]).add(Double.parseDouble(a[1]));
                out.add("added=ok");
            }
            case "adder48-dsum" -> {
                double v = redisson.getDoubleAdder(a[0]).sum();
                out.add("sum=" + v);
            }
            case "adder48-dreset" -> {
                redisson.getDoubleAdder(a[0]).reset();
                out.add("reset=ok");
            }
            // --- Round 49: RFunction load (no REPLACE) fails on existing lib ---
            case "fn48-loadplain" -> {
                try {
                    redisson.getFunction().load(a[0], a[1]);
                    out.add("loaded=ok");
                } catch (Exception e) {
                    String msg = String.valueOf(e.getMessage()).split("\n")[0];
                    out.add("err=" + msg);
                }
            }
            case "fn48-loadandreplace" -> {
                // body is space-free (probe args split on spaces); the value
                // is interpolated into the Lua string literal
                String body = "redis.register_function{function_name='p49tsf',callback=function(keys,args)return('\"" + a[1] + "\"')end,flags={'no-writes'}}";
                redisson.getFunction().loadAndReplace(a[0], body);
                out.add("replaced=ok");
            }
            // --- Round 42: TimeSeries remove family / RingBuffer reads / stream readMany ---
            case "ts48-getandremove" -> {
                Object v = redisson.<Double, String>getTimeSeries(a[0]).getAndRemove(Long.parseLong(a[1]));
                out.add("value=" + v);
            }
            case "ts48-remove" -> {
                out.add("removed=" + redisson.<Double, String>getTimeSeries(a[0]).remove(Long.parseLong(a[1])));
            }
            case "ts48-removerange" -> {
                out.add("count=" + redisson.<Double, String>getTimeSeries(a[0])
                        .removeRange(Long.parseLong(a[1]), Long.parseLong(a[2])));
            }
            case "rb48-add" -> {
                redisson.<String>getRingBuffer(a[0]).add(a[1]);
                out.add("added=ok");
            }
            case "rb48-readnewest" -> {
                var vs = redisson.<String>getRingBuffer(a[0]).readNewest(Integer.parseInt(a[1]));
                out.add("newest=" + String.join(",", vs));
            }
            case "rb48-readoldest" -> {
                var vs = redisson.<String>getRingBuffer(a[0]).readOldest(Integer.parseInt(a[1]));
                out.add("oldest=" + String.join(",", vs));
            }
            case "stream48-readmany" -> {
                // stream48-readmany <count> <name1> <name2> — two-stream XREAD
                // from the beginning (greaterThan only has (id1, name2, id2) forms)
                var zero = new org.redisson.api.stream.StreamMessageId(0);
                var args2 = org.redisson.api.stream.StreamMultiReadArgs
                        .greaterThan(zero, a[2], zero).count(Integer.parseInt(a[0]));
                var res = redisson.<String, String>getStream(a[1]).read(args2);
                for (var e : res.entrySet()) {
                    for (var m : e.getValue().entrySet()) {
                        out.add("stream=" + e.getKey() + " id=" + m.getKey() + " fields=" + m.getValue());
                    }
                }
            }
            // --- Round 43: geo addIfExists / rate limiter PER_CLIENT / bitset bytes ---
            case "geo48-addifexists" -> {
                Boolean ok = redisson.<String>getGeo(a[0]).addIfExists(
                        Double.parseDouble(a[1]), Double.parseDouble(a[2]), a[3]);
                out.add("updated=" + ok);
            }
            case "rate48-setratepc" -> {
                boolean ok = redisson.getRateLimiter(a[0]).trySetRate(
                        RateType.PER_CLIENT, Long.parseLong(a[1]),
                        java.time.Duration.ofSeconds(Long.parseLong(a[2])));
                out.add("set=" + ok);
            }
            case "bs48-getbyte" -> {
                out.add("byte=" + redisson.getBitSet(a[0]).getByte(Long.parseLong(a[1])));
            }
            case "bs48-setbyte" -> {
                out.add("prev=" + redisson.getBitSet(a[0]).setByte(
                        Long.parseLong(a[1]), Byte.parseByte(a[2])));
            }
            // --- Round 44: conditional expire / TS native rules / stream ackdel ---
            case "exp48-if" -> {
                // a[1]=set|notset|greater|less, a[2]=ttlMs (Duration overloads)
                java.time.Duration d = java.time.Duration.ofMillis(Long.parseLong(a[2]));
                boolean ok;
                switch (a[1]) {
                    case "set" -> ok = redisson.getBucket(a[0]).expireIfSet(d);
                    case "notset" -> ok = redisson.getBucket(a[0]).expireIfNotSet(d);
                    case "greater" -> ok = redisson.getBucket(a[0]).expireIfGreater(d);
                    default -> ok = redisson.getBucket(a[0]).expireIfLess(d);
                }
                out.add("ok=" + ok);
            }
            case "tsn48-addrule" -> {
                // tsn48-addrule <src> <dest> <agg> <bucketMs> — dest must exist
                redisson.getTimeSeriesNative(a[0]).createRule(org.redisson.api.tsnative.TSRuleArgs
                        .destination(a[1], org.redisson.api.tsnative.TSAggregation.valueOf(a[2]),
                                java.time.Duration.ofMillis(Long.parseLong(a[3]))));
                out.add("rule=ok");
            }
            case "tsn48-ruledest" -> {
                var dest = redisson.getTimeSeriesNative(a[0]);
                out.add("firstTs=" + dest.firstTimestamp() + " size=" + dest.size());
            }
            case "stream48-ackdel" -> {
                // stream48-ackdel <name> <group> <refpolicy> <ids...> — XACKDEL
                var res = redisson.<String, String>getStream(a[0]).ack(
                        org.redisson.api.stream.StreamAckArgs.group(a[1])
                                .ids(java.util.Arrays.stream(a, 3, a.length)
                                        .map(id -> {
                                            String[] q = id.split("-");
                                            return new org.redisson.api.stream.StreamMessageId(
                                                    Long.parseLong(q[0]), Long.parseLong(q[1]));
                                        }).toArray(org.redisson.api.stream.StreamMessageId[]::new))
                                .removeReferences());
                for (var e : res.entrySet()) {
                    out.add("id=" + e.getKey() + " status=" + e.getValue());
                }
            }
            // --- Round 45: list LINSERT / stream XNACK / deque LMOVE ---
            case "list48-addbefore" -> {
                out.add("len=" + redisson.<String>getList(a[0]).addBefore(a[1], a[2]));
            }
            case "list48-addafter" -> {
                out.add("len=" + redisson.<String>getList(a[0]).addAfter(a[1], a[2]));
            }
            case "stream48-nack" -> {
                // stream48-nack <name> <group> <mode> <ids...> — XNACK (8.8+)
                long n = redisson.<String, String>getStream(a[0]).nack(
                        org.redisson.api.stream.StreamNackArgs.group(a[1],
                                org.redisson.api.stream.StreamNackMode.valueOf(a[2]))
                                .ids(java.util.Arrays.stream(a, 3, a.length)
                                        .map(id -> {
                                            String[] q = id.split("-");
                                            return new org.redisson.api.stream.StreamMessageId(
                                                    Long.parseLong(q[0]), Long.parseLong(q[1]));
                                        }).toArray(org.redisson.api.stream.StreamMessageId[]::new)));
                out.add("nacked=" + n);
            }
            case "bd48-movefirst" -> {
                // DequeMoveArgs.pollFirst().addLastTo(dest) — LMOVE LEFT RIGHT
                Object v = redisson.<String>getBlockingDeque(a[0]).move(java.time.Duration.ZERO,
                        org.redisson.api.queue.DequeMoveArgs.pollFirst().addLastTo(a[1]));
                out.add("value=" + v);
            }
            // --- Round 46: audit-closed families (lex / scored / setcache) ---
            case "lex48-pollfirst" -> {
                out.add("value=" + redisson.getLexSortedSet(a[0]).pollFirst());
            }
            case "lex48-polllast" -> {
                out.add("value=" + redisson.getLexSortedSet(a[0]).pollLast());
            }
            case "lex48-revrank" -> {
                Integer r = redisson.getLexSortedSet(a[0]).revRank(a[1]);
                out.add("revrank=" + r);
            }
            case "lex48-rangehead" -> {
                for (String v : redisson.getLexSortedSet(a[0]).rangeHead(a[1], a[2].equals("1"))) {
                    out.add("v=" + v);
                }
            }
            case "zset48-rankentry" -> {
                var re = redisson.<String>getScoredSortedSet(a[0]).rankEntry(a[1]);
                out.add(re == null ? "null" : "rank=" + re.getRank() + " score=" + re.getScore());
            }
            case "zset48-pollentries" -> {
                var es = a[1].equals("first")
                        ? redisson.<String>getScoredSortedSet(a[0]).pollFirstEntries(Integer.parseInt(a[2]))
                        : redisson.<String>getScoredSortedSet(a[0]).pollLastEntries(Integer.parseInt(a[2]));
                for (var e : es) {
                    out.add("value=" + e.getValue() + "@" + e.getScore());
                }
            }
            case "zset48-rangeto" -> {
                int n = redisson.<String>getScoredSortedSet(a[0]).rangeTo(a[1],
                        Integer.parseInt(a[2]), Integer.parseInt(a[3]));
                out.add("stored=" + n);
            }
            case "zset48-retainall" -> {
                java.util.List<String> keep = java.util.Arrays.asList(java.util.Arrays.copyOfRange(a, 1, a.length));
                out.add("changed=" + redisson.<String>getScoredSortedSet(a[0]).retainAll(keep));
            }
            case "sc48-addif" -> {
                // sc48-addif name absent|exists|less|greater value ttlMs
                java.time.Duration d = java.time.Duration.ofMillis(Long.parseLong(a[3]));
                boolean ok;
                switch (a[1]) {
                    case "absent" -> ok = redisson.<String>getSetCache(a[0]).addIfAbsent(d, a[2]);
                    case "exists" -> ok = redisson.<String>getSetCache(a[0]).addIfExists(d, a[2]);
                    case "less" -> ok = redisson.<String>getSetCache(a[0]).addIfLess(d, a[2]);
                    default -> ok = redisson.<String>getSetCache(a[0]).addIfGreater(d, a[2]);
                }
                out.add("ok=" + ok);
            }
            // --- Round 47: bucket GETEX/LCS/DIGEST, semaphore releaseIfExists,
            // PES tryRelease/getPermits, MapCache entry-expiry family, stream
            // trimNonStrict, list fastSet/fastRemove/subList ---
            case "rb48-sia" -> {
                out.add("ok=" + redisson.<String>getBucket(a[0]).setIfAbsent(a[1]));
            }
            case "rb48-getclear" -> {
                String v = redisson.<String>getBucket(a[0]).getAndClearExpire();
                out.add("value=" + v);
            }
            case "rb48-lcs" -> {
                String v = redisson.<String>getBucket(a[0]).findCommon(a[1]);
                out.add("common=" + v);
            }
            case "rb48-lcslen" -> {
                out.add("len=" + redisson.<String>getBucket(a[0]).findCommonLength(a[1]));
            }
            case "rb48-digest" -> {
                out.add("digest=" + redisson.<String>getBucket(a[0]).getDigest());
            }
            case "sem48-relifexists" -> {
                out.add("ok=" + redisson.getSemaphore(a[0]).releaseIfExists(Integer.parseInt(a[1])));
            }
            case "pes48-getpermits" -> {
                out.add("permits=" + redisson.getPermitExpirableSemaphore(a[0]).getPermits());
            }
            case "pes48-tryrelease" -> {
                out.add("ok=" + redisson.getPermitExpirableSemaphore(a[0]).tryRelease(a[1]));
            }
            case "pes48-tryreleaseids" -> {
                var ids = java.util.Arrays.asList(a[1].split(","));
                out.add("released=" + redisson.getPermitExpirableSemaphore(a[0]).tryRelease(ids));
            }
            case "mc48-updatettl" -> {
                boolean ok = redisson.<String, String>getMapCache(a[0]).updateEntryExpiration(a[1],
                        Long.parseLong(a[2]), TimeUnit.MILLISECONDS,
                        Long.parseLong(a[3]), TimeUnit.MILLISECONDS);
                out.add("ok=" + ok);
            }
            case "mc48-entryns" -> {
                boolean ok = redisson.<String, String>getMapCache(a[0]).expireEntryIfNotSet(a[1],
                        java.time.Duration.ofMillis(Long.parseLong(a[2])),
                        Long.parseLong(a[3]) > 0 ? java.time.Duration.ofMillis(Long.parseLong(a[3])) : null);
                out.add("ok=" + ok);
            }
            case "mc48-getallttl" -> {
                var m = redisson.<String, String>getMapCache(a[0]).getAllWithTTLOnly(
                        new java.util.HashSet<>(java.util.Arrays.asList(a[1].split(","))));
                for (var e : m.entrySet()) out.add(e.getKey() + "=" + e.getValue());
            }
            case "stream48-trimns" -> {
                long removed = redisson.<String, String>getStream(a[0]).trimNonStrict(
                        org.redisson.api.stream.StreamTrimArgs.maxLen(Integer.parseInt(a[1])).noLimit());
                out.add("removed=" + removed);
            }
            case "list48-fastset" -> {
                redisson.<String>getList(a[0]).fastSet(Integer.parseInt(a[1]), a[2]);
                out.add("set=ok");
            }
            case "list48-fastremove" -> {
                redisson.<String>getList(a[0]).fastRemove(Integer.parseInt(a[1]));
                out.add("removed=ok");
            }
            case "list48-sublist" -> {
                var vs = redisson.<String>getList(a[0]).subList(Integer.parseInt(a[1]), Integer.parseInt(a[2])).readAll();
                out.add("sub=" + String.join(",", vs));
            }
            // --- Round 48: ZMPOP/BZMPOP entries family, zset bg takeFirst,
            // bloom config getters, BF-native insert/info/scandump/loadchunk,
            // multimap core ops ---
            case "zset48-zmpop" -> {
                // zset48-zmpop <MIN|MAX> <count> <name1,name2>
                int count = Integer.parseInt(a[1]);
                String[] names = a[2].split(",");
                String[] rest = java.util.Arrays.copyOfRange(names, 1, names.length);
                var m = a[1].equals("MIN")
                        ? redisson.<String>getScoredSortedSet(names[0]).pollFirstEntriesFromAny(count, rest)
                        : redisson.<String>getScoredSortedSet(names[0]).pollLastEntriesFromAny(count, rest);
                for (var e : m.entrySet()) {
                    for (var e2 : e.getValue().entrySet()) out.add(e.getKey() + "|" + e2.getKey() + "@" + e2.getValue());
                }
            }
            case "zset48-bzmpop" -> {
                // zset48-bzmpop <MIN|MAX> <timeoutS> <count> <name1,name2>
                java.time.Duration d = java.time.Duration.ofSeconds(Long.parseLong(a[1]));
                int count = Integer.parseInt(a[2]);
                String[] names = a[3].split(",");
                String[] rest = java.util.Arrays.copyOfRange(names, 1, names.length);
                var m = a[1].equals("MIN")
                        ? redisson.<String>getScoredSortedSet(names[0]).pollFirstEntriesFromAny(d, count, rest)
                        : redisson.<String>getScoredSortedSet(names[0]).pollLastEntriesFromAny(d, count, rest);
                for (var e : m.entrySet()) {
                    for (var e2 : e.getValue().entrySet()) out.add(e.getKey() + "|" + e2.getKey() + "@" + e2.getValue());
                }
            }
            case "bf48-init" -> {
                // bf48-init <name> <expectedInsertions> <falseProbability>
                boolean ok = redisson.getBloomFilter(a[0]).tryInit(Long.parseLong(a[1]), Double.parseDouble(a[2]));
                out.add("ok=" + ok);
            }
            case "bf48-cfg" -> {
                var bf = redisson.getBloomFilter(a[0]);
                out.add("exp=" + bf.getExpectedInsertions() + " fp=" + bf.getFalseProbability()
                        + " size=" + bf.getSize() + " iters=" + bf.getHashIterations());
            }
            case "bfn48-addmany" -> {
                java.util.Set<String> added = redisson.<String>getBloomFilterNative(a[0]).add(java.util.Arrays.asList(a[1].split(",")));
                out.add("added=" + String.join(",", added));
            }
            case "bfn48-insert" -> {
                // bfn48-insert <name> <capacity|0> <nocreate|0|1> <i1,i2>
                var opt = org.redisson.api.bloomfilter.BloomFilterInsertArgs.<String>elements(java.util.Arrays.asList(a[3].split(",")));
                if (!a[1].equals("0")) opt = opt.capacity(Long.parseLong(a[1]));
                opt = opt.noCreate(a[2].equals("1"));
                java.util.Set<String> added = redisson.<String>getBloomFilterNative(a[0]).insert(opt);
                out.add("added=" + String.join(",", added));
            }
            case "bfn48-existsmany" -> {
                java.util.Set<String> got = redisson.<String>getBloomFilterNative(a[0]).exists(java.util.Arrays.asList(a[1].split(",")));
                out.add("exists=" + String.join(",", got));
            }
            case "bfn48-info" -> {
                var i = redisson.<String>getBloomFilterNative(a[0]).getInfo();
                out.add("info=" + i.getCapacity() + "," + i.getSize() + "," + i.getSubFilterCount() + "," + i.getItemCount() + "," + i.getExpansionRate());
            }
            case "bfn48-scandump" -> {
                // one line per chunk: chunk=<iter> <hex>; replayable via loadChunk
                long it = 0;
                for (;;) {
                    var c = redisson.<String>getBloomFilterNative(a[0]).scanDump(it);
                    if (c.getIterator() == 0) break;
                    it = c.getIterator();
                    out.add("chunk=" + it + " " + java.util.HexFormat.of().formatHex(c.getData()));
                }
                out.add("dumpend");
            }
            case "bfn48-loadchunk" -> {
                // bfn48-loadchunk <name> <iter> <hexdata>
                redisson.<String>getBloomFilterNative(a[0]).loadChunk(Long.parseLong(a[1]), java.util.HexFormat.of().parseHex(a[2]));
                out.add("loaded=ok");
            }
            case "mm48-putall" -> {
                redisson.<String, String>getListMultimap(a[0]).putAll(a[1], java.util.Arrays.asList(a[2].split(",")));
                out.add("putall=ok");
            }
            case "mm48-getalllist" -> {
                var vs = redisson.<String, String>getListMultimap(a[0]).getAll(a[1]);
                out.add("values=" + String.join(",", vs));
            }
            case "mm48-replacevalues" -> {
                var prev = redisson.<String, String>getListMultimap(a[0]).replaceValues(a[1], java.util.Arrays.asList(a[2].split(",")));
                out.add("prev=" + String.join(",", prev));
            }
            case "mm48-removeall" -> {
                var prev = redisson.<String, String>getListMultimap(a[0]).removeAll(a[1]);
                out.add("prev=" + String.join(",", prev));
            }
            case "mm48-entries" -> {
                for (var e : redisson.<String, String>getListMultimap(a[0]).entries()) out.add(e.getKey() + "=" + e.getValue());
            }
            case "mm48-containsentry" -> {
                out.add("ok=" + redisson.<String, String>getListMultimap(a[0]).containsEntry(a[1], a[2]));
            }
            case "mm48-keysize" -> {
                out.add("keys=" + redisson.<String, String>getListMultimap(a[0]).keySize());
            }
            case "mm48-fastremove" -> {
                out.add("removed=" + redisson.<String, String>getListMultimap(a[0]).fastRemove(java.util.Arrays.copyOfRange(a, 1, a.length)));
            }
            case "mm48-readallkeyset" -> {
                out.add("keys=" + String.join(",", redisson.<String, String>getListMultimap(a[0]).readAllKeySet()));
            }
            default -> throw new IllegalArgumentException("unknown command: " + cmd);
        }
        return out;
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static org.redisson.codec.JacksonCodec<java.util.Map> jsonMapCodec() {
        return new org.redisson.codec.JacksonCodec<>(java.util.Map.class);
    }

    private static CompareAndDeleteArgs caddArgs(String op, double threshold) {
        switch (op) {
            case "<": return CompareAndDeleteArgs.less(threshold);
            case "<=": return CompareAndDeleteArgs.lessOrEqual(threshold);
            case ">": return CompareAndDeleteArgs.greater(threshold);
            case ">=": return CompareAndDeleteArgs.greaterOrEqual(threshold);
            case "==": return CompareAndDeleteArgs.equal(threshold);
            case "~=": return CompareAndDeleteArgs.notEqual(threshold);
            default: throw new IllegalArgumentException("bad op: " + op);
        }
    }

    private TsProbe48() {
    }
}
