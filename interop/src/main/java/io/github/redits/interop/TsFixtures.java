package io.github.redits.interop;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.redisson.api.RAtomicDouble;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RDeque;
import org.redisson.api.RGeo;
import org.redisson.api.RHyperLogLog;
import org.redisson.api.RLexSortedSet;
import org.redisson.api.RList;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RQueue;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RSemaphore;
import org.redisson.api.RSet;
import org.redisson.api.RSetCache;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.geo.GeoEntry;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamMessageId;

/**
 * Fixtures shared with redi.ts. Java writes under {@code <ns>j:} and reads
 * TS-written data under {@code <ns>t:}. The TS side mirrors these exact
 * keys/values (see interop/README.md + test/live-interop.test.ts).
 */
public final class TsFixtures {

    private final RedissonClient redisson;

    public TsFixtures(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public void clean(String ns) {
        redisson.getKeys().deleteByPattern(ns + "*");
    }

    /** Java writes canonical fixtures under {@code ns + "j:"}. */
    public void writeJavaSide(String ns) {
        String J = ns + "j:";

        redisson.<Object>getBucket(J + "bucket:str").set("hello from java");
        redisson.getBucket(J + "bucket:int").set(42);
        redisson.getBucket(J + "bucket:long").set(3_000_000_000L);
        redisson.getBucket(J + "bucket:double").set(3.14);
        redisson.getBucket(J + "bucket:bool").set(true);
        redisson.getBucket(J + "bucket:list").set(List.of(1, 2, 3));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("a", 1);
        nested.put("b", "two");
        redisson.getBucket(J + "bucket:map").set(nested);

        redisson.getAtomicLong(J + "atomiclong").set(100);
        redisson.getAtomicDouble(J + "atomicdouble").set(2.71);

        RMap<String, Object> map = redisson.getMap(J + "map");
        map.clear();
        map.put("str", "value");
        map.put("int", 42);
        map.put("long", 3_000_000_000L);
        map.put("double", 3.14);
        map.put("bool", true);
        map.put("list", List.of(1, 2, 3));

        RSet<Object> set = redisson.getSet(J + "set");
        set.clear();
        set.add("alpha");
        set.add("beta");
        set.add(42);
        set.add(3.14);

        RList<Object> list = redisson.getList(J + "list");
        list.clear();
        list.add("one");
        list.add(2);
        list.add(3.14);
        list.add(true);

        RQueue<Object> q = redisson.getQueue(J + "queue");
        q.clear();
        q.add("q1");
        q.add("q2");

        RDeque<Object> dq = redisson.getDeque(J + "deque");
        dq.clear();
        dq.addFirst("d1");
        dq.addLast("d2");

        RScoredSortedSet<Object> zset = redisson.getScoredSortedSet(J + "zset");
        zset.clear();
        zset.add(1.0, "low");
        zset.add(2.0, "mid");
        zset.add(3.0, "high");

        RLexSortedSet lex = redisson.getLexSortedSet(J + "lex");
        lex.delete();
        lex.add("apple");
        lex.add("banana");
        lex.add("cherry");

        RBlockingQueue<Object> bq = redisson.getBlockingQueue(J + "bqueue");
        bq.clear();
        bq.add("bq1");
        bq.add("bq2");

        RBloomFilter<String> bf = redisson.getBloomFilter(J + "bloom");
        bf.delete();
        bf.tryInit(1000L, 0.03);
        bf.add("maybe1");
        bf.add("maybe2");

        RMapCache<String, Object> mc = redisson.getMapCache(J + "mapcache");
        mc.clear();
        mc.put("k1", "v1");

        RSetCache<Object> sc = redisson.getSetCache(J + "setcache");
        sc.clear();
        sc.add("s1");

        RCountDownLatch cdl = redisson.getCountDownLatch(J + "latch");
        cdl.delete();
        cdl.trySetCount(3);

        RSemaphore sem = redisson.getSemaphore(J + "sem");
        sem.delete();
        sem.trySetPermits(5);

        RHyperLogLog<Integer> hll = redisson.getHyperLogLog(J + "hll");
        hll.delete();
        hll.addAll(List.of(1, 2, 3, 4, 5));

        RGeo<String> geo = redisson.getGeo(J + "geo");
        geo.delete();
        geo.add(new GeoEntry(13.361389, 38.115556, "Palermo"));
        geo.add(new GeoEntry(15.087269, 37.502669, "Catania"));

        RStream<String, String> stream = redisson.getStream(J + "stream");
        stream.delete();
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("field", "value");
        stream.add(StreamAddArgs.entries(fields));
    }

    /**
     * Read TS-written fixtures under {@code ns + "t:"}. The TS side writes
     * the mirror image of {@link #writeJavaSide} (see README contract).
     */
    public List<String> readTsSide(String ns) {
        List<String> r = new ArrayList<>();
        String P = ns + "t:";

        record(r, "bucket:str", () -> eq(redisson.<Object>getBucket(P + "bucket:str").get(), "hello from ts"),
                () -> actual(redisson.<Object>getBucket(P + "bucket:str").get()));
        record(r, "bucket:int", () -> eq(redisson.<Object>getBucket(P + "bucket:int").get(), 7),
                () -> actual(redisson.<Object>getBucket(P + "bucket:int").get()));
        record(r, "bucket:long", () -> eq(redisson.<Object>getBucket(P + "bucket:long").get(), 5_000_000_000L),
                () -> actual(redisson.<Object>getBucket(P + "bucket:long").get()));
        record(r, "bucket:double", () -> eq(redisson.<Object>getBucket(P + "bucket:double").get(), 1.41),
                () -> actual(redisson.<Object>getBucket(P + "bucket:double").get()));
        record(r, "bucket:bool", () -> eq(redisson.<Object>getBucket(P + "bucket:bool").get(), false),
                () -> actual(redisson.<Object>getBucket(P + "bucket:bool").get()));
        record(r, "bucket:list", () -> eq(redisson.<Object>getBucket(P + "bucket:list").get(), List.of(4, 5, 6)),
                () -> actual(redisson.<Object>getBucket(P + "bucket:list").get()));

        record(r, "atomiclong", () -> redisson.getAtomicLong(P + "atomiclong").get() == 200,
                () -> String.valueOf(redisson.getAtomicLong(P + "atomiclong").get()));
        record(r, "atomicdouble", () -> Math.abs(redisson.getAtomicDouble(P + "atomicdouble").get() - 6.28) < 1e-9,
                () -> String.valueOf(redisson.getAtomicDouble(P + "atomicdouble").get()));

        RMap<String, Object> map = redisson.getMap(P + "map");
        record(r, "map.get(name)", () -> eq(map.get("name"), "redi.ts"), () -> actual(map.get("name")));
        record(r, "map.size", () -> map.size() == 3, () -> String.valueOf(map.size()));

        RSet<Object> set = redisson.getSet(P + "set");
        record(r, "set.contains(x)", () -> set.contains("x"), () -> actual(set.readAll()));
        record(r, "set.contains(99)", () -> set.contains(99), () -> "");
        record(r, "set.size", () -> set.size() == 3, () -> String.valueOf(set.size()));

        RList<Object> list = redisson.getList(P + "list");
        record(r, "list", () -> eq(list.readAll(), List.of("a", "b", "c")), () -> actual(list.readAll()));

        RQueue<Object> q = redisson.getQueue(P + "queue");
        record(r, "queue.peek", () -> eq(q.peek(), "qa"), () -> actual(q.readAll()));

        RDeque<Object> dq = redisson.getDeque(P + "deque");
        record(r, "deque", () -> eq(dq.readAll(), List.of("d_first", "d_last")), () -> actual(dq.readAll()));

        RLexSortedSet lex = redisson.getLexSortedSet(P + "lex");
        record(r, "lex.contains(tx1)", () -> lex.contains("tx1"), () -> actual(lex.range(0, -1)));
        record(r, "lex.size", () -> lex.size() == 2, () -> String.valueOf(lex.size()));

        RBlockingQueue<Object> bq = redisson.getBlockingQueue(P + "bqueue");
        record(r, "bqueue.peek", () -> eq(bq.peek(), "tbq1"), () -> actual(bq.readAll()));

        RScoredSortedSet<Object> zset = redisson.getScoredSortedSet(P + "zset");
        record(r, "zset.getScore(a)", () -> {
            Double s = zset.getScore("a");
            return s != null && Math.abs(s - 10.0) < 1e-9;
        }, () -> String.valueOf(zset.getScore("a")));

        RBloomFilter<String> bf = redisson.getBloomFilter(P + "bloom");
        record(r, "bloom.contains(tsbloom1)", () -> bf.contains("tsbloom1"), () -> "count=" + bf.count());

        RMapCache<String, Object> mc = redisson.getMapCache(P + "mapcache");
        record(r, "mapcache.get(tk)", () -> eq(mc.get("tk"), "tv"), () -> actual(mc.get("tk")));

        RSetCache<Object> sc = redisson.getSetCache(P + "setcache");
        record(r, "setcache.contains(tsc1)", () -> sc.contains("tsc1"), () -> actual(sc.readAll()));

        record(r, "latch.count", () -> redisson.getCountDownLatch(P + "latch").getCount() == 4,
                () -> String.valueOf(redisson.getCountDownLatch(P + "latch").getCount()));
        record(r, "sem.permits", () -> redisson.getSemaphore(P + "sem").availablePermits() == 8,
                () -> String.valueOf(redisson.getSemaphore(P + "sem").availablePermits()));
        record(r, "hll.count", () -> redisson.<Integer>getHyperLogLog(P + "hll").count() == 3,
                () -> String.valueOf(redisson.<Integer>getHyperLogLog(P + "hll").count()));

        RGeo<String> geo = redisson.getGeo(P + "geo");
        record(r, "geo.contains(TPalermo)", () -> geo.contains("TPalermo"), () -> String.valueOf(geo.size()));

        RStream<String, String> stream = redisson.getStream(P + "stream");
        record(r, "stream.range", () -> {
            Map<StreamMessageId, Map<String, String>> sread = stream.range(StreamMessageId.MIN, StreamMessageId.MAX);
            return sread != null && !sread.isEmpty();
        }, () -> "size=" + stream.size());

        return r;
    }

    private static boolean eq(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
        }
        return Objects.equals(a, b);
    }

    private static String actual(Object o) {
        return o == null ? "null" : (o.getClass().getSimpleName() + ":" + o);
    }

    private static void check(List<String> r, String label, boolean ok, String detail) {
        String d = detail == null || detail.isEmpty() ? "" : " | " + detail;
        r.add((ok ? "[PASS] " : "[FAIL] ") + label + d);
    }

    @FunctionalInterface
    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private static void record(List<String> r, String label,
                               ThrowingSupplier<Boolean> test, ThrowingSupplier<String> detail) {
        try {
            boolean ok = Boolean.TRUE.equals(test.get());
            String det = detail == null ? "" : detail.get();
            check(r, label, ok, det);
        } catch (Throwable t) {
            String msg = t.getMessage();
            r.add("[ERROR] " + label + " | " + t.getClass().getSimpleName()
                    + (msg == null ? "" : ": " + msg));
        }
    }
}
