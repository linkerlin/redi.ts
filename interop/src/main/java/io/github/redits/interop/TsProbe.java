package io.github.redits.interop;

import java.util.List;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

/** CLI: TsProbe [clean|write-java|read-ts|roundtrip|lock-try|lock-hold|rate-try|sem-try|repl] [args...]. */
public final class TsProbe {

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "roundtrip" : args[0].toLowerCase();
        String ns = args.length >= 2 ? args[1] : "TsJavaT:";

        String host = System.getenv().getOrDefault("REDIS_HOST", "127.0.0.1");
        String port = System.getenv().getOrDefault("REDIS_PORT", "6379");

        Config config = new Config();
        config.setCodec(new JsonJacksonCodec());
        config.useSingleServer().setAddress("redis://" + host + ":" + port);

        RedissonClient redisson = Redisson.create(config);
        TsFixtures fx = new TsFixtures(redisson);
        try {
            if ("repl".equals(mode)) {
                // Resident probe: one client, many commands (see TsRepl).
                TsRepl.serve(redisson, fx, ns);
                return;
            }
            run(mode, args, ns, redisson, fx);
        } finally {
            redisson.shutdown();
        }
    }

    /** Executes one probe command against an existing client. */
    static void run(String mode, String[] args, String ns, RedissonClient redisson, TsFixtures fx) throws Exception {
        switch (mode) {
                case "clean" -> {
                    fx.clean(ns);
                    System.out.println("cleaned " + ns + "*");
                }
                case "write-java" -> {
                    fx.writeJavaSide(ns);
                    System.out.println("java fixtures written under " + ns + "j:");
                }
                case "read-ts" -> {
                    System.out.println("reading TS fixtures under " + ns + "t:");
                    List<String> lines = fx.readTsSide(ns);
                    lines.forEach(System.out::println);
                    long fails = lines.stream().filter(l -> !l.startsWith("[PASS]")).count();
                    if (fails > 0) throw new IllegalStateException("read-ts fixtures failed: " + fails);
                }
                case "roundtrip" -> {
                    fx.clean(ns);
                    fx.writeJavaSide(ns);
                    System.out.println("java fixtures written under " + ns + "j:");
                    System.out.println("now run the TS writer, then: mvn exec:java -Dexec.args=\"read-ts " + ns + "\"");
                }
                case "lock-try" -> {
                    // args: lock-try <fullLockName> — single attempt, prints ACQUIRED|CONTENDED
                    String lockName = args.length >= 2 ? args[1] : "TsJavaT:lock:x";
                    boolean ok;
                    try {
                        ok = redisson.getLock(lockName).tryLock(0, 30, java.util.concurrent.TimeUnit.SECONDS);
                        if (ok) redisson.getLock(lockName).unlock();
                    } catch (Exception e) {
                        throw new IllegalStateException("lock-try failed: " + e, e);
                    }
                    System.out.println(ok ? "ACQUIRED" : "CONTENDED");
                }
                case "lock-hold" -> {
                    // args: lock-hold <fullLockName> <holdMs> — holds then releases, prints HELD|RELEASED
                    String lockName = args.length >= 2 ? args[1] : "TsJavaT:lock:x";
                    long holdMs = args.length >= 3 ? Long.parseLong(args[2]) : 3000;
                    var l = redisson.getLock(lockName);
                    l.lock(30, java.util.concurrent.TimeUnit.SECONDS);
                    System.out.println("HELD");
                    System.out.flush();
                    Thread.sleep(holdMs);
                    l.unlock();
                    System.out.println("RELEASED");
                }
                case "rate-try" -> {
                    // args: rate-try <fullLimiterName> <permits> — prints true|false
                    String rlName = args.length >= 2 ? args[1] : "TsJavaT:ratelimit:x";
                    long permits = args.length >= 3 ? Long.parseLong(args[2]) : 1;
                    System.out.println(redisson.getRateLimiter(rlName).tryAcquire(permits) ? "true" : "false");
                }
                case "rate-set" -> {
                    // args: rate-set <fullLimiterName> <rate> <intervalMs>
                    String rlName = args.length >= 2 ? args[1] : "TsJavaT:ratelimit:x";
                    long rate = args.length >= 3 ? Long.parseLong(args[2]) : 5;
                    long intervalMs = args.length >= 4 ? Long.parseLong(args[3]) : 60000;
                    var rl = redisson.getRateLimiter(rlName);
                    rl.trySetRate(org.redisson.api.RateType.OVERALL, rate,
                            java.time.Duration.ofMillis(intervalMs));
                    System.out.println("RATE_SET " + rate + "/" + intervalMs);
                }
                case "sem-try" -> {
                    // args: sem-try <fullSemName> — single tryAcquire, prints true|false
                    String semName = args.length >= 2 ? args[1] : "TsJavaT:sem:x";
                    System.out.println(redisson.getSemaphore(semName).tryAcquire() ? "true" : "false");
                }
                case "rw-read-try", "rw-write-try" -> {
                    // args: rw-{read,write}-try <fullRwName> — prints ACQUIRED|CONTENDED
                    String rwName = args.length >= 2 ? args[1] : "TsJavaT:rw:x";
                    var l = mode.equals("rw-read-try")
                            ? redisson.getReadWriteLock(rwName).readLock()
                            : redisson.getReadWriteLock(rwName).writeLock();
                    boolean ok = l.tryLock(0, 30, java.util.concurrent.TimeUnit.SECONDS);
                    if (ok) l.unlock();
                    System.out.println(ok ? "ACQUIRED" : "CONTENDED");
                }
                case "rw-read-hold", "rw-write-hold" -> {
                    // args: rw-{read,write}-hold <fullRwName> <holdMs>
                    String rwName = args.length >= 2 ? args[1] : "TsJavaT:rw:x";
                    long holdMs = args.length >= 3 ? Long.parseLong(args[2]) : 3000;
                    var l = mode.equals("rw-read-hold")
                            ? redisson.getReadWriteLock(rwName).readLock()
                            : redisson.getReadWriteLock(rwName).writeLock();
                    l.lock(30, java.util.concurrent.TimeUnit.SECONDS);
                    System.out.println("HELD");
                    System.out.flush();
                    Thread.sleep(holdMs);
                    l.unlock();
                    System.out.println("RELEASED");
                }
                case "fair-try" -> {
                    System.out.println("NO_FAIR_IN_461");
                }
                case "fenced-token" -> {
                    // args: fenced-token <fullLockName> — acquire+unlock, prints TOKEN:<n>
                    String lockName = args.length >= 2 ? args[1] : "TsJavaT:fenced:x";
                    var fl = redisson.getFencedLock(lockName);
                    Long tok = fl.tryLockAndGetToken();
                    if (tok == null) {
                        System.out.println("CONTENDED");
                    } else {
                        fl.unlock();
                        System.out.println("TOKEN:" + tok);
                    }
                }
                case "dq-offer2" -> {
                    // args: dq-offer2 <dest> <v1> <delay1Ms> <v2> <delay2Ms>
                    // Two offers from ONE JVM so deadlines keep their relative
                    // order (separate JVMs boot ~7s apart, dwarfing small delays).
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:dq7:x";
                    var destQ = redisson.<String>getBlockingQueue(dest);
                    var dq = redisson.<String>getDelayedQueue(destQ);
                    dq.offer(args.length >= 3 ? args[2] : "v1",
                            args.length >= 4 ? Long.parseLong(args[3]) : 2000,
                            java.util.concurrent.TimeUnit.MILLISECONDS);
                    dq.offer(args.length >= 5 ? args[4] : "v2",
                            args.length >= 6 ? Long.parseLong(args[5]) : 800,
                            java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("OFFERED2");
                }
                case "dq-offer" -> {
                    // args: dq-offer <destQueue> <value> <delayMs>
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:dq:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    long delayMs = args.length >= 4 ? Long.parseLong(args[3]) : 2000;
                    var destQ = redisson.<String>getBlockingQueue(dest);
                    redisson.<String>getDelayedQueue(destQ).offer(value, delayMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("OFFERED");
                }
                case "dq-poll" -> {
                    // args: dq-poll <destQueue> <timeoutMs> — prints GOT:<v>|EMPTY
                    // NOTE: creating the DelayedQueue wrapper schedules the
                    // transfer task (subscribe → immediate pushTask scan).
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:dq:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 8000;
                    var destQ = redisson.<String>getBlockingQueue(dest);
                    redisson.<String>getDelayedQueue(destQ);
                    String v = destQ.poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println(v == null ? "EMPTY" : ("GOT:" + v));
                }
                case "dq-size" -> {
                    // args: dq-size <destQueue> — prints DQ_SIZE:<n>
                    // Java size() = staging LIST length, not destination LLEN.
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:dq:x";
                    var destQ = redisson.<String>getBlockingQueue(dest);
                    System.out.println("DQ_SIZE:" + redisson.<String>getDelayedQueue(destQ).size());
                }
                case "rt-publish" -> {
                    // args: rt-publish <topic> <msg>
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:rt:x";
                    String msg = args.length >= 3 ? args[2] : "hello";
                    redisson.getReliableTopic(topic).publish(msg);
                    System.out.println("PUBLISHED");
                }
                case "rt-listen" -> {
                    // args: rt-listen <topic> <timeoutMs> <resultKey>
                    // prints GOT:<msg>|TIMEOUT and stores it at resultKey
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:rt:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (topic + ":listen-result");
                    var latch = new java.util.concurrent.CountDownLatch(1);
                    var got = new java.util.concurrent.atomic.AtomicReference<String>();
                    var rt = redisson.getReliableTopic(topic);
                    rt.addListener(String.class, (ch, msg) -> {
                        got.set(msg);
                        latch.countDown();
                    });
                    boolean ok = latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = ok ? ("GOT:" + got.get()) : "TIMEOUT";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "ts-add" -> {
                    // args: ts-add <name> <timestamp> <value>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ts:x";
                    long ts = args.length >= 3 ? Long.parseLong(args[2]) : System.currentTimeMillis();
                    String value = args.length >= 4 ? args[3] : "v";
                    redisson.<String, String>getTimeSeries(name).add(ts, value);
                    System.out.println("TS_ADDED");
                }
                case "ts-range" -> {
                    // args: ts-range <name> <from> <to> — prints TS_VALUES:[...]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ts:x";
                    long from = args.length >= 3 ? Long.parseLong(args[2]) : 0;
                    long to = args.length >= 4 ? Long.parseLong(args[3]) : Long.MAX_VALUE;
                    var vals = redisson.<String, String>getTimeSeries(name).range(from, to);
                    System.out.println("TS_VALUES:" + vals);
                }
                case "longadder-add" -> {
                    // args: longadder-add <name> <delta>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:adder:x";
                    long delta = args.length >= 3 ? Long.parseLong(args[2]) : 1;
                    redisson.getLongAdder(name).add(delta);
                    System.out.println("ADDER_ADDED");
                }
                case "longadder-sum" -> {
                    // args: longadder-sum <name> — prints ADDER_SUM:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:adder:x";
                    System.out.println("ADDER_SUM:" + redisson.getLongAdder(name).sum());
                }
                case "longadder-cohort" -> {
                    // args: longadder-cohort <name> <delta> <signalKey>
                    // Adds delta, marks readyKey, waits until signalKey exists
                    // (TS has added + summed), then sums and stores result at
                    // resultKey. Keeps this JVM (and its local buffer +
                    // listener) alive across both sums. Prints too.
                    String name = args.length >= 2 ? args[1] : "TsJavaT:adder:x";
                    long delta = args.length >= 3 ? Long.parseLong(args[2]) : 100;
                    String signal = args.length >= 4 ? args[3] : (name + ":ts-done");
                    String readyKey = signal + ":ready";
                    String resultKey = signal + ":result";
                    var adder = redisson.getLongAdder(name);
                    adder.add(delta);
                    redisson.<String>getBucket(readyKey).set("1");
                    System.out.println("READY");
                    System.out.flush();
                    long deadline = System.currentTimeMillis() + 90000;
                    boolean seen = false;
                    while (System.currentTimeMillis() < deadline) {
                        if (redisson.getBucket(signal).isExists()) {
                            seen = true;
                            break;
                        }
                        Thread.sleep(200);
                    }
                    String verdict;
                    if (!seen) {
                        verdict = "ADDER_NOSIGNAL";
                    } else {
                        verdict = "ADDER_SUM:" + adder.sum();
                    }
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "pes-available" -> {
                    // args: pes-available <name> — prints PES_AVAILABLE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:pes:x";
                    System.out.println("PES_AVAILABLE:" + redisson.getPermitExpirableSemaphore(name).availablePermits());
                }
                // ---- phase 4: pub/sub, streams, multimap, batch, misc ----
                case "topic-publish" -> {
                    // args: topic-publish <topic> <msg>
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:topic:x";
                    String msg = args.length >= 3 ? args[2] : "hello";
                    long n = redisson.getTopic(topic).publish(msg);
                    System.out.println("PUBLISHED:" + n);
                }
                case "topic-listen" -> {
                    // args: topic-listen <topic> <timeoutMs> <resultKey>
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:topic:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (topic + ":listen-result");
                    var latch = new java.util.concurrent.CountDownLatch(1);
                    var got = new java.util.concurrent.atomic.AtomicReference<String>();
                    redisson.getTopic(topic).addListener(String.class, (ch, msg) -> {
                        got.set(msg);
                        latch.countDown();
                    });
                    boolean ok = latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = ok ? ("GOT:" + got.get()) : "TIMEOUT";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "stream-group-create" -> {
                    // args: stream-group-create <stream> <group>
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:stream:x";
                    String group = args.length >= 3 ? args[2] : "g1";
                    try {
                        redisson.getStream(stream).createGroup(
                                org.redisson.api.stream.StreamCreateGroupArgs.name(group).makeStream());
                    } catch (Exception e) {
                        if (!String.valueOf(e).contains("BUSYGROUP")) throw e;
                    }
                    System.out.println("GROUP_CREATED");
                }
                case "stream-group-read" -> {
                    // args: stream-group-read <stream> <group> <consumer> <resultKey> — prints STREAM_GOT:<firstValue>|EMPTY
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:stream:x";
                    String group = args.length >= 3 ? args[2] : "g1";
                    String consumer = args.length >= 4 ? args[3] : "c1";
                    String resultKey = args.length >= 5 ? args[4] : (stream + ":read-result");
                    var s = redisson.<String, String>getStream(stream);
                    var r = s.readGroup(group, consumer,
                            org.redisson.api.stream.StreamReadGroupArgs
                                    .greaterThan(org.redisson.api.stream.StreamMessageId.NEVER_DELIVERED)
                                    .count(10)
                                    .timeout(java.time.Duration.ofSeconds(5)));
                    String verdict = "EMPTY";
                    if (r != null && !r.isEmpty()) {
                        var first = r.values().iterator().next();
                        verdict = "STREAM_GOT:" + first;
                    }
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "multimap-put" -> {
                    // args: multimap-put <name> <key> <value>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mm:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    redisson.<String, String>getSetMultimap(name).put(key, value);
                    System.out.println("MM_PUT");
                }
                case "batch-map-put" -> {
                    // args: batch-map-put <mapName> <key> <value> — via createBatch
                    String name = args.length >= 2 ? args[1] : "TsJavaT:batchmap:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    var batch = redisson.createBatch();
                    batch.<String, String>getMap(name).putAsync(key, value);
                    batch.execute();
                    System.out.println("BATCH_DONE");
                }
                case "bq-offer" -> {
                    // args: bq-offer <name> <value> — bounded blocking queue offer
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bq:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    redisson.<String>getBoundedBlockingQueue(name).offer(value);
                    System.out.println("BQ_OFFERED");
                }
                case "bq-capacity" -> {
                    // args: bq-capacity <name> <n> — trySetCapacity, prints BQ_CAP:<bool>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bq:x";
                    int n = args.length >= 3 ? Integer.parseInt(args[2]) : 2;
                    System.out.println("BQ_CAP:" + redisson.<String>getBoundedBlockingQueue(name).trySetCapacity(n));
                }
                case "bq-remaining" -> {
                    // args: bq-remaining <name> — available permits (Java semantics)
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bq:x";
                    System.out.println("BQ_REMAIN:" + redisson.<String>getBoundedBlockingQueue(name).remainingCapacity());
                }
                case "bbq-put" -> {
                    // args: bbq-put <queue> <value> <resultKey> — blocking put;
                    // writes BQ_PUT_STARTED before blocking and BQ_PUT_OK after.
                    String q = args.length >= 2 ? args[1] : "TsJavaT:bbq17:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    String resultKey = args.length >= 4 ? args[3] : (q + ":put-result");
                    try {
                        redisson.getBucket(resultKey).set("BQ_PUT_STARTED");
                        redisson.<String>getBoundedBlockingQueue(q).put(value);
                        redisson.getBucket(resultKey).set("BQ_PUT_OK");
                    } catch (Exception e) {
                        redisson.getBucket(resultKey).set("BQ_PUT_ERROR:" + e);
                        throw e;
                    }
                    System.out.println("BQ_PUT_OK");
                }
                case "idgen-init" -> {
                    // args: idgen-init <name> <value> <alloc> — prints IDGEN_INIT:<bool>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:idgen:x";
                    long value = args.length >= 3 ? Long.parseLong(args[2]) : 1000;
                    long alloc = args.length >= 4 ? Long.parseLong(args[3]) : 1;
                    System.out.println("IDGEN_INIT:" + redisson.getIdGenerator(name).tryInit(value, alloc));
                }
                case "idgen-next" -> {
                    // args: idgen-next <name> — prints IDGEN:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:idgen:x";
                    System.out.println("IDGEN:" + redisson.getIdGenerator(name).nextId());
                }
                case "mc-put-ttl" -> {
                    // args: mc-put-ttl <name> <key> <value> <ttlMs> — MapCache per-entry TTL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcttl:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    long ttlMs = args.length >= 5 ? Long.parseLong(args[4]) : 2000;
                    redisson.<String, String>getMapCache(name).put(key, value, ttlMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("MC_TTL_SET");
                }
                case "ring-setcap" -> {
                    // args: ring-setcap <name> <cap> — prints RING_CAP:<bool>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ring:x";
                    int cap = args.length >= 3 ? Integer.parseInt(args[2]) : 3;
                    System.out.println("RING_CAP:" + redisson.<String>getRingBuffer(name).trySetCapacity(cap));
                }
                case "ring-add" -> {
                    // args: ring-add <name> <value>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ring:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    redisson.<String>getRingBuffer(name).add(value);
                    System.out.println("RING_ADDED");
                }
                // ---- phase 5 ----
                case "sharded-publish" -> {
                    // args: sharded-publish <topic> <msg>
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:sht:x";
                    String msg = args.length >= 3 ? args[2] : "hello";
                    long n = redisson.getShardedTopic(topic).publish(msg);
                    System.out.println("PUBLISHED:" + n);
                }
                case "sharded-listen" -> {
                    // args: sharded-listen <topic> <timeoutMs> <resultKey>
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:sht:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (topic + ":listen-result");
                    var latch = new java.util.concurrent.CountDownLatch(1);
                    var got = new java.util.concurrent.atomic.AtomicReference<String>();
                    redisson.getShardedTopic(topic).addListener(String.class, (ch, msg) -> {
                        got.set(msg);
                        latch.countDown();
                    });
                    boolean ok = latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = ok ? ("GOT:" + got.get()) : "TIMEOUT";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "pattern-listen" -> {
                    // args: pattern-listen <pattern> <timeoutMs> <resultKey>
                    String pattern = args.length >= 2 ? args[1] : "TsJavaT:pat:*";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : "TsJavaT:pat:listen-result";
                    var latch = new java.util.concurrent.CountDownLatch(1);
                    var got = new java.util.concurrent.atomic.AtomicReference<String>();
                    redisson.getPatternTopic(pattern).addListener(String.class, (pat, ch, msg) -> {
                        got.set(ch + "=" + msg);
                        latch.countDown();
                    });
                    boolean ok = latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = ok ? ("GOT:" + got.get()) : "TIMEOUT";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "q-offer" -> {
                    // args: q-offer <queue> <value> — plain blocking-queue RPUSH
                    String q = args.length >= 2 ? args[1] : "TsJavaT:qh:x";
                    String v = args.length >= 3 ? args[2] : "v";
                    redisson.<String>getBlockingQueue(q).offer(v);
                    System.out.println("Q_OFFERED");
                }
                case "bq-take" -> {                    // args: bq-take <queue> <timeoutMs> <resultKey>
                    String q = args.length >= 2 ? args[1] : "TsJavaT:bqh:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (q + ":take-result");
                    String v = redisson.<String>getBlockingQueue(q).poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = v == null ? "EMPTY" : ("GOT:" + v);
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "latch-set" -> {
                    // args: latch-set <name> <count>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:latch:x";
                    long n = args.length >= 3 ? Long.parseLong(args[2]) : 1;
                    redisson.getCountDownLatch(name).trySetCount(n);
                    System.out.println("LATCH_SET");
                }
                case "latch-await" -> {
                    // args: latch-await <name> <timeoutMs> <resultKey>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:latch:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (name + ":await-result");
                    boolean open;
                    try {
                        open = redisson.getCountDownLatch(name).await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        open = false;
                    }
                    String verdict = open ? "LATCH_OPEN" : "LATCH_CLOSED";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "latch-countdown" -> {
                    // args: latch-countdown <name>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:latch:x";
                    redisson.getCountDownLatch(name).countDown();
                    System.out.println("LATCH_CD");
                }
                case "geo-add" -> {
                    // args: geo-add <name> <lon> <lat> <member>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    String member = args.length >= 5 ? args[4] : "Palermo";
                    redisson.<String>getGeo(name).add(lon, lat, member);
                    System.out.println("GEO_ADDED");
                }
                case "geo-dist" -> {
                    // args: geo-dist <name> <m1> <m2> — prints GEO_DIST:<meters>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo:x";
                    String m1 = args.length >= 3 ? args[2] : "a";
                    String m2 = args.length >= 4 ? args[3] : "b";
                    Double d = redisson.<String>getGeo(name).dist(m1, m2, org.redisson.api.geo.GeoUnit.METERS);
                    System.out.println("GEO_DIST:" + d);
                }
                case "bitset-set" -> {
                    // args: bitset-set <name> <bit>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits:x";
                    long bit = args.length >= 3 ? Long.parseLong(args[2]) : 7;
                    redisson.getBitSet(name).set(bit, true);
                    System.out.println("BIT_SET");
                }
                case "bitset-get" -> {
                    // args: bitset-get <name> <bit> — prints BIT:0|1
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits:x";
                    long bit = args.length >= 3 ? Long.parseLong(args[2]) : 7;
                    System.out.println("BIT:" + (redisson.getBitSet(name).get(bit) ? 1 : 0));
                }
                case "hll-add" -> {
                    // args: hll-add <name> <csvInts>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:hll5:x";
                    String csv = args.length >= 3 ? args[2] : "1,2,3";
                    var hll = redisson.<Integer>getHyperLogLog(name);
                    for (String p : csv.split(",")) hll.add(Integer.parseInt(p.trim()));
                    System.out.println("HLL_ADDED");
                }
                case "hll-count" -> {
                    // args: hll-count <name> — prints HLL_COUNT:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:hll5:x";
                    System.out.println("HLL_COUNT:" + redisson.getHyperLogLog(name).count());
                }
                case "setcache-add-ttl" -> {
                    // args: setcache-add-ttl <name> <value> <ttlMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:sc5:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    long ttlMs = args.length >= 4 ? Long.parseLong(args[3]) : 1500;
                    redisson.<String>getSetCache(name).add(value, ttlMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("SC_TTL_SET");
                }
                case "mc-put-idle" -> {
                    // args: mc-put-idle <name> <key> <value> <maxIdleMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc5:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    long idleMs = args.length >= 5 ? Long.parseLong(args[4]) : 1500;
                    redisson.<String, String>getMapCache(name).put(key, value, 0, java.util.concurrent.TimeUnit.MILLISECONDS, idleMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("MC_IDLE_SET");
                }
                case "mcnative-put" -> {
                    // args: mcnative-put <name> <key> <value> <ttlMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    long ttlMs = args.length >= 5 ? Long.parseLong(args[4]) : 60000;
                    redisson.<String, String>getMapCacheNative(name).put(key, value, java.time.Duration.ofMillis(ttlMs));
                    System.out.println("MCN_SET");
                }
                case "mcnative-get" -> {
                    // args: mcnative-get <name> <key> — prints MCN_GET:<v>|MCN_NULL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String v = redisson.<String, String>getMapCacheNative(name).get(key);
                    System.out.println(v == null ? "MCN_NULL" : ("MCN_GET:" + v));
                }
                case "mcnative-expire" -> {
                    // args: mcnative-expire <name> <key> <ttlMs> — prints MCN_EXPIRE:true|false
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    long ttlMs = args.length >= 4 ? Long.parseLong(args[3]) : 60000;
                    boolean ok = redisson.<String, String>getMapCacheNative(name)
                            .expireEntry(key, java.time.Duration.ofMillis(ttlMs));
                    System.out.println("MCN_EXPIRE:" + ok);
                }
                case "mcnative-expire-notset" -> {
                    // args: mcnative-expire-notset <name> <key> <ttlMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    long ttlMs = args.length >= 4 ? Long.parseLong(args[3]) : 60000;
                    System.out.println("MCN_EXPIRE:" + redisson.<String, String>getMapCacheNative(name)
                            .expireEntryIfNotSet(key, java.time.Duration.ofMillis(ttlMs)));
                }
                case "mcnative-expire-gt" -> {
                    // args: mcnative-expire-gt <name> <key> <ttlMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    long ttlMs = args.length >= 4 ? Long.parseLong(args[3]) : 60000;
                    System.out.println("MCN_EXPIRE:" + redisson.<String, String>getMapCacheNative(name)
                            .expireEntryIfGreater(key, java.time.Duration.ofMillis(ttlMs)));
                }
                case "mcnative-expire-lt" -> {
                    // args: mcnative-expire-lt <name> <key> <ttlMs>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    long ttlMs = args.length >= 4 ? Long.parseLong(args[3]) : 60000;
                    System.out.println("MCN_EXPIRE:" + redisson.<String, String>getMapCacheNative(name)
                            .expireEntryIfLess(key, java.time.Duration.ofMillis(ttlMs)));
                }
                case "mcnative-clearexpire" -> {
                    // args: mcnative-clearexpire <name> <key> — prints MCN_CLEAR:true|false
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    System.out.println("MCN_CLEAR:" + redisson.<String, String>getMapCacheNative(name).clearExpire(key));
                }
                case "mcnative-ttl" -> {
                    // args: mcnative-ttl <name> <key> — prints MCN_TTL:<ms>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    System.out.println("MCN_TTL:" + redisson.<String, String>getMapCacheNative(name).remainTimeToLive(key));
                }
                case "mcnative-putifabsent" -> {
                    // args: mcnative-putifabsent <name> <key> <value> <ttlMs>
                    // prints MCN_PIA:<oldValue>|MCN_PIA:null
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mcn:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    long ttlMs = args.length >= 5 ? Long.parseLong(args[4]) : 60000;
                    String old = redisson.<String, String>getMapCacheNative(name)
                            .putIfAbsent(key, value, java.time.Duration.ofMillis(ttlMs));
                    System.out.println(old == null ? "MCN_PIA:null" : ("MCN_PIA:" + old));
                }
                case "script-eval" -> {
                    // args: script-eval <luaBodyReturningInt>
                    String body = args.length >= 2 ? args[1] : "return 40+2";
                    Object r = redisson.getScript().eval(
                            org.redisson.api.RScript.Mode.READ_WRITE, body,
                            org.redisson.api.RScript.ReturnType.LONG);
                    System.out.println("SCRIPT_RES:" + r);
                }
                case "vector-contains" -> {
                    // args: vector-contains <name> <element> — prints VEC_HAS:true|false
                    String name = args.length >= 2 ? args[1] : "TsJavaT:vec5:x";
                    String element = args.length >= 3 ? args[2] : "e1";
                    System.out.println("VEC_HAS:" + redisson.getVectorSet(name).contains(element));
                }
                // ---- phase 6 ----
                case "mc-listen" -> {
                    // args: mc-listen <name> <timeoutMs> <resultKey>
                    // collects C:/U:/R:/X: lines from MapCache entry listeners.
                    // Marks <resultKey>:ready AFTER subscribing so writers can
                    // wait deterministically (fixed sleeps race JVM boot).
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc6:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 20000;
                    String resultKey = args.length >= 4 ? args[3] : (name + ":events");
                    var mc = redisson.<String, String>getMapCache(name);
                    var events = new java.util.concurrent.CopyOnWriteArrayList<String>();
                    mc.addListener((org.redisson.api.map.event.EntryCreatedListener<String, String>) e ->
                            events.add("C:" + e.getKey() + "=" + e.getValue()));
                    mc.addListener((org.redisson.api.map.event.EntryUpdatedListener<String, String>) e ->
                            events.add("U:" + e.getKey() + "=" + e.getValue()));
                    mc.addListener((org.redisson.api.map.event.EntryRemovedListener<String, String>) e ->
                            events.add("R:" + e.getKey() + "=" + e.getOldValue()));
                    mc.addListener((org.redisson.api.map.event.EntryExpiredListener<String, String>) e ->
                            events.add("X:" + e.getKey() + "=" + e.getValue()));
                    try {
                        redisson.<String>getBucket(resultKey + ":ready").set("1");
                    } catch (Exception ignored) {
                    }
                    Thread.sleep(timeoutMs);
                    String verdict = String.join("|", events);
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println("MC_EVENTS:" + verdict);
                }
                case "fn-call" -> {
                    // args: fn-call <funcName> — prints FN_RES:<v>
                    // NOTE: FunctionMode.READ sends FCALL_RO, which Redis rejects
                    // for functions without the no-writes flag — use WRITE.
                    String func = args.length >= 2 ? args[1] : "p6answer";
                    Object v = redisson.getFunction().call(
                            org.redisson.api.FunctionMode.WRITE, func,
                            org.redisson.api.FunctionResult.VALUE);
                    System.out.println("FN_RES:" + v);
                }
                case "buckets-get" -> {
                    // args: buckets-get <k1> <k2> — prints BUCKETS:k1=v1,k2=v2
                    String k1 = args.length >= 2 ? args[1] : "TsJavaT:b6:a";
                    String k2 = args.length >= 3 ? args[2] : "TsJavaT:b6:b";
                    var m = redisson.<String>getBuckets().get(k1, k2);
                    System.out.println("BUCKETS:" + k1 + "=" + m.get(k1) + "," + k2 + "=" + m.get(k2));
                }
                // ---- phase 7 ----
                case "bstream-set" -> {
                    // args: bstream-set <name> <text> (ASCII-safe payloads only)
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bs7:x";
                    String text = args.length >= 3 ? args[2] : "hello";
                    redisson.getBinaryStream(name).set(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    System.out.println("BS_SET");
                }
                case "bstream-get" -> {
                    // args: bstream-get <name> — prints BS_GET:<text>|BS_NULL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bs7:x";
                    byte[] v = redisson.getBinaryStream(name).get();
                    System.out.println(v == null ? "BS_NULL"
                            : ("BS_GET:" + new String(v, java.nio.charset.StandardCharsets.UTF_8)));
                }
                case "rt-fanout" -> {
                    // args: rt-fanout <topic> <timeoutMs> <resultKey> — two groups must BOTH get it
                    String topic = args.length >= 2 ? args[1] : "TsJavaT:rt7:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 12000;
                    String resultKey = args.length >= 4 ? args[3] : (topic + ":fanout-result");
                    var latch = new java.util.concurrent.CountDownLatch(2);
                    var got = java.util.Collections.synchronizedSet(new java.util.TreeSet<String>());
                    var rt = redisson.getReliableTopic(topic);
                    rt.addListener(String.class, (ch, msg) -> {
                        if (got.add("G1:" + msg)) latch.countDown();
                    });
                    rt.addListener(String.class, (ch, msg) -> {
                        if (got.add("G2:" + msg)) latch.countDown();
                    });
                    boolean ok = latch.await(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = ok ? ("FANOUT:" + String.join(",", got)) : "FANOUT_TIMEOUT";
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "mc-get" -> {
                    // args: mc-get <name> <key> — prints MC_GET:<v>|MC_NULL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc7:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String v = redisson.<String, String>getMapCache(name).get(key);
                    System.out.println(v == null ? "MC_NULL" : ("MC_GET:" + v));
                }
                case "mc-size" -> {
                    // args: mc-size <name> — prints MC_SIZE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc7:x";
                    System.out.println("MC_SIZE:" + redisson.getMapCache(name).size());
                }
                case "mm-get" -> {                    // args: mm-get <name> <key> — CACHE reader (honours TTL).
                    // NOTE: plain getSetMultimap().get() ignores the timeout
                    // ZSET by design; expiry must be read through the cache.
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mm7:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    System.out.println("MM_GET:" + new java.util.ArrayList<>(
                            redisson.<String, String>getSetMultimapCache(name).get(key)));
                }
                case "zset-readunion" -> {
                    // args: zset-readunion <a> <b> — Java readUnion (self a + b)
                    String a = args.length >= 2 ? args[1] : "TsJavaT:zs11:a";
                    String b = args.length >= 3 ? args[2] : "TsJavaT:zs11:b";
                    System.out.println("READUNION:" + new java.util.ArrayList<>(
                            redisson.<String>getScoredSortedSet(a).readUnion(b)));
                }
                case "set-union" -> {                     // args: set-union <dest> <a> <b> — Java union (self IS an input)
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:set15:d";
                    String a = args.length >= 3 ? args[2] : "TsJavaT:set15:a";
                    String b = args.length >= 4 ? args[3] : "TsJavaT:set15:b";
                    System.out.println("SET_UNION:" + redisson.<String>getSet(dest).union(a, b));
                }
                case "set-intersection" -> {              // args: set-intersection <dest> <a> <b>
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:set15:d";
                    String a = args.length >= 3 ? args[2] : "TsJavaT:set15:a";
                    String b = args.length >= 4 ? args[3] : "TsJavaT:set15:b";
                    System.out.println("SET_INTERSECTION:" + redisson.<String>getSet(dest).intersection(a, b));
                }
                case "set-diff" -> {                      // args: set-diff <dest> <a> <b>
                    String dest = args.length >= 2 ? args[1] : "TsJavaT:set15:d";
                    String a = args.length >= 3 ? args[2] : "TsJavaT:set15:a";
                    String b = args.length >= 4 ? args[3] : "TsJavaT:set15:b";
                    System.out.println("SET_DIFF:" + redisson.<String>getSet(dest).diff(a, b));
                }
                case "zset-score" -> {                    // args: zset-score <name> <member> — prints ZSCORE:<d>|ZSCORE:null
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs7:x";
                    String member = args.length >= 3 ? args[2] : "m";
                    Double s = redisson.<String>getScoredSortedSet(name).getScore(member);
                    System.out.println("ZSCORE:" + s);
                }
                case "map-get" -> {
                    // args: map-get <name> <key> — prints MAP_GET:<v>|MAP_NULL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map7:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    Object v = redisson.<String, Object>getMap(name).get(key);
                    System.out.println(v == null ? "MAP_NULL" : ("MAP_GET:" + v));
                }
                case "atomic-get" -> {
                    // args: atomic-get <name> — prints ATOMIC:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:at7:x";
                    System.out.println("ATOMIC:" + redisson.getAtomicLong(name).get());
                }
                case "atomic-set-long" -> {
                    // args: atomic-set-long <name> <value>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:at18:x";
                    long value = args.length >= 3 ? Long.parseLong(args[2]) : 0L;
                    redisson.getAtomicLong(name).set(value);
                    System.out.println("ATOMIC_SET_OK");
                }
                case "atomic-cas-long" -> {
                    // args: atomic-cas-long <name> <expect> <update>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:at18:x";
                    long expect = args.length >= 3 ? Long.parseLong(args[2]) : 0L;
                    long update = args.length >= 4 ? Long.parseLong(args[3]) : 0L;
                    System.out.println("ATOMIC_CAS:" + redisson.getAtomicLong(name).compareAndSet(expect, update));
                }
                case "bucket-set-long" -> {
                    // args: bucket-set-long <name> <value>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:long18:x";
                    long value = args.length >= 3 ? Long.parseLong(args[2]) : 0L;
                    redisson.<Long>getBucket(name).set(value);
                    System.out.println("BUCKET_LONG_SET");
                }
                case "bucket-get-long" -> {
                    // args: bucket-get-long <name>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:long18:x";
                    Object v = redisson.getBucket(name).get();
                    System.out.println(v == null ? "BUCKET_LONG_NULL" : ("BUCKET_LONG:" + v));
                }
                case "stream-add" -> {
                    // args: stream-add <stream> <field> <value> — prints STREAM_ADDED:<id>
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:st7:x";
                    String field = args.length >= 3 ? args[2] : "f";
                    String value = args.length >= 4 ? args[3] : "v";
                    var f = new java.util.LinkedHashMap<String, String>();
                    f.put(field, value);
                    var id = redisson.<String, String>getStream(stream)
                            .add(org.redisson.api.stream.StreamAddArgs.entries(f));
                    System.out.println("STREAM_ADDED:" + id);
                }
                case "stream-size" -> {                    // args: stream-size <stream> — prints STREAM_SIZE:<n>
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:st7:x";
                    System.out.println("STREAM_SIZE:" + redisson.getStream(stream).size());
                }
                case "stream-pending" -> {                // args: stream-pending <stream> <group> — prints PENDING:<n>
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:st13:x";
                    String group = args.length >= 3 ? args[2] : "g1";
                    int n = redisson.<String, String>getStream(stream).listPending(
                            group,
                            org.redisson.api.stream.StreamMessageId.MIN,
                            org.redisson.api.stream.StreamMessageId.MAX, 100).size();
                    System.out.println("PENDING:" + n);
                }
                case "stream-groups" -> {                 // args: stream-groups <stream> — prints GROUPS:[g1, g2]
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:st13:x";
                    var names = new java.util.ArrayList<String>();
                    for (var g : redisson.getStream(stream).listGroups()) names.add(g.getName());
                    System.out.println("GROUPS:" + names);
                }
                case "stream-claim" -> {                  // args: stream-claim <stream> <group> <consumer> <id>
                    String stream = args.length >= 2 ? args[1] : "TsJavaT:st13:x";
                    String group = args.length >= 3 ? args[2] : "g1";
                    String consumer = args.length >= 4 ? args[3] : "rescuer";
                    String[] parts = (args.length >= 5 ? args[4] : "0-0").split("-");
                    var claimed = redisson.<String, String>getStream(stream).claim(
                            group, consumer, 0, java.util.concurrent.TimeUnit.MILLISECONDS,
                            new org.redisson.api.stream.StreamMessageId(
                                    Long.parseLong(parts[0]), Long.parseLong(parts[1])));
                    System.out.println("CLAIMED:" + claimed.size());
                }
                case "bbq-take" -> {                    // args: bbq-take <queue> <timeoutMs> <resultKey> — bounded blocking take
                    String q = args.length >= 2 ? args[1] : "TsJavaT:bbq7:x";
                    long timeoutMs = args.length >= 3 ? Long.parseLong(args[2]) : 10000;
                    String resultKey = args.length >= 4 ? args[3] : (q + ":take-result");
                    String v = redisson.<String>getBoundedBlockingQueue(q)
                            .poll(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    String verdict = v == null ? "EMPTY" : ("GOT:" + v);
                    try {
                        redisson.<String>getBucket(resultKey).set(verdict);
                    } catch (Exception ignored) {
                    }
                    System.out.println(verdict);
                }
                case "ts-add-label" -> {
                    // args: ts-add-label <name> <timestamp> <value> <label>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ts5:x";
                    long ts = args.length >= 3 ? Long.parseLong(args[2]) : System.currentTimeMillis();
                    String value = args.length >= 4 ? args[3] : "v";
                    String label = args.length >= 5 ? args[4] : "lbl";
                    redisson.<String, String>getTimeSeries(name).add(ts, value, label);
                    System.out.println("TS_LBL_ADDED");
                }
                case "lock-hold-re" -> {
                    // args: lock-hold-re <name> <holdMs> — reentrant double-lock, prints RE_HOLDCOUNT:2
                    String lockName = args.length >= 2 ? args[1] : "TsJavaT:relock:x";
                    long holdMs = args.length >= 3 ? Long.parseLong(args[2]) : 4000;
                    var l = redisson.getLock(lockName);
                    l.lock(30, java.util.concurrent.TimeUnit.SECONDS);
                    l.lock(30, java.util.concurrent.TimeUnit.SECONDS);
                    System.out.println("RE_HOLDCOUNT:" + l.getHoldCount());
                    System.out.flush();
                    Thread.sleep(holdMs);
                    l.unlock();
                    l.unlock();
                    System.out.println("RELEASED");
                }
                case "keys-rename" -> {
                    // args: keys-rename <src> <dest>
                    String src = args.length >= 2 ? args[1] : "TsJavaT:k5:a";
                    String dest = args.length >= 3 ? args[2] : "TsJavaT:k5:b";
                    redisson.getKeys().rename(src, dest);
                    System.out.println("RENAMED");
                }
                case "keys-expire" -> {
                    // args: keys-expire <key> <ttlMs>
                    String key = args.length >= 2 ? args[1] : "TsJavaT:k5:x";
                    long ttlMs = args.length >= 3 ? Long.parseLong(args[2]) : 1500;
                    redisson.getKeys().expire(key, ttlMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                    System.out.println("EXPIRED");
                }
                case "json-set" -> {
                    // args: json-set <name> — JsonBucket.set(Map.of("a",1)) via JacksonCodec<Map>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:p35:j";
                    var jsonCodec = new org.redisson.codec.JacksonCodec<java.util.Map>(java.util.Map.class);
                    redisson.getJsonBucket(name, jsonCodec).set(java.util.Map.of("a", 1));
                    System.out.println("JSON_SET");
                }
                case "json-get" -> {
                    // args: json-get <name> — JsonBucket.get() through JacksonCodec<Map>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:p35:j";
                    var jsonCodec = new org.redisson.codec.JacksonCodec<java.util.Map>(java.util.Map.class);
                    Object v = redisson.getJsonBucket(name, jsonCodec).get();
                    System.out.println("JSON_GET:" + v);
                }
                case "vector-add" -> {
                    // args: vector-add <name> <element> <csvDoubles>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:vec5:x";
                    String element = args.length >= 3 ? args[2] : "e1";
                    String csv = args.length >= 4 ? args[3] : "1,0,0";
                    String[] parts = csv.split(",");
                    Double[] vec = new Double[parts.length];
                    for (int i = 0; i < parts.length; i++) vec[i] = Double.parseDouble(parts[i].trim());
                    redisson.getVectorSet(name).add(
                            org.redisson.api.vector.VectorAddArgs.element(element).vector(vec));
                    System.out.println("VEC_ADDED");
                }
                case "vector-info" -> {
                    // args: vector-info <name> — prints VEC_SIZE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:vec5:x";
                    System.out.println("VEC_SIZE:" + redisson.getVectorSet(name).size());
                }
                // ---- phase 8 ----
                case "sc-contains" -> {
                    // args: sc-contains <name> <value> — prints SC_HAS:true|false
                    String name = args.length >= 2 ? args[1] : "TsJavaT:sc8:x";
                    String value = args.length >= 3 ? args[2] : "v";
                    System.out.println("SC_HAS:" + redisson.<String>getSetCache(name).contains(value));
                }
                case "bucket-get" -> {
                    // args: bucket-get <name> — prints BUCKET_GET:<v>|BUCKET_NULL
                    String name = args.length >= 2 ? args[1] : "TsJavaT:b8:x";
                    Object v = redisson.<Object>getBucket(name).get();
                    System.out.println(v == null ? "BUCKET_NULL" : ("BUCKET_GET:" + v));
                }
                case "deque-read" -> {
                    // args: deque-read <name> — prints DEQUE:[a, b]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:dq8:x";
                    System.out.println("DEQUE:" + new java.util.ArrayList<>(
                            redisson.<String>getDeque(name).readAll()));
                }
                case "list-read" -> {
                    // args: list-read <name> — prints LIST:[a, b]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:li8:x";
                    System.out.println("LIST:" + new java.util.ArrayList<>(
                            redisson.<String>getList(name).readAll()));
                }
                case "set-read" -> {
                    // args: set-read <name> — prints SET:[...] sorted for determinism
                    String name = args.length >= 2 ? args[1] : "TsJavaT:set8:x";
                    var members = new java.util.ArrayList<>(
                            redisson.<String>getSet(name).readAll());
                    java.util.Collections.sort(members);
                    System.out.println("SET:" + members);
                }
                case "atomicdouble-add" -> {
                    // args: atomicdouble-add <name> <delta>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ad8:x";
                    double delta = args.length >= 3 ? Double.parseDouble(args[2]) : 0.1;
                    redisson.getAtomicDouble(name).addAndGet(delta);
                    System.out.println("AD_ADDED");
                }
                case "atomicdouble-get" -> {
                    // args: atomicdouble-get <name> — prints AD_GET:<d>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ad8:x";
                    System.out.println("AD_GET:" + redisson.getAtomicDouble(name).get());
                }
                case "lmm-get" -> {
                    // args: lmm-get <name> <key> — LIST variant, prints LMM_GET:[...]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:lmm8:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    System.out.println("LMM_GET:" + new java.util.ArrayList<>(
                            redisson.<String, String>getListMultimap(name).get(key)));
                }
                // ---- phase 19 ----
                case "map-lock-name" -> {
                    // args: map-lock-name <name> <key> — six Java-derived companion names
                    String name = args.length >= 2 ? args[1] : "TsJavaT:s19:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    var map = redisson.<String, String>getMap(name);
                    System.out.println("SYNC:" + map.getLock(key).getName()
                            + "|" + map.getFairLock(key).getName()
                            + "|" + map.getReadWriteLock(key).readLock().getName()
                            + "|" + map.getSemaphore(key).getName()
                            + "|" + map.getCountDownLatch(key).getName()
                            + "|" + map.getPermitExpirableSemaphore(key).getName());
                }
                case "map-lock-try" -> {
                    // args: map-lock-try <name> <key> — prints ACQUIRED|CONTENDED
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ml19:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    var l = redisson.<String, String>getMap(name).getLock(key);
                    if (l.tryLock()) {
                        System.out.println("ACQUIRED");
                        l.unlock();
                    } else {
                        System.out.println("CONTENDED");
                    }
                }
                case "map-lock-hold" -> {
                    // args: map-lock-hold <name> <key> <holdMs> — HELD ... RELEASED
                    String name = args.length >= 2 ? args[1] : "TsJavaT:ml19:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    long holdMs = args.length >= 4 ? Long.parseLong(args[3]) : 4000;
                    var l = redisson.<String, String>getMap(name).getLock(key);
                    l.lock(30, java.util.concurrent.TimeUnit.SECONDS);
                    System.out.println("HELD");
                    System.out.flush();
                    Thread.sleep(holdMs);
                    l.unlock();
                    System.out.println("RELEASED");
                }
                case "mapcache-put" -> {
                    // args: mapcache-put <name> <key> <value> — prints MC_PUT
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc19:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    redisson.<String, String>getMapCache(name).put(key, value);
                    System.out.println("MC_PUT");
                }
                case "mapcache-remove" -> {
                    // args: mapcache-remove <name> <key> — prints MC_REMOVED
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc19:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    redisson.<String, String>getMapCache(name).remove(key);
                    System.out.println("MC_REMOVED");
                }
                case "mapcache-listen" -> {
                    // args: mapcache-listen <name> <listenMs> — prints MCE:<type>:<key>:<value>:<old>
                    // for every event seen, then MC_LISTEN_DONE
                    String name = args.length >= 2 ? args[1] : "TsJavaT:mc19:x";
                    long listenMs = args.length >= 3 ? Long.parseLong(args[2]) : 3000;
                    var cache = redisson.<String, String>getMapCache(name);
                    var events = java.util.Collections.synchronizedList(new java.util.ArrayList<String>());
                    cache.addListener(new org.redisson.api.map.event.EntryCreatedListener<String, String>() {
                        @Override
                        public void onCreated(org.redisson.api.map.event.EntryEvent<String, String> e) {
                            events.add("CREATED:" + e.getKey() + ":" + e.getValue() + ":null");
                        }
                    });
                    cache.addListener(new org.redisson.api.map.event.EntryUpdatedListener<String, String>() {
                        @Override
                        public void onUpdated(org.redisson.api.map.event.EntryEvent<String, String> e) {
                            events.add("UPDATED:" + e.getKey() + ":" + e.getValue() + ":" + e.getOldValue());
                        }
                    });
                    cache.addListener(new org.redisson.api.map.event.EntryRemovedListener<String, String>() {
                        @Override
                        public void onRemoved(org.redisson.api.map.event.EntryEvent<String, String> e) {
                            events.add("REMOVED:" + e.getKey() + ":null:" + e.getOldValue());
                        }
                    });
                    Thread.sleep(listenMs);
                    for (String ev : events) {
                        System.out.println("MCE:" + ev);
                    }
                    System.out.println("MC_LISTEN_DONE");
                }
                // ---- phase 20: RMap HSCAN/HSTRLEN, RScoredSortedSet score ops, RBitSet BITFIELD/BITOP, RGeo search ----
                case "map-put" -> {
                    // args: map-put <name> <key> <value> — prints MAP_PUT
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    String value = args.length >= 4 ? args[3] : "v";
                    redisson.<String, String>getMap(name).put(key, value);
                    System.out.println("MAP_PUT");
                }
                case "map-value-size" -> {
                    // args: map-value-size <name> <key> — prints MAP_VSIZE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    String key = args.length >= 3 ? args[2] : "k";
                    System.out.println("MAP_VSIZE:" + redisson.<String, String>getMap(name).valueSize(key));
                }
                case "map-random-keys" -> {
                    // args: map-random-keys <name> <count> — prints MAP_RKEYS:sorted
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    int count = args.length >= 3 ? Integer.parseInt(args[2]) : 1;
                    var keys = new java.util.TreeSet<>(redisson.<String, String>getMap(name).randomKeys(count));
                    System.out.println("MAP_RKEYS:" + keys);
                }
                case "map-random-entries" -> {
                    // args: map-random-entries <name> <count> — prints MAP_RENTRIES:k=v;k=v
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    int count = args.length >= 3 ? Integer.parseInt(args[2]) : 1;
                    var entries = new java.util.TreeMap<>(redisson.<String, String>getMap(name).randomEntries(count));
                    StringBuilder sb = new StringBuilder();
                    entries.forEach((k, v) -> sb.append(sb.length() == 0 ? "" : ";").append(k).append("=").append(v));
                    System.out.println("MAP_RENTRIES:" + sb);
                }
                case "map-scan-keys" -> {
                    // args: map-scan-keys <name> <pattern> <count> — reads the Java keySet HSCAN view
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    String pattern = args.length >= 3 ? args[2] : "*";
                    int count = args.length >= 4 ? Integer.parseInt(args[3]) : 10;
                    var keys = new java.util.TreeSet<String>();
                    for (String k : redisson.<String, String>getMap(name).keySet(pattern, count)) keys.add(k);
                    System.out.println("MAP_SCAN_KEYS:" + keys);
                }
                case "map-scan-entries" -> {
                    // args: map-scan-entries <name> <pattern> <count> — reads the Java entrySet HSCAN view
                    String name = args.length >= 2 ? args[1] : "TsJavaT:map20:x";
                    String pattern = args.length >= 3 ? args[2] : "*";
                    int count = args.length >= 4 ? Integer.parseInt(args[3]) : 10;
                    var entries = new java.util.TreeMap<String, String>();
                    for (var e : redisson.<String, String>getMap(name).entrySet(pattern, count)) {
                        entries.put(e.getKey(), e.getValue());
                    }
                    StringBuilder sb = new StringBuilder();
                    entries.forEach((k, v) -> sb.append(sb.length() == 0 ? "" : ";").append(k).append("=").append(v));
                    System.out.println("MAP_SCAN_ENTRIES:" + sb);
                }
                case "zset-addscore" -> {
                    // args: zset-addscore <name> <member> <delta> — prints ZSET_ADDSCORE:<score>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    String member = args.length >= 3 ? args[2] : "m";
                    double delta = args.length >= 4 ? Double.parseDouble(args[3]) : 1;
                    System.out.println("ZSET_ADDSCORE:" + redisson.<String>getScoredSortedSet(name).addScore(member, delta));
                }
                case "zset-first-last" -> {
                    // args: zset-first-last <name> — prints ZSET_FIRST_LAST:<first>|<last>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    var z = redisson.<String>getScoredSortedSet(name);
                    System.out.println("ZSET_FIRST_LAST:" + z.firstScore() + "|" + z.lastScore());
                }
                case "zset-rank" -> {
                    // args: zset-rank <name> <member> — prints ZSET_RANKS:<rank>|<revRank>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    String member = args.length >= 3 ? args[2] : "m";
                    var z = redisson.<String>getScoredSortedSet(name);
                    System.out.println("ZSET_RANKS:" + z.rank(member) + "|" + z.revRank(member));
                }
                case "zset-count" -> {
                    // args: zset-count <name> <start> <startIncl> <end> <endIncl> — prints ZSET_COUNT:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    double start = args.length >= 3 ? Double.parseDouble(args[2]) : 0;
                    boolean startIncl = args.length < 4 || Boolean.parseBoolean(args[3]);
                    double end = args.length >= 5 ? Double.parseDouble(args[4]) : 0;
                    boolean endIncl = args.length < 6 || Boolean.parseBoolean(args[5]);
                    System.out.println("ZSET_COUNT:" + redisson.<String>getScoredSortedSet(name)
                            .count(start, startIncl, end, endIncl));
                }
                case "zset-rev" -> {
                    // args: zset-rev <name> <startIndex> <endIndex> — prints ZSET_REV:[...]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    int start = args.length >= 3 ? Integer.parseInt(args[2]) : 0;
                    int end = args.length >= 4 ? Integer.parseInt(args[3]) : -1;
                    System.out.println("ZSET_REV:" + new java.util.ArrayList<>(
                            redisson.<String>getScoredSortedSet(name).valueRangeReversed(start, end)));
                }
                case "zset-rev-score" -> {
                    // args: zset-rev-score <name> <start> <startIncl> <end> <endIncl> — prints ZSET_REV_SCORE:[...]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    double start = args.length >= 3 ? Double.parseDouble(args[2]) : 0;
                    boolean startIncl = args.length < 4 || Boolean.parseBoolean(args[3]);
                    double end = args.length >= 5 ? Double.parseDouble(args[4]) : 0;
                    boolean endIncl = args.length < 6 || Boolean.parseBoolean(args[5]);
                    System.out.println("ZSET_REV_SCORE:" + new java.util.ArrayList<>(
                            redisson.<String>getScoredSortedSet(name).valueRangeReversed(start, startIncl, end, endIncl)));
                }
                case "zset-entry-rev" -> {
                    // args: zset-entry-rev <name> <startIndex> <endIndex> — prints ZSET_ENTRY_REV:v=score;...
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    int start = args.length >= 3 ? Integer.parseInt(args[2]) : 0;
                    int end = args.length >= 4 ? Integer.parseInt(args[3]) : -1;
                    StringBuilder sb = new StringBuilder();
                    for (var e : redisson.<String>getScoredSortedSet(name).entryRangeReversed(start, end)) {
                        sb.append(sb.length() == 0 ? "" : ";").append(e.getValue()).append("=").append(e.getScore());
                    }
                    System.out.println("ZSET_ENTRY_REV:" + sb);
                }
                case "zset-entry-range" -> {
                    // args: zset-entry-range <name> <start> <startIncl> <end> <endIncl> — prints ZSET_ENTRY_RANGE:v=score;...
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    double start = args.length >= 3 ? Double.parseDouble(args[2]) : 0;
                    boolean startIncl = args.length < 4 || Boolean.parseBoolean(args[3]);
                    double end = args.length >= 5 ? Double.parseDouble(args[4]) : 0;
                    boolean endIncl = args.length < 6 || Boolean.parseBoolean(args[5]);
                    StringBuilder sb = new StringBuilder();
                    for (var e : redisson.<String>getScoredSortedSet(name).entryRange(start, startIncl, end, endIncl)) {
                        sb.append(sb.length() == 0 ? "" : ";").append(e.getValue()).append("=").append(e.getScore());
                    }
                    System.out.println("ZSET_ENTRY_RANGE:" + sb);
                }
                case "zset-remove-score" -> {
                    // args: zset-remove-score <name> <start> <startIncl> <end> <endIncl> — prints ZSET_RM_SCORE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    double start = args.length >= 3 ? Double.parseDouble(args[2]) : 0;
                    boolean startIncl = args.length < 4 || Boolean.parseBoolean(args[3]);
                    double end = args.length >= 5 ? Double.parseDouble(args[4]) : 0;
                    boolean endIncl = args.length < 6 || Boolean.parseBoolean(args[5]);
                    System.out.println("ZSET_RM_SCORE:" + redisson.<String>getScoredSortedSet(name)
                            .removeRangeByScore(start, startIncl, end, endIncl));
                }
                case "zset-remove-rank" -> {
                    // args: zset-remove-rank <name> <startIndex> <endIndex> — prints ZSET_RM_RANK:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:zs20:x";
                    int start = args.length >= 3 ? Integer.parseInt(args[2]) : 0;
                    int end = args.length >= 4 ? Integer.parseInt(args[3]) : 0;
                    System.out.println("ZSET_RM_RANK:" + redisson.<String>getScoredSortedSet(name).removeRangeByRank(start, end));
                }
                case "bitset-size" -> {
                    // args: bitset-size <name> — prints BITS_SIZE:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    System.out.println("BITS_SIZE:" + redisson.getBitSet(name).size());
                }
                case "bitset-length" -> {
                    // args: bitset-length <name> — prints BITS_LEN:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    System.out.println("BITS_LEN:" + redisson.getBitSet(name).length());
                }
                case "bitset-cardinality" -> {
                    // args: bitset-cardinality <name> — prints BITS_CARD:<n>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    System.out.println("BITS_CARD:" + redisson.getBitSet(name).cardinality());
                }
                case "bitset-setrange" -> {
                    // args: bitset-setrange <name> <from> <to> <0|1> — Java set(from, to, value): `to` EXCLUSIVE
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    long from = args.length >= 3 ? Long.parseLong(args[2]) : 0;
                    long to = args.length >= 4 ? Long.parseLong(args[3]) : 8;
                    boolean value = args.length < 5 || Boolean.parseBoolean(args[4]) || "1".equals(args[4]);
                    redisson.getBitSet(name).set(from, to, value);
                    System.out.println("BITS_SETRANGE");
                }
                case "bitset-clearrange" -> {
                    // args: bitset-clearrange <name> <from> <to> — Java clear(from, to): `to` EXCLUSIVE
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    long from = args.length >= 3 ? Long.parseLong(args[2]) : 0;
                    long to = args.length >= 4 ? Long.parseLong(args[3]) : 8;
                    redisson.getBitSet(name).clear(from, to);
                    System.out.println("BITS_CLEARRANGE");
                }
                case "bitset-typed-set" -> {
                    // args: bitset-typed-set <name> <u|i> <size> <offset> <value> — prints BITS_TSET:<old>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    boolean unsigned = args.length >= 3 && args[2].startsWith("u");
                    int size = args.length >= 4 ? Integer.parseInt(args[3]) : 8;
                    long offset = args.length >= 5 ? Long.parseLong(args[4]) : 0;
                    long value = args.length >= 6 ? Long.parseLong(args[5]) : 0;
                    var bs = redisson.getBitSet(name);
                    long old = unsigned ? bs.setUnsigned(size, offset, value) : bs.setSigned(size, offset, value);
                    System.out.println("BITS_TSET:" + old);
                }
                case "bitset-typed-get" -> {
                    // args: bitset-typed-get <name> <u|i> <size> <offset> — prints BITS_TGET:<v>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    boolean unsigned = args.length >= 3 && args[2].startsWith("u");
                    int size = args.length >= 4 ? Integer.parseInt(args[3]) : 8;
                    long offset = args.length >= 5 ? Long.parseLong(args[4]) : 0;
                    var bs = redisson.getBitSet(name);
                    long v = unsigned ? bs.getUnsigned(size, offset) : bs.getSigned(size, offset);
                    System.out.println("BITS_TGET:" + v);
                }
                case "bitset-typed-incr" -> {
                    // args: bitset-typed-incr <name> <u|i> <size> <offset> <increment> — prints BITS_TINCR:<v>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    boolean unsigned = args.length >= 3 && args[2].startsWith("u");
                    int size = args.length >= 4 ? Integer.parseInt(args[3]) : 8;
                    long offset = args.length >= 5 ? Long.parseLong(args[4]) : 0;
                    long incr = args.length >= 6 ? Long.parseLong(args[5]) : 1;
                    var bs = redisson.getBitSet(name);
                    long v = unsigned ? bs.incrementAndGetUnsigned(size, offset, incr) : bs.incrementAndGetSigned(size, offset, incr);
                    System.out.println("BITS_TINCR:" + v);
                }
                case "bitset-long-set" -> {
                    // args: bitset-long-set <name> <offset> <value> — prints BITS_LSET:<old>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    long offset = args.length >= 3 ? Long.parseLong(args[2]) : 0;
                    long value = args.length >= 4 ? Long.parseLong(args[3]) : 0;
                    System.out.println("BITS_LSET:" + redisson.getBitSet(name).setLong(offset, value));
                }
                case "bitset-long-get" -> {
                    // args: bitset-long-get <name> <offset> — prints BITS_LGET:<v>
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    long offset = args.length >= 3 ? Long.parseLong(args[2]) : 0;
                    System.out.println("BITS_LGET:" + redisson.getBitSet(name).getLong(offset));
                }
                case "bitset-bitfield" -> {
                    // args: bitset-bitfield <name> <spec> — spec = op[:overflow]|GET:<u|i><size>:<offset>|SET:...:<value>|INCRBY:...
                    // comma-separated operations, e.g. "SET:u8:0:5,GET:u8:0" — prints BITS_BITFIELD:[v1, v2]
                    String name = args.length >= 2 ? args[1] : "TsJavaT:bits20:x";
                    String spec = args.length >= 3 ? args[2] : "GET:u8:0";
                    var bf = org.redisson.api.bitset.BitFieldArgs.create();
                    for (String opSpec : spec.split(",")) {
                        String[] p = opSpec.split(":");
                        String op = p[0].toUpperCase();
                        switch (op) {
                            case "OVERFLOW" -> bf = bf.overflow(org.redisson.api.bitset.BitFieldOverflow.valueOf(p[1].toUpperCase()));
                            case "GET" -> {
                                boolean unsigned = p[1].startsWith("u");
                                int size = Integer.parseInt(p[1].substring(1));
                                long offset = Long.parseLong(p[2]);
                                bf = unsigned ? bf.getUnsigned(size, org.redisson.api.bitset.BitOffset.bit(offset))
                                        : bf.getSigned(size, org.redisson.api.bitset.BitOffset.bit(offset));
                            }
                            case "SET" -> {
                                boolean unsigned = p[1].startsWith("u");
                                int size = Integer.parseInt(p[1].substring(1));
                                bf = unsigned
                                        ? bf.setUnsigned(size, org.redisson.api.bitset.BitOffset.bit(Long.parseLong(p[2])), Long.parseLong(p[3]))
                                        : bf.setSigned(size, org.redisson.api.bitset.BitOffset.bit(Long.parseLong(p[2])), Long.parseLong(p[3]));
                            }
                            case "INCRBY" -> {
                                boolean unsigned = p[1].startsWith("u");
                                int size = Integer.parseInt(p[1].substring(1));
                                bf = unsigned
                                        ? bf.incrementUnsignedBy(size, org.redisson.api.bitset.BitOffset.bit(Long.parseLong(p[2])), Long.parseLong(p[3]))
                                        : bf.incrementSignedBy(size, org.redisson.api.bitset.BitOffset.bit(Long.parseLong(p[2])), Long.parseLong(p[3]));
                            }
                            default -> throw new IllegalArgumentException("bad bitfield op: " + op);
                        }
                    }
                    System.out.println("BITS_BITFIELD:" + redisson.getBitSet(name)
                            .bitField((org.redisson.api.bitset.BitFieldArgs) bf));
                }
                case "bitset-op" -> {
                    // args: bitset-op <op> <name> [other1 other2 ...] — op: or|and|xor|not|diff|diff1|andor|one
                    String op = args.length >= 2 ? args[1] : "or";
                    String name = args.length >= 3 ? args[2] : "TsJavaT:bits20:x";
                    String[] others = args.length > 3 ? java.util.Arrays.copyOfRange(args, 3, args.length) : new String[0];
                    var bs = redisson.getBitSet(name);
                    long n = switch (op.toLowerCase()) {
                        case "or" -> bs.or(others);
                        case "and" -> bs.and(others);
                        case "xor" -> bs.xor(others);
                        case "not" -> bs.not();
                        case "diff" -> bs.diff(others);
                        case "diff1", "diffinverse" -> bs.diffInverse(others);
                        case "andor" -> bs.andOr(others);
                        case "one", "setexclusive" -> bs.setExclusive(others);
                        default -> throw new IllegalArgumentException("unknown bitset op: " + op);
                    };
                    System.out.println("BITS_OP:" + n);
                }
                case "geo-search-radius" -> {
                    // args: geo-search-radius <name> <lon> <lat> <radius> <unit> [count] [order] — prints GEO_SEARCH:sorted
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    double radius = args.length >= 5 ? Double.parseDouble(args[4]) : 200;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 6 ? args[5] : "km");
                    var search = org.redisson.api.geo.GeoSearchArgs.from(lon, lat).radius(radius, unit);
                    int count = args.length >= 7 ? Integer.parseInt(args[6]) : -1;
                    if (count > 0) search = search.count(count);
                    if (args.length >= 8) search = search.order(org.redisson.api.geo.GeoOrder.valueOf(args[7].toUpperCase()));
                    var members = new java.util.TreeSet<>(redisson.<String>getGeo(name).search(search));
                    System.out.println("GEO_SEARCH:" + members);
                }
                case "geo-search-member" -> {
                    // args: geo-search-member <name> <member> <radius> <unit> [count] — prints GEO_SEARCH_MEMBER:sorted
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    String member = args.length >= 3 ? args[2] : "m";
                    double radius = args.length >= 4 ? Double.parseDouble(args[3]) : 200;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 5 ? args[4] : "km");
                    var search = org.redisson.api.geo.GeoSearchArgs.from(member).radius(radius, unit);
                    int count = args.length >= 6 ? Integer.parseInt(args[5]) : -1;
                    if (count > 0) search = search.count(count);
                    var members = new java.util.TreeSet<>(redisson.<String>getGeo(name).search(search));
                    System.out.println("GEO_SEARCH_MEMBER:" + members);
                }
                case "geo-search-box" -> {
                    // args: geo-search-box <name> <lon> <lat> <width> <height> <unit> [count] [order] — prints GEO_SEARCH:sorted
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    double width = args.length >= 5 ? Double.parseDouble(args[4]) : 400;
                    double height = args.length >= 6 ? Double.parseDouble(args[5]) : 400;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 7 ? args[6] : "km");
                    var search = org.redisson.api.geo.GeoSearchArgs.from(lon, lat).box(width, height, unit);
                    int count = args.length >= 8 ? Integer.parseInt(args[7]) : -1;
                    if (count > 0) search = search.count(count);
                    if (args.length >= 9) search = search.order(org.redisson.api.geo.GeoOrder.valueOf(args[8].toUpperCase()));
                    var members = new java.util.TreeSet<>(redisson.<String>getGeo(name).search(search));
                    System.out.println("GEO_SEARCH_BOX:" + members);
                }
                case "geo-search-dist" -> {
                    // args: geo-search-dist <name> <lon> <lat> <radius> <unit> — prints GEO_SEARCH_DIST:m=d;...
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    double radius = args.length >= 5 ? Double.parseDouble(args[4]) : 200;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 6 ? args[5] : "km");
                    var found = redisson.<String>getGeo(name)
                            .searchWithDistance(org.redisson.api.geo.GeoSearchArgs.from(lon, lat).radius(radius, unit));
                    var sorted = new java.util.TreeMap<String, Double>(found);
                    StringBuilder sb = new StringBuilder();
                    sorted.forEach((m, d) -> sb.append(sb.length() == 0 ? "" : ";").append(m).append("=").append(d));
                    System.out.println("GEO_SEARCH_DIST:" + sb);
                }
                case "geo-search-dist-box" -> {
                    // args: geo-search-dist-box <name> <lon> <lat> <width> <height> <unit> — GEOSEARCH WITHDIST path
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    double width = args.length >= 5 ? Double.parseDouble(args[4]) : 400;
                    double height = args.length >= 6 ? Double.parseDouble(args[5]) : 400;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 7 ? args[6] : "km");
                    var found = redisson.<String>getGeo(name)
                            .searchWithDistance(org.redisson.api.geo.GeoSearchArgs.from(lon, lat).box(width, height, unit));
                    var sorted = new java.util.TreeMap<String, Double>(found);
                    StringBuilder sb = new StringBuilder();
                    sorted.forEach((m, d) -> sb.append(sb.length() == 0 ? "" : ";").append(m).append("=").append(d));
                    System.out.println("GEO_SEARCH_DIST_BOX:" + sb);
                }
                case "geo-search-pos" -> {
                    // args: geo-search-pos <name> <lon> <lat> <radius> <unit> — prints GEO_SEARCH_POS:m=[lon, lat];...
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    double radius = args.length >= 5 ? Double.parseDouble(args[4]) : 200;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 6 ? args[5] : "km");
                    var found = redisson.<String>getGeo(name)
                            .searchWithPosition(org.redisson.api.geo.GeoSearchArgs.from(lon, lat).radius(radius, unit));
                    var sorted = new java.util.TreeMap<String, org.redisson.api.geo.GeoPosition>(found);
                    StringBuilder sb = new StringBuilder();
                    sorted.forEach((m, p) -> sb.append(sb.length() == 0 ? "" : ";")
                            .append(m).append("=").append(p == null ? "null" : ("[" + p.getLongitude() + "," + p.getLatitude() + "]")));
                    System.out.println("GEO_SEARCH_POS:" + sb);
                }
                case "geo-store-search" -> {
                    // args: geo-store-search <name> <dest> <lon> <lat> <width> <height> <unit> — GEOSEARCHSTORE
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    String dest = args.length >= 3 ? args[2] : "TsJavaT:geo20:dest";
                    double lon = args.length >= 4 ? Double.parseDouble(args[3]) : 13.361389;
                    double lat = args.length >= 5 ? Double.parseDouble(args[4]) : 38.115556;
                    double width = args.length >= 6 ? Double.parseDouble(args[5]) : 400;
                    double height = args.length >= 7 ? Double.parseDouble(args[6]) : 400;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 8 ? args[7] : "km");
                    System.out.println("GEO_STORE:" + redisson.<String>getGeo(name)
                            .storeSearchTo(dest, org.redisson.api.geo.GeoSearchArgs.from(lon, lat).box(width, height, unit)));
                }
                case "geo-store-sorted" -> {
                    // args: geo-store-sorted <name> <dest> <lon> <lat> <radius> <unit> — legacy GEORADIUS ... STOREDIST
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    String dest = args.length >= 3 ? args[2] : "TsJavaT:geo20:dest";
                    double lon = args.length >= 4 ? Double.parseDouble(args[3]) : 13.361389;
                    double lat = args.length >= 5 ? Double.parseDouble(args[4]) : 38.115556;
                    double radius = args.length >= 6 ? Double.parseDouble(args[5]) : 200;
                    org.redisson.api.geo.GeoUnit unit = geoUnit(args.length >= 7 ? args[6] : "km");
                    System.out.println("GEO_STORE_SORTED:" + redisson.<String>getGeo(name)
                            .storeSortedSearchTo(dest, org.redisson.api.geo.GeoSearchArgs.from(lon, lat).radius(radius, unit)));
                }
                case "geo-add-if-exists" -> {
                    // args: geo-add-if-exists <name> <lon> <lat> <member> — prints GEO_AIE:true|false
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    String member = args.length >= 5 ? args[4] : "Palermo";
                    System.out.println("GEO_AIE:" + redisson.<String>getGeo(name).addIfExists(lon, lat, member));
                }
                case "geo-add-if-exists-xx" -> {
                    // args: geo-add-if-exists-xx <name> <lon> <lat> <member> — addIfExists(GeoEntry...) → GEOADD XX
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    double lon = args.length >= 3 ? Double.parseDouble(args[2]) : 13.361389;
                    double lat = args.length >= 4 ? Double.parseDouble(args[3]) : 38.115556;
                    String member = args.length >= 5 ? args[4] : "Palermo";
                    System.out.println("GEO_AIE_XX:" + redisson.<String>getGeo(name).addIfExists(
                            new org.redisson.api.geo.GeoEntry(lon, lat, member)));
                }
                case "geo-pos" -> {
                    // args: geo-pos <name> <member> — prints GEO_POS:[lon, lat]|GEO_POS:absent
                    String name = args.length >= 2 ? args[1] : "TsJavaT:geo20:x";
                    String member = args.length >= 3 ? args[2] : "m";
                    var positions = redisson.<String>getGeo(name).pos(member);
                    var p = positions.get(member);
                    System.out.println("GEO_POS:" + (p == null ? "absent" : ("[" + p.getLongitude() + "," + p.getLatitude() + "]")));
                }
                default -> throw new IllegalArgumentException("unknown mode: " + mode
                        + "; usage: TsProbe [clean|write-java|read-ts|roundtrip|lock-try|lock-hold|rate-try|rate-set|sem-try|repl] [...]");
        }
    }

    private TsProbe() {
    }

    /** Maps m/km/mi/ft (or enum-ish names) to the 4.6.1 GeoUnit. */
    private static org.redisson.api.geo.GeoUnit geoUnit(String s) {
        return switch (s.toLowerCase()) {
            case "m", "meters" -> org.redisson.api.geo.GeoUnit.METERS;
            case "km", "kilometers" -> org.redisson.api.geo.GeoUnit.KILOMETERS;
            case "mi", "miles" -> org.redisson.api.geo.GeoUnit.MILES;
            case "ft", "feet" -> org.redisson.api.geo.GeoUnit.FEET;
            default -> throw new IllegalArgumentException("unknown geo unit: " + s);
        };
    }
}
