package dev.minilog.bench;

import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.client.RecordMetadata;
import dev.minilog.common.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark and chaos tool. Every mode starts a fresh local cluster (1 controller + 3 broker JVMs
 * on localhost), runs, prints {@code RESULT key=value ...} lines, and stops the cluster.
 *
 * <pre>
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench produce  --size 100 --acks -1 --batch 16384 --messages 1000000
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench consume  --size 100 --messages 1000000
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench e2e      --rate 5000 --seconds 20 --acks -1
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench fsync    --policy every-1 --acks 1 --messages 100000
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench failover --iterations 20
 * java -cp build/libs/minilog.jar dev.minilog.bench.Bench chaos    --seconds 120
 * </pre>
 */
public final class Bench {
    private final Map<String, String> opts = new HashMap<>();
    private final String mode;

    private Bench(String[] args) {
        mode = args.length > 0 ? args[0] : "help";
        for (int i = 1; i + 1 < args.length; i += 2) {
            opts.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
    }

    public static void main(String[] args) throws Exception {
        new Bench(args).run();
        System.exit(0);
    }

    private String opt(String key, String def) {
        return opts.getOrDefault(key, def);
    }

    private int intOpt(String key, int def) {
        return Integer.parseInt(opt(key, String.valueOf(def)));
    }

    private void run() throws Exception {
        System.out.printf("# minilog bench mode=%s opts=%s started=%s java=%s cores=%d%n", mode, opts,
                LocalDateTime.now().withNano(0), System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors());
        switch (mode) {
            case "produce" -> produce();
            case "consume" -> consume();
            case "e2e" -> endToEndLatency();
            case "fsync" -> fsync();
            case "failover" -> failover();
            case "chaos" -> chaos();
            default -> System.out.println("modes: produce consume e2e fsync failover chaos (see class javadoc)");
        }
    }

    private LocalCluster cluster(List<String> brokerOverrides, List<String> controllerOverrides) {
        return new LocalCluster(Path.of(opt("dir", "bench-data/" + mode)), intOpt("port", 19090), 3,
                brokerOverrides, controllerOverrides);
    }

    private static byte[] payload(int size, Random rnd) {
        byte[] b = new byte[size];
        rnd.nextBytes(b);
        return b;
    }

    // ================================================================== throughput

    /** Sends {@code messages} records as fast as the producer allows; reports msgs/s and MiB/s. */
    private void produce() throws Exception {
        produceRun(List.of(), "produce");
    }

    private void fsync() throws Exception {
        String policy = opt("policy", "none");
        List<String> overrides = switch (policy) {
            case "none" -> List.of();
            case "every-1" -> List.of("log.flush.interval.messages=1");
            case "every-1000" -> List.of("log.flush.interval.messages=1000");
            case "interval-1000ms" -> List.of("log.flush.interval.ms=1000");
            default -> throw new IllegalArgumentException("policy: none | every-1 | every-1000 | interval-1000ms");
        };
        produceRun(overrides, "fsync policy=" + policy);
    }

    private void produceRun(List<String> brokerOverrides, String label) throws Exception {
        int size = intOpt("size", 100);
        int acks = intOpt("acks", -1);
        int batch = intOpt("batch", 16384);
        int linger = intOpt("linger", 5);
        int messages = intOpt("messages", 1_000_000);
        int partitions = intOpt("partitions", 3);
        int warmup = Math.min(messages / 10, 50_000);
        try (LocalCluster cluster = cluster(brokerOverrides, List.of()).start()) {
            cluster.createTopic("bench", partitions, 3);
            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(acks).withBatching(batch, linger);
            Random rnd = new Random(42);
            byte[][] values = new byte[64][];
            for (int i = 0; i < values.length; i++) {
                values[i] = payload(size, rnd);
            }
            try (Producer producer = new Producer(cfg)) {
                for (int i = 0; i < warmup; i++) {
                    producer.send("bench", null, values[i & 63]);
                }
                producer.flush();
                AtomicInteger failed = new AtomicInteger();
                long start = System.nanoTime();
                for (int i = 0; i < messages; i++) {
                    producer.send("bench", null, values[i & 63]).whenComplete((m, e) -> {
                        if (e != null) {
                            failed.incrementAndGet();
                        }
                    });
                }
                producer.flush();
                double secs = (System.nanoTime() - start) / 1e9;
                System.out.printf("RESULT %s size=%d acks=%d batch=%d linger=%d messages=%d failed=%d seconds=%.3f "
                                + "msgs_per_sec=%.0f mib_per_sec=%.2f%n", label, size, acks, batch, linger, messages,
                        failed.get(), secs, messages / secs, messages * (double) size / (1 << 20) / secs);
            }
            cluster.deleteData();
        }
    }

    /** Fills the topic, then measures how fast one consumer reads it all back from offset 0. */
    private void consume() throws Exception {
        int size = intOpt("size", 100);
        int messages = intOpt("messages", 1_000_000);
        int partitions = intOpt("partitions", 3);
        try (LocalCluster cluster = cluster(List.of(), List.of()).start()) {
            cluster.createTopic("bench", partitions, 3);
            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1).withBatching(131072, 5);
            byte[] value = payload(size, new Random(1));
            try (Producer producer = new Producer(cfg)) {
                for (int i = 0; i < messages; i++) {
                    producer.send("bench", null, value);
                }
                producer.flush();
            }
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                tps.add(new TopicPartition("bench", p));
            }
            ConsumerConfig ccfg = ConsumerConfig.defaults(cluster.bootstrap(), null).withFetch(100, intOpt("fetch-bytes", 1 << 20));
            try (Consumer consumer = new Consumer(ccfg)) {
                consumer.assign(tps);
                long count = 0;
                long bytes = 0;
                long start = System.nanoTime();
                while (count < messages) {
                    for (ConsumerRecord r : consumer.poll(Duration.ofMillis(500))) {
                        count++;
                        bytes += r.value().length;
                    }
                }
                double secs = (System.nanoTime() - start) / 1e9;
                System.out.printf("RESULT consume size=%d messages=%d seconds=%.3f msgs_per_sec=%.0f mib_per_sec=%.2f%n",
                        size, count, secs, count / secs, bytes / (double) (1 << 20) / secs);
            }
            cluster.deleteData();
        }
    }

    // ================================================================== latency

    /**
     * Producer and consumer in this JVM. Each value starts with System.nanoTime() at send; the
     * consumer computes now - sendTime when it receives the record. Fixed send rate; the first
     * {@code warmup-seconds} are discarded.
     */
    private void endToEndLatency() throws Exception {
        int rate = intOpt("rate", 5000);
        int seconds = intOpt("seconds", 20);
        int warmupSecs = intOpt("warmup-seconds", 5);
        int acks = intOpt("acks", -1);
        int size = intOpt("size", 100);
        int partitions = intOpt("partitions", 3);
        try (LocalCluster cluster = cluster(List.of(), List.of()).start()) {
            cluster.createTopic("lat", partitions, 3);
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < partitions; p++) {
                tps.add(new TopicPartition("lat", p));
            }
            ConcurrentLinkedQueue<Long> samples = new ConcurrentLinkedQueue<>();
            AtomicBoolean consuming = new AtomicBoolean(true);
            AtomicLong measureFrom = new AtomicLong(Long.MAX_VALUE);
            AtomicLong received = new AtomicLong();
            Thread consumerThread = Thread.ofPlatform().start(() -> {
                try (Consumer c = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), null).withFetch(500, 1 << 20))) {
                    c.assign(tps);
                    while (consuming.get()) {
                        for (ConsumerRecord r : c.poll(Duration.ofMillis(100))) {
                            long now = System.nanoTime();
                            long sent = ByteBuffer.wrap(r.value()).getLong();
                            received.incrementAndGet();
                            if (sent >= measureFrom.get()) {
                                samples.add(now - sent);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            Thread.sleep(500);
            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(acks).withBatching(16384, 0);
            long sent = 0;
            try (Producer producer = new Producer(cfg)) {
                long start = System.nanoTime();
                long intervalNs = 1_000_000_000L / rate;
                long end = start + TimeUnit.SECONDS.toNanos(seconds + warmupSecs);
                long next = start;
                measureFrom.set(start + TimeUnit.SECONDS.toNanos(warmupSecs));
                while (System.nanoTime() < end) {
                    long now = System.nanoTime();
                    if (now < next) {
                        if (next - now > 200_000) {
                            Thread.sleep(0, 100_000);
                        } else {
                            Thread.onSpinWait();
                        }
                        continue;
                    }
                    byte[] v = new byte[Math.max(size, 8)];
                    ByteBuffer.wrap(v).putLong(System.nanoTime());
                    producer.send("lat", null, v);
                    sent++;
                    next += intervalNs;
                }
                producer.flush();
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (received.get() < sent && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            consuming.set(false);
            consumerThread.join();
            long[] arr = samples.stream().mapToLong(Long::longValue).toArray();
            Stats s = new Stats(arr);
            System.out.printf("RESULT e2e acks=%d rate=%d size=%d sent=%d received=%d samples=%d p50_ms=%.3f p99_ms=%.3f "
                            + "p999_ms=%.3f max_ms=%.3f%n", acks, rate, size, sent, received.get(), s.count(),
                    s.percentile(50) / 1e6, s.percentile(99) / 1e6, s.percentile(99.9) / 1e6, s.max() / 1e6);
            cluster.deleteData();
        }
    }

    // ================================================================== failover

    /**
     * Repeatedly kill -9 the leader of a single-partition RF=3 topic while a producer sends
     * acks=all records one at a time. Failover time = time from the kill until the first
     * successful acknowledgement of a record sent after the kill (client-observed unavailability).
     */
    private void failover() throws Exception {
        int iterations = intOpt("iterations", 20);
        String session = opt("session-ms", "1500");
        try (LocalCluster cluster = cluster(List.of(), List.of("broker.session.timeout.ms=" + session)).start()) {
            cluster.createTopic("fo", 1, 3);
            TopicPartition tp = new TopicPartition("fo", 0);
            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1).withBatching(16384, 0)
                    .withTimeouts(1000, 60_000);
            List<Long> failoverMs = new ArrayList<>();
            Set<String> acked = ConcurrentHashMap.newKeySet();
            try (Producer producer = new Producer(cfg)) {
                AtomicLong lastAckNs = new AtomicLong();
                AtomicLong killNs = new AtomicLong(Long.MAX_VALUE);
                AtomicLong firstAckAfterKillNs = new AtomicLong(0);
                AtomicBoolean running = new AtomicBoolean(true);
                AtomicInteger seq = new AtomicInteger();
                Thread writer = Thread.ofPlatform().start(() -> {
                    while (running.get()) {
                        try {
                            long sentAt = System.nanoTime();
                            String v = "f" + seq.getAndIncrement();
                            producer.send(tp, null, v.getBytes()).get();
                            acked.add(v);
                            long now = System.nanoTime();
                            lastAckNs.set(now);
                            if (sentAt > killNs.get() && firstAckAfterKillNs.get() == 0) {
                                firstAckAfterKillNs.set(now);
                            }
                        } catch (Exception e) {
                            // delivery timeout; keep going
                        }
                    }
                });
                for (int i = 0; i < iterations; i++) {
                    Thread.sleep(1000);
                    int leader = cluster.leaderOf(tp);
                    firstAckAfterKillNs.set(0);
                    long k = System.nanoTime();
                    killNs.set(k);
                    cluster.kill(leader);
                    while (firstAckAfterKillNs.get() == 0) {
                        Thread.sleep(1);
                    }
                    long ms = (firstAckAfterKillNs.get() - k) / 1_000_000;
                    failoverMs.add(ms);
                    killNs.set(Long.MAX_VALUE);
                    System.out.printf("ITER %d killed=%d new_leader=%d failover_ms=%d%n", i, leader, cluster.leaderOf(tp), ms);
                    cluster.restart(leader);
                    cluster.awaitFullIsr("fo", 60_000);
                }
                running.set(false);
                writer.join();
            }
            long lost = verifyAllPresent(cluster, tp, acked);
            Stats s = new Stats(failoverMs.stream().mapToLong(Long::longValue).toArray());
            System.out.printf("RESULT failover session_timeout_ms=%s iterations=%d median_ms=%d p95_ms=%d min_ms=%d max_ms=%d "
                    + "acked=%d lost_acked=%d%n", session, iterations, s.percentile(50), s.percentile(95), s.min(), s.max(),
                    acked.size(), lost);
            cluster.deleteData();
        }
    }

    private static long verifyAllPresent(LocalCluster cluster, TopicPartition tp, Set<String> acked) throws Exception {
        Set<String> seen = new java.util.HashSet<>();
        try (Consumer c = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), null))) {
            c.assign(List.of(tp));
            int idle = 0;
            while (idle < 10) {
                List<ConsumerRecord> recs = c.poll(Duration.ofMillis(100));
                idle = recs.isEmpty() ? idle + 1 : 0;
                recs.forEach(r -> seen.add(new String(r.value(), StandardCharsets.UTF_8)));
            }
        }
        return acked.stream().filter(v -> !seen.contains(v)).count();
    }

    // ================================================================== chaos

    private record Consumed(TopicPartition tp, long offset, String value) {
    }

    /**
     * Producers (idempotent, acks=all) and a 3-member consumer group run while brokers are
     * kill -9'ed at random. One consumer is also replaced halfway (rebalance under chaos).
     * Afterwards: every acknowledged record must have been consumed exactly once by the group,
     * and within each partition every producer's records must appear in send order.
     */
    private void chaos() throws Exception {
        int seconds = intOpt("seconds", 120);
        long seed = Long.parseLong(opt("seed", String.valueOf(System.currentTimeMillis())));
        Random rnd = new Random(seed);
        int partitions = 6;
        try (LocalCluster cluster = cluster(List.of(), List.of()).start()) {
            cluster.createTopic("chaos", partitions, 3);
            Set<String> acked = ConcurrentHashMap.newKeySet();
            AtomicInteger failedSends = new AtomicInteger();
            ConcurrentLinkedQueue<Consumed> consumed = new ConcurrentLinkedQueue<>();
            AtomicBoolean producing = new AtomicBoolean(true);
            List<Thread> producers = new ArrayList<>();
            for (int pid = 0; pid < 2; pid++) {
                int id = pid;
                producers.add(Thread.ofPlatform().start(() -> runChaosProducer(cluster.bootstrap(), id, producing,
                        acked, failedSends)));
            }
            List<ChaosConsumer> consumers = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                consumers.add(new ChaosConsumer(cluster.bootstrap(), consumed));
            }

            long end = System.currentTimeMillis() + seconds * 1000L;
            int kills = 0;
            boolean replaced = false;
            List<String> killLog = new ArrayList<>();
            while (System.currentTimeMillis() < end) {
                Thread.sleep(2000 + rnd.nextInt(3000));
                if (!replaced && System.currentTimeMillis() > end - seconds * 500L) {
                    consumers.removeFirst().stop();
                    consumers.add(new ChaosConsumer(cluster.bootstrap(), consumed));
                    replaced = true;
                    System.out.println("EVENT replaced one consumer (rebalance)");
                }
                List<Integer> live = cluster.liveBrokers();
                int victim = live.get(rnd.nextInt(live.size()));
                cluster.kill(victim);
                kills++;
                long downMs = 1000 + rnd.nextInt(3000);
                killLog.add(victim + ":" + downMs);
                System.out.printf("EVENT kill -9 broker %d, restart after %d ms (acked so far %d)%n", victim, downMs, acked.size());
                Thread.sleep(downMs);
                cluster.restart(victim);
                cluster.awaitFullIsr("chaos", 60_000);
            }
            producing.set(false);
            for (Thread t : producers) {
                t.join();
            }
            // Let the group drain everything that was acknowledged.
            long drainDeadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < drainDeadline) {
                Set<String> seen = new java.util.HashSet<>();
                consumed.forEach(c -> seen.add(c.value()));
                if (seen.containsAll(acked)) {
                    break;
                }
                Thread.sleep(200);
            }
            Thread.sleep(1000);
            for (ChaosConsumer c : consumers) {
                c.stop();
            }

            // ---- verification
            Map<String, Integer> counts = new HashMap<>();
            consumed.forEach(c -> counts.merge(c.value(), 1, Integer::sum));
            long lost = acked.stream().filter(v -> !counts.containsKey(v)).count();
            long duplicates = counts.values().stream().filter(n -> n > 1).count();
            long unackedButConsumed = counts.keySet().stream().filter(v -> !acked.contains(v)).count();
            long orderViolations = 0;
            Map<TopicPartition, List<Consumed>> byPartition = new HashMap<>();
            consumed.forEach(c -> byPartition.computeIfAbsent(c.tp(), k -> new ArrayList<>()).add(c));
            for (List<Consumed> list : byPartition.values()) {
                list.sort((a, b) -> Long.compare(a.offset(), b.offset()));
                Map<String, Long> lastSeq = new HashMap<>();
                for (Consumed c : list) {
                    String[] parts = c.value().split("-");
                    long s = Long.parseLong(parts[1]);
                    Long prev = lastSeq.put(parts[0], s);
                    if (prev != null && prev >= s) {
                        orderViolations++;
                    }
                }
            }
            System.out.printf("RESULT chaos seed=%d seconds=%d kills=%d acked=%d consumed_records=%d distinct_consumed=%d "
                            + "lost_acked=%d duplicates=%d unacked_but_consumed=%d failed_sends=%d order_violations=%d%n",
                    seed, seconds, kills, acked.size(), consumed.size(), counts.size(), lost, duplicates,
                    unackedButConsumed, failedSends.get(), orderViolations);
            System.out.println("KILLS " + killLog);
            cluster.deleteData();
        }
    }

    private static void runChaosProducer(String bootstrap, int id, AtomicBoolean producing, Set<String> acked,
                                         AtomicInteger failedSends) {
        ProducerConfig cfg = ProducerConfig.defaults(bootstrap).withAcks(-1).withBatching(16384, 5)
                .withTimeouts(2000, 120_000);
        try (Producer producer = new Producer(cfg)) {
            long seq = 0;
            List<CompletableFuture<RecordMetadata>> window = new ArrayList<>();
            while (producing.get()) {
                String v = "p" + id + "-" + seq++;
                CompletableFuture<RecordMetadata> f = producer.send("chaos", v.getBytes(), v.getBytes());
                f.whenComplete((m, e) -> {
                    if (e == null) {
                        acked.add(v);
                    } else {
                        failedSends.incrementAndGet();
                    }
                });
                window.add(f);
                if (window.size() >= 200) {
                    window.forEach(x -> x.exceptionally(e -> null).join());
                    window.clear();
                    Thread.sleep(2); // ~ tens of thousands of msgs/s total; keeps the run CPU-friendly
                }
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final class ChaosConsumer {
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread thread;

        ChaosConsumer(String bootstrap, ConcurrentLinkedQueue<Consumed> out) {
            thread = Thread.ofPlatform().start(() -> {
                ConsumerConfig cfg = ConsumerConfig.defaults(bootstrap, "chaos-group").withAutoCommitInterval(200);
                try (Consumer c = new Consumer(cfg)) {
                    c.subscribe(List.of("chaos"));
                    while (running.get()) {
                        try {
                            for (ConsumerRecord r : c.poll(Duration.ofMillis(200))) {
                                out.add(new Consumed(r.partition(), r.offset(), new String(r.value(), StandardCharsets.UTF_8)));
                            }
                        } catch (Exception e) {
                            System.out.println("consumer poll error (retrying): " + e);
                            Thread.sleep(100);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        void stop() throws InterruptedException {
            running.set(false);
            thread.join();
        }
    }
}
