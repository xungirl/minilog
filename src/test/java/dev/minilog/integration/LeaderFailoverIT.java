package dev.minilog.integration;

import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.client.RecordMetadata;
import dev.minilog.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The core durability claim: with acks=all (and min.insync.replicas=2), killing the partition
 * leader never loses an acknowledged record. Repeated over many kill/restart iterations.
 */
class LeaderFailoverIT {
    static final int ITERATIONS = 20;

    @TempDir
    Path dir;

    @Test
    void killingTheLeaderLosesNoAcknowledgedRecords() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("durable", 2, 3);
            List<TopicPartition> tps = List.of(new TopicPartition("durable", 0), new TopicPartition("durable", 1));
            Set<String> acked = ConcurrentHashMap.newKeySet();
            AtomicInteger failedSends = new AtomicInteger();
            AtomicInteger seq = new AtomicInteger();
            int totalKills = 0;

            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1)
                    .withBatching(4096, 1).withTimeouts(3000, 30_000);
            try (Producer producer = new Producer(cfg)) {
                for (int iter = 0; iter < ITERATIONS; iter++) {
                    TopicPartition victimTp = tps.get(iter % 2);
                    AtomicBoolean running = new AtomicBoolean(true);
                    int ackedBefore = acked.size();
                    Thread writer = Thread.ofPlatform().start(() -> {
                        List<CompletableFuture<RecordMetadata>> pending = new ArrayList<>();
                        while (running.get()) {
                            int i = seq.getAndIncrement();
                            String v = "m" + i;
                            CompletableFuture<RecordMetadata> f = producer.send(tps.get(i % 2), null, v.getBytes());
                            f.whenComplete((md, err) -> {
                                if (err == null) {
                                    acked.add(v);
                                } else {
                                    failedSends.incrementAndGet();
                                }
                            });
                            pending.add(f);
                            if (pending.size() >= 50) {
                                pending.forEach(p -> p.exceptionally(e -> null).join());
                                pending.clear();
                            }
                        }
                        pending.forEach(p -> p.exceptionally(e -> null).join());
                    });
                    TestCluster.waitUntil(() -> acked.size() > ackedBefore + 200, 20_000, "writes before kill");
                    int leader = cluster.leader(victimTp);
                    cluster.kill(leader);
                    totalKills++;
                    int ackedAtKill = acked.size();
                    TestCluster.waitUntil(() -> cluster.leader(victimTp) >= 0 && cluster.leader(victimTp) != leader,
                            10_000, "new leader elected");
                    TestCluster.waitUntil(() -> acked.size() > ackedAtKill + 200, 20_000, "writes continue after failover");
                    running.set(false);
                    writer.join();
                    producer.flush();
                    cluster.restart(leader);
                    for (TopicPartition tp : tps) {
                        TestCluster.waitUntil(() -> cluster.isr(tp).size() == 3, 20_000, "restarted broker back in ISR");
                    }
                }
            }
            assertEquals(ITERATIONS, totalKills);

            // Read both partitions from the beginning through the (current) leaders.
            Map<String, Integer> seen = new HashMap<>();
            try (Consumer consumer = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), null))) {
                consumer.assign(tps);
                long deadline = System.currentTimeMillis() + 30_000;
                int idle = 0;
                while (System.currentTimeMillis() < deadline && idle < 10) {
                    List<ConsumerRecord> recs = consumer.poll(Duration.ofMillis(100));
                    idle = recs.isEmpty() ? idle + 1 : 0;
                    recs.forEach(r -> seen.merge(new String(r.value()), 1, Integer::sum));
                }
            }
            List<String> lost = acked.stream().filter(v -> !seen.containsKey(v)).toList();
            List<String> duplicated = seen.entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).toList();
            System.out.printf("LeaderFailoverIT: kills=%d acked=%d consumed=%d lost=%d duplicated=%d failedSends=%d%n",
                    totalKills, acked.size(), seen.size(), lost.size(), duplicated.size(), failedSends.get());
            assertTrue(acked.size() > ITERATIONS * 400);
            assertEquals(List.of(), lost, "acknowledged records lost");
            assertEquals(List.of(), duplicated, "records duplicated");
        }
    }
}
