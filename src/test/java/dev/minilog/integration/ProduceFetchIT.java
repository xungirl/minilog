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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProduceFetchIT {
    @TempDir
    Path dir;

    @Test
    void produceAndConsumeEndToEndAcrossThreeBrokers() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("orders", 3, 3);
            int n = 3000;
            List<CompletableFuture<RecordMetadata>> acks = new ArrayList<>();
            try (Producer producer = new Producer(ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1))) {
                for (int i = 0; i < n; i++) {
                    String key = "customer-" + (i % 50);
                    acks.add(producer.send("orders", key.getBytes(), ("order-" + i).getBytes()));
                }
                producer.flush();
            }
            Map<TopicPartition, Long> lastOffset = new HashMap<>();
            for (CompletableFuture<RecordMetadata> f : acks) {
                RecordMetadata md = f.get();
                assertTrue(md.offset() >= 0);
                lastOffset.merge(md.partition(), md.offset(), Math::max);
            }
            assertEquals(3, lastOffset.size(), "keys spread over all partitions");

            try (Consumer consumer = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), "g1"))) {
                consumer.subscribe(List.of("orders"));
                List<ConsumerRecord> got = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 20_000;
                while (got.size() < n && System.currentTimeMillis() < deadline) {
                    got.addAll(consumer.poll(Duration.ofMillis(200)));
                }
                assertEquals(n, got.size());
                // per-partition order is preserved and every key stays on one partition
                Map<TopicPartition, Long> prev = new HashMap<>();
                Map<String, TopicPartition> keyHome = new HashMap<>();
                Map<String, Integer> lastOrderPerKey = new HashMap<>();
                for (ConsumerRecord r : got) {
                    Long p = prev.put(r.partition(), r.offset());
                    assertTrue(p == null || r.offset() == p + 1, "contiguous offsets in " + r.partition());
                    String key = new String(r.key(), StandardCharsets.UTF_8);
                    TopicPartition home = keyHome.putIfAbsent(key, r.partition());
                    assertTrue(home == null || home.equals(r.partition()));
                    int order = Integer.parseInt(new String(r.value(), StandardCharsets.UTF_8).substring(6));
                    Integer last = lastOrderPerKey.put(key, order);
                    assertTrue(last == null || last < order, "per-key order");
                }
            }
        }
    }

    @Test
    void acksZeroAndOneAlsoDeliver() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("t", 2, 3);
            for (int acks : new int[]{0, 1}) {
                try (Producer producer = new Producer(ProducerConfig.defaults(cluster.bootstrap()).withAcks(acks))) {
                    for (int i = 0; i < 500; i++) {
                        producer.send("t", null, ("a" + acks + "-" + i).getBytes());
                    }
                    producer.flush();
                }
            }
            try (Consumer consumer = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), null))) {
                consumer.assign(List.of(new TopicPartition("t", 0), new TopicPartition("t", 1)));
                int count = 0;
                long deadline = System.currentTimeMillis() + 15_000;
                while (count < 1000 && System.currentTimeMillis() < deadline) {
                    count += consumer.poll(Duration.ofMillis(200)).size();
                }
                assertEquals(1000, count);
            }
        }
    }
}
