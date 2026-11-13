package dev.minilog.integration;

import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.common.TopicPartition;
import dev.minilog.group.RangeAssignor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupIT {
    @TempDir
    Path dir;

    @Test
    void rangeAssignorSplitsContiguousRanges() {
        Map<String, List<TopicPartition>> a = RangeAssignor.assign(
                Map.of("m1", Set.of("t"), "m2", Set.of("t"), "m3", Set.of("t")), topic -> 7);
        assertEquals(List.of(tp(0), tp(1), tp(2)), a.get("m1"));
        assertEquals(List.of(tp(3), tp(4)), a.get("m2"));
        assertEquals(List.of(tp(5), tp(6)), a.get("m3"));
    }

    private static TopicPartition tp(int p) {
        return new TopicPartition("t", p);
    }

    /** One background consumer thread that records everything it receives. */
    private static final class Member {
        final Consumer consumer;
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread thread;
        volatile Throwable error;

        Member(String bootstrap, Map<String, Integer> seen) {
            consumer = new Consumer(ConsumerConfig.defaults(bootstrap, "group-a")
                    .withSessionTimeout(3000, 100).withAutoCommitInterval(0));
            consumer.subscribe(List.of("events"));
            thread = Thread.ofPlatform().start(() -> {
                try {
                    while (running.get()) {
                        for (ConsumerRecord r : consumer.poll(Duration.ofMillis(100))) {
                            seen.merge(new String(r.value()), 1, Integer::sum);
                        }
                    }
                } catch (Throwable t) {
                    error = t;
                }
            });
        }

        void stop() throws InterruptedException {
            running.set(false);
            thread.join();
            consumer.close(); // commits final positions and leaves the group
        }
    }

    @Test
    void rebalancesOnJoinAndLeaveWithoutLossOrDuplicates() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("events", 6, 3);
            Map<String, Integer> seen = new ConcurrentHashMap<>();
            List<String> produced = new ArrayList<>();
            try (Producer producer = new Producer(ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1))) {
                Member m1 = new Member(cluster.bootstrap(), seen);
                TestCluster.waitUntil(() -> m1.consumer.assignment().size() == 6, 10_000, "m1 owns all 6");

                produceBatch(producer, produced, 0, 300);
                Member m2 = new Member(cluster.bootstrap(), seen);
                TestCluster.waitUntil(() -> m2.consumer.assignment().size() == 3
                        && m1.consumer.assignment().size() == 3, 10_000, "split 3/3 after m2 joins");
                Set<TopicPartition> union = new HashSet<>(m1.consumer.assignment());
                union.addAll(m2.consumer.assignment());
                assertEquals(6, union.size(), "assignments are disjoint and cover all partitions");

                produceBatch(producer, produced, 300, 300);
                Member m3 = new Member(cluster.bootstrap(), seen);
                TestCluster.waitUntil(() -> m1.consumer.assignment().size() == 2 && m2.consumer.assignment().size() == 2
                        && m3.consumer.assignment().size() == 2, 10_000, "2/2/2 after m3 joins");

                produceBatch(producer, produced, 600, 300);
                m2.stop(); // leaves the group -> rebalance to 3/3
                TestCluster.waitUntil(() -> m1.consumer.assignment().size() == 3
                        && m3.consumer.assignment().size() == 3, 10_000, "3/3 after m2 leaves");

                produceBatch(producer, produced, 900, 300);
                TestCluster.waitUntil(() -> seen.size() == produced.size(), 20_000, "all records consumed");
                m1.stop();
                m3.stop();
                for (Member m : List.of(m1, m2, m3)) {
                    assertEquals(null, m.error);
                }
            }
            assertEquals(1200, produced.size());
            for (String v : produced) {
                assertEquals(1, seen.getOrDefault(v, 0), "exactly once across rebalances: " + v);
            }
        }
    }

    private static void produceBatch(Producer producer, List<String> produced, int from, int count) throws Exception {
        for (int i = from; i < from + count; i++) {
            String v = "e-" + i;
            producer.send("events", null, v.getBytes());
            produced.add(v);
        }
        producer.flush();
    }

    @Test
    void crashedMemberIsEvictedAfterSessionTimeout() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("events", 4, 3);
            Map<String, Integer> seen = new ConcurrentHashMap<>();
            Member survivor = new Member(cluster.bootstrap(), seen);
            TestCluster.waitUntil(() -> survivor.consumer.assignment().size() == 4, 10_000, "survivor owns all");
            // A member that joins and then goes silent (no close, no heartbeats).
            Consumer zombie = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), "group-a")
                    .withSessionTimeout(1000, 100));
            zombie.subscribe(List.of("events"));
            Thread t = Thread.ofPlatform().start(() -> {
                try {
                    zombie.poll(Duration.ofMillis(10));
                } catch (Exception ignored) {
                    // ignored
                }
            });
            TestCluster.waitUntil(() -> survivor.consumer.assignment().size() == 2, 10_000, "split with zombie");
            t.join();
            // Simulate a crash: stop heartbeats without leaving the group.
            java.lang.reflect.Field closed = Consumer.class.getDeclaredField("closed");
            closed.setAccessible(true);
            closed.set(zombie, true);
            long start = System.currentTimeMillis();
            TestCluster.waitUntil(() -> survivor.consumer.assignment().size() == 4, 10_000, "zombie evicted");
            assertTrue(System.currentTimeMillis() - start >= 500, "eviction waits for the session timeout");
            survivor.stop();
        }
    }
}
