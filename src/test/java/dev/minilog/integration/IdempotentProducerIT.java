package dev.minilog.integration;

import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.Messages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The broker processes a produce request but the response is "lost" (connection closed before
 * replying). The producer cannot know whether the write happened and retries.
 */
class IdempotentProducerIT {
    @TempDir
    Path dir;

    private Map<String, Integer> runWithLostAcks(boolean idempotent, Path clusterDir) throws Exception {
        try (TestCluster cluster = new TestCluster(clusterDir, 3)) {
            cluster.createTopic("pay", 1, 3);
            TopicPartition tp = new TopicPartition("pay", 0);
            int leader = cluster.leader(tp);
            AtomicInteger dropped = new AtomicInteger();
            // drop the response of every 3rd produce request, 10 times in total
            AtomicInteger seenRequests = new AtomicInteger();
            cluster.broker(leader).dropResponsesIf(m -> m instanceof Messages.ProduceRequest
                    && seenRequests.incrementAndGet() % 3 == 0 && dropped.incrementAndGet() <= 10);
            ProducerConfig cfg = ProducerConfig.defaults(cluster.bootstrap()).withAcks(-1)
                    .withIdempotence(idempotent).withBatching(1024, 0);
            try (Producer producer = new Producer(cfg)) {
                List<java.util.concurrent.CompletableFuture<?>> futures = new ArrayList<>();
                for (int i = 0; i < 200; i++) {
                    futures.add(producer.send(tp, null, ("payment-" + i).getBytes()));
                    if (i % 10 == 9) {
                        producer.flush();
                    }
                }
                producer.flush();
                for (var f : futures) {
                    f.get(); // every record acknowledged despite the lost responses
                }
            }
            assertTrue(dropped.get() >= 10, "responses were actually dropped: " + dropped.get());
            Map<String, Integer> counts = new HashMap<>();
            try (Consumer consumer = new Consumer(ConsumerConfig.defaults(cluster.bootstrap(), null))) {
                consumer.assign(List.of(tp));
                long deadline = System.currentTimeMillis() + 10_000;
                int total = 0;
                while (System.currentTimeMillis() < deadline) {
                    List<ConsumerRecord> recs = consumer.poll(Duration.ofMillis(300));
                    if (recs.isEmpty() && total >= 200) {
                        break;
                    }
                    for (ConsumerRecord r : recs) {
                        counts.merge(new String(r.value()), 1, Integer::sum);
                        total++;
                    }
                }
            }
            return counts;
        }
    }

    @Test
    void idempotentProducerWritesEachRecordExactlyOnceDespiteRetries() throws Exception {
        Map<String, Integer> counts = runWithLostAcks(true, dir.resolve("idem"));
        assertEquals(200, counts.size());
        counts.forEach((v, c) -> assertEquals(1, c, "duplicate of " + v));
    }

    @Test
    void nonIdempotentProducerDuplicatesOnRetry() throws Exception {
        Map<String, Integer> counts = runWithLostAcks(false, dir.resolve("plain"));
        assertEquals(200, counts.size());
        long duplicated = counts.values().stream().filter(c -> c > 1).count();
        assertTrue(duplicated > 0, "without idempotence, lost acks cause duplicates (control experiment)");
    }
}
