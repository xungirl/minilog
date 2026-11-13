package dev.minilog.integration;

import dev.minilog.broker.Partition;
import dev.minilog.client.Consumer;
import dev.minilog.client.ConsumerConfig;
import dev.minilog.client.ConsumerRecord;
import dev.minilog.client.Producer;
import dev.minilog.client.ProducerConfig;
import dev.minilog.common.Errors;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Messages;
import dev.minilog.storage.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationIT {
    @TempDir
    Path dir;

    private static void produce(String bootstrap, TopicPartition tp, int acks, int from, int count) throws Exception {
        try (Producer p = new Producer(ProducerConfig.defaults(bootstrap).withAcks(acks))) {
            List<java.util.concurrent.CompletableFuture<?>> fs = new ArrayList<>();
            for (int i = from; i < from + count; i++) {
                fs.add(p.send(tp, null, ("r" + i).getBytes()));
            }
            p.flush();
            for (var f : fs) {
                f.get();
            }
        }
    }

    private static List<ConsumerRecord> consumeFrom(String bootstrap, TopicPartition tp, long offset, long waitMs)
            throws Exception {
        try (Consumer c = new Consumer(ConsumerConfig.defaults(bootstrap, null))) {
            c.assign(List.of(tp));
            c.seek(tp, offset);
            List<ConsumerRecord> out = new ArrayList<>();
            long deadline = System.currentTimeMillis() + waitMs;
            while (System.currentTimeMillis() < deadline) {
                out.addAll(c.poll(Duration.ofMillis(50)));
            }
            return out;
        }
    }

    @Test
    void highWatermarkOnlyAdvancesWhenTheIsrHasTheRecords() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("hw", 1, 3);
            TopicPartition tp = new TopicPartition("hw", 0);
            produce(cluster.bootstrap(), tp, -1, 0, 100);
            int leaderId = cluster.leader(tp);
            Partition leader = cluster.broker(leaderId).replicaManager().partition(tp);
            assertEquals(100, leader.highWatermark());
            TestCluster.waitUntil(() -> cluster.isr(tp).size() == 3, 5000, "full ISR");

            // Followers stop replicating; the leader accepts acks=1 writes it alone has.
            List<Integer> followers = cluster.liveBrokerIds().stream().filter(id -> id != leaderId).toList();
            followers.forEach(id -> cluster.broker(id).replicaManager().setReplicationPaused(true));
            Thread.sleep(150); // let in-flight follower fetches finish
            long lagStart = System.currentTimeMillis();
            produce(cluster.bootstrap(), tp, 1, 100, 50);
            assertEquals(150, leader.log().logEndOffset());
            assertEquals(100, leader.highWatermark(), "uncommitted records stay above the HW");
            assertEquals(0, consumeFrom(cluster.bootstrap(), tp, 100, 200).size(),
                    "consumers cannot read above the HW");
            assertTrue(System.currentTimeMillis() - lagStart < TestCluster.REPLICA_LAG_MS,
                    "assertions ran before the ISR could shrink");

            // After replica.lag.time.max.ms the leader shrinks the ISR to itself; HW = min LEO over ISR = 150.
            TestCluster.waitUntil(() -> cluster.isr(tp).equals(List.of(leaderId)), 5000, "ISR shrinks to leader");
            TestCluster.waitUntil(() -> leader.highWatermark() == 150, 2000, "HW follows the shrunken ISR");
            // With ISR (1) < min.insync.replicas (2), acks=all writes are refused.
            try (Connection c = new Connection("localhost", cluster.broker(leaderId).port(), 5000)) {
                Messages.ProduceResponse r = c.call(ApiKey.PRODUCE, new Messages.ProduceRequest((short) -1, 1000, -1,
                        List.of(new Messages.ProduceRequest.PartitionBatch("hw", 0, -1,
                                List.of(new SimpleRecord(1, null, "x".getBytes()))))));
                assertEquals(Errors.NOT_ENOUGH_REPLICAS, r.results().getFirst().error());
            }

            followers.forEach(id -> cluster.broker(id).replicaManager().setReplicationPaused(false));
            TestCluster.waitUntil(() -> cluster.isr(tp).size() == 3, 10_000, "followers rejoin the ISR");
            for (int id : followers) {
                TestCluster.waitUntil(() -> cluster.broker(id).replicaManager().partition(tp).log().logEndOffset() == 150,
                        5000, "follower caught up");
            }
            assertEquals(150, consumeFrom(cluster.bootstrap(), tp, 0, 500).size());
        }
    }

    @Test
    void laggingFollowerDropsOutOfIsrAndRejoins() throws Exception {
        try (TestCluster cluster = new TestCluster(dir, 3)) {
            cluster.createTopic("lag", 1, 3);
            TopicPartition tp = new TopicPartition("lag", 0);
            produce(cluster.bootstrap(), tp, -1, 0, 50);
            TestCluster.waitUntil(() -> cluster.isr(tp).size() == 3, 5000, "full ISR");
            int leaderId = cluster.leader(tp);
            int slow = cluster.liveBrokerIds().stream().filter(id -> id != leaderId).findFirst().orElseThrow();
            cluster.broker(slow).replicaManager().setReplicationPaused(true);

            long pausedAt = System.currentTimeMillis();
            TestCluster.waitUntil(() -> !cluster.isr(tp).contains(slow), 5000, "slow follower removed from ISR");
            long removedAfter = System.currentTimeMillis() - pausedAt;
            assertTrue(removedAfter >= TestCluster.REPLICA_LAG_MS - 200,
                    "not removed before replica.lag.time.max.ms (" + removedAfter + " ms)");
            assertEquals(2, cluster.isr(tp).size());

            // acks=all still succeeds with 2 in-sync replicas (min.insync.replicas = 2)
            produce(cluster.bootstrap(), tp, -1, 50, 200);
            assertEquals(50, cluster.broker(slow).replicaManager().partition(tp).log().logEndOffset());

            cluster.broker(slow).replicaManager().setReplicationPaused(false);
            TestCluster.waitUntil(() -> cluster.isr(tp).contains(slow), 10_000, "follower rejoins ISR");
            TestCluster.waitUntil(() -> cluster.broker(slow).replicaManager().partition(tp).log().logEndOffset() == 250,
                    5000, "follower caught up");
        }
    }
}
