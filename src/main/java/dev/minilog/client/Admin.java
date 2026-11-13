package dev.minilog.client;

import dev.minilog.common.Errors;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.protocol.Messages;

import java.io.Closeable;
import java.io.IOException;

/** Administrative operations: create a topic and wait until every partition has a working leader. */
public final class Admin implements Closeable {
    private final ClusterClient cluster;

    public Admin(String bootstrap) {
        this.cluster = new ClusterClient(bootstrap, 10_000);
    }

    public void createTopic(String topic, int partitions, int replicationFactor) throws IOException, InterruptedException {
        cluster.refreshMetadata();
        Messages.CreateTopicResponse resp = cluster.controller().call(ApiKey.CREATE_TOPIC,
                new Messages.CreateTopicRequest(topic, partitions, (short) replicationFactor));
        if (resp.error() != Errors.NONE) {
            throw new IOException("CreateTopic failed: " + resp.error() + " " + resp.message());
        }
        awaitLeaders(topic, partitions, 15_000);
    }

    /** Waits until every partition's leader broker reports itself as leader (via ListOffsets). */
    public void awaitLeaders(String topic, int partitions, long timeoutMs) throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        for (int p = 0; p < partitions; p++) {
            TopicPartition tp = new TopicPartition(topic, p);
            while (true) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException("Timed out waiting for leader of " + tp);
                }
                try {
                    ClusterMetadata md = cluster.refreshMetadata();
                    int leader = cluster.leaderFor(tp);
                    if (leader >= 0 && md.broker(leader).isPresent()) {
                        Messages.ListOffsetsResponse r = cluster.connectionTo(leader).call(ApiKey.LIST_OFFSETS,
                                new Messages.ListOffsetsRequest(topic, p, Messages.LATEST));
                        if (r.error() == Errors.NONE) {
                            break;
                        }
                    }
                } catch (IOException e) {
                    // broker not reachable yet
                }
                Thread.sleep(20);
            }
        }
    }

    public ClusterMetadata describe() throws IOException {
        return cluster.refreshMetadata();
    }

    @Override
    public void close() {
        cluster.close();
    }
}
