package dev.minilog.integration;

import dev.minilog.broker.Broker;
import dev.minilog.broker.BrokerConfig;
import dev.minilog.client.Admin;
import dev.minilog.common.TopicPartition;
import dev.minilog.controller.ControllerConfig;
import dev.minilog.controller.ControllerServer;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.storage.LogConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * An in-process cluster for tests: one controller and N brokers in this JVM, each with its own
 * data directory and an ephemeral port. Brokers can be killed (crash simulation) and restarted.
 */
public final class TestCluster implements AutoCloseable {
    public static final long SESSION_TIMEOUT_MS = 600;
    public static final long REPLICA_LAG_MS = 1000;

    private final Path dir;
    private final ControllerServer controller;
    private final Map<Integer, Broker> brokers = new HashMap<>();
    private final Map<Integer, BrokerConfig> configs = new HashMap<>();

    public TestCluster(Path dir, int brokerCount) throws IOException {
        this(dir, brokerCount, LogConfig.defaults());
    }

    public TestCluster(Path dir, int brokerCount, LogConfig logConfig) throws IOException {
        this.dir = dir;
        this.controller = new ControllerServer(new ControllerConfig("localhost", 0, dir.resolve("controller"),
                SESSION_TIMEOUT_MS)).start();
        for (int id = 1; id <= brokerCount; id++) {
            BrokerConfig cfg = new BrokerConfig(id, "localhost", 0, dir.resolve("broker-" + id), "localhost",
                    controller.port(), 50, REPLICA_LAG_MS, 100, 1024 * 1024, 2, logConfig);
            configs.put(id, cfg);
            brokers.put(id, new Broker(cfg).start());
        }
        waitUntil(() -> controller.controller().metadata().brokers().size() == brokerCount, 10_000, "brokers registered");
    }

    public String bootstrap() {
        StringBuilder sb = new StringBuilder("localhost:" + controller.port());
        brokers.values().forEach(b -> sb.append(",localhost:").append(b.port()));
        return sb.toString();
    }

    public void createTopic(String topic, int partitions, int rf) throws Exception {
        try (Admin admin = new Admin(bootstrap())) {
            admin.createTopic(topic, partitions, rf);
        }
        // let followers start fetching so ISR membership is real
        waitUntil(() -> {
            for (int p = 0; p < partitions; p++) {
                if (leader(new TopicPartition(topic, p)) < 0) {
                    return false;
                }
            }
            return true;
        }, 10_000, "leaders elected");
    }

    public ClusterMetadata metadata() {
        return controller.controller().metadata();
    }

    public int leader(TopicPartition tp) {
        return metadata().partition(tp).map(ClusterMetadata.PartitionMetadata::leader).orElse(-1);
    }

    public List<Integer> isr(TopicPartition tp) {
        return metadata().partition(tp).map(ClusterMetadata.PartitionMetadata::isr).orElse(List.of());
    }

    public Broker broker(int id) {
        return brokers.get(id);
    }

    public List<Integer> liveBrokerIds() {
        return new ArrayList<>(brokers.keySet());
    }

    /** Simulates kill -9 of a broker. */
    public void kill(int id) {
        Broker b = brokers.remove(id);
        if (b != null) {
            b.kill();
        }
    }

    public void restart(int id) throws IOException {
        brokers.put(id, new Broker(configs.get(id)).start());
    }

    public static void waitUntil(BooleanSupplier condition, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("Timed out waiting for: " + what);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (Broker b : brokers.values()) {
            b.close();
        }
        brokers.clear();
        controller.close();
    }

    public Path dir() {
        return dir;
    }
}
