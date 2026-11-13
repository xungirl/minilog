package dev.minilog.bench;

import dev.minilog.client.Admin;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.ClusterMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Launches a real local cluster as separate JVM processes (1 controller + N brokers on
 * localhost), so benchmarks can {@code kill -9} a broker exactly like a crash.
 */
public final class LocalCluster implements AutoCloseable {
    public static final List<String> JVM_FLAGS = List.of("-Xms512m", "-Xmx512m", "-XX:+UseG1GC");

    private final Path dir;
    private final int basePort;
    private final int brokerCount;
    private final List<String> brokerOverrides;
    private final List<String> controllerOverrides;
    private final Map<Integer, Process> brokers = new HashMap<>();
    private Process controller;

    public LocalCluster(Path dir, int basePort, int brokerCount, List<String> brokerOverrides,
                        List<String> controllerOverrides) {
        this.dir = dir;
        this.basePort = basePort;
        this.brokerCount = brokerCount;
        this.brokerOverrides = brokerOverrides;
        this.controllerOverrides = controllerOverrides;
    }

    public LocalCluster start() throws Exception {
        deleteRecursively(dir);
        Files.createDirectories(dir.resolve("logs"));
        List<String> cargs = new ArrayList<>(List.of("port=" + basePort, "data.dir=" + dir.resolve("controller")));
        cargs.addAll(controllerOverrides);
        controller = launch("dev.minilog.controller.ControllerMain", cargs, "controller");
        waitForPort(basePort);
        for (int id = 1; id <= brokerCount; id++) {
            startBroker(id);
        }
        long deadline = System.currentTimeMillis() + 20_000;
        try (Admin admin = new Admin(bootstrap())) {
            while (admin.describe().brokers().size() < brokerCount) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException("brokers did not register");
                }
                Thread.sleep(50);
            }
        }
        return this;
    }

    private void startBroker(int id) throws Exception {
        List<String> args = new ArrayList<>(List.of("broker.id=" + id, "port=" + (basePort + id),
                "data.dir=" + dir.resolve("broker-" + id), "controller.port=" + basePort));
        args.addAll(brokerOverrides);
        brokers.put(id, launch("dev.minilog.broker.BrokerMain", args, "broker-" + id));
        waitForPort(basePort + id);
    }

    private Process launch(String mainClass, List<String> args, String name) throws IOException {
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> cmd = new ArrayList<>();
        cmd.add(java);
        cmd.addAll(JVM_FLAGS);
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        cmd.add(mainClass);
        cmd.addAll(args);
        Path log = dir.resolve("logs").resolve(name + ".log");
        return new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile())).start();
    }

    private static void waitForPort(int port) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000;
        while (true) {
            try (java.net.Socket ignored = new java.net.Socket("localhost", port)) {
                return;
            } catch (IOException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IOException("port " + port + " did not open");
                }
                Thread.sleep(20);
            }
        }
    }

    public String bootstrap() {
        StringBuilder sb = new StringBuilder("localhost:" + basePort);
        for (int id = 1; id <= brokerCount; id++) {
            sb.append(",localhost:").append(basePort + id);
        }
        return sb.toString();
    }

    public void createTopic(String topic, int partitions, int rf) throws Exception {
        try (Admin admin = new Admin(bootstrap())) {
            admin.createTopic(topic, partitions, rf);
        }
        awaitFullIsr(topic, 30_000);
    }

    public ClusterMetadata metadata() throws IOException {
        try (Admin admin = new Admin(bootstrap())) {
            return admin.describe();
        }
    }

    public int leaderOf(TopicPartition tp) throws IOException {
        return metadata().partition(tp).map(ClusterMetadata.PartitionMetadata::leader).orElse(-1);
    }

    public void awaitFullIsr(String topic, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            ClusterMetadata md = metadata();
            boolean full = md.topic(topic).map(t -> t.partitions().stream()
                    .allMatch(p -> p.leader() >= 0 && p.isr().size() == p.replicas().size())).orElse(false);
            if (full) {
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("ISR of " + topic + " not full: " + md.topic(topic));
            }
            Thread.sleep(50);
        }
    }

    /** kill -9 (SIGKILL): no shutdown hook, no flush, no clean-shutdown marker. */
    public void kill(int id) throws InterruptedException {
        Process p = brokers.remove(id);
        if (p != null) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
        }
    }

    public void restart(int id) throws Exception {
        startBroker(id);
    }

    public boolean isAlive(int id) {
        Process p = brokers.get(id);
        return p != null && p.isAlive();
    }

    public List<Integer> liveBrokers() {
        return brokers.keySet().stream().sorted().toList();
    }

    @Override
    public void close() throws IOException {
        try {
            stopAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void stopAll() throws InterruptedException {
        for (Process p : brokers.values()) {
            p.destroy(); // SIGTERM: clean shutdown via shutdown hook
        }
        for (Process p : brokers.values()) {
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                p.destroyForcibly();
            }
        }
        brokers.clear();
        if (controller != null) {
            controller.destroy();
            controller.waitFor(10, TimeUnit.SECONDS);
        }
    }

    public void deleteData() throws IOException {
        // keep logs, drop data (benchmarks can write GBs)
        for (int id = 1; id <= brokerCount; id++) {
            deleteRecursively(dir.resolve("broker-" + id));
        }
        deleteRecursively(dir.resolve("controller"));
    }

    static void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(p)) {
            for (Path x : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(x);
            }
        }
    }
}
