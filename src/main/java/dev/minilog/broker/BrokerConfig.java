package dev.minilog.broker;

import dev.minilog.storage.LogConfig;

import java.nio.file.Path;
import java.util.Properties;

/**
 * Broker settings.
 *
 * @param heartbeatIntervalMs     how often the broker heartbeats to the controller
 * @param replicaLagTimeMs        a follower that has not caught up for this long is removed from the ISR
 * @param replicaFetchMaxWaitMs   long-poll time of follower fetch requests
 * @param replicaFetchMaxBytes    max bytes per partition per follower fetch
 * @param minInsyncReplicas       acks=all writes are rejected if the ISR is smaller than this
 */
public record BrokerConfig(int brokerId,
                           String host,
                           int port,
                           Path dataDir,
                           String controllerHost,
                           int controllerPort,
                           long heartbeatIntervalMs,
                           long replicaLagTimeMs,
                           int replicaFetchMaxWaitMs,
                           int replicaFetchMaxBytes,
                           int minInsyncReplicas,
                           LogConfig logConfig) {

    public static BrokerConfig fromProperties(Properties p) {
        LogConfig log = LogConfig.defaults()
                .withSegmentBytes(Integer.parseInt(p.getProperty("log.segment.bytes", "67108864")))
                .withIndexIntervalBytes(Integer.parseInt(p.getProperty("log.index.interval.bytes", "4096")))
                .withFlush(Long.parseLong(p.getProperty("log.flush.interval.messages", String.valueOf(Long.MAX_VALUE))),
                        Long.parseLong(p.getProperty("log.flush.interval.ms", "0")))
                .withRetention(Long.parseLong(p.getProperty("log.retention.ms", String.valueOf(7L * 24 * 3600 * 1000))),
                        Long.parseLong(p.getProperty("log.retention.bytes", "-1")))
                .withRetentionCheckIntervalMs(Long.parseLong(p.getProperty("log.retention.check.interval.ms", "30000")));
        return new BrokerConfig(
                Integer.parseInt(p.getProperty("broker.id", "1")),
                p.getProperty("host", "localhost"),
                Integer.parseInt(p.getProperty("port", "9091")),
                Path.of(p.getProperty("data.dir", "data/broker-1")),
                p.getProperty("controller.host", "localhost"),
                Integer.parseInt(p.getProperty("controller.port", "9090")),
                Long.parseLong(p.getProperty("heartbeat.interval.ms", "100")),
                Long.parseLong(p.getProperty("replica.lag.time.max.ms", "3000")),
                Integer.parseInt(p.getProperty("replica.fetch.wait.max.ms", "200")),
                Integer.parseInt(p.getProperty("replica.fetch.max.bytes", "1048576")),
                Integer.parseInt(p.getProperty("min.insync.replicas", "2")),
                log);
    }

    public BrokerConfig withPort(int newPort) {
        return new BrokerConfig(brokerId, host, newPort, dataDir, controllerHost, controllerPort, heartbeatIntervalMs,
                replicaLagTimeMs, replicaFetchMaxWaitMs, replicaFetchMaxBytes, minInsyncReplicas, logConfig);
    }
}
