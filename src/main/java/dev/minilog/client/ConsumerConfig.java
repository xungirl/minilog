package dev.minilog.client;

/**
 * Consumer settings.
 *
 * @param groupId            consumer group; null for manual assignment without a group
 * @param autoOffsetReset    "earliest" or "latest": where to start when the group has no committed offset
 * @param autoCommitIntervalMs commit positions from poll() at most this often (0 = commit on every poll)
 */
public record ConsumerConfig(String bootstrap,
                             String groupId,
                             int sessionTimeoutMs,
                             int rebalanceTimeoutMs,
                             int heartbeatIntervalMs,
                             int fetchMaxWaitMs,
                             int maxPartitionFetchBytes,
                             String autoOffsetReset,
                             int autoCommitIntervalMs,
                             int requestTimeoutMs) {

    public static ConsumerConfig defaults(String bootstrap, String groupId) {
        return new ConsumerConfig(bootstrap, groupId, 6000, 10_000, 500, 100, 1024 * 1024, "earliest", 1000, 10_000);
    }

    public ConsumerConfig withSessionTimeout(int sessionMs, int heartbeatMs) {
        return new ConsumerConfig(bootstrap, groupId, sessionMs, rebalanceTimeoutMs, heartbeatMs, fetchMaxWaitMs,
                maxPartitionFetchBytes, autoOffsetReset, autoCommitIntervalMs, requestTimeoutMs);
    }

    public ConsumerConfig withFetch(int maxWaitMs, int maxBytes) {
        return new ConsumerConfig(bootstrap, groupId, sessionTimeoutMs, rebalanceTimeoutMs, heartbeatIntervalMs,
                maxWaitMs, maxBytes, autoOffsetReset, autoCommitIntervalMs, requestTimeoutMs);
    }

    public ConsumerConfig withAutoCommitInterval(int ms) {
        return new ConsumerConfig(bootstrap, groupId, sessionTimeoutMs, rebalanceTimeoutMs, heartbeatIntervalMs,
                fetchMaxWaitMs, maxPartitionFetchBytes, autoOffsetReset, ms, requestTimeoutMs);
    }
}
