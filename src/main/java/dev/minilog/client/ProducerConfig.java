package dev.minilog.client;

/**
 * Producer settings (names follow Kafka's where the meaning is the same).
 *
 * @param acks              0 (fire and forget), 1 (leader wrote it), -1 (all in-sync replicas have it)
 * @param batchSize         max bytes per partition batch; a full batch is sent immediately
 * @param lingerMs          how long a non-full batch may wait for more records
 * @param deliveryTimeoutMs give up on a record (and fail its future) after this long, including retries
 * @param retryBackoffMs    pause between retries
 * @param idempotence       attach producerId + sequence numbers so the broker drops duplicate retries
 * @param bufferMemory      max bytes of unsent records; send() blocks when exceeded (back-pressure)
 */
public record ProducerConfig(String bootstrap,
                             short acks,
                             int batchSize,
                             int lingerMs,
                             int requestTimeoutMs,
                             int deliveryTimeoutMs,
                             int retryBackoffMs,
                             boolean idempotence,
                             long bufferMemory) {

    public static ProducerConfig defaults(String bootstrap) {
        return new ProducerConfig(bootstrap, (short) -1, 16 * 1024, 5, 10_000, 60_000, 50, true, 32L * 1024 * 1024);
    }

    public ProducerConfig withAcks(int v) {
        return new ProducerConfig(bootstrap, (short) v, batchSize, lingerMs, requestTimeoutMs, deliveryTimeoutMs,
                retryBackoffMs, idempotence, bufferMemory);
    }

    public ProducerConfig withBatching(int batchBytes, int linger) {
        return new ProducerConfig(bootstrap, acks, batchBytes, linger, requestTimeoutMs, deliveryTimeoutMs,
                retryBackoffMs, idempotence, bufferMemory);
    }

    public ProducerConfig withIdempotence(boolean v) {
        return new ProducerConfig(bootstrap, acks, batchSize, lingerMs, requestTimeoutMs, deliveryTimeoutMs,
                retryBackoffMs, v, bufferMemory);
    }

    public ProducerConfig withTimeouts(int requestTimeout, int deliveryTimeout) {
        return new ProducerConfig(bootstrap, acks, batchSize, lingerMs, requestTimeout, deliveryTimeout,
                retryBackoffMs, idempotence, bufferMemory);
    }
}
