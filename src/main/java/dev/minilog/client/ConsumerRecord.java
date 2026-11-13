package dev.minilog.client;

import dev.minilog.common.TopicPartition;

/** A record delivered to the application. */
public record ConsumerRecord(TopicPartition partition, long offset, long timestamp, byte[] key, byte[] value) {
}
