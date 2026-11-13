package dev.minilog.client;

import dev.minilog.common.TopicPartition;

/** Where an acknowledged record was written. Offset is -1 for acks=0 (the producer never learns it). */
public record RecordMetadata(TopicPartition partition, long offset) {
}
