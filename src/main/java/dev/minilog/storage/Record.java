package dev.minilog.storage;

/**
 * One record as stored in the log and sent over the wire.
 *
 * @param offset      position of the record in its partition (0, 1, 2, ...)
 * @param timestamp   producer-supplied creation time in epoch millis (used for time-based retention)
 * @param leaderEpoch epoch of the leader that first appended the record (used for log truncation after failover)
 * @param producerId  id of an idempotent producer, or -1
 * @param sequence    per-partition sequence number from that producer, or -1
 * @param key         optional key (may be null); used by the producer for partitioning
 * @param value       payload (may be null)
 */
public record Record(long offset, long timestamp, int leaderEpoch, long producerId, int sequence,
                     byte[] key, byte[] value) {
}
