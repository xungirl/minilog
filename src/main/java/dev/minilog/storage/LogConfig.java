package dev.minilog.storage;

/**
 * Storage settings for every partition log on a broker.
 *
 * @param segmentBytes             roll to a new segment once the active one reaches this size
 * @param indexIntervalBytes       add one sparse index entry per this many bytes of log
 * @param maxIndexBytes            pre-allocated (memory-mapped) size of each .index file
 * @param flushIntervalMessages    fsync after this many messages (Long.MAX_VALUE = never by count)
 * @param flushIntervalMs          fsync at least this often (0 = no time-based flush)
 * @param retentionMs              delete segments whose newest record is older than this (-1 = keep forever)
 * @param retentionBytes           delete oldest segments while the log is bigger than this (-1 = unlimited)
 * @param retentionCheckIntervalMs how often the retention task runs
 */
public record LogConfig(int segmentBytes,
                        int indexIntervalBytes,
                        int maxIndexBytes,
                        long flushIntervalMessages,
                        long flushIntervalMs,
                        long retentionMs,
                        long retentionBytes,
                        long retentionCheckIntervalMs) {

    public static LogConfig defaults() {
        return new LogConfig(64 * 1024 * 1024, 4096, 1024 * 1024,
                Long.MAX_VALUE, 0, 7L * 24 * 3600 * 1000, -1, 30_000);
    }

    public LogConfig withSegmentBytes(int v) {
        return new LogConfig(v, indexIntervalBytes, maxIndexBytes, flushIntervalMessages, flushIntervalMs,
                retentionMs, retentionBytes, retentionCheckIntervalMs);
    }

    public LogConfig withIndexIntervalBytes(int v) {
        return new LogConfig(segmentBytes, v, maxIndexBytes, flushIntervalMessages, flushIntervalMs,
                retentionMs, retentionBytes, retentionCheckIntervalMs);
    }

    public LogConfig withFlush(long messages, long intervalMs) {
        return new LogConfig(segmentBytes, indexIntervalBytes, maxIndexBytes, messages, intervalMs,
                retentionMs, retentionBytes, retentionCheckIntervalMs);
    }

    public LogConfig withRetention(long ms, long bytes) {
        return new LogConfig(segmentBytes, indexIntervalBytes, maxIndexBytes, flushIntervalMessages, flushIntervalMs,
                ms, bytes, retentionCheckIntervalMs);
    }

    public LogConfig withRetentionCheckIntervalMs(long v) {
        return new LogConfig(segmentBytes, indexIntervalBytes, maxIndexBytes, flushIntervalMessages, flushIntervalMs,
                retentionMs, retentionBytes, v);
    }
}
