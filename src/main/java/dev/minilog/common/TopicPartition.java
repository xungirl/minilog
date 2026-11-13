package dev.minilog.common;

/**
 * Identifies one partition of a topic, e.g. {@code orders-2}.
 * Partitions are the unit of ordering, storage and replication.
 */
public record TopicPartition(String topic, int partition) {

    /** Directory name used on disk for this partition's log, e.g. {@code orders-2}. */
    public String dirName() {
        return topic + "-" + partition;
    }

    /** Parses a directory name produced by {@link #dirName()}. Topic names may themselves contain '-'. */
    public static TopicPartition fromDirName(String name) {
        int dash = name.lastIndexOf('-');
        if (dash <= 0 || dash == name.length() - 1) {
            throw new IllegalArgumentException("Not a partition directory: " + name);
        }
        return new TopicPartition(name.substring(0, dash), Integer.parseInt(name.substring(dash + 1)));
    }

    @Override
    public String toString() {
        return dirName();
    }
}
