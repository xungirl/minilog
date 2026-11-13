package dev.minilog.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Remembers at which offset each leader epoch started, e.g. {epoch 0 -> 0, epoch 1 -> 950, epoch 3 -> 1200}.
 * Persisted to a small text file ({@code leader-epoch-checkpoint}) on every change.
 *
 * <p>Why: after a leader change, a follower may hold records the new leader never received
 * (written by the old leader but not yet committed). The follower asks the leader "where did my
 * latest epoch end in <em>your</em> log?" and truncates to that point before fetching, so the
 * replicas' logs never diverge. This is the idea of Kafka's KIP-101/KIP-279, simplified.
 *
 * <p>Not thread-safe: guarded by the owning {@link Log}'s lock.
 */
public final class LeaderEpochCache {
    /** Answer to "where does epoch E end?": the largest epoch &lt;= E that the log knows, and its end offset. */
    public record EpochEndOffset(int epoch, long endOffset) {
        public static final EpochEndOffset UNDEFINED = new EpochEndOffset(-1, -1);
    }

    private final Path file;
    private final TreeMap<Integer, Long> epochs = new TreeMap<>();

    public LeaderEpochCache(Path file) throws IOException {
        this.file = file;
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file)) {
                if (!line.isBlank()) {
                    String[] parts = line.trim().split(" ");
                    epochs.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
                }
            }
        }
    }

    /** Records that {@code epoch} starts at {@code startOffset}, if it is newer than the latest known epoch. */
    public void assign(int epoch, long startOffset) {
        if (epoch < 0 || (!epochs.isEmpty() && epochs.lastKey() >= epoch)) {
            return;
        }
        // An older epoch entry can never start at or after a newer one.
        epochs.entrySet().removeIf(e -> e.getValue() > startOffset);
        epochs.put(epoch, startOffset);
        persist();
    }

    public int latestEpoch() {
        return epochs.isEmpty() ? -1 : epochs.lastKey();
    }

    /**
     * Where does {@code requestedEpoch} end in this log? Returns the largest known epoch &lt;= the
     * requested one, together with the start offset of the next epoch (or the log end offset if it
     * is the latest epoch).
     */
    public EpochEndOffset endOffsetFor(int requestedEpoch, long logEndOffset) {
        Map.Entry<Integer, Long> floor = epochs.floorEntry(requestedEpoch);
        if (floor == null) {
            return EpochEndOffset.UNDEFINED;
        }
        Map.Entry<Integer, Long> next = epochs.higherEntry(floor.getKey());
        return new EpochEndOffset(floor.getKey(), next == null ? logEndOffset : next.getValue());
    }

    /** Drops epochs that start at or after {@code endOffset} (called when the log is truncated). */
    public void truncateFromEnd(long endOffset) {
        if (epochs.entrySet().removeIf(e -> e.getValue() >= endOffset)) {
            persist();
        }
    }

    public void clear() {
        epochs.clear();
        persist();
    }

    public List<String> describe() {
        List<String> out = new ArrayList<>();
        epochs.forEach((e, o) -> out.add(e + "@" + o));
        return out;
    }

    private void persist() {
        StringBuilder sb = new StringBuilder();
        epochs.forEach((epoch, offset) -> sb.append(epoch).append(' ').append(offset).append('\n'));
        try {
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, sb.toString());
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
