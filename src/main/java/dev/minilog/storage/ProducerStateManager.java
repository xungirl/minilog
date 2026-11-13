package dev.minilog.storage;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-partition memory of the last sequence number appended by each idempotent producer.
 *
 * <p>A producer numbers the records it sends to a partition 0, 1, 2, ... and retries a batch with
 * the <em>same</em> numbers. When a batch arrives the leader checks:
 * <ul>
 *   <li>baseSequence == lastSequence + 1 → new data, append it</li>
 *   <li>the whole batch is &lt;= lastSequence → a retry of something already written, do not append again</li>
 *   <li>anything else → a gap, reject with OUT_OF_ORDER_SEQUENCE</li>
 * </ul>
 * The state is rebuilt from the log itself (records carry producerId + sequence), so it survives
 * restarts and is identical on followers, which matters when a follower becomes leader.
 *
 * <p>Not thread-safe: guarded by the owning {@link Log}'s lock.
 */
public final class ProducerStateManager {
    public static final long NO_PRODUCER_ID = -1;

    /** The last record appended by a producer: its sequence number and its offset. */
    public record ProducerEntry(int lastSequence, long lastOffset) {
    }

    /** Result of checking an incoming batch. */
    public sealed interface Check permits Append, Duplicate, OutOfOrder {
    }

    public record Append() implements Check {
    }

    /** @param baseOffset offset of the already-written batch, or -1 if it is older than the last one */
    public record Duplicate(long baseOffset) implements Check {
    }

    public record OutOfOrder(int expected, int received) implements Check {
    }

    private final Map<Long, ProducerEntry> producers = new HashMap<>();

    public Check check(long producerId, int baseSequence, int count) {
        if (producerId == NO_PRODUCER_ID) {
            return new Append();
        }
        ProducerEntry last = producers.get(producerId);
        if (last == null) {
            // Unknown producer (first batch, or its history was deleted by retention): accept.
            return new Append();
        }
        if (baseSequence == last.lastSequence() + 1) {
            return new Append();
        }
        int batchLastSequence = baseSequence + count - 1;
        if (batchLastSequence == last.lastSequence()) {
            // Retry of the most recent batch: its records end exactly at lastOffset.
            return new Duplicate(last.lastOffset() - (count - 1));
        }
        if (batchLastSequence < last.lastSequence()) {
            return new Duplicate(-1);
        }
        return new OutOfOrder(last.lastSequence() + 1, baseSequence);
    }

    /** Called for every appended record (on leaders and followers, and during rebuilds). */
    public void onRecord(Record r) {
        if (r.producerId() != NO_PRODUCER_ID) {
            producers.put(r.producerId(), new ProducerEntry(r.sequence(), r.offset()));
        }
    }

    public void clear() {
        producers.clear();
    }

    public int size() {
        return producers.size();
    }
}
