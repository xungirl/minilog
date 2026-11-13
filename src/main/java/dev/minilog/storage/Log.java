package dev.minilog.storage;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.MiniLogException;
import dev.minilog.common.Time;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * The log of one partition: an ordered list of {@link LogSegment}s, of which only the last
 * ("active") one is written to.
 *
 * <pre>
 *  00000000000000000000.log  00000000000000052113.log  00000000000000104391.log  (active)
 *  [offsets 0 .. 52112]      [52113 .. 104390]         [104391 .. LEO-1]
 * </pre>
 *
 * Locking: a read-write lock. Appends, truncation, rolling and retention take the write lock;
 * fetches take the read lock, so many consumers can read concurrently.
 */
public final class Log implements Closeable {
    private static final Logger LOG = Logger.get("log");

    /** Offsets the leader assigned to an appended batch, or the original offsets of a duplicate. */
    public record AppendResult(long baseOffset, long lastOffset, boolean duplicate) {
    }

    private final Path dir;
    private final LogConfig config;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final LeaderEpochCache epochCache;
    private final ProducerStateManager producerState = new ProducerStateManager();

    private volatile long logEndOffset;
    private volatile long logStartOffset;
    private long unflushedMessages;
    private long lastFlushMs = Time.nowMs();

    private Log(Path dir, LogConfig config) throws IOException {
        this.dir = dir;
        this.config = config;
        Files.createDirectories(dir);
        this.epochCache = new LeaderEpochCache(dir.resolve("leader-epoch-checkpoint"));
    }

    /**
     * Opens (or creates) the log in {@code dir}.
     *
     * @param cleanShutdown whether the previous process shut down cleanly. If not, the active
     *                      segment is fully scanned: a torn tail write is truncated and its index rebuilt.
     */
    public static Log open(Path dir, LogConfig config, boolean cleanShutdown) throws IOException {
        Log log = new Log(dir, config);
        log.load(cleanShutdown);
        return log;
    }

    private void load(boolean cleanShutdown) throws IOException {
        List<Long> baseOffsets = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".log"))
                    .forEach(n -> baseOffsets.add(Long.parseLong(n.substring(0, n.length() - 4))));
        }
        baseOffsets.sort(null);
        for (int i = 0; i < baseOffsets.size(); i++) {
            long base = baseOffsets.get(i);
            LogSegment seg = LogSegment.open(dir, base, config);
            boolean active = i == baseOffsets.size() - 1;
            if (active && !cleanShutdown) {
                int truncated = seg.recover(r -> { });
                LOG.info("Recovered %s segment %d after unclean shutdown (truncated %d bytes)",
                        dir.getFileName(), base, truncated);
            } else {
                seg.loadNextOffset();
            }
            if (!active) {
                seg.onRolled();
            }
            segments.put(base, seg);
        }
        if (segments.isEmpty()) {
            segments.put(0L, LogSegment.create(dir, 0, config));
        }
        logStartOffset = segments.firstKey();
        logEndOffset = segments.lastEntry().getValue().nextOffset();
        epochCache.truncateFromEnd(logEndOffset);
        rebuildProducerState();
    }

    /**
     * Rebuilds the idempotent-producer table by scanning the log. Simple but O(log size);
     * Kafka avoids the full scan with periodic producer-state snapshots.
     */
    private void rebuildProducerState() throws IOException {
        producerState.clear();
        for (LogSegment seg : segments.values()) {
            try {
                seg.forEachRecord(producerState::onRecord);
            } catch (CorruptRecordException e) {
                // A damaged inactive segment must not stop the broker from starting; reads of the
                // damaged range will fail their CRC check and surface the problem to clients.
                LOG.warn("Corrupt record in %s while rebuilding producer state: %s", seg.logFile(), e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------ appends

    /**
     * Leader append: assigns offsets, stamps the leader epoch and writes the whole batch with one
     * positional write. Duplicate retries from an idempotent producer are detected and not re-written.
     */
    public AppendResult appendAsLeader(long producerId, int baseSequence, List<SimpleRecord> batch, int leaderEpoch) {
        lock.writeLock().lock();
        try {
            ProducerStateManager.Check check = producerState.check(producerId, baseSequence, batch.size());
            if (check instanceof ProducerStateManager.Duplicate dup) {
                return new AppendResult(dup.baseOffset(), dup.baseOffset() < 0 ? -1 : dup.baseOffset() + batch.size() - 1, true);
            }
            if (check instanceof ProducerStateManager.OutOfOrder ooo) {
                throw new MiniLogException(Errors.OUT_OF_ORDER_SEQUENCE,
                        "producer " + producerId + " expected sequence " + ooo.expected() + " got " + ooo.received());
            }
            epochCache.assign(leaderEpoch, logEndOffset);
            long base = logEndOffset;
            List<Record> records = new ArrayList<>(batch.size());
            long maxTs = -1;
            for (int i = 0; i < batch.size(); i++) {
                SimpleRecord sr = batch.get(i);
                int seq = producerId == ProducerStateManager.NO_PRODUCER_ID ? -1 : baseSequence + i;
                records.add(new Record(base + i, sr.timestamp(), leaderEpoch, producerId, seq, sr.key(), sr.value()));
                maxTs = Math.max(maxTs, sr.timestamp());
            }
            ByteBuffer encoded = RecordCodec.encodeAll(records);
            appendEncoded(base, base + records.size() - 1, maxTs, encoded);
            records.forEach(producerState::onRecord);
            return new AppendResult(base, base + records.size() - 1, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Follower append: the bytes come straight from the leader's log (already offset-assigned and
     * CRC-protected). They are validated, then written as-is.
     */
    public void appendAsFollower(byte[] rawRecords) {
        if (rawRecords.length == 0) {
            return;
        }
        lock.writeLock().lock();
        try {
            List<Record> records = RecordCodec.decodeAll(rawRecords); // validates every CRC
            long expected = logEndOffset;
            long maxTs = -1;
            for (Record r : records) {
                if (r.offset() != expected) {
                    throw new IllegalStateException(dir.getFileName() + ": follower expected offset " + expected
                            + " but leader sent " + r.offset());
                }
                expected++;
                maxTs = Math.max(maxTs, r.timestamp());
                epochCache.assign(r.leaderEpoch(), r.offset());
            }
            appendEncoded(records.getFirst().offset(), records.getLast().offset(), maxTs, ByteBuffer.wrap(rawRecords));
            records.forEach(producerState::onRecord);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void appendEncoded(long firstOffset, long lastOffset, long maxTs, ByteBuffer bytes) throws IOException {
        LogSegment active = segments.lastEntry().getValue();
        if (active.size() > 0 && (long) active.size() + bytes.remaining() > config.segmentBytes()) {
            active = roll();
        }
        active.append(firstOffset, lastOffset, maxTs, bytes);
        logEndOffset = lastOffset + 1;
        unflushedMessages += lastOffset - firstOffset + 1;
        if (unflushedMessages >= config.flushIntervalMessages()) {
            flushLocked();
        }
    }

    /** Starts a new active segment whose base offset is the current log end offset. */
    private LogSegment roll() throws IOException {
        LogSegment old = segments.lastEntry().getValue();
        old.onRolled(); // fsync + trim the index of the segment that just became read-only
        LogSegment fresh = LogSegment.create(dir, logEndOffset, config);
        segments.put(logEndOffset, fresh);
        LOG.debug("Rolled %s at offset %d", dir.getFileName(), logEndOffset);
        return fresh;
    }

    // ------------------------------------------------------------------ reads

    /**
     * Reads records starting at {@code startOffset}. {@code maxOffsetExclusive} lets the caller
     * hide uncommitted records: consumers pass the high watermark, followers pass the log end.
     */
    public byte[] read(long startOffset, int maxBytes, long maxOffsetExclusive) {
        lock.readLock().lock();
        try {
            if (startOffset < logStartOffset || startOffset > logEndOffset) {
                throw new OffsetOutOfRangeException(dir.getFileName() + ": offset " + startOffset
                        + " not in [" + logStartOffset + ", " + logEndOffset + "]");
            }
            long limit = Math.min(maxOffsetExclusive, logEndOffset);
            if (startOffset >= limit) {
                return new byte[0];
            }
            Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);
            while (entry != null) {
                byte[] data = entry.getValue().read(startOffset, maxBytes, limit);
                if (data.length > 0) {
                    return data;
                }
                entry = segments.higherEntry(entry.getKey());
            }
            return new byte[0];
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------ truncation & retention

    /** Removes every record with offset &gt;= {@code offset} (follower log reconciliation). */
    public void truncateTo(long offset) {
        lock.writeLock().lock();
        try {
            if (offset >= logEndOffset) {
                return;
            }
            LOG.info("Truncating %s from %d to %d", dir.getFileName(), logEndOffset, offset);
            if (offset <= logStartOffset) {
                truncateFullyAndStartAt(offset);
                return;
            }
            while (segments.lastKey() > offset || segments.lastKey() == offset && segments.size() > 1) {
                segments.pollLastEntry().getValue().delete();
            }
            LogSegment active = segments.lastEntry().getValue();
            active.onReactivated();
            active.truncateTo(offset);
            logEndOffset = offset;
            epochCache.truncateFromEnd(offset);
            rebuildProducerState();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Deletes all data and restarts the log at {@code offset}. */
    public void truncateFullyAndStartAt(long offset) {
        lock.writeLock().lock();
        try {
            for (LogSegment seg : segments.values()) {
                seg.delete();
            }
            segments.clear();
            segments.put(offset, LogSegment.create(dir, offset, config));
            logStartOffset = offset;
            logEndOffset = offset;
            epochCache.clear();
            producerState.clear();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Applies time- and size-based retention. Only whole, inactive segments are deleted, and only
     * segments entirely below {@code maxDeletableOffset} (the broker passes the high watermark so
     * uncommitted data is never deleted).
     *
     * @return number of segments deleted
     */
    public int deleteOldSegments(long nowMs, long maxDeletableOffset) {
        lock.writeLock().lock();
        try {
            int deleted = 0;
            long totalBytes = sizeInBytes();
            while (segments.size() > 1) {
                Map.Entry<Long, LogSegment> first = segments.firstEntry();
                LogSegment seg = first.getValue();
                long segmentEnd = segments.higherKey(first.getKey()); // next segment's base offset
                if (segmentEnd > maxDeletableOffset) {
                    break;
                }
                boolean expiredByTime = config.retentionMs() >= 0 && seg.maxTimestamp() >= 0
                        && nowMs - seg.maxTimestamp() > config.retentionMs();
                boolean overSize = config.retentionBytes() >= 0 && totalBytes - seg.size() >= config.retentionBytes();
                if (!expiredByTime && !overSize) {
                    break;
                }
                segments.pollFirstEntry();
                totalBytes -= seg.size();
                seg.delete();
                deleted++;
                LOG.info("Retention deleted segment %d of %s (%s)", seg.baseOffset(), dir.getFileName(),
                        expiredByTime ? "time" : "size");
            }
            logStartOffset = segments.firstKey();
            return deleted;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------ flushing

    /** fsyncs the active segment. Called by the size policy (in-line) or the time policy (background). */
    public void flush() {
        lock.writeLock().lock();
        try {
            flushLocked();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushLocked() throws IOException {
        if (unflushedMessages > 0) {
            segments.lastEntry().getValue().flush();
            unflushedMessages = 0;
        }
        lastFlushMs = Time.nowMs();
    }

    /** Time-based flush policy, called periodically by the {@link LogManager}. */
    public void maybeFlushByTime(long nowMs) {
        if (config.flushIntervalMs() > 0 && nowMs - lastFlushMs >= config.flushIntervalMs()) {
            flush();
        }
    }

    // ------------------------------------------------------------------ leader epochs

    /** Called when this replica becomes leader for {@code epoch}: the epoch starts at the current end. */
    public void assignEpochStart(int epoch) {
        lock.writeLock().lock();
        try {
            epochCache.assign(epoch, logEndOffset);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int latestEpoch() {
        lock.readLock().lock();
        try {
            return epochCache.latestEpoch();
        } finally {
            lock.readLock().unlock();
        }
    }

    public LeaderEpochCache.EpochEndOffset endOffsetForEpoch(int epoch) {
        lock.readLock().lock();
        try {
            return epochCache.endOffsetFor(epoch, logEndOffset);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------ lifecycle & accessors

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flushLocked();
            for (LogSegment seg : segments.values()) {
                seg.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Simulates a process crash: closes file handles without fsync or index trimming. */
    public void closeWithoutFlush() throws IOException {
        lock.writeLock().lock();
        try {
            for (LogSegment seg : segments.values()) {
                seg.closeWithoutFlush();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public int segmentCount() {
        return segments.size();
    }

    public List<LogSegment> segments() {
        return List.copyOf(segments.values());
    }

    public long sizeInBytes() {
        long total = 0;
        for (LogSegment s : segments.values()) {
            total += s.size();
        }
        return total;
    }

    public Path dir() {
        return dir;
    }

    public LogConfig config() {
        return config;
    }
}
