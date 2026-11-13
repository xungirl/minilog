package dev.minilog.storage;

import dev.minilog.common.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

/**
 * One segment of a partition log: an append-only {@code .log} file plus its sparse {@code .index}.
 * Files are named after the segment's base offset (the first offset it contains), zero-padded so a
 * directory listing sorts in offset order: {@code 00000000000000001234.log}.
 *
 * <p>Not thread-safe: the owning {@link Log} serialises writers and guards readers with a lock.
 */
public final class LogSegment implements Closeable {
    private static final Logger LOG = Logger.get("segment");

    private final long baseOffset;
    private final Path logFile;
    private final FileChannel channel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;

    private int size;                 // bytes in the .log file that hold complete records
    private long nextOffset;          // offset the next appended record will get
    private int bytesSinceLastIndexEntry;
    private long maxTimestamp = -1;   // newest record timestamp, for time-based retention

    private LogSegment(Path dir, long baseOffset, LogConfig config) throws IOException {
        this.baseOffset = baseOffset;
        this.logFile = dir.resolve(fileName(baseOffset) + ".log");
        this.channel = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.index = new OffsetIndex(dir.resolve(fileName(baseOffset) + ".index"), baseOffset, config.maxIndexBytes());
        this.indexIntervalBytes = config.indexIntervalBytes();
        this.size = (int) channel.size();
        this.nextOffset = baseOffset;
    }

    /** Creates a brand-new empty segment. */
    public static LogSegment create(Path dir, long baseOffset, LogConfig config) throws IOException {
        Files.deleteIfExists(dir.resolve(fileName(baseOffset) + ".log"));
        Files.deleteIfExists(dir.resolve(fileName(baseOffset) + ".index"));
        return new LogSegment(dir, baseOffset, config);
    }

    /**
     * Opens an existing segment. Does not scan the whole file: the caller decides whether to run
     * {@link #recover} (after a crash) or the cheaper {@link #loadNextOffset()}.
     */
    public static LogSegment open(Path dir, long baseOffset, LogConfig config) throws IOException {
        return new LogSegment(dir, baseOffset, config);
    }

    public static String fileName(long offset) {
        return String.format("%020d", offset);
    }

    // ------------------------------------------------------------------ write path

    /**
     * Appends already-encoded records (offsets firstOffset..lastOffset) at the end of the file.
     * Adds a sparse index entry when enough bytes have passed since the previous one.
     */
    public void append(long firstOffset, long lastOffset, long maxTs, ByteBuffer records) throws IOException {
        int bytes = records.remaining();
        if (bytesSinceLastIndexEntry >= indexIntervalBytes && !index.isFull()) {
            index.append(firstOffset, size);
            bytesSinceLastIndexEntry = 0;
        }
        long position = size;
        while (records.hasRemaining()) {
            position += channel.write(records, position);
        }
        size += bytes;
        bytesSinceLastIndexEntry += bytes;
        nextOffset = lastOffset + 1;
        maxTimestamp = Math.max(maxTimestamp, maxTs);
    }

    public void flush() throws IOException {
        channel.force(false); // data only; file metadata (mtime) is not needed for recovery
        index.flush();
    }

    // ------------------------------------------------------------------ read path

    /**
     * Returns the byte position of the first record with offset &gt;= target, or {@link #size}
     * if every record in this segment is below target. Uses the index, then scans forward.
     */
    public int translateOffset(long target) throws IOException {
        OffsetIndex.Entry entry = index.lookup(target);
        int pos = entry.position();
        ByteBuffer header = ByteBuffer.allocate(RecordCodec.SCAN_HEADER);
        while (pos < size) {
            header.clear();
            readFully(header, pos);
            int length = header.getInt(0);
            long offset = header.getLong(RecordCodec.OFFSET_POS);
            if (offset >= target) {
                return pos;
            }
            pos += RecordCodec.LENGTH_SIZE + length;
        }
        return size;
    }

    /**
     * Reads whole records starting at {@code startOffset}, up to {@code maxBytes} and stopping
     * before {@code maxOffsetExclusive}. Always returns at least one record if one is available,
     * even if it is larger than maxBytes, so a consumer can never get stuck on a big record.
     *
     * @return raw record bytes, possibly empty
     */
    public byte[] read(long startOffset, int maxBytes, long maxOffsetExclusive) throws IOException {
        int startPos = translateOffset(startOffset);
        int pos = startPos;
        ByteBuffer header = ByteBuffer.allocate(RecordCodec.SCAN_HEADER);
        while (pos < size) {
            header.clear();
            readFully(header, pos);
            int recordSize = RecordCodec.LENGTH_SIZE + header.getInt(0);
            long offset = header.getLong(RecordCodec.OFFSET_POS);
            if (offset >= maxOffsetExclusive) {
                break;
            }
            if (pos + recordSize - startPos > maxBytes && pos > startPos) {
                break;
            }
            pos += recordSize;
        }
        ByteBuffer out = ByteBuffer.allocate(pos - startPos);
        readFully(out, startPos);
        return out.array();
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        while (buf.hasRemaining()) {
            int n = channel.read(buf, position + buf.position());
            if (n < 0) {
                throw new IOException("Unexpected end of segment " + logFile);
            }
        }
    }

    /** Iterates over every record in the segment, validating CRCs (used for state rebuilds). */
    public void forEachRecord(Consumer<Record> action) throws IOException {
        ByteBuffer all = ByteBuffer.allocate(size);
        readFully(all, 0);
        all.flip();
        while (all.hasRemaining()) {
            action.accept(RecordCodec.read(all));
        }
    }

    // ------------------------------------------------------------------ recovery

    /**
     * Crash recovery: scans every record from the start, validating framing, CRC and offset
     * continuity. Everything after the first bad record (typically a torn write at the tail) is
     * truncated. The index is rebuilt from scratch during the scan.
     *
     * @param onRecord called for each valid record (used to rebuild the leader-epoch cache)
     * @return number of bytes truncated
     */
    public int recover(Consumer<Record> onRecord) throws IOException {
        index.ensureWritable();
        index.reset();
        long fileSize = channel.size();
        ByteBuffer all = ByteBuffer.allocate((int) fileSize);
        while (all.hasRemaining()) {
            if (channel.read(all, all.position()) < 0) {
                break;
            }
        }
        all.flip();
        int validBytes = 0;
        long expectedOffset = baseOffset;
        bytesSinceLastIndexEntry = 0;
        maxTimestamp = -1;
        while (all.hasRemaining()) {
            int start = all.position();
            Record r;
            try {
                r = RecordCodec.read(all);
            } catch (CorruptRecordException e) {
                LOG.warn("Recovery of %s stopped at byte %d: %s", logFile.getFileName(), start, e.getMessage());
                break;
            }
            if (r.offset() < expectedOffset) {
                LOG.warn("Recovery of %s found non-increasing offset %d at byte %d", logFile.getFileName(), r.offset(), start);
                break;
            }
            if (bytesSinceLastIndexEntry >= indexIntervalBytes && !index.isFull()) {
                index.append(r.offset(), start);
                bytesSinceLastIndexEntry = 0;
            }
            bytesSinceLastIndexEntry += all.position() - start;
            expectedOffset = r.offset() + 1;
            maxTimestamp = Math.max(maxTimestamp, r.timestamp());
            validBytes = all.position();
            onRecord.accept(r);
        }
        int truncated = (int) (fileSize - validBytes);
        if (truncated > 0) {
            channel.truncate(validBytes);
            channel.force(true);
        }
        size = validBytes;
        nextOffset = expectedOffset;
        return truncated;
    }

    /**
     * Cheap open path after a clean shutdown: start from the last index entry and scan headers to
     * the end of the file to find the next offset. If the index looks broken, falls back to a full
     * {@link #recover}.
     */
    public void loadNextOffset() throws IOException {
        if (!index.isValid(size)) {
            LOG.warn("Index %s failed sanity check; rebuilding", index.file().getFileName());
            recover(r -> { });
            return;
        }
        OffsetIndex.Entry last = index.lastEntry();
        int pos = last.position();
        long next = baseOffset;
        ByteBuffer header = ByteBuffer.allocate(RecordCodec.SCAN_HEADER);
        int lastRecordStart = pos;
        while (pos < size) {
            header.clear();
            readFully(header, pos);
            lastRecordStart = pos;
            next = header.getLong(RecordCodec.OFFSET_POS) + 1;
            pos += RecordCodec.LENGTH_SIZE + header.getInt(0);
        }
        nextOffset = next;
        bytesSinceLastIndexEntry = size - last.position();
        // Newest timestamp: read the last record (records are appended in time order by producers;
        // good enough for retention, which is coarse-grained by design).
        if (size > 0) {
            ByteBuffer rec = ByteBuffer.allocate(size - lastRecordStart);
            readFully(rec, lastRecordStart);
            rec.flip();
            maxTimestamp = RecordCodec.read(rec).timestamp();
        }
    }

    /** Removes every record with offset &gt;= {@code offset} from this segment. */
    public void truncateTo(long offset) throws IOException {
        int pos = translateOffset(offset);
        if (pos >= size) {
            return;
        }
        channel.truncate(pos);
        size = pos;
        index.truncateTo(offset);
        nextOffset = offset;
        bytesSinceLastIndexEntry = size - index.lastEntry().position();
    }

    /** Called when this segment stops being the active one: shrink the index file. */
    public void onRolled() throws IOException {
        flush();
        index.trimToValidSize();
    }

    /** Called when this segment becomes active again after the log was truncated into it. */
    public void onReactivated() throws IOException {
        index.ensureWritable();
    }

    @Override
    public void close() throws IOException {
        index.close();
        channel.close();
    }

    /** Closes file handles without flushing or trimming (used to simulate a crash in tests). */
    public void closeWithoutFlush() throws IOException {
        channel.close();
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(logFile);
        Files.deleteIfExists(index.file());
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public int size() {
        return size;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public int indexEntries() {
        return index.entries();
    }

    public Path logFile() {
        return logFile;
    }

    public Path indexFile() {
        return index.file();
    }
}
