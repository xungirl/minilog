package dev.minilog.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Sparse offset index for one segment, stored in a memory-mapped {@code .index} file.
 *
 * <p>Each 8-byte entry maps a <em>relative</em> offset (offset - baseOffset, an int32) to the byte
 * position of that record in the segment's {@code .log} file. Entries are only added every
 * {@code indexIntervalBytes} of log data, so the index stays small enough to live in memory.
 * To find offset X we binary-search for the greatest entry &lt;= X, then scan the log forward
 * from that position (at most ~indexIntervalBytes).
 *
 * <p>The file is pre-allocated to {@code maxIndexBytes} while the segment is active and trimmed to
 * the real size when the segment is rolled or closed. Not thread-safe: guarded by the owning
 * {@link Log}'s lock.
 */
public final class OffsetIndex implements Closeable {
    public static final int ENTRY_SIZE = 8;

    /** An index hit: the record with {@code offset} starts at byte {@code position}. */
    public record Entry(long offset, int position) {
    }

    private final Path file;
    private final long baseOffset;
    private final int maxIndexBytes;
    private MappedByteBuffer mmap;
    private int entries;

    public OffsetIndex(Path file, long baseOffset, int maxIndexBytes) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexBytes = maxIndexBytes - (maxIndexBytes % ENTRY_SIZE);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long existing = raf.length();
            // After a clean close the file is trimmed, so length / 8 is the entry count.
            // After a crash the active segment's index may be untrimmed; the Log rebuilds it.
            this.entries = (int) Math.min(existing / ENTRY_SIZE, this.maxIndexBytes / ENTRY_SIZE);
            map(raf, Math.max(this.maxIndexBytes, entries * ENTRY_SIZE));
        }
    }

    private void map(RandomAccessFile raf, int bytes) throws IOException {
        raf.setLength(bytes);
        // The mapping stays valid after the file is closed.
        mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, bytes);
    }

    public int entries() {
        return entries;
    }

    public boolean isFull() {
        return (entries + 1) * ENTRY_SIZE > mmap.capacity();
    }

    public long baseOffset() {
        return baseOffset;
    }

    /** Appends an entry; offsets must be strictly increasing. */
    public void append(long offset, int position) {
        if (isFull()) {
            throw new IllegalStateException("Index full: " + file);
        }
        if (entries > 0 && offset <= entryAt(entries - 1).offset()) {
            throw new IllegalArgumentException("Index offsets must increase: " + offset);
        }
        int at = entries * ENTRY_SIZE;
        mmap.putInt(at, (int) (offset - baseOffset));
        mmap.putInt(at + 4, position);
        entries++;
    }

    /**
     * Returns the entry with the greatest offset &lt;= target, or (baseOffset, 0) if there is none.
     * Classic binary search over fixed-size entries in the mapped buffer.
     */
    public Entry lookup(long target) {
        int lo = 0;
        int hi = entries - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long midOffset = entryAt(mid).offset();
            if (midOffset <= target) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found == -1 ? new Entry(baseOffset, 0) : entryAt(found);
    }

    public Entry entryAt(int i) {
        int at = i * ENTRY_SIZE;
        return new Entry(baseOffset + mmap.getInt(at), mmap.getInt(at + 4));
    }

    public Entry lastEntry() {
        return entries == 0 ? new Entry(baseOffset, 0) : entryAt(entries - 1);
    }

    /** Removes every entry whose offset is &gt;= {@code offset}. */
    public void truncateTo(long offset) {
        int keep = 0;
        while (keep < entries && entryAt(keep).offset() < offset) {
            keep++;
        }
        entries = keep;
    }

    public void reset() {
        entries = 0;
    }

    /**
     * Checks invariants after loading: offsets strictly increase and positions are inside the log.
     * A failed check means the index must be rebuilt from the log.
     */
    public boolean isValid(long logSizeBytes) {
        long prevOffset = -1;
        int prevPos = -1;
        for (int i = 0; i < entries; i++) {
            Entry e = entryAt(i);
            if (e.offset() <= prevOffset || e.position() <= prevPos && i > 0 || e.position() >= logSizeBytes
                    || e.offset() < baseOffset) {
                return false;
            }
            prevOffset = e.offset();
            prevPos = e.position();
        }
        return true;
    }

    public void flush() {
        mmap.force();
    }

    /** Shrinks the file to the bytes actually used (called when a segment stops being active). */
    public void trimToValidSize() throws IOException {
        resize(entries * ENTRY_SIZE);
    }

    /** Re-grows the file so it can accept appends again (a segment became active after truncation). */
    public void ensureWritable() throws IOException {
        if (mmap.capacity() < maxIndexBytes) {
            resize(maxIndexBytes);
        }
    }

    private void resize(int bytes) throws IOException {
        mmap.force();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            map(raf, bytes);
        }
    }

    @Override
    public void close() throws IOException {
        trimToValidSize();
    }

    public Path file() {
        return file;
    }
}
