package dev.minilog.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CorruptionAndRecoveryTest {
    @TempDir
    Path dir;

    @Test
    void crcDetectsASingleFlippedBit() {
        Record r = new Record(42, 1000, 3, 7, 9, "key".getBytes(), "hello world".getBytes());
        ByteBuffer buf = ByteBuffer.allocate(RecordCodec.sizeInBytes(r));
        RecordCodec.write(buf, r);
        byte[] bytes = buf.array();
        Record decoded = RecordCodec.read(ByteBuffer.wrap(bytes));
        assertEquals(42, decoded.offset());
        assertArrayEquals("hello world".getBytes(), decoded.value());

        for (int pos = RecordCodec.OFFSET_POS; pos < bytes.length; pos++) {
            byte[] copy = bytes.clone();
            copy[pos] ^= 0x01;
            assertThrows(CorruptRecordException.class, () -> RecordCodec.read(ByteBuffer.wrap(copy)),
                    "flip at byte " + bytesToString(pos));
        }
    }

    private static String bytesToString(int pos) {
        return Integer.toString(pos);
    }

    @Test
    void corruptRecordOnDiskIsDetectedOnRead() throws Exception {
        LogConfig cfg = LogConfig.defaults();
        Path segmentFile;
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 10, 50);
            segmentFile = log.segments().getFirst().logFile();
        }
        // flip one byte inside the value of the 5th record
        int recordSize = RecordCodec.sizeInBytes("k1".getBytes(), new byte[50]);
        try (RandomAccessFile raf = new RandomAccessFile(segmentFile.toFile(), "rw")) {
            long pos = 4L * recordSize + recordSize - 5;
            raf.seek(pos);
            int b = raf.read();
            raf.seek(pos);
            raf.write(b ^ 0xFF);
        }
        try (Log log = Log.open(dir, cfg, true)) {
            byte[] raw = log.read(0, 1 << 20, Long.MAX_VALUE);
            assertThrows(CorruptRecordException.class, () -> RecordCodec.decodeAll(raw));
            // records before the damaged one are still readable
            assertEquals(4, RecordCodec.decodeAll(log.read(0, 4 * recordSize, Long.MAX_VALUE)).size());
        }
    }

    @Test
    void recoveryTruncatesTornTailWrite() throws Exception {
        LogConfig cfg = LogConfig.defaults().withIndexIntervalBytes(256);
        Path segmentFile;
        long sizeBefore;
        Log log = Log.open(dir, cfg, true);
        StorageTestUtil.appendMessages(log, 100, 100);
        segmentFile = log.segments().getLast().logFile();
        log.closeWithoutFlush(); // simulate a crash: index left untrimmed, no clean marker
        sizeBefore = Files.size(segmentFile);

        // A torn write: the first 30 bytes of what would have been record 100.
        Record partial = new Record(100, 1, 0, -1, -1, "k".getBytes(), new byte[100]);
        ByteBuffer buf = ByteBuffer.allocate(RecordCodec.sizeInBytes(partial));
        RecordCodec.write(buf, partial);
        Files.write(segmentFile, java.util.Arrays.copyOf(buf.array(), 30), StandardOpenOption.APPEND);
        assertEquals(sizeBefore + 30, Files.size(segmentFile));

        try (Log recovered = Log.open(dir, cfg, false)) {
            assertEquals(100, recovered.logEndOffset());
            assertEquals(sizeBefore, Files.size(segmentFile), "torn bytes must be truncated");
            List<Record> all = StorageTestUtil.readAll(recovered);
            assertEquals(100, all.size());
            assertTrue(recovered.segments().getLast().indexEntries() > 0, "index rebuilt");
            // the log accepts new appends at the right offset after recovery
            StorageTestUtil.appendMessages(recovered, 5, 100);
            assertEquals(105, recovered.logEndOffset());
            assertEquals(104, StorageTestUtil.readAll(recovered).getLast().offset());
        }
    }

    @Test
    void recoveryTruncatesAtCorruptTailRecord() throws Exception {
        LogConfig cfg = LogConfig.defaults();
        Log log = Log.open(dir, cfg, true);
        StorageTestUtil.appendMessages(log, 20, 100);
        Path segmentFile = log.segments().getLast().logFile();
        log.closeWithoutFlush();
        long size = Files.size(segmentFile);
        try (RandomAccessFile raf = new RandomAccessFile(segmentFile.toFile(), "rw")) {
            raf.seek(size - 10); // inside the last record's value
            raf.write(0x7F);
        }
        try (Log recovered = Log.open(dir, cfg, false)) {
            assertEquals(19, recovered.logEndOffset(), "last (corrupt) record dropped");
            assertEquals(19, StorageTestUtil.readAll(recovered).size());
        }
    }

    @Test
    void missingOrBrokenIndexIsRebuilt() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(30_000).withIndexIntervalBytes(512);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 600, 100);
        }
        // Corrupt the first segment's index with garbage (decreasing offsets).
        Path firstIndex;
        try (var files = Files.list(dir)) {
            firstIndex = files.filter(p -> p.toString().endsWith(".index")).sorted().findFirst().orElseThrow();
        }
        byte[] garbage = new byte[64];
        for (int i = 0; i < garbage.length; i++) {
            garbage[i] = (byte) (255 - i);
        }
        Files.write(firstIndex, garbage);
        try (Log log = Log.open(dir, cfg, true)) {
            assertEquals(600, log.logEndOffset());
            for (long off = 0; off < 600; off += 13) {
                assertEquals(off, RecordCodec.decodeAll(log.read(off, 1, Long.MAX_VALUE)).getFirst().offset());
            }
        }
    }
}
