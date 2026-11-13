package dev.minilog.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogSegmentTest {
    @TempDir
    Path dir;

    @Test
    void rollsSegmentsWhenSizeLimitIsReached() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(10_000).withIndexIntervalBytes(512);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 1000, 100); // ~150 bytes per record on disk
            assertEquals(1000, log.logEndOffset());
            assertTrue(log.segmentCount() > 10, "expected many segments, got " + log.segmentCount());
            for (LogSegment s : log.segments()) {
                assertTrue(s.size() <= 10_000, "segment over limit: " + s.size());
            }
            // segments are contiguous: each base offset is the previous segment's next offset
            List<LogSegment> segs = log.segments();
            for (int i = 1; i < segs.size(); i++) {
                assertEquals(segs.get(i - 1).nextOffset(), segs.get(i).baseOffset());
            }
        }
    }

    @Test
    void sparseIndexLookupFindsEveryOffset() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(1 << 20).withIndexIntervalBytes(1024);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 2000, 100);
            LogSegment seg = log.segments().getFirst();
            int records = 2000;
            assertTrue(seg.indexEntries() > 0);
            assertTrue(seg.indexEntries() < records / 5, "index should be sparse: " + seg.indexEntries());
            for (long target = 0; target < records; target += 37) {
                List<Record> got = RecordCodec.decodeAll(log.read(target, 1, Long.MAX_VALUE));
                assertEquals(1, got.size());
                assertEquals(target, got.getFirst().offset());
                assertEquals("v" + target, StorageTestUtil.valuePrefix(got.getFirst()));
            }
        }
    }

    @Test
    void indexLookupReturnsFloorEntry() throws Exception {
        Path f = dir.resolve("x.index");
        OffsetIndex idx = new OffsetIndex(f, 100, 4096);
        idx.append(110, 1000);
        idx.append(125, 2500);
        idx.append(160, 4000);
        assertEquals(new OffsetIndex.Entry(100, 0), idx.lookup(105));
        assertEquals(new OffsetIndex.Entry(110, 1000), idx.lookup(110));
        assertEquals(new OffsetIndex.Entry(125, 2500), idx.lookup(159));
        assertEquals(new OffsetIndex.Entry(160, 4000), idx.lookup(10_000));
        idx.close();
        // reopened after a clean close the trimmed file holds exactly 3 entries
        OffsetIndex reopened = new OffsetIndex(f, 100, 4096);
        assertEquals(3, reopened.entries());
        assertEquals(new OffsetIndex.Entry(125, 2500), reopened.lookup(130));
        reopened.close();
    }

    @Test
    void readRespectsMaxBytesAndMaxOffset() throws Exception {
        try (Log log = Log.open(dir, LogConfig.defaults(), true)) {
            StorageTestUtil.appendMessages(log, 100, 100);
            List<Record> upToHw = RecordCodec.decodeAll(log.read(10, 1 << 20, 20));
            assertEquals(10, upToHw.size());
            assertEquals(19, upToHw.getLast().offset());
            int one = RecordCodec.sizeInBytes(upToHw.getFirst());
            assertEquals(3, RecordCodec.decodeAll(log.read(0, one * 3 + 5, Long.MAX_VALUE)).size());
            // a record bigger than maxBytes is still returned so consumers make progress
            assertEquals(1, RecordCodec.decodeAll(log.read(0, 1, Long.MAX_VALUE)).size());
        }
    }

    @Test
    void reopenAfterCleanShutdownKeepsEverything() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(20_000);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 500, 100);
        }
        try (Log log = Log.open(dir, cfg, true)) {
            assertEquals(500, log.logEndOffset());
            List<Record> all = StorageTestUtil.readAll(log);
            assertEquals(500, all.size());
            for (int i = 0; i < 500; i++) {
                assertEquals(i, all.get(i).offset());
            }
            StorageTestUtil.appendMessages(log, 10, 100);
            assertEquals(510, log.logEndOffset());
        }
    }
}
