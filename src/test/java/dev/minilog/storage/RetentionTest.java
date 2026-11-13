package dev.minilog.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetentionTest {
    @TempDir
    Path dir;

    @Test
    void timeBasedRetentionDeletesOnlyExpiredInactiveSegments() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(5_000).withRetention(60_000, -1);
        long now = System.currentTimeMillis();
        try (Log log = Log.open(dir, cfg, true)) {
            // 200 "old" records (2 hours ago), then 50 fresh ones
            for (int i = 0; i < 200; i++) {
                log.appendAsLeader(-1, -1, StorageTestUtil.batch(i, 1, 100, now - 7_200_000), 0);
            }
            int oldSegments = log.segmentCount();
            for (int i = 200; i < 250; i++) {
                log.appendAsLeader(-1, -1, StorageTestUtil.batch(i, 1, 100, now), 0);
            }
            int before = log.segmentCount();
            int deleted = log.deleteOldSegments(now, Long.MAX_VALUE);
            assertTrue(deleted > 0);
            assertTrue(deleted <= oldSegments);
            assertEquals(before - deleted, log.segmentCount());
            assertTrue(log.logStartOffset() > 0);
            assertTrue(log.logStartOffset() <= 200, "fresh records must survive");
            assertThrows(OffsetOutOfRangeException.class, () -> log.read(0, 100, Long.MAX_VALUE));
            assertEquals(249, StorageTestUtil.readAll(log).getLast().offset());
        }
    }

    @Test
    void sizeBasedRetentionKeepsLogUnderLimit() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(5_000).withRetention(-1, 20_000);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 1000, 100);
            assertTrue(log.sizeInBytes() > 100_000);
            log.deleteOldSegments(System.currentTimeMillis(), Long.MAX_VALUE);
            assertTrue(log.sizeInBytes() <= 20_000 + 5_000, "size after retention: " + log.sizeInBytes());
            assertEquals(1000, log.logEndOffset());
        }
    }

    @Test
    void retentionNeverDeletesAboveTheHighWatermark() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(5_000).withRetention(-1, 0);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 500, 100);
            log.deleteOldSegments(System.currentTimeMillis(), 100); // pretend HW = 100
            assertTrue(log.logStartOffset() <= 100);
            assertTrue(log.logStartOffset() > 0);
        }
    }

    @Test
    void activeSegmentIsNeverDeleted() throws Exception {
        LogConfig cfg = LogConfig.defaults().withRetention(0, 0);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 10, 100);
            assertEquals(0, log.deleteOldSegments(System.currentTimeMillis() + 10_000, Long.MAX_VALUE));
            assertEquals(10, StorageTestUtil.readAll(log).size());
        }
    }
}
