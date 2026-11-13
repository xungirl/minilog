package dev.minilog.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTruncationTest {
    @TempDir
    Path dir;

    @Test
    void truncateAcrossSegmentsThenAppend() throws Exception {
        LogConfig cfg = LogConfig.defaults().withSegmentBytes(5_000).withIndexIntervalBytes(256);
        try (Log log = Log.open(dir, cfg, true)) {
            StorageTestUtil.appendMessages(log, 300, 100);
            int segs = log.segmentCount();
            log.truncateTo(123);
            assertEquals(123, log.logEndOffset());
            assertTrue(log.segmentCount() < segs);
            StorageTestUtil.appendMessages(log, 50, 100);
            List<Record> all = StorageTestUtil.readAll(log);
            assertEquals(173, all.size());
            for (int i = 0; i < all.size(); i++) {
                assertEquals(i, all.get(i).offset());
            }
        }
    }

    @Test
    void epochCacheTracksEpochStartsAndTruncation() throws Exception {
        try (Log log = Log.open(dir, LogConfig.defaults(), true)) {
            log.appendAsLeader(-1, -1, StorageTestUtil.batch(0, 10, 10, 1), 0);
            log.appendAsLeader(-1, -1, StorageTestUtil.batch(10, 5, 10, 1), 2);
            log.appendAsLeader(-1, -1, StorageTestUtil.batch(15, 5, 10, 1), 5);
            assertEquals(new LeaderEpochCache.EpochEndOffset(0, 10), log.endOffsetForEpoch(0));
            assertEquals(new LeaderEpochCache.EpochEndOffset(0, 10), log.endOffsetForEpoch(1));
            assertEquals(new LeaderEpochCache.EpochEndOffset(2, 15), log.endOffsetForEpoch(3));
            assertEquals(new LeaderEpochCache.EpochEndOffset(5, 20), log.endOffsetForEpoch(5));
            log.truncateTo(12);
            assertEquals(2, log.latestEpoch());
            assertEquals(new LeaderEpochCache.EpochEndOffset(2, 12), log.endOffsetForEpoch(7));
        }
    }

    @Test
    void producerStateDeduplicatesRetriesAndSurvivesRestart() throws Exception {
        try (Log log = Log.open(dir, LogConfig.defaults(), true)) {
            Log.AppendResult first = log.appendAsLeader(77, 0, StorageTestUtil.batch(0, 3, 10, 1), 0);
            Log.AppendResult retry = log.appendAsLeader(77, 0, StorageTestUtil.batch(0, 3, 10, 1), 0);
            assertTrue(retry.duplicate());
            assertEquals(first.baseOffset(), retry.baseOffset());
            assertEquals(3, log.logEndOffset());
        }
        try (Log log = Log.open(dir, LogConfig.defaults(), true)) {
            Log.AppendResult retry = log.appendAsLeader(77, 0, StorageTestUtil.batch(0, 3, 10, 1), 0);
            assertTrue(retry.duplicate(), "producer state is rebuilt from the log on restart");
            Log.AppendResult next = log.appendAsLeader(77, 3, StorageTestUtil.batch(3, 2, 10, 1), 0);
            assertEquals(3, next.baseOffset());
            var ex = org.junit.jupiter.api.Assertions.assertThrows(dev.minilog.common.MiniLogException.class,
                    () -> log.appendAsLeader(77, 9, StorageTestUtil.batch(9, 1, 10, 1), 0));
            assertInstanceOf(dev.minilog.common.MiniLogException.class, ex);
        }
    }
}
