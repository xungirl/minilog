package dev.minilog.storage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class StorageTestUtil {
    private StorageTestUtil() {
    }

    static List<SimpleRecord> batch(int from, int count, int valueSize, long timestamp) {
        List<SimpleRecord> out = new ArrayList<>();
        for (int i = from; i < from + count; i++) {
            out.add(new SimpleRecord(timestamp, ("k" + i).getBytes(StandardCharsets.UTF_8), value(i, valueSize)));
        }
        return out;
    }

    static byte[] value(int i, int size) {
        byte[] prefix = ("v" + i + ":").getBytes(StandardCharsets.UTF_8);
        byte[] v = new byte[Math.max(size, prefix.length)];
        System.arraycopy(prefix, 0, v, 0, prefix.length);
        return v;
    }

    static String valuePrefix(Record r) {
        String s = new String(r.value(), StandardCharsets.UTF_8);
        return s.substring(0, s.indexOf(':'));
    }

    static void appendMessages(Log log, int count, int valueSize) {
        for (int i = 0; i < count; i++) {
            log.appendAsLeader(-1, -1, batch((int) log.logEndOffset(), 1, valueSize, System.currentTimeMillis()), 0);
        }
    }

    static List<Record> readAll(Log log) {
        List<Record> out = new ArrayList<>();
        long offset = log.logStartOffset();
        while (offset < log.logEndOffset()) {
            List<Record> chunk = RecordCodec.decodeAll(log.read(offset, 64 * 1024, Long.MAX_VALUE));
            out.addAll(chunk);
            offset = chunk.getLast().offset() + 1;
        }
        return out;
    }
}
