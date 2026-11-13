package dev.minilog.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Binary format of a record. The same bytes are used on disk and on the wire, so the broker can
 * serve a fetch by copying file bytes without re-encoding (the same trick Kafka uses).
 *
 * <pre>
 * +--------+--------+--------+-----------+-------------+------------+----------+--------+-----+----------+-------+
 * | length | crc    | offset | timestamp | leaderEpoch | producerId | sequence | keyLen | key | valueLen | value |
 * | int32  | uint32 | int64  | int64     | int32       | int64      | int32    | int32  |     | int32    |       |
 * +--------+--------+--------+-----------+-------------+------------+----------+--------+-----+----------+-------+
 * </pre>
 *
 * {@code length} counts every byte after the length field. {@code crc} is CRC-32C over every byte
 * after the crc field, so any bit flip in offset, metadata, key or value is detected.
 * keyLen/valueLen of -1 encode null.
 */
public final class RecordCodec {
    public static final int LENGTH_SIZE = 4;
    public static final int CRC_SIZE = 4;
    /** Offset of the 'offset' field from the start of a record. */
    public static final int OFFSET_POS = LENGTH_SIZE + CRC_SIZE;
    /** Fixed bytes per record (everything except key and value bytes). */
    public static final int OVERHEAD = 4 + 4 + 8 + 8 + 4 + 8 + 4 + 4 + 4;
    /** Bytes we must read to learn a record's size and offset when scanning a log. */
    public static final int SCAN_HEADER = LENGTH_SIZE + CRC_SIZE + 8;

    private RecordCodec() {
    }

    public static int sizeInBytes(byte[] key, byte[] value) {
        return OVERHEAD + (key == null ? 0 : key.length) + (value == null ? 0 : value.length);
    }

    public static int sizeInBytes(Record r) {
        return sizeInBytes(r.key(), r.value());
    }

    /** Writes one record at the buffer's position and advances it. */
    public static void write(ByteBuffer buf, Record r) {
        int start = buf.position();
        int size = sizeInBytes(r);
        buf.putInt(size - LENGTH_SIZE);
        buf.putInt(0); // crc placeholder, filled in below
        buf.putLong(r.offset());
        buf.putLong(r.timestamp());
        buf.putInt(r.leaderEpoch());
        buf.putLong(r.producerId());
        buf.putInt(r.sequence());
        putBytes(buf, r.key());
        putBytes(buf, r.value());
        int crc = crc(buf, start + OFFSET_POS, buf.position());
        buf.putInt(start + LENGTH_SIZE, crc);
    }

    /**
     * Reads and validates one record at the buffer's position, advancing past it.
     *
     * @throws CorruptRecordException if the record is truncated or its CRC does not match
     */
    public static Record read(ByteBuffer buf) {
        int start = buf.position();
        if (buf.remaining() < LENGTH_SIZE) {
            throw new CorruptRecordException("Truncated record header at " + start);
        }
        int length = buf.getInt();
        if (length < OVERHEAD - LENGTH_SIZE || length > buf.remaining()) {
            throw new CorruptRecordException("Invalid record length " + length + " at " + start);
        }
        int storedCrc = buf.getInt();
        int end = start + LENGTH_SIZE + length;
        int actualCrc = crc(buf, start + OFFSET_POS, end);
        if (storedCrc != actualCrc) {
            throw new CorruptRecordException("CRC mismatch at position " + start
                    + ": stored=" + Integer.toHexString(storedCrc) + " computed=" + Integer.toHexString(actualCrc));
        }
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        int epoch = buf.getInt();
        long producerId = buf.getLong();
        int sequence = buf.getInt();
        byte[] key = getBytes(buf, end);
        byte[] value = getBytes(buf, end);
        if (buf.position() != end) {
            throw new CorruptRecordException("Record length does not match contents at " + start);
        }
        return new Record(offset, timestamp, epoch, producerId, sequence, key, value);
    }

    /** Decodes and validates every record in a byte array (e.g. a fetch response). */
    public static List<Record> decodeAll(byte[] bytes) {
        List<Record> records = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        while (buf.hasRemaining()) {
            records.add(read(buf));
        }
        return records;
    }

    /** Encodes records into one contiguous buffer, ready to write to a file in a single call. */
    public static ByteBuffer encodeAll(List<Record> records) {
        int total = 0;
        for (Record r : records) {
            total += sizeInBytes(r);
        }
        ByteBuffer buf = ByteBuffer.allocate(total);
        for (Record r : records) {
            write(buf, r);
        }
        buf.flip();
        return buf;
    }

    private static int crc(ByteBuffer buf, int from, int to) {
        CRC32C crc = new CRC32C();
        ByteBuffer slice = buf.duplicate();
        slice.limit(to).position(from);
        crc.update(slice);
        return (int) crc.getValue();
    }

    private static void putBytes(ByteBuffer buf, byte[] bytes) {
        if (bytes == null) {
            buf.putInt(-1);
        } else {
            buf.putInt(bytes.length);
            buf.put(bytes);
        }
    }

    private static byte[] getBytes(ByteBuffer buf, int end) {
        int len = buf.getInt();
        if (len == -1) {
            return null;
        }
        if (len < 0 || buf.position() + len > end) {
            throw new CorruptRecordException("Invalid field length " + len);
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }
}
