package dev.minilog.protocol;

import dev.minilog.common.TopicPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Primitive encodings shared by all messages. All integers are big-endian (Java's default).
 * <ul>
 *   <li>string: int16 length (-1 = null) + UTF-8 bytes</li>
 *   <li>bytes: int32 length (-1 = null) + raw bytes</li>
 *   <li>list: int32 count + elements</li>
 * </ul>
 */
public final class Wire {
    private Wire() {
    }

    @FunctionalInterface
    public interface Writer<T> {
        void write(DataOutputStream out, T value) throws IOException;
    }

    @FunctionalInterface
    public interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }

    public static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeShort(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String too long");
        }
        out.writeShort(b.length);
        out.write(b);
    }

    public static String readString(DataInputStream in) throws IOException {
        short len = in.readShort();
        if (len < 0) {
            return null;
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    public static void writeBytes(DataOutputStream out, byte[] b) throws IOException {
        if (b == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(b.length);
        out.write(b);
    }

    public static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    public static <T> void writeList(DataOutputStream out, List<T> list, Writer<T> writer) throws IOException {
        out.writeInt(list.size());
        for (T t : list) {
            writer.write(out, t);
        }
    }

    public static <T> List<T> readList(DataInputStream in, Reader<T> reader) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > 10_000_000) {
            throw new IOException("Invalid list length " + n);
        }
        List<T> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(reader.read(in));
        }
        return list;
    }

    public static void writeIntList(DataOutputStream out, List<Integer> list) throws IOException {
        writeList(out, list, DataOutputStream::writeInt);
    }

    public static List<Integer> readIntList(DataInputStream in) throws IOException {
        return readList(in, DataInputStream::readInt);
    }

    public static void writeStringList(DataOutputStream out, List<String> list) throws IOException {
        writeList(out, list, Wire::writeString);
    }

    public static List<String> readStringList(DataInputStream in) throws IOException {
        return readList(in, Wire::readString);
    }

    public static void writeTopicPartition(DataOutputStream out, TopicPartition tp) throws IOException {
        writeString(out, tp.topic());
        out.writeInt(tp.partition());
    }

    public static TopicPartition readTopicPartition(DataInputStream in) throws IOException {
        return new TopicPartition(readString(in), in.readInt());
    }
}
