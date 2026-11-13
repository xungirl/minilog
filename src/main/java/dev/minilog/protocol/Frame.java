package dev.minilog.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * TCP framing: every message is {@code int32 size | payload}. Without a length prefix the receiver
 * could not tell where one message ends in a byte stream.
 */
public final class Frame {
    /** Upper bound so a corrupt or malicious size field cannot make us allocate gigabytes. */
    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private Frame() {
    }

    public static void write(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    public static byte[] read(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > MAX_FRAME_BYTES) {
            throw new IOException("Invalid frame size " + size);
        }
        byte[] payload = new byte[size];
        in.readFully(payload);
        return payload;
    }
}
