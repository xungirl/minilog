package dev.minilog.protocol;

import java.io.DataOutputStream;
import java.io.IOException;

/** A request or response body. Each implementation also has a static {@code read(DataInputStream)}. */
public interface Message {
    void write(DataOutputStream out) throws IOException;
}
