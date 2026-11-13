package dev.minilog.storage;

/** Thrown when a record fails its CRC check or its framing is invalid. */
public class CorruptRecordException extends RuntimeException {
    public CorruptRecordException(String message) {
        super(message);
    }
}
