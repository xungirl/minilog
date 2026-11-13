package dev.minilog.storage;

/** Thrown when a read asks for an offset below the log start or above the log end. */
public class OffsetOutOfRangeException extends RuntimeException {
    public OffsetOutOfRangeException(String message) {
        super(message);
    }
}
