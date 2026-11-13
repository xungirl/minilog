package dev.minilog.common;

/** An exception that carries a protocol {@link Errors} code so it can be returned to a client. */
public class MiniLogException extends RuntimeException {
    private final Errors error;

    public MiniLogException(Errors error, String message) {
        super(error + ": " + message);
        this.error = error;
    }

    public Errors error() {
        return error;
    }
}
