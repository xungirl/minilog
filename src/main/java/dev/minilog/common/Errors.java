package dev.minilog.common;

/**
 * Error codes carried in protocol responses. Codes are minilog's own (not Kafka's numbering).
 * {@link #retriable()} tells clients whether retrying the same request may succeed.
 */
public enum Errors {
    NONE(0, false),
    UNKNOWN_SERVER_ERROR(1, false),
    CORRUPT_MESSAGE(2, true),
    UNKNOWN_TOPIC_OR_PARTITION(3, true),
    OFFSET_OUT_OF_RANGE(4, false),
    NOT_LEADER(5, true),
    REQUEST_TIMED_OUT(6, true),
    NOT_ENOUGH_REPLICAS(7, true),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(8, true),
    TOPIC_ALREADY_EXISTS(9, false),
    INVALID_REQUEST(10, false),
    INVALID_REPLICATION_FACTOR(11, false),
    OUT_OF_ORDER_SEQUENCE(12, false),
    FENCED_LEADER_EPOCH(13, true),
    ILLEGAL_GENERATION(14, false),
    UNKNOWN_MEMBER_ID(15, false),
    REBALANCE_IN_PROGRESS(16, false),
    STALE_ISR_VERSION(17, false),
    LEADER_NOT_AVAILABLE(18, true);

    private final short code;
    private final boolean retriable;

    Errors(int code, boolean retriable) {
        this.code = (short) code;
        this.retriable = retriable;
    }

    public short code() {
        return code;
    }

    public boolean retriable() {
        return retriable;
    }

    public static Errors fromCode(short code) {
        for (Errors e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        return UNKNOWN_SERVER_ERROR;
    }
}
