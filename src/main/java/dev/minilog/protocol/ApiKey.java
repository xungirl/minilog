package dev.minilog.protocol;

/** Every request type in the minilog protocol. See PROTOCOL.md for the byte layouts. */
public enum ApiKey {
    PRODUCE(0),
    FETCH(1),
    LIST_OFFSETS(2),
    METADATA(3),
    CREATE_TOPIC(4),
    OFFSET_COMMIT(5),
    OFFSET_FETCH(6),
    JOIN_GROUP(7),
    HEARTBEAT(8),
    LEAVE_GROUP(9),
    // ---- internal (broker <-> controller, broker <-> broker)
    BROKER_HEARTBEAT(10),
    ALTER_ISR(11),
    OFFSET_FOR_LEADER_EPOCH(12);

    private final short id;

    ApiKey(int id) {
        this.id = (short) id;
    }

    public short id() {
        return id;
    }

    public static ApiKey fromId(short id) {
        for (ApiKey k : values()) {
            if (k.id == id) {
                return k;
            }
        }
        throw new IllegalArgumentException("Unknown api key " + id);
    }
}
