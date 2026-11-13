package dev.minilog.storage;

/** A record as the producer sends it: no offset yet (the leader assigns offsets). */
public record SimpleRecord(long timestamp, byte[] key, byte[] value) {
}
