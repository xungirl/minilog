package dev.minilog.protocol;

import dev.minilog.common.Errors;
import dev.minilog.common.TopicPartition;
import dev.minilog.storage.SimpleRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * All request and response bodies, grouped by API. Each is an immutable record with a
 * {@code write} method and a static {@code read} method that mirror each other field by field.
 * Error codes are sent as int16 ({@link Errors#code()}).
 */
public final class Messages {
    private Messages() {
    }

    private static void writeError(DataOutputStream out, Errors e) throws IOException {
        out.writeShort(e.code());
    }

    private static Errors readError(DataInputStream in) throws IOException {
        return Errors.fromCode(in.readShort());
    }

    // =================================================================== PRODUCE

    /**
     * @param acks       0 = no response, 1 = leader appended, -1 = all in-sync replicas have it
     * @param producerId idempotent producer id, or -1
     */
    public record ProduceRequest(short acks, int timeoutMs, long producerId, List<PartitionBatch> batches)
            implements Message {
        /** One batch of records for one partition; record i has sequence baseSequence + i. */
        public record PartitionBatch(String topic, int partition, int baseSequence, List<SimpleRecord> records) {
            public TopicPartition tp() {
                return new TopicPartition(topic, partition);
            }
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            out.writeShort(acks);
            out.writeInt(timeoutMs);
            out.writeLong(producerId);
            Wire.writeList(out, batches, (o, b) -> {
                Wire.writeString(o, b.topic());
                o.writeInt(b.partition());
                o.writeInt(b.baseSequence());
                Wire.writeList(o, b.records(), (o2, r) -> {
                    o2.writeLong(r.timestamp());
                    Wire.writeBytes(o2, r.key());
                    Wire.writeBytes(o2, r.value());
                });
            });
        }

        public static ProduceRequest read(DataInputStream in) throws IOException {
            short acks = in.readShort();
            int timeout = in.readInt();
            long pid = in.readLong();
            List<PartitionBatch> batches = Wire.readList(in, i -> new PartitionBatch(Wire.readString(i), i.readInt(),
                    i.readInt(), Wire.readList(i, i2 -> new SimpleRecord(i2.readLong(), Wire.readBytes(i2), Wire.readBytes(i2)))));
            return new ProduceRequest(acks, timeout, pid, batches);
        }
    }

    public record ProduceResponse(List<PartitionResult> results) implements Message {
        public record PartitionResult(String topic, int partition, Errors error, long baseOffset) {
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeList(out, results, (o, r) -> {
                Wire.writeString(o, r.topic());
                o.writeInt(r.partition());
                writeError(o, r.error());
                o.writeLong(r.baseOffset());
            });
        }

        public static ProduceResponse read(DataInputStream in) throws IOException {
            return new ProduceResponse(Wire.readList(in, i -> new PartitionResult(Wire.readString(i), i.readInt(),
                    readError(i), i.readLong())));
        }
    }

    // =================================================================== FETCH

    /**
     * @param replicaId -1 for consumers; the follower's broker id for replication fetches
     * @param maxWaitMs long-poll: how long the leader may wait for new data before answering
     */
    public record FetchRequest(int replicaId, int maxWaitMs, List<FetchPartition> partitions) implements Message {
        /** @param currentLeaderEpoch epoch the fetcher believes the leader has, or -1 to skip fencing */
        public record FetchPartition(String topic, int partition, long fetchOffset, int maxBytes, int currentLeaderEpoch) {
            public TopicPartition tp() {
                return new TopicPartition(topic, partition);
            }
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            out.writeInt(replicaId);
            out.writeInt(maxWaitMs);
            Wire.writeList(out, partitions, (o, p) -> {
                Wire.writeString(o, p.topic());
                o.writeInt(p.partition());
                o.writeLong(p.fetchOffset());
                o.writeInt(p.maxBytes());
                o.writeInt(p.currentLeaderEpoch());
            });
        }

        public static FetchRequest read(DataInputStream in) throws IOException {
            int replicaId = in.readInt();
            int maxWait = in.readInt();
            return new FetchRequest(replicaId, maxWait, Wire.readList(in, i -> new FetchPartition(Wire.readString(i),
                    i.readInt(), i.readLong(), i.readInt(), i.readInt())));
        }
    }

    public record FetchResponse(List<PartitionData> partitions) implements Message {
        /** @param records raw log bytes (see RecordCodec); decoded and CRC-checked by the receiver */
        public record PartitionData(String topic, int partition, Errors error, long highWatermark,
                                    long logStartOffset, long logEndOffset, byte[] records) {
            public TopicPartition tp() {
                return new TopicPartition(topic, partition);
            }
        }

        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeList(out, partitions, (o, p) -> {
                Wire.writeString(o, p.topic());
                o.writeInt(p.partition());
                writeError(o, p.error());
                o.writeLong(p.highWatermark());
                o.writeLong(p.logStartOffset());
                o.writeLong(p.logEndOffset());
                Wire.writeBytes(o, p.records());
            });
        }

        public static FetchResponse read(DataInputStream in) throws IOException {
            return new FetchResponse(Wire.readList(in, i -> new PartitionData(Wire.readString(i), i.readInt(),
                    readError(i), i.readLong(), i.readLong(), i.readLong(), Wire.readBytes(i))));
        }
    }

    // =================================================================== LIST_OFFSETS

    public static final long LATEST = -1;
    public static final long EARLIEST = -2;

    /** @param timestamp {@link #LATEST} (high watermark) or {@link #EARLIEST} (log start offset) */
    public record ListOffsetsRequest(String topic, int partition, long timestamp) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, topic);
            out.writeInt(partition);
            out.writeLong(timestamp);
        }

        public static ListOffsetsRequest read(DataInputStream in) throws IOException {
            return new ListOffsetsRequest(Wire.readString(in), in.readInt(), in.readLong());
        }
    }

    public record ListOffsetsResponse(Errors error, long offset) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            out.writeLong(offset);
        }

        public static ListOffsetsResponse read(DataInputStream in) throws IOException {
            return new ListOffsetsResponse(readError(in), in.readLong());
        }
    }

    // =================================================================== METADATA

    public record MetadataRequest() implements Message {
        @Override
        public void write(DataOutputStream out) {
        }

        public static MetadataRequest read(DataInputStream in) {
            return new MetadataRequest();
        }
    }

    public record MetadataResponse(ClusterMetadata metadata) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            metadata.write(out);
        }

        public static MetadataResponse read(DataInputStream in) throws IOException {
            return new MetadataResponse(ClusterMetadata.read(in));
        }
    }

    // =================================================================== CREATE_TOPIC

    public record CreateTopicRequest(String topic, int partitions, short replicationFactor) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, topic);
            out.writeInt(partitions);
            out.writeShort(replicationFactor);
        }

        public static CreateTopicRequest read(DataInputStream in) throws IOException {
            return new CreateTopicRequest(Wire.readString(in), in.readInt(), in.readShort());
        }
    }

    public record CreateTopicResponse(Errors error, String message) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            Wire.writeString(out, message);
        }

        public static CreateTopicResponse read(DataInputStream in) throws IOException {
            return new CreateTopicResponse(readError(in), Wire.readString(in));
        }
    }

    // =================================================================== OFFSET_COMMIT / OFFSET_FETCH

    public record PartitionOffset(String topic, int partition, long offset) {
        public TopicPartition tp() {
            return new TopicPartition(topic, partition);
        }

        static void write(DataOutputStream o, PartitionOffset p) throws IOException {
            Wire.writeString(o, p.topic());
            o.writeInt(p.partition());
            o.writeLong(p.offset());
        }

        static PartitionOffset read(DataInputStream i) throws IOException {
            return new PartitionOffset(Wire.readString(i), i.readInt(), i.readLong());
        }
    }

    /** @param generation the group generation the member belongs to, or -1 for a group-less commit */
    public record OffsetCommitRequest(String group, String memberId, int generation, List<PartitionOffset> offsets)
            implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, group);
            Wire.writeString(out, memberId);
            out.writeInt(generation);
            Wire.writeList(out, offsets, PartitionOffset::write);
        }

        public static OffsetCommitRequest read(DataInputStream in) throws IOException {
            return new OffsetCommitRequest(Wire.readString(in), Wire.readString(in), in.readInt(),
                    Wire.readList(in, PartitionOffset::read));
        }
    }

    public record OffsetCommitResponse(Errors error) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
        }

        public static OffsetCommitResponse read(DataInputStream in) throws IOException {
            return new OffsetCommitResponse(readError(in));
        }
    }

    public record OffsetFetchRequest(String group, List<TopicPartition> partitions) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, group);
            Wire.writeList(out, partitions, Wire::writeTopicPartition);
        }

        public static OffsetFetchRequest read(DataInputStream in) throws IOException {
            return new OffsetFetchRequest(Wire.readString(in), Wire.readList(in, Wire::readTopicPartition));
        }
    }

    /** Offsets are -1 for partitions with no committed offset. */
    public record OffsetFetchResponse(List<PartitionOffset> offsets) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeList(out, offsets, PartitionOffset::write);
        }

        public static OffsetFetchResponse read(DataInputStream in) throws IOException {
            return new OffsetFetchResponse(Wire.readList(in, PartitionOffset::read));
        }
    }

    // =================================================================== consumer groups

    /** @param memberId empty on the first join; the coordinator assigns one */
    public record JoinGroupRequest(String group, String memberId, int sessionTimeoutMs, int rebalanceTimeoutMs,
                                   List<String> topics) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, group);
            Wire.writeString(out, memberId);
            out.writeInt(sessionTimeoutMs);
            out.writeInt(rebalanceTimeoutMs);
            Wire.writeStringList(out, topics);
        }

        public static JoinGroupRequest read(DataInputStream in) throws IOException {
            return new JoinGroupRequest(Wire.readString(in), Wire.readString(in), in.readInt(), in.readInt(),
                    Wire.readStringList(in));
        }
    }

    public record JoinGroupResponse(Errors error, int generation, String memberId, List<TopicPartition> assignment)
            implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            out.writeInt(generation);
            Wire.writeString(out, memberId);
            Wire.writeList(out, assignment, Wire::writeTopicPartition);
        }

        public static JoinGroupResponse read(DataInputStream in) throws IOException {
            return new JoinGroupResponse(readError(in), in.readInt(), Wire.readString(in),
                    Wire.readList(in, Wire::readTopicPartition));
        }
    }

    public record HeartbeatRequest(String group, String memberId, int generation) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, group);
            Wire.writeString(out, memberId);
            out.writeInt(generation);
        }

        public static HeartbeatRequest read(DataInputStream in) throws IOException {
            return new HeartbeatRequest(Wire.readString(in), Wire.readString(in), in.readInt());
        }
    }

    /** Reusable response that carries only an error code (Heartbeat, LeaveGroup). */
    public record ErrorResponse(Errors error) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
        }

        public static ErrorResponse read(DataInputStream in) throws IOException {
            return new ErrorResponse(readError(in));
        }
    }

    public record LeaveGroupRequest(String group, String memberId) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, group);
            Wire.writeString(out, memberId);
        }

        public static LeaveGroupRequest read(DataInputStream in) throws IOException {
            return new LeaveGroupRequest(Wire.readString(in), Wire.readString(in));
        }
    }

    // =================================================================== internal: controller

    /**
     * Sent by every broker to the controller every heartbeat interval. Doubles as registration:
     * a new {@code incarnationId} tells the controller the broker restarted.
     */
    public record BrokerHeartbeatRequest(int brokerId, long incarnationId, String host, int port,
                                         long knownMetadataVersion) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            out.writeInt(brokerId);
            out.writeLong(incarnationId);
            Wire.writeString(out, host);
            out.writeInt(port);
            out.writeLong(knownMetadataVersion);
        }

        public static BrokerHeartbeatRequest read(DataInputStream in) throws IOException {
            return new BrokerHeartbeatRequest(in.readInt(), in.readLong(), Wire.readString(in), in.readInt(), in.readLong());
        }
    }

    /** @param metadata newer metadata if the broker's version is stale, otherwise null */
    public record BrokerHeartbeatResponse(Errors error, ClusterMetadata metadata) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            out.writeBoolean(metadata != null);
            if (metadata != null) {
                metadata.write(out);
            }
        }

        public static BrokerHeartbeatResponse read(DataInputStream in) throws IOException {
            Errors e = readError(in);
            return new BrokerHeartbeatResponse(e, in.readBoolean() ? ClusterMetadata.read(in) : null);
        }
    }

    /** A partition leader asks the controller to change the ISR (shrink a lagging follower or add a caught-up one). */
    public record AlterIsrRequest(int brokerId, String topic, int partition, int leaderEpoch, int isrVersion,
                                  List<Integer> newIsr) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            out.writeInt(brokerId);
            Wire.writeString(out, topic);
            out.writeInt(partition);
            out.writeInt(leaderEpoch);
            out.writeInt(isrVersion);
            Wire.writeIntList(out, newIsr);
        }

        public static AlterIsrRequest read(DataInputStream in) throws IOException {
            return new AlterIsrRequest(in.readInt(), Wire.readString(in), in.readInt(), in.readInt(), in.readInt(),
                    Wire.readIntList(in));
        }
    }

    public record AlterIsrResponse(Errors error, int leaderEpoch, int isrVersion, List<Integer> isr) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            out.writeInt(leaderEpoch);
            out.writeInt(isrVersion);
            Wire.writeIntList(out, isr);
        }

        public static AlterIsrResponse read(DataInputStream in) throws IOException {
            return new AlterIsrResponse(readError(in), in.readInt(), in.readInt(), Wire.readIntList(in));
        }
    }

    // =================================================================== internal: replication

    /** A new follower asks the leader where {@code leaderEpoch} ends in the leader's log. */
    public record OffsetForLeaderEpochRequest(String topic, int partition, int leaderEpoch) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, topic);
            out.writeInt(partition);
            out.writeInt(leaderEpoch);
        }

        public static OffsetForLeaderEpochRequest read(DataInputStream in) throws IOException {
            return new OffsetForLeaderEpochRequest(Wire.readString(in), in.readInt(), in.readInt());
        }
    }

    public record OffsetForLeaderEpochResponse(Errors error, int leaderEpoch, long endOffset) implements Message {
        @Override
        public void write(DataOutputStream out) throws IOException {
            writeError(out, error);
            out.writeInt(leaderEpoch);
            out.writeLong(endOffset);
        }

        public static OffsetForLeaderEpochResponse read(DataInputStream in) throws IOException {
            return new OffsetForLeaderEpochResponse(readError(in), in.readInt(), in.readLong());
        }
    }
}
