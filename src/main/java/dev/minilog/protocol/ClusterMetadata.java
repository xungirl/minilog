package dev.minilog.protocol;

import dev.minilog.common.TopicPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A versioned snapshot of cluster state, owned by the controller and cached by every broker and
 * client: which brokers are alive, and for each partition its replicas, leader, leader epoch and ISR.
 */
public record ClusterMetadata(long version, Endpoint controller, List<BrokerInfo> brokers, List<TopicMetadata> topics)
        implements Message {

    public static final ClusterMetadata EMPTY = new ClusterMetadata(-1, new Endpoint("", -1), List.of(), List.of());

    public record Endpoint(String host, int port) {
        void write(DataOutputStream out) throws IOException {
            Wire.writeString(out, host);
            out.writeInt(port);
        }

        static Endpoint read(DataInputStream in) throws IOException {
            return new Endpoint(Wire.readString(in), in.readInt());
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    public record BrokerInfo(int id, String host, int port) {
        public Endpoint endpoint() {
            return new Endpoint(host, port);
        }
    }

    /**
     * @param leader      broker id of the leader, or -1 if the partition is offline
     * @param leaderEpoch incremented on every leader change; used for fencing and log truncation
     * @param isrVersion  incremented on every ISR change; stale AlterIsr requests are rejected
     */
    public record PartitionMetadata(int partition, int leader, int leaderEpoch, int isrVersion,
                                    List<Integer> replicas, List<Integer> isr) {
    }

    public record TopicMetadata(String name, List<PartitionMetadata> partitions) {
    }

    // ------------------------------------------------------------------ lookups

    public Optional<BrokerInfo> broker(int id) {
        return brokers.stream().filter(b -> b.id() == id).findFirst();
    }

    public Optional<TopicMetadata> topic(String name) {
        return topics.stream().filter(t -> t.name().equals(name)).findFirst();
    }

    public Optional<PartitionMetadata> partition(TopicPartition tp) {
        return topic(tp.topic()).flatMap(t -> t.partitions().stream()
                .filter(p -> p.partition() == tp.partition()).findFirst());
    }

    public Map<TopicPartition, PartitionMetadata> allPartitions() {
        Map<TopicPartition, PartitionMetadata> out = new HashMap<>();
        for (TopicMetadata t : topics) {
            for (PartitionMetadata p : t.partitions()) {
                out.put(new TopicPartition(t.name(), p.partition()), p);
            }
        }
        return out;
    }

    // ------------------------------------------------------------------ encoding

    @Override
    public void write(DataOutputStream out) throws IOException {
        out.writeLong(version);
        controller.write(out);
        Wire.writeList(out, brokers, (o, b) -> {
            o.writeInt(b.id());
            Wire.writeString(o, b.host());
            o.writeInt(b.port());
        });
        Wire.writeList(out, topics, (o, t) -> {
            Wire.writeString(o, t.name());
            Wire.writeList(o, t.partitions(), (o2, p) -> {
                o2.writeInt(p.partition());
                o2.writeInt(p.leader());
                o2.writeInt(p.leaderEpoch());
                o2.writeInt(p.isrVersion());
                Wire.writeIntList(o2, p.replicas());
                Wire.writeIntList(o2, p.isr());
            });
        });
    }

    public static ClusterMetadata read(DataInputStream in) throws IOException {
        long version = in.readLong();
        Endpoint controller = Endpoint.read(in);
        List<BrokerInfo> brokers = Wire.readList(in, i -> new BrokerInfo(i.readInt(), Wire.readString(i), i.readInt()));
        List<TopicMetadata> topics = Wire.readList(in, i -> new TopicMetadata(Wire.readString(i),
                Wire.readList(i, i2 -> new PartitionMetadata(i2.readInt(), i2.readInt(), i2.readInt(), i2.readInt(),
                        Wire.readIntList(i2), Wire.readIntList(i2)))));
        return new ClusterMetadata(version, controller, brokers, topics);
    }
}
