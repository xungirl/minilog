package dev.minilog.client;

import dev.minilog.common.Logger;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.protocol.Messages;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared client plumbing: bootstrap, metadata cache, and one connection per broker (plus one to
 * the controller, which hosts topic creation and the group coordinator).
 */
public final class ClusterClient implements Closeable {
    private static final Logger LOG = Logger.get("client");

    private final List<ClusterMetadata.Endpoint> bootstrap;
    private final int requestTimeoutMs;
    private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();
    private volatile ClusterMetadata metadata = ClusterMetadata.EMPTY;
    private volatile Connection controllerConnection;

    /** @param bootstrap comma-separated host:port list, e.g. {@code localhost:9090,localhost:9091} */
    public ClusterClient(String bootstrap, int requestTimeoutMs) {
        this.bootstrap = new ArrayList<>();
        for (String hp : bootstrap.split(",")) {
            String[] parts = hp.trim().split(":");
            this.bootstrap.add(new ClusterMetadata.Endpoint(parts[0], Integer.parseInt(parts[1])));
        }
        this.requestTimeoutMs = requestTimeoutMs;
    }

    /** Fetches fresh metadata from any reachable node (known brokers first, then bootstrap list). */
    public ClusterMetadata refreshMetadata() throws IOException {
        List<ClusterMetadata.Endpoint> candidates = new ArrayList<>();
        ClusterMetadata current = metadata;
        if (current.controller().port() > 0) {
            candidates.add(current.controller());
        }
        current.brokers().forEach(b -> candidates.add(b.endpoint()));
        candidates.addAll(bootstrap);
        IOException last = null;
        for (ClusterMetadata.Endpoint ep : candidates) {
            try (Connection c = new Connection(ep.host(), ep.port(), requestTimeoutMs)) {
                Messages.MetadataResponse resp = c.call(ApiKey.METADATA, new Messages.MetadataRequest());
                if (resp.metadata().version() >= metadata.version() || metadata.version() < 0) {
                    metadata = resp.metadata();
                }
                return metadata;
            } catch (IOException e) {
                last = e;
            }
        }
        throw last != null ? last : new IOException("no bootstrap servers");
    }

    public ClusterMetadata metadata() {
        return metadata;
    }

    public int partitionCount(String topic) throws IOException {
        Optional<ClusterMetadata.TopicMetadata> t = metadata.topic(topic);
        if (t.isEmpty()) {
            t = refreshMetadata().topic(topic);
        }
        return t.map(tm -> tm.partitions().size()).orElseThrow(() -> new IOException("Unknown topic " + topic));
    }

    /** Leader broker id for a partition, or -1 if unknown/offline. */
    public int leaderFor(TopicPartition tp) {
        return metadata.partition(tp).map(ClusterMetadata.PartitionMetadata::leader).orElse(-1);
    }

    public int leaderEpochFor(TopicPartition tp) {
        return metadata.partition(tp).map(ClusterMetadata.PartitionMetadata::leaderEpoch).orElse(-1);
    }

    /** Returns (creating if necessary) the connection to a broker. */
    public Connection connectionTo(int brokerId) throws IOException {
        Connection c = connections.get(brokerId);
        if (c != null && !c.isBroken()) {
            return c;
        }
        ClusterMetadata.BrokerInfo b = metadata.broker(brokerId)
                .orElseThrow(() -> new IOException("Broker " + brokerId + " not in metadata"));
        c = new Connection(b.host(), b.port(), requestTimeoutMs);
        connections.put(brokerId, c);
        return c;
    }

    /** A connection to the controller / group coordinator. */
    public Connection controller() throws IOException {
        Connection c = controllerConnection;
        if (c != null && !c.isBroken()) {
            return c;
        }
        if (metadata.controller().port() <= 0) {
            refreshMetadata();
        }
        c = new Connection(metadata.controller().host(), metadata.controller().port(), requestTimeoutMs);
        controllerConnection = c;
        return c;
    }

    /** Opens a separate controller connection (e.g. for a heartbeat thread that must not share one). */
    public Connection newControllerConnection() throws IOException {
        if (metadata.controller().port() <= 0) {
            refreshMetadata();
        }
        return new Connection(metadata.controller().host(), metadata.controller().port(), requestTimeoutMs);
    }

    public void invalidate(int brokerId) {
        Connection c = connections.remove(brokerId);
        if (c != null) {
            c.close();
        }
        LOG.debug("invalidated connection to broker %d", brokerId);
    }

    @Override
    public void close() {
        connections.values().forEach(Connection::close);
        connections.clear();
        if (controllerConnection != null) {
            controllerConnection.close();
        }
    }
}
