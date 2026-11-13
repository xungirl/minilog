package dev.minilog.controller;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.protocol.ClusterMetadata.BrokerInfo;
import dev.minilog.protocol.ClusterMetadata.PartitionMetadata;
import dev.minilog.protocol.ClusterMetadata.TopicMetadata;
import dev.minilog.protocol.Messages.AlterIsrRequest;
import dev.minilog.protocol.Messages.AlterIsrResponse;
import dev.minilog.protocol.Messages.BrokerHeartbeatRequest;
import dev.minilog.protocol.Messages.BrokerHeartbeatResponse;
import dev.minilog.protocol.Messages.CreateTopicRequest;
import dev.minilog.protocol.Messages.CreateTopicResponse;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The single, static cluster controller: the source of truth for partition leadership and ISR.
 *
 * <p>Responsibilities (what ZooKeeper/KRaft plus the Kafka controller do, radically simplified):
 * <ol>
 *   <li><b>Liveness:</b> brokers heartbeat every ~100 ms; a broker silent for
 *       {@code brokerSessionTimeoutMs} is declared dead.</li>
 *   <li><b>Leader election:</b> when a leader dies, pick the first live replica that is in the ISR,
 *       bump the leader epoch. Never elect an out-of-sync replica (no "unclean" election): if no
 *       ISR member is alive the partition stays offline until one returns.</li>
 *   <li><b>ISR changes:</b> leaders ask to shrink/expand the ISR (AlterIsr); the controller applies
 *       the change only if the request is based on the current leader epoch and ISR version.</li>
 *   <li><b>Metadata distribution:</b> every change bumps a version number; brokers receive the new
 *       snapshot in their next heartbeat response (pull model).</li>
 * </ol>
 *
 * <p>Limitation (documented in README): the controller itself is a single point of failure. Its
 * topic/partition state is persisted to disk so it can be restarted, but while it is down no
 * failover can happen. Kafka removes this SPOF with a Raft quorum (KRaft).
 */
public final class Controller {
    private static final Logger LOG = Logger.get("controller");

    /** Live broker registration. */
    private record Registration(int id, long incarnationId, String host, int port, long lastHeartbeatMs) {
    }

    /** Mutable per-partition state, guarded by {@link #lock}. */
    private static final class PartitionState {
        final List<Integer> replicas;
        int leader;
        int leaderEpoch;
        int isrVersion;
        List<Integer> isr;

        PartitionState(List<Integer> replicas, int leader, int leaderEpoch, int isrVersion, List<Integer> isr) {
            this.replicas = List.copyOf(replicas);
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.isrVersion = isrVersion;
            this.isr = List.copyOf(isr);
        }
    }

    private final ControllerConfig config;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<Integer, Registration> liveBrokers = new TreeMap<>();
    private final Map<String, List<PartitionState>> topics = new TreeMap<>();
    private final Path stateFile;
    private long metadataVersion;
    private volatile ClusterMetadata snapshot = ClusterMetadata.EMPTY;
    private volatile int advertisedPort;
    private final long startedAtMs = Time.nowMs();
    /** Brokers named in persisted state that have not heartbeated since this controller started. */
    private final Set<Integer> awaitingFirstHeartbeat = new java.util.HashSet<>();

    public Controller(ControllerConfig config) throws IOException {
        this.config = config;
        Files.createDirectories(config.dataDir());
        this.stateFile = config.dataDir().resolve("controller-state.bin");
        loadState();
    }

    void setAdvertisedPort(int port) {
        lock.lock();
        try {
            this.advertisedPort = port;
            publish();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ broker liveness

    public BrokerHeartbeatResponse handleBrokerHeartbeat(BrokerHeartbeatRequest req) {
        lock.lock();
        try {
            long now = Time.nowMs();
            Registration prev = liveBrokers.get(req.brokerId());
            boolean changed = false;
            if (prev == null || prev.incarnationId() != req.incarnationId()
                    || !prev.host().equals(req.host()) || prev.port() != req.port()) {
                if (prev != null && prev.incarnationId() != req.incarnationId()) {
                    // Restarted before its session expired: treat as a failure first, so the
                    // restarted broker re-earns its ISR membership by catching up.
                    LOG.info("Broker %d restarted (new incarnation)", req.brokerId());
                    onBrokerFailure(req.brokerId());
                }
                LOG.info("Broker %d registered at %s:%d", req.brokerId(), req.host(), req.port());
                changed = true;
            }
            liveBrokers.put(req.brokerId(), new Registration(req.brokerId(), req.incarnationId(), req.host(),
                    req.port(), now));
            if (changed) {
                electLeadersForOfflinePartitions();
                bumpVersion();
            }
            ClusterMetadata md = snapshot;
            return new BrokerHeartbeatResponse(Errors.NONE, md.version() != req.knownMetadataVersion() ? md : null);
        } finally {
            lock.unlock();
        }
    }

    /** Called periodically: declare brokers dead whose heartbeats stopped. */
    public void checkBrokerSessions() {
        lock.lock();
        try {
            long now = Time.nowMs();
            List<Integer> expired = new ArrayList<>();
            for (Registration r : liveBrokers.values()) {
                if (now - r.lastHeartbeatMs() > config.brokerSessionTimeoutMs()) {
                    expired.add(r.id());
                }
            }
            // After a controller restart, brokers that never re-register are treated as failed.
            if (now - startedAtMs > config.brokerSessionTimeoutMs() && !awaitingFirstHeartbeat.isEmpty()) {
                for (int id : awaitingFirstHeartbeat) {
                    if (!liveBrokers.containsKey(id)) {
                        LOG.info("Broker %d did not re-register after controller restart", id);
                        onBrokerFailure(id);
                        expired.add(-1);
                    }
                }
                awaitingFirstHeartbeat.clear();
            }
            for (int id : expired) {
                if (id < 0) {
                    continue;
                }
                LOG.info("Broker %d session expired; failing over its partitions", id);
                liveBrokers.remove(id);
                onBrokerFailure(id);
            }
            if (!expired.isEmpty()) {
                bumpVersion();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Moves leadership away from a failed broker and removes it from every ISR. Caller holds the lock. */
    private void onBrokerFailure(int brokerId) {
        for (Map.Entry<String, List<PartitionState>> t : topics.entrySet()) {
            for (int p = 0; p < t.getValue().size(); p++) {
                PartitionState ps = t.getValue().get(p);
                if (!ps.replicas.contains(brokerId)) {
                    continue;
                }
                if (ps.leader == brokerId) {
                    OptionalInt newLeader = ps.isr.stream()
                            .mapToInt(Integer::intValue)
                            .filter(id -> id != brokerId && liveBrokers.containsKey(id))
                            .findFirst();
                    ps.leaderEpoch++;
                    if (newLeader.isPresent()) {
                        ps.leader = newLeader.getAsInt();
                        ps.isr = without(ps.isr, brokerId);
                        ps.isrVersion++;
                        LOG.info("%s-%d: leader %d -> %d (epoch %d), isr=%s", t.getKey(), p, brokerId, ps.leader,
                                ps.leaderEpoch, ps.isr);
                    } else {
                        // Keep the ISR as-is: the last in-sync replica is the only safe future leader.
                        ps.leader = -1;
                        LOG.warn("%s-%d is OFFLINE: no live in-sync replica (isr=%s)", t.getKey(), p, ps.isr);
                    }
                } else if (ps.isr.contains(brokerId) && ps.isr.size() > 1) {
                    ps.isr = without(ps.isr, brokerId);
                    ps.isrVersion++;
                }
            }
        }
    }

    /** A broker came back: partitions that were offline and have it in their ISR get it as leader. */
    private void electLeadersForOfflinePartitions() {
        for (Map.Entry<String, List<PartitionState>> t : topics.entrySet()) {
            for (int p = 0; p < t.getValue().size(); p++) {
                PartitionState ps = t.getValue().get(p);
                if (ps.leader != -1) {
                    continue;
                }
                for (int candidate : ps.isr) {
                    if (liveBrokers.containsKey(candidate)) {
                        ps.leader = candidate;
                        ps.leaderEpoch++;
                        LOG.info("%s-%d back online with leader %d (epoch %d)", t.getKey(), p, candidate, ps.leaderEpoch);
                        break;
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ topics & ISR

    public CreateTopicResponse createTopic(CreateTopicRequest req) {
        lock.lock();
        try {
            if (req.topic() == null || req.topic().isBlank() || !req.topic().matches("[A-Za-z0-9._-]+")) {
                return new CreateTopicResponse(Errors.INVALID_REQUEST, "invalid topic name");
            }
            if (topics.containsKey(req.topic())) {
                return new CreateTopicResponse(Errors.TOPIC_ALREADY_EXISTS, req.topic());
            }
            List<Integer> brokers = new ArrayList<>(liveBrokers.keySet());
            if (req.partitions() <= 0 || req.replicationFactor() <= 0 || req.replicationFactor() > brokers.size()) {
                return new CreateTopicResponse(Errors.INVALID_REPLICATION_FACTOR,
                        "replication factor " + req.replicationFactor() + " with " + brokers.size() + " live brokers");
            }
            // Round-robin placement: partition p's replicas start at broker (p mod n), so leaders
            // (the first replica) are spread evenly across brokers.
            List<PartitionState> partitions = new ArrayList<>();
            for (int p = 0; p < req.partitions(); p++) {
                List<Integer> replicas = new ArrayList<>();
                for (int r = 0; r < req.replicationFactor(); r++) {
                    replicas.add(brokers.get((p + r) % brokers.size()));
                }
                partitions.add(new PartitionState(replicas, replicas.getFirst(), 0, 0, replicas));
            }
            topics.put(req.topic(), partitions);
            bumpVersion();
            LOG.info("Created topic %s with %d partitions, RF=%d", req.topic(), req.partitions(), req.replicationFactor());
            return new CreateTopicResponse(Errors.NONE, "created");
        } finally {
            lock.unlock();
        }
    }

    public AlterIsrResponse alterIsr(AlterIsrRequest req) {
        lock.lock();
        try {
            List<PartitionState> parts = topics.get(req.topic());
            if (parts == null || req.partition() < 0 || req.partition() >= parts.size()) {
                return new AlterIsrResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, List.of());
            }
            PartitionState ps = parts.get(req.partition());
            if (ps.leader != req.brokerId() || ps.leaderEpoch != req.leaderEpoch()) {
                return new AlterIsrResponse(Errors.FENCED_LEADER_EPOCH, ps.leaderEpoch, ps.isrVersion, ps.isr);
            }
            if (ps.isrVersion != req.isrVersion()) {
                return new AlterIsrResponse(Errors.STALE_ISR_VERSION, ps.leaderEpoch, ps.isrVersion, ps.isr);
            }
            Set<Integer> requested = new LinkedHashSet<>(req.newIsr());
            boolean valid = requested.contains(ps.leader)
                    && ps.replicas.containsAll(requested)
                    && requested.stream().allMatch(liveBrokers::containsKey);
            if (!valid) {
                return new AlterIsrResponse(Errors.INVALID_REQUEST, ps.leaderEpoch, ps.isrVersion, ps.isr);
            }
            LOG.info("%s-%d: ISR %s -> %s", req.topic(), req.partition(), ps.isr, requested);
            ps.isr = List.copyOf(requested);
            ps.isrVersion++;
            bumpVersion();
            return new AlterIsrResponse(Errors.NONE, ps.leaderEpoch, ps.isrVersion, ps.isr);
        } finally {
            lock.unlock();
        }
    }

    public ClusterMetadata metadata() {
        return snapshot;
    }

    /** Number of partitions of a topic, or 0 if unknown (used by the group coordinator). */
    public int partitionCount(String topic) {
        return snapshot.topic(topic).map(t -> t.partitions().size()).orElse(0);
    }

    // ------------------------------------------------------------------ versioning & persistence

    private void bumpVersion() {
        metadataVersion++;
        publish();
        persistState();
    }

    private void publish() {
        List<BrokerInfo> brokers = liveBrokers.values().stream()
                .map(r -> new BrokerInfo(r.id(), r.host(), r.port())).toList();
        List<TopicMetadata> topicList = new ArrayList<>();
        for (Map.Entry<String, List<PartitionState>> t : topics.entrySet()) {
            List<PartitionMetadata> parts = new ArrayList<>();
            for (int p = 0; p < t.getValue().size(); p++) {
                PartitionState ps = t.getValue().get(p);
                parts.add(new PartitionMetadata(p, ps.leader, ps.leaderEpoch, ps.isrVersion, ps.replicas, ps.isr));
            }
            topicList.add(new TopicMetadata(t.getKey(), parts));
        }
        snapshot = new ClusterMetadata(metadataVersion, new ClusterMetadata.Endpoint(config.host(), advertisedPort),
                brokers, topicList);
    }

    /** Persists topic/partition state with the same binary encoding used on the wire. */
    private void persistState() {
        try {
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp)))) {
                snapshot.write(out);
            }
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadState() throws IOException {
        if (!Files.exists(stateFile)) {
            publish();
            return;
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(stateFile)))) {
            ClusterMetadata md = ClusterMetadata.read(in);
            metadataVersion = md.version();
            for (TopicMetadata t : md.topics()) {
                List<PartitionState> parts = new ArrayList<>();
                for (PartitionMetadata p : t.partitions()) {
                    awaitingFirstHeartbeat.addAll(p.replicas());
                    parts.add(new PartitionState(p.replicas(), p.leader(), p.leaderEpoch(), p.isrVersion(), p.isr()));
                }
                topics.put(t.name(), parts);
            }
            LOG.info("Loaded controller state: %d topics, version %d", topics.size(), metadataVersion);
        }
        publish();
    }

    private static List<Integer> without(List<Integer> list, int id) {
        return list.stream().filter(x -> x != id).toList();
    }
}
