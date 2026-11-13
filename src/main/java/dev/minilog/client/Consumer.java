package dev.minilog.client;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Messages;
import dev.minilog.protocol.Messages.FetchRequest;
import dev.minilog.protocol.Messages.FetchResponse;
import dev.minilog.protocol.Messages.PartitionOffset;
import dev.minilog.storage.Record;
import dev.minilog.storage.RecordCodec;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Pull-based consumer with optional consumer-group membership.
 *
 * <p>Typical loop:
 * <pre>{@code
 * consumer.subscribe(List.of("orders"));
 * while (running) {
 *     for (ConsumerRecord r : consumer.poll(Duration.ofMillis(500))) { process(r); }
 * }
 * }</pre>
 *
 * <p><b>Delivery semantics: at-least-once.</b> poll() commits the positions of records returned
 * by <em>previous</em> polls (the application has processed them by the time it polls again).
 * If the process crashes after processing but before the next commit, those records are
 * delivered again to whoever takes over the partition.
 *
 * <p>Not thread-safe (like Kafka's consumer): use it from one thread. A background virtual
 * thread only sends heartbeats and raises a "rebalance needed" flag.
 */
public final class Consumer implements Closeable {
    private static final Logger LOG = Logger.get("consumer");

    private final ConsumerConfig config;
    private final ClusterClient cluster;
    private final ExecutorService ioPool = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<TopicPartition, Long> positions = new LinkedHashMap<>();
    private List<String> subscription = List.of();
    private volatile String memberId = "";
    private volatile int generation = -1;
    private volatile boolean needsRejoin;
    private volatile boolean closed;
    private Thread heartbeatThread;
    private long lastCommitMs;
    private Map<TopicPartition, Long> lastCommitted = new HashMap<>();

    public Consumer(ConsumerConfig config) {
        this.config = config;
        this.cluster = new ClusterClient(config.bootstrap(), config.requestTimeoutMs() + config.fetchMaxWaitMs());
    }

    // ------------------------------------------------------------------ assignment

    /** Joins the consumer group and subscribes to topics; partitions are assigned by the coordinator. */
    public void subscribe(List<String> topics) {
        if (config.groupId() == null) {
            throw new IllegalStateException("subscribe() requires a groupId; use assign() otherwise");
        }
        this.subscription = List.copyOf(topics);
        this.needsRejoin = true;
    }

    /** Manual assignment without a group. Positions start per autoOffsetReset unless {@link #seek} is used. */
    public void assign(List<TopicPartition> partitions) throws IOException {
        invalidatePendingFetches();
        positions.clear();
        cluster.refreshMetadata();
        for (TopicPartition tp : partitions) {
            positions.put(tp, resetOffset(tp));
        }
    }

    public void seek(TopicPartition tp, long offset) {
        invalidatePendingFetches();
        positions.put(tp, offset);
    }

    public List<TopicPartition> assignment() {
        return List.copyOf(positions.keySet());
    }

    public int generation() {
        return generation;
    }

    public String memberId() {
        return memberId;
    }

    // ------------------------------------------------------------------ poll

    public List<ConsumerRecord> poll(Duration timeout) throws IOException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        do {
            if (!subscription.isEmpty() && needsRejoin) {
                rejoinGroup();
            }
            maybeAutoCommit();
            List<ConsumerRecord> records = fetchOnce();
            if (!records.isEmpty()) {
                return records;
            }
        } while (System.currentTimeMillis() < deadline && !closed);
        return List.of();
    }

    /** A fetch that is still in flight, tagged with the assignment epoch it was built for. */
    private record PendingFetch(int leader, long assignmentEpoch, Future<FetchResponse> future) {
    }

    private final Map<Integer, PendingFetch> pending = new HashMap<>();
    private long assignmentEpoch;
    private final java.util.concurrent.LinkedBlockingQueue<Integer> completions = new java.util.concurrent.LinkedBlockingQueue<>();

    /**
     * Keeps one long-poll fetch in flight per leader broker and returns as soon as <em>any</em> of
     * them completes. Waiting for all of them would let an idle partition's long poll
     * (fetch.max.wait.ms) delay records that are already available from another broker.
     */
    private List<ConsumerRecord> fetchOnce() throws IOException {
        if (positions.isEmpty()) {
            Time.sleepQuietly(Math.min(50, config.fetchMaxWaitMs()));
            return List.of();
        }
        if (cluster.metadata().version() < 0) {
            cluster.refreshMetadata();
        }
        Map<Integer, List<FetchRequest.FetchPartition>> byLeader = new HashMap<>();
        boolean missingLeader = false;
        for (Map.Entry<TopicPartition, Long> e : positions.entrySet()) {
            int leader = cluster.leaderFor(e.getKey());
            if (leader < 0) {
                missingLeader = true;
                continue;
            }
            byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(new FetchRequest.FetchPartition(
                    e.getKey().topic(), e.getKey().partition(), e.getValue(), config.maxPartitionFetchBytes(), -1));
        }
        if (missingLeader) {
            refreshQuietly();
        }
        for (Map.Entry<Integer, List<FetchRequest.FetchPartition>> e : byLeader.entrySet()) {
            int leader = e.getKey();
            if (!pending.containsKey(leader)) {
                FetchRequest req = new FetchRequest(-1, config.fetchMaxWaitMs(), e.getValue());
                pending.put(leader, new PendingFetch(leader, assignmentEpoch, ioPool.submit(() -> {
                    try {
                        return cluster.connectionTo(leader).<FetchResponse>call(ApiKey.FETCH, req);
                    } finally {
                        completions.offer(leader); // wakes the poll thread
                    }
                })));
            }
        }
        // Wait until at least one in-flight fetch completes (bounded by the long-poll time).
        if (pending.values().stream().noneMatch(p -> p.future().isDone())) {
            try {
                completions.poll(config.fetchMaxWaitMs() + 1000L, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        completions.clear();
        List<ConsumerRecord> out = new ArrayList<>();
        boolean refresh = false;
        for (PendingFetch pf : List.copyOf(pending.values())) {
            if (!pf.future().isDone()) {
                continue;
            }
            pending.remove(pf.leader());
            FetchResponse resp;
            try {
                resp = pf.future().get();
            } catch (Exception ex) {
                cluster.invalidate(pf.leader());
                refresh = true;
                continue;
            }
            if (pf.assignmentEpoch() != assignmentEpoch) {
                continue; // built for an old assignment/position: discard
            }
            for (FetchResponse.PartitionData pd : resp.partitions()) {
                TopicPartition tp = pd.tp();
                Long position = positions.get(tp);
                if (position == null) {
                    continue;
                }
                switch (pd.error()) {
                    case NONE -> {
                        for (Record r : RecordCodec.decodeAll(pd.records())) { // CRC checked here
                            if (r.offset() >= position) {
                                out.add(new ConsumerRecord(tp, r.offset(), r.timestamp(), r.key(), r.value()));
                                position = r.offset() + 1;
                            }
                        }
                        positions.put(tp, position);
                    }
                    case OFFSET_OUT_OF_RANGE -> {
                        long reset = resetOffset(tp);
                        LOG.warn("%s: offset %d out of range, resetting to %d", tp, position, reset);
                        positions.put(tp, reset);
                    }
                    default -> refresh = true; // NOT_LEADER etc.: leadership moved
                }
            }
        }
        if (refresh) {
            refreshQuietly();
            if (out.isEmpty()) {
                Time.sleepQuietly(20);
            }
        }
        return out;
    }

    /** Positions changed outside the fetch path: results of in-flight fetches must be ignored. */
    private void invalidatePendingFetches() {
        assignmentEpoch++;
    }

    private void refreshQuietly() {
        try {
            cluster.refreshMetadata();
        } catch (IOException e) {
            LOG.debug("metadata refresh failed: %s", e);
        }
    }

    /** Earliest or latest offset of a partition, per autoOffsetReset. Retries across leader changes. */
    private long resetOffset(TopicPartition tp) throws IOException {
        long ts = "latest".equals(config.autoOffsetReset()) ? Messages.LATEST : Messages.EARLIEST;
        for (int attempt = 0; attempt < 200; attempt++) {
            int leader = cluster.leaderFor(tp);
            if (leader >= 0) {
                try {
                    Messages.ListOffsetsResponse r = cluster.connectionTo(leader).call(ApiKey.LIST_OFFSETS,
                            new Messages.ListOffsetsRequest(tp.topic(), tp.partition(), ts));
                    if (r.error() == Errors.NONE) {
                        return r.offset();
                    }
                } catch (IOException e) {
                    cluster.invalidate(leader);
                }
            }
            Time.sleepQuietly(50);
            refreshQuietly();
        }
        throw new IOException("Could not find offset for " + tp);
    }

    // ------------------------------------------------------------------ group membership

    private void rejoinGroup() throws IOException {
        if (generation >= 0) {
            commitSync(); // hand over cleanly: the next owner starts exactly where we stopped
        }
        while (!closed) {
            Messages.JoinGroupResponse resp;
            try {
                resp = cluster.controller().call(ApiKey.JOIN_GROUP, new Messages.JoinGroupRequest(config.groupId(),
                        memberId, config.sessionTimeoutMs(), config.rebalanceTimeoutMs(), subscription));
            } catch (IOException e) {
                LOG.warn("join failed, retrying: %s", e);
                Time.sleepQuietly(200);
                continue;
            }
            if (resp.error() == Errors.UNKNOWN_MEMBER_ID) {
                memberId = "";
                continue;
            }
            if (resp.error() != Errors.NONE) {
                throw new IOException("JoinGroup failed: " + resp.error());
            }
            memberId = resp.memberId();
            generation = resp.generation();
            needsRejoin = false;
            onAssignment(resp.assignment());
            startHeartbeatThread();
            return;
        }
    }

    private void onAssignment(List<TopicPartition> assignment) throws IOException {
        invalidatePendingFetches();
        positions.clear();
        lastCommitted = new HashMap<>();
        if (assignment.isEmpty()) {
            LOG.info("%s generation %d: no partitions assigned", memberId, generation);
            return;
        }
        cluster.refreshMetadata();
        Messages.OffsetFetchResponse committed = cluster.controller().call(ApiKey.OFFSET_FETCH,
                new Messages.OffsetFetchRequest(config.groupId(), assignment));
        for (PartitionOffset po : committed.offsets()) {
            long pos = po.offset() >= 0 ? po.offset() : resetOffset(po.tp());
            positions.put(po.tp(), pos);
            lastCommitted.put(po.tp(), po.offset());
        }
        LOG.info("%s generation %d: assigned %s", memberId, generation, positions);
    }

    private void startHeartbeatThread() {
        if (heartbeatThread != null) {
            return;
        }
        heartbeatThread = Thread.ofVirtual().name("consumer-heartbeat").start(() -> {
            Connection conn = null;
            while (!closed) {
                try {
                    if (conn == null || conn.isBroken()) {
                        conn = cluster.newControllerConnection();
                    }
                    if (!needsRejoin) {
                        Messages.ErrorResponse r = conn.call(ApiKey.HEARTBEAT,
                                new Messages.HeartbeatRequest(config.groupId(), memberId, generation));
                        if (r.error() != Errors.NONE) {
                            needsRejoin = true;
                        }
                    }
                } catch (IOException e) {
                    conn = null;
                }
                Time.sleepQuietly(config.heartbeatIntervalMs());
            }
            if (conn != null) {
                conn.close();
            }
        });
    }

    // ------------------------------------------------------------------ commits

    private void maybeAutoCommit() throws IOException {
        if (generation >= 0 && System.currentTimeMillis() - lastCommitMs >= config.autoCommitIntervalMs()) {
            commitSync();
        }
    }

    /** Commits the current position (next offset to read) of every assigned partition. */
    public void commitSync() throws IOException {
        if (config.groupId() == null) {
            return;
        }
        List<PartitionOffset> offsets = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> e : positions.entrySet()) {
            if (!e.getValue().equals(lastCommitted.get(e.getKey()))) {
                offsets.add(new PartitionOffset(e.getKey().topic(), e.getKey().partition(), e.getValue()));
            }
        }
        lastCommitMs = System.currentTimeMillis();
        if (offsets.isEmpty()) {
            return;
        }
        Messages.OffsetCommitResponse r = cluster.controller().call(ApiKey.OFFSET_COMMIT,
                new Messages.OffsetCommitRequest(config.groupId(), memberId, generation, offsets));
        if (r.error() == Errors.NONE) {
            offsets.forEach(po -> lastCommitted.put(po.tp(), po.offset()));
        } else {
            // ILLEGAL_GENERATION etc.: we were fenced out by a rebalance. Rejoin; the records since
            // the last successful commit may be redelivered (at-least-once).
            LOG.warn("commit rejected: %s", r.error());
            needsRejoin = true;
        }
    }

    public Map<TopicPartition, Long> positions() {
        return Map.copyOf(positions);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            if (generation >= 0) {
                commitSync();
                cluster.controller().call(ApiKey.LEAVE_GROUP, new Messages.LeaveGroupRequest(config.groupId(), memberId));
            }
        } catch (IOException e) {
            LOG.warn("close: %s", e);
        }
        closed = true;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        ioPool.shutdownNow();
        cluster.close();
    }
}
