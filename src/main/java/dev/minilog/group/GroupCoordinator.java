package dev.minilog.group;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.Messages.ErrorResponse;
import dev.minilog.protocol.Messages.HeartbeatRequest;
import dev.minilog.protocol.Messages.JoinGroupRequest;
import dev.minilog.protocol.Messages.JoinGroupResponse;
import dev.minilog.protocol.Messages.LeaveGroupRequest;
import dev.minilog.protocol.Messages.OffsetCommitRequest;
import dev.minilog.protocol.Messages.OffsetCommitResponse;
import dev.minilog.protocol.Messages.OffsetFetchRequest;
import dev.minilog.protocol.Messages.OffsetFetchResponse;
import dev.minilog.protocol.Messages.PartitionOffset;
import dev.minilog.storage.Log;
import dev.minilog.storage.LogConfig;
import dev.minilog.storage.Record;
import dev.minilog.storage.RecordCodec;
import dev.minilog.storage.SimpleRecord;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.ToIntFunction;

/**
 * Consumer-group membership, partition assignment and committed offsets.
 *
 * <p><b>Rebalance protocol</b> (an "eager" protocol like Kafka's classic one, with the
 * assignment computed on the coordinator instead of by a group leader):
 * <pre>
 *  STABLE --(member joins / leaves / times out)--> PREPARING_REBALANCE
 *  PREPARING_REBALANCE: every member's heartbeat returns REBALANCE_IN_PROGRESS; each member
 *     commits its offsets and re-sends JoinGroup, which blocks until all members have rejoined
 *     (or the rebalance timeout evicts stragglers)
 *  all joined --> generation++, range-assign partitions, answer every JoinGroup --> STABLE
 * </pre>
 * Because every member commits <em>before</em> rejoining and a new owner reads the committed
 * offset <em>after</em> the barrier, a clean rebalance neither loses nor duplicates records.
 * Commits carrying an old generation are rejected (fencing zombies).
 *
 * <p><b>Offset storage:</b> commits are appended to an internal minilog {@link Log}
 * (like Kafka's {@code __consumer_offsets}) and replayed at startup. Unlike Kafka the log is not
 * compacted and not replicated - it lives on the controller node.
 */
public final class GroupCoordinator implements Closeable {
    private static final Logger LOG = Logger.get("group-coordinator");

    private enum State { EMPTY, PREPARING_REBALANCE, STABLE }

    private static final class Member {
        final String id;
        Set<String> topics;
        int sessionTimeoutMs;
        long lastHeartbeatMs;
        boolean joinedThisRound;

        Member(String id) {
            this.id = id;
        }
    }

    private static final class Group {
        final String id;
        State state = State.EMPTY;
        int generation;
        long completedRounds;
        long rebalanceDeadlineMs;
        final Map<String, Member> members = new LinkedHashMap<>();
        Map<String, List<TopicPartition>> assignment = Map.of();

        Group(String id) {
            this.id = id;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition groupChanged = lock.newCondition();
    private final Map<String, Group> groups = new HashMap<>();
    private final Map<String, Map<TopicPartition, Long>> offsets = new ConcurrentHashMap<>();
    private final ToIntFunction<String> partitionCount;
    private final Log offsetsLog;

    public GroupCoordinator(Path offsetsDir, ToIntFunction<String> partitionCount) throws IOException {
        this.partitionCount = partitionCount;
        this.offsetsLog = Log.open(offsetsDir, LogConfig.defaults(), false);
        replayOffsets();
    }

    // ------------------------------------------------------------------ membership

    public JoinGroupResponse join(JoinGroupRequest req) throws InterruptedException {
        lock.lock();
        try {
            Group g = groups.computeIfAbsent(req.group(), Group::new);
            String memberId = req.memberId() == null || req.memberId().isEmpty()
                    ? "member-" + UUID.randomUUID().toString().substring(0, 8) : req.memberId();
            Member m = g.members.computeIfAbsent(memberId, Member::new);
            m.topics = Set.copyOf(req.topics());
            m.sessionTimeoutMs = req.sessionTimeoutMs();
            m.lastHeartbeatMs = Time.nowMs();
            if (g.state != State.PREPARING_REBALANCE) {
                startRebalance(g, req.rebalanceTimeoutMs(), "member " + memberId + " joined");
            }
            m.joinedThisRound = true;
            long round = g.completedRounds;
            maybeCompleteRebalance(g);
            while (g.completedRounds == round) {
                long remaining = g.rebalanceDeadlineMs - Time.nowMs();
                if (remaining <= 0) {
                    evictStragglersAndComplete(g);
                    break;
                }
                groupChanged.await(remaining, TimeUnit.MILLISECONDS);
            }
            if (!g.members.containsKey(memberId)) {
                return new JoinGroupResponse(Errors.UNKNOWN_MEMBER_ID, -1, memberId, List.of());
            }
            return new JoinGroupResponse(Errors.NONE, g.generation, memberId,
                    g.assignment.getOrDefault(memberId, List.of()));
        } finally {
            lock.unlock();
        }
    }

    public ErrorResponse heartbeat(HeartbeatRequest req) {
        lock.lock();
        try {
            Group g = groups.get(req.group());
            Member m = g == null ? null : g.members.get(req.memberId());
            if (m == null) {
                return new ErrorResponse(Errors.UNKNOWN_MEMBER_ID);
            }
            m.lastHeartbeatMs = Time.nowMs();
            if (g.state == State.PREPARING_REBALANCE) {
                return new ErrorResponse(Errors.REBALANCE_IN_PROGRESS);
            }
            if (req.generation() != g.generation) {
                return new ErrorResponse(Errors.ILLEGAL_GENERATION);
            }
            return new ErrorResponse(Errors.NONE);
        } finally {
            lock.unlock();
        }
    }

    public ErrorResponse leave(LeaveGroupRequest req) {
        lock.lock();
        try {
            Group g = groups.get(req.group());
            if (g == null || g.members.remove(req.memberId()) == null) {
                return new ErrorResponse(Errors.UNKNOWN_MEMBER_ID);
            }
            onMembershipShrunk(g, "member " + req.memberId() + " left");
            return new ErrorResponse(Errors.NONE);
        } finally {
            lock.unlock();
        }
    }

    /** Periodic task: expire members whose session timed out, finish overdue rebalances. */
    public void checkSessions() {
        lock.lock();
        try {
            long now = Time.nowMs();
            for (Group g : groups.values()) {
                if (g.state == State.PREPARING_REBALANCE && now >= g.rebalanceDeadlineMs) {
                    evictStragglersAndComplete(g);
                    continue;
                }
                List<String> expired = new ArrayList<>();
                for (Member m : g.members.values()) {
                    if (now - m.lastHeartbeatMs > m.sessionTimeoutMs) {
                        expired.add(m.id);
                    }
                }
                if (!expired.isEmpty()) {
                    expired.forEach(g.members::remove);
                    onMembershipShrunk(g, "session timeout of " + expired);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void onMembershipShrunk(Group g, String reason) {
        if (g.members.isEmpty()) {
            g.state = State.EMPTY;
            g.generation++;
            g.assignment = Map.of();
            g.completedRounds++;
            groupChanged.signalAll();
            LOG.info("Group %s is now empty (%s)", g.id, reason);
        } else if (g.state == State.PREPARING_REBALANCE) {
            maybeCompleteRebalance(g);
        } else {
            startRebalance(g, defaultRebalanceTimeout(g), reason);
        }
    }

    private int defaultRebalanceTimeout(Group g) {
        return g.members.values().stream().mapToInt(m -> m.sessionTimeoutMs).max().orElse(10_000);
    }

    private void startRebalance(Group g, int rebalanceTimeoutMs, String reason) {
        g.state = State.PREPARING_REBALANCE;
        g.rebalanceDeadlineMs = Time.nowMs() + rebalanceTimeoutMs;
        g.members.values().forEach(m -> m.joinedThisRound = false);
        LOG.info("Group %s: preparing rebalance (%s)", g.id, reason);
    }

    private void maybeCompleteRebalance(Group g) {
        if (g.state == State.PREPARING_REBALANCE && g.members.values().stream().allMatch(m -> m.joinedThisRound)) {
            completeRebalance(g);
        }
    }

    private void evictStragglersAndComplete(Group g) {
        g.members.values().removeIf(m -> !m.joinedThisRound);
        if (g.members.isEmpty()) {
            onMembershipShrunk(g, "all members timed out during rebalance");
        } else {
            completeRebalance(g);
        }
    }

    private void completeRebalance(Group g) {
        Map<String, Set<String>> subscriptions = new LinkedHashMap<>();
        g.members.values().forEach(m -> subscriptions.put(m.id, m.topics));
        g.assignment = RangeAssignor.assign(subscriptions, partitionCount);
        g.generation++;
        g.state = State.STABLE;
        g.completedRounds++;
        long now = Time.nowMs();
        g.members.values().forEach(m -> m.lastHeartbeatMs = now);
        LOG.info("Group %s: generation %d assignment %s", g.id, g.generation, g.assignment);
        groupChanged.signalAll();
    }

    // ------------------------------------------------------------------ offsets

    public OffsetCommitResponse commit(OffsetCommitRequest req) {
        lock.lock();
        try {
            if (req.generation() >= 0) {
                Group g = groups.get(req.group());
                if (g == null || !g.members.containsKey(req.memberId())) {
                    return new OffsetCommitResponse(Errors.UNKNOWN_MEMBER_ID);
                }
                // Allowed during PREPARING_REBALANCE: members commit before rejoining.
                if (req.generation() != g.generation) {
                    return new OffsetCommitResponse(Errors.ILLEGAL_GENERATION);
                }
            }
            List<SimpleRecord> records = new ArrayList<>();
            long now = Time.nowMs();
            for (PartitionOffset po : req.offsets()) {
                byte[] key = (req.group() + "\u0000" + po.topic() + "\u0000" + po.partition()).getBytes(StandardCharsets.UTF_8);
                byte[] value = ByteBuffer.allocate(8).putLong(po.offset()).array();
                records.add(new SimpleRecord(now, key, value));
            }
            if (!records.isEmpty()) {
                offsetsLog.appendAsLeader(-1, -1, records, 0);
            }
            Map<TopicPartition, Long> groupOffsets = offsets.computeIfAbsent(req.group(), k -> new ConcurrentHashMap<>());
            req.offsets().forEach(po -> groupOffsets.put(po.tp(), po.offset()));
            return new OffsetCommitResponse(Errors.NONE);
        } finally {
            lock.unlock();
        }
    }

    public OffsetFetchResponse fetchOffsets(OffsetFetchRequest req) {
        Map<TopicPartition, Long> groupOffsets = offsets.getOrDefault(req.group(), Map.of());
        List<PartitionOffset> out = new ArrayList<>();
        for (TopicPartition tp : req.partitions()) {
            out.add(new PartitionOffset(tp.topic(), tp.partition(), groupOffsets.getOrDefault(tp, -1L)));
        }
        return new OffsetFetchResponse(out);
    }

    private void replayOffsets() {
        long offset = offsetsLog.logStartOffset();
        int count = 0;
        while (offset < offsetsLog.logEndOffset()) {
            List<Record> records = RecordCodec.decodeAll(offsetsLog.read(offset, 1 << 20, Long.MAX_VALUE));
            for (Record r : records) {
                String[] key = new String(r.key(), StandardCharsets.UTF_8).split("\u0000");
                long committed = ByteBuffer.wrap(r.value()).getLong();
                offsets.computeIfAbsent(key[0], k -> new ConcurrentHashMap<>())
                        .put(new TopicPartition(key[1], Integer.parseInt(key[2])), committed);
                count++;
            }
            offset = records.getLast().offset() + 1;
        }
        LOG.info("Replayed %d offset commits for %d groups", count, offsets.size());
    }

    @Override
    public void close() throws IOException {
        offsetsLog.close();
    }
}
