package dev.minilog.broker;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.MiniLogException;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.ClusterMetadata.PartitionMetadata;
import dev.minilog.protocol.Messages.AlterIsrResponse;
import dev.minilog.protocol.Messages.FetchRequest.FetchPartition;
import dev.minilog.protocol.Messages.FetchResponse.PartitionData;
import dev.minilog.storage.Log;
import dev.minilog.storage.OffsetOutOfRangeException;
import dev.minilog.storage.SimpleRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Replication state of one partition on one broker: either the leader or a follower.
 *
 * <h2>Leader bookkeeping</h2>
 * <ul>
 *   <li><b>LEO</b> (log end offset): the next offset to be written, per replica. The leader learns
 *       a follower's LEO from the {@code fetchOffset} of its fetch requests ("I have everything below X").</li>
 *   <li><b>ISR</b> (in-sync replicas): replicas that are caught up. A follower that has not
 *       caught up for {@code replicaLagTimeMs} is removed; one whose LEO reaches the high watermark is re-added.
 *       All ISR changes go through the controller (AlterIsr) so the controller never elects a
 *       replica the leader did not consider in sync.</li>
 *   <li><b>HW</b> (high watermark) = min LEO over the ISR. Everything below the HW exists on every
 *       ISR member, i.e. it is <em>committed</em>: consumers can only read below the HW, and
 *       acks=all producers are answered once the HW passes their records.</li>
 * </ul>
 * While an ISR change is in flight, the HW is computed over (current ISR ∪ proposed ISR), so it
 * never advances past a replica the controller might already consider in sync.
 */
public final class Partition {
    private static final Logger LOG = Logger.get("partition");

    public enum Role { NONE, LEADER, FOLLOWER }

    /** What the leader knows about one follower. */
    private static final class FollowerState {
        long logEndOffset = -1;      // unknown until the follower's first fetch in this epoch
        long lastCaughtUpMs;
        long lastFetchLeaderLeo = -1;
        long lastFetchMs = -1;

        FollowerState(long nowMs) {
            this.lastCaughtUpMs = nowMs; // grace period after a leader change
        }
    }

    /** How the leader talks to the controller about ISR changes. */
    @FunctionalInterface
    public interface IsrChannel {
        AlterIsrResponse alterIsr(TopicPartition tp, int leaderEpoch, int isrVersion, List<Integer> newIsr) throws Exception;
    }

    private final TopicPartition tp;
    private final int localId;
    private final Log log;
    private final BrokerConfig config;
    private final DataNotifier notifier;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hwChanged = lock.newCondition();

    // all fields below are guarded by lock (volatile ones may be read without it)
    private volatile Role role = Role.NONE;
    private volatile int leaderId = -1;
    private volatile int leaderEpoch = -1;
    private List<Integer> replicas = List.of();
    private volatile List<Integer> isr = List.of();
    private int isrVersion = -1;
    private List<Integer> pendingIsr;
    private long leaderEpochStartOffset;
    private final Map<Integer, FollowerState> followers = new HashMap<>();
    private volatile long highWatermark;

    public Partition(TopicPartition tp, int localId, Log log, BrokerConfig config, DataNotifier notifier) {
        this.tp = tp;
        this.localId = localId;
        this.log = log;
        this.config = config;
        this.notifier = notifier;
    }

    // ------------------------------------------------------------------ role changes

    public void makeLeader(PartitionMetadata pm) {
        lock.lock();
        try {
            role = Role.LEADER;
            leaderId = localId;
            leaderEpoch = pm.leaderEpoch();
            replicas = pm.replicas();
            isr = pm.isr();
            isrVersion = pm.isrVersion();
            pendingIsr = null;
            log.assignEpochStart(leaderEpoch);
            leaderEpochStartOffset = log.logEndOffset();
            long now = Time.nowMs();
            followers.clear();
            for (int id : replicas) {
                if (id != localId) {
                    followers.put(id, new FollowerState(now));
                }
            }
            LOG.info("%s: became LEADER epoch %d, LEO=%d HW=%d isr=%s", tp, leaderEpoch, log.logEndOffset(),
                    highWatermark, isr);
            maybeIncrementHighWatermark();
            hwChanged.signalAll();
        } finally {
            lock.unlock();
        }
        notifier.signal();
    }

    public void makeFollower(PartitionMetadata pm) {
        lock.lock();
        try {
            role = Role.FOLLOWER;
            leaderId = pm.leader();
            leaderEpoch = pm.leaderEpoch();
            replicas = pm.replicas();
            isr = pm.isr();
            isrVersion = pm.isrVersion();
            pendingIsr = null;
            followers.clear();
            LOG.info("%s: became FOLLOWER of %d epoch %d, LEO=%d HW=%d", tp, leaderId, leaderEpoch,
                    log.logEndOffset(), highWatermark);
            hwChanged.signalAll(); // wake acks=all waiters so they fail with NOT_LEADER
        } finally {
            lock.unlock();
        }
        notifier.signal();
    }

    /** Same leader epoch, but the controller changed the ISR (e.g. removed a dead follower). */
    public void updateIsrFromMetadata(PartitionMetadata pm) {
        boolean advanced;
        lock.lock();
        try {
            if (role != Role.LEADER || pm.leaderEpoch() != leaderEpoch || pendingIsr != null
                    || pm.isrVersion() <= isrVersion) {
                return;
            }
            isr = pm.isr();
            isrVersion = pm.isrVersion();
            advanced = maybeIncrementHighWatermark();
        } finally {
            lock.unlock();
        }
        if (advanced) {
            notifier.signal();
        }
    }

    // ------------------------------------------------------------------ leader: produce

    public Log.AppendResult appendAsLeader(long producerId, int baseSequence, List<SimpleRecord> records, short acks) {
        Log.AppendResult result;
        lock.lock();
        try {
            if (role != Role.LEADER) {
                throw new MiniLogException(Errors.NOT_LEADER, tp + " leader is " + leaderId);
            }
            if (acks == -1 && isr.size() < config.minInsyncReplicas()) {
                throw new MiniLogException(Errors.NOT_ENOUGH_REPLICAS, tp + " isr=" + isr);
            }
            result = log.appendAsLeader(producerId, baseSequence, records, leaderEpoch);
            maybeIncrementHighWatermark();
        } finally {
            lock.unlock();
        }
        notifier.signal();
        return result;
    }

    /**
     * acks=all: blocks (cheaply, on a virtual thread) until the high watermark passes
     * {@code requiredOffset}, leadership is lost, or the timeout expires.
     */
    public Errors awaitCommitted(long requiredOffset, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        lock.lock();
        try {
            while (true) {
                if (role != Role.LEADER) {
                    return Errors.NOT_LEADER;
                }
                if (highWatermark >= requiredOffset) {
                    return isr.size() < config.minInsyncReplicas() ? Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND : Errors.NONE;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return Errors.REQUEST_TIMED_OUT;
                }
                hwChanged.awaitNanos(remaining);
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ leader: fetch

    /** Records a follower's progress: its fetch offset is its LEO. May advance the high watermark. */
    public void updateFollowerFetchState(int replicaId, long fetchOffset) {
        boolean advanced;
        lock.lock();
        try {
            if (role != Role.LEADER) {
                return;
            }
            FollowerState fs = followers.get(replicaId);
            if (fs == null) {
                return;
            }
            long now = Time.nowMs();
            long leaderLeo = log.logEndOffset();
            if (fetchOffset >= leaderLeo) {
                fs.lastCaughtUpMs = now;
            } else if (fetchOffset >= fs.lastFetchLeaderLeo && fs.lastFetchMs > 0) {
                // It has everything the leader had at its previous fetch: caught up as of then.
                fs.lastCaughtUpMs = Math.max(fs.lastCaughtUpMs, fs.lastFetchMs);
            }
            fs.logEndOffset = fetchOffset;
            fs.lastFetchLeaderLeo = leaderLeo;
            fs.lastFetchMs = now;
            advanced = maybeIncrementHighWatermark();
        } finally {
            lock.unlock();
        }
        if (advanced) {
            notifier.signal();
        }
    }

    /**
     * Serves one partition of a fetch. Followers may read up to the log end; consumers only up to
     * the high watermark, so they never see records that could still be lost in a failover.
     */
    public PartitionData read(FetchPartition fp, boolean fromFollower) {
        if (role != Role.LEADER) {
            return error(fp, Errors.NOT_LEADER);
        }
        if (fp.currentLeaderEpoch() >= 0 && fp.currentLeaderEpoch() != leaderEpoch) {
            return error(fp, Errors.FENCED_LEADER_EPOCH);
        }
        long hw = highWatermark;
        try {
            byte[] data = log.read(fp.fetchOffset(), fp.maxBytes(), fromFollower ? Long.MAX_VALUE : hw);
            return new PartitionData(fp.topic(), fp.partition(), Errors.NONE, hw, log.logStartOffset(),
                    log.logEndOffset(), data);
        } catch (OffsetOutOfRangeException e) {
            return error(fp, Errors.OFFSET_OUT_OF_RANGE);
        }
    }

    private PartitionData error(FetchPartition fp, Errors e) {
        return new PartitionData(fp.topic(), fp.partition(), e, highWatermark, log.logStartOffset(),
                log.logEndOffset(), new byte[0]);
    }

    // ------------------------------------------------------------------ leader: ISR maintenance

    /**
     * Periodic check (every ~100 ms): shrink the ISR by followers that stopped catching up, expand
     * it with followers that caught up to the high watermark. The change is proposed to the
     * controller and applied only when the controller accepts it.
     */
    public void maybeUpdateIsr(IsrChannel channel) {
        List<Integer> proposed;
        int epoch;
        int version;
        lock.lock();
        try {
            if (role != Role.LEADER || pendingIsr != null) {
                return;
            }
            long now = Time.nowMs();
            Set<Integer> next = new LinkedHashSet<>();
            for (int id : isr) {
                FollowerState fs = followers.get(id);
                if (id == localId || (fs != null && now - fs.lastCaughtUpMs <= config.replicaLagTimeMs())) {
                    next.add(id);
                }
            }
            for (int id : replicas) {
                FollowerState fs = followers.get(id);
                if (!next.contains(id) && !isr.contains(id) && fs != null
                        && fs.logEndOffset >= Math.max(highWatermark, leaderEpochStartOffset)
                        && now - fs.lastCaughtUpMs <= config.replicaLagTimeMs()) {
                    next.add(id);
                }
            }
            if (next.equals(new LinkedHashSet<>(isr))) {
                return;
            }
            proposed = new ArrayList<>(next);
            pendingIsr = proposed;
            epoch = leaderEpoch;
            version = isrVersion;
        } finally {
            lock.unlock();
        }

        AlterIsrResponse response = null;
        try {
            response = channel.alterIsr(tp, epoch, version, proposed);
        } catch (Exception e) {
            LOG.warn("%s: AlterIsr failed: %s", tp, e);
        }

        boolean advanced = false;
        lock.lock();
        try {
            pendingIsr = null;
            if (response != null && leaderEpoch == epoch && response.leaderEpoch() == epoch
                    && response.isrVersion() > isrVersion
                    && (response.error() == Errors.NONE || response.error() == Errors.STALE_ISR_VERSION)) {
                if (response.error() == Errors.NONE) {
                    LOG.info("%s: ISR %s -> %s", tp, isr, response.isr());
                }
                isr = response.isr();
                isrVersion = response.isrVersion();
            }
            advanced = maybeIncrementHighWatermark();
        } finally {
            lock.unlock();
        }
        if (advanced) {
            notifier.signal();
        }
    }

    /** HW = min LEO over (ISR ∪ pending ISR). Never moves backwards. Caller holds the lock. */
    private boolean maybeIncrementHighWatermark() {
        if (role != Role.LEADER) {
            return false;
        }
        Set<Integer> effective = new LinkedHashSet<>(isr);
        if (pendingIsr != null) {
            effective.addAll(pendingIsr);
        }
        long min = log.logEndOffset();
        for (int id : effective) {
            if (id != localId) {
                FollowerState fs = followers.get(id);
                min = Math.min(min, fs == null ? -1 : fs.logEndOffset);
            }
        }
        if (min > highWatermark) {
            highWatermark = min;
            hwChanged.signalAll();
            return true;
        }
        return false;
    }

    // ------------------------------------------------------------------ follower side

    /** Appends records fetched from the leader, if this replica is still a follower in that epoch. */
    public boolean appendAsFollower(byte[] records, long leaderHighWatermark, int expectedEpoch) {
        lock.lock();
        try {
            if (role != Role.FOLLOWER || leaderEpoch != expectedEpoch) {
                return false;
            }
            log.appendAsFollower(records);
            long newHw = Math.min(leaderHighWatermark, log.logEndOffset());
            if (newHw > highWatermark) {
                highWatermark = newHw;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Follower log reconciliation after a leader change. */
    public void truncateAsFollower(long offset, int expectedEpoch) {
        lock.lock();
        try {
            if (role != Role.FOLLOWER || leaderEpoch != expectedEpoch) {
                return;
            }
            log.truncateTo(offset);
            highWatermark = Math.min(highWatermark, log.logEndOffset());
        } finally {
            lock.unlock();
        }
    }

    public void truncateFullyAndStartAt(long offset, int expectedEpoch) {
        lock.lock();
        try {
            if (role != Role.FOLLOWER || leaderEpoch != expectedEpoch) {
                return;
            }
            log.truncateFullyAndStartAt(offset);
            highWatermark = offset;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ accessors

    public TopicPartition tp() {
        return tp;
    }

    public Role role() {
        return role;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public int leaderId() {
        return leaderId;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public List<Integer> isr() {
        return isr;
    }

    public Log log() {
        return log;
    }
}
