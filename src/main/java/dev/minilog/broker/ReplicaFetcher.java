package dev.minilog.broker;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.protocol.Messages.FetchRequest;
import dev.minilog.protocol.Messages.FetchRequest.FetchPartition;
import dev.minilog.protocol.Messages.FetchResponse;
import dev.minilog.protocol.Messages.FetchResponse.PartitionData;
import dev.minilog.protocol.Messages.OffsetForLeaderEpochRequest;
import dev.minilog.protocol.Messages.OffsetForLeaderEpochResponse;
import dev.minilog.storage.LeaderEpochCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pull-based replication: one fetcher (virtual thread) per leader broker, fetching every
 * partition this broker follows on that leader in a single long-poll request.
 *
 * <p>Before fetching a partition for a new leader epoch, the fetcher reconciles the local log
 * with the leader's: it asks "where does my latest epoch end in your log?"
 * (OffsetForLeaderEpoch) and truncates anything after that point. This removes records the old
 * leader wrote but never committed, so replicas cannot diverge.
 */
final class ReplicaFetcher {
    private static final Logger LOG = Logger.get("replica-fetcher");

    /** Per-partition fetch state; {@code truncated} becomes true once reconciliation is done. */
    private record FetchState(int leaderEpoch, boolean truncated) {
    }

    private final int leaderId;
    private final ReplicaManager replicaManager;
    private final BrokerConfig config;
    private final Map<TopicPartition, FetchState> partitions = new ConcurrentHashMap<>();
    private final Thread thread;
    private volatile boolean running = true;
    private Connection connection;

    ReplicaFetcher(int leaderId, ReplicaManager replicaManager, BrokerConfig config) {
        this.leaderId = leaderId;
        this.replicaManager = replicaManager;
        this.config = config;
        this.thread = Thread.ofVirtual().name("fetcher-" + config.brokerId() + "-from-" + leaderId).unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    void addPartition(TopicPartition tp, int leaderEpoch) {
        partitions.put(tp, new FetchState(leaderEpoch, false));
    }

    void removePartition(TopicPartition tp) {
        partitions.remove(tp);
    }

    void shutdown() {
        running = false;
        thread.interrupt();
        if (connection != null) {
            connection.close();
        }
    }

    private void run() {
        while (running) {
            try {
                if (partitions.isEmpty() || replicaManager.isReplicationPaused()) {
                    Time.sleepQuietly(20);
                    continue;
                }
                if (!ensureConnected()) {
                    Time.sleepQuietly(50);
                    continue;
                }
                for (Map.Entry<TopicPartition, FetchState> e : partitions.entrySet()) {
                    if (!e.getValue().truncated()) {
                        reconcile(e.getKey(), e.getValue());
                    }
                }
                fetchOnce();
            } catch (IOException e) {
                closeConnection();
                if (running) {
                    LOG.debug("fetch from %d failed: %s", leaderId, e);
                    Time.sleepQuietly(50);
                }
            } catch (RuntimeException e) {
                closeConnection();
                if (running) {
                    LOG.warn("fetcher for leader %d error: %s", leaderId, e);
                    Time.sleepQuietly(100);
                }
            }
        }
    }

    private boolean ensureConnected() throws IOException {
        if (connection != null && !connection.isBroken()) {
            return true;
        }
        Optional<ClusterMetadata.BrokerInfo> leader = replicaManager.metadata().broker(leaderId);
        if (leader.isEmpty()) {
            return false;
        }
        connection = new Connection(leader.get().host(), leader.get().port(), config.replicaFetchMaxWaitMs() + 5000);
        return true;
    }

    private void closeConnection() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    /** Truncates the local log to the point where it agrees with the leader's (leader-epoch based). */
    private void reconcile(TopicPartition tp, FetchState state) throws IOException {
        Partition p = replicaManager.partition(tp);
        if (p == null) {
            return;
        }
        int localEpoch = p.log().latestEpoch();
        long leo = p.log().logEndOffset();
        long target;
        if (localEpoch < 0) {
            target = leo; // nothing written under any epoch: nothing to reconcile
        } else {
            OffsetForLeaderEpochResponse resp = connection.call(ApiKey.OFFSET_FOR_LEADER_EPOCH,
                    new OffsetForLeaderEpochRequest(tp.topic(), tp.partition(), localEpoch));
            if (resp.error() != Errors.NONE) {
                Time.sleepQuietly(20); // leader not ready yet for this epoch; retry next round
                return;
            }
            if (resp.leaderEpoch() < 0) {
                target = Math.min(leo, p.highWatermark());
            } else {
                target = Math.min(leo, resp.endOffset());
                if (resp.leaderEpoch() < localEpoch) {
                    // The leader never saw our latest epoch: keep only what we wrote up to the
                    // end of the epoch the leader does know.
                    LeaderEpochCache.EpochEndOffset ours = p.log().endOffsetForEpoch(resp.leaderEpoch());
                    if (ours.endOffset() >= 0) {
                        target = Math.min(target, ours.endOffset());
                    }
                }
            }
        }
        if (target < leo) {
            LOG.info("%s: truncating follower log %d -> %d before fetching from leader %d (epoch %d)",
                    tp, leo, target, leaderId, state.leaderEpoch());
        }
        p.truncateAsFollower(target, state.leaderEpoch());
        partitions.replace(tp, state, new FetchState(state.leaderEpoch(), true));
    }

    private void fetchOnce() throws IOException {
        List<FetchPartition> request = new ArrayList<>();
        Map<TopicPartition, FetchState> snapshot = Map.copyOf(partitions);
        for (Map.Entry<TopicPartition, FetchState> e : snapshot.entrySet()) {
            Partition p = replicaManager.partition(e.getKey());
            if (p != null && e.getValue().truncated()) {
                request.add(new FetchPartition(e.getKey().topic(), e.getKey().partition(), p.log().logEndOffset(),
                        config.replicaFetchMaxBytes(), e.getValue().leaderEpoch()));
            }
        }
        if (request.isEmpty()) {
            Time.sleepQuietly(10);
            return;
        }
        FetchResponse resp = connection.call(ApiKey.FETCH,
                new FetchRequest(config.brokerId(), config.replicaFetchMaxWaitMs(), request));
        boolean backoff = false;
        for (PartitionData pd : resp.partitions()) {
            FetchState state = snapshot.get(pd.tp());
            Partition p = replicaManager.partition(pd.tp());
            if (state == null || p == null || partitions.get(pd.tp()) != state) {
                continue; // reassigned while the request was in flight
            }
            switch (pd.error()) {
                case NONE -> p.appendAsFollower(pd.records(), pd.highWatermark(), state.leaderEpoch());
                case OFFSET_OUT_OF_RANGE -> {
                    if (p.log().logEndOffset() < pd.logStartOffset()) {
                        p.truncateFullyAndStartAt(pd.logStartOffset(), state.leaderEpoch());
                    } else {
                        partitions.replace(pd.tp(), state, new FetchState(state.leaderEpoch(), false));
                    }
                }
                default -> backoff = true; // NOT_LEADER / FENCED_LEADER_EPOCH: wait for new metadata
            }
        }
        if (backoff) {
            Time.sleepQuietly(20);
        }
    }
}
