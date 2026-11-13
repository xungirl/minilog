package dev.minilog.broker;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.MiniLogException;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.ClusterMetadata;
import dev.minilog.protocol.ClusterMetadata.PartitionMetadata;
import dev.minilog.protocol.Messages.FetchRequest;
import dev.minilog.protocol.Messages.FetchRequest.FetchPartition;
import dev.minilog.protocol.Messages.FetchResponse;
import dev.minilog.protocol.Messages.FetchResponse.PartitionData;
import dev.minilog.protocol.Messages.ListOffsetsRequest;
import dev.minilog.protocol.Messages.ListOffsetsResponse;
import dev.minilog.protocol.Messages.OffsetForLeaderEpochRequest;
import dev.minilog.protocol.Messages.OffsetForLeaderEpochResponse;
import dev.minilog.protocol.Messages.ProduceRequest;
import dev.minilog.protocol.Messages.ProduceResponse;
import dev.minilog.protocol.Messages.ProduceResponse.PartitionResult;
import dev.minilog.storage.LeaderEpochCache;
import dev.minilog.storage.Log;
import dev.minilog.storage.LogManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Everything a broker does with partitions: applying leadership decisions from the controller,
 * serving produce/fetch for partitions it leads, and running replica fetchers for partitions it follows.
 */
public final class ReplicaManager {
    private static final Logger LOG = Logger.get("replica-manager");

    private final BrokerConfig config;
    private final LogManager logManager;
    private final DataNotifier notifier = new DataNotifier();
    private final Map<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();
    private final Map<Integer, ReplicaFetcher> fetchers = new ConcurrentHashMap<>();
    private final ReentrantLock metadataLock = new ReentrantLock();
    private volatile ClusterMetadata metadata = ClusterMetadata.EMPTY;
    private volatile boolean replicationPaused;

    public ReplicaManager(BrokerConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
    }

    // ------------------------------------------------------------------ metadata / role changes

    /** Applies a new metadata snapshot from the controller: become leader or follower where it changed. */
    public void applyMetadata(ClusterMetadata md) {
        metadataLock.lock();
        try {
            if (md.version() <= metadata.version()) {
                return;
            }
            metadata = md;
            int me = config.brokerId();
            for (Map.Entry<TopicPartition, PartitionMetadata> e : md.allPartitions().entrySet()) {
                TopicPartition tp = e.getKey();
                PartitionMetadata pm = e.getValue();
                if (!pm.replicas().contains(me)) {
                    continue;
                }
                Partition p = partitions.computeIfAbsent(tp,
                        k -> new Partition(k, me, logManager.getOrCreate(k), config, notifier));
                boolean newEpoch = pm.leaderEpoch() > p.leaderEpoch();
                if (pm.leader() == me) {
                    if (!p.isLeader() || newEpoch) {
                        stopFetching(tp);
                        p.makeLeader(pm);
                    } else {
                        p.updateIsrFromMetadata(pm);
                    }
                } else if (newEpoch || p.isLeader() || p.role() == Partition.Role.NONE) {
                    stopFetching(tp);
                    p.makeFollower(pm);
                    if (pm.leader() >= 0) {
                        fetchers.computeIfAbsent(pm.leader(), id -> {
                            ReplicaFetcher f = new ReplicaFetcher(id, this, config);
                            f.start();
                            return f;
                        }).addPartition(tp, pm.leaderEpoch());
                    }
                }
            }
        } finally {
            metadataLock.unlock();
        }
    }

    private void stopFetching(TopicPartition tp) {
        fetchers.values().forEach(f -> f.removePartition(tp));
    }

    // ------------------------------------------------------------------ produce

    public ProduceResponse produce(ProduceRequest req) throws InterruptedException {
        List<PartitionResult> results = new ArrayList<>();
        List<Long> requiredOffsets = new ArrayList<>();
        for (ProduceRequest.PartitionBatch batch : req.batches()) {
            Partition p = partitions.get(batch.tp());
            try {
                if (p == null) {
                    throw new MiniLogException(metadata.partition(batch.tp()).isPresent()
                            ? Errors.NOT_LEADER : Errors.UNKNOWN_TOPIC_OR_PARTITION, batch.tp().toString());
                }
                Log.AppendResult r = p.appendAsLeader(req.producerId(), batch.baseSequence(), batch.records(), req.acks());
                results.add(new PartitionResult(batch.topic(), batch.partition(), Errors.NONE, r.baseOffset()));
                requiredOffsets.add(r.lastOffset() + 1);
            } catch (MiniLogException e) {
                results.add(new PartitionResult(batch.topic(), batch.partition(), e.error(), -1));
                requiredOffsets.add(-1L);
            } catch (RuntimeException e) {
                LOG.warn("produce to %s failed: %s", batch.tp(), e);
                results.add(new PartitionResult(batch.topic(), batch.partition(), Errors.UNKNOWN_SERVER_ERROR, -1));
                requiredOffsets.add(-1L);
            }
        }
        if (req.acks() == -1) {
            long deadline = System.currentTimeMillis() + req.timeoutMs();
            for (int i = 0; i < results.size(); i++) {
                PartitionResult r = results.get(i);
                if (r.error() != Errors.NONE || requiredOffsets.get(i) <= 0) {
                    continue;
                }
                Partition p = partitions.get(new TopicPartition(r.topic(), r.partition()));
                Errors e = p.awaitCommitted(requiredOffsets.get(i), Math.max(0, deadline - System.currentTimeMillis()));
                if (e != Errors.NONE) {
                    results.set(i, new PartitionResult(r.topic(), r.partition(), e, -1));
                }
            }
        }
        return new ProduceResponse(results);
    }

    // ------------------------------------------------------------------ fetch

    /**
     * Long-poll fetch: returns as soon as any partition has data (or an error), otherwise waits up
     * to maxWaitMs for an append / high-watermark change.
     */
    public FetchResponse fetch(FetchRequest req) throws InterruptedException {
        boolean fromFollower = req.replicaId() >= 0;
        if (fromFollower) {
            for (FetchPartition fp : req.partitions()) {
                Partition p = partitions.get(fp.tp());
                if (p != null) {
                    p.updateFollowerFetchState(req.replicaId(), fp.fetchOffset());
                }
            }
        }
        long deadline = System.currentTimeMillis() + req.maxWaitMs();
        while (true) {
            long seen = notifier.version();
            List<PartitionData> out = new ArrayList<>();
            boolean ready = false;
            for (FetchPartition fp : req.partitions()) {
                Partition p = partitions.get(fp.tp());
                PartitionData pd = p == null
                        ? new PartitionData(fp.topic(), fp.partition(), Errors.NOT_LEADER, -1, -1, -1, new byte[0])
                        : p.read(fp, fromFollower);
                ready |= pd.error() != Errors.NONE || pd.records().length > 0;
                out.add(pd);
            }
            long remaining = deadline - System.currentTimeMillis();
            if (ready || remaining <= 0) {
                return new FetchResponse(out);
            }
            notifier.awaitChange(seen, remaining);
        }
    }

    // ------------------------------------------------------------------ offsets

    public ListOffsetsResponse listOffsets(ListOffsetsRequest req) {
        Partition p = partitions.get(new TopicPartition(req.topic(), req.partition()));
        if (p == null || !p.isLeader()) {
            return new ListOffsetsResponse(Errors.NOT_LEADER, -1);
        }
        long offset = req.timestamp() == dev.minilog.protocol.Messages.EARLIEST
                ? p.log().logStartOffset() : p.highWatermark();
        return new ListOffsetsResponse(Errors.NONE, offset);
    }

    public OffsetForLeaderEpochResponse offsetForLeaderEpoch(OffsetForLeaderEpochRequest req) {
        Partition p = partitions.get(new TopicPartition(req.topic(), req.partition()));
        if (p == null || !p.isLeader()) {
            return new OffsetForLeaderEpochResponse(Errors.NOT_LEADER, -1, -1);
        }
        LeaderEpochCache.EpochEndOffset eo = p.log().endOffsetForEpoch(req.leaderEpoch());
        return new OffsetForLeaderEpochResponse(Errors.NONE, eo.epoch(), eo.endOffset());
    }

    // ------------------------------------------------------------------ background

    /** Runs ISR shrink/expand checks for every partition this broker leads. */
    public void maintainIsr(Partition.IsrChannel channel) {
        for (Partition p : partitions.values()) {
            if (p.isLeader()) {
                p.maybeUpdateIsr(channel);
            }
        }
    }

    /** Retention may only delete below the high watermark. */
    public long maxDeletableOffset(TopicPartition tp) {
        Partition p = partitions.get(tp);
        return p == null ? 0 : p.highWatermark();
    }

    public void shutdown() {
        fetchers.values().forEach(ReplicaFetcher::shutdown);
        fetchers.clear();
    }

    // ------------------------------------------------------------------ accessors

    public ClusterMetadata metadata() {
        return metadata;
    }

    public Partition partition(TopicPartition tp) {
        return partitions.get(tp);
    }

    public Map<TopicPartition, Partition> partitions() {
        return Map.copyOf(partitions);
    }

    public boolean isReplicationPaused() {
        return replicationPaused;
    }

    /** Test hook: stop this broker's followers from fetching (simulates a slow/partitioned follower). */
    public void setReplicationPaused(boolean paused) {
        this.replicationPaused = paused;
    }
}
