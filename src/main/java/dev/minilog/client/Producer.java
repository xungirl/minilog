package dev.minilog.client;

import dev.minilog.common.Errors;
import dev.minilog.common.Logger;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Messages.ProduceRequest;
import dev.minilog.protocol.Messages.ProduceResponse;
import dev.minilog.storage.RecordCodec;
import dev.minilog.storage.SimpleRecord;

import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronous, batching, retrying producer.
 *
 * <pre>
 *  send() ──► accumulator: one queue of batches per partition ──► sender thread
 *                                                                 │ drains ready batches (full, or older than linger.ms)
 *                                                                 │ groups them by leader broker
 *                                                                 │ sends one ProduceRequest per broker, in parallel
 *                                                                 ▼ completes futures / re-queues failed batches at the head
 * </pre>
 *
 * <p><b>Idempotence:</b> the producer has a random 63-bit producerId and numbers the records it
 * sends to each partition 0, 1, 2, ... A sequence is assigned once, when a batch is first sent,
 * and a retried batch keeps it, so the leader recognises the retry and does not write it twice.
 * Each partition has at most one batch in flight, which keeps retries in order.
 */
public final class Producer implements Closeable {
    private static final Logger LOG = Logger.get("producer");
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Records for one partition that are sent together. */
    private static final class Batch {
        final TopicPartition tp;
        final long createdMs;
        final List<SimpleRecord> records = new ArrayList<>();
        final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        int bytes;
        boolean sealed;          // true once sent: no more records may be added
        int baseSequence = -1;   // assigned on first send, kept on retries
        long producerId = -1;    // producer id the sequence belongs to
        int attempts;
        long retryAfterMs;

        Batch(TopicPartition tp, long createdMs) {
            this.tp = tp;
            this.createdMs = createdMs;
        }
    }

    private final ProducerConfig config;
    private final ClusterClient cluster;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<TopicPartition, Deque<Batch>> queues = new HashMap<>();
    private final Map<TopicPartition, Integer> nextSequence = new HashMap<>();
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final ExecutorService ioPool = Executors.newVirtualThreadPerTaskExecutor();
    private final Thread sender;
    private long bufferedBytes;
    private int inFlightBatches;
    private int flushWaiters;
    private volatile long producerId;
    private volatile boolean closing;
    private volatile boolean metadataStale = true;

    public Producer(ProducerConfig config) {
        this.config = config;
        this.cluster = new ClusterClient(config.bootstrap(), config.requestTimeoutMs() + 5000);
        this.producerId = config.idempotence() ? newProducerId() : -1;
        this.sender = Thread.ofPlatform().name("producer-sender").daemon().start(this::senderLoop);
    }

    private static long newProducerId() {
        return RANDOM.nextLong() & Long.MAX_VALUE;
    }

    // ------------------------------------------------------------------ public API

    /** Queues a record; the future completes when the broker acknowledges it (per the acks setting). */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) throws IOException {
        int partition = choosePartition(topic, key);
        return send(new TopicPartition(topic, partition), key, value);
    }

    /** Queues a record for an explicit partition. */
    public CompletableFuture<RecordMetadata> send(TopicPartition tp, byte[] key, byte[] value) {
        SimpleRecord record = new SimpleRecord(System.currentTimeMillis(), key, value);
        int size = RecordCodec.sizeInBytes(key, value);
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        boolean newBatch = false;
        lock.lock();
        try {
            while (bufferedBytes + size > config.bufferMemory() && !closing) {
                changed.awaitUninterruptibly(); // back-pressure: wait for the sender to free memory
            }
            if (closing) {
                throw new IllegalStateException("producer closed");
            }
            Deque<Batch> q = queues.computeIfAbsent(tp, k -> new ArrayDeque<>());
            Batch last = q.peekLast();
            if (last == null || last.sealed || (last.bytes + size > config.batchSize() && !last.records.isEmpty())) {
                last = new Batch(tp, System.currentTimeMillis());
                q.addLast(last);
                newBatch = true;
            }
            last.records.add(record);
            last.futures.add(future);
            last.bytes += size;
            bufferedBytes += size;
            // Wake the sender when a batch is full, or when a new batch starts so that the sender
            // can schedule it for createdMs + linger.ms (otherwise it would sleep until its next poll).
            if (newBatch || last.bytes >= config.batchSize()) {
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
        return future;
    }

    /**
     * Key-hash partitioning: the same key always goes to the same partition, which preserves
     * per-key ordering. Records without a key are spread round-robin.
     */
    public int choosePartition(String topic, byte[] key) throws IOException {
        int n = cluster.partitionCount(topic);
        if (key == null) {
            return Math.floorMod(roundRobin.getAndIncrement(), n);
        }
        return Math.floorMod(Arrays.hashCode(key), n);
    }

    /** Blocks until every record sent so far has been acknowledged or failed. */
    public void flush() {
        lock.lock();
        try {
            flushWaiters++;
            changed.signalAll();
            while (bufferedBytes > 0 || inFlightBatches > 0) {
                changed.awaitUninterruptibly();
            }
            flushWaiters--;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        flush();
        closing = true;
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        try {
            sender.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ioPool.shutdownNow();
        cluster.close();
    }

    // ------------------------------------------------------------------ sender

    private void senderLoop() {
        while (!closing) {
            try {
                List<Batch> ready = awaitReadyBatches();
                if (ready.isEmpty()) {
                    continue;
                }
                if (metadataStale) {
                    refreshMetadataQuietly();
                }
                sendBatches(ready);
            } catch (RuntimeException e) {
                LOG.warn("sender loop error: %s", e);
            }
        }
    }

    /**
     * Waits until at least one partition's head batch is ready (full, lingered long enough, or
     * flush requested), then takes the head batch of every ready partition.
     */
    private List<Batch> awaitReadyBatches() {
        lock.lock();
        try {
            while (!closing) {
                long now = System.currentTimeMillis();
                long nextWakeup = Long.MAX_VALUE;
                List<Batch> ready = new ArrayList<>();
                boolean flushing = flushWaiters > 0;
                for (Deque<Batch> q : queues.values()) {
                    Batch head = q.peekFirst();
                    if (head == null) {
                        continue;
                    }
                    long readyAt = head.sealed ? head.retryAfterMs
                            : (head.bytes >= config.batchSize() || q.size() > 1 || flushing
                            ? now : head.createdMs + config.lingerMs());
                    if (readyAt <= now) {
                        Batch b = q.pollFirst();
                        b.sealed = true;
                        ready.add(b);
                    } else {
                        nextWakeup = Math.min(nextWakeup, readyAt);
                    }
                }
                if (!ready.isEmpty()) {
                    inFlightBatches += ready.size();
                    return ready;
                }
                long waitMs = nextWakeup == Long.MAX_VALUE ? 100 : Math.max(1, nextWakeup - now);
                changed.await(waitMs, TimeUnit.MILLISECONDS);
            }
            return List.of();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return List.of();
        } finally {
            lock.unlock();
        }
    }

    private void sendBatches(List<Batch> batches) {
        long now = System.currentTimeMillis();
        long pid = producerId;
        Map<Integer, List<Batch>> byLeader = new HashMap<>();
        List<Batch> noLeader = new ArrayList<>();
        for (Batch b : batches) {
            // Assign sequence numbers on first send, or again if the producer id was reset.
            if (pid >= 0 && b.producerId != pid) {
                b.producerId = pid;
                b.baseSequence = nextSequence.merge(b.tp, b.records.size(), Integer::sum) - b.records.size();
            }
            int leader = cluster.leaderFor(b.tp);
            if (leader < 0 || cluster.metadata().broker(leader).isEmpty()) {
                noLeader.add(b);
            } else {
                byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(b);
            }
        }
        for (Batch b : noLeader) {
            onFailure(b, Errors.LEADER_NOT_AVAILABLE, now);
        }
        List<Future<?>> inFlight = new ArrayList<>();
        for (Map.Entry<Integer, List<Batch>> e : byLeader.entrySet()) {
            inFlight.add(ioPool.submit(() -> sendToBroker(e.getKey(), pid, e.getValue())));
        }
        for (Future<?> f : inFlight) {
            try {
                f.get();
            } catch (Exception ex) {
                LOG.warn("send task failed: %s", ex);
            }
        }
    }

    private void sendToBroker(int brokerId, long pid, List<Batch> batches) {
        List<ProduceRequest.PartitionBatch> payload = new ArrayList<>();
        for (Batch b : batches) {
            payload.add(new ProduceRequest.PartitionBatch(b.tp.topic(), b.tp.partition(), b.baseSequence, b.records));
        }
        ProduceRequest request = new ProduceRequest(config.acks(), config.requestTimeoutMs(), pid, payload);
        long now = System.currentTimeMillis();
        try {
            Connection conn = cluster.connectionTo(brokerId);
            if (config.acks() == 0) {
                conn.sendNoResponse(ApiKey.PRODUCE, request);
                batches.forEach(b -> onSuccess(b, -1));
                return;
            }
            ProduceResponse response = conn.call(ApiKey.PRODUCE, request);
            for (int i = 0; i < batches.size(); i++) {
                ProduceResponse.PartitionResult r = response.results().get(i);
                if (r.error() == Errors.NONE) {
                    onSuccess(batches.get(i), r.baseOffset());
                } else {
                    onFailure(batches.get(i), r.error(), now);
                }
            }
        } catch (IOException e) {
            // Connection failed: the request may or may not have been applied. Retrying is safe
            // with idempotence (the broker deduplicates); without it this can duplicate records.
            cluster.invalidate(brokerId);
            batches.forEach(b -> onFailure(b, Errors.NOT_LEADER, now));
        }
    }

    private void onSuccess(Batch b, long baseOffset) {
        for (int i = 0; i < b.futures.size(); i++) {
            b.futures.get(i).complete(new RecordMetadata(b.tp, baseOffset < 0 ? -1 : baseOffset + i));
        }
        release(b);
    }

    private void onFailure(Batch b, Errors error, long now) {
        b.attempts++;
        boolean expired = now - b.createdMs > config.deliveryTimeoutMs();
        if (error.retriable() && !expired && !closing) {
            metadataStale = true;
            b.retryAfterMs = now + config.retryBackoffMs();
            lock.lock();
            try {
                queues.computeIfAbsent(b.tp, k -> new ArrayDeque<>()).addFirst(b); // retry before newer batches
                inFlightBatches--;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
            return;
        }
        LOG.warn("giving up on batch for %s after %d attempts: %s", b.tp, b.attempts, error);
        if (producerId >= 0) {
            // The sequence for this partition now has a gap the broker will never see filled.
            // Like Kafka's epoch bump, start over with a fresh producer id.
            resetProducerId();
        }
        Exception ex = new IOException("Produce to " + b.tp + " failed: " + error);
        b.futures.forEach(f -> f.completeExceptionally(ex));
        release(b);
    }

    private void resetProducerId() {
        lock.lock();
        try {
            producerId = newProducerId();
            nextSequence.clear();
        } finally {
            lock.unlock();
        }
    }

    private void release(Batch b) {
        lock.lock();
        try {
            bufferedBytes -= b.bytes;
            inFlightBatches--;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void refreshMetadataQuietly() {
        try {
            cluster.refreshMetadata();
            metadataStale = false;
        } catch (IOException e) {
            LOG.debug("metadata refresh failed: %s", e);
        }
    }
}
