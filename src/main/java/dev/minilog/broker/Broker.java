package dev.minilog.broker;

import dev.minilog.common.Logger;
import dev.minilog.common.Time;
import dev.minilog.common.TopicPartition;
import dev.minilog.network.Connection;
import dev.minilog.network.SocketServer;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Message;
import dev.minilog.protocol.Messages;
import dev.minilog.protocol.Messages.AlterIsrResponse;
import dev.minilog.protocol.Messages.BrokerHeartbeatRequest;
import dev.minilog.protocol.Messages.BrokerHeartbeatResponse;
import dev.minilog.storage.LogManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

/**
 * A broker process: storage ({@link LogManager}), replication ({@link ReplicaManager}), the TCP
 * server, and two background loops: heartbeats to the controller (which also deliver metadata)
 * and ISR maintenance.
 */
public final class Broker implements Closeable {
    private final Logger log;
    private final BrokerConfig config;
    private final long incarnationId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    private LogManager logManager;
    private ReplicaManager replicaManager;
    private SocketServer server;
    private volatile boolean running;
    private Thread heartbeatThread;
    private Thread isrThread;
    private Connection heartbeatConnection;
    private Connection isrConnection;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.log = Logger.get("broker-" + config.brokerId());
    }

    public Broker start() throws IOException {
        logManager = new LogManager(config.dataDir(), config.logConfig());
        replicaManager = new ReplicaManager(config, logManager);
        server = new SocketServer("broker-" + config.brokerId() + "-net", config.host(), config.port(), this::handle);
        server.start();
        logManager.startBackgroundTasks(replicaManager::maxDeletableOffset);
        running = true;
        heartbeatThread = Thread.ofVirtual().name("heartbeat-" + config.brokerId()).start(this::heartbeatLoop);
        isrThread = Thread.ofVirtual().name("isr-" + config.brokerId()).start(this::isrLoop);
        log.info("Broker %d listening on %s:%d (data %s)", config.brokerId(), config.host(), server.port(), config.dataDir());
        return this;
    }

    private Message handle(ApiKey api, Message req) throws Exception {
        return switch (api) {
            case PRODUCE -> {
                Messages.ProduceRequest pr = (Messages.ProduceRequest) req;
                Messages.ProduceResponse resp = replicaManager.produce(pr);
                yield pr.acks() == 0 ? null : resp;
            }
            case FETCH -> replicaManager.fetch((Messages.FetchRequest) req);
            case LIST_OFFSETS -> replicaManager.listOffsets((Messages.ListOffsetsRequest) req);
            case METADATA -> new Messages.MetadataResponse(replicaManager.metadata());
            case OFFSET_FOR_LEADER_EPOCH -> replicaManager.offsetForLeaderEpoch((Messages.OffsetForLeaderEpochRequest) req);
            default -> throw new IllegalArgumentException("Broker does not serve " + api + " (send it to the controller)");
        };
    }

    // ------------------------------------------------------------------ background loops

    private void heartbeatLoop() {
        while (running) {
            try {
                if (heartbeatConnection == null || heartbeatConnection.isBroken()) {
                    heartbeatConnection = new Connection(config.controllerHost(), config.controllerPort(), 5000);
                }
                BrokerHeartbeatResponse resp = heartbeatConnection.call(ApiKey.BROKER_HEARTBEAT,
                        new BrokerHeartbeatRequest(config.brokerId(), incarnationId, config.host(), server.port(),
                                replicaManager.metadata().version()));
                if (resp.metadata() != null && running) {
                    replicaManager.applyMetadata(resp.metadata());
                }
            } catch (IOException e) {
                heartbeatConnection = null;
                if (running) {
                    log.debug("heartbeat failed: %s", e);
                }
            } catch (RuntimeException e) {
                log.warn("heartbeat loop error: %s", e);
            }
            Time.sleepQuietly(config.heartbeatIntervalMs());
        }
    }

    private void isrLoop() {
        while (running) {
            try {
                replicaManager.maintainIsr(this::sendAlterIsr);
            } catch (RuntimeException e) {
                log.warn("isr loop error: %s", e);
            }
            Time.sleepQuietly(100);
        }
    }

    private AlterIsrResponse sendAlterIsr(TopicPartition tp, int leaderEpoch, int isrVersion, List<Integer> newIsr)
            throws IOException {
        if (!running) {
            throw new IOException("broker stopping");
        }
        try {
            if (isrConnection == null || isrConnection.isBroken()) {
                isrConnection = new Connection(config.controllerHost(), config.controllerPort(), 5000);
            }
            return isrConnection.call(ApiKey.ALTER_ISR, new Messages.AlterIsrRequest(config.brokerId(), tp.topic(),
                    tp.partition(), leaderEpoch, isrVersion, newIsr));
        } catch (IOException e) {
            isrConnection = null;
            throw e;
        }
    }

    // ------------------------------------------------------------------ lifecycle

    /** Clean shutdown: stop serving, flush logs, write the clean-shutdown marker. */
    @Override
    public void close() throws IOException {
        stopThreads();
        logManager.close();
        log.info("Broker %d stopped cleanly", config.brokerId());
    }

    /**
     * Test helper simulating {@code kill -9}: stop everything abruptly, no flush, no clean-shutdown
     * marker. (Written data is still in the OS page cache, exactly as after a real process crash.)
     */
    public void kill() {
        stopThreads();
        logManager.closeWithoutFlush();
        log.info("Broker %d KILLED", config.brokerId());
    }

    private void stopThreads() {
        running = false;
        server.close();
        replicaManager.shutdown();
        heartbeatThread.interrupt();
        isrThread.interrupt();
        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }
        if (isrConnection != null) {
            isrConnection.close();
        }
        try {
            heartbeatThread.join(2000);
            isrThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int port() {
        return server.port();
    }

    public int id() {
        return config.brokerId();
    }

    public BrokerConfig config() {
        return config;
    }

    public ReplicaManager replicaManager() {
        return replicaManager;
    }

    /** Test hook: process requests matching the predicate but drop the response (lost ack). */
    public void dropResponsesIf(Predicate<Message> predicate) {
        server.setDropResponseIf(predicate);
    }
}
