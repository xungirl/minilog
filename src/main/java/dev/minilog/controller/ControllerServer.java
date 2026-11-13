package dev.minilog.controller;

import dev.minilog.common.Logger;
import dev.minilog.group.GroupCoordinator;
import dev.minilog.network.SocketServer;
import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Message;
import dev.minilog.protocol.Messages;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The controller process: the {@link Controller} state machine plus the {@link GroupCoordinator},
 * served over the same binary protocol as the brokers.
 */
public final class ControllerServer implements Closeable {
    private static final Logger LOG = Logger.get("controller-server");

    private final Controller controller;
    private final GroupCoordinator groups;
    private final SocketServer server;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "controller-timer");
        t.setDaemon(true);
        return t;
    });

    public ControllerServer(ControllerConfig config) throws IOException {
        this.controller = new Controller(config);
        this.groups = new GroupCoordinator(config.dataDir().resolve("__consumer_offsets"), controller::partitionCount);
        this.server = new SocketServer("controller-net", config.host(), config.port(), this::handle);
    }

    public ControllerServer start() throws IOException {
        server.start();
        controller.setAdvertisedPort(server.port());
        scheduler.scheduleAtFixedRate(this::tick, 50, 50, TimeUnit.MILLISECONDS);
        LOG.info("Controller listening on port %d", server.port());
        return this;
    }

    private void tick() {
        try {
            controller.checkBrokerSessions();
            groups.checkSessions();
        } catch (RuntimeException e) {
            LOG.warn("tick failed: %s", e);
        }
    }

    private Message handle(ApiKey api, Message req) throws Exception {
        return switch (api) {
            case BROKER_HEARTBEAT -> controller.handleBrokerHeartbeat((Messages.BrokerHeartbeatRequest) req);
            case ALTER_ISR -> controller.alterIsr((Messages.AlterIsrRequest) req);
            case CREATE_TOPIC -> controller.createTopic((Messages.CreateTopicRequest) req);
            case METADATA -> new Messages.MetadataResponse(controller.metadata());
            case JOIN_GROUP -> groups.join((Messages.JoinGroupRequest) req);
            case HEARTBEAT -> groups.heartbeat((Messages.HeartbeatRequest) req);
            case LEAVE_GROUP -> groups.leave((Messages.LeaveGroupRequest) req);
            case OFFSET_COMMIT -> groups.commit((Messages.OffsetCommitRequest) req);
            case OFFSET_FETCH -> groups.fetchOffsets((Messages.OffsetFetchRequest) req);
            default -> throw new IllegalArgumentException("Controller does not serve " + api);
        };
    }

    public int port() {
        return server.port();
    }

    public Controller controller() {
        return controller;
    }

    @Override
    public void close() throws IOException {
        scheduler.shutdownNow();
        server.close();
        groups.close();
    }
}
