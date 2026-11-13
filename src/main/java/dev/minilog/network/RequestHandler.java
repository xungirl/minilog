package dev.minilog.network;

import dev.minilog.protocol.ApiKey;
import dev.minilog.protocol.Message;

/** Server-side request dispatch. Implemented by the broker and the controller. */
@FunctionalInterface
public interface RequestHandler {
    /**
     * Handles one request. Runs on the connection's virtual thread, so it may block (e.g. a
     * long-poll fetch or an acks=all produce waiting for replication).
     *
     * @return the response, or null if no response must be sent (produce with acks=0)
     */
    Message handle(ApiKey api, Message request) throws Exception;
}
