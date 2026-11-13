package dev.minilog.protocol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Maps between messages and frame payloads.
 *
 * <pre>
 * request frame payload:  int16 apiKey | int32 correlationId | body
 * response frame payload: int32 correlationId | body
 * </pre>
 * The correlation id lets a client match a response to its request and detect protocol bugs.
 */
public final class Codec {
    private Codec() {
    }

    public record Request(ApiKey api, int correlationId, Message body) {
    }

    public static byte[] encodeRequest(ApiKey api, int correlationId, Message body) {
        return encode(out -> {
            out.writeShort(api.id());
            out.writeInt(correlationId);
            body.write(out);
        });
    }

    public static Request decodeRequest(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        ApiKey api = ApiKey.fromId(in.readShort());
        int correlationId = in.readInt();
        Message body = switch (api) {
            case PRODUCE -> Messages.ProduceRequest.read(in);
            case FETCH -> Messages.FetchRequest.read(in);
            case LIST_OFFSETS -> Messages.ListOffsetsRequest.read(in);
            case METADATA -> Messages.MetadataRequest.read(in);
            case CREATE_TOPIC -> Messages.CreateTopicRequest.read(in);
            case OFFSET_COMMIT -> Messages.OffsetCommitRequest.read(in);
            case OFFSET_FETCH -> Messages.OffsetFetchRequest.read(in);
            case JOIN_GROUP -> Messages.JoinGroupRequest.read(in);
            case HEARTBEAT -> Messages.HeartbeatRequest.read(in);
            case LEAVE_GROUP -> Messages.LeaveGroupRequest.read(in);
            case BROKER_HEARTBEAT -> Messages.BrokerHeartbeatRequest.read(in);
            case ALTER_ISR -> Messages.AlterIsrRequest.read(in);
            case OFFSET_FOR_LEADER_EPOCH -> Messages.OffsetForLeaderEpochRequest.read(in);
        };
        requireFullyConsumed(in, api);
        return new Request(api, correlationId, body);
    }

    public static byte[] encodeResponse(int correlationId, Message body) {
        return encode(out -> {
            out.writeInt(correlationId);
            body.write(out);
        });
    }

    /** Decodes a response body; the caller knows which API it called. */
    public static Message decodeResponse(ApiKey api, int expectedCorrelationId, byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        int correlationId = in.readInt();
        if (correlationId != expectedCorrelationId) {
            throw new IOException("Correlation id mismatch: expected " + expectedCorrelationId + " got " + correlationId);
        }
        Message body = switch (api) {
            case PRODUCE -> Messages.ProduceResponse.read(in);
            case FETCH -> Messages.FetchResponse.read(in);
            case LIST_OFFSETS -> Messages.ListOffsetsResponse.read(in);
            case METADATA -> Messages.MetadataResponse.read(in);
            case CREATE_TOPIC -> Messages.CreateTopicResponse.read(in);
            case OFFSET_COMMIT -> Messages.OffsetCommitResponse.read(in);
            case OFFSET_FETCH -> Messages.OffsetFetchResponse.read(in);
            case JOIN_GROUP -> Messages.JoinGroupResponse.read(in);
            case HEARTBEAT, LEAVE_GROUP -> Messages.ErrorResponse.read(in);
            case BROKER_HEARTBEAT -> Messages.BrokerHeartbeatResponse.read(in);
            case ALTER_ISR -> Messages.AlterIsrResponse.read(in);
            case OFFSET_FOR_LEADER_EPOCH -> Messages.OffsetForLeaderEpochResponse.read(in);
        };
        requireFullyConsumed(in, api);
        return body;
    }

    private static void requireFullyConsumed(DataInputStream in, ApiKey api) throws IOException {
        if (in.available() != 0) {
            throw new IOException(api + ": " + in.available() + " trailing bytes in frame");
        }
    }

    @FunctionalInterface
    private interface Body {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] encode(Body body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(bytes);
            body.write(out);
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e); // cannot happen for an in-memory stream
        }
    }
}
