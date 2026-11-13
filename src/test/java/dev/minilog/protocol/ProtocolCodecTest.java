package dev.minilog.protocol;

import dev.minilog.common.Errors;
import dev.minilog.common.TopicPartition;
import dev.minilog.protocol.Messages.*;
import dev.minilog.storage.SimpleRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolCodecTest {

    static ClusterMetadata sampleMetadata() {
        return new ClusterMetadata(7, new ClusterMetadata.Endpoint("localhost", 9090),
                List.of(new ClusterMetadata.BrokerInfo(1, "localhost", 9091), new ClusterMetadata.BrokerInfo(2, "h2", 9092)),
                List.of(new ClusterMetadata.TopicMetadata("orders", List.of(
                        new ClusterMetadata.PartitionMetadata(0, 1, 3, 5, List.of(1, 2, 3), List.of(1, 2)),
                        new ClusterMetadata.PartitionMetadata(1, -1, 4, 0, List.of(2, 3, 1), List.of(2))))));
    }

    static Stream<Object[]> requests() {
        return Stream.of(
                new Object[]{ApiKey.PRODUCE, new ProduceRequest((short) -1, 3000, 42L, List.of(
                        new ProduceRequest.PartitionBatch("orders", 2, 10, List.of(
                                new SimpleRecord(1L, "k".getBytes(), "v".getBytes()),
                                new SimpleRecord(2L, null, new byte[0]),
                                new SimpleRecord(3L, "k3".getBytes(), null)))))},
                new Object[]{ApiKey.FETCH, new FetchRequest(-1, 500, List.of(
                        new FetchRequest.FetchPartition("orders", 0, 123L, 1 << 20, -1),
                        new FetchRequest.FetchPartition("x", 4, 0L, 10, 7)))},
                new Object[]{ApiKey.LIST_OFFSETS, new ListOffsetsRequest("orders", 1, Messages.EARLIEST)},
                new Object[]{ApiKey.METADATA, new MetadataRequest()},
                new Object[]{ApiKey.CREATE_TOPIC, new CreateTopicRequest("orders", 6, (short) 3)},
                new Object[]{ApiKey.OFFSET_COMMIT, new OffsetCommitRequest("g", "m-1", 4,
                        List.of(new PartitionOffset("orders", 0, 99L)))},
                new Object[]{ApiKey.OFFSET_FETCH, new OffsetFetchRequest("g", List.of(new TopicPartition("orders", 0)))},
                new Object[]{ApiKey.JOIN_GROUP, new JoinGroupRequest("g", "", 6000, 10000, List.of("a", "b"))},
                new Object[]{ApiKey.HEARTBEAT, new HeartbeatRequest("g", "m-1", 3)},
                new Object[]{ApiKey.LEAVE_GROUP, new LeaveGroupRequest("g", "m-1")},
                new Object[]{ApiKey.BROKER_HEARTBEAT, new BrokerHeartbeatRequest(2, 99L, "localhost", 9092, 5L)},
                new Object[]{ApiKey.ALTER_ISR, new AlterIsrRequest(1, "orders", 0, 3, 5, List.of(1, 3))},
                new Object[]{ApiKey.OFFSET_FOR_LEADER_EPOCH, new OffsetForLeaderEpochRequest("orders", 0, 3)});
    }

    @ParameterizedTest
    @MethodSource("requests")
    void requestRoundTrip(ApiKey api, Message request) throws IOException {
        byte[] payload = Codec.encodeRequest(api, 17, request);
        Codec.Request decoded = Codec.decodeRequest(payload);
        assertEquals(api, decoded.api());
        assertEquals(17, decoded.correlationId());
        // Re-encoding the decoded message must give identical bytes (records with byte[] have identity equals)
        assertArrayEquals(payload, Codec.encodeRequest(api, 17, decoded.body()));
    }

    static Stream<Object[]> responses() {
        return Stream.of(
                new Object[]{ApiKey.PRODUCE, new ProduceResponse(List.of(
                        new ProduceResponse.PartitionResult("orders", 0, Errors.NONE, 100L),
                        new ProduceResponse.PartitionResult("orders", 1, Errors.NOT_LEADER, -1L)))},
                new Object[]{ApiKey.FETCH, new FetchResponse(List.of(new FetchResponse.PartitionData("orders", 0,
                        Errors.NONE, 50L, 0L, 60L, new byte[]{1, 2, 3})))},
                new Object[]{ApiKey.LIST_OFFSETS, new ListOffsetsResponse(Errors.NONE, 77L)},
                new Object[]{ApiKey.METADATA, new MetadataResponse(sampleMetadata())},
                new Object[]{ApiKey.CREATE_TOPIC, new CreateTopicResponse(Errors.TOPIC_ALREADY_EXISTS, "orders")},
                new Object[]{ApiKey.OFFSET_COMMIT, new OffsetCommitResponse(Errors.ILLEGAL_GENERATION)},
                new Object[]{ApiKey.OFFSET_FETCH, new OffsetFetchResponse(List.of(new PartitionOffset("o", 1, -1L)))},
                new Object[]{ApiKey.JOIN_GROUP, new JoinGroupResponse(Errors.NONE, 3, "m-1",
                        List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)))},
                new Object[]{ApiKey.HEARTBEAT, new ErrorResponse(Errors.REBALANCE_IN_PROGRESS)},
                new Object[]{ApiKey.BROKER_HEARTBEAT, new BrokerHeartbeatResponse(Errors.NONE, sampleMetadata())},
                new Object[]{ApiKey.BROKER_HEARTBEAT, new BrokerHeartbeatResponse(Errors.NONE, null)},
                new Object[]{ApiKey.ALTER_ISR, new AlterIsrResponse(Errors.NONE, 3, 6, List.of(1, 2, 3))},
                new Object[]{ApiKey.OFFSET_FOR_LEADER_EPOCH, new OffsetForLeaderEpochResponse(Errors.NONE, 2, 1234L)});
    }

    @ParameterizedTest
    @MethodSource("responses")
    void responseRoundTrip(ApiKey api, Message response) throws IOException {
        byte[] payload = Codec.encodeResponse(5, response);
        Message decoded = Codec.decodeResponse(api, 5, payload);
        assertArrayEquals(payload, Codec.encodeResponse(5, decoded));
        if (!(response instanceof FetchResponse)) {
            assertEquals(response.toString().replaceAll("\\[B@\\w+", ""), decoded.toString().replaceAll("\\[B@\\w+", ""));
        }
    }

    @Test
    void decodedValuesMatchFieldByField() throws IOException {
        ProduceRequest req = new ProduceRequest((short) 1, 100, 9L, List.of(new ProduceRequest.PartitionBatch("t", 3, 7,
                List.of(new SimpleRecord(5L, "key".getBytes(), "value".getBytes())))));
        ProduceRequest back = (ProduceRequest) Codec.decodeRequest(Codec.encodeRequest(ApiKey.PRODUCE, 1, req)).body();
        assertEquals(1, back.acks());
        assertEquals(9L, back.producerId());
        ProduceRequest.PartitionBatch b = back.batches().getFirst();
        assertEquals(new TopicPartition("t", 3), b.tp());
        assertEquals(7, b.baseSequence());
        assertArrayEquals("value".getBytes(), b.records().getFirst().value());
        assertEquals(sampleMetadata(), ((MetadataResponse) Codec.decodeResponse(ApiKey.METADATA, 2,
                Codec.encodeResponse(2, new MetadataResponse(sampleMetadata())))).metadata());
    }

    @Test
    void framingAndCorrelationErrorsAreDetected() throws IOException {
        byte[] payload = Codec.encodeResponse(5, new ListOffsetsResponse(Errors.NONE, 1));
        assertThrows(IOException.class, () -> Codec.decodeResponse(ApiKey.LIST_OFFSETS, 6, payload));
        byte[] trailing = Arrays.copyOf(payload, payload.length + 1);
        assertThrows(IOException.class, () -> Codec.decodeResponse(ApiKey.LIST_OFFSETS, 5, trailing));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Frame.write(new DataOutputStream(bytes), payload);
        assertArrayEquals(payload, Frame.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))));
        byte[] hugeSize = {0x7F, 0, 0, 0};
        assertThrows(IOException.class, () -> Frame.read(new DataInputStream(new ByteArrayInputStream(hugeSize))));
    }
}
