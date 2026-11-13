# minilog wire protocol

A small binary request/response protocol over TCP. Every request gets exactly one response,
except `PRODUCE` with `acks=0`, which gets none. Requests on one connection are handled strictly
in order. The source of truth is `src/main/java/dev/minilog/protocol/` (`Messages.java`,
`Codec.java`, `Frame.java`, `Wire.java`). Every message has a round-trip test in
`ProtocolCodecTest`.

## Framing

```
frame            = int32 size | payload          (size = number of payload bytes, max 64 MiB)
request payload  = int16 apiKey | int32 correlationId | request body
response payload = int32 correlationId | response body
```

All integers are big-endian (Java's `DataOutputStream` default). The client checks that each
response's `correlationId` matches the request it sent. A frame with trailing bytes is
rejected.

## Primitive types

| type      | encoding                                         |
|-----------|--------------------------------------------------|
| int16/32/64 | big-endian two's complement                    |
| bool      | 1 byte (0 / 1)                                   |
| string    | int16 length (-1 = null) + UTF-8 bytes           |
| bytes     | int32 length (-1 = null) + raw bytes             |
| [T]       | int32 count + count × T                          |
| error     | int16 code (see table at the end)                |

## Record format (on disk and on the wire)

`FETCH` responses carry raw log bytes, so the broker never re-encodes records:

```
int32 length        bytes after this field
uint32 crc          CRC-32C of every byte after this field
int64 offset
int64 timestamp     epoch millis, set by the producer
int32 leaderEpoch   epoch of the leader that first appended the record
int64 producerId    -1 if not idempotent
int32 sequence      -1 if not idempotent
int32 keyLength     -1 = null
bytes key
int32 valueLength   -1 = null
bytes value
```

Fixed overhead: 48 bytes per record.

## APIs

| key | name | served by | purpose |
|----:|------|-----------|---------|
| 0 | PRODUCE | partition leader | append batches |
| 1 | FETCH | partition leader | read records (consumers and followers) |
| 2 | LIST_OFFSETS | partition leader | earliest / latest (high watermark) offset |
| 3 | METADATA | any broker or controller | brokers, leaders, ISR |
| 4 | CREATE_TOPIC | controller | create a topic |
| 5 | OFFSET_COMMIT | controller (group coordinator) | commit consumer positions |
| 6 | OFFSET_FETCH | controller (group coordinator) | read committed positions |
| 7 | JOIN_GROUP | controller (group coordinator) | join / rejoin a group, get assignment |
| 8 | HEARTBEAT | controller (group coordinator) | keep membership alive, learn of rebalances |
| 9 | LEAVE_GROUP | controller (group coordinator) | leave a group cleanly |
| 10 | BROKER_HEARTBEAT | controller (internal) | broker liveness + metadata delivery |
| 11 | ALTER_ISR | controller (internal) | leader proposes an ISR change |
| 12 | OFFSET_FOR_LEADER_EPOCH | partition leader (internal) | follower log reconciliation |

### 0 PRODUCE

```
request:  int16 acks (0 | 1 | -1) | int32 timeoutMs | int64 producerId
          [ string topic | int32 partition | int32 baseSequence
            [ int64 timestamp | bytes key | bytes value ] ]
response: [ string topic | int32 partition | error | int64 baseOffset ]
```

* `acks=0`: no response is sent.
* `acks=1`: answered after the leader has appended the batch.
* `acks=-1` ("all"): answered once the high watermark passes the batch, meaning every ISR
  member has it. The request is rejected with `NOT_ENOUGH_REPLICAS` if ISR size <
  `min.insync.replicas`. If the ISR shrinks below that while waiting, the error is
  `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. If the HW does not advance within `timeoutMs`, the error
  is `REQUEST_TIMED_OUT`.
* Idempotence: record *i* of a batch has sequence `baseSequence + i`. A retried batch that was
  already written returns the original `baseOffset` and is not appended again. A gap returns
  `OUT_OF_ORDER_SEQUENCE`.

### 1 FETCH

```
request:  int32 replicaId (-1 = consumer, else follower broker id) | int32 maxWaitMs
          [ string topic | int32 partition | int64 fetchOffset | int32 maxBytes | int32 currentLeaderEpoch ]
response: [ string topic | int32 partition | error | int64 highWatermark
            | int64 logStartOffset | int64 logEndOffset | bytes records ]
```

* Consumers can read only below the high watermark. Followers can read up to the log end.
* For a follower, `fetchOffset` is also its log end offset: the leader uses it to update the
  follower's progress, the ISR and the HW.
* Long poll: if no partition has data, the leader waits up to `maxWaitMs` for an append or HW
  change.
* At least one whole record is returned even if it is larger than `maxBytes`.
* `currentLeaderEpoch` ≥ 0 makes the leader reject the fetch with `FENCED_LEADER_EPOCH` when
  epochs differ.

### 2 LIST_OFFSETS
```
request:  string topic | int32 partition | int64 timestamp (-1 = latest/HW, -2 = earliest)
response: error | int64 offset
```

### 3 METADATA
```
request:  (empty)
response: ClusterMetadata
ClusterMetadata = int64 version | string controllerHost | int32 controllerPort
                  [ int32 brokerId | string host | int32 port ]                  (live brokers)
                  [ string topic [ int32 partition | int32 leader (-1 = offline) | int32 leaderEpoch
                                   | int32 isrVersion | [int32] replicas | [int32] isr ] ]
```

### 4 CREATE_TOPIC
```
request:  string topic | int32 partitions | int16 replicationFactor
response: error | string message
```
Replicas are placed round-robin over live brokers. The first replica is the initial leader.

### 5 OFFSET_COMMIT / 6 OFFSET_FETCH
```
OFFSET_COMMIT request:  string group | string memberId | int32 generation (-1 = no group check)
                        [ string topic | int32 partition | int64 offset ]
OFFSET_COMMIT response: error            (ILLEGAL_GENERATION if the generation is stale)
OFFSET_FETCH  request:  string group | [ string topic | int32 partition ]
OFFSET_FETCH  response: [ string topic | int32 partition | int64 offset (-1 = none) ]
```

### 7 JOIN_GROUP / 8 HEARTBEAT / 9 LEAVE_GROUP
```
JOIN_GROUP  request:  string group | string memberId ("" on first join) | int32 sessionTimeoutMs
                      | int32 rebalanceTimeoutMs | [string] topics
JOIN_GROUP  response: error | int32 generation | string memberId | [ string topic | int32 partition ]
HEARTBEAT   request:  string group | string memberId | int32 generation
HEARTBEAT   response: error   (REBALANCE_IN_PROGRESS → commit, then rejoin)
LEAVE_GROUP request:  string group | string memberId
LEAVE_GROUP response: error
```
`JOIN_GROUP` blocks until every current member has rejoined (the rebalance barrier) or the
rebalance timeout evicts the members that did not.

### 10 BROKER_HEARTBEAT (internal)
```
request:  int32 brokerId | int64 incarnationId | string host | int32 port | int64 knownMetadataVersion
response: error | bool hasMetadata | [ClusterMetadata if hasMetadata]
```
A new `incarnationId` means the broker restarted. The controller then removes it from every
ISR, so it has to catch up again before it can rejoin.

### 11 ALTER_ISR (internal)
```
request:  int32 brokerId | string topic | int32 partition | int32 leaderEpoch | int32 isrVersion | [int32] newIsr
response: error | int32 leaderEpoch | int32 isrVersion | [int32] isr
```
The controller rejects the request when the sender is not the current leader
(`FENCED_LEADER_EPOCH`), when `isrVersion` is stale (`STALE_ISR_VERSION`), or when the new ISR
contains dead brokers or non-replicas (`INVALID_REQUEST`).

### 12 OFFSET_FOR_LEADER_EPOCH (internal)
```
request:  string topic | int32 partition | int32 leaderEpoch
response: error | int32 leaderEpoch | int64 endOffset
```
Returns the largest epoch ≤ the requested one that the leader knows, plus that epoch's end
offset in the leader's log. The follower truncates to `min(endOffset, its own end of that epoch)`.

## Error codes

| code | name | retriable |
|-----:|------|:---------:|
| 0 | NONE | |
| 1 | UNKNOWN_SERVER_ERROR | no |
| 2 | CORRUPT_MESSAGE | yes |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | yes |
| 4 | OFFSET_OUT_OF_RANGE | no |
| 5 | NOT_LEADER | yes |
| 6 | REQUEST_TIMED_OUT | yes |
| 7 | NOT_ENOUGH_REPLICAS | yes |
| 8 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | yes |
| 9 | TOPIC_ALREADY_EXISTS | no |
| 10 | INVALID_REQUEST | no |
| 11 | INVALID_REPLICATION_FACTOR | no |
| 12 | OUT_OF_ORDER_SEQUENCE | no |
| 13 | FENCED_LEADER_EPOCH | yes |
| 14 | ILLEGAL_GENERATION | no |
| 15 | UNKNOWN_MEMBER_ID | no |
| 16 | REBALANCE_IN_PROGRESS | no |
| 17 | STALE_ISR_VERSION | no |
| 18 | LEADER_NOT_AVAILABLE | yes |
