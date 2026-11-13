# minilog

A Kafka-inspired, replicated commit log and message broker written in Java 21 with no
runtime dependencies. JUnit 5 is used only for tests.

Overview:

- **Storage engine.** Segmented append-only logs with a memory-mapped sparse offset index.
  Every record is CRC-32C checked. A torn tail write is truncated on restart. Old segments are
  deleted by time- and size-based retention. The fsync policy is configurable.
- **Broker.** A binary TCP protocol ([PROTOCOL.md](PROTOCOL.md)) served by one virtual thread
  per connection. Supports batched produce with acks 0 / 1 / all, long-poll fetch, list
  offsets, and consumer-group offset commit/fetch.
- **Replication.** Three brokers. Followers pull from the partition leader. The leader tracks
  the ISR and a high watermark, so consumers only see committed records. Leader epochs drive
  follower log truncation. A static controller detects dead brokers through heartbeats and
  elects new leaders, only from the ISR.
- **Clients.** The producer does key-hash partitioning, batching (`batch.size`, `linger.ms`) and
  idempotent retries. The consumer has a poll loop, consumer groups with range assignment,
  rebalancing on join/leave/timeout, and committed offsets.
- **Evidence.** 55 JUnit tests (10/10 consecutive full-suite runs passed). There is a benchmark
  tool, and a chaos mode that `kill -9`s broker processes at random and then checks
  exactly-once consumption and per-partition ordering. Every number below was measured on one
  laptop. See [BENCHMARK.md](BENCHMARK.md) and the raw output in [`results/`](results/).

> **Measurement environment:** Apple M2 (8 cores, 24 GiB), macOS 15.4, OpenJDK 21.0.5
> (Temurin), measured 2026-11-12. **All brokers, the controller and the clients run on one
> machine over localhost.** There is no real network, and the three replicas share one SSD and
> one page cache. Compare numbers only with each other, not with Kafka on a real cluster.

## Project Journey

This section is the build log: what each stage set out to do, what was chosen and why, how it
was checked, what was measured, and what went wrong. Numbers come from files in `results/`.
Where a number came only from terminal output during development, the text says so.

### Stage 1 — Reproducible build

- **Goal:** a build anyone can run with only a JDK installed.
- **Decision:** a Gradle wrapper (9.4.1) checked into the repo. Maven was the alternative.
  Gradle was already installed through Homebrew, so `gradle wrapper` produced a wrapper
  without adding another tool. The only dependency is JUnit 5.11.3 (tests only).
- **Built:** `build.gradle.kts` (Java 21 toolchain, `-Xlint:all`), `gradlew`, `.gitignore`.
- **Verified:** `./gradlew --version` → Gradle 9.4.1.
- **What went wrong:** the first `./gradlew` call failed while downloading the Gradle
  distribution with `timeout (10000ms)` at about 20 %. The wrapper's default 10 s network
  timeout was too short for this connection. Fix: `networkTimeout=120000` in
  `gradle/wrapper/gradle-wrapper.properties`. The download then finished.

### Stage 2 — Storage engine

- **Goal:** topics → partitions → segmented append-only logs with an index, checksums, crash
  recovery, retention and a configurable fsync policy.
- **Decisions:**
  - *Record format:* one length-prefixed record with its own CRC-32C, rather than Kafka's
    record-batch format with batch headers and compression. It is simpler to explain and still
    detects torn writes and bit flips. The same bytes go over the wire, so fetch never
    re-encodes.
  - *Index:* a sparse index (one entry per 4 KiB) in a memory-mapped file, rather than a dense
    index or no index (scanning). The index stays tiny; a lookup is a binary search plus at
    most 4 KiB of sequential scan.
  - *Crash detection:* a `.clean_shutdown` marker, as in Kafka. Recovery is a full CRC scan of
    the active segment only.
  - *Leader epochs and producer sequence numbers stored in every record,* so replication and
    idempotence state can be rebuilt from the log itself.
- **Built:** `RecordCodec`, `OffsetIndex`, `LogSegment`, `Log`, `LogManager`,
  `LeaderEpochCache`, `ProducerStateManager`.
- **Verified:** 17 storage tests: segment roll and contiguity; index lookup for every 37th
  offset; floor lookup; a single-bit flip at every byte position is detected; on-disk
  corruption is detected on read; a 30-byte torn tail is truncated on recovery; a corrupt tail
  record is dropped; a garbage index is rebuilt; time, size and HW-bounded retention; the
  active segment is never deleted; truncation across segments; the epoch cache; producer
  dedup surviving a restart.
- **What went wrong:** on the first run, 16 of 17 tests passed.
  `corruptRecordOnDiskIsDetectedOnRead` failed because `Log.open()` itself threw
  `CorruptRecordException`: rebuilding producer state scans every segment, and one damaged
  byte in an *inactive* segment stopped the broker from starting at all. Fix: catch the
  exception per segment, log a warning and keep starting. The damaged range still fails its
  CRC when a client reads it, which is what the test asserts.
- **Re-run:** `./gradlew test --tests 'dev.minilog.storage.*'`

### Stage 3 — Protocol and network layer

- **Goal:** a documented binary TCP protocol and a server that can block per request without
  using up OS threads.
- **Decisions:** `DataOutputStream`/`DataInputStream` with length-prefixed frames and
  correlation ids. Rejected JSON (adds a dependency, harder to frame) and Protobuf (adds a
  dependency and code generation). For the server: **virtual threads + blocking sockets**,
  rejected Netty (a dependency, plus a callback-style handler) and raw NIO selectors (harder to
  read). Blocking sections use `ReentrantLock`, because `synchronized` pins virtual threads in
  Java 21.
- **Built:** `ApiKey`, `Messages` (13 APIs, 26 message types), `Codec`, `Frame`, `Wire`,
  `ClusterMetadata`, `SocketServer`, `Connection`, [PROTOCOL.md](PROTOCOL.md).
- **Verified:** `ProtocolCodecTest`, 28 tests. Every request and response round-trips to
  identical bytes. A wrong correlation id, trailing bytes and an oversized frame are rejected.

### Stage 4 — Replication, controller, clients

- **Goal:** 3 brokers, leader/follower pull replication, ISR and high watermark, failover
  without losing acks=all writes, plus producer and consumer libraries.
- **Decisions and alternatives:**
  - *Controller:* one static controller process with heartbeat-based liveness. Alternatives
    were designating one broker as controller (then killing that broker also kills the
    controller, which makes failover tests meaningless) and Raft (correct, but a project in
    itself). The single point of failure is documented.
  - *ISR changes go through the controller* (AlterIsr with leader epoch and ISR version), and
    the HW uses current ∪ pending ISR while a change is in flight. Letting the leader change
    the ISR locally was rejected because it can lose acknowledged data (see Design decisions).
  - *Follower truncation by leader epoch* rather than truncate-to-HW (KIP-101's data-loss
    case).
  - *A restarted broker is treated as failed* (new incarnation id), so it must catch up
    before it rejoins the ISR.
  - *Group coordinator:* hosted on the controller, eager rebalance with a server-side join
    barrier, range assignment computed on the server. Rejected client-side assignment
    (SyncGroup) and cooperative rebalancing as extra complexity.
  - *Producer idempotence:* client-random producer id, per-partition sequence numbers, one
    in-flight batch per partition.
- **Built:** `Controller`, `GroupCoordinator`, `RangeAssignor`, `Partition`,
  `ReplicaManager`, `ReplicaFetcher`, `Broker`, `Producer`, `Consumer`, `Admin`, plus an
  in-process `TestCluster` for tests.
- **Verified (all passed on the first run):**
  - `ProduceFetchIT`: 3,000 keyed records over 3 partitions, contiguous offsets, per-key order.
  - `ConsumerGroupIT`: join → join → leave rebalances, 1,200 records each consumed exactly
    once, a crashed member evicted after its session timeout.
  - `IdempotentProducerIT`: the leader drops 10 produce responses after applying them. With
    idempotence, 200 records are each written once. In the control run without idempotence,
    duplicates appear.
  - `ReplicationIT`: with followers paused, leader LEO = 150 while HW stays at 100, and
    consumers see nothing above 100. After the ISR shrinks to the leader, HW = 150 and acks=all
    is refused with `NOT_ENOUGH_REPLICAS`. Followers rejoin after resuming. A single lagging
    follower is removed only after `replica.lag.time.max.ms` and rejoins later.
  - `LeaderFailoverIT`: 20 leader kills under continuous acks=all load. The first run (seen in
    the test output during development) had 13,182 acked, 0 lost, 0 duplicated. The last
    recorded run, kept in `results/test-runs/leader-failover-it-last-run.txt`, had 14,500
    acked, 0 lost, 0 duplicated.

### Stage 5 — Flakiness check

- **Goal:** prove the timing-sensitive integration tests are stable.
- **Built:** `scripts/repeat-tests.sh N` runs `./gradlew test --rerun-tasks` N times and logs
  pass/fail and timings.
- **Result:** 10/10 runs passed (55 tests, 32–41 s each). After the stage 6 client changes,
  5/5 more passed (26–27 s each). Source: `results/test-runs/summary.txt`.
- **Honest note:** no flaky test showed up in these 15 runs, so there is no flaky-test root
  cause to report. Timing assertions were written with margins up front. For example,
  `ReplicationIT` asserts that its "HW stays at 100" checks finished before the 1 s ISR lag
  timeout could fire, instead of assuming it.

### Stage 6 — First benchmarks found two latency bugs

- **Goal:** end-to-end latency at a fixed 5,000 msgs/s.
- **What went wrong (1):** the first e2e run gave p50 ≈ 42 ms and p99 ≈ 105 ms, even with
  acks=1. That was terminal output, later reproduced and saved in
  `results/journey/e2e-before-fixes.txt`, variant A: p50 49.5 ms (acks=1) / 44.4 ms
  (acks=all). acks=1 being as slow as acks=all ruled out replication. Dropping the rate to
  500/s made the p50 *worse* (≈ 96 ms in the terminal run), which pointed at a timer rather
  than load. **Root cause:** `Producer.send()` only woke the sender thread when a batch was
  *full*. With `linger.ms=0` a new non-full batch waited for the sender's 100 ms idle poll.
  **Fix:** also signal when a new batch is created.
- **What went wrong (2):** with fix 1 applied, rare tails remained: max 504 ms with acks=all
  (variant B in the same file). **Root cause:** the consumer sent one fetch per leader in
  parallel and waited for *all* of them, so an idle partition's 500 ms long poll held back
  records that had already arrived from other brokers. **Fix:** keep one fetch in flight per
  leader across polls and return on the first completion. Results of fetches built for an
  outdated assignment are discarded.
- **Measured after both fixes** (`results/e2e.txt`, 3 runs): acks=all p50 0.249 ms (range
  0.243–0.566), p99 0.863 ms (range 0.840–61.906). One run had a 61.9 ms p99, see
  BENCHMARK §3.
- **Re-run:** `./scripts/run-benchmarks.sh e2e`

### Stage 7 — Throughput, fsync, failover and chaos runs

- **Throughput** (`produce.txt`, `produce-long.txt`, `consume*.txt`): moving from 16 KiB to
  128 KiB batches roughly doubled 1 KiB acks=all throughput (≈ 98 k → 208 k msgs/s, short runs).
  Sustained acks=all: 160 k msgs/s at 1 KiB (156 MiB/s) and 462 k msgs/s at 100 B (medians of
  3). **Surprise:** acks=0 was barely faster than acks=1 with 100 B records. The single
  synchronous sender thread is the bottleneck, not the brokers. This is left as a documented
  trade-off.
- **fsync:** fsync on every append cut throughput from 94.9 k to 1.9 k msgs/s (≈ 49×). A
  1-second background fsync cost nothing measurable.
- **Failover — what went wrong (3):** the first failover run reported `min_ms=0` once (raw
  output kept in `results/failover-buggy-clock.txt`). **Root cause:** the timer started
  *before* `destroyForcibly()`, and the dying leader acked one more record in the few
  milliseconds before SIGKILL landed. **Fix:** start the clock after `Process.waitFor()`
  confirms exit, then re-run. Corrected results: median 1,574 ms / p95 1,647 ms with a 1.5 s
  session timeout, and 595 / 663 ms with 500 ms. 0 acked records were lost in either run.
- **Chaos:** 3 × 120 s runs with seeds 20261112, 7 and 42. 52 random `kill -9`s of brokers, a
  consumer replaced mid-run, 9,634,000 acked records. 0 lost, 0 duplicated, 0 per-partition
  order violations.
- **What went wrong (4):** I edited `scripts/run-benchmarks.sh` while it was running. Bash
  reads scripts incrementally, picked up the half-changed file and printed
  `line 68: 1024: command not found`. The partial output was discarded, and the long and
  failover sections were re-run cleanly. Lesson: never edit a running shell script.
- **What went wrong (5):** the first README walkthrough used `CLI="java -cp ..."; $CLI describe`.
  That fails in zsh, the macOS default shell, because zsh does not word-split unquoted
  variables. The smoke test caught it (`no such file or directory`). Fix: a
  `scripts/cli.sh` wrapper. The full walkthrough was then verified: create, produce, kill
  broker 1, leader moves (epoch 0 → 1), consume, restart, the ISR returns to 3.
- **Re-run:** `./scripts/run-benchmarks.sh produce|consume|long|fsync|failover|chaos`

### Key metrics

| metric | value | how measured | source file |
|---|---|---|---|
| Acked records lost on leader kill (JUnit) | 0 of 14,500, over 20 kills | `LeaderFailoverIT`, in-process cluster, acks=all, min ISR 2 | `results/test-runs/leader-failover-it-last-run.txt` |
| Acked records lost / duplicated under chaos | 0 / 0 of 9,634,000, over 52 `kill -9`s | `Bench chaos`, 3 × 120 s, 2 idempotent producers, 3-member group | `results/chaos.txt` (lines 24, 45, 69) |
| Leader failover time (session timeout 1.5 s) | median 1,574 ms, p95 1,647 ms (n=25) | kill -9 → first ack of a record sent after the kill | `results/failover.txt` line 31 |
| Leader failover time (session timeout 0.5 s) | median 595 ms, p95 663 ms (n=25) | same | `results/failover.txt` line 59 |
| Sustained producer throughput, 1 KiB, acks=all | 160,075 msgs/s, 156.3 MiB/s (range 144,812–163,382) | 3 M records, 128 KiB batches, 3 runs | `results/produce-long.txt` / BENCHMARK.md line 50 |
| Sustained producer throughput, 100 B, acks=all | 461,997 msgs/s (range 416,414–523,476) | 10 M records, 128 KiB batches, 3 runs | `results/produce-long.txt` / BENCHMARK.md line 49 |
| Consumer throughput, 100 B | 890,825 msgs/s (range 877,991–1,032,527) | 5 M records from offset 0, 3 runs | `results/consume-long.txt` / BENCHMARK.md line 90 |
| End-to-end latency, acks=all @ 5 k msgs/s | p50 0.249 ms, p99 0.863 ms (median of 3; worst run p99 61.9 ms) | in-JVM nanoTime, 100 k samples per run | `results/e2e.txt` / BENCHMARK.md line 113 |
| e2e p50 before vs after the producer wake-up fix | 49.5 ms → 0.27 ms (acks=1) | same benchmark, pre-fix build reproduced | `results/journey/e2e-before-fixes.txt`, `results/e2e.txt` |
| fsync-every-append slowdown | 94,858 → 1,934 msgs/s (≈ 49×) | 100 k × 1 KiB, acks=1, 3 runs each | `results/fsync.txt` / BENCHMARK.md lines 128, 131 |
| Test-suite stability | 15/15 full runs passed (55 tests each) | `scripts/repeat-tests.sh` | `results/test-runs/summary.txt` |

### Next steps

1. Replace the static controller with a 3-node Raft quorum (KRaft-style) and move consumer
   offsets into a replicated, compacted internal topic.
2. Producer: several in-flight requests per broker (a sequence window of up to 5, as in
   Kafka) to lift the single-sender-thread ceiling. Then re-measure acks 0 vs 1.
3. Zero-copy fetch with `FileChannel.transferTo`, and pooled direct buffers.
4. Producer-state snapshots, so startup does not scan the whole log.
5. Preferred-leader rebalancing, and self-fencing of a leader that loses contact with the
   controller.
6. Run the chaos test across real machines, with network-partition injection (Jepsen-style),
   and with consumer crashes, to measure redelivery under at-least-once.

## Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │ Controller (1 process, static)                │
                         │  • broker liveness (heartbeats, 1.5 s timeout) │
                         │  • leader election from ISR, leader epochs    │
                         │  • ISR changes (AlterIsr), metadata versions  │
                         │  • group coordinator + __consumer_offsets log │
                         └───────▲───────────────▲───────────────▲──────┘
          heartbeat (100 ms) ←→  │ metadata       │               │
                         ┌───────┴──────┐ ┌──────┴───────┐ ┌─────┴────────┐
 Producer ──Produce────► │  Broker 1    │ │  Broker 2    │ │  Broker 3    │
 (batch, linger,         │ orders-0 (L) │ │ orders-0 (F) │ │ orders-0 (F) │
  idempotent seq)        │ orders-1 (F) │◄┤ orders-1 (L) ├►│ orders-1 (F) │
                         │ orders-2 (F) │ │ orders-2 (F) │ │ orders-2 (L) │
 Consumer ◄──Fetch────── └──────▲───────┘ └──────────────┘ └──────────────┘
 (group member, poll,           │ followers pull (Fetch with replicaId, long-poll)
  commit via controller)        └─ fetchOffset = follower LEO → leader updates ISR / HW

 One partition on disk (data/broker-1/orders-0/):
   00000000000000000000.log   00000000000000000000.index   (rolled, read-only)
   00000000000000052113.log   00000000000000052113.index   (active segment)
   leader-epoch-checkpoint
```

Source layout (`src/main/java/dev/minilog/`):

| package | contents |
|---|---|
| `storage` | `RecordCodec` (format + CRC), `OffsetIndex` (mmap sparse index), `LogSegment`, `Log`, `LogManager`, `LeaderEpochCache`, `ProducerStateManager` |
| `protocol` | `Messages` (all request/response records), `Codec`, `Frame`, `ClusterMetadata` |
| `network` | `SocketServer` (virtual thread per connection), `Connection` (blocking client) |
| `controller` | `Controller` (liveness, election, ISR), `ControllerServer`, `ControllerMain` |
| `group` | `GroupCoordinator` (rebalance barrier, offsets), `RangeAssignor` |
| `broker` | `Partition` (LEO / ISR / HW), `ReplicaManager`, `ReplicaFetcher`, `Broker`, `BrokerMain` |
| `client` | `Producer`, `Consumer`, `Admin`, `ClusterClient` |
| `bench` | `Bench` (throughput, latency, fsync, failover, chaos), `LocalCluster` (spawns broker JVMs) |
| `tools` | `Cli` (create-topic, describe, produce from stdin, consume) |

## How it maps to Kafka, and where it deliberately differs

| Kafka concept | minilog | difference |
|---|---|---|
| Topic / partition / offset | same | |
| Log segments, `.log` + `.index`, sparse index | same idea (`LogSegment`, `OffsetIndex`) | no time index, no compaction |
| Record batch format v2 | simpler per-record format, CRC-32C per record | records are not compressed and not grouped into batch headers |
| `acks`, `min.insync.replicas` | same semantics | |
| ISR, high watermark, `replica.lag.time.max.ms` | same semantics (`Partition`) | |
| Leader epoch truncation (KIP-101/279) | `LeaderEpochCache` + `OFFSET_FOR_LEADER_EPOCH` | simplified |
| AlterPartition (KIP-497) and "maximal ISR" HW | `ALTER_ISR` + `pendingIsr` | |
| ZooKeeper / KRaft controller quorum | **one static controller process** | single point of failure (see below) |
| Unclean leader election | never done | partition stays offline until an ISR member returns |
| Idempotent producer | per-partition sequence numbers, dedup state rebuilt from the log | producerId is a random 63-bit number generated by the client, not broker-assigned. No producer epochs. Only the last batch per producer is cached (enough with one in-flight batch per partition). |
| Transactions / exactly-once | **not implemented** | consumers are at-least-once |
| Group coordinator on `__consumer_offsets` partition leaders | hosted by the controller; offsets in an internal minilog `Log` on the controller | not replicated, not compacted |
| Group protocol (JoinGroup/SyncGroup, client-side assignors) | JoinGroup only; the coordinator runs range assignment | eager "stop-the-world" rebalance, no cooperative/incremental rebalancing |
| Zero-copy `sendfile` | no; the record bytes are copied but never re-encoded | |
| Netty / NIO | blocking sockets on virtual threads | see design decisions |

### The controller (why no ZooKeeper/KRaft, and what that costs)

A separate controller process is the single source of truth for leadership and ISR membership:

1. **Liveness.** Each broker sends `BROKER_HEARTBEAT` every 100 ms. A broker silent for
   `broker.session.timeout.ms` (1500 ms by default) is declared dead. A heartbeat with a new
   `incarnationId` means the broker restarted. The controller treats that as a failure first,
   so the restarted replica has to catch up before it re-enters the ISR.
2. **Election.** When a leader dies, the first live ISR member becomes leader and the leader
   epoch is bumped. If no ISR member is alive, the partition goes offline rather than electing
   a replica that may be missing committed data.
3. **ISR changes** come from partition leaders (`ALTER_ISR`). They are accepted only if the
   leader epoch and ISR version match, and only if every proposed member is alive.
4. **Distribution.** Every change bumps a metadata version. Brokers receive the new snapshot in
   their next heartbeat response (pull). Clients ask any node for `METADATA`.
5. **Persistence.** Topic/partition state is written (atomically, via rename) using the same
   binary encoding as the wire format, so a restarted controller resumes with the same leaders
   and epochs.

**Honest limitation:** the controller is a single point of failure. While it is down, brokers
keep serving their current partitions, but no failover, ISR change, topic creation or group
operation can happen. Kafka avoids this with a replicated Raft quorum (KRaft). Replacing the
controller with a 3-node Raft group is the first item under Next steps. The failover times
below are dominated by the session timeout, which is a tunable trade-off: a shorter timeout
means faster failover but more false positives during GC pauses.

## Design decisions and trade-offs

- **Virtual threads + blocking IO instead of NIO/Netty.** Each connection is a virtual thread
  running `read frame → handle → write`. Handlers can block cheaply: a long-poll fetch waits on
  a `Condition` for new data, and an `acks=all` produce waits for the high watermark. Neither
  needs a callback chain. All blocking sections use `ReentrantLock`, not `synchronized`,
  because in Java 21 blocking inside `synchronized` pins the carrier thread. What this costs:
  no buffer pooling and no `sendfile`. Netty would also add a dependency, and this project's
  goal is code that can be read top to bottom.
- **No JSON library.** Everything, including controller state on disk, uses the one binary codec
  in `protocol/`. That codec is round-trip tested for all 26 message types.
- **Same bytes on disk and on the wire.** A fetch returns raw segment bytes. A follower validates
  every CRC and appends the bytes as they are.
- **Durability comes from replication, not fsync, by default** (Kafka's default too).
  `log.flush.interval.messages` / `log.flush.interval.ms` turn on fsync. The cost is measured
  below. Caveat: with all three replicas on one laptop, replication cannot protect against that
  laptop losing power.
- **Pull-based replication.** A follower's fetch offset doubles as its acknowledgement. A
  follower that falls behind catches up with large fetches. One fetcher thread per leader
  broker multiplexes all of its partitions into a single long-poll request.
- **ISR changes go through the controller, and the HW uses the "maximal" ISR while a change is
  in flight.** Without this, a leader could shrink the ISR locally, advance the HW and ack
  writes, then die. The controller might then elect the removed follower and lose
  acknowledged data.
- **Leader-epoch truncation instead of "truncate to HW".** Truncating to a possibly stale local
  HW can discard committed records (KIP-101). minilog instead asks the leader where the
  follower's latest epoch ends.
- **Retention never deletes above the high watermark** and never deletes the active segment.
- **Eager rebalance with a server-side barrier.** On a rebalance, every member commits and then
  rejoins. `JOIN_GROUP` blocks until all members have rejoined. A new owner reads the committed
  offset after the barrier. Clean rebalances therefore neither lose nor duplicate records, and
  stale generations are fenced at commit time. The cost is that the whole group pauses during a
  rebalance.
- **Simplicity over peak throughput in the clients.** The producer's sender thread works in
  synchronous rounds: drain the ready batches, send one request per broker in parallel, wait
  for all of them. That gives exactly one in-flight batch per partition, which makes idempotent
  retries trivially ordered, but it caps throughput. The benchmarks show this: `acks=0` is not
  much faster than `acks=1` with 100-byte records, because the client is the bottleneck.

## Running it

Requirements: JDK 21. The Gradle wrapper is checked in, so nothing else is needed.

```bash
./gradlew test                    # 55 tests, ~30 s on the M2
./gradlew jar                     # build/libs/minilog.jar

./scripts/cluster.sh start        # controller :9090 + brokers :9091-9093 (logs in .cluster/)
./scripts/cli.sh create-topic orders 3 3
printf 'alice:order-1\nbob:order-2\nalice:order-3\n' | ./scripts/cli.sh produce orders
./scripts/cli.sh consume orders my-group   # Ctrl-C to stop
./scripts/cli.sh describe                  # leaders, epochs, ISR

./scripts/cluster.sh kill 1       # kill -9 broker 1; `cli.sh describe` shows the new leader/ISR
./scripts/cluster.sh restart 1
./scripts/cluster.sh stop
```

Benchmarks (each run starts its own fresh 3-broker cluster on ports 19090-19093):

```bash
./scripts/run-benchmarks.sh all        # or: produce | consume | e2e | fsync | failover | chaos
python3 scripts/summarize.py           # medians and ranges from results/*.txt
./scripts/repeat-tests.sh 10           # full test suite 10 times (flakiness check)
```

## Results summary

Full tables, methodology and caveats: [BENCHMARK.md](BENCHMARK.md). Each row is a median of
3 runs with the min–max range in parentheses, unless the row says otherwise.

| what | result |
|---|---|
| Producer, 1 KiB, acks=all, 128 KiB batches (sustained, 3 M records) | **160,075 msgs/s, 156 MiB/s** (144,812 – 163,382) |
| Producer, 100 B, acks=all, 128 KiB batches (sustained, 10 M records) | **461,997 msgs/s** (416,414 – 523,476) |
| Producer, 1 KiB, 128 KiB batches, acks 0 / 1 / all (short runs) | 287,585 / 237,283 / 207,749 msgs/s |
| Batch size 16 KiB → 128 KiB, 1 KiB records, acks=all (short runs) | 97,649 → 207,749 msgs/s |
| Consumer, 100 B / 1 KiB (sustained) | 890,825 msgs/s / 342,275 msgs/s (334 MiB/s) |
| End-to-end latency @ 5,000 msgs/s, acks=all | p50 **0.249 ms**, p99 **0.863 ms** (one of 3 runs: p99 61.9 ms) |
| fsync on every append vs none (1 KiB, acks=1) | 1,934 vs 94,858 msgs/s (≈ 49× slower) |
| Leader failover (kill -9 → first new ack), session timeout 1.5 s / 0.5 s, n=25 each | median **1,574 / 595 ms**, p95 1,647 / 663 ms |
| Chaos: 3 × 120 s, 52 random broker kills, 9,634,000 acked records | **0 lost, 0 duplicated, 0 order violations** |
| Test suite | 55 tests; 15 / 15 consecutive full runs passed |

All processes share one 8-core laptop and talk over localhost; see the caveats in BENCHMARK.md.

## Tests

| area | test |
|---|---|
| segment roll, sparse index lookup | `LogSegmentTest` (5 tests) |
| CRC corruption detection, torn-tail recovery, index rebuild | `CorruptionAndRecoveryTest` (5) |
| retention (time, size, HW bound, active segment) | `RetentionTest` (4) |
| truncation, leader-epoch cache, producer-state rebuild | `LogTruncationTest` (3) |
| protocol encode/decode (all 26 messages, framing errors) | `ProtocolCodecTest` (28) |
| produce/fetch end to end, acks 0/1/all, per-key ordering | `ProduceFetchIT` (2) |
| consumer group rebalance on join/leave, session-timeout eviction | `ConsumerGroupIT` (3) |
| idempotent producer dedup under lost acks (plus a non-idempotent control group) | `IdempotentProducerIT` (2) |
| HW respects ISR, ISR shrink → HW, `NOT_ENOUGH_REPLICAS`; follower drops out of and rejoins the ISR | `ReplicationIT` (2) |
| leader kill with acks=all, 20 iterations, 0 acknowledged records lost | `LeaderFailoverIT` (1) |

Integration tests run an in-process cluster (`TestCluster`): one controller and three brokers
in the test JVM with ephemeral ports. "Kill" closes sockets and file handles without flushing
or writing the clean-shutdown marker. The benchmark's failover and chaos modes use real
`kill -9` on separate processes.

## Known limitations

- Single static controller (SPOF); consumer offsets live only on the controller.
- Consumers are at-least-once. No transactions. Exactly-once *consumption* was observed in the
  chaos runs only because consumers there never crashed (one was replaced via a clean close).
- The producer id is random on the client side, not broker-allocated. After a delivery timeout
  the producer switches to a new id, so batches queued at that moment lose their dedup
  protection.
- Producer-state recovery scans the whole log at startup (Kafka uses snapshots). Segment
  recovery reads a whole segment into heap memory.
- No preferred-leader rebalancing. After failovers, leadership stays wherever it moved.
- No self-fencing of a leader that is partitioned from the controller. Such a leader can still
  accept `acks=1` writes until it learns of the new epoch. `acks=all` writes stall instead,
  because followers stop fetching from it.
- Time-based retention uses the largest producer-supplied timestamp in a segment.
- Index resize relies on GC to unmap old mappings (Java has no public unmap). Fine on
  macOS/Linux; untested on Windows.
- On macOS, `fsync` (`FileChannel.force`) is not a full write barrier to stable storage
  (`F_FULLFSYNC` is). I did not verify which call the JDK uses here, so the fsync numbers show
  what `force()` cost on this machine, not a guarantee of power-loss durability.
- Everything was tested on one machine. Network partitions, packet loss and multi-host latency
  were not tested.
