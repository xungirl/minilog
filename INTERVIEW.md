# minilog 面试讲解手册（中文）

> 这份文档写给要在面试里讲清楚这个项目的你。第一部分按「教学顺序」逐个文件讲，每一块分三层：
> **(a) 骨架：它做什么**，**(b) 为什么这样设计**，**(c) 值得掌握的 Java 语法和 API**。
> 第二部分是大约 20 道高频面试题，答案都落在本项目的代码上。
> 所有数字都出自 `BENCHMARK.md` 和 `results/`，是这台 M2 MacBook 上实际跑出来的，面试时请照原样引用，不要四舍五入成更好看的数字。

推荐阅读顺序（先存储，再网络，再分布式，最后客户端）：

```
1 storage/RecordCodec      记录格式 + CRC
2 storage/OffsetIndex      稀疏索引 + mmap
3 storage/LogSegment       一个分段 = .log + .index，崩溃恢复
4 storage/Log              分段列表、滚动、截断、保留、fsync 策略
5 storage/LeaderEpochCache + ProducerStateManager   复制和幂等需要的两个小状态
6 storage/LogManager       目录管理、clean shutdown 标记、后台任务
7 protocol/*               自定义二进制协议
8 network/SocketServer + Connection   虚拟线程服务端 / 阻塞客户端
9 controller/Controller    心跳、选主、ISR 变更、元数据版本
10 broker/Partition        LEO / ISR / HW —— 整个项目最核心的类
11 broker/ReplicaFetcher + ReplicaManager   拉取式复制、epoch 截断、长轮询
12 group/GroupCoordinator + RangeAssignor   消费组再均衡
13 client/Producer         批量、linger、重试、幂等序列号
14 client/Consumer         poll 循环、提交位点、再均衡
15 bench/*                 压测和混沌测试怎么测的
```

---

## 第一部分：逐文件讲解

### 1. `storage/RecordCodec.java` —— 一条记录在磁盘上长什么样

**(a) 骨架**
每条记录的格式是 `length | crc | offset | timestamp | leaderEpoch | producerId | sequence | keyLen | key | valueLen | value`，固定开销 48 字节。`write()` 先写一个 crc 占位，算完 CRC 再回填。`read()` 依次检查长度合法、CRC 匹配、字段长度不越界，任何一项不对都抛 `CorruptRecordException`。

**(b) 为什么**
- **长度前缀**：日志是一串连续的字节，没有长度就不知道一条记录在哪里结束，也就没法跳过记录、没法做恢复扫描。
- **CRC 覆盖 offset 之后的所有字节**：磁盘位翻转、写了一半的记录（torn write）、网络传输出错都能发现。测试 `crcDetectsASingleFlippedBit` 把每一个字节各翻一位，每次都能检测出来。
- **磁盘格式 = 网络格式**：Fetch 时 broker 把文件字节原样发出去，不重新编码，follower 收到后原样写盘。这就是 Kafka「零拷贝」思路的简化版（我们没用 `sendfile`，但省掉了序列化这一步）。
- **记录里带 leaderEpoch、producerId、sequence**：这样复制状态和幂等状态都能从日志本身重建，不用另外存一份。

**(c) Java 知识点**
- `ByteBuffer`：`putInt / getLong` 是相对操作，会移动 position；`putInt(index, v)` 是绝对操作，不移动 position（回填 CRC 用的就是它）。`flip()` 把写模式切成读模式。`duplicate()` 共享底层数据，但 position/limit 是独立的，所以算 CRC 时可以随意设置范围，不影响原 buffer。
- `java.util.zip.CRC32C`：JDK 9+ 自带，在 ARM/x86 上有硬件指令加速，Kafka 用的也是 CRC32C。
- `record`（Java 16+）：`Record`、`SimpleRecord` 都是不可变数据类，自动生成构造器、访问器、`equals/hashCode/toString`。

### 2. `storage/OffsetIndex.java` —— 稀疏索引 + 内存映射

**(a) 骨架**
`.index` 文件里每 8 字节是一个条目：`(相对 offset:int32, 文件位置:int32)`。每写满 `indexIntervalBytes`（默认 4KB）的日志才加一个条目，所以索引是**稀疏的**。`lookup(target)` 用二分查找找到「offset ≤ target 的最大条目」，然后由 `LogSegment` 从这个位置往后顺序扫描，最多扫一个 interval 的字节就能定位到 target。

**(b) 为什么**
- **稀疏**：如果每条记录都建索引，1 亿条记录就要 800MB 索引。稀疏索引小到可以整个放进内存（page cache），代价是查找时多扫最多 4KB，顺序读 4KB 非常便宜。
- **相对 offset 用 int32**：segment 的 baseOffset 是 long，条目里只存差值，每个条目省 4 字节。
- **mmap**：索引文件映射到内存后，二分查找就是读内存，不需要 `read()` 系统调用。活跃 segment 的索引文件会预分配到 `maxIndexBytes`，segment 滚动或关闭时再 `trimToValidSize()` 截到实际大小。
- **崩溃后不信任索引**：`isValid()` 检查 offset 严格递增、position 在文件范围内，检查不通过就从 .log 重建。索引是「可以重建的派生数据」，丢了也不会丢数据。

**(c) Java 知识点**
- `FileChannel.map(READ_WRITE, 0, size)` 返回 `MappedByteBuffer`。文件关闭后映射依然有效。`force()` 把脏页刷回磁盘。
- Java 没有公开的 unmap API（要等 GC 回收），所以 Kafka 用了 `Unsafe` 来强制 unmap。我们的 resize 采用「重新 map 一个新的」，旧映射交给 GC 回收（README 里写了这个限制）。
- 二分查找写法：`int mid = (lo + hi) >>> 1;` 用无符号右移避免 `lo + hi` 溢出，这是面试常考点。

### 3. `storage/LogSegment.java` —— 一个分段

**(a) 骨架**
一个 segment 就是 `000…000123.log` 加 `000…000123.index`，文件名是 baseOffset 补零到 20 位（这样按字典序排就是按 offset 排）。主要方法：
- `append()`：在文件末尾做一次**定位写**（positional write），必要时加一个索引条目。
- `translateOffset()`：索引查找 + 顺序扫描记录头（只读 16 字节：length/crc/offset）。
- `read(startOffset, maxBytes, maxOffsetExclusive)`：返回完整记录的原始字节；`maxOffsetExclusive` 用来挡住高水位以上的数据。即使第一条记录比 maxBytes 大也一定返回它，否则消费者会永远卡在这条大记录上。
- `recover()`：崩溃恢复。从头逐条校验（长度、CRC、offset 单调），遇到第一条坏记录就 `truncate` 掉后面所有字节，同时重建索引。
- `loadNextOffset()`：正常关闭后的快速路径，只从最后一个索引条目开始往后扫。

**(b) 为什么**
- **为什么要分段**：保留策略只需要删除整个文件（O(1)，不用在大文件中间挖洞）；恢复时只需要扫描最后一个 segment；索引条目里的相对 offset 能放进 int32。
- **torn write**：进程在 `write()` 写到一半时被 kill，或者机器断电，文件尾部会留下半条记录。因为有长度前缀和 CRC，`recover()` 能准确找到最后一条完整记录的结尾。对应测试 `recoveryTruncatesTornTailWrite`：人为在文件末尾追加 30 字节的半条记录，重启后这 30 字节被截掉，并且可以在正确的 offset 上继续追加。

**(c) Java 知识点**
- `FileChannel.write(buf, position)` / `read(buf, position)`：定位读写不修改 channel 自身的位置，多线程同时读也是安全的，比 `RandomAccessFile.seek + read` 更适合并发。
- `channel.force(false)`：只刷数据不刷元数据（类似 `fdatasync`），比 `force(true)` 快。
- `channel.truncate(size)`：截断文件。
- `String.format("%020d", offset)`：补零到 20 位。

### 4. `storage/Log.java` —— 一个分区的完整日志

**(a) 骨架**
`ConcurrentSkipListMap<Long baseOffset, LogSegment>` 保存所有 segment，最后一个是活跃段。
- `appendAsLeader()`：幂等检查 → 分配 offset → 打上 leaderEpoch → 编码成一个 ByteBuffer → 一次写入。
- `appendAsFollower()`：收到 leader 发来的原始字节，校验 CRC 和 offset 连续性后原样写入。
- `read()`：`floorEntry(startOffset)` 找到所在的 segment。
- `truncateTo()`：删掉 offset 之后的 segment，再截断最后一段（follower 和 leader 对齐日志时用）。
- `deleteOldSegments()`：基于时间和大小的保留策略，**只删非活跃段、只删高水位以下的段**。
- 刷盘策略：`flushIntervalMessages`（每 N 条 fsync 一次）和 `flushIntervalMs`（后台定时 fsync）。

**(b) 为什么**
- `floorEntry` 是跳表 / 红黑树的 O(log n) 操作，「找到 ≤ x 的最大 key」正好对应「这个 offset 在哪个 segment 里」。
- **读写锁**：追加、截断、滚动、删除拿写锁；读拿读锁。多个消费者可以并发读，又不会读到正在被删除的文件。
- **默认不 fsync**：和 Kafka 默认一样，持久性靠「多副本 + OS page cache」，而不是每条消息都刷盘。`BENCHMARK.md` 的 fsync 实验量化了这个取舍。
- **保留不越过高水位**：未提交的数据不能被删。

**(c) Java 知识点**
- `ConcurrentSkipListMap` 是线程安全的有序 Map，提供 `floorEntry / higherEntry / lastEntry / pollFirstEntry`。
- `ReentrantReadWriteLock`：`lock.readLock().lock()` 必须配合 `try { … } finally { unlock(); }`。
- `instanceof` 模式匹配（Java 16+）：`if (check instanceof Duplicate dup) { dup.baseOffset() }`。
- `sealed interface Check permits Append, Duplicate, OutOfOrder`（Java 17）：编译器知道只有这三种实现。

### 5. `LeaderEpochCache` 和 `ProducerStateManager`

**LeaderEpochCache**
- (a) 记录每个 leader epoch 从哪个 offset 开始，例如 `{0→0, 2→950, 5→1200}`，持久化到 `leader-epoch-checkpoint`（先写 tmp 文件，再原子 rename）。
- (b) 解决「换主之后副本日志分叉」的问题：旧 leader 可能写了一些还没提交的记录就挂了。它恢复后作为 follower，先问新 leader「我最新的 epoch E 在你那里到哪里结束？」，然后截断到那个位置。这是 Kafka KIP-101 / KIP-279 的思路。早期的「截断到自己的 HW」方案在两次连续故障时会丢数据，这个方案就是为了解决它。
- (c) `TreeMap.floorEntry / higherEntry`；用 `Files.move(tmp, file, ATOMIC_MOVE)` 做原子替换，这样崩溃时文件要么是旧的、要么是新的，不会只写了一半。

**ProducerStateManager**
- (a) 每个分区维护 `producerId → (lastSequence, lastOffset)`。
- (b) 收到一个批次：`baseSeq == last+1` 就追加；整批 ≤ last 说明是重试，返回原来的 offset，不重复写；否则返回 `OUT_OF_ORDER_SEQUENCE`。这个状态从日志重建（记录里带有 pid/seq），所以 follower 上的状态和 leader 完全一致，换主后去重依然有效。
- (c) 用 sealed interface + record 表示三种检查结果，比返回 int 状态码清晰得多。

### 6. `storage/LogManager.java`

- (a) 管理数据目录里的所有分区日志，运行后台刷盘和保留任务。
- (b) **clean shutdown 标记**：正常关闭的最后一步写 `.clean_shutdown` 文件，启动时删除它。启动时如果没有这个文件，说明上次是崩溃退出，需要对每个日志的活跃段做完整恢复。Kafka 用的是同样的机制。
- (c) `Executors.newSingleThreadScheduledExecutor` + `scheduleAtFixedRate`；`computeIfAbsent` 懒加载创建日志。

### 7. `protocol/*` —— 自定义二进制协议

- (a) 帧格式：`int32 size | payload`；请求 payload 是 `int16 apiKey | int32 correlationId | body`。`Messages.java` 里每个 record 都有 `write()` 和一个对称的静态 `read()`。完整说明见 `PROTOCOL.md`。
- (b)
  - **长度前缀分帧**：TCP 是字节流，没有消息边界，必须自己分帧。
  - **correlationId**：客户端用它检查响应和请求是否对得上，能尽早发现协议 bug。
  - **帧大小上限 64MB**：防止一个损坏或恶意的 size 字段让服务端去分配几个 GB 的内存。
  - **不用 JSON / Protobuf**：零依赖、字节可控；`ProtocolCodecTest` 对 26 种消息做了「编码 → 解码 → 再编码，字节必须完全相同」的往返测试。
- (c) `DataOutputStream / DataInputStream`（大端序，`readFully` 保证读满）；switch 表达式 `case PRODUCE -> …`（Java 14+），编译器会检查枚举是否覆盖完全；函数式接口 `Wire.Writer<T>` 加 lambda 实现通用的 list 编码。

### 8. `network/SocketServer.java` 和 `Connection.java` —— 虚拟线程

**(a) 骨架**
服务端：一个 acceptor 线程，每接受一个连接就 `Thread.ofVirtual().start(() -> serve(socket))`。`serve` 是一个最朴素的循环：读一帧 → 处理 → 写响应。客户端 `Connection`：发请求，阻塞等响应，同一时间一个连接上只有一个请求在途。

**(b) 为什么用虚拟线程而不是 NIO / Netty**
- 虚拟线程阻塞时会从载体线程（carrier thread）上卸载，不占用 OS 线程，所以「一个连接一个线程」的写法可以扩展到上万连接。
- 代码是线性的，易读，易调试。handler 可以直接阻塞：长轮询 fetch 等新数据、acks=all 等复制完成，都只需要在 `Condition` 上 `await`，不需要回调链或状态机。
- 代价：没有 Netty 的零拷贝和 buffer 池，吞吐上限更低。README 里写明了这一点。

**(c) Java 知识点（重点）**
- `Thread.ofVirtual().name(...).start(runnable)`、`Executors.newVirtualThreadPerTaskExecutor()`。
- **pinning（钉住）**：在 Java 21 中，虚拟线程在 `synchronized` 块里阻塞时会把载体线程一起钉住（JDK 24 已修复）。所以本项目会阻塞的地方一律用 `ReentrantLock` + `Condition`，不用 `synchronized`。这是面试加分点。
- `socket.setTcpNoDelay(true)`：关闭 Nagle 算法，小请求不会被攒包延迟。
- `BufferedInputStream / BufferedOutputStream` 加 `flush()`；try-with-resources 可以同时管理多个资源。

### 9. `controller/Controller.java` —— 静态控制器

**(a) 骨架**
- broker 每 100ms 发一次心跳，心跳同时承担注册的作用；超过 `brokerSessionTimeoutMs`（默认 1500ms）没有心跳就判定该 broker 死亡。
- `onBrokerFailure`：对它担任 leader 的分区，从 **ISR 中挑第一个还活着的副本**做新 leader，leaderEpoch+1；它担任 follower 的分区，把它从 ISR 移除。
- ISR 里没有任何活着的副本时，分区下线（leader=-1），不做 unclean election。
- `alterIsr`：leader 申请修改 ISR，校验 leaderEpoch 和 isrVersion 都匹配才接受。
- 每次变更 `metadataVersion++`，broker 通过心跳响应拉取新的元数据（pull 模型）；状态用与网络协议相同的二进制编码持久化。

**(b) 为什么**
- 没有 ZooKeeper / KRaft，所以用一个单独的 controller 进程作为唯一事实来源。它本身是单点故障，这是 README 里如实写出的限制。
- **只从 ISR 选主**是 acks=all 不丢数据的关键：被 ack 的数据在 ISR 的每个成员上都有。
- **broker 重启（incarnationId 变化）先当作一次故障处理**：重启后的 broker 被移出 ISR，必须追上进度才能重新加入。这样就避免了「刚重启、日志落后的副本还留在 ISR 里，又被选成 leader」的情况。
- **isrVersion（类似 Kafka 的 partitionEpoch）**：防止过期的 AlterIsr 覆盖新状态，比如把已经死掉的 broker 重新加回 ISR。

**(c) Java 知识点**：`ReentrantLock` 保护全部可变状态；`volatile ClusterMetadata snapshot` 发布不可变快照，读者无需加锁（copy-on-write 思想）；`TreeMap` 让输出有序、便于调试。

### 10. `broker/Partition.java` —— LEO / ISR / HW（最核心）

**(a) 骨架**
- **leader 侧**：维护每个 follower 的 `logEndOffset`、`lastCaughtUpMs`。follower 每次 fetch 带上的 `fetchOffset` 就是它的 LEO（含义是「我已经有 fetchOffset 之前的全部数据」）。
- **HW = ISR 中所有成员 LEO 的最小值**，只增不减。消费者只能读 HW 以下的数据；acks=all 的请求在 `awaitCommitted()` 上等待，直到 HW 越过它写入的最后一条记录。
- **ISR 维护**（每 100ms）：超过 `replicaLagTimeMs` 没有追上的 follower 移出 ISR；LEO ≥ HW 的 follower 加回 ISR。所有变更都先发 AlterIsr 给 controller，controller 接受后才在本地生效。
- **pendingIsr**：AlterIsr 在途期间，HW 按「当前 ISR ∪ 提议的 ISR」计算。原因是 controller 可能已经认为新成员属于 ISR，如果这时 leader 挂掉，新成员可能被选为 leader，HW 不能超过它的 LEO。这对应 Kafka 的 "maximal ISR" 思想。
- **follower 侧**：`appendAsFollower` 写入后，`HW = min(leaderHW, 自己的 LEO)`。

**(b) 为什么**
- **lastCaughtUp 的判定规则**：fetchOffset ≥ leader 当前 LEO 时，算「此刻已追上」；fetchOffset ≥ 上一次 fetch 时 leader 的 LEO 时，算「在上一次 fetch 时追上了」。这样持续高写入下的正常 follower 不会被误判为落后。
- **min.insync.replicas=2**：acks=all 要求 ISR 至少有 2 个成员，否则直接拒绝。HW 达到之后还会再检查一次 ISR 大小（`NOT_ENOUGH_REPLICAS_AFTER_APPEND`）。否则 ISR 缩到只剩 leader 时，acks=all 就退化成了 acks=1。
- **每个分区一把锁**，角色切换和追加互斥，避免「正在追加时变成了 follower」这类竞态。

**(c) Java 知识点**：`Condition.awaitNanos(remaining)` 实现带超时的等待，循环里重新检查条件，用来应对虚假唤醒；`volatile` 字段允许无锁读取（例如 `highWatermark`）；`LinkedHashSet` 求并集时保持顺序。

### 11. `broker/ReplicaFetcher.java` 和 `ReplicaManager.java`

**(a) 骨架**
- 每个 leader broker 对应一个 fetcher 虚拟线程，它把在这个 leader 上跟随的所有分区合并成一个长轮询 Fetch 请求。
- 新 epoch 的第一次拉取之前，先执行 `reconcile()`：发送 OffsetForLeaderEpoch 请求，然后截断本地日志。
- `ReplicaManager.applyMetadata`：根据新元数据决定 makeLeader 还是 makeFollower。
- `produce`：追加数据，acks=all 时等待 HW。
- `fetch`：长轮询，用 `DataNotifier`（全局版本号加 Condition）唤醒等待的请求。

**(b) 为什么**
- **拉（pull）而不是推（push）**：follower 自己控制节奏，落后时可以批量追赶。fetchOffset 天然就是确认信息，不需要额外的 ack 协议。这和 Kafka 的选择一致。
- **长轮询**：没有新数据时不必空转，数据一到马上返回，延迟低、CPU 也省。
- **fetch 请求带 currentLeaderEpoch**：epoch 对不上就返回 FENCED，防止 follower 从一个还没完成角色切换的 broker 拉数据。

**(c) Java 知识点**：`ConcurrentHashMap.computeIfAbsent` 懒创建 fetcher；`Map.copyOf` 生成快照后再遍历；`partitions.replace(tp, old, new)` 是 CAS 风格的条件替换。

### 12. `group/GroupCoordinator.java` 和 `RangeAssignor.java`

**(a) 骨架**
状态机：`EMPTY → PREPARING_REBALANCE → STABLE`。
- 新成员加入、成员离开或心跳超时，都会触发进入 PREPARING 状态。
- 在 PREPARING 状态下，所有成员的心跳都返回 `REBALANCE_IN_PROGRESS`。成员先提交位点再重新 Join，Join 请求会阻塞，直到所有成员都到齐（**屏障**），或者超时把没来的成员踢掉。
- 所有成员到齐后 generation+1，按 range 策略分配分区，同时唤醒所有阻塞的 Join。
- 位点提交写入一个内部的 minilog `Log`（相当于 `__consumer_offsets`），启动时回放。

**(b) 为什么**
- **先提交再重新加入，新 owner 在屏障之后才读取位点**：所以正常的再均衡既不丢也不重。`ConsumerGroupIT` 验证了加入、加入、离开三次再均衡之后，1200 条记录每条恰好被消费一次。
- **generation 防护（fencing）**：被踢出的「僵尸」消费者用旧的 generation 提交会被拒绝（`ILLEGAL_GENERATION`）。
- **分配逻辑放在协调者上**（Kafka 是由组内 leader 消费者计算分配）：实现更简单，代价是不能自定义分配策略。
- **Range 分配**：分区和成员都排序，每个成员分到连续的一段，前 `n % m` 个成员各多分一个。

**(c) Java 知识点**：`Condition.await(remaining, MILLISECONDS)` 实现屏障；用 `completedRounds` 计数器区分「这一轮是否已经完成」，避免丢失唤醒信号；Stream API：`filter / map / sorted / toList()`。

### 13. `client/Producer.java`

**(a) 骨架**
- `send()` 只把记录放进累加器（每个分区一个批次队列），立即返回 `CompletableFuture`。
- sender 线程循环：选出就绪的批次（批次已满、等待超过 linger.ms，或者有 flush 请求），按 leader 分组，每个 broker 发一个 ProduceRequest（用虚拟线程并行发送）。
- 失败处理：可重试的错误把批次放回队头，**保留原来的序列号**；不可重试的错误或超过 delivery.timeout 就让 future 失败，并换一个新的 producerId。
- 分区选择：有 key 时 `floorMod(Arrays.hashCode(key), n)`，无 key 时轮询。

**(b) 为什么**
- **batch.size + linger.ms**：用一点延迟换吞吐，一次网络往返和一次文件写入就能带上成百上千条记录。`BENCHMARK.md` 里有 16KB 和 128KB 两种批次大小的对比。
- **幂等**：序列号在批次第一次发送时分配，重试时不变，broker 据此去重。每个分区同时最多只有 1 个批次在途，因此重试不会乱序。producerId 是客户端生成的随机 63 位数（Kafka 由 broker 分配，这是 README 里写明的简化）。
- **背压**：缓冲字节数超过 `bufferMemory` 时 `send()` 阻塞，防止 OOM。
- **调试经历（面试可讲）**：第一次跑端到端延迟时 p50 高达 42ms。原因是 `send()` 只在批次满了时才唤醒 sender，在 linger=0 的情况下，sender 要睡到它 100ms 一次的空闲轮询才发现新批次。修复后 p50 降到 0.5ms 左右。完整过程见 README 的 Project Journey。

**(c) Java 知识点**：`CompletableFuture`（`complete / completeExceptionally / whenComplete`）；`ReentrantLock` + `Condition` 实现生产者-消费者模型（`awaitUninterruptibly`、`signalAll`）；`ArrayDeque` 的 `addFirst / pollFirst`；`Math.floorMod` 处理负的 hashCode（`Math.abs(Integer.MIN_VALUE)` 仍是负数，这是经典坑）。

### 14. `client/Consumer.java`

**(a) 骨架**
- `poll()`：需要时重新入组 → 定期自动提交 → 拉取。
- 拉取：**每个 leader 保持一个在途的长轮询请求**，任意一个返回就处理并返回结果，其他的继续在途。
- 再均衡：后台心跳线程只负责设置 `needsRejoin` 标志，真正的提交和重新入组在 poll 线程里完成。
- 位点语义：commit 提交的是「下一条要读的 offset」。

**(b) 为什么**
- **at-least-once**：poll 时提交的是之前所有 poll 返回过的记录，到下一次 poll 时应用已经处理完它们。如果在处理完之后、提交之前崩溃，这些记录会被重新投递。
- **为什么不等所有 leader 都返回**：一个空闲分区的长轮询要等满 `fetchMaxWaitMs`，会拖慢其他分区已经到达的数据。这是实测发现的问题（修复前 max 延迟约 500–1000ms），修复方法见 Journey。
- **消费者不是线程安全的**（和 Kafka 一样），单线程使用。

**(c) Java 知识点**：`Future` 和 `LinkedBlockingQueue.poll(timeout)` 组合实现「等任意一个完成」；`volatile` 标志位用于跨线程通信；`record PendingFetch` 带上 assignmentEpoch，结果过期就丢弃。

### 15. `bench/*` —— 怎么测的

- `LocalCluster` 用 `ProcessBuilder` 启动 1 个 controller 和 3 个 broker **独立 JVM 进程**（`-Xms512m -Xmx512m -XX:+UseG1GC`），`destroyForcibly()` 就是真正的 `kill -9`（SIGKILL）。
- 每个吞吐 / 延迟测试都在全新的集群上运行，测完删除数据。
- 延迟：消息体前 8 字节写入 `System.nanoTime()`，生产者和消费者在同一个 JVM 里，所以时钟一致；前 5 秒预热的数据丢弃不计。
- failover：单分区 RF=3，生产者同步逐条发送（acks=all）。从 kill 的那一刻开始计时，到「kill 之后发出的第一条记录被 ack」为止，也就是客户端感知到的不可用时间。
- chaos：2 个幂等生产者 + 3 个消费者组成员，随机 kill -9 broker，并在中途替换一个消费者。结束后校验：每条已 ack 的记录在组内恰好被消费一次，每个分区内同一生产者的序列号严格递增。

---

## 第二部分：高频面试题（答案落在本项目上）

> 数字以 `BENCHMARK.md` 为准，下面引用的数字都在那里有出处。

**Q1. 为什么 append-only log 快？**
写永远是追加到文件尾部，属于顺序 IO。机械盘省掉了寻道，SSD 对顺序写也最友好，而且 OS 可以合并写、预读。本项目里一个批次编码成一个 ByteBuffer，用一次 `FileChannel.write` 写入（`Log.appendAsLeader`），读也是顺序扫描。另外记录一旦写入就不再修改，所以读写之间几乎不需要协调（读写锁里，读者之间不互斥）。

**Q2. 顺序 IO 和 page cache 的关系？为什么默认不 fsync？**
`write()` 只是把数据写进 OS 的 page cache，由内核异步刷盘；刚写入的数据马上被消费时，也是直接从 page cache 读出来的。fsync 会强制刷盘、等待设备确认，代价很高。本项目和 Kafka 一样，默认依赖「3 副本 + page cache」来保证持久性：只要不是 3 台机器同时断电，已提交的数据就不会丢。fsync 的影响有实测数据（BENCHMARK.md 的 fsync 一节）。注意：本实验里 3 个 broker 在同一台机器上，所以「多副本」并不能防御整机断电，这一点要主动说明。

**Q3. offset 索引是怎么查的？**
两步。第一步，用 `ConcurrentSkipListMap.floorEntry(offset)` 找到 segment（O(log 段数)）。第二步，在 segment 的 mmap 稀疏索引上二分查找 ≤ offset 的最大条目，得到一个文件位置，然后从这个位置往后只读记录头（16 字节）顺序扫描，最多扫一个 `indexIntervalBytes`（4KB），找到第一条 offset ≥ target 的记录。对应代码是 `OffsetIndex.lookup` 和 `LogSegment.translateOffset`。

**Q4. 为什么索引是稀疏的？代价是什么？**
为了让索引小到能常驻内存。代价是每次查找多扫最多 4KB，但这是顺序读，而且大概率命中 page cache。

**Q5. ISR 和高水位分别保证什么？**
ISR 是「跟得上 leader 的副本集合」。HW 是 ISR 中所有成员 LEO 的最小值，所以 HW 以下的记录在每个 ISR 成员上都存在，也就是已提交。保证有三条：(1) 消费者只能读到 HW 以下的数据，所以不会读到换主后消失的记录；(2) acks=all 等到 HW 越过记录才返回；(3) controller 只从 ISR 中选新 leader，所以新 leader 一定有所有已提交的记录。测试 `highWatermarkOnlyAdvancesWhenTheIsrHasTheRecords` 验证了这三点：暂停 follower 之后，leader 的 LEO=150 而 HW=100，消费者读不到 100 以上的数据。

**Q6. follower 什么时候被踢出 ISR，什么时候回来？**
踢出：`replicaLagTimeMs` 内一直没有追上（`lastCaughtUpMs` 太旧），或者 broker 心跳超时（由 controller 直接移除）。回来：LEO ≥ HW 并且在当前 epoch 内 fetch 过。这些变更都经过 controller（AlterIsr）。测试 `laggingFollowerDropsOutOfIsrAndRejoins` 覆盖了这个过程。

**Q7. 为什么 ISR 变更要经过 controller，而不是 leader 自己改？**
如果 leader 在本地把 F 移出 ISR，就会推进 HW 并对生产者 ack。但 controller 仍然认为 F 在 ISR 里，这时 leader 挂掉，controller 可能选 F 当新 leader，而 F 没有那些已 ack 的记录，数据就丢了。所以必须先让 controller 确认，再在本地生效。扩大 ISR 时同理，在途期间 HW 按并集计算（pendingIsr）。

**Q8. leader 挂了会发生什么？（完整时间线）**
1. broker 心跳停止；
2. 超过 `broker.session.timeout.ms` 后，controller 判定它死亡；
3. 从 ISR 中选第一个活着的副本当 leader，epoch+1，把死掉的 broker 移出 ISR，元数据版本 +1；
4. 其他 broker 在下一次心跳时拿到新元数据，新 leader 执行 `makeLeader`（在 epoch cache 里记下新 epoch 从当前 LEO 开始），follower 执行 reconcile 截断后转向新 leader 拉取；
5. 生产者收到 NOT_LEADER 或连接失败，刷新元数据后重试同一个批次（序列号不变，所以不会重复）；
6. 旧 leader 重启后，被 controller 当作新的 incarnation，按 epoch 截断未提交的尾部，追上进度后重新进入 ISR。

实测的 failover 时间见 BENCHMARK.md：中位数主要由 session timeout 决定（1500ms 和 500ms 两档都测了）。

**Q9. acks=0 / 1 / all 的区别？各会丢什么？**
- 0：不等任何确认，连写没写成功都不知道，offset 返回 -1；
- 1：leader 写入 page cache 就返回，如果 leader 在 follower 复制之前挂了，这条消息就丢了；
- all：等 HW 越过记录，并且 ISR ≥ min.insync.replicas。

测试 `killingTheLeaderLosesNoAcknowledgedRecords` 做了 20 轮 kill leader，0 条已 ack 的消息丢失。吞吐对比见 BENCHMARK.md。

**Q10. at-least-once、at-most-once、exactly-once 分别是什么？本项目做到了哪个？**
- at-most-once：先提交再处理，崩溃时会丢；
- at-least-once：先处理再提交，崩溃时会重复。本项目的 Consumer 是这一种；
- exactly-once（Kafka 的 EOS）：需要幂等生产者，加上事务，把「消费位点和输出」原子提交。

本项目实现了**生产端的幂等**（生产重试不重复写），但**没有实现事务**。chaos 测试里看到的「每条恰好消费一次」，前提是只 kill broker、消费者正常运行（中途替换消费者也是干净的 close）。如果消费者自己崩溃，仍然可能重复消费。面试时必须这样诚实地讲。

**Q11. 幂等生产者是怎么实现的？**
生产者有一个 producerId，对每个分区的记录编号 0, 1, 2…，序列号在批次第一次发送时分配，重试时保持不变，并且每个分区最多 1 个批次在途。broker 的 `ProducerStateManager` 记录每个 pid 的 lastSequence：收到重复的批次时返回原 offset，不再追加；出现跳号就报错。这个状态从日志重建（记录里带有 pid/seq），所以换主之后依然有效。测试 `idempotentProducerWritesEachRecordExactlyOnceDespiteRetries` 人为丢弃了 10 次响应，200 条记录每条恰好一份；对照组（关闭幂等）出现了重复。

**Q12. 消费组再均衡是怎么工作的？**
见第 12 节：触发 → PREPARING → 心跳返回 REBALANCE_IN_PROGRESS → 成员先提交、再 Join（阻塞在屏障上）→ 全员到齐后 generation+1 并按 range 分配 → 各成员从已提交位点开始读。eager 协议的缺点是再均衡期间整个组暂停消费（stop-the-world），Kafka 后来引入了增量协作式再均衡（cooperative rebalancing）来改善这一点。

**Q13. 什么是 leader epoch，为什么只靠 HW 截断不够？**
如果 follower 重启后截断到自己的 HW，而它的 HW 落后于真正的 HW，就可能截掉已提交的记录。如果它这时又被选成 leader，就会丢数据（KIP-101 描述的场景）。用 epoch 截断：问 leader「我最新的 epoch 在你那里到哪里结束」，只截掉 leader 确实没有的部分。另外，本项目的 controller 会把重启的 broker 移出 ISR，又多了一层保护。

**Q14. 崩溃恢复具体做了什么？**
启动时没有 `.clean_shutdown` 标记，就对活跃段执行 `recover()`：逐条校验长度、CRC 和 offset 单调性，截掉第一条坏记录之后的所有内容，重建索引，epoch cache 截断到新的 LEO，再扫描日志重建 producer 状态。非活跃段只做索引合理性检查，检查不通过就重建。

**Q15. 为什么用 Java 21 虚拟线程？有什么坑？**
优点见第 8 节。坑：(1) `synchronized` 里阻塞会钉住载体线程（Java 21），所以本项目用 ReentrantLock；(2) 虚拟线程不适合 CPU 密集型任务；(3) ThreadLocal 要慎用，因为虚拟线程可能有上百万个。

**Q16. 数字是怎么测出来的？可信吗？**
在一台 M2（8 核）MacBook 上，3 个 broker 和 1 个 controller 都是独立的 JVM 进程，全部在 localhost 上。网络是回环网卡，没有真实的网络延迟，所以延迟数字偏乐观。3 个副本共用一块 SSD，也共享 page cache。关键实验各跑 3 次并报告范围。命令、JVM 参数、原始输出都在 `BENCHMARK.md` 和 `results/` 里。可以主动说明：「这些数字不能直接和真实集群上的 Kafka 对比，它们的意义在于同一环境下不同配置之间的相对差异。」

**Q17. 吞吐瓶颈在哪里？怎么优化？**
（可以引用 BENCHMARK.md 里 acks 和 batch 的对比）可能的方向：(1) 生产者每轮同步等待所有 broker 返回，可以改为每个 broker 多个请求在途（需要配合序列号窗口）；(2) fetch 读取时把文件字节拷贝到堆上，可以用 `FileChannel.transferTo`（sendfile）实现零拷贝；(3) 协议用的是 DataOutputStream 加 byte[] 拷贝，可以换成直接 ByteBuffer 加 buffer 池；(4) 3 个 broker 共享 8 个核和一块盘，跑在多台机器上的结果会完全不同。

**Q18. 如果把它产品化，你会做什么？**
- controller 改成 Raft 多副本（相当于 KRaft），消除单点；
- `__consumer_offsets` 改成多副本并做日志压缩（compaction）；
- 生产者状态做快照，避免启动时全量扫描日志；
- 优先副本选举（preferred leader election），让 leader 分布重新均衡；
- 加上 TLS 和认证、配额、监控指标（JMX / Prometheus），以及 sendfile 零拷贝；
- 事务和 EOS；
- 分区重分配、动态加 broker；
- 用 Jepsen 风格的测试做更多网络分区场景。

**Q19. 这个设计有哪些你知道的缺陷？**
主动说出来是加分项：
- controller 是单点；
- 位点存储没有副本；
- producerId 由客户端随机生成（碰撞概率极低，但不是零）；
- 网络分区下可能出现「僵尸 leader」：它在被隔离的一侧继续接受 acks=1 写入，直到它自己发现。本项目没有实现 broker 自我隔离（self-fencing）。acks=all 能防住，因为 follower 不再从它那里拉数据，HW 推进不了；
- 保留策略的时间判断用的是 segment 里最大的生产者时间戳，时间戳由客户端设置；
- 所有测试都在单机上进行。

**Q20. 你在这个项目里遇到的最难的 bug？**
可以讲 Journey 里的两个延迟 bug：一个是 sender 线程漏掉唤醒信号，一个是消费者等待所有 leader 都返回。讲的时候按「怎么发现（e2e 压测 p50 42ms）→ 怎么定位（acks=1 和 acks=all 的延迟一样高，说明瓶颈不在复制；把速率降到 500/s 后延迟反而变成 96ms，说明是在等某种超时）→ 怎么修 → 修完测到多少」这个顺序。数字以 README 和 BENCHMARK.md 为准。

**Q21. 如何证明 acks=all 不丢数据？**
两层证据：(1) JUnit 测试 `LeaderFailoverIT`，在进程内做 20 轮 kill leader 和重启，每轮之后把所有已 ack 的记录和全量消费结果对比，丢失 0 条、重复 0 条；(2) bench 的 failover 和 chaos 模式用真正的 `kill -9` 杀独立进程，结果在 `results/failover.txt` 和 `results/chaos.txt`。

---

## 附：面试时可以直接引用的数字（出处见括号）

- 持续写入，1 KiB，acks=all：**160,075 msgs/s（156 MiB/s）**，3 次范围 144,812–163,382（`BENCHMARK.md` 第 50 行，`results/produce-long.txt`）
- 持续写入，100 B，acks=all：**461,997 msgs/s**，3 次范围 416,414–523,476（`BENCHMARK.md` 第 49 行）
- 端到端延迟，5,000 msgs/s，acks=all：**p50 0.249 ms，p99 0.863 ms**（3 次的中位数；其中一次的 p99 是 61.9 ms，要如实说明）（`BENCHMARK.md` 第 113 行）
- 每次追加都 fsync 和不 fsync 相比：1,934 vs 94,858 msgs/s，**约慢 49 倍**（`BENCHMARK.md` 第 128、131 行）
- Leader failover：session timeout 设为 1.5 s 时，**中位数 1,574 ms，p95 1,647 ms**；设为 0.5 s 时，**中位数 595 ms，p95 663 ms**（各 25 次）（`results/failover.txt` 第 31、59 行）
- Chaos：3 次，每次 120 s，**52 次随机 kill -9 broker，9,634,000 条已 ack 记录，丢失 0 条、重复 0 条、乱序 0 次**（`results/chaos.txt` 第 24、45、69 行）
- 调试故事：生产者唤醒 bug 修复前后，e2e 的 p50 从 **49.5 ms 降到 0.27 ms**（acks=1）（`results/journey/e2e-before-fixes.txt` 和 `results/e2e.txt`）
- 测试：55 个，连续 15 次全量运行全部通过（`results/test-runs/summary.txt`）

讲这些数字时一定要先说明：**所有进程都跑在同一台 8 核笔记本上，通过 localhost 通信**。
