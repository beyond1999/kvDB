关于kv数据库的Q&A

### 切分 segment

### TTL是什么

### 后台合并（compaction）如何合并

### CRC32 校验是什么

WAL是什么

### 内存索引：key -> {segmentID, offset, size, ttl, tombstone} 是什么

### 按大小轮转活跃段 什么意思

后台合并：周期性把旧段里仍“最新”的键拎出来合并成新段，丢弃墓碑和过期版本。

可选 TTL；SyncEvery 控制刷盘频率；Close() 会 flush+fsync。

代码集中在一个包里便于你今天迭代；后续可拆包。

你今天可以这样推进（4 小时冲刺版）：

跑通：go run . 看输出与临时目录；用 Set/Get/Del 自测。

压力写入：调大 MaxSegmentSize，写几万条键，观察 cold 段数量与合并行为。

增强点（任选两项就算“中等+”）

崩溃恢复加速：写入 hint 索引文件，加快 Open() 时的重建。

批量写：Batch 将多条记录聚合一次刷盘，提升吞吐。

并发：读多写单已支持（RWMutex）；可增加并发基准（bench）。

前缀扫描（轻量版）：在内存里为 key 维护一个有序结构（如 []string + 二分）做 PrefixGet。

备份/快照：定期复制活跃段并阻塞短暂写入，形成一致性快照。

如果你愿意，我可以：

加上 基准测试（Go benchmark） 脚本，给你 QPS/延迟曲线；

拆包成 storage/segment, engine, compaction 三层结构；

增加 WAL 校验工具（离线扫描并报告坏记录）。





跳表是什么

B+ tree结构是什么样的

f.flush

f.fsync

跳表或平衡树

 SSTable 文件（Sorted String Table）

**WAL → MemTable → Immutable MemTable**。

Spark, Flink, Presto/Trino

分层、多路归并、写放大问题

tombstone怎么写，segment是什么、MemTable（内存跳表）是什么、SSTables是什么

写放大问题

1. **你理解哪些关键设计点？**

   - Append-only 日志为什么简单可靠？
   - 为什么需要 Tombstone？
   - 启动时如何恢复索引？为什么要做 CRC 校验？
   - 为什么要做 segment rotation？如果不做会怎样？
   - compaction 为什么必要？怎么选择要合并的段？
   - TTL 在日志里怎么表示？过期时是怎么处理的？

2. **你能自己实现核心模块吗？**

   - `encode/decode` record
   - 内存索引（map: key → {seg,off}）
   - 基本的 Set/Get/Del
   - 启动时扫 segment 重建索引

   👉 面试官不会要求你写全量 1000 行，但可能给你 30 分钟，让你“写个简化版 Append-only KV 引擎”。

3. **你能分析 trade-off 吗？**

   - 和 LSM Tree（LevelDB/RocksDB）相比，Bitcask 有什么优缺点？
   - 什么时候全量内存索引会爆炸？怎么优化？
   - 你怎么保证崩溃恢复？怎么处理 torn record？



1. 在纸上或白板上 **画出数据流**（Set → Append log → 更新索引 → Flush）。
2. 手写一个 **mini 版**（几百行以内），能跑通 Set/Get/Del，附带 CRC 校验和 tombstone。
    这能证明你不是只 copy 了代码，而是真懂。
3. 准备几句 **设计 trade-off 的答案**，比如：
   - Bitcask 适合写多读少、value 大小中等的场景（日志、缓存）。
   - 它的缺点是全量索引必须放内存，key 特别多时不合适。
   - LSM Tree 更适合大规模 key，但写放大比较严重。

> “我自己用 Go 实现过一个 Bitcask-inspired KV，核心是 append-only log + 内存索引 + tombstone + compaction。我写过从编码/解码 record，到 segment 轮转，再到后台 compaction 的逻辑。整个大概 1k 行。面试现场如果需要我可以写一个简化版，展示核心思想。”