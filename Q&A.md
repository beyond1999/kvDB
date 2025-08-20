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