# blob-cache 重写过程记录

> 本文档是 foyer（Rust，v0.22.6）→ blob-cache（Go）重写过程的工作记录，目的是向审阅者说明重写的思路、决策与取舍。曾短期加入 `.gitignore` 不入库，后按项目要求随 `docs: add foyer-to-Go rewrite process notes` 提交并推送到 GitHub，随源码一同维护。

## 1. 背景与总体思路

foyer 是一个 Rust 混合缓存库：内存缓存（foyer-memory，可插拔驱逐算法）+ 磁盘缓存（foyer-storage，block 引擎）+ 混合封装（foyer crate 的 HybridCache）+ 公共组件（foyer-common）。

重写策略是**自底向上、按依赖顺序分阶段提交**：每实现一个可编译、可测试的模块就提交一次，共 11 个 commit。重写以 foyer 的**语义保真**为目标（同样的生命周期、驱逐行为、事件语义），但在 API 形态上遵循 Go 惯例（泛型、context、阻塞式调用）。

## 2. 模块对照表

| foyer (Rust) | blob-cache (Go) | 说明 |
|---|---|---|
| foyer-common `code.rs` | `common/hash.go` + `common/xxhash.go` | 纯 Go 实现 XXH64（对照官方向量测试）；磁盘索引持久化 hash，必须跨进程稳定，故不能用 `hash/maphash` |
| foyer-common `error.rs` | `common/error.go` | ErrorKind + 有序 context + source，实现 `Unwrap` 支持 `errors.Is/As` |
| foyer-common `event.rs` | `common/event.go` | Event 枚举 + 泛型 `EventListener[K,V]` 接口 |
| foyer-common `properties.rs` | `common/properties.go` | trait `Properties` → 具体结构体（见 §4.2） |
| foyer-common `metrics.rs` | `common/metrics.go` | prometheus registry → 原子计数器 + `Snapshot()` |
| foyer-memory `record.rs` | `memory/record.go` | record + 原子引用计数 + IN_INDEXER/IN_EVICTION 位标志 |
| foyer-memory `eviction/*` | `memory/eviction*.go` | Eviction trait → 泛型接口；五种算法全部实现 |
| foyer-memory `raw.rs` | `memory/cache.go` | RawCacheShard（分片 + indexer map + 驱逐 + 事件） |
| foyer-memory `inflight.rs` | `memory/inflight.go` | GetOrFetch 单飞去重 |
| foyer-memory `piece.rs` | `memory/piece.go` | Piece（去掉了显式引用计数，见 §4.1） |
| foyer-memory pipe | `memory/pipe.go` | 无界队列（mutex+cond），Send 永不阻塞 |
| foyer-storage `io/device/*` | `storage/device.go` | fsDevice：目录下每 blob 一个文件，`ReadAt/WriteAt` |
| foyer-storage `serde.rs` | `storage/serde.go` | blob 文件 + entry 记录格式（见 §4.4） |
| foyer-storage `code.rs`(Code) | `storage/code.go` | `Codec[K,V]`（gob 默认实现 + BytesCodec 快路径） |
| foyer-storage `engine/block/indexer.rs` | `storage/indexer.go` | 分片 map：hash → (blobID, offset, len) |
| foyer-storage `keeper.rs` | `storage/keeper.go` | 写队列去重（current 标志防旧覆盖新） |
| foyer-storage `engine/block/flusher.rs` | `storage/engine_blob.go` | 批量刷写 + blob 轮转（大幅简化，见 §4.4） |
| foyer-storage `engine/block/recover.rs` | `storage/engine_blob.go recover()` | 全量扫描恢复，CRC 校验，torn-tail 截断 |
| foyer-storage `engine/block/reclaimer.rs` | `storage/engine_blob.go vacuum()` | 回收 = 幸存条目重写 + 删 blob（见 §4.5） |
| foyer-storage `store.rs` | `storage/store.go` | Store 门面：稳定哈希、admission filter、Load/Enqueue/Delete |
| foyer `hybrid/*` | `hybrid/cache.go` `hybrid/builder.go` | HybridCache：get 提升、GetOrFetch 链、pipe 泵 |
| foyer examples | `examples/memory` `examples/hybrid` | 可运行示例 |

## 3. 提交演进（11 个 commit）

1. `chore: initialize repository with go module scaffold` — git init（main 分支）、.gitignore、Apache-2.0 LICENSE、README。**.gitignore 中预先加入 `REWRITE-NOTES.md` 防止本文档误提交**。
2. `feat(common): add errors, events, entry properties and xxHash64 hasher` — 公共层；xxHash64 用官方测试向量（`""`→0xEF46…、`"abc"`→0x44BC…）+ 分块增量一致性验证。
3. `feat(memory): add in-memory cache core with ref-counted entries and pluggable eviction` — record/引用计数/LRU/FIFO/Cache 分片/builder/pipe/事件。此阶段用 foyer 源码逐行对照了 emplace/evict/get/remove/release 的锁序与事件时序。
4. `feat(memory): add w-TinyLFU, S3-FIFO and Sieve eviction algorithms` — count-min sketch 自实现（eps/confidence → buckets/hashes 按标准公式），三种算法严格对照 foyer 实现逐条翻译（S3FIFO 的 ghost 队列按权重容量、频率 2bit 饱和计数；Sieve 的 hand 指针语义）。
5. `feat(memory): GetOrFetch with per-key in-flight deduplication` — leader/follower 单飞，follower 通过 channel 拿到 Clone 后的 entry（引用计数在 leader 侧完成）。
6. `feat(storage): add filesystem blob device and I/O statistics`。
7. `feat(storage): add entry serde, codec, indexer, keeper and engine interface` — 定义磁盘格式与 Engine 接口（Load/RecoverMode/StorageFilter/EstimatedSize 等都对照 foyer 命名）。
8. `feat(storage): add blob engine with flusher, recovery, reclaim and pickers` — 核心：提交队列 + 单 goroutine flusher + 轮转 + 恢复 + vacuum。开发中发现并修复两个关键 bug：entry 头 24→28 字节（props 与 crc 重叠）；Wait 提前返回（wait channel 在写盘前被 close）。
9. `feat(storage): add Store facade with stable hashing, admission filter and builder` — Store 在 Enqueue 时**重算稳定哈希**（见 §4.3）。
10. `feat(hybrid): add HybridCache combining memory and disk layers` — insert 链、get 磁盘命中提升（InsertPiece + SourceMemory）、GetOrFetch 三级链路、pipe 泵 goroutine、Close（flush-on-close 先 Resize(0) 全量驱逐过 pipe 再关）。
11. `feat: add hybrid and memory examples, update README usage` — 两个示例实际运行验证输出正确。
12. （本次）`REWRITE-NOTES.md` — 本文档，不提交。

每个 commit 都满足：`go vet ./...` + `go test ./... -race` 全绿（测试共约 40 个用例，含 `-race` 并发压测）。

## 4. 关键设计决策与偏差

### 4.1 引用计数：Arc/Drop → 原子计数 + finalizer

foyer 的核心机制是 `Arc<Record>` + `Drop`：entry 释放时若引用归零则调用 `eviction.release`（LRU unpin）。Go 没有确定性析构，方案：

- `CacheEntry` 内含堆上的 `entryHandle`（持引用计数），`Release()` 显式释放；
- 同时 `runtime.SetFinalizer(handle, release)` 兜底：忘记 Release 的 entry 在 GC 后自动释放，不会永久 pin；
- 实测确认了 foyer 的真实语义并照搬：**Insert 返回的 entry 不 pin（LRU 不保护），只有 Get 触发 acquire 才 pin**。测试因此改为 `Get` 后持有验证 pin 行为。

### 4.2 Properties：trait → 具体结构体

foyer 的 `Properties` trait + `GenericProperties` 在 Go 中直接改为值结构体 `common.Properties{Phantom, Hint, Location, Age}`，`With*` 方法返回副本。类型安全等价，避免了 Go 接口模拟 trait 的样板代码。

### 4.3 双哈希设计（与 foyer 的一个结构性偏差）

foyer 内存/磁盘共用一个 XxHash64（对 key 直接 hash）。Go 泛型下无法对任意 `comparable K` 高效取字节，故拆成两层：

- **内存层**：`maphash.Comparable`（进程内随机种子），用于分片、S3FIFO ghost、LFU cms —— 只在进程内有意义；
- **磁盘层**：key 经 `Codec` 序列化成字节后 `xxHash64`（跨进程稳定），由 `Store.Enqueue/Load/Delete` 统一计算；Enqueue 时用稳定哈希**重建 piece**，保证 keeper/indexer/磁盘索引哈希一致。

### 4.4 磁盘引擎：block 引擎 → blob 引擎

foyer 的 block 引擎（固定 block、blob index 区、io task、异步轮转、tombstone log）非常复杂。blob-cache 采用同概念的简化设计（也契合项目名）：

- blob 文件布局：`[8B header: magic/version][entry...]`；
- entry 记录：`magic(4) hash(8) keyLen(4) valLen(4) props(1B) crc32(4) key value`（头 28B，CRC 覆盖 key||value）；
- 单 flusher goroutine：`mu+cond` 无界提交队列 → 批量编码 → 顺序追加，空间不足则轮转（Sync 旧 blob、CreateBlob 写头）；
- 恢复：按 blob id 升序全量扫描，CRC 校验，坏尾截断（RecoverWary 默认 / Strict 报错 / Quiet 跳文件）；
- 回收（vacuum）：blob 数超过 `TotalCapacity/blobSize` 时，按 picker（Fifo / InvalidRatio）选受害者，把幸存条目重写进当前 blob 后删除受害者文件。

**vacuum 的关键语义决策**：调试中发现"幸存者放不下也轮转"会造成 blob 数净变化为 0 的死循环。最终定为：**vacuum 绝不轮转**——当前 blob 装不下的幸存者直接淘汰（这就是磁盘层驱逐），且跳过已被更新的条目（`ix.get(hash)` 指向非受害者时跳过，防止旧数据覆盖新数据）。每次 vacuum 净减一个 blob，保证终止性。

### 4.5 已知限制（有意简化）

- **删除不跨重启**：Delete 只删内存索引，重启扫描后已删条目会复活。与 foyer 不启用 tombstone log 时行为一致（foyer 的 tombstone log 未重写）。
- 未实现：压缩（zstd/lz4）、IO throttle、io_uring/psync 抽象、tombstone log、HybridCacheWriter、opendal 引擎、fastrace 追踪、prometheus registry。
- 恢复/vacuum 将整个 blob 读入内存（blob 文件上限默认 64MB，仅启动/回收时发生）。
- vacuum 重写幸存者时若中途空间不足，同一 hash 的更晚副本可能被淘汰而保留较早副本（值仍有效但略旧，下次写入覆盖）。

## 5. API 形态映射（Rust → Go）

- `async fn load/close` → `Load(ctx, ...) / Close(ctx)`（阻塞 + context 取消）；
- foyer 的惰性 future（`GetOrFetch` 返回 Future、`try_unwrap/need_await`）→ 直接阻塞式 `GetOrFetch(ctx, key, fetch)`；
- `Arc<dyn EventListener>` → 泛型接口 `EventListener[K,V]` + 函数适配器；
- builder 两阶段（`HybridCacheBuilderPhaseMemory/Storage`）→ 单 builder + option 函数切片（`WithMemoryOptions/WithStorage(dir, opts...)`）；
- 泛型约束 `Key: Hash+Eq` → `K comparable`；`Code` trait → 显式 `Codec[K,V]`。

## 6. 测试验证

- `go vet ./... && go test ./... -race` 全绿；race 开关下含 8 goroutine × 2000 次 Insert/Get/Remove/Touch 混合并发压测（memory）、300 条多 blob 回收压测（storage）、16 goroutine 单飞去重（memory/hybrid）。
- xxHash64 对照官方测试向量与分块增量一致性；
- 端到端场景测试：插入→落盘→重启→恢复读回（hybrid），写坏 blob 尾部→Wary 恢复保留有效前缀 / Strict 拒绝打开；
- 两个示例程序真实运行，事件/指标输出符合预期。

## 7. 深度 Review（两轮，13 项修复）

初版完成后做了两轮系统性 review：第一轮自查 + 第二轮引入独立 agent 做对抗性审查（逐行对照 foyer Rust 源码 + 实证复现）。发现并修复：

**第一轮（自查）：**
1. **inflight 晚到 follower 竞态**：follower 在 leader `complete()` 之后、`unregister()` 之前注册会拿到空 entry + nil error。修复：`completed` 标志 + call.mu 内补发克隆（`TestInflightLateFollower`）。
2. **keeper 未接入 Load**：foyer 的 `Store::load` 先查 keeper（未落盘条目可命中），初版漏掉。重构：keeper 移到 Store、`Engine.Enqueue` 接收 `*PieceRef`（flush 完成后 `ref.Remove()`）。
3. **同批次 enqueue+delete 顺序错误**：delete 在写盘前处理会被同批 insert 复活。修复：delete 延迟到写盘之后统一应用。
4. **恢复时选中头损坏的 blob 作为当前写 blob**：offset=0 会覆盖文件头导致下次恢复丢数据。修复：只在 `Written >= blobHeaderSize` 的 blob 里选 current，否则开新 blob。

**第二轮（独立审查）：**
5. **Touch 永久 pin LRU 条目**（实证复现）：Touch 调用 acquire 却无配对 release，反复 touch 会逐渐 pin 死整个缓存。修复：对齐 foyer 语义（touch = get_noop，纯存在性检查，不 pin、不重排、不涨频率）。
6. **indexer.insertBatch 非稳定排序**（实证复现）：同 hash 的批内多副本经 pdqsort 乱序后旧值覆盖新值，之后所有 Load 永远返回旧数据。修复：`SortStableFunc` + 明确"最后者胜"契约（`TestInsertBatchDuplicateOrderIsStable`、`TestVacuumStaleDuplicate`）。
7. **LocationInMem 条目经驱逐 pipe 泄漏到磁盘**：修复：pipe 泵丢弃 InMem 条目（对齐 foyer 的 hybrid pipe send）（`TestHybridInMemNotPersisted`）。
8. **Destroy(ctx) 取消时新旧 flusher 并存**：ctx 已取消时 Close 提前返回，随后 reset 状态并再起一个 flusher，旧 flusher 可能未退出 → 两个 flusher 并发写坏状态。修复：Destroy 无条件 join 旧 flusher（不可中断、原子完成）。
9. **GetOrFetch 磁盘加载错误直接失败**：foyer 中可选 fetch 的错误会被丢弃并回退到用户 fetch。修复：磁盘加载错误穿透到 fetch（`TestHybridGetOrFetchFallsBackOnDiskError`）。
10. **混合缓存缺 Get 去重 / 磁盘提升丢失 Age 与 SourceDisk**：重写为 hybrid 层 inflight（复用 memory 的实现），Get/GetOrFetch 统一走 leader/follower；磁盘命中以 `SourceDisk` + 存储的 Age 提升进内存。
11. **Load 与 vacuum 竞态**：读已删除 blob 的 not-exist 错误映射为 miss，不再向上抛错。
12. 其余小项：emplace 已驻留快路径（对齐 foyer）、phantom 替换的 entries 指标、Remove 触发 OnLeave(EventRemove)、写失败清理 keeper refs、vacuum 拒绝回收当前 blob、OnDisk 插入 force=false。

## 7.5 深度 Review 第三轮（独立审查，8 项修复）

针对 round 2 大改后的新代码（hybrid inflight 重写、缓冲乒乓复用、Destroy 重写）再做一轮独立对抗审查，修复：

1. **同批 `[delete, insert]` 丢失重插条目**（round 1 修复的反向副作用）：延迟应用 delete 会让同批稍后的 insert 被该 delete 删掉。重构：`handle` 严格按提交顺序内联处理（写入即索引、delete 即时应用），顺序语义与调用方完全一致（`TestStoreDeleteThenInsertSameBatch`）。
2. **写失败后 keeper 注册泄漏**：批量写中途失败只清理了已写部分，失败条目及其后所有条目的 keeper 注册永久残留 → Load 永远命中幽灵值。重构后逐条处理：失败即中止剩余并逐个 `ref.Remove()`（`TestStoreWriteFailureDropsKeeper`，用注入写失败的 Device 包装器验证）。
3. **batch 中止后 Wait 永久挂起**：同批的 wait 提交被 batchErr 跳过，waitCh 永不关闭。修复：waiter 收集不受错误影响（`TestStoreWriteFailureDropsKeeper` 覆盖）。
4. **hybrid Get leader 无 panic 防护**：codec/weighter/监听器 panic 会让 inflight 泄漏、该 key 永久挂起。修复：与 GetOrFetch 相同的 recover→Complete→Unregister→re-panic（`TestHybridGetPanicDoesNotLeakInflight`）。
5. **Get follower 对 leader miss 误记 hit 指标**。
6. **MayContains 缺 keeper 检查**：对齐 foyer 的 `keeper.contains || engine.may_contains`（`TestMayContainsBeforeFlush`）。
7. **vacuum 同 victim 内同 hash 多副本重复重插**：改为单遍扫描保留每 hash 的最后副本。
8. 其余：自定义 picker 返回 current 时 reclaim 自旋防护、`closeErr` 成功批次后清零、Close/Destroy 并发串行化（destroyMu）、vacuum 重插同样受 MaxInt32 守护、清理不再使用的 rawRecord 与批量缓冲字段。

审查确认干净的区域：inflight 协议（Register/Follow/Complete/Unregister/Done 无丢失无挂起）、缓冲乒乓复用无别名问题、keeper 指针身份在所有 supersede+flush 交错下安全、recover/vacuum 记账、pipePump 与 flushOnClose 顺序、Oversize 守护与 on-disk 格式一致性。

## 7.6 深度 Review 第四轮（新视角：模型差分 + 故障注入 + soak）

前三轮都是"读代码对照 foyer"，这一轮换成**自动化对抗测试**视角，新增四套常驻测试套件：

1. **memory 策略无关差分测试**（`memory/differential_test.go`）：随机操作流（8 个种子 × 4000 操作 × 随机驱逐算法/分片数）持续校验跨算法不变量——命中必返回最新值、Contains 与 Get 一致、Remove 后必缺席、Clear/EvictAll 后清零、usage/entries 指标与实际状态零漂移、容量上界。
2. **memory 精确 LRU 参考模型**（`memory/cache_model_test.go`）：单分片纯 LRU 与一个逐操作对账的参考实现比对——连驱逐受害者都逐一验证（含并发变体）。它先"抓到"两个分歧，核实后均为**模型伪差分**：(a) verify 的 Get 会触碰 LRU 顺序，模型需同步；(b) foyer 的 emplace 是"先驱逐到 capacity-weight、后替换"（raw.rs:190），满载替换会多驱逐一个无辜条目——实现与 foyer 完全一致，模型修正后全绿。这两个语义现在被测试钉死。
3. **storage 差分 + 随机重启**（`storage/differential_test.go`）：随机 enqueue/delete/overwrite/重启（8 种子），核心不变量：Load 永远返回最新入队值、绝无脏值；重启后 delete 复活语义被显式建模。
4. **storage 故障注入**（`storage/fault_test.go`）：包装 Device 随机注入 Read/Write/Sync/ReadAll/CreateBlob 失败。不变量：确认态之外的 Load 绝无脏值/外来值、不 panic 不挂起、故障停止并重启后每个确认条目按确认值恢复。
5. **storage fuzz**（`storage/store_fuzz_test.go`）：随机大小值 + 覆盖 + 删除 + 重启，分 no-reclaim（强断言：全部存活）与 reclaim（只查无脏值）两种配置 × 6 种子。
6. **hybrid 并发 soak**（`hybrid/soak_test.go`）：8 worker × 1500 随机操作 + 后台 churn，全程 -race，命中值必须属于值池。

**本轮发现的实现 bug（1 个，独立 commit 修复）：**

- **vacuum 复活已删除条目**：删除只移除内存索引，victim blob 里的数据副本变成"索引中不存在"，而 vacuum 把索引未命中的条目当作孤儿**重插**——导致删除后若触发回收，key 立即复活且可能带着旧副本。修复：对齐 foyer reclaimer 语义（不在 indexer 中的条目 = 已删/已覆盖 → 丢弃）。回归测试 `TestVacuumDropsDeletedEntries`。

**本轮的测试侧修复**（非实现 bug，但值得记录）：differential verify 的 Get 会改变 LRU 顺序需同步模型、故障注入窗口内 enqueue 应标记"状态未知"、fuzz 的驱逐丢失阈值需按工作负载推导、soak 的 churn goroutine 停止顺序死锁、faultDevice 的 RNG 数据竞争。

## 8. 性能

基准环境：AMD Ryzen 5 2500U / Linux / go1.25.3，`-benchmem -benchtime 1s`：

| 基准 | ns/op | B/op | allocs/op |
|---|---|---|---|
| memory Insert | 1453 | 283 | 5 |
| memory Get 命中（默认 w-TinyLFU） | 782 | 48 | 2 |
| memory Get 命中（关闭 entry finalizer） | 499 | 32 | 1 |
| memory Get 未命中 | 53 | 0 | 0 |
| memory 混合负载（并行） | 512 | 68 | 2 |
| storage Enqueue（提交路径） | 3507 | 1068 | 4 |
| storage Load 磁盘命中 | 1843 | 192 | 3 |
| storage Load keeper 命中 | 1641 | 175 | 2 |
| hybrid Get 内存命中 | 691 | 53 | 3 |
| hybrid Get 磁盘命中（页缓存） | 808 | 56 | 3 |
| hybrid GetOrFetch 命中 | 661 | 55 | 3 |
| hybrid 混合负载（并行） | 1361 | 223 | 5 |

review 中由 profile 驱动的优化：
- **entry finalizer 可配置**（`WithEntryFinalizers(false)`）：pprof 显示 finalizer 占 Get 路径 ~17%（CPU）+ GC 存活压力；默认开启保安全，关闭后 Get 从 782→499ns。Java 系缓存在 Go 的惯用替代是强制 Release，这里两种都支持。
- **cms 哈希**：每行一次完整 XXH64（占 Get ~8%）→ 单次 xxhash + splitmix64 派生行号，消除该热点。
- **LFU 同队列 MRU 移动**：Remove+PushBack（每次 Get 分配一个 list.Element）→ `MoveToBack`（零分配），Get 分配 3→2。
- **enqueue 双重编码**：Store 为算哈希/大小编码一次、flusher 再编码一次 → `EnqueuePrepared` 复用载荷，只编码一次。
- **提交队列/batch 缓冲乒乓复用**：批间换手后从 nil 重新扩容（EnqueuePrepared 占 38% 分配）→ 双缓冲 + 容量下限，append 归零（pprof 对象数 54/2s）。
- **keeper 三分配合一**：pendingEntry+atomic.Bool+PieceRef → 单 PieceRef（指针身份判 supersede）。
- **insertBatch 分组**：每批分配 map → 稳定排序分组。

与 foyer 的对比定位：算法结构（分片锁、无界提交队列、批量刷写、引用计数 entry）与 foyer 同构，绝对值差异主要来自 Go 的 GC/finalizer 与 Rust Arc/Drop 的零成本差异；关闭 finalizer 后内存命中路径 ~500ns，分配 1 次/操作，处于 Go 缓存库的正常水位。

## 9. 复现步骤

```bash
cd ~/projects/blob-cache
git log --oneline          # 查看分阶段提交
go test ./... -race        # 全量测试
go test ./... -bench . -benchmem   # 基准
go run ./examples/memory   # 内存缓存示例
go run ./examples/hybrid   # 混合缓存示例（数据写入 ./cache-data，已被 gitignore）
```
