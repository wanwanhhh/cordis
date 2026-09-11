# Cordis 开发规范

> 给维护者：改动本 crate 时必须遵守的约束、流程与测试要求。

---

## 1. 模块结构

```text
src/
  lib.rs        # 公共导出与文档
  id.rs         # ScopeId / TaskId / Blockers（依赖叶子，不 import 任何框架概念）
  context.rs    # Builder / Context / Runtime / Configurator / ScopeLease / ScopeCore / 后台任务
  error.rs      # Error / ErrorKind / Phase
  event.rs      # Event / EventHandler / ErasedEventHandler / Subscription
  notify.rs     # 旁路通知的有界 FIFO lane（tokio 门控）
  plugin.rs     # Plugin / Dependency / PluginDependency
  registry.rs   # ScopeRegistry：按名字管理子作用域（tokio 门控）
  service.rs    # ServiceRegistry / DynamicValue
  waiters.rs    # 一次性电平信号 + 带代际的等待者槽位表
```

新增能力优先放在对应模块；跨模块类型通过 crate 根导出统一公开。

依赖方向：`error.rs` / `event.rs` 只依赖 `id.rs` 取身份类型，**不得**反向 import `context.rs`——`context.rs` 是装配过程所在的高层模块，被底层错误/事件模块依赖会形成模块环。`context.rs` 目前仍是最大的单文件（三阶段模型与生命周期状态机彼此耦合，拆开只会把最敏感的并发不变量摊成跨文件约定），这是有意保留的。

---

## 2. 核心不变量

1. **冻结点唯一**
   - `Data` 只有在其所属 `Builder::build()` 中 `Arc::new` 一次。
   - 冻结后不存在框架可见的 `&mut Data` 路径。

2. **`Context` 只读**
   - `Context` 上没有 `provide` / `plugin` / `on` / `off` / `start` / `stop` / `require_mut` / `request_stop`。
   - `scope()` 是 `Context` 上唯一的构建入口，但它只递增父计数并返回新 `Builder`，不修改既有服务注册表。
   - `spawn()`（`tokio` feature）只登记后台任务到簿记注册表，不触碰冻结数据面。
   - 停止**请求**不放在 `Context` 上：`request_stop` 只存在于 `Runtime::stop_handle()` 显式授出的 `StopHandle`，避免每个插件都获得环境权限。
   - 所有服务/插件写操作只存在于 `Builder` 或 `Configurator`。

3. **`Runtime` 不 Clone**
   - `Runtime` 是生命周期唯一所有者。
   - 控制方法必须是 `&mut self`。

4. **零 `unsafe`**
   - 禁止引入 `unsafe`。
   - 不用 `ManuallyDrop` 等绕开析构顺序的工具。

5. **内部可变白名单**
   - 白名单是**穷举**的：框架里每一处内部可变都必须在此列出，未列出即视为违规（否则「白名单」只是装饰）。
   - 生命周期与信号：`ScopeCore { inner: Mutex<ScopeInner>, tag: AtomicU64, cancellation: Waiters, settled: Waiters }`（`tag` 是派生缓存，只在 `commit` 的同一临界区刷新，供免锁快路径读）；`Waiters { fired: AtomicBool, state: Mutex<State { slots, free }> }`（用于 `Data.core.cancellation`、`Data.core.settled` 与 `TaskCell.finished`）。
   - `Data` 上的关闭回调与任务：`closing_hooks: Mutex<ClosingHooks>`（表与派发状态 `NotStarted`/`Dispatching`/`Dispatched` **同锁**）、`closing_hook_seq: AtomicUsize`、`closing_errors: Mutex<Vec<Error>>`、`tasks: TaskRegistry`（`tokio` feature；`{ tasks: Mutex<Vec<Arc<TaskCell>>>, next_id: AtomicU64, compact_at: AtomicUsize }`）。
   - `TaskCell`：`abort: OnceLock<AbortHandle>`、`finished: Waiters`、`outcome: Mutex<Option<TaskOutcome>>`、`report_settled: AtomicBool`（任务体 `Err` 的 `TaskFailed` 是否已送达）。
   - 通知 hub（`tokio` feature）：`NotifyHub { shutdown: watch::Sender<bool>, closed: AtomicBool, inputs: Mutex<Vec<LaneInput>>, 每 lane 的 dropped/rejected: AtomicU64 }`。
   - 可选注册表（`tokio` feature）：`ScopeRegistry { parent: OnceLock<Context>, state: Mutex<RegistryState { entries: HashMap<String, Slot>, shutting_down: bool }> }`。停机闩与登记表**必须同一把锁**。
   - 其余：懒工厂的 `OnceLock<T> + init_lock: Mutex<()>`、`DynamicValue` 服务内部的 `RwLock`、`Runtime.active: AtomicBool`（只服务 drop 诊断）、计数器 `static NEXT_CONTEXT_ID: AtomicUsize` 与 `TaskRegistry.next_id: AtomicU64`。
   - 白名单项只做框架生命周期簿记，不构成通用可变通道；运行期可变配置仍必须通过 `DynamicValue` 暴露。
   - `children`（`BTreeSet<ScopeId>`）必须与生命周期状态在同一把 `ScopeCore.inner` 锁内维护，**不得**拆成 `Data` 上的独立 `Mutex`：阻塞清单、租约数与「是否已提交关闭」必须同临界区读取。不要再引入独立的 `leases` 计数——集合大小恒等于活跃租约数，两份表示只会制造分叉。

6. **租约字段序与归还点**
   - `Runtime.lease` 必须是最后一个声明字段（`Builder.lease` 同理位于 `plugins` / `dispose` 之后）。
   - 归还点有两个：`stop_impl` 进入 `Stopped` 之前显式 `take()`（清理已完成），或字段析构（未 stop 的 Runtime / Builder）。
   - 不得在 `impl Drop for Runtime` 方法体里归还租约。
   - **`take()` 必须早于 `set_lifecycle(Stopped)`**：否则会出现「`is_stopped()` 已为真、父 `children()` 仍列出该子」的可观测窗口。
   - 必须保留 `scope_lease_release_happens_after_child_plugin_drop` 探针测试。

---

## 3. 生命周期状态机

生命周期状态**没有本地副本**，唯一真值源是 `ScopeCore` 里 `Mutex<ScopeInner>` 保护的 `{ lifecycle, requested, abandoned, children }`。`Context::scope()` 的「检查未关闭 + 登记子 id」与 `Runtime::stop` 的「提交关闭（`begin_close`）」「确认集合为空 + 转入 `Stopping`（`enter_cleanup`）」在同一临界区内判定，拆成两个独立原子会在两步之间裂开竞态（父停止的同时长出子作用域，或阻塞清单与状态不一致）。热路径 `is_stopping()` / `is_stopped()` 读 `ScopeCore` 的派生 `AtomicU64` tag，单次原子读、不拿锁。

（活跃子作用域没有独立的 `leases` 计数：`children` 是 `BTreeSet<ScopeId>`，集合大小恒等于活跃租约数。）

7 个状态与完整转换表见 `docs/architecture.md` §2.1。此处只列维护者必须守住的不变量：

- 状态只由 `&mut self` 的 `start_with` / `stop_impl` 推进（`Runtime` 不 `Clone`，不存在 owner 之外的写者）。
- `Built` / `Starting` / `Running` / `Failed` 可提交停止进入 `Closing`；租约为零时转 `Stopping`（唯一可续跑的清理进行态）；`Stopped` 是终态。
- `Failed` 与 `Running` **平行**，不是终态：失败后 `stop` 仍须回收已进入启动流程的插件。`Stopped` 只在插件 `stop`、任务排空、dispose 全部走完、**租约已归还**之后落地。
- `stop` 被活跃子作用域阻塞时返回 `StopOutcome::Blocked(Blockers)`（**不得**再引入 `ErrorKind::ActiveScopes`）：父层已进入 `Closing`（拒绝新工作、取消已广播），但清理未开始；`Blockers` 必须与实际租约同临界区读取，调用方回收子级后重试即可续跑。意图不可逆，不得提供「回退到 `Running`」的路径。
- `start` 重入语义：`Running` 幂等 no-op；`Failed` 返回带 `source` 的 `StartFailed`；`Starting`（start future 被取消）返回不带 `source` 的 `StartFailed`；`Closing` / `Stopping` / `Stopped` 返回 `Ok`——`Ok` 只表示「不再需要启动」，不等于本次调用完成了启动。
- 未 `start` 的 `stop` 只跑 dispose hooks，不调用插件 stop（自持清理 future 的 `started_plugins` 为空）。
- **清理由 `Runtime` 自持的 future 驱动**：进入 `Stopping` 时把 `plugins` / `dispose` / `started_plugins` / `ctx` / deadline move 进 `stop_future` 并**构造一次**；此后 `stop_impl` 只 `poll_fn` poll 它。调用方丢弃外层 await 不中断清理，重入 poll 同一 future，因此每个插件 `stop` 与 dispose hook **只被调用一次**。**不得**退回「游标 + 重放」的实现——那会重新要求插件 `stop` 可重入。累积错误保存在 future 内部，跨重入保留。
- 任务排空可续跑且**不重放**，且**不把在飞句柄挂在 `Runtime` 上**：表元素是与 `TaskHandle` 共享的 `Arc<TaskCell>`，「先 await 完成信号、再取出」使被丢弃的 future 不摘走任何表项，重入重新处理同一个 cell（完成信号电平触发，已结束的立即返回）。「取出—判断—上报」之间不得插入 `await`，否则取消会落进中间造成漏报——这条不变量同时承担去重：被丢弃的 future 只可能停在 await 上，因此不存在「已上报的 cell 又回到表里」的情形，不需要额外的去重标志。
- **`spawn` 的剪除是摊销的，且必须保住「排空仍需上报」的结局**：只在表长跨过 `compact_at` 阈值时做一次 `retain`，随后按实际长度翻倍，n 次 `spawn` 的累计代价是 O(n)。按 `is_finished()` 盲删会让一条 panic 因为它之后又有人 `spawn` 过而被静默丢掉。`TaskCell::needs_drain_report` 是这条判断的唯一出处（覆盖 `Panicked`、`Aborted(Timeout)`、以及 `Failed` 但 `report_settled == false`）。
- **「结局是 `Failed`」不等于「`TaskFailed` 已送达」**：`report_settled` 只在 `emit_notify` 正常返回后置位，且先于 `finished.fire_all()`。排空不得仅凭结局类别就把 `Failed` 当作已上报——上报可能被 handler panic、排空超时 abort 在 await 点取消、或宿主丢弃 future 截断。`TaskOutcomeKind` 必须把 `Failed` 与 `Completed` 分开（合并会让这条区分不可表达）。`drain_report_error` 对「`Failed` 且未送达」补报 `TaskFailed`。
- **清理 future 体不得展开**：`plugin.stop`、dispose 与关闭回调都经隔离包装（`run_lifecycle_unit` / `run_close_hook`）把 panic 转成 `ErrorKind::LifecyclePanicked`，绝不让用户代码 panic 逃出 `stop_future`。逃出会 poison 该 future（`self.stop_future` 清不掉）、跳过后续插件与全部 dispose、并让重入 poll 触发 `async fn resumed after panicking`。`plugin_stop_panic_is_isolated_and_cleanup_completes` / `dispose_panic_is_isolated_and_cleanup_completes` 守着它。
- **关闭回调恰好一次**：回调表与派发状态（`NotStarted`/`Dispatching`/`Dispatched`）必须同锁；`on_closing` 的「入表 + 判定是否即时自跑」与 `run_closing_hooks` 的「迁移状态 + 取快照」在同一临界区。分属两域会出现同一回调跑两次或永不触发。链式注册有三道上界，触顶经 `mark_cutoff` 每层至多记一条 `CloseHookDispatchOverflow`：轮次 `MAX_CLOSING_HOOK_ROUNDS`、执行的嵌套深度（线程局部计数，跨层共用）、**链式注册**总数 `MAX_CHAINED_CLOSING_HOOKS`（按层计）。「链式」= 注册发生在关闭钩子执行上下文内（线程局部深度 `> 0`）或目标层已开始派发；**不得**改成「按目标层派发状态」判定（会被向 `NotStarted` 层注册绕过并跨层指数放大），也**不得**改成「所有运行期注册一律计费」（长生命周期层反复注册/注销会误耗尽）。`run_hook_one` 是深度维护的**唯一出处**（每次 `run_close_hook` 外围 `ClosingHookDepth::enter()`），即时自跑与 `run_hook_batch`（轮次批次、收尾 remaining）都经它；三个执行路径缺一不可地被它守护。**不得**去掉深度闸或总数闸——前者防栈溢出 abort（`self_propagating_close_hook_is_bounded_and_reported`），后者防分支式自增殖按 `k^16` 膨胀（`branching_close_hook_registrations_are_bounded`），链条口径由 `runtime_registration_into_not_started_layer_is_budgeted` 与 `runtime_registration_churn_outside_dispatch_does_not_exhaust_budget` 双向锚定。`closing_errors` 在清理 future 起首与末尾各取一次。
- **提交停止立刻广播取消**（`Waiters::fire_all`），顺序是：`begin_close`（置 `Closing` + 刷新 tag）→ 无条件 `fire_all` → `enter_cleanup`（集合为空则转 `Stopping`）→ 插件逆序 `stop` → 排空任务 → dispose。广播必须早于插件 `stop` 与任务排空；即使 `enter_cleanup` 因活跃子作用域返回 `Blocked`，广播也必须已经发生（否则「任何停止路径都会唤醒」的承诺被打破）。这是硬不变量，`cancelled_fires_before_task_drain` 与 `cancelled_is_per_layer_and_fires_on_blocked_stop_commit` 守着它。
- `Waiters` 必须电平触发，且「置位 + 唤醒」与「复查 + 入列」在同一把锁下完成，否则会出现「先查后注册」的丢唤醒。
- `StopHandle::request_stop()` 只置请求位并广播，**不得**改动生命周期状态或 `is_stopping`：请求与清理是两件事。
- 任务取消分三种成因：`TaskHandle::abort()`（`Aborted(AbortReason::Owner)`）不得计入停止错误；只有排空预算耗尽（`Aborted(AbortReason::Timeout)`）才报 `TaskAborted`；宿主丢弃 future 的兜底记为 `Aborted(AbortReason::HostShutdown)`，也不上报。它们必须写进同一个 `TaskOutcome`，**不得**拆成「结局 + 旁边一个来源原子」——那会留下「结局已落定、来源标记还没写入」的窗口，把 owner 取消误报成超时。取消方必须补发完成信号。`TaskOutcome::drain_error` 是排空上报的唯一映射（`Failed` 由 `drain_report_error` 依 `report_settled` 先决），`wait()` 与排空不得各自 match 一份；`Panicked` 必须携带 `PanicInfo`（保留 panic 载荷消息）。
- **完成信号的写者必须覆盖整个任务，而不只是任务体**：`emit_notify` 跑的是用户 handler，它 panic 时 wrapper 会在写结局之前展开。`FinishOnUnwind` 是那条兜底路径，不得删除；删掉它，一次 handler panic 就能让 `stop()` / `TaskHandle::wait` 永久挂起（`panicking_task_failed_handler_does_not_hang_stop` 守着）。
- `Waiters` 的注册项按注册身份 `WaitId { index, generation }` 区分（首次 poll 时从该信号的槽位表分配），**不得**按 waker 相等判重或摘除：同一任务里的两个 `cancelled()` 等待者共享同一个 waker，按 waker 去重会让其中一个的 `Drop` 摘掉另一个的唤醒源（`dropping_one_cancelled_waiter_keeps_the_other_registered` 守着）。
- `start_failed_error` 假设「`Failed` 必有 `start_error`」：`Failed` 只由 `start_with` 在写入 `start_error` 之后设置，生命周期是普通枚举、不会被破坏成非法值，因此无根因兜底已删除。
- 任务排空发生在插件 stop 之后、dispose hooks 之前；`spawn` 在持有任务锁的临界区内检查停止态并填好 `AbortHandle`，杜绝 stop/spawn 竞态与「表里已有 cell 但 abort 句柄为空」。
- 排空期间**不得持任务表锁**：`while let Some(t) = lock().pop()` 的临时值会活到循环体结束，必须写成 `loop { let Some(..) = ... else { break } }`。
- `stop_with_timeout` 的预算为全部任务的总预算；超时任务 abort 后记 `TaskAborted`，不阻塞后续任务与 dispose。预算在**进入清理时**一次算定并随自持 future 存续，重入**不重置**——那才是真正的总上界。
- **插件名唯一性必须在 `apply` 之前占住**（`Data.declared_names`）：`apply` 是同步可重入窗口，内部可经 `Configurator::plugin` 再注册。若查重依赖 `plugin_index`（`apply` 成功后才写入），同名嵌套注册会看到陈旧表而被接受，导致同名插件装两份、依赖解析歧义。预声明必须进 undo 日志，失败/回滚时撤销。
- **注册表停机闩与登记表同锁**：`shutting_down` 与 `entries` 分属两个同步域会在「认领后、发布前」裂开窗口，让停表后仍发布出 `Running`。`open` 的认领与发布复查、`close_all` 的置位与快照都必须在同一临界区。**取出 `Runtime` 的 `stop().await` 一律由 `ClosingGuard` 持有**（`close` 与 `open` 两处回收路径共用）：非终态结局（`Blocked`）或 await 点被取消时必须把 owner 放回、名字不释放（`restore` 用 `insert`，因为 `open` 发布被拒后条目已被摘掉），可 `close` 续跑；只有终态才 `discard` 摘名并析构。缺少它会命中 debug Drop 护栏或静默跳过插件 `stop`，并留下永久 `Busy` 的空槽（`close_cancellation_keeps_scope_recoverable` / `failed_start_cancellation_keeps_owner_recoverable` 守着）。
- 注册表**不得**保留不可达的错误变体：所有锁都 `into_inner` 解毒，没有 `RegistryError::Poisoned` 的构造点。

---

## 4. 依赖与排序

- `compute_schedule` 是唯一依赖解析入口：串行拓扑序与并行分层必须来自同一张 `indegree`/`dependents` 图的一次计算。
- Builder 与 Runtime 不得各自复制一份拓扑实现。
- 可选依赖语义固定为：缺失可容忍，存在则必须按序启动。
- 环检测只发生在串行 Kahn 选点阶段（报 `PluginDependencyCycle`）；分层采用最长路径深度划分，图无环时层内容与非空性由构造保证，不得再引入独立于该图的第二套分层或空层防御分支。

---

## 5. 错误处理

- 所有框架错误使用结构化 `Error { phase, plugin, kind, source }`。
- 不新增 `to_string()` 压平的旧式错误变体。
- 单点错误用 `matches!(err.kind(), ...)`。
- 聚合错误使用 `ErrorKind::Multiple`，不要另造 `Error::Multiple` 变体。
- 嵌套插件失败时保留内层插件名；外层只补 phase，不覆盖已有 plugin。

---

## 6. 异步与取消安全

- 生命周期方法统一接收 `&Context`。
- `ready` / `dispose` 遍历时不得 `mem::take` + `await` 后放回，避免取消丢 hook。
- 事件 handler 不持有框架可变引用。

---

## 7. 测试要求

提交前必须通过：

```bash
cargo fmt --check
cargo clippy --all-targets
cargo clippy --no-default-features --all-targets
cargo test --all-targets
cargo test --no-default-features
cargo test --doc

# `cargo test --all-targets` 只**编译** examples，`main()` 不会运行，示例里的
# assert 因此不参与判定。改动示例或文档引用的示例后必须真正跑一遍：
for example in examples/*.rs; do cargo run --quiet --example "$(basename "$example" .rs)"; done
```

必须维持的测试类别：

- 插件/服务基本工作流
- `apply` 失败（返回 `Err` 或 panic 展开）回滚（含事件、订阅 ID、嵌套插件）
- c-lite 租约：Builder drop / 未 stop 的 Runtime drop / try_build 失败回传 / build 成功转移；子 `Runtime` 走完停止流程时归还租约（归还后随即进入 `Stopped`；`stopped_child_runtime_releases_parent_lease`、`stopped_child_scope_is_observable`）
- 父 stop 与子存活：活跃子阻塞父 stop 返回 `Blocked`，且父已进入 `Closing`（拒绝新工作、取消已广播）
- ScopeLease 时序探针（`lease` 字段序与「停后即从父清单摘除」）
- start-after-stop no-op
- 未 start 的 stop 只跑 dispose
- 可选依赖存在时仍按序启动
- 插件依赖环检测
- 事件串行/并行/冒泡/Bail/取消订阅
- 结构化错误 kind/phase/plugin
- 生命周期转换：`Failed` 重入返回 `StartFailed`、首错可经 `source` / `start_error()` 取回、部分启动的插件全部被回收
- `Starting`（start future 被取消）重入 `StartFailed`，已进入的插件仍可被 `stop` 回收
- `stop` 续跑：插件段 / 任务排空段 / dispose 段的 future 分别被丢弃后，重入仍完成清理且不谎报成功；自持 future 下插件 `stop` / dispose 只被调用一次
- 累积错误跨重入保留（保存在自持清理 future 内部）
- `stop_with_timeout` 预算不随重入重置
- 取消信号：未停止时保持挂起、停止后立即就绪（电平触发）；广播**早于**任务排空
- `StopHandle`：请求幂等、唤醒等待者，且不改变 `is_stopping`、不拒绝 `spawn`/`scope`
- `TaskHandle`：`abort()` 后 `wait()` 返回 `TaskOutcome::Aborted(AbortReason::Owner)`，排空不计入停止错误；`wait()` 对任务体错误（`Failed`）与 panic（`Panicked` + `PanicInfo::message`）的返回；任务 id 与 `TaskFailed` 事件一致
- panic 任务在排空时上报 `TaskFailed`
- `ScopeId` 同源：`Builder::id()` / `Context::id()` / `children()` 一致，build 后 `handle().id()` 不变
- Drop 护栏：debug 构建下「曾进入启动流程却未到 `Stopped`」即 drop 会 panic，消息区分「从未 stop」「被活跃子阻塞（列出 ids）」「stop 已提交但清理未走完」三种成因；未到 `Stopped` 就 drop 时 `Context::stopped()` 返回 `Settlement::Abandoned`，且本层随即拒绝 `scope` / `spawn`
- `Context::stopped()` 正常路径返回 `Settlement::Stopped`；`is_stopped()` 与 `children()` 不变式：已 `Stopped` 的子必已归还租约、不在父清单
- `StopOutcome`：`Blocked` 后重试续跑；`into_result` 把 `Blocked` 折叠为 `StopBlocked`，`blockers()` / `errors()` 取值

---

## 8. 公共 API 纪律

- 对外 API 变更需同步：
  - `README.md`
  - `docs/architecture.md`
  - `docs/USAGE.md`
- 新增对外行为特性需带可运行示例（或扩展示有示例）；`examples/` 被 `cargo test --all-targets` 编译验证，USAGE 对应小节需标注示例出处。
- 注意 USAGE.md 里的代码片段**不参与编译**（本 crate 目前只有 1 个 doctest）。改动文档片段时必须自行核对，否则类型不匹配、闭包生命周期推断失败这类错误会静默留存。
- 删除旧 API 前确认无内部引用，且示例与测试全部迁移。
- 不导出 `Data` / `PluginRecord` / `ScopeLease` 等内部实现类型。
