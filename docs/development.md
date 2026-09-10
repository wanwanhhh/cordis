# Cordis 开发规范

> 给维护者：改动本 crate 时必须遵守的约束、流程与测试要求。

---

## 1. 模块结构

```text
src/
  lib.rs        # 公共导出与文档
  context.rs    # Builder / Context / Runtime / Configurator / ScopeLease
  error.rs      # Error / ErrorKind / Phase
  event.rs      # Event / EventHandler / Subscription
  plugin.rs     # Plugin / Dependency / PluginDependency
  service.rs    # ServiceRegistry
```

新增能力优先放在对应模块；跨模块类型通过 crate 根导出统一公开。

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
   - 只允许：`OnceLock` + `init_lock: Mutex<()>`（懒工厂串行初始化）、`Gate`（`Mutex<GateState>` 保护生命周期状态与子作用域租约计数，配 `AtomicBool stopping` 单向闩供热路径无锁读）、`Data.children: Mutex<Vec<usize>>`（子作用域 id 注册表）、`Signal`（`AtomicBool` + `Mutex<Vec<Waiter>>`，`Waiter { token, waker }`，token 由信号自带的 `AtomicU64` 分配；用于 `Data.cancellation` 与 `TaskCell.finished`）、`Data.stop_requested: AtomicBool`、`Data.tasks`（`tokio` feature 任务注册表，元素为 `Arc<TaskCell>`，cell 内为 `OnceLock<AbortHandle>` + `Signal` + `Mutex<Option<TaskOutcome>>`）、`DynamicValue` 服务内部的 `RwLock`、以及计数器 `static NEXT_CONTEXT_ID: AtomicUsize` 与 `TaskRegistry.next_id: AtomicU64`。
   - 白名单项只做框架生命周期簿记，不构成通用可变通道；运行期可变配置仍必须通过 `DynamicValue` 暴露。

6. **租约字段序**
   - `ScopeLease` 必须是 `Runtime` 最后一个声明字段。
   - 不在 `impl Drop for Runtime` 方法体里做租约归还（由最后一个字段 `ScopeLease` 的析构完成）。
   - 必须保留 `scope_lease_release_happens_after_child_plugin_drop` 探针测试。

---

## 3. 生命周期状态机

生命周期状态**没有本地副本**，唯一真值源是 `Gate` 里 `Mutex<GateState>` 保护的 `{ lifecycle, leases }`。`Context::scope()` 的「检查未停止 + 租约加一」与 `Runtime::stop` 的「确认租约为零 + 转入 `Stopping`」在同一临界区内判定，拆成两个独立原子会在两步之间裂开竞态（父停止的同时长出子作用域）。热路径 `is_stopping()` 读 `Gate` 的 `AtomicBool` 单向闩，不拿锁。

6 个状态与完整转换表见 `docs/architecture.md` §2.1。此处只列维护者必须守住的不变量：

- 状态只由 `&mut self` 的 `start_with` / `stop_impl` 推进（`Runtime` 不 `Clone`，不存在 owner 之外的写者）。
- `Built` / `Starting` / `Running` / `Failed` 可直接进入 `Stopping`；`Stopping` 是唯一可续跑的清理进行态；`Stopped` 是终态。
- `Failed` 与 `Running` **平行**，不是终态：失败后 `stop` 仍须回收已进入启动流程的插件。`Stopped` 只在插件 `stop`、任务排空、dispose 全部走完之后落地。
- `ActiveScopes`（租约非零）**不得改动状态**，错误必须携带活跃子 id 清单；调用方清理完子 `Builder` / `Runtime` 后可重试。
- `start` 重入语义：`Running` 幂等 no-op；`Failed` 返回带 `source` 的 `StartFailed`；`Starting`（start future 被取消）返回不带 `source` 的 `StartFailed`；`Stopping` / `Stopped` 返回 `Ok`——`Ok` 只表示「不再需要启动」，不等于本次调用完成了启动。
- 未 `start` 的 `stop` 只跑 dispose hooks，不调用插件 stop（`StopProgress.plugins` 初值为 0）。
- **清理必须可续跑**：`StopProgress` 两个游标与累积错误（`Runtime.stop_errors`）只在对应 `await` 返回后推进/写回，并跨重入保留。任何「先推进游标再 `await`」的写法都会重新引入丢项或假成功。
- 任务排空同样 at-least-once，但**不把在飞句柄挂在 `Runtime` 上**：表元素是与 `TaskHandle` 共享的 `Arc<TaskCell>`，「先 await 完成信号、再取出」使被丢弃的 future 不摘走任何表项，重入重新处理同一个 cell（完成信号电平触发，已结束的立即返回）。「取出—判断—上报」之间不得插入 `await`，否则取消会落进中间造成漏报——这条不变量同时承担去重：被丢弃的 future 只可能停在 await 上，因此不存在「已上报的 cell 又回到表里」的情形，不需要额外的去重标志。
- **`spawn` 的剪除是摊销的，且必须保住「排空仍需上报」的结局**：只在表长跨过 `compact_at` 阈值时做一次 `retain`，随后按实际长度翻倍，n 次 `spawn` 的累计代价是 O(n)。按 `is_finished()` 盲删会让一条 panic 因为它之后又有人 `spawn` 过而被静默丢掉。`TaskCell::needs_drain_report` 是这条判断的唯一出处（覆盖 `Panicked` 与 `AbortedByTimeout`）。
- **进入关闭流程立刻广播取消**（`Signal::fire`），顺序是：转入 `Stopping`（置 `stopping` 闩）→ 广播 → 插件逆序 `stop` → 排空任务 → dispose。广播晚于排空会让「优雅收尾」失去窗口；这是硬不变量，`cancelled_fires_before_task_drain` 守着它。
- `Signal` 必须电平触发，且「置位 + 唤醒」与「复查 + 入列」在同一把锁下完成，否则会出现「先查后注册」的丢唤醒。
- `StopHandle::request_stop()` 只置请求位并广播，**不得**改动生命周期状态或 `is_stopping`：请求与清理是两件事。
- 任务取消分两个语义：`TaskHandle::abort()`（owner 意图）不得计入停止错误；只有排空预算耗尽才报 `TaskAborted`。两者必须写进同一个 `TaskOutcome`（`AbortedByOwner` / `AbortedByTimeout`），**不得**拆成「结局 + 旁边一个来源原子」——那会留下「结局已落定、来源标记还没写入」的窗口，把 owner 取消误报成超时。取消方必须补发完成信号——被 abort 的任务不会再执行收尾代码。
- **完成信号的写者必须覆盖整个任务，而不只是任务体**：`emit_notify` 跑的是用户 handler，它 panic 时 wrapper 会在写结局之前展开。`FinishOnUnwind` 是那条兜底路径，不得删除；删掉它，一次 handler panic 就能让 `stop()` / `TaskHandle::wait` 永久挂起（`panicking_task_failed_handler_does_not_hang_stop` 守着）。
- `Signal` 的注册项按 token 区分（token 由该信号自带的 `AtomicU64` 分配，只需信号内唯一），**不得**按 waker 相等判重或摘除：同一任务里的两个 `cancelled()` 等待者共享同一个 waker，按 waker 去重会让其中一个的 `Drop` 摘掉另一个的唤醒源（`dropping_one_cancelled_waiter_keeps_the_other_registered` 守着）。
- `start_failed_error` 假设「`Failed` 必有 `start_error`」：`Failed` 只由 `start_with` 在写入 `start_error` 之后设置，生命周期是普通枚举、不会被破坏成非法值，因此无根因兜底已删除。
- 任务排空发生在插件 stop 之后、dispose hooks 之前；`spawn` 在持有任务锁的临界区内检查停止态并填好 `AbortHandle`，杜绝 stop/spawn 竞态与「表里已有 cell 但 abort 句柄为空」。
- 排空期间**不得持任务表锁**：`while let Some(t) = lock().pop()` 的临时值会活到循环体结束，必须写成 `loop { let Some(..) = ... else { break } }`。
- `stop_with_timeout` 的预算为全部任务的总预算；超时任务 abort 后记 `TaskAborted`，不阻塞后续任务与 dispose。预算按每次调用重算，重入会拿到新预算。

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
- c-lite 租约四路：Builder drop / Runtime drop / try_build 失败回传 / build 成功转移
- 父 stop 与子 Runtime 存活
- ScopeLease 时序探针
- start-after-stop no-op
- 未 start 的 stop 只跑 dispose
- 可选依赖存在时仍按序启动
- 插件依赖环检测
- 事件串行/并行/冒泡/Bail/取消订阅
- 结构化错误 kind/phase/plugin
- 生命周期转换：`Failed` 重入返回 `StartFailed`、首错可经 `source` / `start_error()` 取回、部分启动的插件全部被回收
- `Starting`（start future 被取消）重入 `StartFailed`，已进入的插件仍可被 `stop` 回收
- `stop` 续跑：插件段 / 任务排空段 / dispose 段的 future 分别被丢弃后，重入仍完成清理且不谎报成功
- 累积错误跨重入保留（`stop_errors`）
- 取消信号：未停止时保持挂起、停止后立即就绪（电平触发）；广播**早于**任务排空
- `StopHandle`：请求幂等、唤醒等待者，且不改变 `is_stopping`、不拒绝 `spawn`/`scope`
- `TaskHandle`：`abort()` 后 `wait()` 返回 `Ok` 且排空不计入停止错误；`wait()` 对任务体错误与 panic 的返回；任务 id 与 `TaskFailed` 事件一致
- panic 任务在排空时上报 `TaskFailed`
- `id()` 与 `children()` 同源：Builder 阶段即可登记，build 后 `handle().id()` 不变
- Drop 护栏：debug 构建下「曾进入启动流程却未 `stop`」即 drop 会 panic

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
