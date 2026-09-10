# Cordis 底层架构

> 当前实现权威说明。面向使用者请配合 [USAGE.md](USAGE.md) 阅读。

---

## 1. 核心模型

Cordis 使用三段式上下文模型，把构建期、运行期和生命周期控制拆成三个类型：

| 类型 | 角色 | 可变性 | Clone |
|---|---|---|---|
| `Builder` | 装配期注册服务、插件、hook、事件 | 独占 `&mut` | 否 |
| `Context` | 只读数据句柄 | 只读 | 是 |
| `Runtime` | 生命周期唯一所有者 | `&mut self` 控制启停 | 否 |

```text
Builder::new()
   │ 注册 provide / plugin / on_ready / on_dispose / on
   ▼
Builder::build()          // 唯一冻结点：Arc::new(Data)
   ▼
Runtime                   // start / stop / handle()
   │
   ▼
Context                   // require / require_all_recursive / contains / has_plugin / emit / emit_notify / scope / is_stopping / children / spawn
```

冻结点之后不存在任何框架可见的 `&mut Data` 路径。`Context` 只读且可跨线程共享。

---

## 2. 生命周期状态

### 2.1 `Runtime`

- `Runtime::start()`：默认分层并行启动。
- `Runtime::start_serial()`：保留旧串行总序语义。
- `Runtime::stop()`：停止已启动插件的逆序清理，排空本层 `Context::spawn` 注册的后台任务（不设超时），最后执行 dispose hooks。
- `Runtime::stop_with_timeout(Duration)`：任务排空共享该预算，超时后强制取消未完成任务并计入聚合错误。预算大到 `Instant` 无法表示时按「不设超时」处理（不 panic、也不报错）。
- `Runtime::stop_handle() -> StopHandle`：取一个可 `Clone` 的停止**请求**句柄（见 3.4.1），关闭本身仍由 `Runtime` 执行。
- start-after-stop 是 no-op。
- 未 `start` 就 `stop` 时，只执行 dispose hooks，不调用插件自身的 `stop`。

#### 生命周期状态机

生命周期状态**没有本地副本**，唯一真值源是 `Data.state` 的同一个原子字：低 60 位是子作用域租约计数，高 4 位是状态（二者同字的原因见 §3.2）。

```text
Built ──start──▶ Starting ──成功──▶ Running
                    │                  │
                    └───失败──▶ Failed │
                                       ▼
        Built / Starting / Running / Failed ──▶ Stopping ──▶ Stopped（终态）
```

| 状态 | 含义 | 重入 `start()` | `stop()` |
|---|---|---|---|
| `Built` | 已构建，未尝试启动 | 正常启动 | 转 `Stopping`，**不**回收插件 |
| `Starting` | 启动进行中，或被中断 | `Err(StartFailed)` | 转 `Stopping`，回收已记入插件 |
| `Running` | 启动成功 | no-op | 转 `Stopping` |
| `Failed` | 启动失败，插件待回收 | `Err(StartFailed)` | 转 `Stopping`，回收已记入插件 |
| `Stopping` | 清理进行中 | no-op | **续跑**未完成的清理 |
| `Stopped` | 终态 | no-op | 幂等 `Ok` |

- `Failed` 与 `Running` **平行**，不是终态：失败后 `stop` 仍须回收已启动插件，`Stopped` 才是终态。
- 调度计算失败（依赖图问题）停在 `Built`：一个插件都没进入启动流程，`stop` 无需回收。由于 `build()` 已校验依赖图，该分支是防御性的。
- **中途丢弃 start future 会停在 `Starting`**：部分插件已启动、流程未完成。重入 `start()` 返回 `ErrorKind::StartFailed`（不带 `source`）而非 `Ok`。不续跑——部分启动的状态不该被「接着启动」，调用方应 `stop` 回收后重建。
- **`stop` 可续跑**：插件与 dispose 的进度记在 `StopProgress` 游标里，三者都只在对应 `await` 真正返回后才推进。中途丢弃 stop future 会停在 `Stopping`，下次 `stop()` 从断点继续；正在处理的项会被重试，不漏项也不报成功。代价是插件 `stop` 需要能吃下一次中断后重入。
- 任务排空同样 at-least-once，但**不再需要把在飞句柄挂在 `Runtime` 上**：任务表的元素是与 `TaskHandle` 共享的 `Arc<TaskCell>`，「先 await 完成信号、再取出」使得被丢弃的 future 不会摘走任何表项，重入重新处理同一个 cell（见 3.5）。旧实现必须把 `JoinHandle` 存进 `Runtime.draining`，否则局部句柄析构会 detach 任务、重入误判「已排空」。
- **进入关闭流程会立刻广播取消信号**，且早于插件 `stop` 与任务排空（见 3.4.1）：等待 `Context::cancelled()` 的长驻任务因此能自己收尾，而不是被排空超时踢掉。
- 累积的清理错误存在 `Runtime.stop_errors`，跨重入保留，进入 `Stopped` 时一并交出，不会因中断丢掉早先一轮的失败。
- `stop_with_timeout` 的预算**每次调用重新计算**：中断后重入会拿到新的完整预算，即反复中断可以延长总时长。
- 进入 `Stopped` 是在插件 `stop`、任务排空、dispose **全部走完之后**。
- **`start()` 返回 `Ok` 不等于「本次调用完成了启动」**：它只表示运行时不再需要启动——`Running` 是幂等 no-op，`Stopping`/`Stopped` 是停止后的 no-op。只有启动尝试本身出了问题才返回 `Err`：`Failed`（已失败，根因在 `source`）与 `Starting`（被中断，无 `source`）。

### 2.2 重复调用

- 重复 `start` 安全 no-op（`Running` 态）。
- **`start` 失败后重复 `start` 不是 no-op**：返回 `ErrorKind::StartFailed`。旧实现把「失败」记成「已启动」，重入会静默返回 `Ok(())` 且一个插件都不启动。
- **`stop` 在 `Stopping` 态重入不是 no-op**：继续未完成的清理（见上）。
- 已 `Stopped` 的 `Runtime` 再次 `stop` 安全 no-op。
- `stop` 失败会继续清理，并把错误聚合为 `ErrorKind::Multiple`；错误不阻止状态进入 `Stopped`。

---

## 3. Scope 与 c-lite 租约

### 3.1 scope 创建

```rust
let child: Builder = ctx.scope()?;
```

`Context::scope()` 在父 `Data.state` 上原子递增 child 计数。父 `Runtime` 已停止时返回 `ErrorKind::Stopping`；计数达到上限（2^60 - 1，高位留给生命周期状态）时返回 `ErrorKind::TooManyScopes`。

### 3.2 租约

- 租约由 `ScopeLease { parent, child_id }` 持有父 `Data` 强引用与本层 `context_id`。
- 租约在 `Context::scope()` 时交给子 `Builder`，`build()` 时 move 给子 `Runtime`。
- 只在 `Runtime` / `Builder` 的字段析构阶段释放，绝不在 `stop()` 成功路径释放。
- `Runtime` 中 `_lease` 必须是最后一个字段，保证插件字段先析构、再归还父计数。
- `ScopeLease::drop` 先把本层 id 从父注册表摘除，再 `fetch_sub(1, SeqCst)` 归还父计数。
- **计数与生命周期状态共用父 `Data.state` 的同一个 u64**（低 60 位计数、高 4 位状态）。这不是编码风格问题：`Context::scope()` 的「确认未停止 + 计数加一」与 `Runtime::stop` 的「确认计数为零 + 转入 `Stopping`」都必须是一次 CAS，拆成两个字段会在两步之间裂开竞态——父停止的同时长出子作用域。

### 3.3 父 stop 与子存活

- 父 `Runtime::stop()` 只接受本层没有活跃子 Runtime/Builder，否则返回 `ErrorKind::ActiveScopes { count, ids }`。
- `ids` 即当前活跃子作用域的 `context_id` 清单（与 `Context::children()` 一致），供管理器定位阻塞方。
- `ActiveScopes` 失败时不会改动状态（停在原状态），调用方停掉/丢弃子级后可重试。
- 仅子 `Context` 句柄存活不阻塞父 stop。
- 子 `Runtime` 即使已 `stop` 但未 `drop`，仍占租约并阻塞父 stop。

### 3.4 停止可观测性与取消信号

- `Context::is_stopping()`：本层是否已进入停止流程（`Stopping` / `Stopped`）。
- `Context::children()`：本层活跃子作用域 id 清单（含未 build 的子 Builder 与未 drop 的子 Runtime）。
- `Context::id()`：本层 `context_id`；子作用域的 id 与父 `children()` 里列出的值同源，`Builder::id()` 在 build 前就能取到。
- `Context::parent()`：父级句柄，用于向上遍历。
- `Context::cancelled()`：可等待、**电平触发**的停止信号。

### 3.4.1 取消信号（`Signal`）

单个 `Data` 持有一个 `Signal`：`AtomicBool` + `Mutex<Vec<Waiter>>`（`Waiter { token, waker }`）。两处触发合用一个原语——`Data.cancellation`（停止）与 `TaskCell.finished`（任务结局）。

- **必须电平触发**：`poll` 先查标志、已置位即 `Ready`，否则入列；置位与唤醒在锁内完成，`poll` 的「复查 + 入列」也在同一把锁下，因此不存在「先查后注册」的丢唤醒窗口。边沿触发做不到这点——停止可能在任何等待者注册之前就已发生。
- **注册项按「哪个 future 注册的」区分，而不是按 waker 相等**：`Signal` 里每个条目是 `(token, Waker)`，`poll` 按 token 更新/新增（同一个 future 被反复 poll 只更新 waker，所以列表长度 = 活着的等待 future 数）。按 waker 判重是个陷阱——同一任务里的两个 `cancelled()` 等待者由 executor 用同一个 waker 轮询，会共享一条记录，其中一个 future 被丢弃时就把另一个仍然存活的等待者的唤醒源一起摘掉，停止时无人被唤醒。
- **等待者会在放弃等待时主动摘除自己**：`cancelled()` 返回的 future 在 `Drop` 里按自己的 token 调 `Signal::unregister`。等待者常写成 `select! { _ = ctx.cancelled() => …, _ = work => … }`，先等到 `work` 就不再关心停止信号；不摘除的话，长生命周期作用域的等待者列表会随这类任务单调增长。
- 排空与 `TaskHandle::wait` 走 `Signal::wait`，每次调用分配一个 token 且不注销：它们的生命周期绑定在短命的 `TaskCell` 上，残项在 `fire` 时整体清空，不构成无界增长。
- 不依赖 tokio：`poll` 只用 `std::task::{Poll, Waker}`，`cancelled()` 可在任意 executor 上等待。
- **触发时机是硬不变量**：`enter_stopping()` 的 CAS 成功后立刻广播，必须早于插件 `stop` 与任务排空。否则长驻任务收到信号时排空已经在等它，「优雅收尾」就没有窗口。
- `poll_cancelled` 额外复查生命周期状态——这是一个**免锁快路径**（已进入停止的等待者不必抢 `Signal` 的锁），不是正确性所必需：`fire` 总会唤醒已注册的等待者，而 CAS 与 `fire` 之间到达的等待者会在 `fire` 的锁内被 drain。
- `StopHandle::request_stop()` 是第二个触发点：置请求位（`stop_requested`，与生命周期状态**分开**）后广播。请求不改变 `is_stopping`，也不拒绝 `scope` / `spawn`——请求不是清理。

### 3.5 后台任务（`tokio` feature）

`Context::spawn(fut)` 把任务登记到本层 `Data` 的任务注册表并返回 `TaskHandle`：

- 要求当前线程处于 tokio runtime 上下文，否则返回 `ErrorKind::NoTaskRuntime`。
- 任务输出必须是 `Result<(), Error>`；返回 `Err` 时以 `emit_notify` 发出 `TaskFailed` 事件（沿父链冒泡）。
- 注册表在每次 `spawn` 时惰性剪除已结束的 cell（内存边界），但**保住「排空仍需上报」的结局**（`needs_drain_report` 覆盖 `Panicked` 与 `AbortedByTimeout`；其中后者总在同一轮被取出，实际能驻留到下次 `spawn` 的只有 panic）：按 `is_finished` 盲删会让一条 panic 因为它之后又有人 `spawn` 过而被静默丢掉，`stop` 反而报成功。`task_count()` 只统计不剪除，同理——剪除会销毁尚未上报的结局。本层进入停止后 `spawn` 返回 `ErrorKind::Stopping`。
- 注册表元素是 `Arc<TaskCell>`，与 `TaskHandle` 共享：cell 持 `AbortHandle` + 完成信号 + 结局。**不保存 `JoinHandle`**——旧实现为此不得不在 `Runtime` 上挂 `draining` 字段来防「stop future 被丢弃时句柄析构 detach 任务」，现在被丢弃的 future 不会从表里移除任何东西，重入直接重新处理同一个 cell（完成信号电平触发，已结束的立即返回）。
- **完成信号必须有写者，而且要覆盖整个任务**：`catch_unwind` 只兜住任务体；结局的**上报路径**（`emit_notify` 会跑用户 handler）与任务体同在一个任务里，它 panic 时 wrapper 会在写结局之前展开——没有兜底则完成信号永不触发，`stop()` 与 `TaskHandle::wait` 一起挂死，`stop_with_timeout` 还会把 panic 误报成 `TaskAborted`。因此另有 `FinishOnUnwind` 守卫按成因兜底：正在展开（真 panic）记 `Panicked`，宿主直接把 future 丢掉（runtime 关闭、未记录的 abort）记取消——后者若也记 panic，会让一次 `stop()` 凭空多出 `TaskFailed` 假失败。两种情况都必须触发完成信号，否则排空会等一个永远不来的信号。panic hook 照常输出。
- 剪除（`spawn` 里的 `retain`）只回收「不需要排空上报」的结局，`needs_drain_report` 是唯一出处：`Panicked`（无事件出口）与 `AbortedByTimeout`（只有排空上报）必须留在表里。代价是**未被 `stop` 清理前，表里会累积 panic 过的 cell**（每个很小）；上游应当在监控里用 `TaskFailed` 事件而不是依赖 `stop` 报错来发现任务 panic。
- `Context::task_count()` 统计尚未落定结局的任务，含 `stop` 正在排空的那一个。
- `stop` 顺序：CAS 转入 `Stopping` 并广播取消 → 插件逆序 `stop` → 排空任务（`stop_with_timeout` 预算内等待，超时 `abort` 并上报 `ErrorKind::TaskAborted`）→ dispose hooks。
- **取消来源写进结局本身**：`TaskOutcome` 区分 `AbortedByOwner` 与 `AbortedByTimeout`。owner 通过 `TaskHandle::abort()` 的取消不计入停止错误（否则「我让你停」会被报成失败），只有排空预算耗尽的取消才上报 `TaskAborted`。两者用同一个 `Mutex<Option<TaskOutcome>>` 的「先写者胜」落定，因此排空只读一次结局就能正确归类——分成「结局 + 旁边一个来源原子」会留下「结局已是取消、来源标记还没写入」的窗口，把 owner 取消误报成超时。取消方负责补发完成信号：被 abort 的任务不会再执行收尾代码。owner `abort` 与排空超时**真正同刻**并发时按「先写者胜」归类——先落定的一方定义这次取消的性质，这是可接受的平局语义，但值得知道它存在。
- 排空「先 await 结局、再取出」：await 被取消时不摘表，重入重新看到它；「取出—判断—上报」之间没有 await 点，因此相对取消是原子的——被丢弃的 stop future 只会停在 await 上，不会落在中间造成漏报或重报，无需额外的去重标志。
- abort 尽力而为：卡在阻塞调用里的任务要等其让出执行权才会真正取消。
- 不经 `stop()` 直接 drop `Runtime` 不排空任务（`Drop` 不做异步清理），未完成任务随注册表丢弃而脱离框架管理。

---

## 4. 服务

- `ServiceRegistry` 支持普通服务、懒加载工厂、集合服务。
- `Builder` 阶段可 `provide` / `provide_factory` / `provide_collect` / `provide_dynamic` / `require_mut`。
- `Context` 阶段只读：`require` / `try_require` / `require_all` / `require_all_recursive` / `require_dynamic` / `contains`；观测与生命周期句柄为 `is_stopping` / `cancelled` / `children` / `parent` / `id`，任务侧为 `spawn`（返回 `TaskHandle`）/ `task_count`。
- `contains` 做完整存在性检查：普通服务 / 工厂 / 集合，任一层级存在即为真。`Dependency` 校验走另一套单例语义（`contains_type`），集合服务不满足单例依赖。
- `require_all` 只查本层，不沿父链冒泡；`require_all_recursive` 会依次汇总本层和所有父层集合。
- `provide_dynamic` 注册的是 `Arc<DynamicValue<T>>`，运行期可通过 `require_dynamic` 获得共享句柄并修改内部值。
- 懒工厂以 `Mutex` 串行化初始化：成功路径工厂至多执行一次，所有并发访问者拿到同一首个实例；失败不缓存，保留可重试语义。初始化锁跨工厂调用持有，工厂应为非阻塞纯计算。

---

## 5. 插件

### 5.1 `Plugin` trait

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn priority(&self) -> i32;
    fn scope(&self) -> PluginScope;
    fn dependencies(&self) -> Vec<Dependency>;
    fn plugin_dependencies(&self) -> Vec<PluginDependency>;
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error>;
    async fn start(&self, ctx: &Context) -> Result<(), Error>;
    async fn stop(&self, ctx: &Context) -> Result<(), Error>;
}
```

- `scope` 默认 `Any`，在 `Builder::plugin()` 注册阶段校验；`Root` / `Child` 插件装在错误层级返回 `PluginScopeMismatch`。
- `dependencies` / `plugin_dependencies` 在注册时求值并缓存进 `PluginRecord`。
- `apply` 收到窄接口 `Configurator`，不能修改既有服务，也不能执行 start/stop/emit/scope。
- `start` / `stop` 只收 `&Context`，生命周期方法不存在框架级可变别名。

### 5.2 依赖与顺序

- 服务依赖：`Dependency::of::<T>()`。
- 插件间依赖：`PluginDependency::of("plugin-name")`。
- 可选依赖只放宽“必须存在”，不改变“存在时必须按顺序启动”的语义。
- `plugin_dependencies` 是唯一启动顺序契约。
- `priority` 参与 `start()` 与 `start_serial()` 共用的拓扑选点（影响同层内次序与逆序停止次序）；但分层并行 `start()` 的层划分只由依赖边决定，同层内不保证 priority/注册序总序。

### 5.3 启动分层并行

- 默认 `start()` 对拓扑同层插件使用 `join_all` 并发启动。
- `start_serial()` 保留旧串行总序。
- 同层插件之间不存在 priority/注册序的总序保证。

### 5.4 回滚

单个 `plugin()` 是事务性的：

- `apply` 失败时回滚本次新增的服务、插件、hooks、事件订阅；
- 集合服务按注册表快照记录的各类型长度精确截断，失败插件追加的集合元素不会泄漏；
- 嵌套插件注册失败会保留内层插件名，同时外层副作用一并回滚；
- `plugin_with_config` 会连同配置服务一起回滚。

---

## 6. 生命周期 Hook

- `on_ready`：`start()` 成功启动所有插件后执行。
- `on_dispose`：`stop()` 时始终执行。
- `LifecycleHook::call(&mut self, ctx: &Context)`。
- ready/dispose 不得在 `await` 期间 `mem::take` 出 Runtime，避免 future 取消后丢失 hook。

---

## 7. 事件系统

- 注册在 `Builder` 阶段：`on::<E, _>(handler)`。
- 触发在 `Context` 阶段：`ctx.emit(event)` / `ctx.emit_parallel(event)` / `ctx.emit_notify(event)` / `ctx.emit_notify_parallel(event)`。
- 事件沿父链冒泡。
- handler 收到的是其注册层级的 `Context`。
- `emit_parallel` 错误聚合使用 `Phase::Event` + `ErrorKind::Multiple`。
- `emit_notify` / `emit_notify_parallel` 不抛错，而是返回收集到的 handler 错误；handler 错误不阻断后续 handler 与父链冒泡，但 `Bail` 仍会停止冒泡。

---

## 8. 错误模型

```rust
pub struct Error {
    pub phase: Phase,
    pub plugin: Option<&'static str>,
    pub kind: ErrorKind,
    // source: Option<Arc<dyn Error + Send + Sync + 'static>>（私有；用 source() 访问）
}
```

```rust
pub enum Phase {
    Apply, Verify, Build, Start, Ready, Stop, Dispose, Event,
}

pub enum ErrorKind {
    ServiceNotFound(String),
    ServiceAlreadyRegistered(String),
    PluginNameAlreadyRegistered(String),
    PluginDependencyNotFound(String),
    PluginDependencyCycle,
    PluginScopeMismatch {
        plugin_name: String,
        expected: PluginScope,
        actual: PluginScope,
    },
    ActiveScopes {
        count: u64,
        ids: Vec<usize>,
    },
    Stopping,
    TooManyScopes,
    SubscriptionNotFound,
    NoTaskRuntime,
    TaskFailed { task_id: u64 },
    TaskAborted { task_id: u64 },
    StartFailed,
    Other,
    Multiple(Vec<Error>),
}
```

- 底层错误通过 `source` 保留错误链，不 `to_string()` 压平。
- `source` 字段内部是 `Arc<dyn Error + Send + Sync>`，`with_source` 的公开签名与 `source()` 的返回类型不变。`Error` 与 `ErrorKind` 因此可 `Clone`：克隆会深拷贝 `Multiple` 的子错误树，但每个子错误的 `source` 只增引用计数，因此两份共享同一条底层链。这是 `Runtime` 能同时把首错交给调用方又保留一份的前提；`Error` 仍不实现 `PartialEq`。
- **契约**：凡经 `with_source` 附加的来源，穿过 `into_phase`、`Multiple` 聚合和 start/stop 的阶段标注后都不被丢弃、不被压平，可按 `source()` 逐层 `downcast_ref` 取回。
- 单点错误用 `matches!(err.kind(), ...)` 判断。
- `Stopping` 同时覆盖 `scope()` 与 `spawn()` 的停止后拒绝。

---

## 9. 非目标 / 边界

- 不支持插件热加载。
- 不支持运行时动态注册服务；运行期可变依赖服务自身同步。
- 无 `Drop` 自动异步 `stop()`；必须显式 `stop()`。
- `Drop` 只做护栏不做清理：debug 构建下若「曾进入启动流程却未到 `Stopped`」会 `debug_assert!` 硬失败，release 下静默——正确性不应依赖这条诊断。
- c-lite 不防御“不 `stop` 直接 drop 子 Runtime”导致的深层异步清理缺失。
- 任务取消基于 tokio 协作式 abort，尽力而为；卡在同步阻塞调用中的任务无法被强制杀死。
- 不提供父 stop 自动传播/强制停止子 scope；父 stop 仍被活跃子租约阻塞，但 `ids` 可观测。取消信号是**每层各自**的：`cancelled()` 只在本层进入停止或收到本层请求时完成，父层不代子层广播。这不构成缺口——父 stop 本就被活跃子租约挡住，子层只能由它自己的 `stop` 收口，那一刻它的信号就会触发。
- `StopHandle::request_stop()` 只广播请求，不触发清理，也不自动把本层推进到 `Stopping`；关闭始终由持有 `Runtime` 的 owner 执行。
- 不提供运行期事件退订（`Context::off`）。handler 表在 `build` 时冻结，好让 emit 走无锁查表；为运行期可变性给它加同步等于拿热路径换低频能力。需要按请求动态分发的场景，请在应用层维护自己的分发表。
