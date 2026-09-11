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
Context                   // require / require_all_recursive / contains / has_plugin / emit / emit_notify / notify / notify_stats / on_closing / scope / is_stopping / is_stopped / cancelled / stopped / children / spawn
```

冻结点之后不存在任何框架可见的 `&mut Data` 路径。`Context` 只读且可跨线程共享。

---

## 2. 生命周期状态

### 2.1 `Runtime`

- `Runtime::start()`：默认分层并行启动。
- `Runtime::start_serial()`：保留旧串行总序语义。
- `Runtime::stop() -> StopOutcome`：**提交**停止意图（不可逆，见状态机），逆序停止已启动插件、排空本层 `Context::spawn` 注册的后台任务（不设超时），最后执行 dispose hooks。
- `Runtime::stop_with_timeout(Duration) -> StopOutcome`：任务排空共享该预算，超时后强制取消未完成任务并计入 `Stopped { errors }`。预算大到 `Instant` 无法表示时按「不设超时」处理（不 panic、也不报错）。预算在**进入清理时**一次算定并随清理 future 存续，重入不重置。
- `Runtime::stop_handle() -> StopHandle`：取一个可 `Clone` 的停止**请求**句柄（见 3.4.1），关闭本身仍由 `Runtime` 执行。
- start-after-stop 是 no-op；已提交停止（`Closing`）也拒绝启动。
- 未 `start` 就 `stop` 时，只执行 dispose hooks，不调用插件自身的 `stop`。

#### 生命周期状态机

生命周期状态**没有本地副本**，唯一真值源是 `ScopeCore` 中 `Mutex<ScopeInner>` 保护的 `{ lifecycle, requested, abandoned, children }`（状态与租约需在同一临界区判定的原因见 §3.2）。热路径 `is_stopping()` / `is_stopped()` 读 `ScopeCore` 的派生 tag（`SHUTTING` / `STOPPED` 位），单次原子读、不拿锁。

```text
Built ──start──▶ Starting ──成功──▶ Running
                    │                  │
                    └───失败──▶ Failed │
                                       ▼
Built / Starting / Running / Failed ──stop──▶ Closing ──租约归零──▶ Stopping ──▶ Stopped（终态）
                                                 │
                                                 └─仍有活跃子作用域─▶ 返回 Blocked（留在 Closing，可重试）
```

| 状态 | 含义 | 重入 `start()` | `stop()` |
|---|---|---|---|
| `Built` | 已构建，未尝试启动 | 正常启动 | 提交 → `Closing`，**不**回收插件 |
| `Starting` | 启动进行中，或被中断 | `Err(StartFailed)` | 提交 → `Closing`，回收已记入插件 |
| `Running` | 启动成功 | no-op | 提交 → `Closing` |
| `Failed` | 启动失败，插件待回收 | `Err(StartFailed)` | 提交 → `Closing`，回收已记入插件 |
| `Closing` | 已提交停止：拒绝 `scope`/`spawn`，取消已广播；**等子租约归零，清理未开始** | no-op | 重查租约；归零则转 `Stopping`，否则返回 `Blocked` |
| `Stopping` | 租约已归零，清理进行中（自持 future） | no-op | **续跑**同一个清理 future |
| `Stopped` | 终态 | no-op | 幂等 `Stopped { errors: [] }` |

- `Failed` 与 `Running` **平行**，不是终态：失败后 `stop` 仍须回收已启动插件，`Stopped` 才是终态。
- 调度在 `build()`（`validate()`）算好后随 `Runtime` 保存，`start` 不再有调度失败分支：依赖图问题在 `build()` 就以 `Err` 返回，根本产生不出 `Runtime`。
- **中途丢弃 start future 会停在 `Starting`**：部分插件已启动、流程未完成。重入 `start()` 返回 `ErrorKind::StartFailed`（不带 `source`）而非 `Ok`。不续跑——部分启动的状态不该被「接着启动」，调用方应 `stop` 回收后重建。
- **`stop` 提交不可逆**：owner 一旦调用 `stop()`，本层就进入 `Closing`，永久拒绝新工作并广播取消；即使被活跃子作用域挡住（返回 `StopOutcome::Blocked`），也不会退回 `Running`。这是 `Waiters` 单向性的直接后果——取消信号一经触发就永远就绪，允许状态回退只会制造永久不一致。等阻塞方回收后重试 `stop()` 即续跑。
- **清理执行体是 `Runtime` 自持的 future**（`Runtime.stop_future`，进入 `Stopping` 时构造一次）：调用方 `await` 的只是对它的 poll，丢弃外层 await 不会中断清理，重入继续 poll 同一个 future。因此每个插件 `stop` 与 dispose hook **只被调用一次**，插件不需要可重入。旧实现用 `StopProgress` 游标从中断项**开头重放**，因此要求插件 `stop` 幂等；自持 future 停在原 await 点，不再重放。
- **timeout 预算随清理 future 存续**：进入 `Stopping` 时一次算定，重入不重置（那才是真正的总上界）。反过来，若已带着「不设超时」进入清理，之后再用 `stop_with_timeout` 不会收紧。
- 累积的清理错误保存在清理 future 内部，跨重入保留，进入 `Stopped` 时随 `StopOutcome::Stopped { errors }` 交出。
- **进入 `Closing` 即广播取消信号**，早于插件 `stop` 与任务排空（见 3.4.1）：等待 `Context::cancelled()` 的长驻任务因此能自己收尾，而不是被排空超时踢掉。
- **`start()` 返回 `Ok` 不等于「本次调用完成了启动」**：它只表示运行时不再需要启动——`Running` 是幂等 no-op，`Closing`/`Stopping`/`Stopped` 是停止后的 no-op。只有启动尝试本身出了问题才返回 `Err`：`Failed`（已失败，根因在 `source`）与 `Starting`（被中断，无 `source`）。

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

`Context::scope()` 在父 `ScopeCore` 的临界区内登记一个子作用域租约，并把子 `ScopeId` 记入父的阻塞清单。父 `Runtime` 已提交关闭（`Closing` / `Stopping` / `Stopped`）时返回 `ErrorKind::Stopping`；租约是 `usize`，不存在可实际触及的上限。

### 3.2 租约

- 租约由 `ScopeLease { parent, child_id }` 持有父 `Data` 强引用与本层 `ScopeId`。
- 租约在 `Context::scope()` 时交给子 `Builder`，`build()` 时 move 给子 `Runtime`。
- **子 `Runtime` 走完停止流程时提前归还租约（归还后随即进入 `Stopped`）**（插件、任务、dispose 都已回收）；`Builder` 与「未 stop 就 drop」的 `Runtime` 仍在字段析构阶段归还。
- `Runtime.lease` 必须是最后一个字段：未 stop 的析构路径上要保证插件字段先析构、再归还父计数。
- `ScopeCore::release_lease(child_id)` 在同一临界区内 `-= 1` 并从阻塞清单摘除该 id。
- **计数、生命周期状态与阻塞清单（`children`）共用父 `ScopeCore` 的同一把锁**。这不是编码风格问题：`Context::scope()` 的「确认未停止 + 租约加一 + 登记 id」与 `Runtime::stop` 的「确认租约为零 + 转入 `Stopping`」必须在同一临界区判定。拆成两个独立原子会在两步之间裂开竞态——父停止的同时长出子作用域，或报告的阻塞清单与实际租约不一致（早期实现每次现取两个快照，`count` 与 `ids` 会互相矛盾）。`scope` / `stop` 都是低频路径，热路径仍是单原子读。

### 3.3 父 stop 与子存活

- 父 `Runtime::stop()` 若有活跃子 Runtime/Builder，返回 `StopOutcome::Blocked(Blockers)`；`Blockers::ids()` 即当前**尚未归还租约**的子作用域 `ScopeId` 清单（与 `Context::children()` 同源、同临界区取出），供管理器定位阻塞方。不变式单向：列出的必未 `Stopped`，不在清单即租约已归还。
- 被阻塞时父**已**提交停止（进入 `Closing`）：拒绝新工作、取消已广播，但清理未开始。调用方回收子级后重试 `stop()` 即续跑。
- 仅子 `Context` 句柄存活不阻塞父 stop。
- 子 `Runtime` 走完停止流程时归还租约（归还后随即进入 `Stopped`），**不再阻塞父 stop**（即使尚未 `drop`）；未 stop 就被 drop 的子仍占租约。

### 3.4 停止可观测性与取消信号

- `Context::is_stopping()`：本层是否已提交关闭（`Closing` / `Stopping` / `Stopped`），即不再接受新工作。**不**表示清理已开始或已停稳。
- `Context::is_stopped()`：本层是否已进入 `Stopped` 终态。
- `Context::children()`：本层**尚未归还租约**的子作用域 `ScopeId` 清单（含未 build 的子 Builder）。不变式是单向的：列出的必未 `Stopped`；不在清单 ⇒ 租约已归还。
- `Context::id()`：本层 `ScopeId`；子作用域的 id 与父 `children()` 里列出的值同源，`Builder::id()` 在 build 前就能取到。
- `Context::parent()`：父级句柄，用于向上遍历。
- `Context::cancelled()`：可等待、**电平触发**的「开始收尾」信号。
- `Context::stopped()`：可等待、**电平触发**的「已停稳」信号，结局为 `Settlement::{Stopped, Abandoned}`。

### 3.4.1 取消信号（`ScopeCore` + `Waiters`）

生命周期状态收敛在每层一个 `Data.core: ScopeCore` 里，它同时持有**两路**电平信号：`cancellation`（开始收尾）与 `settled`（已停稳）；`TaskCell.finished`（任务结局）复用同一 `Waiters` 原语。每个 `Waiters` 是 `AtomicBool`（触发位）+ `Mutex<{ slots, free }>`（带代际的下标槽位表）。

`ScopeCore` 之前是 `Data` 上的**六份并行表示**（阶段枚举、`shutting_down` / `stop_requested` / `abandoned` 三个原子，加两个信号），合并为「一个锁内权威快照 + 一个派生 tag」：

- `ScopeInner { lifecycle, requested, abandoned, children }` 是唯一真值源，**所有迁移都经 `ScopeCore::commit`**：在同一临界区内改 `inner`，然后按 `inner` 重算并 `Release` 写入 `tag`。
- 活跃子作用域**不另存租约计数**：`children` 是 `BTreeSet<ScopeId>`，`ScopeId` 进程内唯一、一个 id 至多一个租约，因此集合大小恒等于活跃租约数。「租约为零才转 `Stopping`」就是 `children.is_empty()`；集合与判定同临界区，不存在计数与清单分叉。用 `BTreeSet` 而非 `Vec`：`acquire`/`release` 是 O(log k)，避免 `Vec::retain` 在「一个父作用域下挂很多会话子作用域并批量关闭」时的 O(k²)；迭代恒为 id 升序，`Context::children()` 顺序因此确定。
- `tag` 是派生标志位（`SHUTTING` / `STOPPED` / `REQUESTED` / `ABANDONED`），供免锁快路径单次读取。它不是第二真值源——只在 `commit` 里更新，所以不会陈旧。
- 收益：`is_stopping()` / `is_stopped()` / `is_stop_requested()` / `is_abandoned()` / `is_settled()` 都是**单次原子读、不拿锁**（此前 `is_stopped()` 要拿 `ScopeCore` 锁）。
- **撕裂读被消除**：`poll_settled` 此前先读 `lifecycle`（拿锁）、再读 `abandoned`（原子），两次之间状态可能已推进；现在「走完流程」与「被遗弃」在同一个 tag 里，一次读给出一致快照。

`Waiters` 本身：

- **必须电平触发**：`register` 先查触发位、已触发即返回 `None`（调用方直接就绪），否则分配槽位；置位与取走在同一把锁内完成，`register` 的「复查 + 注册」也在同一把锁下，因此不存在「先查后注册」的丢唤醒窗口。边沿触发做不到这点——停止可能在任何等待者注册之前就已发生。
- **注册项按「哪个 future 注册的」区分，而不是按 waker 相等**：每个 future 首次 poll 时拿到一个 `WaitId { index, generation }`，`update` 只改自己那个槽位的 waker。按 waker 判重是个陷阱——同一任务里的两个 `cancelled()` 等待者由 executor 用同一个 waker 轮询，会共享一条记录，其中一个 future 被丢弃时就把另一个仍然存活的等待者的唤醒源一起摘掉，停止时无人被唤醒。
- **槽位按需复用，注销是 O(1)**：`release` 只清自己那个槽位并把它推回 `free`，不像旧的「全局自增 token + `retain`」那样每次注销都扫一遍全部等待者。`generation` 防槽位复用后的 ABA：旧持有者的 `release` / `update` 因代际不符被忽略，不会误伤复用同一槽位的新等待者。槽位总数收敛到「并发等待者峰值」。
- **等待者会在放弃等待时主动摘除自己**：`cancelled()` / `stopped()` 返回的具名 future 在 `Drop` 里释放自己的槽位。等待者常写成 `select! { _ = ctx.cancelled() => …, _ = work => … }`，先等到 `work` 就不再关心停止信号；不摘除的话，长生命周期作用域的等待者表会随这类任务单调增长。
- 排空与 `TaskHandle::wait` 走 `Waiters::wait`，其 future 被丢弃时同样在 `Drop` 里释放槽位；触发时全部槽位被取走（触发后 `release` 是 no-op）。
- **唤醒在锁外**：`fire_all` 在锁内取走全部 waker，出锁后再 `wake`——waker 是 executor 或用户代码，不能在持锁期间调用。
- 不依赖 tokio：只用 `std::task::{Poll, Waker}`，`cancelled()` / `stopped()` 可在任意 executor 上等待。
- **`cancellation` 的触发时机是硬不变量**：`Runtime::stop` 提交停止（`ScopeCore::begin_close` 在同一锁内置 `Closing` 与单向闩）后**无条件广播**，必须早于插件 `stop` 与任务排空。即使 stop 随即被活跃子作用域阻塞（`Blocked`），信号也已发出——owner 表达停止意图本身就该唤醒长驻任务收尾。
- `poll_cancelled` 额外复查关闭单向闩——**免锁快路径**（已提交关闭的等待者不必抢 `Waiters` 的锁），不是正确性所必需：`fire_all` 总会唤醒已注册的等待者，而置位与取走之间到达的等待者会在同一把锁内被 drain。`poll_settled` **不**做对应复查：`settled` 的两个触发点都在更新 tag 之后才 `fire_all`，「`fired` 可见 ⇒ tag 位可见」，且 `register` 会在锁内复查 `fired`，多查一次不会带来新的就绪。
- **`settled` 的触发时机**：`Runtime::stop` 走到 `Stopped`（清理全部完成、租约已归还）时触发；`Runtime` 在未到 `Stopped` 就被丢弃时，`Drop` 先提交 `Closing`、广播取消、置 `abandoned`，再触发 `settled`，等待者拿到 `Settlement::Abandoned` 而不是永久挂起。`poll_settled` 先查 `is_settled()`（`STOPPED | ABANDONED` 同一个 tag，单次原子读）。
- `StopHandle::request_stop()` 是 `cancellation` 的另一个触发点（与 owner `stop()` 提交、`Runtime` 被遗弃并列）：置 `ScopeInner.requested`（映射为 `REQUESTED` tag 位，与 `SHUTTING` **分开**）后广播。请求不改变 `is_stopping`，也不拒绝 `scope` / `spawn`——请求不是清理。

### 3.5 后台任务（`tokio` feature）

`Context::spawn(fut)` 把任务登记到本层 `Data` 的任务注册表并返回 `TaskHandle`：

- 要求当前线程处于 tokio runtime 上下文，否则返回 `ErrorKind::NoTaskRuntime`。
- 任务输出必须是 `Result<(), Error>`；返回 `Err` 时发出 `TaskFailed` 事件（沿父链冒泡）。**上报顺序是硬约束：先写权威结局 `TaskOutcome::Failed`，再内联上报，最后才发完成信号。** `Ok(Err)` 没有第二条出口——`drain_error` 对 `Failed` 返回 `None`（任务体返回的 `Err` 不进 `stop` 汇总错误），`TaskFailed` 是唯一通路。但这个「不需要排空上报」的前提是**上报已完整送达**，因此另有 `TaskCell.report_settled` 位记录这件事（见下）。`TaskFailed` 走内联 `emit_notify` 而**不是**异步 `notify`——后者有界可丢，而这条通知丢不起。
- 注册表在 `spawn` 时**摊销剪除**已结束的 cell（内存边界）：仅当表长跨过 `compact_at` 阈值时做一次 `retain`，随后阈值按实际长度翻倍，n 次 `spawn` 累计 O(n)。剪除**保住「排空仍需上报」的结局**（`needs_drain_report` 覆盖 `Panicked` 与 `Aborted(Timeout)`）：按 `is_finished` 盲删会让一条 panic 因为它之后又有人 `spawn` 过而被静默丢掉，`stop` 反而报成功。`task_count()` 只统计不剪除，同理——剪除会销毁尚未上报的结局。本层进入关闭后 `spawn` 返回 `ErrorKind::Stopping`。
- 注册表元素是 `Arc<TaskCell>`，与 `TaskHandle` 共享：cell 持 `AbortHandle` + 完成信号 + 结局。**不保存 `JoinHandle`**——旧实现为此不得不在 `Runtime` 上挂 `draining` 字段来防「stop future 被丢弃时句柄析构 detach 任务」，现在被丢弃的 future 不会从表里移除任何东西，重入直接重新处理同一个 cell（完成信号电平触发，已结束的立即返回）。
- **完成信号必须有写者，而且要覆盖整个任务**：`catch_unwind` 只兜住任务体；结局的**上报路径**（`emit_notify` 会跑用户 handler）与任务体同在一个任务里，它 panic 时 wrapper 会在补发完成信号前展开——没有兜底则完成信号永不触发，`stop()` 与 `TaskHandle::wait` 一起挂死，`stop_with_timeout` 还会把 panic 误报成 `TaskAborted`。因此另有 `FinishOnUnwind` 守卫，判据是「**完成信号是否已发出**」而不是「结局是否已写」：失败分支先写 `Failed`、后上报、最后发信号，上报 panic 时结局已写但信号未发，按结局判据提前返回就会让排空挂死。守卫按成因补写缺失的结局：正在展开（真 panic）记 `Panicked(PanicInfo)`，宿主直接把 future 丢掉（runtime 关闭、未记录的 abort）记 `Aborted(AbortReason::HostShutdown)`——后者若也记 panic，会让一次 `stop()` 凭空多出 `TaskFailed` 假失败。结局写入是先写者胜，因此守卫不会用 `Panicked` 覆盖已经记下的真实失败。panic hook 照常输出；`PanicInfo::message` 另外留存 `&str`/`String` 载荷。
- **`Failed` 是否「已送达」由 `TaskCell.report_settled` 单独记录**：只在 `emit_notify` **正常返回后**置位，且必须先于 `finished.fire_all()`（`Release`/`Acquire` 保证「看到完成信号即看到已送达」）。因此上报被**任何**方式截断——handler panic、排空超时 abort 在 await 点取消上报 future、宿主丢弃 future——都表现为「结局是 `Failed` 而 `report_settled == false`」，排空据此以 `ErrorKind::TaskFailed { task_id }` 兜底上报。把「结局是 `Failed`」当成「已上报」是错的：`TaskOutcomeKind` 因此把 `Failed` 与 `Completed` 分开，`needs_drain_report` / `drain_report_error` 都看这个位。
- 剪除（`spawn` 里阈值触发的 `retain`）只回收「不需要排空上报」的结局，`needs_drain_report` 是唯一出处：`Panicked`（无事件出口）、`Aborted(Timeout)`（只有排空上报）、以及 `Failed` 但 `report_settled == false`（上报未送达）必须留在表里。代价是**未被 `stop` 清理前，表里会累积这些 cell**（每个很小）；上游应当在监控里用 `TaskFailed` 事件而不是依赖 `stop` 报错来发现任务失败。
- `Context::task_count()` 统计尚未落定结局的任务，含 `stop` 正在排空的那一个。
- `stop` 顺序：提交停止（`Closing`，广播取消）→ 租约归零转 `Stopping` → 插件逆序 `stop` → 排空任务（`stop_with_timeout` 预算内等待，超时 `abort` 并上报 `ErrorKind::TaskAborted`）→ dispose hooks。
- **任务结局是一等公开类型**：`TaskOutcome = Completed | Failed(Error) | Panicked(PanicInfo) | Aborted(AbortReason)`，`AbortReason = Owner | Timeout | HostShutdown`。`TaskHandle::wait()` 直接返回它，不再折叠成 `Result<(), Error>`：「跑完了」与「被取消了」在类型上分开，取消成因也保留。需要旧的 `Result` 视图时用 `TaskOutcome::into_result(task_id)`。
- **取消来源写进结局本身**：`Aborted(Owner)`（owner 通过 `TaskHandle::abort()`）不计入停止错误（否则「我让你停」会被报成失败），只有排空预算耗尽的 `Aborted(Timeout)` 才上报 `TaskAborted`。`Aborted(HostShutdown)` 是宿主丢弃 future 的兜底路径，也不上报。四者用同一个 `Mutex<Option<TaskOutcome>>` 的「先写者胜」落定，因此排空只读一次结局就能正确归类——分成「结局 + 旁边一个来源原子」会留下「结局已是取消、来源标记还没写入」的窗口，把 owner 取消误报成超时。取消方负责补发完成信号：被 abort 的任务不会再执行收尾代码。owner `abort` 与排空超时**真正同刻**并发时按「先写者胜」归类——先落定的一方定义这次取消的性质，这是可接受的平局语义，但值得知道它存在。
- 排空与 `wait()` 的错误归类**同源**：`TaskOutcome::drain_error(task_id)` 是唯一映射（`Panicked` → `TaskFailed`、`Timeout` → `TaskAborted`、其余 `None`）。`Failed` 由 [`TaskCell::drain_report_error`] 在此映射之前按「`report_settled` 是否已置位」决定：未送达才补报 `TaskFailed`，已送达则返回 `None`。
- 排空「先 await 结局、再取出」：await 被取消时不摘表，重入重新看到它；「取出—判断—上报」之间没有 await 点，因此相对取消是原子的——被丢弃的 stop future 只会停在 await 上，不会落在中间造成漏报或重报，无需额外的去重标志。
- abort 尽力而为：卡在阻塞调用里的任务要等其让出执行权才会真正取消。
- 不经 `stop()` 直接 drop `Runtime` 不排空任务（`Drop` 不做异步清理），未完成任务随注册表丢弃而脱离框架管理。

### 3.6 受管子作用域注册表（`ScopeRegistry`，`tokio`）

可选工具，**不进核心**：它只组合公开 API（`Context::scope` / `Runtime::stop_with_timeout` / `StopOutcome`），可原样搬进独立 crate。

- **所有权**：`Runtime` 被登记表 slot 独占持有，从不以 `Arc<Runtime>` 外泄——`Runtime` 非 `Sync`、`stop` 需 `&mut self`，共享执行权会直接破坏「所有权唯一」。对外只授只读 `Context`。注册表因此不可 `Clone`，但可按 `&self` 跨任务使用。
- **认领先于构建**：`open` 先在锁内占住名字，占住了才 `ctx.scope()`。并发输家在认领阶段失败、**从未 `scope()`**，所以不产生租约、也不需要对已 `start` 的 `Runtime` 做回滚——这是手写版最难收敛的一段。占位由 `Reservation` 的 `Drop` 兜底，`open` 的 future 中途被丢弃时摘掉占位，名字不会永久泄漏；而回收路径上的 `stop().await` 由 `ClosingGuard` 兜底（见下），只有 `start().await` 是明文契约上的未兜底窗口（owner 应让 `open` 跑完，或用外层超时把「创建」当整体）。
- **关闭是状态迁移，不是所有权转出**：`close` 在锁内把 `Running` 迁到 `Closing`（名字**全程仍被占住**，并发同名 `open` 只得 `AlreadyExists`），取出 owned `Runtime` 后释放锁再 `await`。结局是 `Blocked`（仍被孙级阻塞）时**不得析构 owner**：`Runtime` 放回 `Closing(Some(..))`，等调用方回收孙级后重试 `close` 续跑。把非终态当终态 `drop` 会命中 `Runtime` 的 Drop 护栏（debug）或静默跳过插件 `stop`（release）。这条「await 点被取消也不丢 owner」由 `ClosingGuard` 保证（`close` 与 `open` 的两处回收 `stop` 共用）：取消时 `restore` 用 `insert` 重新占住名字并放回 `Closing(Some)`，等价 `Blocked`、可重试。`get` 只对 `Running` 授出句柄；`Reserved`/`Closing` 返回 `None`，并发 `close` 得到 `RegistryError::Busy` 而不是被谎报成 `NotFound`。
- **启动失败的回收有例外**：一般情形半成品 `stop` + drop、名字释放可重试；但若回收 `stop` 被活跃孙级 `Blocked`（插件在 `start` 里泄漏了子作用域），owner 以 `Closing(Some)` 保留、名字不释放，调用方须先回收孙级再对同名 `close` 续跑。
- **三分结局**：`CloseReport { outcome: StopOutcome, status: CloseStatus }`。`CloseStatus::{Clean, Aborted, Failed, Blocked}` 由 `status_of` 单点从 `StopOutcome` 推导（`Aborted` 优先于 `Failed`，因为超时强杀解释了错误里的取消成因）；「超时强杀」的唯一定义仍是被强制取消的 `ErrorKind::TaskAborted`，不新增第二套分类真相。`close` 单次调用只跑一轮 `stop`，「被阻塞则重试」是调用方的决定（`Blocked` 不是失败），因此报告里不再带一个恒为 1 的 `attempts` 字段。
- **收口顺序与线性化点**：停机闩 `shutting_down` 与登记表**同一把锁**。`close_all` 在临界区内「置停机 + 取快照」；`open` 的「查停机 + 认领」与**发布复查**也在同一临界区。因此停机开始后不可能再发布出 `Running`——在飞 `open` 若在 `start().await` 期间撞上停机，会拒绝发布、就地回收刚建好的 `Runtime` 并返回 `ShuttingDown`。子级就是父级的租约，「子级全停 → 父级才停」由注册表兜住；孙级由各自的 owner 负责，注册表不越权。但 `close_all` 返回**不等于**父级立刻可停：一个已认领、尚未发布的 `open` 可能已经 `scope()` 出子作用域、持有父级租约，要等它发布复查停机并回滚后才归还——这段窗口里父级 `stop` 会瞬时 `Blocked`。要一次停干净，先确认所有 `open` future 已结束，或对父级 `stop` 的 `Blocked` 重试一次。

---

## 4. 服务

- `ServiceRegistry` 支持普通服务、懒加载工厂、集合服务。
- `Builder` 阶段可 `provide` / `provide_factory` / `provide_collect` / `provide_dynamic` / `require_mut`。
- `Context` 阶段只读：`require` / `try_require` / `require_all` / `require_all_recursive` / `require_dynamic` / `contains`；观测与生命周期句柄为 `is_stopping` / `cancelled` / `children` / `parent` / `id`，任务侧为 `spawn`（返回 `TaskHandle`）/ `task_count`。
- `contains` 做完整存在性检查：普通服务 / 工厂 / 集合，任一层级存在即为真。`Dependency` 校验走另一套单例语义（`contains_type`），集合服务不满足单例依赖。
- 命名空间划分是刻意的：`contains::<T>()` 对单例、工厂、集合任一种存在即为真（存在性可见性），但 `provide::<T>()` 的重复检查与 `Dependency` 校验只针对单例与工厂，因此集合既不满足单例依赖、也不遮蔽同类型单例；`remove::<T>()` 只移除普通服务，工厂与集合有各自通道。
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
- 跨作用域的 `PluginDependency` 只保证「父层注册过该名字」，**不建立父子作用域的启动顺序**：父 `Runtime` 可以只 `build` 未 `start`，子作用域仍能独立 `start`（`src/lib.rs` 的 scope 测试编码了这一契约）。需要严格顺序时在应用层显式同步。
- `priority` 参与 `start()` 与 `start_serial()` 共用的拓扑选点（影响同层内次序与逆序停止次序）；但分层并行 `start()` 的层划分只由依赖边决定，同层内不保证 priority/注册序总序。

### 5.3 启动分层并行

- 默认 `start()` 对拓扑同层插件使用 `join_all` 并发启动。
- `start_serial()` 保留旧串行总序。
- 同层插件之间不存在 priority/注册序的总序保证。

### 5.4 回滚

单个 `plugin()` 是事务性的：

- `apply` 失败（返回 `Err` **或 panic 展开**）时回滚本次新增的服务、插件、hooks、事件订阅；
- 回滚用**增量撤销日志**（undo log）逆序 replay 到本次 `plugin()` 的检查点，代价与本次改动量成正比，而非与注册表总规模成正比；
- 集合服务按「弹出该类型最后压入的元素」精确撤销，失败插件追加的集合元素不会泄漏，也不会误删其他插件压入的元素；
- 嵌套插件注册失败会保留内层插件名，同时外层副作用一并回滚（各层各持检查点，逆操作幂等）；
- panic 路径用 `catch_unwind` + `resume_unwind`：先回滚再原样向上传播 panic，不把崩溃降级成 `Err`；
- `plugin_with_config` 会连同配置服务一起回滚。

---

## 6. 生命周期 Hook

- `on_ready`：`start()` 成功启动所有插件后执行。
- `on_dispose`：`stop()` 时始终执行（在插件 `stop` 与任务排空之后、dispose 阶段）。
- `on_closing`：本层**开始关闭**那一刻（`begin_close` 首次提交为 `Closing`）同步执行一次，早于插件 `stop`、任务排空与 dispose。见下。
- `LifecycleHook::call(&mut self, ctx: &Context)`。
- ready/dispose 不得在 `await` 期间 `mem::take` 出 Runtime，避免 future 取消后丢失 hook。

### 6.1 `on_closing`（同步关闭回调）

`CloseHook::call(&self, ctx: &Context)`，**同步**（签名刻意不留 await 位置：在回调里等待 `ctx.stopped()` 等于等自己造成的关闭完成，会自死锁）。

- 注册：装配期 `Builder::on_closing` / `Configurator::on_closing`（随本层存续）；运行期 `Context::on_closing` 返回 `CloseHandle`，**丢弃即注销**。
- 触发点是 `begin_close()` 的 `bool` 返回值（首次提交为 `true`）——现成的原子沿，天然满足「每层至多一次、单向不可复位」。同一提交点也负责广播 `cancellation`，顺序是：置状态 → 锁外同步跑回调 → 唤醒等待者。
- **panic 被 `catch_unwind` 隔离**并转成 `ErrorKind::LifecyclePanicked { message }`（`phase` 为 `Close`）计入 `StopOutcome.errors`。这段代码运行在 owner `stop()` 的 future 里，panic 逃出会让本层停在不可逆的 `Closing` 却永远没有清理 future。插件 `stop` 与 dispose 用同一个隔离包装（`phase` 为 `Stop`/`Dispose`），因此清理 future 体在构造上不可展开。
- **遗弃路径不触发**：owner 未 `stop` 就丢弃 `Runtime` 时，`Drop` 只置 `Abandoned` + 唤醒 pull 等待者，**不执行任何用户代码**——`Drop` 可能在栈展开中运行，此时调用户代码会二次 panic 并 abort。语义因此不对称：`cancelled()`/`stopped()` 在遗弃时必被释放（`Stopped`/`Abandoned`），`on_closing` 则永不触发。
- 回调执行时全部插件已 `apply`、服务未被回收，因此 `ctx.require` 一定拿得到——比 `on_dispose`（插件 `stop` 之后）更安全。
- **恰好一次**：回调表与派发状态（`NotStarted`/`Dispatching`/`Dispatched`）在**同一把锁**内。已进入 `Closing` 及更晚时再注册，只有派发已结束（或本层被遗弃、不会再派发）才**立即同步调用一次**；未派发/派发中的注册并入后续轮次。链式注册有三道上界：轮次 `MAX_CLOSING_HOOK_ROUNDS`（16）、执行的**嵌套深度**（同为 16，用线程局部计数，跨层共用，故总嵌套有全局上界）、以及**链式注册**总数 `MAX_CHAINED_CLOSING_HOOKS`（4096，按层计）。「链式」的判据是「注册发生在关闭钩子执行上下文内（线程局部深度 `> 0`，任何层），或目标层已开始派发」——只看目标层派发状态会被「向尚未开始派发的另一层注册」绕过并跨层放大；而对所有运行期注册一律计费又会误伤长生命周期层的反复注册/注销。装配期经 `Builder`/`Configurator` 声明、以及层存活期间普通代码的动态注册都不计。轮次触顶时收尾把**那一刻已在表内**的剩余项各执行一次；任一上界触顶都经 `mark_cutoff` 记**一条** `ErrorKind::CloseHookDispatchOverflow`（每层至多一条）。三闸缺一不可：没有深度闸，自增殖回调同步递归到栈溢出（abort，`catch_unwind` 拦不住）；没有总数闸，一次注册 k 个副本的回调按 `k^16` 膨胀。`closing_errors` 在清理 future 的起手与**末尾**各取一次，覆盖清理期（插件 `stop`/dispose 中）注册并 panic 的回调；终态之后或遗弃层的这类错误无消费者。

---

## 7. 事件系统

- 注册在 `Builder` 阶段：`on::<E, _>(handler)`。
- 事件沿父链冒泡（本层先、祖先后）；handler 收到的是其注册层级的 `Context`。
- **三个投递档位，按「要不要等订阅者」选**：

| 档位 | 方法 | 语义 |
|---|---|---|
| 严格管线 | `emit` / `emit_parallel` | 调用方 await 整条链；可聚合错误、可短路 |
| 内联旁路 | `emit_notify` / `emit_notify_parallel` | 调用方等全部 handler 跑完；handler 错误不阻断后续与冒泡，但**要等**。失败上报这类「不能丢」的走这里 |
| 异步通知 | `notify`（`tokio`） | 同步入队即返回，**不等任何 handler**；每订阅者一条有界 FIFO lane，满了丢新来的并计数 |

- `emit_parallel` 错误聚合使用 `Phase::Event` + `ErrorKind::Multiple`。
- `emit_notify` / `emit_notify_parallel` 不抛错，而是返回收集到的 handler 错误；`Bail` 仍会停止冒泡。
- **内部实现是两条正交轴，不是两个布尔**：`Scheduling::{Ordered, Concurrent}` 决定同一层怎么排，`ErrorPolicy::{Abort, Collect}` 决定出错后收不收手。四种组合由 `dispatch_layer` 各自一处定义——`emit = (Ordered, Abort)`、`emit_parallel = (Concurrent, Abort)`、`emit_notify = (Ordered, Collect)`、`emit_notify_parallel = (Concurrent, Collect)`。两条轴都遵守同一条冒泡规则：`Bail` 停止向上冒泡；`Abort` 在出错时同样停止。
- `notify::<E>(event) -> Receipt`：`delivered` 是**成功入队**份数（不是已处理完），`dropped` 是 lane 满被丢的，`rejected` 是 hub 已排空后拒收的。`Context::notify_stats()` 返回各订阅者的 `Backlog { handler_id, queued, dropped, rejected }`。
  - **不提供跨订阅者 / 跨层的短路**：每个订阅者独立排队后，「让后面的人别再收到」已无确定含义。需要确定性短路请用 `emit`。
  - lane 在**冻结点**一次建好，此后只读，`enqueue` 不需要任何锁；worker 在 `Runtime::start` 启动，只持 `Weak<Data>`（强引用会形成 `worker → Data → hub → lane` 的环，owner 不 `stop` 就丢 `Runtime` 时 Data 永不释放、通道也不关闭）。
  - 代价：每个订阅者在冻结时就占有一条有界 lane（含 channel），即便该事件从不走 `notify`。换来的是 `notify` 不需要 tokio 上下文（只 `try_send`）、worker 能在 `start()` 一次起齐，不必在同步入队路径上惰性建 lane + 惰性 spawn。已评估「首次 `notify` 时按需建 lane」：它会把锁/CAS 引入通知热路径，得不偿失，故不采用；该常数成本由单个作用域内的 handler 数界定。
  - 停止排空排在**任务排空之后**（任务体可能在返回前发通知），共享同一 deadline 预算；预算耗尽直接 detach 剩余 worker，不报成停止失败（「有通知没送完」不是停止失败）。
  - 无 `tokio` 构建没有执行器驱动 worker，`notify` 退化为「调用方同步跑完本层及祖先的全部 handler」，回执的 `delivered` 即实跑数。
- 无 `tokio` 时 `notify_stats()` 返回空表。

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
    Apply, Verify, Build, Require, Start, Ready, Close, Stop, Dispose, Event,
}

#[non_exhaustive]
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
    StopBlocked(Blockers),
    Stopping,
    SubscriptionNotFound,
    NoTaskRuntime,
    TaskFailed { task_id: TaskId },
    TaskAborted { task_id: TaskId },
    LifecyclePanicked { message: Option<String> },
    CloseHookDispatchOverflow,
    StartFailed,
    Other,
    Multiple(Vec<Error>),
}
```

`ErrorKind` 标注 `#[non_exhaustive]`（持续增补的分类表，下游 `match` 保留兜底分支）；`Phase::Close` 对应关闭回调、`LifecyclePanicked` 覆盖关闭回调 / 插件 `stop` / dispose 三类 panic 站点（由 `phase` 区分）。

- 底层错误通过 `source` 保留错误链，不 `to_string()` 压平。
- `source` 字段内部是 `Arc<dyn Error + Send + Sync>`，`with_source` 的公开签名与 `source()` 的返回类型不变。`Error` 与 `ErrorKind` 因此可 `Clone`：克隆会深拷贝 `Multiple` 的子错误树，但每个子错误的 `source` 只增引用计数，因此两份共享同一条底层链。这是 `Runtime` 能同时把首错交给调用方又保留一份的前提；`Error` 仍不实现 `PartialEq`。
- **契约**：凡经 `with_source` 附加的来源，穿过 `into_phase`、`Multiple` 聚合和 start/stop 的阶段标注后都不被丢弃、不被压平，可按 `source()` 逐层 `downcast_ref` 取回。
- 单点错误用 `matches!(err.kind(), ...)` 判断。
- `Stopping` 同时覆盖 `scope()` 与 `spawn()` 的关闭后拒绝（拒绝条件是 `Closing` / `Stopping` / `Stopped`）。
- `ErrorKind::StopBlocked(Blockers)` 只在 `StopOutcome::into_result()` 折叠 `Blocked` 时出现。`Runtime::stop` 本身不再用错误表达「被活跃子作用域挡住」——那是 `StopOutcome::Blocked`，不是失败，也不是清理错误。
- `TaskId` / `ScopeId` 是不透明 newtype（`#[repr(transparent)]`），两者在类型上不可互换，把作用域 id 误传到任务 id 位置会编译失败；读数值用 `.get()`。

---

## 9. 非目标 / 边界

- 不支持插件热加载。
- 不支持运行时动态注册服务；运行期可变依赖服务自身同步。
- 无 `Drop` 自动异步 `stop()`；必须显式 `stop()`。
- `Drop` 只做护栏不做清理，但**会**把未到 `Stopped` 的本层关掉：提交 `Closing`（拒绝新 `scope` / `spawn`）、广播取消、投递 `Settlement::Abandoned`。debug 构建下若「曾进入启动流程却未到 `Stopped`」再硬失败，消息按 `lifecycle` 与**现读**的 `children()` 区分「从未 stop」「被活跃子作用域阻塞（列出 ids）」「stop 已提交但清理未走完」三种成因；release 下静默——正确性不应依赖这条诊断。
- c-lite 不防御「不 `stop` 直接 drop 子 Runtime」导致的深层异步清理缺失。
- 任务取消基于 tokio 协作式 abort，尽力而为；卡在同步阻塞调用中的任务无法被强制杀死。
- 不提供父 stop 自动传播/强制停止子 scope；父 stop 仍被活跃子作用域阻塞并返回 `Blocked`，但 `Blockers::ids()` 可观测。取消信号是**每层各自**的：`cancelled()` 只在本层提交停止、收到本层请求或被 owner 遗弃时完成，父层不代子层广播。这不构成缺口——子层只能由它自己的 `stop` 收口，那一刻它的信号就会触发。
- `StopHandle::request_stop()` 只广播请求，不触发清理，也不自动把本层推进到 `Stopping`；关闭始终由持有 `Runtime` 的 owner 执行。
- 不提供运行期事件退订（`Context::off`）。handler 表在 `build` 时冻结，好让 emit 走无锁查表；为运行期可变性给它加同步等于拿热路径换低频能力。需要按请求动态分发的场景，请在应用层维护自己的分发表。
