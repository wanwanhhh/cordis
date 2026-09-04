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
- `Runtime::stop_with_timeout(Duration)`：任务排空共享该预算，超时后强制取消未完成任务并计入聚合错误。
- start-after-stop 是 no-op，对应状态机为：

```rust
if self.started || self.stopped {
    return Ok(());
}
```

- 未 `start` 就 `stop` 时，只执行 dispose hooks，不调用插件自身的 `stop`。

### 2.2 重复调用

- 重复 `start` 安全 no-op。
- 重复 `stop` 安全 no-op。
- `stop` 失败会继续清理，并把错误聚合为 `ErrorKind::Multiple`。

---

## 3. Scope 与 c-lite 租约

### 3.1 scope 创建

```rust
let child: Builder = ctx.scope()?;
```

`Context::scope()` 在父 `Data.state` 上原子递增 child 计数。父 `Runtime` 已停止时返回 `ErrorKind::Stopping`；计数达到上限（2^63 - 1）时返回 `ErrorKind::TooManyScopes`。

### 3.2 租约

- 租约由 `ScopeLease { parent, child_id }` 持有父 `Data` 强引用与本层 `context_id`。
- 租约在 `Context::scope()` 时交给子 `Builder`，`build()` 时 move 给子 `Runtime`。
- 只在 `Runtime` / `Builder` 的字段析构阶段释放，绝不在 `stop()` 成功路径释放。
- `Runtime` 中 `_lease` 必须是最后一个字段，保证插件字段先析构、再归还父计数。
- `ScopeLease::drop` 先把本层 id 从父注册表摘除，再 `fetch_sub(1, SeqCst)` 归还父计数。

### 3.3 父 stop 与子存活

- 父 `Runtime::stop()` 只接受本层没有活跃子 Runtime/Builder，否则返回 `ErrorKind::ActiveScopes { count, ids }`。
- `ids` 即当前活跃子作用域的 `context_id` 清单（与 `Context::children()` 一致），供管理器定位阻塞方。
- `ActiveScopes` 失败时不会置 `STOPPED`，调用方停掉/丢弃子级后可重试。
- 仅子 `Context` 句柄存活不阻塞父 stop。
- 子 `Runtime` 即使已 `stop` 但未 `drop`，仍占租约并阻塞父 stop。

### 3.4 停止可观测性

- `Context::is_stopping()`：本层是否已置停止标志。
- `Context::children()`：本层活跃子作用域 id 清单（含未 build 的子 Builder 与未 drop 的子 Runtime）。
- `Context::parent()`：父级句柄，用于向上遍历。

### 3.5 后台任务（`tokio` feature）

`Context::spawn(fut)` 把任务登记到本层 `Data` 的任务注册表（默认启用 `tokio` feature 时可用）：

- 要求当前线程处于 tokio runtime 上下文，否则返回 `ErrorKind::NoTaskRuntime`。
- 任务输出必须是 `Result<(), Error>`；返回 `Err` 时以 `emit_notify` 发出 `TaskFailed` 事件（沿父链冒泡）；panic 不捕获，在停止排空时以 `ErrorKind::TaskFailed` 上报。
- 注册表在每次 `spawn` / `task_count()` 时惰性剪除已完成任务；本层进入停止后 `spawn` 返回 `ErrorKind::Stopping`。
- `stop` 顺序：置停止标志 → 插件逆序 `stop` → 排空任务（`stop_with_timeout` 预算内等待，超时 `abort` 并上报 `ErrorKind::TaskAborted`）→ dispose hooks。
- abort 尽力而为：卡在阻塞调用里的任务要等其让出执行权才会真正取消。
- 不经 `stop()` 直接 drop `Runtime` 不排空任务（`Drop` 不做异步清理），未完成任务随句柄丢弃而脱离框架管理。

---

## 4. 服务

- `ServiceRegistry` 支持普通服务、懒加载工厂、集合服务。
- `Builder` 阶段可 `provide` / `provide_factory` / `provide_collect` / `provide_dynamic` / `require_mut`。
- `Context` 阶段只读：`require` / `try_require` / `require_all` / `require_all_recursive` / `require_dynamic` / `contains`。
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
    // source: Option<Box<dyn Error + Send + Sync + 'static>>
}
```

```rust
pub enum Phase {
    Apply, Verify, Build, Start, Ready, Stop, Dispose, Event,
}

pub enum ErrorKind {
    ServiceNotFound(String),
    ServiceAlreadyRegistered(String),
    ServiceTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
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
    Other,
    Multiple(Vec<Error>),
}
```

- 底层错误通过 `source` 保留错误链，不 `to_string()` 压平。
- `Error` 含 `Box<dyn Error>`，因此不实现 `Clone + PartialEq`。
- 单点错误用 `matches!(err.kind(), ...)` 判断。
- `Stopping` 同时覆盖 `scope()` 与 `spawn()` 的停止后拒绝。

---

## 9. 非目标 / 边界

- 不支持插件热加载。
- 不支持运行时动态注册服务；运行期可变依赖服务自身同步。
- 无 `Drop` 自动异步 `stop()`；必须显式 `stop()`。
- c-lite 不防御“不 `stop` 直接 drop 子 Runtime”导致的深层异步清理缺失。
- 任务取消基于 tokio 协作式 abort，尽力而为；卡在同步阻塞调用中的任务无法被强制杀死。
- 不提供父 stop 自动传播/强制停止子 scope；父 stop 仍被活跃子租约阻塞，但 `ids` 可观测。
