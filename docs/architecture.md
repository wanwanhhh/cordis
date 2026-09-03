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
Context                   // require / contains / has_plugin / emit / scope
```

冻结点之后不存在任何框架可见的 `&mut Data` 路径。`Context` 只读且可跨线程共享。

---

## 2. 生命周期状态

### 2.1 `Runtime`

- `Runtime::start()`：默认分层并行启动。
- `Runtime::start_serial()`：保留旧串行总序语义。
- `Runtime::stop()`：停止已启动插件的逆序清理，并执行 dispose hooks。
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

`Context::scope()` 在父 `Data.state` 上原子递增 child 计数。父 `Runtime` 已停止时返回 `ErrorKind::Stopping`。

### 3.2 租约

- 租约由 `ScopeLease(Arc<Data>)` 持有父 `Data` 强引用。
- 租约在 `Context::scope()` 时交给子 `Builder`，`build()` 时 move 给子 `Runtime`。
- 只在 `Runtime` / `Builder` 的字段析构阶段释放，绝不在 `stop()` 成功路径释放。
- `Runtime` 中 `_lease` 必须是最后一个字段，保证插件字段先析构、再归还父计数。
- `ScopeLease::drop` 使用 `fetch_sub(1, SeqCst)`。

### 3.3 父 stop 与子存活

- 父 `Runtime::stop()` 只接受本层没有活跃子 Runtime/Builder，否则返回 `ErrorKind::ActiveScopes { count }`。
- `ActiveScopes` 失败时不会置 `STOPPED`，调用方停掉/丢弃子级后可重试。
- 仅子 `Context` 句柄存活不阻塞父 stop。
- 子 `Runtime` 即使已 `stop` 但未 `drop`，仍占租约并阻塞父 stop。

---

## 4. 服务

- `ServiceRegistry` 支持普通服务、懒加载工厂、集合服务。
- `Builder` 阶段可 `provide` / `provide_factory` / `provide_collect` / `require_mut`。
- `Context` 阶段只读：`require` / `try_require` / `require_all` / `contains`。
- `require_all` 只查本层，不沿父链冒泡。
- 懒工厂使用裸 `OnceLock`：并发首次访问不保证工厂只执行一次，但成功实例只缓存一个。

---

## 5. 插件

### 5.1 `Plugin` trait

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn priority(&self) -> i32;
    fn dependencies(&self) -> Vec<Dependency>;
    fn plugin_dependencies(&self) -> Vec<PluginDependency>;
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error>;
    async fn start(&self, ctx: &Context) -> Result<(), Error>;
    async fn stop(&self, ctx: &Context) -> Result<(), Error>;
}
```

- `dependencies` / `plugin_dependencies` 在注册时求值并缓存进 `PluginRecord`。
- `apply` 收到窄接口 `Configurator`，不能修改既有服务，也不能执行 start/stop/emit/scope。
- `start` / `stop` 只收 `&Context`，生命周期方法不存在框架级可变别名。

### 5.2 依赖与顺序

- 服务依赖：`Dependency::of::<T>()`。
- 插件间依赖：`PluginDependency::of("plugin-name")`。
- 可选依赖只放宽“必须存在”，不改变“存在时必须按顺序启动”的语义。
- `plugin_dependencies` 是唯一启动顺序契约。
- `priority` 只在 `start_serial()` 的拓扑选点中生效；`start()` 默认分层并行不读取 priority。

### 5.3 启动分层并行

- 默认 `start()` 对拓扑同层插件使用 `join_all` 并发启动。
- `start_serial()` 保留旧串行总序。
- 同层插件之间不存在 priority/注册序的总序保证。

### 5.4 回滚

单个 `plugin()` 是事务性的：

- `apply` 失败时回滚本次新增的服务、插件、hooks、事件订阅；
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
- 触发在 `Context` 阶段：`ctx.emit(event)` / `ctx.emit_parallel(event)`。
- 事件沿父链冒泡。
- handler 收到的是其注册层级的 `Context`。
- `emit_parallel` 错误聚合使用 `Phase::Event` + `ErrorKind::Multiple`。

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
    ActiveScopes { count: u64 },
    Stopping,
    TooManyScopes,
    SubscriptionNotFound,
    Other,
    Multiple(Vec<Error>),
}
```

- 底层错误通过 `source` 保留错误链，不 `to_string()` 压平。
- `Error` 含 `Box<dyn Error>`，因此不实现 `Clone + PartialEq`。
- 单点错误用 `matches!(err.kind(), ...)` 判断。

---

## 9. 非目标 / 边界

- 不支持插件热加载。
- 不支持运行时动态注册服务；运行期可变依赖服务自身同步。
- 无 `Drop` 自动异步 `stop()`；必须显式 `stop()`。
- c-lite 不防御“不 `stop` 直接 drop 子 Runtime”导致的深层异步清理缺失。
