# Cordis 使用指南

> 面向使用者：如何用当前 `cordis` crate 搭建插件化 Rust 应用。

---

## 1. 引入依赖

```toml
[dependencies]
cordis = { path = "../cordis" }
```

当前 crate 不绑定具体 async runtime，示例中使用 `futures::executor::block_on`。实际项目也可使用 `tokio`、`async-std` 等任意 runtime。

---

## 2. 最小应用

```rust
use cordis::{Builder, Configurator, Error, Plugin};

struct MyService;

struct MyPlugin;

impl Plugin for MyPlugin {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(MyService)?;
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut builder = Builder::new();
        builder.plugin(MyPlugin)?;

        let mut rt = builder.build()?;
        rt.start().await?;
        rt.stop().await?;

        Ok(())
    })
}
```

`Builder` 负责装配期可变注册；`build()` 是唯一冻结点；`Runtime` 是生命周期唯一所有者；`Context` 是只读句柄。`Builder` 与 `ServiceRegistry` 均实现 `Default`，`Builder::default()` 等价于 `Builder::new()`。

### 2.1 装配自检与可恢复构建

`verify()` 不消费 Builder，提前完成 `build()` 的全部校验（服务依赖、插件依赖、循环依赖）：

```rust
builder.verify()?; // 等价于 verify_dependencies()，失败时 Builder 仍然可用
```

`build()` 在校验失败时错误同样返回，但 Builder 随 `self` 被消耗；“失败后需要拿回半成品继续修正”的场景用 `try_build()`——校验失败时把 Builder（连作用域租约）完整带回：

```rust
let built = builder.try_build();
match built {
    Ok(rt) => { /* 正常使用 rt */ }
    Err((builder, err)) => {
        // 修正后还能再 build
        eprintln!("装配校验失败: {err}");
    }
}
```

装配期辅助接口：

> 回滚与 `try_build` 带回修正的可运行版本见 `examples/dynamic.rs`（步骤 1–2）。

- `builder.plugins(iter)`：批量注册同类型插件，逐个失败即中止；
- `builder.verify_dependencies()`：等同于 `verify()` 的显式名称；
- `builder.require::<T>()`：装配期读取已注册服务（含父链）；
- `builder.try_require::<T>()`：装配期可选服务，返回 `Option<&T>`；
- `builder.require_dynamic::<T>()`：装配期获取 `Arc<DynamicValue<T>>` 共享句柄；
- `builder.has_plugin(name)`：判断插件是否已注册（局部 + 父链）；
- `builder.contains::<T>()` / `ctx.contains::<T>()`：存在性检查（普通服务 / 工厂 / 集合，局部 + 父链），不产生错误。注意与 `Dependency` 校验语义不同：集合服务不算满足单例依赖。

---

## 3. 插件

### 3.1 基础 Plugin

```rust
#[async_trait]
impl Plugin for MyPlugin {
    fn name(&self) -> &'static str {
        "my-plugin"
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        Ok(())
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    async fn stop(&self, ctx: &Context) -> Result<(), Error> {
        Ok(())
    }
}
```

- `apply`：同步，用于注册服务、注册 hook、注册事件
- `start`：异步，用于初始化资源
- `stop`：异步，用于清理资源

> 注意：如果插件覆盖了 `start` / `stop` 等异步方法，`impl Plugin` 前需要加 `#[async_trait]`；如果只实现 `apply` 等同步方法，可省略。

#### 闭包作为轻量插件

不需要命名结构体时，闭包也可直接作为插件（只实现 `apply` 的轻量插件）：

```rust
builder.plugin(|cfg: &mut Configurator<'_>| {
    cfg.provide(MyService)?;
    Ok(())
})?;
```

### 3.2 插件元信息

```rust
impl Plugin for MyPlugin {
    fn name(&self) -> &'static str { "my-plugin" }
    fn version(&self) -> &'static str { "0.1.0" }
    fn priority(&self) -> i32 { 10 }
}
```

`priority` 参与 `start()` 与 `start_serial()` 的拓扑选点（并影响随之确定的同层内次序与逆序停止次序）；但默认 `start()` 的分层切分只由依赖决定，同层插件之间不保证 priority/注册序总序。插件间顺序以 `plugin_dependencies` 为唯一契约。需要旧串行总序时可使用 `Runtime::start_serial()`。

同一 `Builder` / `Context` 中插件 `name()` 必须唯一；重复注册会返回 `ErrorKind::PluginNameAlreadyRegistered`。

### 3.3 插件依赖

```rust
impl Plugin for MyPlugin {
    fn dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::of::<Database>()]
    }

    fn plugin_dependencies(&self) -> Vec<PluginDependency> {
        vec![PluginDependency::of("memory-plugin")]
    }
}
```

可选依赖：

```rust
Dependency::optional_of::<OptionalService>()
PluginDependency::optional_of("optional-plugin")
```

两个结构体字段均为公开（`type_id` / `name` / `optional`、`plugin_name` / `optional`），也可手动构造，但通常使用上述构造函数即可。

插件依赖会做拓扑排序；循环依赖报错。

可选插件依赖可以通过 `has_plugin` 判断目标插件是否存在：

```rust
if ctx.has_plugin("optional-plugin") {
    // 启用可选能力
}
```

`has_plugin` 在 `Builder`、`Configurator` 和 `Context` 上都可用，查询范围都是“本层 + 父链”：

```rust
builder.has_plugin("optional-plugin");
cfg.has_plugin("optional-plugin");
ctx.has_plugin("optional-plugin");
```

> 可运行示例：`examples/dynamic.rs`（步骤 4，`consumer` 的可选服务/插件依赖）。

### 3.4 插件配置

```rust
builder.plugin_with_config(MyPlugin, MyConfig {
    model: "gpt-4o".into(),
})?;
```

插件读取：

```rust
impl Plugin for MyPlugin {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        let config = cfg.require::<MyConfig>()?;
        Ok(())
    }
}
```

> 可运行示例：`examples/dynamic.rs`（步骤 4）。

### 3.5 插件作用域

默认插件可以安装在根或子作用域。你可以限制插件安装位置：

```rust
use cordis::PluginScope;

impl Plugin for RootOnlyPlugin {
    fn scope(&self) -> PluginScope {
        PluginScope::Root
    }
}

impl Plugin for ChildOnlyPlugin {
    fn scope(&self) -> PluginScope {
        // 任意非根作用域，包括嵌套子作用域
        PluginScope::Child
    }
}
```

作用域不匹配会在注册阶段返回 `ErrorKind::PluginScopeMismatch`。

`Builder::is_root()` / `Builder::depth()` 可用来查询当前作用域层级：

```rust
assert!(Builder::new().is_root());
assert_eq!(Builder::new().depth(), 0);
```

> 可运行示例：`examples/dynamic.rs`（步骤 3、6）。

### 3.6 插件内部：Configurator

`apply(&self, cfg: &mut Configurator<'_>)` 收到的 `Configurator` 是插件的注册窗口，能力覆盖 Builder 的注册面：`provide` / `provide_factory` / `provide_collect` / `provide_dynamic` / `require` / `try_require` / `require_all` / `require_all_recursive` / `require_dynamic` / `contains` / `has_plugin` / `plugin` / `plugins` / `plugin_with_config` / `on` / `off` / `on_ready` / `on_dispose`，并且**可以注册嵌套子插件**：

```rust
fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
    cfg.provide(MyInnerService)?;
    cfg.plugin(InnerPlugin)?; // 嵌套子插件，同样受 PluginScope 门禁
    cfg.plugins([PluginA, PluginB])?; // 批量注册子插件
    cfg.plugin_with_config(ConfiguredPlugin, MyConfig::default())?; // 子插件带配置
    cfg.on::<MyEvent, _>(FnEventHandler(|_, _| Ok(EventControl::Continue)))?;
    Ok(())
}
```

回滚语义：`apply` 返回错误时，本次新增的服务、hooks、事件订阅与嵌套插件**整体回滚**，不会留下半初始化状态；错误信息中保留内层插件名。

---

## 4. 服务

### 4.1 普通服务

```rust
builder.provide(MyService::new())?;

let service = ctx.require::<MyService>()?;
```

### 4.2 可选服务

```rust
if let Some(db) = ctx.try_require::<Database>()? {
    db.connect().await?;
}
```

### 4.3 多实现

```rust
builder.provide_collect(OpenAiProvider::new())?;
builder.provide_collect(ClaudeProvider::new())?;

let providers = ctx.require_all::<Arc<dyn LlmProvider>>()?;
```

`require_all` 只返回当前层局部集合，不继承父级。需要读取父级集合时使用 `require_all_recursive`：

```rust
let providers = ctx.require_all_recursive::<Arc<dyn LlmProvider>>()?;
// 顺序：先当前层，再沿父链向上
```

### 4.4 懒加载工厂

```rust
builder.provide_factory(|| {
    Ok(ExpensiveService::new())
})?;

let service = ctx.require::<ExpensiveService>()?;
```

- 第一次访问时创建
- 并发首次访问串行化：成功路径工厂至多执行一次，所有访问者拿到同一实例
- 成功结果缓存
- 失败不缓存，下次重试
- 工厂应为非阻塞纯计算（初始化锁跨工厂调用持有）

### 4.5 构建期可变引用

```rust
builder.provide_factory(|| Ok::<u32, Error>(0))?;
*builder.require_mut::<u32>()? += 1;
```

`require_mut` 只存在于 `Builder`；运行 `build()` 之后不存在框架可见的 `&mut` 服务路径。

### 4.6 运行时动态配置

`provide_dynamic` 允许在运行期修改配置，而不破坏 `build()` 之后的只读 DI 模型：

```rust
builder.provide_dynamic(42_u32)?;

let dynamic = ctx.require_dynamic::<u32>()?;
assert_eq!(*dynamic.read(), 42);

dynamic.set(7);
dynamic.update(|value| *value += 1);
assert_eq!(*dynamic.read(), 8);
```

装配期或插件 `apply` 阶段也可以直接取得同一个共享句柄：

```rust
let dynamic = builder.require_dynamic::<u32>()?;
// 或
let dynamic = cfg.require_dynamic::<u32>()?;
```

如果想手动构造动态值再注册 `Arc<DynamicValue<T>>`，也可直接使用 `DynamicValue::new`：

```rust
builder.provide(Arc::new(DynamicValue::new(initial_config)))?;
```

`provide_dynamic` 实际注册的是 `Arc<DynamicValue<T>>`，不占用原始 `T` 的服务槽位；子作用域也能通过父链读取同一个动态配置句柄。

> 注意：`DynamicValue` 底层使用 `RwLock`。如果写锁被 panic 污染，`read` / `write` / `set` / `update` 会直接 panic。

多字段需要同步变更时用 `write()` 拿独占引用，一次改完：

```rust
{
    let mut guard = dynamic.write();
    guard.field_a = 1;
    guard.field_b = 2;
}
```

> 可运行示例：`examples/dynamic.rs`（步骤 5）。

### 4.7 独立使用 ServiceRegistry

`ServiceRegistry` 是公开的服务注册表实现，通常由 `Builder` 内部使用；如果你需要脱离 `Builder` 独立维护一组服务，也可以直接使用：

```rust
use cordis::ServiceRegistry;

let mut registry = ServiceRegistry::new();
registry.provide(MyService)?;
registry.provide_factory(|| Ok::<_, Error>(ExpensiveService::new()))?;
registry.provide_collect(Provider::new())?;

let service = registry.get::<MyService>()?;
let maybe = registry.try_get::<OptionalService>()?;
let all = registry.all::<Provider>()?;

// 可变引用：普通服务与工厂均可（工厂会先物化再返回 &mut）
let mutable = registry.get_mut::<MyService>()?;

if registry.contains::<MyService>() {
    // ...
}

// 只有普通服务可以被 remove 取出
let value = registry.remove::<MyService>()?;
```

> 注意：`ServiceRegistry::all` 只返回本注册表的集合；`Builder` / `Context` 的 `require_all_recursive` 才会沿父链汇总。

---

## 5. Context / 子作用域

每个子作用域是一个新的 `Builder`：

```rust
let ctx: Context = rt.handle();
let mut agent = ctx.scope()?;

agent.provide(AgentConfig::new("planner"))?;
agent.plugin(PlannerPlugin)?;

let mut agent_rt = agent.build()?;
agent_rt.start().await?;
```

- 子作用域可以看到父级服务
- 子作用域服务对父级不可见
- 子作用域可以遮蔽父级服务
- 子作用域可以嵌套
- `Context` 是只读句柄，没有 `provide` / `plugin` / `on` / `off` / `require_mut` / `start` / `stop`；`spawn` 只登记后台任务，不修改服务注册表
- `Context::scope()` 返回 `Result<Builder, Error>`：父 `Runtime` 进入停止后返回 `ErrorKind::Stopping`；本层活跃子作用域计数达到上限（2^63 - 1）时返回 `ErrorKind::TooManyScopes`
- 停止可观测性与后台任务见 5.2 / 5.3，可运行示例：`examples/tasks.rs`

### 5.1 c-lite 租约

父 `Runtime::stop()` 会检查本层活跃子 `Builder` / `Runtime`：

```rust
let child_builder = ctx.scope()?;
// 父 stop 此时返回 ActiveScopes { count: 1, ids }，ids 为活跃子作用域清单
let err = rt.stop().await.unwrap_err();
assert!(matches!(err.kind, ErrorKind::ActiveScopes { count: 1, .. }));

drop(child_builder);
rt.stop().await?;
```

仅子 `Context` 句柄存活不阻塞父 `stop`；子 `Runtime` 即使 `stop()` 后未 `drop()` 仍占租约。

### 5.2 停止可观测性

`Context` 提供作用域树的观测面（只读，不改变生命周期语义）：

```rust
ctx.is_stopping();       // 本层是否已置停止标志
ctx.children();          // 活跃子作用域 context id 清单
let parent = ctx.parent(); // 父级句柄，根级为 None
```

典型用法——管理器轮询收口：

```rust
loop {
    match rt.stop().await {
        Ok(()) => break,
        Err(Error { kind: ErrorKind::ActiveScopes { ids, .. }, .. }) => {
            eprintln!("等待子作用域退出: {ids:?}");
        }
        Err(other) => return Err(other),
    }
}
```

### 5.3 后台任务与优雅停止（`tokio` feature，默认启用）

`Context::spawn` 登记的后台任务纳入本层生命周期（需在 tokio runtime 上下文内调用）：

```rust
let id = ctx.spawn(async move {
    // 长驻服务循环节点
    while !watcher.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    Ok(())
})?;
assert_eq!(ctx.task_count(), 1);
```

- `Runtime::stop()`：插件 `stop` 之后、dispose 之前排空任务，不设超时。
- `Runtime::stop_with_timeout(Duration)`：预算内排空，超时后强制取消未完成任务，以 `ErrorKind::TaskAborted { task_id }` 计入聚合错误。
- 任务返回 `Err` 时发出 `TaskFailed` 事件（旁路通知、沿父链冒泡），可提前 `builder.on::<TaskFailed, _>(...)` 订阅做监控。
- 本层停止后 `spawn` 返回 `ErrorKind::Stopping`；无 tokio 上下文时返回 `ErrorKind::NoTaskRuntime`。
- 任务 panic 不被捕获，停止排空时以 `ErrorKind::TaskFailed { task_id }` 上报。

```rust
// 典型服务端关停：给后台任务 5 秒优雅退出窗口
rt.stop_with_timeout(std::time::Duration::from_secs(5)).await?;
```

---

## 6. 生命周期 Hook

### 6.1 on_ready

```rust
builder.on_ready(SyncHook(|ctx: &Context| {
    let logger = ctx.require::<Logger>()?;
    logger.log("ready");
    Ok(())
}))?;
```

### 6.2 on_dispose

```rust
builder.on_dispose(SyncHook(|ctx: &Context| {
    Ok(())
}))?;
```

### 6.3 async hook

```rust
builder.on_ready(AsyncHook(|ctx: Context| async move {
    let db = ctx.require::<Database>()?;
    db.connect().await?;
    Ok(())
}))?;
```

### 6.4 自定义 LifecycleHook

`LifecycleHook` 也是公开 trait；需要封装带状态或复用逻辑的钩子时，可以直接实现：

```rust
#[async_trait]
impl LifecycleHook for MyHook {
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        let logger = ctx.require::<Logger>()?;
        logger.log("custom lifecycle hook");
        Ok(())
    }
}

builder.on_ready(MyHook)?;
builder.on_dispose(MyHook)?;
```

---

## 7. 事件系统

### 7.1 定义事件

```rust
struct UserMessage(String);
```

任何 `Send + Sync + 'static` 类型都可以作为事件。

### 7.2 注册 handler

```rust
builder.on::<UserMessage, _>(FnEventHandler(
    |event: &UserMessage, ctx: &Context| {
        println!("{}", event.0);
        Ok(EventControl::Continue)
    },
))?;
```

`FnEventHandler` / `AsyncFnEventHandler` 是便利包装；需要携带状态或复用逻辑时，为自己的类型实现 `EventHandler<E>` 即可，两者都是它的包装形式：

```rust
struct LoggingHandler;

#[async_trait]
impl EventHandler<UserMessage> for LoggingHandler {
    async fn handle(
        &self,
        event: &UserMessage,
        ctx: &Context,
    ) -> Result<EventControl, Error> {
        let logger = ctx.require::<Logger>()?;
        logger.log(&event.0);
        Ok(EventControl::Continue)
    }
}

builder.on::<UserMessage, _>(LoggingHandler)?;
```

### 7.3 异步 handler

```rust
builder.on::<UserMessage, _>(AsyncFnEventHandler(
    |event: &UserMessage, ctx: Context| async move {
        let llm = ctx.require::<Arc<dyn LlmProvider>>()?;
        let answer = llm.chat(&event.0).await?;
        Ok(EventControl::Continue)
    },
))?;
```

### 7.4 发出事件

```rust
let ctx = rt.handle();
ctx.emit(UserMessage("hello".into())).await?;
ctx.emit_parallel(UserMessage("hello".into())).await?;
```

### 7.5 取消订阅

只能在装配期取消——`Builder` 阶段或插件 `apply` 阶段的 `Configurator`（见 §3.6）；运行期 `Context` 没有 `off`：

```rust
let sub = builder.on::<UserMessage, _>(handler)?;
builder.off(sub)?;
```

`Subscription` 是 `Copy` 轻量句柄；**drop 句柄不会退订**，取消必须显式 `off`。

`off` 只能在该订阅所属的同一个 Builder / Context 上调用；跨 Builder 调用会返回 `ErrorKind::SubscriptionNotFound`：

```rust
let root_sub = root_builder.on::<UserMessage, _>(handler)?;
let mut child = root_ctx.scope()?;
let err = child.off(root_sub).unwrap_err(); // SubscriptionNotFound
```

### 7.6 父链冒泡

子作用域内 `emit` 会沿父链向上冒泡：

```text
子 Context handlers
  ↓
父级 Context handlers
  ↓
根 Context handlers
```

`EventControl::Bail` 会停止后续 handlers 和向上冒泡。并行模式（`emit_parallel` / `emit_notify_parallel`）下同层 handlers 已全部并发执行，`Bail` 只能停止向父链冒泡，无法撤回本层已开始的 handler。

### 7.7 旁路通知

`emit_notify` 适合横切事件：handler 失败不阻断主流程，但错误不会被静默吞掉：

```rust
let errors = ctx.emit_notify(ConfigChanged).await;
for error in errors {
    log::warn!("config event handler failed: {error}");
}
```

`emit_notify_parallel` 是并行版本。

`emit` / `emit_parallel` 保持原有严格错误传播语义；`emit_notify` / `emit_notify_parallel` 返回 `Vec<Error>`，调用方自行决定记录方式。

> 冒泡 / `off` / `Bail` / 严格错误 / notify / parallel 的可运行版本见 `examples/events.rs`。

---

## 8. 错误处理


统一使用：

```rust
Result<_, Error>
```

`Error` 是结构化类型：

```rust
pub struct Error {
    pub phase: Phase,
    pub plugin: Option<&'static str>,
    pub kind: ErrorKind,
    // source: Option<Box<dyn Error + Send + Sync + 'static>>
}
```

`Phase` 标注错误发生的生命周期阶段，全部取值：

```rust
Phase::Apply | Phase::Verify | Phase::Build | Phase::Start
Phase::Ready | Phase::Stop | Phase::Dispose | Phase::Event
```

构造与消费错误：

```rust
// 插件/hook 内构造框架错误
Err(Error::new(Phase::Start, ErrorKind::Other))?;

// 携带来源错误链：with_source 是关联函数，不是实例方法
let err = Error::with_source(
    Phase::Event,
    ErrorKind::Other,
    std::io::Error::other("disk"),
);

// 读取底层来源（source 可向下取到 std Error）
if let Some(source) = err.source() {
    eprintln!("底层错误: {source}");
}

// 聚合错误的展平消费
if err.is_multiple() {
    if let ErrorKind::Multiple(errors) = err.kind() {
        for sub in errors { eprintln!("子错误: {sub}"); }
    }
}

// 把错误标记到新的阶段/插件：只补 phase，不改变 kind、不覆盖已有内层插件名、保留错误链
let err = err.into_phase(Phase::Start, Some("my-plugin"));
```

常见 `ErrorKind`：

```rust
ErrorKind::ServiceNotFound
ErrorKind::ServiceAlreadyRegistered
ErrorKind::ServiceTypeMismatch { expected, found }
ErrorKind::SubscriptionNotFound
ErrorKind::PluginDependencyNotFound
ErrorKind::PluginNameAlreadyRegistered
ErrorKind::PluginDependencyCycle
ErrorKind::PluginScopeMismatch {
    plugin_name: String,
    expected: PluginScope,
    actual: PluginScope,
}
ErrorKind::ActiveScopes { count, ids }
ErrorKind::Stopping
ErrorKind::TooManyScopes
ErrorKind::NoTaskRuntime
ErrorKind::TaskFailed { task_id }
ErrorKind::TaskAborted { task_id }
ErrorKind::Other
ErrorKind::Multiple
```

- 事件并行派发聚合错误使用 `Phase::Event` + `ErrorKind::Multiple`
- `start()` 失败 fail-fast
- `stop()` 失败会继续清理并聚合为 `ErrorKind::Multiple`
- `start-after-stop` 是 no-op：Runtime 停止后不会再次启动插件
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用插件自身的 `stop`
- `stop()` 幂等：已停止的 `Runtime` 再次 `stop` 直接返回 `Ok`
- 被 `ActiveScopes` 拒绝的 `stop()` **不进入 stopped 状态**：子作用域照常工作，`ids` 定位阻塞方，清理完子 `Builder` / `Runtime` 后可重试停止
- `ErrorKind::Stopping` 同时覆盖停止后的 `scope()` 与 `spawn()` 拒绝
- `stop_with_timeout` 排空后台任务的总预算共享给全部任务；逐个超时/abort 记为 `TaskAborted`
- `Runtime` 的 `Drop` 不做任何异步清理；租约随 `ScopeLease` 字段析构归还，spawn 任务脱离管理（这是需要 SessionManager 类收口层的根因）

---

## 9. 配置与运行期变更

配置通常作为服务管理：

```rust
struct ConfigService {
    current: RwLock<Config>,
}

impl ConfigService {
    fn reload(&self) -> Result<(), Error> {
        let new = read_config("config.toml")?;
        *self.current.write().unwrap() = new;
        Ok(())
    }
}
```

修改配置后：

```rust
let config = ctx.require::<Arc<ConfigService>>()?;
config.reload()?;
ctx.emit(ConfigChanged).await?;
```

---

## 10. 非目标

当前架构明确不支持：

- 插件热加载
- 运行时动态卸载插件
- 完整 ConfigSchema 自动校验
- 服务拦截器 / 装饰器
- 多进程 / 跨进程事件

这些可以在业务层自行实现，或后续版本再补。

如果当前需要横切能力（如日志、鉴权、追踪），可以先通过“包装类型 + 新服务”手动实现。

---

## 11. 快速参考

```rust
use std::sync::Arc;

use cordis::{
    Builder, Context, Runtime, Configurator, Plugin, PluginScope,
    Dependency, PluginDependency,
    Event, EventControl, EventHandler, FnEventHandler, AsyncFnEventHandler,
    Subscription, LifecycleHook, SyncHook, AsyncHook,
    DynamicValue, ServiceRegistry,
    Error, ErrorKind, Phase,
};

let mut builder = Builder::new();
builder.plugin_with_config(MyPlugin, MyConfig::default())?;
builder.provide_collect(Arc::new(OpenAiProvider::new()) as Arc<dyn LlmProvider>)?;
builder.provide_dynamic(42_u32)?;
builder.verify_dependencies()?; // 可选：不消费的装配自检
let _ = builder.has_plugin("optional-plugin");
let _ = builder.require_dynamic::<u32>()?;

let mut rt = builder.build()?;
rt.start().await?;

let ctx = rt.handle();
let dynamic = ctx.require_dynamic::<u32>()?;
let _ = ctx.require_all_recursive::<Arc<dyn LlmProvider>>()?;
let _ = ctx.emit_notify(ConfigChanged).await;

rt.stop().await?;
```
