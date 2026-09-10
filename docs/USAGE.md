# Cordis 使用指南

> 面向使用者：如何用当前 `cordis` crate 搭建插件化 Rust 应用。

---

## 目录

1. [引入依赖](#1-引入依赖)
2. [最小应用](#2-最小应用)
3. [插件](#3-插件)
4. [服务](#4-服务)
5. [Context / 子作用域](#5-context--子作用域)
6. [生命周期 Hook](#6-生命周期-hook)
7. [事件系统](#7-事件系统)
8. [错误处理](#8-错误处理)
9. [配置与运行期变更](#9-配置与运行期变更)
10. [非目标](#10-非目标)
11. [快速参考](#11-快速参考)

---

## 1. 引入依赖

```toml
[dependencies]
cordis = { path = "../cordis" }
futures = "0.3"       # 本文档示例用 futures::executor::block_on 驱动
async-trait = "0.1"   # 异步插件实现 start/stop 需要（#[async_trait]）

# 只有用到 Context::spawn / stop_with_timeout 时才需要：它们要求你自己的 tokio
# runtime 上下文（spawn 走 Handle::try_current），§5.3 的 select! 还需要 macros。
# 用 #[tokio::main] 入口再加 "rt-multi-thread"（它默认建多线程 runtime）。
tokio = { version = "1", features = ["rt", "time", "macros"] }
```

`cordis` 默认启用 `tokio` feature，它决定 `Context::spawn`（及 `TaskHandle`）是否可用；`spawn` 要求处于 tokio runtime 上下文。`Runtime::stop_with_timeout` **始终存在**——只是没有 spawn 任务时它没有可排空的对象，预算不起作用；有任务时它用 `tokio::time` 计时，要求 runtime 启用 time driver。不需要后台任务时：

```toml
cordis = { path = "../cordis", default-features = false }
```

生命周期本身不绑定 async runtime——`Builder` / `Runtime` 的控制方法是普通 async fn，可用 `futures::executor`、`tokio`、`async-std` 等任意 executor 驱动；`Context::cancelled()` 同样与 runtime 无关。**但 `Context::spawn` 与 `stop_with_timeout` 的排空计时只支持 tokio**：前者必须在 tokio runtime 上下文内调用，后者用 `tokio::time` 计时。框架当前不提供 executor 抽象。

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

        // 服务通过只读 Context 句柄取出
        let ctx = rt.handle();
        let _service = ctx.require::<MyService>()?;

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
    cfg.plugins([PluginA, PluginB])?; // 批量注册；I: IntoIterator<Item = P>，元素须同类型
    cfg.plugin(PluginC)?;              // 异构插件请逐个注册（数组字面量要求同类型）
    cfg.plugin_with_config(ConfiguredPlugin, MyConfig::default())?; // 子插件带配置
    cfg.on::<MyEvent, _>(FnEventHandler(|_: &MyEvent, _: &Context| Ok(EventControl::Continue)))?;
    Ok(())
}
```

回滚语义：`apply` 返回错误**或 panic 展开**时，本次新增的服务、hooks、事件订阅与嵌套插件**整体回滚**，不会留下半初始化状态；错误信息中保留内层插件名。

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
// 集合服务以注册时的**精确类型**为键：多实现必须统一成同一个 trait object 类型，
// 否则 require_all 查不到（静默返回空集合，不报错）。
builder.provide_collect(Arc::new(OpenAiProvider::new()) as Arc<dyn LlmProvider>)?;
builder.provide_collect(Arc::new(ClaudeProvider::new()) as Arc<dyn LlmProvider>)?;

let providers = ctx.require_all::<Arc<dyn LlmProvider>>()?;
```

`require_all` 只返回当前层局部集合，不继承父级。需要读取父级集合时使用 `require_all_recursive`：

```rust
let providers = ctx.require_all_recursive::<Arc<dyn LlmProvider>>()?;
// 顺序：先当前层，再沿父链向上
```

> 可运行示例：`examples/services.rs`（集合服务 + 懒加载工厂 + 动态配置的正确写法）。

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

> 注意：`DynamicValue` 底层使用 `RwLock`。持锁线程 panic 造成的锁中毒被容忍（与框架其余部分一致）：`read` / `write` / `set` / `update` 获取中毒态锁并继续工作，返回中毒时刻的数据，不会把单次用户 panic 放大为读路径崩溃。`read` / `write` 返回的是**锁守卫**（`Deref` / `DerefMut`）而不是数据快照：不要跨 `await` 持有（守卫非 `Send`，跨 `await` 会编译失败）；框架热路径无锁，但每次读动态配置都要拿一次 `RwLock`，成本由使用者自担。

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
- `Context::scope()` 返回 `Result<Builder, Error>`：父 `Runtime` 进入停止后返回 `ErrorKind::Stopping`
- 停止可观测性、取消信号与后台任务见 5.2 / 5.3 / 5.4，可运行示例：`examples/tasks.rs`（子作用域与嵌套的作用域树另见 `examples/scopes.rs`）

### 5.1 c-lite 租约

「c-lite」指本框架采用的轻量租约计数模型：子作用域的存活以父级 `Gate` 里的一个租约计数表示，父级停止时只检查这个计数。父 `Runtime::stop()` 会检查本层活跃子 `Builder` / `Runtime`：

```rust
let child_builder = ctx.scope()?;
// 父 stop 此时返回 ActiveScopes { count: 1, ids }，ids 为活跃子作用域清单
let err = rt.stop().await.unwrap_err();
assert!(matches!(err.kind, ErrorKind::ActiveScopes { count: 1, .. }));

drop(child_builder);
rt.stop().await?;
```

仅子 `Context` 句柄存活不阻塞父 `stop`；子 `Runtime` 即使 `stop()` 后未 `drop()` 仍占租约。

### 5.2 停止可观测性与取消信号

`Context` 提供作用域树的观测面，以及一个可等待的停止信号：

```rust
ctx.is_stopping();         // 本层是否已进入停止流程（Stopping / Stopped）
ctx.children();            // 活跃子作用域 context id 清单
ctx.id();                  // 本层 context id，与父 children() 里列出的值同源
let parent = ctx.parent(); // 父级句柄，根级为 None

ctx.cancelled().await;     // 电平触发：已停止或已收到停止请求时立即完成
```

`cancelled()` 的触发点有两处，都在框架内部：进入关闭流程那一刻（**早于**插件 `stop` 与任务排空），以及显式的停止请求（见 5.4）。因此它不会漏掉任何停止路径，长驻任务应当用它，而不是「轮询 `is_stopping()` + 猜一个间隔」。它不依赖 tokio，可在任意 executor 上等待。

信号是**本层**的：`ctx.cancelled()` 只在本层进入停止或收到本层请求时完成，父层不会代子层广播。这不构成缺口——父 `stop` 本就被活跃子租约挡住，子层只能由它自己的 `stop` 收口，那一刻信号就会触发。

`id()` 让框架的 `children()` 清单与应用自己的表直接对上——子作用域 id 在 `scope()` 那一刻就已登记，先用 `Builder::id()` 记下来即可。

典型用法——管理器轮询收口：

```rust
loop {
    match rt.stop().await {
        Ok(()) => break,
        Err(Error { kind: ErrorKind::ActiveScopes { ids, .. }, .. }) => {
            eprintln!("等待子作用域退出: {ids:?}");
            // `enter_stopping` 失败是同步返回的，必须让出执行权再重试，
            // 否则这个循环会空转打满一个核。（下面用 tokio 的定时器让出执行权；
            // 前文的 `cancelled()` / 生命周期 API 本身与 runtime 无关，换用你所用
            // executor 的等价让出方式即可。）
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err(other) => return Err(other),
    }
}
```

### 5.3 后台任务与优雅停止（`tokio` feature，默认启用）

`Context::spawn` 登记的后台任务纳入本层生命周期（需在 tokio runtime 上下文内调用），返回 `TaskHandle`：

```rust
let wait_ctx = ctx.clone();
let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
let task = ctx.spawn(async move {
    loop {
        // 长驻循环等取消信号，而不是轮询停止标志
        tokio::select! {
            () = wait_ctx.cancelled() => break,
            _ = tick.tick() => { /* 干正事 */ }
        }
    }
    Ok(())
})?;

assert_eq!(task.id(), 0);        // 本作用域内唯一，从 0 递增
assert!(!task.is_finished());
rt.stop().await?;                // 排空：等它收到信号后自然退出
```

这段需要你自己的 tokio runtime（`Context::spawn` 内部用 `Handle::try_current()`，因此必须在 runtime 上下文中调用；`select!` 还要求 `tokio` 的 `macros` feature）。依赖见 §1，可用入口见 `examples/tasks.rs`（其中是不含 `interval` 的等价写法）。

- `Runtime::stop()`：插件 `stop` 之后、dispose 之前排空任务，不设超时。
- `Runtime::stop_with_timeout(Duration)`：预算内排空，超时后强制取消未完成任务，以 `ErrorKind::TaskAborted { task_id }` 计入聚合错误。预算按**每次调用**计算，中断后重入会拿到新的完整预算。排空计时使用 `tokio::time`，存在待排空任务时要求当前 runtime 启用 time driver。预算大到 `Instant` 无法表示（如 `Duration::MAX`）时按「不设超时」处理，而不是返回错误——需要真正的上界就别传天文数字。
- `stop()` 可续跑：中途丢弃 stop future 后重入会从断点继续（含任务排空段），累计错误跨重入保留。
- `TaskHandle` 提供 `id()` / `is_finished()` / `abort()` / `wait()`；**丢弃句柄不等于取消**（它不是 guard，drop 后任务照常运行）。
- `TaskHandle::wait()` 返回任务结局：正常结束 `Ok(())`；任务体返回 `Err` 时原样返回该错误；panic 为 `ErrorKind::TaskFailed`；被取消为 `Ok(())`——取消是请求，不是失败。
- **两种取消语义不同**：owner 通过 `TaskHandle::abort()` 主动取消**不计入**停止错误；只有排空预算耗尽的取消才上报 `ErrorKind::TaskAborted`。
- 任务返回 `Err` 时发出 `TaskFailed { task_id: u64, error: Arc<Error> }` 事件（旁路通知、沿父链冒泡），可提前 `builder.on::<TaskFailed, _>(...)` 订阅做监控。
- 本层停止后 `spawn` 返回 `ErrorKind::Stopping`；无 tokio 上下文时返回 `ErrorKind::NoTaskRuntime`。
- `task_count()` 统计尚未落定结局的任务，含仍在运行、正被排空的那一个。
- 任务 panic 被捕获为结局，停止排空时以 `ErrorKind::TaskFailed { task_id }` 上报；panic hook 照常输出。`TaskFailed` 事件的 handler 自己 panic 也不会让停止挂起——上报路径同样被兜住，只是该任务会被记为 panic 结局。

```rust
// 典型服务端关停：给后台任务 5 秒优雅退出窗口
rt.stop_with_timeout(std::time::Duration::from_secs(5)).await?;
```

### 5.4 停止请求（`StopHandle`）

`Runtime::stop_handle()` 返回可 `Clone` 的 `StopHandle`，把「请求停止」的能力显式授出，适合信号处理任务、管理端点、测试超时兜底这些拿不到 `Runtime` 的位置：

```rust
let handle = rt.stop_handle();   // Clone + Send + Sync
handle.request_stop();           // 幂等：广播取消信号 + 置位请求标记
assert!(handle.is_stop_requested());
assert!(!ctx.is_stopping());     // 请求不等于进入清理

// owner 侧等信号（也可以直接 stop），再执行真正的关闭
rt.handle().cancelled().await;
rt.stop().await?;
```

- `request_stop()` 只做两件事：置位请求标记、唤醒全部 `cancelled()` 等待者。**不**执行清理，也**不**让 `is_stopping()` 变 true、不拒绝 `scope()` / `spawn()`——「请求」与「已进入清理」必须分开，否则拒绝新工作的依据会在租约检查之前就被置位。
- 刻意不放在 `Context` 上：那等于给每个插件环境权限。谁能停，应当是 owner 显式授出的能力。
- `StopHandle::cancelled()` 与 `Context::cancelled()` 语义相同；关闭仍然只有一条路径——持有 `Runtime` 的 owner 调用 `stop()` / `stop_with_timeout()`。

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

异步 handler 返回的 future 必须是 `'static`，所以**不能在 `async move` 里借用事件引用**——要在闭包体内先把需要的数据取成拥有所有权的值：

```rust
builder.on::<UserMessage, _>(AsyncFnEventHandler(
    |event: &UserMessage, ctx: Context| {
        let text = event.0.clone(); // 先克隆，future 不再借用 event
        async move {
            let llm = ctx.require::<Arc<dyn LlmProvider>>()?;
            let answer = llm.chat(&text).await?;
            Ok(EventControl::Continue)
        }
    },
))?;
```

> 可运行示例：`examples/events.rs`。

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
    // source: Option<Arc<dyn Error + Send + Sync + 'static>>（私有；用 source() 访问）
}
```

`Phase` 标注错误发生的生命周期阶段，全部取值：

```rust
Phase::Apply | Phase::Verify | Phase::Build | Phase::Require | Phase::Start
Phase::Ready | Phase::Stop | Phase::Dispose | Phase::Event
```

`Build` 与 `Require` 的区别：同一个 `ServiceNotFound`，装配期 `Builder::require` 失败报 `Build`，运行期 `Context::require` 失败报 `Require`，便于排障定位。

构造与消费错误：

```rust
// 插件/hook 内构造框架错误
return Err(Error::new(Phase::Start, ErrorKind::Other));

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

启动失败与重入：

```rust
let err = rt.start().await.unwrap_err();
// 首次失败就是聚合错误，可直接遍历子错误
if let ErrorKind::Multiple(errors) = &err.kind {
    for sub in errors { eprintln!("启动子错误: {sub}"); }
}

// 也可从 Runtime 查回（同一份错误，共享 source 链）
if let Some(aggregate) = rt.start_error() {
    eprintln!("启动失败: {aggregate}");
}

// 失败后重入被拒绝，根因挂在 source 链上
let reentry = rt.start().await.unwrap_err();
assert!(matches!(reentry.kind, ErrorKind::StartFailed));
if let Some(aggregate) = reentry.source().and_then(|s| s.downcast_ref::<Error>()) {
    assert!(matches!(aggregate.kind, ErrorKind::Multiple(_)));
}
```

启动失败后的回收：`Failed` 是显式状态，`stop()` 会回收已进入启动流程的插件，所以「启动失败也要收口」是稳定的几行。框架刻意不提供 `start_or_cleanup()`：那会引入一个必须长期维护的错误契约（清理失败与启动失败谁当主错），省下的只是样板。

```rust
/// 应用侧范式，不是框架 API：两个错误都不丢。
async fn start_or_rollback(rt: &mut Runtime) -> Result<(), Error> {
    let Err(start_err) = rt.start().await else {
        return Ok(());
    };
    match rt.stop().await {
        Ok(()) => Err(start_err),
        // 清理也失败：两者都保留，谁都不覆盖谁。
        Err(cleanup_err) => Err(Error::new(
            Phase::Start,
            ErrorKind::Multiple(vec![start_err, cleanup_err]),
        )),
    }
}
```

常见 `ErrorKind`：

```rust
ErrorKind::ServiceNotFound
ErrorKind::ServiceAlreadyRegistered
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
ErrorKind::NoTaskRuntime
ErrorKind::TaskFailed { task_id }
ErrorKind::TaskAborted { task_id }
ErrorKind::StartFailed
ErrorKind::Other
ErrorKind::Multiple
```

- 事件并行派发聚合错误使用 `Phase::Event` + `ErrorKind::Multiple`
- `start()` 失败 fail-fast，并进入失败态：插件 / ready 阶段的首次失败按原样返回 `ErrorKind::Multiple`，同时可由 `Runtime::start_error()` 查回（`Error` 可 `Clone`，两份共享同一条错误链）。依赖图问题（缺失依赖 / 环）在 `build()` 阶段就以 `Err` 返回，因此不存在「已 build 成功却在 `start` 时调度失败」的 Runtime
- 失败后重入 `start()` 返回 `ErrorKind::StartFailed`，**不再静默返回 `Ok`**，根因挂在 `source` 链上；插件不会被再次启动，`stop()` 仍可回收已启动插件
- 上一次 `start` 被中途丢弃（future 取消）会停在未完成态，重入同样返回 `ErrorKind::StartFailed`，此时没有 `source`
- `stop()` 可续跑：中途丢弃 stop future 后重入会从断点继续，不报假成功；因此插件 `stop` 应能承受一次中断后重入
- `stop()` 失败会继续清理并聚合为 `ErrorKind::Multiple`
- `start-after-stop` 是 no-op：Runtime 停止后不会再次启动插件
- `start()` 返回 `Ok` 只表示「运行时不再需要启动」——`Running` 是幂等 no-op，`Stopping`/`Stopped` 是停止后的 no-op；**它不等于本次调用完成了启动**。只有启动尝试本身出问题才返回 `Err`（见上两条）
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用插件自身的 `stop`（`build()` 成功后从未启动的 Runtime 属于这一类）
- `stop()` 幂等：已停止的 `Runtime` 再次 `stop` 直接返回 `Ok`
- 被 `ActiveScopes` 拒绝的 `stop()` **不进入 stopped 状态**：子作用域照常工作，`ids` 定位阻塞方，清理完子 `Builder` / `Runtime` 后可重试停止
- `ErrorKind::Stopping` 同时覆盖停止后的 `scope()` 与 `spawn()` 拒绝
- `stop_with_timeout` 排空后台任务的总预算共享给全部任务；逐个超时/abort 记为 `TaskAborted`
- `Runtime` 的 `Drop` 不做任何异步清理；租约随 `ScopeLease` 字段析构归还，spawn 任务脱离管理（这是需要一层管理器统一持有并 `stop` 各子 `Runtime` 的根因）
- `Drop` 只做护栏：debug 构建下若曾进入启动流程却未 `stop`，会 `debug_assert!` 硬失败，release 下静默

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

以下片段省略了 `MyPlugin` / `MyConfig` / `LlmProvider` 等占位类型，只示范框架 API 的写法；`TaskHandle` / `spawn` / `select!` 相关的行需要 tokio 上下文。

```rust
use std::sync::Arc;

use cordis::{
    Builder, Context, Runtime, Configurator, Plugin, PluginScope,
    Dependency, PluginDependency,
    Event, EventControl, EventHandler, FnEventHandler, AsyncFnEventHandler,
    Subscription, LifecycleHook, SyncHook, AsyncHook,
    TaskFailed, TaskHandle, StopHandle,
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

let scope = ctx.scope()?;            // 子作用域
let child_id = scope.id();           // 与 ctx.children() 同源
let _ = ctx.id();
drop(scope);                         // 释放租约，否则父 stop 会报 ActiveScopes

let task: TaskHandle = ctx.spawn(async { Ok(()) })?;  // 需 tokio 上下文
let _ = task.id();
task.abort();                        // owner 主动取消：不计入停止错误
let _ = task.wait().await;

let stop: StopHandle = rt.stop_handle();  // 可 Clone，交给就近触发的位置
stop.request_stop();
ctx.cancelled().await;               // 电平触发的停止信号

rt.stop().await?;
```
