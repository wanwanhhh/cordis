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
futures = "0.3"       # 仅示例用：驱动生命周期 future
async-trait = "0.1"   # 插件实现 start/stop 时需要 #[async_trait]

# 仅在用到 Context::spawn / 带预算排空 / select! 时需要
tokio = { version = "1", features = ["rt", "time", "macros"] }
```

- `cordis` 默认启用 `tokio` feature，它决定 `Context::spawn`（及 `TaskHandle`）是否可用；`spawn` 必须在 tokio runtime 上下文内调用（内部走 `Handle::try_current()`）。
- `Runtime::stop_with_timeout` 的排空计时用 `tokio::time`；没有 spawn 任务时预算不起作用。`#[tokio::main]` 入口再补 `rt-multi-thread`。
- 生命周期本身不绑定 runtime：`Builder` / `Runtime` 的控制方法、`cancelled()` / `stopped()` 都是普通 async，可用 `futures::executor`、tokio、async-std 等任意 executor。框架不提供 executor 抽象。
- 不做后台任务时可关闭默认 feature：`cordis = { path = "../cordis", default-features = false }`。

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
        let mut builder = Builder::new();        // 装配期：可变注册
        builder.plugin(MyPlugin)?;

        let mut rt = builder.build()?;            // 唯一冻结点
        rt.start().await?;                        // 生命周期唯一所有者

        let ctx = rt.handle();                    // 只读句柄，可 Clone
        let _service = ctx.require::<MyService>()?;

        rt.stop().await.into_result()?;
        Ok(())
    })
}
```

`Builder` 负责装配；`build()` 冻结；`Runtime` 是 owner（不 `Clone`）；`Context` 是只读句柄（`Clone + Send + Sync`）。`Builder` 与 `ServiceRegistry` 都实现 `Default`，`Builder::default()` 等价 `Builder::new()`。

### 2.1 装配自检与可恢复构建

`verify()` 不消费 `Builder`，提前跑完 `build()` 的全部校验（服务依赖、插件依赖、循环依赖）：

```rust
builder.verify()?; // 等价于 verify_dependencies()；失败后 Builder 仍可用
```

`build()` 校验失败时随 `self` 一起消耗；需要「拿回半成品继续改」用 `try_build()`，失败时把 `Builder`（连租约）完整带回：

```rust
match builder.try_build() {
    Ok(rt) => { /* 正常使用 */ }
    Err((builder, err)) => {
        eprintln!("装配校验失败: {err}"); // 修正后仍可再 build
    }
}
```

装配期接口：

- `builder.plugins(iter)`：批量注册同类型插件，逐个失败即中止。
- `builder.require::<T>()` / `try_require::<T>()`：读已注册服务（局部 + 父链），后者返回 `Option<&T>`。
- `builder.require_dynamic::<T>()`：拿 `Arc<DynamicValue<T>>` 共享句柄。
- `builder.has_plugin(name)`：插件是否已注册（局部 + 父链）。
- `builder.contains::<T>()` / `ctx.contains::<T>()`：存在性检查（普通服务 / 工厂 / 集合，局部 + 父链），不报错。注意集合服务**不算**满足单例 `Dependency`。

> 回滚与 `try_build` 带回修正的可运行版本见 `examples/dynamic.rs`（步骤 1–2）。

---

## 3. 插件

### 3.1 基础 Plugin

```rust
#[async_trait]
impl Plugin for MyPlugin {
    fn name(&self) -> &'static str { "my-plugin" }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> { Ok(()) }

    async fn start(&self, ctx: &Context) -> Result<(), Error> { Ok(()) }

    async fn stop(&self, ctx: &Context) -> Result<(), Error> { Ok(()) }
}
```

| 方法 | 时机 | 用途 |
|---|---|---|
| `apply` | 装配期，同步 | 注册服务 / hook / 事件 / 嵌套插件 |
| `start` | `Runtime::start()`，异步 | 初始化资源，失败即 fail-fast |
| `stop` | `Runtime::stop()`，异步 | 清理资源（`start` 未成功也可能被调用） |

> 覆盖了 `start` / `stop` 等异步方法就需要 `#[async_trait]`；只实现 `apply` 可省略。

只实现 `apply` 时可直接传闭包：

```rust
builder.plugin(|cfg: &mut Configurator<'_>| {
    cfg.provide(MyService)?;
    Ok(())
})?;
```

### 3.2 插件元信息

```rust
impl Plugin for MyPlugin {
    fn name(&self) -> &'static str { "my-plugin" }   // 同层必须唯一
    fn version(&self) -> &'static str { "0.1.0" }
    fn priority(&self) -> i32 { 10 }
}
```

`name()` 在同一 `Builder` / `Context` 内必须唯一，重复返回 `ErrorKind::PluginNameAlreadyRegistered`。

`priority` 参与 `start()` 与 `start_serial()` 的拓扑选点；但默认 `start()` 的分层只由依赖决定，同层插件之间不保证 priority / 注册序总序——插件间顺序以 `plugin_dependencies` 为唯一契约。需要旧的串行总序时用 `Runtime::start_serial()`。

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

插件依赖会拓扑排序，循环依赖报错。两个结构体字段公开（`type_id` / `name` / `optional`、`plugin_name` / `optional`），可手动构造，通常用上面的构造函数。

可选插件依赖配合 `has_plugin` 判断目标是否存在；该方法在 `Builder`、`Configurator`、`Context` 上都可用，范围都是「本层 + 父链」：

```rust
if ctx.has_plugin("optional-plugin") {
    // 启用可选能力
}
```

> 示例：`examples/dynamic.rs`（步骤 4）。

### 3.4 插件配置

```rust
builder.plugin_with_config(MyPlugin, MyConfig { model: "gpt-4o".into() })?;
```

```rust
fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
    let config = cfg.require::<MyConfig>()?;
    Ok(())
}
```

> 示例：`examples/dynamic.rs`（步骤 4）。

### 3.5 插件作用域

默认插件可装在根或子作用域；用 `scope()` 限制：

```rust
use cordis::PluginScope;

fn scope(&self) -> PluginScope { PluginScope::Root }   // 只能装在根
fn scope(&self) -> PluginScope { PluginScope::Child }  // 任意非根（含嵌套子作用域）
```

不匹配在注册阶段返回 `ErrorKind::PluginScopeMismatch`。查询当前层级用 `Builder::is_root()` / `Builder::depth()`：

```rust
assert!(Builder::new().is_root());
assert_eq!(Builder::new().depth(), 0);
```

> 示例：`examples/dynamic.rs`（步骤 3、6）。

### 3.6 插件内部：Configurator

`apply` 收到的 `Configurator` 是注册窗口，覆盖 Builder 的注册面，并且**可以注册嵌套子插件**：

```rust
fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
    cfg.provide(MyInnerService)?;
    cfg.plugin(InnerPlugin)?;             // 嵌套子插件，同样受 PluginScope 门禁
    cfg.plugins([PluginA, PluginB])?;     // 批量；元素须同类型
    cfg.plugin_with_config(ConfiguredPlugin, MyConfig::default())?;
    cfg.on::<MyEvent, _>(FnEventHandler(|_: &MyEvent, _: &Context| Ok(EventControl::Continue)))?;
    Ok(())
}
```

可用方法：`provide` / `provide_factory` / `provide_collect` / `provide_dynamic` / `require` / `try_require` / `require_all` / `require_all_recursive` / `require_dynamic` / `contains` / `has_plugin` / `plugin` / `plugins` / `plugin_with_config` / `on` / `off` / `on_ready` / `on_dispose` / `on_closing`。

回滚：`apply` 返回错误**或 panic 展开**时，本次新增的服务、hooks、事件订阅、嵌套插件整体回滚，不留半初始化状态；错误中保留内层插件名。

---

## 4. 服务

### 4.1 普通 / 可选服务

```rust
builder.provide(MyService::new())?;
let service = ctx.require::<MyService>()?;                 // 缺失即 Err

if let Some(db) = ctx.try_require::<Database>()? {         // 缺失为 None
    db.connect().await?;
}
```

### 4.2 多实现（集合服务）

集合以注册时的**精确类型**为键：多实现必须统一成同一个类型，否则 `require_all` 静默返回空集合：

```rust
builder.provide_collect(Arc::new(OpenAiProvider::new()) as Arc<dyn LlmProvider>)?;
builder.provide_collect(Arc::new(ClaudeProvider::new()) as Arc<dyn LlmProvider>)?;

let local = ctx.require_all::<Arc<dyn LlmProvider>>()?;             // 只本层
let all = ctx.require_all_recursive::<Arc<dyn LlmProvider>>()?;     // 本层 + 沿父链向上
```

> 示例：`examples/services.rs`。

### 4.3 懒加载工厂

```rust
builder.provide_factory(|| Ok(ExpensiveService::new()))?;
let service = ctx.require::<ExpensiveService>()?;   // 首次访问时创建
```

首次访问串行化：成功路径工厂至多执行一次、结果缓存、所有访问者拿到同一实例；失败不缓存，下次重试。工厂应为非阻塞纯计算（初始化锁跨工厂调用持有）。

### 4.4 构建期可变引用

```rust
builder.provide_factory(|| Ok::<u32, Error>(0))?;
*builder.require_mut::<u32>()? += 1;
```

`require_mut` 只在 `Builder` 上存在；`build()` 之后没有框架可见的 `&mut` 服务路径。

### 4.5 运行时动态配置

`provide_dynamic` 注册 `Arc<DynamicValue<T>>`，运行期可改，不破坏 `build()` 后的只读 DI：

```rust
builder.provide_dynamic(42_u32)?;

let dynamic = ctx.require_dynamic::<u32>()?;   // Builder / Configurator 上也可取
assert_eq!(*dynamic.read(), 42);
dynamic.set(7);
dynamic.update(|v| *v += 1);
assert_eq!(*dynamic.read(), 8);
```

`provide_dynamic` 不占用原始 `T` 的服务槽位；子作用域可经父链读到同一句柄。也可手动注册：`builder.provide(Arc::new(DynamicValue::new(initial)))?`。

多字段要一致变更时用 `write()` 一次改完：

```rust
{
    let mut guard = dynamic.write();
    guard.field_a = 1;
    guard.field_b = 2;
}
```

注意：

- `read` / `write` 返回**锁守卫**（`Deref` / `DerefMut`）而非快照；守卫非 `Send`，不要跨 `await` 持有。
- 锁中毒被容忍：`read` / `write` / `set` / `update` 会取出中毒态数据继续工作，不把单次用户 panic 放大成读路径崩溃。
- 每次读都拿一次 `RwLock`，成本自担。

> 示例：`examples/dynamic.rs`（步骤 5）。

### 4.6 独立使用 ServiceRegistry

`ServiceRegistry` 通常由 `Builder` 内部使用，也可脱离 Builder 独立维护一组服务：

```rust
use cordis::ServiceRegistry;

let mut registry = ServiceRegistry::new();
registry.provide(MyService)?;
registry.provide_factory(|| Ok::<_, Error>(ExpensiveService::new()))?;
registry.provide_collect(Provider::new())?;

let service = registry.get::<MyService>()?;
let maybe = registry.try_get::<OptionalService>()?;
let all = registry.all::<Provider>()?;                 // 只本注册表
let mutable = registry.get_mut::<MyService>()?;        // 工厂会先物化再返回 &mut
if registry.contains::<MyService>() { /* ... */ }
let value = registry.remove::<MyService>()?;           // 只有普通服务可 remove
```

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

规则：

- 子可见父级服务；父不可见子；子可遮蔽父服务；子可再嵌套。
- `Context` 只读：没有 `provide` / `plugin` / `on` / `off` / `require_mut` / `start` / `stop`；`spawn` 只登记后台任务，不改服务注册表。
- `Context::scope()` 返回 `Result<Builder, Error>`，父 `Runtime` 进入停止后返回 `ErrorKind::Stopping`。

> 示例：`examples/tasks.rs`（子作用域）、`examples/scopes.rs`（嵌套作用域树）。

### 5.1 c-lite 租约与 `Blocked`

子作用域存活以父级 `ScopeCore` 里的租约表示；父 `stop()` 发现仍有活跃子 `Builder` / `Runtime` 时返回 `StopOutcome::Blocked`——**不是错误**，是「前置条件未满足、可重试」：

```rust
let child_builder = ctx.scope()?;
let StopOutcome::Blocked(blockers) = rt.stop().await else { panic!() };
assert_eq!(blockers.len(), 1);          // 阻塞方清单：ids() / len() / is_empty() / describe()
assert!(rt.handle().is_stopping());     // 意图已提交：拒绝新工作

drop(child_builder);
rt.stop().await.into_result()?;         // 回收后重试即续跑
```

被阻塞时已进入 `Closing`（拒绝新 `scope` / `spawn`、广播取消），只是清理未开始；意图不可逆，没有「恢复运行」的路径。

仅子 `Context` 句柄存活不阻塞父 `stop`；子 `Runtime` 走完停止流程即归还租约（随即 `Stopped`），未 `stop` 就被 `drop` 的子仍占租约。

### 5.2 停止可观测性与信号

```rust
ctx.is_stopping();         // 是否已提交关闭（Closing / Stopping / Stopped）
ctx.is_stopped();          // 是否已进入 Stopped 终态
ctx.children();            // 尚未归还租约的子作用域 ScopeId 清单
ctx.id();                  // 本层 ScopeId，与父 children() 同源
ctx.parent();              // 父级句柄，根为 None

ctx.cancelled().await;     // Cancelled：「开始收尾」：提交停止 / 收到停止请求 / owner 遗弃时完成
ctx.stopped().await;       // Settled：「已停稳」，结局 Settlement::{Stopped, Abandoned}
```

- 两个信号层级不同：`cancelled()` 早于插件 `stop` 与排空，长驻任务应在它上面退出；`stopped()` 等清理全部完成。两者都不依赖 tokio。
- 信号是**本层**的，父层不代子层广播。父 `stop` 被活跃子租约挡住时也已提交意图，子层由它自己的 `stop` 收口。
- `stopped()` 的 `Abandoned` 来自 owner 半途丢弃 `Runtime`（例如从未 `stop`），保证等待者不会永久挂起。
- `id()` 让 `children()` 清单与应用自己的表对上：子 id 在 `scope()` 那刻已登记，先存 `Builder::id()` 即可。

等子作用域真正停稳再收口父级：

```rust
rt.stop().await;                    // 提交父级停止意图（可能 Blocked）
for id in ctx.children() {
    child_ctx.stopped().await;      // 按 id 从自己的表取回 Context
}
rt.stop().await.into_result()?;     // 子级停稳后重试
```

### 5.3 后台任务与优雅停止（`tokio`，默认启用）

`Context::spawn` 把任务纳入本层生命周期（需 tokio 上下文），返回 `TaskHandle`：

```rust
let wait_ctx = ctx.clone();
let task = ctx.spawn(async move {
    loop {
        tokio::select! {                       // 长驻循环等取消信号，别轮询
            () = wait_ctx.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => { /* 干正事 */ }
        }
    }
    Ok(())
})?;

assert_eq!(task.id().get(), 0);   // 本作用域内唯一，从 0 递增
assert!(!task.is_finished());
rt.stop().await.into_result()?;   // 排空：等它收到信号后自然退出
```

`TaskHandle`：`id()` / `is_finished()` / `abort()` / `wait()`。**丢弃句柄不等于取消**（不是 guard，drop 后任务照常运行）。

停止：

- `Runtime::stop()`：插件 `stop` 之后、dispose 之前排空任务，不设超时。
- `Runtime::stop_with_timeout(d)`：预算内排空，超时强制取消并记 `ErrorKind::TaskAborted`。预算在**进入清理时**算定、随清理 future 存续，重入不重置；`d` 大到 `Instant` 无法表示（如 `Duration::MAX`）按「不设超时」处理。
- 清理体是 `Runtime` 自持的 future：丢弃 `stop()` 的 await 不中断清理，重入继续 poll 同一个 future，插件 `stop` 与 dispose 只被调用一次。
- `TaskHandle::wait()` 返回完整 `TaskOutcome`：

| 结局 | 含义 | 是否计入停止错误 |
|---|---|---|
| `Completed` | 正常跑完 | 否 |
| `Failed(Error)` | 任务体返回 `Err` | 经 `TaskFailed` 事件上报 |
| `Panicked(PanicInfo)` | panic，`message` 保留载荷 | 排空时上报 |
| `Aborted(Owner)` | `TaskHandle::abort()` | 否（取消是请求） |
| `Aborted(Timeout)` | 排空预算耗尽 | 是，`TaskAborted` |
| `Aborted(HostShutdown)` | 宿主丢弃 future | 否 |

需要旧的 `Result` 视图用 `into_result(task_id)`；`is_success()` 判断是否正常跑完；`drain_error(task_id)` 是按结局类别投影的排空视图（`Failed` 若已由 `TaskFailed` 事件送达则为 `None`）；要区分取消成因直接 match。取消成因类型是 `AbortReason::{Owner, Timeout, HostShutdown}`；`task_id` 为不透明 `TaskId`。

- 任务返回 `Err` 发出 `TaskFailed { task_id, error }` 事件（内联、沿父链冒泡），可提前 `builder.on::<TaskFailed, _>(...)` 订阅监控。该上报若被截断（handler panic、超时在 await 点取消、宿主丢弃 future），排空会补报一次 `TaskFailed`，不会因为「结局是 `Failed`」就当作已送达。
- 本层停止后 `spawn` 返回 `ErrorKind::Stopping`；无 tokio 上下文返回 `ErrorKind::NoTaskRuntime`。
- `task_count()` 统计尚未落定结局的任务（含正被排空的那个）。

```rust
// 典型服务端关停：给后台任务 5 秒优雅退出窗口
rt.stop_with_timeout(std::time::Duration::from_secs(5)).await.into_result()?;
```

### 5.4 停止请求（`StopHandle`）

`Runtime::stop_handle()` 返回可 `Clone + Send + Sync` 的 `StopHandle`，把「请求停止」的能力显式授出，适合信号处理、管理端点、测试兜底：

```rust
let handle = rt.stop_handle();
handle.request_stop();               // 幂等：广播取消 + 置位请求标记
assert!(handle.is_stop_requested());
assert!(!ctx.is_stopping());         // 请求 ≠ 进入清理

rt.handle().cancelled().await;       // owner 侧等信号，再执行真正的关闭
rt.stop().await.into_result()?;
```

`request_stop()` 只置位 + 唤醒 `cancelled()` 等待者：**不**清理、不置 `is_stopping()`、不拒绝 `scope()` / `spawn()`。它刻意不放在 `Context` 上——谁能停应由 owner 显式授出。`StopHandle::cancelled()` 与 `Context::cancelled()` 语义相同。

### 5.5 受管子作用域注册表（`ScopeRegistry`，可选工具）

常驻服务按名字不停开关子作用域（一个会话一个）时用它收口并发正确性：

```rust
use cordis::{ScopeRegistry, CloseStatus};

let registry = ScopeRegistry::new();
registry.bind(&root_ctx)?;                     // 通常在父层 on_ready 里绑定一次

// 并发同名只有一个赢家，其余拿到 AlreadyExists
let session_ctx = registry.open("session-1", |builder| {
    builder.provide(SessionState::new())?;
    builder.plugin(SessionPlugin)?;
    Ok(())
}).await?;

// 每次关闭带预算，返回 CloseReport（status + outcome）
let report = registry.close("session-1", Duration::from_secs(5)).await?;
match report.status {
    CloseStatus::Clean => {}
    CloseStatus::Aborted => warn!("强杀任务: {:?}", report.aborted_tasks()),
    CloseStatus::Failed => warn!("关闭有错: {:?}", report.outcome.errors()),
    CloseStatus::Blocked => warn!("被活跃子作用域挡住，回收孙级后重试 close"),
}

// 停机：先收口全部子级，再停父级，返回 CloseAllReport
let all = registry.close_all(Duration::from_secs(10)).await;
assert!(all.is_clean(), "{all:?}");
root_rt.stop().await.into_result()?;
```

要点：

- **认领先于构建**：`open` 先在锁内占名，占住才 `ctx.scope()`；并发输家从未创建作用域，无需回滚。
- **启动失败自动收干净**：半成品 `stop` + drop，名字释放可重试。**例外**：回收 `stop` 被活跃孙作用域 `Blocked` 时，owner 以 `Closing(Some)` 保留、名字不释放，再 `open` 同名得 `AlreadyExists`——先回收孙级，再对同名 `close` 续跑。
- **`Blocked` 不是终态**：被阻塞的子级仍由注册表持有（`Runtime` 不析构，插件 `stop` 不丢），回收孙级后重试 `close` 即续跑；期间 `get` 返回 `None`，并发 `close` 得 `RegistryError::Busy`。
- **父级通常不会被 Blocked（前提是无在飞 `open`）**：`close_all` 串行关光全部子级，返回后不会再出现可管理的 `Running`。但一个已认领未发布的 `open` 可能已持有父级租约，要等它发布复查停机并回滚后才归还；这段窗口里停父级会瞬时 `Blocked`。要一次停干净，先确认所有 `open` future 已结束，或对父级 `stop` 重试。此时 `close_all` 会对该名字拿到 `Busy` 并计入 `failed`，`is_clean()` 为 `false`。
- **所有权唯一**：注册表在 `Mutex` 里独占持有 `Runtime`，对外只给只读 `Context`，因此不可 `Clone`，但可按 `&self` 跨任务使用。查询用 `get(name)`（仅 `Running` 时授出）、`names()`、`is_shutting_down()`。
- 需要 `tokio`（关闭预算用 `tokio::time`）；只依赖框架公开 API，可原样搬进独立 crate。

---

## 6. 生命周期 Hook

### 6.1 on_ready / on_dispose

```rust
builder.on_ready(SyncHook(|ctx: &Context| {
    let logger = ctx.require::<Logger>()?;
    logger.log("ready");
    Ok(())
}))?;

builder.on_dispose(SyncHook(|_ctx: &Context| Ok(())))?;
```

异步版本用 `AsyncHook`：

```rust
builder.on_ready(AsyncHook(|ctx: Context| async move {
    let db = ctx.require::<Database>()?;
    db.connect().await?;
    Ok(())
}))?;
```

`on_ready` 在所有插件 `start()` 成功后执行；`on_dispose` 在 `stop()` 时始终执行（插件 `stop` 与任务排空之后）。

需要带状态或复用逻辑时直接实现 `LifecycleHook`：

```rust
#[async_trait]
impl LifecycleHook for MyHook {
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        ctx.require::<Logger>()?.log("custom lifecycle hook");
        Ok(())
    }
}

builder.on_ready(MyHook)?;
builder.on_dispose(MyHook)?;
```

### 6.2 on_closing（本层开始关闭时收尾）

在本层**开始关闭**那刻（提交 `stop`、进入 `Closing`）同步调用一次，早于插件 `stop`、任务排空与 dispose。它是 `ctx.cancelled()` 的 push 形态，不需要常驻任务。回调同步——不能 `await`，也不能在其中等 `ctx.stopped()`（等于等自己）。

```rust
// 装配期注册，随本层存续
builder.on_closing(|ctx: &Context| {
    // 此刻所有插件已 apply、服务未被回收，require 一定拿得到
    if let Ok(registry) = ctx.require::<SessionRegistry>() {
        registry.request_close_all();
    }
})?;

// 运行期注册返回 CloseHandle：丢弃即注销
let _handle = ctx.on_closing(move |_ctx: &Context| { /* ... */ });
```

回调类型是 `CloseHook`（同步 `Fn(&Context)`；需要带状态的类型直接实现它）。

要点：

- **每层至多一次、单向不可复位**：挂在首次 `begin_close` 上，不是可反复置位的轮次开关；「这一轮开始/停止」的业务周期请留在应用侧。
- **逐层**：子层关闭不触发父层回调，父层也不代子层触发。
- **回调 panic 不打断停止**：被隔离为 `ErrorKind::LifecyclePanicked`（`phase == Phase::Close`）计入 `StopOutcome`；插件 `stop` / dispose 的 panic 用同一变体。例外：本层已 `Stopped` 之后再注册并立即执行的回调 panic，清理 future 已结束、无人读暂存，只经 panic hook 输出。
- **恰好一次**：已提交关闭后再注册会立即同步调用一次，派发期间注册的并入下一轮，不丢不重。链式注册（钩子执行中触发、或已开始派发后注册）有三道上界（轮次、嵌套深度、总数），触顶不再执行并记一条 `ErrorKind::CloseHookDispatchOverflow`，因此自增殖/分支式回调不会栈溢出或指数膨胀。装配期声明的、以及层存活期间普通代码的动态注册不计入上界。
- **owner 遗弃 `Runtime`（不 `stop` 直接 drop）时不触发**：`Drop` 只置状态、唤醒 `stopped()` 等待者（`Settlement::Abandoned`），不跑用户代码。要收尾就得让 owner 调 `stop`。

---

## 7. 事件系统

### 7.1 定义与注册

```rust
struct UserMessage(String);   // 任何 Send + Sync + 'static 类型都可作事件

builder.on::<UserMessage, _>(FnEventHandler(
    |event: &UserMessage, ctx: &Context| {
        println!("{}", event.0);
        Ok(EventControl::Continue)
    },
))?;
```

需要状态或复用逻辑时实现 `EventHandler<E>`（`FnEventHandler` / `AsyncFnEventHandler` 都是它的包装）：

```rust
struct LoggingHandler;

#[async_trait]
impl EventHandler<UserMessage> for LoggingHandler {
    async fn handle(&self, event: &UserMessage, ctx: &Context) -> Result<EventControl, Error> {
        ctx.require::<Logger>()?.log(&event.0);
        Ok(EventControl::Continue)
    }
}

builder.on::<UserMessage, _>(LoggingHandler)?;
```

### 7.2 异步 handler

异步 handler 的 future 必须 `'static`，**不能在 `async move` 里借用事件引用**——先取成拥有所有权的值：

```rust
builder.on::<UserMessage, _>(AsyncFnEventHandler(
    |event: &UserMessage, ctx: Context| {
        let text = event.0.clone();          // 先克隆，future 不再借用 event
        async move {
            let llm = ctx.require::<Arc<dyn LlmProvider>>()?;
            let answer = llm.chat(&text).await?;
            Ok(EventControl::Continue)
        }
    },
))?;
```

### 7.3 发出事件与取消订阅

```rust
ctx.emit(UserMessage("hello".into())).await?;            // 串行、严格错误传播
ctx.emit_parallel(UserMessage("hello".into())).await?;   // 同层并发
```

取消订阅只在装配期（`Builder` 或 `apply` 里的 `Configurator`），运行期 `Context` 没有 `off`：

```rust
let sub = builder.on::<UserMessage, _>(handler)?;
builder.off(sub)?;
```

`Subscription` 是 `Copy` 轻量句柄；**drop 不退订**，必须显式 `off`。`off` 只能在该订阅所属的同一 Builder / Context 上调用，跨 Builder 返回 `ErrorKind::SubscriptionNotFound`。

### 7.4 父链冒泡与 Bail

子作用域 `emit` 沿父链向上冒泡：子 → 父 → 根。`EventControl::Bail` 停止后续 handler 与向上冒泡；并行模式下同层 handler 已全部并发执行，`Bail` 只能停止向父链冒泡，无法撤回本层已开始的 handler。

### 7.5 旁路通知

`emit_notify` 适合横切事件：handler 错误不阻断主流程，但错误不被静默吞掉，调用方仍要等全部 handler 跑完：

```rust
let errors = ctx.emit_notify(ConfigChanged).await;
for error in errors {
    log::warn!("config event handler failed: {error}");
}
```

`emit_notify_parallel` 是并行版本。订阅者会做 IO、可能慢或卡住时用 `notify`——发完即返回：

```rust
let receipt: Receipt = ctx.notify(ConfigChanged).await;   // delivered / dropped / rejected
if receipt.dropped > 0 {
    for backlog in ctx.notify_stats() {   // Vec<Backlog>，按订阅者查积压 / 丢弃
        log::warn!("subscriber #{}: queued={} dropped={} rejected={}",
            backlog.handler_id, backlog.queued, backlog.dropped, backlog.rejected);
    }
}
```

`notify` 的语义边界：

- 每订阅者一条有界 FIFO lane，内部按收到先后处理，跨订阅者互不干扰；慢订阅者只堆自己的 lane。
- lane 容量 256，**满了丢新来的**并按订阅者计数；绝不阻塞发射方。
- 不提供跨订阅者 / 跨层的短路——需要确定性短路或同步拿错误用 `emit` / `emit_parallel`。
- 需要 tokio（worker 驱动）；无 tokio 构建退化为「调用方同步跑完 handler」，`delivered` 即实跑数。
- 停止时把已入队积压处理完再收尾；停止后再投递计入 `receipt.rejected`。
- `TaskFailed` **不走** `notify`（任务失败的唯一出口，丢不起，走内联上报）。

> 冒泡 / `off` / `Bail` / 严格错误 / notify / parallel 的可运行版本见 `examples/events.rs`。

---

## 8. 错误处理

统一返回 `Result<_, Error>`。`Error` 是结构化类型：

```rust
pub struct Error {
    pub phase: Phase,
    pub plugin: Option<&'static str>,
    pub kind: ErrorKind,
    // source: Option<Arc<dyn Error + Send + Sync + 'static>>（私有；用 source() 访问）
}
```

`Phase` 取值：`Apply` / `Verify` / `Build` / `Require` / `Start` / `Ready` / `Close` / `Stop` / `Dispose` / `Event`。同一个 `ServiceNotFound`，装配期 `Builder::require` 报 `Build`，运行期 `Context::require` 报 `Require`。

构造与消费：

```rust
// 构造
return Err(Error::new(Phase::Start, ErrorKind::Other));

// 携带来源错误链（with_source 是关联函数）
let err = Error::with_source(Phase::Event, ErrorKind::Other, std::io::Error::other("disk"));
if let Some(source) = err.source() { eprintln!("底层错误: {source}"); }

// 聚合错误展平（err.is_multiple() 是便捷判断）
if err.is_multiple()
    && let ErrorKind::Multiple(errors) = err.kind()
{
    for sub in errors { eprintln!("子错误: {sub}"); }
}

// 补阶段 / 插件名：不改 kind、不覆盖已有内层插件名、保留错误链
let err = err.into_phase(Phase::Start, Some("my-plugin"));
```

`ErrorKind` 标注 `#[non_exhaustive]`，下游 `match` 请保留兜底分支。常见变体：

```rust
ErrorKind::ServiceNotFound              ErrorKind::ServiceAlreadyRegistered
ErrorKind::PluginNameAlreadyRegistered  ErrorKind::PluginDependencyNotFound
ErrorKind::PluginDependencyCycle        ErrorKind::SubscriptionNotFound
ErrorKind::PluginScopeMismatch { plugin_name, expected, actual }
ErrorKind::StopBlocked(Blockers)        ErrorKind::Stopping
ErrorKind::NoTaskRuntime                ErrorKind::TaskFailed { task_id }
ErrorKind::TaskAborted { task_id }      ErrorKind::LifecyclePanicked { message }
ErrorKind::CloseHookDispatchOverflow    ErrorKind::StartFailed
ErrorKind::Other                        ErrorKind::Multiple(Vec<Error>)
```

启动失败与重入：

```rust
let err = rt.start().await.unwrap_err();
if let ErrorKind::Multiple(errors) = &err.kind {
    for sub in errors { eprintln!("启动子错误: {sub}"); }
}

// 也可从 Runtime 查回（同一份错误，共享 source 链）
if let Some(aggregate) = rt.start_error() { eprintln!("启动失败: {aggregate}"); }

// 失败后重入被拒绝，根因挂在 source 链上
let reentry = rt.start().await.unwrap_err();
assert!(matches!(reentry.kind, ErrorKind::StartFailed));
```

启动失败后的回收：`Failed` 是显式状态，`stop()` 会回收已进入启动流程的插件。框架刻意不提供 `start_or_cleanup()`（那会引入长期维护的错误契约）。应用侧范式：

```rust
/// 应用侧写法，不是框架 API：两个错误都不丢。
async fn start_or_rollback(rt: &mut Runtime) -> Result<(), Error> {
    let Err(start_err) = rt.start().await else { return Ok(()) };
    match rt.stop().await.into_result() {
        Ok(()) => Err(start_err),
        Err(cleanup_err) => Err(Error::new(
            Phase::Start,
            ErrorKind::Multiple(vec![start_err, cleanup_err]),
        )),
    }
}
```

其它行为要点：

- `start()` fail-fast：插件 / ready 首次失败返回 `ErrorKind::Multiple`，同时可由 `Runtime::start_error()` 查回（`Error` 可 `Clone`，两份共享同一条错误链）。依赖图问题（缺失依赖 / 环）在 `build()` 阶段就返回 `Err`。
- 失败后重入 `start()` 返回 `ErrorKind::StartFailed`（根因在 `source` 链上），插件不会被再次启动；上一次 `start` 被中途丢弃（future 取消）时同样返回它，但没有 `source`。
- `start-after-stop` 是 no-op：停止后不会再次启动插件。`start()` 返回 `Ok` 只表示「不再需要启动」，不等于本次完成了启动。
- `stop()` 幂等：已停止的 `Runtime` 再 `stop` 返回 `StopOutcome::Stopped { errors: [] }`。
- 未 `start` 就 `stop` 只跑 dispose hooks，不调用插件 `stop`。
- `stop()` 失败仍继续清理并聚合为 `ErrorKind::Multiple`；被活跃子作用域挡住则返回 `StopOutcome::Blocked`（不是错误，语义与重试见 §5.1）。
- `Runtime` 的 `Drop` 不做异步清理：已 `Stopped` 的在终态即归还租约，未 `stop` 的随字段析构归还、spawn 任务脱离管理（这是需要一层管理器统一持有并 `stop` 各子 `Runtime` 的根因）。`Drop` 会**关闭**未到 `Stopped` 的本层（提交 `Closing`、广播取消，`stopped()` 等待者拿到 `Abandoned`）；debug 下若曾进入启动流程却未到 `Stopped` 还会硬失败，消息区分「从未 stop」「被活跃子作用域阻塞（列出 ids）」「stop 已提交但清理未走完」三种成因，release 静默。

---

## 9. 配置与运行期变更

配置通常作为服务管理：

```rust
struct ConfigService {
    current: std::sync::RwLock<Config>,
}

impl ConfigService {
    fn reload(&self) -> Result<(), Error> {
        let new = read_config("config.toml")?;
        *self.current.write().unwrap() = new;
        Ok(())
    }
}
```

修改后广播：

```rust
ctx.require::<Arc<ConfigService>>()?.reload()?;
ctx.emit(ConfigChanged).await?;
```

---

## 10. 非目标

当前架构明确不支持：

- 插件热加载、运行时动态卸载插件
- 完整 ConfigSchema 自动校验
- 服务拦截器 / 装饰器
- 多进程 / 跨进程事件

这些可在业务层自行实现，或后续版本再补。需要横切能力（日志、鉴权、追踪）时，可先用「包装类型 + 新服务」手动实现。

---

## 11. 快速参考

以下片段省略占位类型，只示范 API 写法；`TaskHandle` / `spawn` / `select!` 相关行需要 tokio 上下文。

```rust
use std::sync::Arc;

use cordis::{
    Builder, Context, Runtime, Configurator, Plugin, PluginScope,
    Dependency, PluginDependency,
    Event, EventControl, EventHandler, FnEventHandler, AsyncFnEventHandler,
    Subscription, LifecycleHook, SyncHook, AsyncHook, CloseHandle,
    TaskFailed, TaskHandle, StopHandle,
    DynamicValue, ServiceRegistry,
    Error, ErrorKind, Phase,
};

let mut builder = Builder::new();
builder.plugin_with_config(MyPlugin, MyConfig::default())?;
builder.provide_collect(Arc::new(OpenAiProvider::new()) as Arc<dyn LlmProvider>)?;
builder.provide_dynamic(42_u32)?;
builder.verify_dependencies()?;              // 不消费的装配自检
let _ = builder.has_plugin("optional-plugin");
let _ = builder.require_dynamic::<u32>()?;

let mut rt = builder.build()?;
rt.start().await?;

let ctx = rt.handle();
let dynamic = ctx.require_dynamic::<u32>()?;
let _ = ctx.require_all_recursive::<Arc<dyn LlmProvider>>()?;
let _ = ctx.emit_notify(ConfigChanged).await;  // 内联：等 handler 跑完，错误不丢
let _ = ctx.notify(ConfigChanged).await;       // 异步：发完即返回
let _ = ctx.notify_stats();                    // 按订阅者查积压 / 丢弃

let _handle = ctx.on_closing(|_ctx: &Context| { /* 本层开始关闭时收尾 */ });

let scope = ctx.scope()?;            // 子作用域
let child_id = scope.id();           // 与 ctx.children() 同源
let _ = ctx.id();
drop(scope);                         // 释放租约，否则父 stop 返回 Blocked

let task: TaskHandle = ctx.spawn(async { Ok(()) })?;  // 需 tokio 上下文
let _ = task.id();
task.abort();                        // owner 取消：不计入停止错误
let _ = task.wait().await;

let stop: StopHandle = rt.stop_handle();   // 可 Clone，交给就近触发的位置
stop.request_stop();
ctx.cancelled().await;               // 提交停止时触发（早于插件 stop 与排空）

rt.stop().await.into_result()?;
```
