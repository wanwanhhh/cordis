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

`Builder` 负责装配期可变注册；`build()` 是唯一冻结点；`Runtime` 是生命周期唯一所有者；`Context` 是只读句柄。

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

### 3.2 插件元信息

```rust
impl Plugin for MyPlugin {
    fn name(&self) -> &'static str { "my-plugin" }
    fn version(&self) -> &'static str { "0.1.0" }
    fn priority(&self) -> i32 { 10 }
}
```

`priority` 只在 `start_serial()` 的拓扑选点中生效；默认 `start()` 分层并行的同层插件之间不保证 priority/注册序顺序。插件间顺序以 `plugin_dependencies` 为唯一契约。需要旧串行总序时可使用 `Runtime::start_serial()`。

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

插件依赖会做拓扑排序；循环依赖报错。

可选插件依赖可以通过 `has_plugin` 判断目标插件是否存在：

```rust
if ctx.has_plugin("optional-plugin") {
    // 启用可选能力
}
```

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

### 3.5 插件作用域

默认插件可安装在根或子作用域。你可以限制插件安装位置：

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
- 成功结果缓存
- 失败不缓存，下次重试

### 4.5 构建期可变引用

```rust
builder.provide_factory(|| Ok::<u32, Error>(0))?;
*builder.require_mut::<u32>()? += 1;
```

`require_mut` 只存在于 `Builder`；运行 `build()` 之后不存在框架可见的 `&mut` 服务路径。

### 4.6 运行时动态配置

```rust
builder.provide_dynamic(42_u32)?;

let dynamic = ctx.require_dynamic::<u32>()?;
assert_eq!(*dynamic.read(), 42);

dynamic.set(7);
dynamic.update(|value| *value += 1);
assert_eq!(*dynamic.read(), 8);
```

`provide_dynamic` 实际注册的是 `Arc<DynamicValue<T>>`，不占用原始 `T` 的服务槽位；子作用域也能通过父链读取同一个动态配置句柄。

> 注意：`DynamicValue` 底层使用 `RwLock`。如果写锁被 panic 污染，`read` / `write` / `set` / `update` 会直接 panic。

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
- `Context` 是只读句柄，没有 `provide` / `plugin` / `on` / `start` / `stop`
- `Context::scope()` 返回 `Result<Builder, Error>`：父 `Runtime` 进入停止后返回 `ErrorKind::Stopping`

### 5.1 c-lite 租约

父 `Runtime::stop()` 会检查本层活跃子 `Builder` / `Runtime`：

```rust
let child_builder = ctx.scope()?;
// 父 stop 此时返回 ActiveScopes { count: 1 }
let err = rt.stop().await.unwrap_err();
assert!(matches!(err.kind, ErrorKind::ActiveScopes { count: 1 }));

drop(child_builder);
rt.stop().await?;
```

仅子 `Context` 句柄存活不阻塞父 `stop`；子 `Runtime` 即使 `stop()` 后未 `drop()` 仍占租约。

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

必须在 `Builder` 阶段取消：

```rust
let sub = builder.on::<UserMessage, _>(handler)?;
builder.off(sub)?;
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

`EventControl::Bail` 会停止后续 handlers 和向上冒泡。

### 7.7 旁路通知

`emit_notify` 适合横切事件：handler 失败不阻断主流程，但错误不会静默吞掉：

```rust
let errors = ctx.emit_notify(ConfigChanged).await;
for error in errors {
    log::warn!("config event handler failed: {error}");
}
```

`emit_notify_parallel` 是并行版本。`emit` / `emit_parallel` 保持原有严格错误传播语义。

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

常见 `ErrorKind`：

```rust
ErrorKind::ServiceNotFound
ErrorKind::ServiceAlreadyRegistered
ErrorKind::SubscriptionNotFound
ErrorKind::PluginDependencyNotFound
ErrorKind::PluginNameAlreadyRegistered
ErrorKind::PluginDependencyCycle
ErrorKind::PluginScopeMismatch { plugin_name, expected, actual }
ErrorKind::ActiveScopes { count }
ErrorKind::Stopping
ErrorKind::TooManyScopes
ErrorKind::Other
ErrorKind::Multiple
```

- 事件并行派发聚合错误使用 `Phase::Event` + `ErrorKind::Multiple`
- `start()` 失败 fail-fast
- `stop()` 失败会继续清理并聚合为 `ErrorKind::Multiple`
- `start-after-stop` 是 no-op：Runtime 停止后不会再次启动插件
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用插件自身的 `stop`

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
use cordis::{
    Builder, Context, Runtime, Configurator, Plugin, Dependency, PluginDependency,
    Event, EventControl, EventHandler, FnEventHandler, AsyncFnEventHandler,
    Subscription, LifecycleHook, SyncHook, AsyncHook,
    Error, ErrorKind, Phase,
};

let mut builder = Builder::new();
builder.plugin_with_config(MyPlugin, MyConfig::default())?;

let mut rt = builder.build()?;
rt.start().await?;
rt.stop().await?;
```
