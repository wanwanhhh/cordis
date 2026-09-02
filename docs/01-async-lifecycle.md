# 异步生命周期与线程安全设计

> 版本：0.2（已实现）
> 定位：将当前单线程同步 Cordis 底座升级为可支撑 Agent/LLM 场景的异步多线程底座。

---

## 1. 目标

把当前架构从：

```text
Rc + 同步 Plugin + 单线程
```

升级为：

```text
Arc + 异步生命周期 + Send + Sync + 多线程可共享
```

本设计不引入具体业务，只升级底层能力。

---

## 2. 设计原则

1. `apply` 保持同步，用于注册服务、声明依赖、注册回调。
2. `start` / `stop` 异步化。
3. `Context` 是可 `Clone` 的轻量句柄，内部使用 `Arc<ContextInner>`。
4. 所有服务必须为 `Send + Sync + 'static`。
5. 所有插件必须为 `Send + Sync + 'static`。
6. `Context::start()` / `stop()` 为 `async` 方法。
7. Scope 继承相同的异步生命周期语义。
8. 保持“静态集成、不热加载”的哲学。

---

## 3. Context 句柄模型

### 3.1 Context

```rust
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}
```

这是一个轻量句柄：

- `Context::clone()` 只增加 `Arc` 引用计数，成本很低。
- `Context` 可以跨线程、跨任务共享。
- `Context` 本身是 `Send + Sync`。

### 3.2 ContextInner

```rust
struct ContextInner {
    parent: Option<Arc<ContextInner>>,
    services: ServiceRegistry,
    plugins: Vec<Box<dyn Plugin>>,
    ready_hooks: Mutex<Vec<Box<dyn LifecycleHook>>>,
    dispose_hooks: Mutex<Vec<Box<dyn LifecycleHook>>>,
    start_called: bool,
    stopped: bool,
}
```

### 3.3 共享与可变性

- 所有配置、注册插件、注册回调、`start()` 都必须在第一次 `clone()` 之前完成。
- `clone()` 之后，`Context` 进入只读共享模式。
- 共享模式下的可变操作返回 `Error::ContextShared`：
  - `provide`
  - `plugin`
  - `plugins`
  - `on_ready`
  - `on_dispose`
  - `require_mut`
  - `start`
  - `stop`

```rust
let mut ctx = Context::new();
ctx.provide(MyService::new())?;
ctx.plugin(MyPlugin)?;
ctx.start().await?;

let shared = ctx.clone();

// 以下操作会返回 Error::ContextShared
// ctx.provide(...)
// ctx.plugin(...)
// ctx.start().await
// ctx.stop().await
```

注意：**不仅仅是 Scope 导致 ContextShared，`Context` 被 clone 后同样会导致可变操作失败。**

---

## 4. 生命周期 Hook

不使用裸 `Pin<Box<dyn Future>>`，改用 trait object：

```rust
#[async_trait]
pub trait LifecycleHook: Send + Sync + 'static {
    async fn call(&mut self, ctx: &mut Context) -> Result<(), Error>;
}
```

内部存储：

```rust
type ReadyHook = Box<dyn LifecycleHook>;
type DisposeHook = Box<dyn LifecycleHook>;
```

`ContextInner` 中使用：

```rust
ready_hooks: Mutex<Vec<ReadyHook>>,
dispose_hooks: Mutex<Vec<DisposeHook>>,
```

这样：

- `LifecycleHook` 自身满足 `Send + Sync`
- Hook 集合可以被安全存储
- `ContextInner` 可以满足 `Send + Sync`
- 用户实现生命周期回调变得简单，不需要手写 HRTB Future

---

## 5. ServiceRegistry

```rust
pub struct ServiceRegistry {
    services: HashMap<TypeId, StoredService>,
}

struct StoredService {
    type_name: &'static str,
    value: Box<dyn Any + Send + Sync>,
}
```

```rust
impl ServiceRegistry {
    pub fn provide<T>(&mut self, value: T) -> Result<(), Error>
    where
        T: Send + Sync + 'static;
}
```

所有服务必须满足：

```text
T: Send + Sync + 'static
```

---

## 6. Plugin

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn dependencies(&self) -> &'static [Dependency] {
        &[]
    }

    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    async fn stop(&self, ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}
```

约束：

- 插件必须 `Send + Sync + 'static`
- `apply` 保持同步
- `start` / `stop` 可以 `await`
- 闭包插件实现也要求：

```rust
F: Fn(&mut Context) -> Result<(), Error> + Send + Sync + 'static
```

---

## 7. Context 异步 API

### 7.1 生命周期

```rust
impl Context {
    pub async fn start(&mut self) -> Result<(), Error>;
    pub async fn stop(&mut self) -> Result<(), Error>;
}
```

### 7.2 生命周期回调

```rust
impl Context {
    pub fn on_ready(
        &mut self,
        hook: impl LifecycleHook,
    ) -> Result<(), Error>;

    pub fn on_dispose(
        &mut self,
        hook: impl LifecycleHook,
    ) -> Result<(), Error>;
}
```

### 7.3 依赖检查

```rust
pub fn verify_dependencies(&self) -> Result<(), Error>;
```

保持同步。

---

## 8. Scope 模型

### 8.1 Scope 可克隆

```rust
#[derive(Clone)]
pub struct Scope {
    ctx: Context,
}
```

- `Scope` 内部持有 `Context`。
- `Scope` 可以 `Clone`，用于跨任务共享。
- `Scope` clone 后，Scope 进入只读模式，可变操作返回 `Error::ContextShared`。

### 8.2 典型用法

```rust
// 1. 配置并启动根 Context
let mut ctx = Context::new();
ctx.provide(Config::new())?;
ctx.start().await?;

// 2. 配置并启动会话 Scope
let mut session = ctx.scope();
session.plugin(SessionPlugin)?;
session.start().await?;

// 3. 克隆到多个异步任务
let shared_session = session.clone();
tokio::spawn(async move {
    let service = shared_session.require::<SessionService>()?;
    // 只读使用
    service.run().await
});

// 4. 任务结束后，停止 Scope
session.stop().await?;
drop(session);
ctx.stop().await?;
```

### 8.3 嵌套 Scope

```rust
let mut session = ctx.scope();
let mut subflow = session.scope();
```

- 孙级 Scope 继承祖父级、父级服务。
- 孙级 Scope 遮蔽祖先服务。
- 任意后代存活时，祖先可变操作返回 `Error::ContextShared`。
- 推荐启动顺序：根 -> 会话 -> 子流程。
- 推荐停止顺序：子流程 -> 会话 -> 根。

---

## 9. 生命周期语义

### 9.1 start

```text
verify_dependencies
        ↓
所有插件 start().await
        ↓
ready hooks .await
```

- 依赖检查失败时，允许重试。
- 某个插件 `start()` 失败时，停止后续插件启动。
- ready hook 失败时，停止后续 hook。
- 调用方应显式调用 `stop().await` 清理。

### 9.2 stop

```text
插件 stop().await（逆序）
        ↓
dispose hooks .await
```

- 某个插件 `stop()` 失败时，继续逆序清理。
- dispose hooks 即使失败也继续执行。
- 所有错误聚合为 `Error::Multiple`。

---

## 10. 多线程要求

```text
Context: Send + Sync
Scope:   Send + Sync
Plugin:  Send + Sync
Service: Send + Sync
LifecycleHook: Send + Sync
```

服务需要跨线程共享可变状态时：

```rust
type Shared<T> = Arc<RwLock<T>>;
```

禁止使用：

```text
Rc<RefCell<T>>
```

---

## 11. 迁移步骤

1. `Rc` 替换为 `Arc`
2. `Context` 增加 `#[derive(Clone)]`
3. `ContextInner` 中 hooks 改用 `Mutex<Vec<Box<dyn LifecycleHook>>>`
4. 增加 `async-trait` 依赖
5. 定义 `LifecycleHook` trait
6. `ServiceRegistry` 增加 `Send + Sync` 约束
7. `Plugin` 增加 `Send + Sync + 'static`
8. `start` / `stop` 改为 `async`
9. `on_ready` / `on_dispose` 改为接收 `LifecycleHook`
10. `Scope` 增加 `#[derive(Clone)]`
11. 更新所有示例和测试
12. 跑 `cargo test --all-targets` 和 clippy

---

## 12. 预期测试

- 异步插件 `start` / `stop` 正确执行
- async ready / dispose 正确执行
- `LifecycleHook` trait object 可正常注册
- 异步 Scope 启动/停止
- 嵌套异步 Scope
- `Context` clone 后可变操作返回 `Error::ContextShared`
- `Scope` clone 后可变操作返回 `Error::ContextShared`
- 多线程下 `Arc<RwLock<T>>` 服务可共享
- 异步错误聚合
- doctest 保持通过

---

## 13. 非目标

- 不做插件热加载
- 不引入动态加载
- 不改变 `apply` 同步语义
- 不改变依赖声明方式
- 不改变 Scope 所有权和隔离规则
