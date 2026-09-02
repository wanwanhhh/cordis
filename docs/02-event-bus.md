# EventBus / 事件系统设计

> 版本：0.1（已实现）
> 定位：在 Cordis 底座上增加类型化事件系统，让插件之间可以通过事件解耦。

---

## 1. 目标

为当前 Cordis 底座增加：

- 类型化事件
- 异步 handler
- 注册 / 取消订阅
- 事件冒泡（Scope 继承）
- `serial` / `parallel` 并发
- 错误聚合
- Runtime 无关的异步 API

事件系统不会替代服务，而是补充服务无法覆盖的“通知 / 广播 / 解耦”场景。

---

## 2. 设计原则

1. 事件必须类型化，不使用字符串键。
2. 事件 `emit` 是异步的。
3. handler 是异步的。
4. handler 绑定所在 Context，随 Context 销毁一起移除。
5. Scope 事件会向父级冒泡。
6. 不绑定具体 runtime，例如 tokio / async-std。
7. 支持 `serial` 和 `parallel` 两种执行模式。
8. `bail` 是短路语义，不是错误。

---

## 3. 核心类型

### 3.1 Event Marker

```rust
pub trait Event: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> Event for T {}
```

任何满足 `Send + Sync + 'static` 的类型都自动实现 `Event`，无需手动 `impl Event for MyEvent {}`。

### 3.2 EventControl

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventControl {
    Continue,
    Bail,
}
```

- `Continue`：继续执行后续 handler，并继续向上冒泡。
- `Bail`：停止当前 Scope 内后续 handler，也停止向上冒泡。

### 3.3 EventHandler

```rust
#[async_trait]
pub trait EventHandler<E>: Send + Sync + 'static {
    async fn handle(
        &self,
        event: &E,
        ctx: &Context,
    ) -> Result<EventControl, Error>;
}
```

### 3.4 FnEventHandler

同步闭包适配器：

```rust
pub struct FnEventHandler<F>(pub F);

ctx.on::<MyEvent, _>(FnEventHandler(|event: &MyEvent, _: &Context| {
    Ok(EventControl::Continue)
}))?;
```

### 3.5 Subscription

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Subscription {
    context_id: usize,
    handler_id: usize,
}
```

- `context_id` 保存所属 `ContextInner` 的全局唯一 ID。
- `handler_id` 是当前 Context 内的递增 ID。
- `off` 必须校验 `context_id`，避免跨 Context 误删订阅。

---

## 4. API 约定

### 4.1 Context

```rust
impl Context {
    pub fn on<E, H>(
        &mut self,
        handler: H,
    ) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>;

    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error>;

    pub async fn emit<E: Event>(
        &self,
        event: E,
    ) -> Result<(), Error>;

    pub async fn emit_parallel<E: Event>(
        &self,
        event: E,
    ) -> Result<(), Error>;
}
```

### 4.2 Scope

`Scope` 提供与 `Context` 一致的：

- `on`
- `off`
- `emit`
- `emit_parallel`

### 4.3 注册/注销与 Clone 的约束

- `on` / `off` 都是 `&mut self` 操作，必须在 `Context` / `Scope` 首次 `clone()` 前完成。
- `clone()` 后事件系统进入只读模式，仍然可以 `emit`，但不能继续注册或取消订阅。
- 如果 clone 后调用 `on` / `off`，会返回 `Error::ContextShared`。

---

## 5. 执行语义

### 5.1 serial

默认 `emit` 使用 `serial`：

```text
当前 Scope handlers
        ↓
父级 Scope handlers
        ↓
祖父级 Scope handlers
        ↓
...
```

- 执行顺序与注册顺序一致。
- 当前 Scope 的 handler 先执行，父级 later。
- 任一 handler 返回 `Bail` 时，立即停止后续 handler 和向上冒泡。
- handler 返回 `Err` 时，立即停止后续 handler，并**直接返回该错误**，不再包装为 `Multiple`。

### 5.2 parallel

```rust
ctx.emit_parallel(event).await?;
```

- 同一 Scope 内的 handlers 并发执行。
- Scope 内 handlers 全部结束后，再进入父级 Scope。
- `Bail` 只负责停止向上冒泡，不取消已经启动的同 Scope 并发任务。
- 并发执行使用 `futures::future::join_all` 或调用方提供的 executor，不绑定 tokio。
- parallel 模式收集所有 handler 的错误，最后统一返回 `Error::Multiple`。

---

## 6. Scope 冒泡规则

- `emit` 从当前 Scope 开始，向父级递归。
- 子级注册的 handler 先执行。
- 父级注册的 handler 后执行。
- `bail` 会停止向上冒泡。
- 父级 handler 不能删除或修改子级 Scope 内部的 handler。
- Scope 的所有 clone 都 drop 后，其内部 handler 随 `ContextInner` 一起释放。
- 只要还有任意一个 Scope clone 存活，其 handler 仍可被 emit。

---

## 7. 内部存储

### 7.1 存储结构

在 `ContextInner` 中增加：

```rust
event_handlers: Mutex<Vec<Arc<dyn ErasedEventHandler>>>,
```

```rust
#[async_trait]
trait ErasedEventHandler: Send + Sync {
    async fn call(
        &self,
        event: &dyn Any,
        ctx: &Context,
    ) -> Result<EventControl, Error>;
}
```

具体 handler 包装为：

```rust
struct TypedEventHandler<E: Event, H: EventHandler<E>> {
    handler: H,
    _event: PhantomData<E>,
}
```

`call` 将 `&dyn Any` downcast 为 `&E` 后调用用户 handler。

由于 handler 使用 `&self`，`emit_parallel` 可以：

1. 锁住 `event_handlers`
2. `clone()` 出一组 `Arc<dyn ErasedEventHandler>`
3. 释放锁
4. 用 `futures::future::join_all` 并发执行

这避免了 `Mutex<Vec<...>>` 无法为多个 handler 同时提供可变借用的问题。

### 7.2 订阅 ID

每个 handler 分配递增 `usize`，`Subscription` 保存该 ID。

### 7.3 `off`

- `off` 首先校验 `subscription.context_id` 是否等于当前 `ContextInner.context_id`。
- 如果上下文不匹配或订阅不存在，返回 `Error::SubscriptionNotFound`。
- 校验通过后，根据 `handler_id` 从 handler 集合移除。

---

## 8. 错误处理

- serial 模式：遇到第一个 `Err` 立即停止后续 handler，直接返回该错误。
- parallel 模式：收集所有 handler 的错误，最后统一返回 `Error::Multiple(Vec<Error>)`。
- `Bail` 是短路语义，不是错误。
- 即使发生错误，也尽量执行已注册的 handler。
- `off` 对不存在的订阅或跨 Context 订阅返回 `Error::SubscriptionNotFound`。

---

## 9. Runtime 无关性

当前 async 生命周期已经通过 `async fn` 保持 runtime 无关。

EventBus 同样保持 runtime 无关：

- `emit` 返回 `Future`
- `parallel` 使用 `futures` 或调用方传入并发执行策略
- 不在库内部依赖 tokio

---

## 10. 与 Scope / Context 的关系

事件系统不是独立 service，而是 **Context 内建能力**：

- 与 `on_ready` / `on_dispose` 并列
- 与 Scope 继承体系一致
- 与插件生命周期共同管理
- 随 Context 销毁自动清理 handler

---

## 11. 迁移步骤

1. 新增 `Event` / `EventControl` / `EventHandler` 类型
2. `ContextInner` 增加 `event_handlers`
3. 抽象 `ErasedEventHandler`
4. 实现 `Context::on` / `off`
5. 实现 `Context::emit`（serial 冒泡）
6. 实现 `Scope` 的透传方法
7. 实现 `emit_parallel`
8. 补测试
9. 补示例
10. 更新文档和 README

---

## 12. 预期测试

- 事件类型化注册和 emit
- handler 异步执行
- Scope 事件冒泡到父级
- Scope 子级 handler 先于父级执行
- `Bail` 短路
- handler 错误聚合
- `off` 取消订阅
- `emit_parallel` 并发执行
- Scope 销毁后 handler 不再触发
- 父级 handler 不会被子级复制

---

## 13. 非目标

- 不实现消息队列 / MQ
- 不实现跨进程事件
- 不引入具体 runtime
- 不改变现有服务模型
- 不替代 `on_ready` / `on_dispose`
