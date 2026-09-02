# v0.3 服务增强、事件适配器与插件元信息设计

> 版本：0.1（已实现）
> 定位：在现有 Cordis 底座上补齐 Agent 场景最需要的表达能力。

---

## 1. 目标

本阶段完成三块能力：

1. **Service 增强**
   - `try_require`
   - `Collection<T>` / 多实现服务
   - 服务工厂 / 懒加载
   - 服务覆盖与装饰器（先留接口）

2. **AsyncFnEventHandler**
   - 异步事件闭包适配器

3. **插件元信息与优先级**
   - `name`
   - `version`
   - `priority`
   - 可选依赖

---

## 2. Service 增强

### 2.1 try_require

```rust
impl Context {
    pub fn try_require<T: 'static>(&self) -> Result<Option<&T>, Error>;
}
```

语义：

- 服务存在：返回 `Ok(Some(&T))`
- 服务不存在：返回 `Ok(None)`
- 类型不匹配等内部错误：返回 `Err`

Scope 中同样支持，且会检查父级服务。

### 2.2 Collection<T>

用于“同一接口多个实现”：

```rust
impl Context {
    pub fn provide_collect<T: Send + Sync + 'static>(
        &mut self,
        value: T,
    ) -> Result<(), Error>;

    pub fn require_all<T: 'static>(&self) -> Result<Vec<&T>, Error>;
}
```

语义：

- `provide_collect` 多次注册同类型实现
- `require_all` 只返回 **当前 Scope 局部集合**
- v0.3 **不继承父级集合**，与 Scope 隔离语义保持一致
- 后续如果需要“继承父级集合 + 局部追加”，再单独引入 `inherit` 模式

内部存储：

```rust
collections: HashMap<TypeId, Vec<Box<dyn Any + Send + Sync>>>,
```

注意：

- `contains_type` 目前只检查普通服务和工厂，不包含集合。
- 集合通常是“运行时查询”能力，不参与插件的依赖检查。

### 2.3 服务工厂 / 懒加载

```rust
impl Context {
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error>;
}
```

语义：

- 不立即创建实例
- 第一次 `require::<T>()` / `try_require::<T>()` 时创建
- 创建结果缓存
- 重复 `require` 返回同一个实例

### 2.3.1 内部存储

工厂存储在 `ServiceRegistry` 中：

```rust
struct ServiceRegistry {
    services: HashMap<TypeId, StoredService>,
    factories: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}
```

每个工厂类型擦除后保存为：

```rust
struct FactoryEntry<T> {
    factory: Box<dyn Fn() -> Result<T, Error> + Send + Sync>,
    value: OnceLock<T>,
}
```

### 2.3.2 冲突规则

- 同一 `TypeId` 只能注册一个普通服务 **或** 一个工厂。
- 重复注册返回 `Error::ServiceAlreadyRegistered`。
- 不允许“普通服务 + 工厂”同时注册。

### 2.3.3 失败语义

- 工厂第一次执行失败时，**不缓存错误**。
- 下一次 `require::<T>()` / `try_require::<T>()` 会重新执行工厂。
- 成功创建后会缓存实例，后续直接返回缓存。
- 并发首次访问时，**不保证工厂只执行一次**；但成功实例只会缓存一个。
- 如果工厂有外部副作用，应由调用方自行保证幂等。

### 2.3.4 try_require 与工厂联动

`try_require::<T>()` 的行为：

- 普通服务存在 → `Ok(Some(&T))`
- 工厂存在 → 触发工厂创建，成功返回 `Ok(Some(&T))`
- 两者都没有 → `Ok(None)`

`require::<T>()` 的行为：

- 普通服务存在 → `Ok(&T)`
- 工厂存在 → 触发工厂创建，成功返回 `Ok(&T)`
- 两者都没有 → `Err(ServiceNotFound)`

### 2.4 服务覆盖

暂不实现“替换已有服务”。  
当前保持：

```text
同 TypeId 只允许一个普通服务
```

### 2.5 服务装饰器

暂不实现完整拦截器。  
当前用“包装类型 + 新服务”手动实现：

```rust
struct LoggingLlm(Arc<dyn LlmProvider>);
```

---

## 3. AsyncFnEventHandler

### 3.1 设计

```rust
pub struct AsyncFnEventHandler<F>(pub F);

#[async_trait]
impl<E, F, Fut> EventHandler<E> for AsyncFnEventHandler<F>
where
    E: Event,
    F: Fn(&E, Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<EventControl, Error>> + Send + 'static,
{
    async fn handle(
        &self,
        event: &E,
        _ctx: &Context,
    ) -> Result<EventControl, Error> {
        let ctx = _ctx.clone();
        (self.0)(event, ctx).await
    }
}
```

### 3.2 用法

```rust
ctx.on::<UserMessage, _>(AsyncFnEventHandler(
    |event: &UserMessage, ctx: Context| async move {
        let llm = ctx.require::<Arc<dyn LlmProvider>>()?;
        let answer = llm.chat(&event.text).await?;
        Ok(EventControl::Continue)
    },
))?;
```

### 3.3 注意

- 回调接收克隆后的 `Context`
- 适合在异步 handler 中读取服务
- 不提供 `&mut Context`，避免与并发 handler 冲突

---

## 4. 插件元信息与优先级

### 4.1 新增方法

```rust
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn version(&self) -> &'static str {
        "0.0.0"
    }

    fn priority(&self) -> i32 {
        0
    }

    fn dependencies(&self) -> &'static [Dependency] {
        &[]
    }
}
```

### 4.2 可选依赖

`Dependency` 增加：

```rust
pub struct Dependency {
    pub type_id: TypeId,
    pub name: &'static str,
    pub optional: bool,
}

impl Dependency {
    pub fn of<T: 'static>() -> Self;
    pub fn optional_of<T: 'static>() -> Self;
}
```

- 必选依赖缺失：启动失败
- 可选依赖缺失：启动成功
- 可选依赖存在：插件可通过 `try_require` 使用

典型写法：

```rust
impl Plugin for OptionalFeaturePlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        &[Dependency::optional_of::<OptionalDatabase>()]
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        if let Some(db) = ctx.try_require::<OptionalDatabase>()? {
            db.connect().await?;
        }
        Ok(())
    }
}
```

注意：

- 可选依赖不能使用 `ctx.require::<T>()` 强制获取；
- 强制获取仍会返回 `Error::ServiceNotFound`。

### 4.3 启动顺序

按 `priority` 排序：

```text
priority 越大，越先 start
priority 相同：按注册顺序
```

### 4.3.1 排序实现方式

- `start()` 前，根据 `priority` 生成启动序列。
- 该序列保存到 `ContextInner`，例如：

```rust
start_order: Vec<usize>,
```

- `start()` 按该序列依次调用插件 `start().await`。
- `stop()` 按该序列 **逆序** 调用插件 `stop().await`。
- 即使某个插件 `start()` 失败，`stop()` 仍按同一序列逆序清理全部插件；
  未启动插件默认 `stop()` 为空操作。

### 4.4 插件分组

暂不新增组合机制。  
当前可以通过：

```text
一个高层插件在 apply 中注册多个子插件
```

实现组合。

---

## 5. 影响到的模块

- `src/service.rs`：`try_require`、`Collection<T>`、工厂
- `src/context.rs`：`try_require`、`require_all`、`provide_collect`、`provide_factory`、启动排序
- `src/plugin.rs`：`name` / `version` / `priority` / `Dependency.optional` / `optional_of`
- `src/event.rs`：`AsyncFnEventHandler`
- 所有示例和测试同步更新

---

## 6. 迁移步骤

1. 实现 `try_require`
2. 实现 `Collection<T>`
3. 实现服务工厂
4. 实现 `AsyncFnEventHandler`
5. 扩展 `Dependency.optional`
6. 扩展插件元信息
7. 修改 `verify_dependencies` 支持可选依赖
8. 修改 `start` 排序
9. 更新测试
10. 更新文档 / README / 示例

---

## 7. 预期测试

- `try_require` 存在 / 不存在
- `Collection<T>` 多实现注册与查询
- 子 Scope 集合隔离
- 工厂懒加载只创建一次
- `AsyncFnEventHandler` 可注册并 await
- 可选依赖缺失不阻塞启动
- 可选依赖存在时可用
- priority 排序
- 逆序停止
- 插件元信息可读取

---

## 8. 非目标

- 不实现完整服务拦截器
- 不实现插件动态卸载
- 不改变 `Context` / `Scope` 所有权模型
- 不引入具体 runtime
