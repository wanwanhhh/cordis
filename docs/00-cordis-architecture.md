# Cordis 底层约束与开发指导

> 版本：0.1
> 状态：正式基础文档
> 定位：本项目所有 Cordis 相关代码必须遵循的底层约束/开发指导。

本文档是骨架 + 约束 + 指导，不是业务蓝图，不描述任何具体业务。

---

## 1. 文档目的

本文档定义 **Cordis 哲学的 Rust 基础架构骨架**，包括：

- 核心概念
- 核心模块划分
- 核心 API 约定
- 生命周期规范
- 服务注册规范
- 插件开发规范
- 错误处理规范
- 代码编写规范

后续所有开发工作都应以此文档为准。

本文档**不讨论任何具体业务**，只负责建立一套可复用的插件化容器底座。

本架构不预置任何未来业务类型；未来业务应作为插件/服务直接构建在本次底座之上，不需要修改底层架构。

### 规范分级

本文档中：

- **必须（MUST）**：底层约束。代码不满足就不符合本架构，禁止合入。
- **应当（SHOULD）**：开发指导。除非有充分理由，否则应当遵守。
- **可以（MAY）**：可选能力。不是当前必须实现的内容。

所有“必须”项是骨架的硬边界，所有“应当”项是开发时的行为准则。

---

## 2. Cordis 哲学

### 2.1 一切皆插件

所有可扩展能力都以插件形式存在：

- 日志
- 数据库
- 工具
- 业务流程
- 第三方集成

插件不依赖具体业务，只依赖服务接口。

### 2.2 Context 是世界的边界

插件不能绕过 `Context` 互相调用。

`Context` 是插件与插件之间、插件与服务之间唯一的连接通道。

### 2.3 服务是插件之间唯一的契约

插件之间的通信不能直接持有对方对象，必须通过服务完成：

```text
Plugin A  --provide-->  Service
Plugin B  --require-->  Service
```

服务必须具有清晰、稳定的接口。

### 2.4 显式声明依赖

每个插件必须显式声明自己需要哪些服务。

框架负责在启动时检查依赖是否满足。

### 2.5 生命周期由框架管理

插件不能自行控制初始化顺序。

框架负责：

- 执行 apply
- 执行 start
- 执行 ready
- 执行 stop
- 执行 dispose

### 2.6 静态集成

所有插件在编译期集成，不做运行时动态加载。

---

## 3. 核心概念

| 概念 | 含义 |
|---|---|
| `Context` | 插件容器，生命周期管理者，服务注册表宿主 |
| `Plugin` | 可被加载到 `Context` 中的扩展单元 |
| `Dependency` | 插件声明的服务依赖 |
| `Service` | 插件之间共享的能力 |
| `ServiceRegistry` | 按类型保存服务实例的注册表 |
| `Error` | 框架统一的错误类型 |
| `on_ready` | `start()` 阶段结束后产生的回调 |
| `on_dispose` | `stop()` 阶段结束后产生的回调 |

---

## 4. 架构骨架

### 4.1 目录结构

```text
src/
├── lib.rs            # crate 入口，对外导出
├── context.rs        # Context 核心
├── plugin.rs         # Plugin trait
├── service.rs        # ServiceRegistry
└── error.rs          # 统一错误类型
examples/
└── basic.rs          # 最小可运行示例
```

### 4.2 模块职责

| 模块 | 职责 |
|---|---|
| `context.rs` | 管理插件、服务、生命周期回调 |
| `plugin.rs` | 定义插件契约，插件必须实现 `Plugin` |
| `service.rs` | 服务注册表，负责服务存储和类型安全访问 |
| `error.rs` | 定义所有框架层错误 |
| `lib.rs` | 对外统一导出公共 API |

任何新模块的引入都必须先更新本文档并保持职责单一。

---

## 5. 核心 API 约定

### 5.1 Context

```rust
let mut ctx = Context::new();

ctx.provide(Logger::new());  // 注册服务
ctx.plugin(MyPlugin);        // 注册插件

ctx.start();                 // 启动
ctx.stop();                  // 停止
```

必须提供以下方法：

```rust
impl Context {
    pub fn new() -> Self;

    // 插件
    pub fn plugin<P: Plugin + 'static>(&mut self, plugin: P) -> Result<(), Error>;
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>;

    // 服务
    pub fn provide<T: 'static>(&mut self, value: T) -> Result<(), Error>;
    pub fn require<T: 'static>(&self) -> Result<&T, Error>;
    pub fn require_mut<T: 'static>(&mut self) -> Result<&mut T, Error>;
    pub fn contains<T: 'static>(&self) -> bool;

    // 依赖
    pub fn verify_dependencies(&self) -> Result<(), Error>;

    // 生命周期
    pub fn on_ready(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    );
    pub fn on_dispose(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    );

    // 启动/停止
    pub fn start(&mut self) -> Result<(), Error>;
    pub fn stop(&mut self) -> Result<(), Error>;
}
```

### 5.2 Plugin

插件必须实现：

```rust
pub struct Dependency {
    pub type_id: TypeId,
    pub name: &'static str,
}

pub trait Plugin {
    fn dependencies(&self) -> &'static [Dependency] {
        &[]
    }

    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }

    fn start(&self, ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    fn stop(&self, ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}
```

约定：

- 插件通过 `dependencies()` 显式声明依赖。
- `apply` 是同步的，不允许执行重量级初始化。
- `start` 是同步的，用于完成真正的初始化。
- `stop` 是同步的，用于释放资源。
- 三者必须返回 `Result<(), Error>`，不允许 panic。

#### 依赖声明示例

```rust
use std::sync::LazyLock;

struct Config;

impl Plugin for ConfigDependentPlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        // 插件依赖是编译期固定的，因此适合用 static 保存。
        static DEPS: LazyLock<[Dependency; 1]> =
            LazyLock::new(|| [Dependency::of::<Config>()]);
        &DEPS[..]
    }
}
```

说明：

- 返回类型必须是 `&'static [Dependency]`，代表插件依赖集在运行期不可改变。
- 这与“静态集成、不做热加载”的架构哲学一致。
- 插件可以通过 `static` / `LazyLock` 保存依赖声明，避免每次调用构造临时数组。

### 5.3 ServiceRegistry

服务注册表必须使用 `TypeId` 作为键，保存 `Box<dyn Any>` 和提供时的类型名。

```rust
pub struct ServiceRegistry {
    services: HashMap<TypeId, StoredService>,
}

struct StoredService {
    type_name: &'static str,
    value: Box<dyn Any>,
}
```

类型名用于错误诊断；由于注册时以 `TypeId` 为键，
正常 API 不会出现“键类型与实例类型不一致”。

公开方法：

```rust
impl ServiceRegistry {
    pub fn new() -> Self;
    pub fn provide<T: 'static>(&mut self, value: T) -> Result<(), Error>;
    pub fn get<T: 'static>(&self) -> Result<&T, Error>;
    pub fn get_mut<T: 'static>(&mut self) -> Result<&mut T, Error>;
    pub fn contains<T: 'static>(&self) -> bool;
    pub fn remove<T: 'static>(&mut self) -> Result<T, Error>;
}
```

`contains_type`、`type_ids`、`retain` 目前是 crate 内部方法，
仅供 `Context` 实现依赖检查与 `apply` 失败回滚使用，不对外暴露。

---

## 6. 生命周期规范

### 6.1 启动流程

```text
Context::new()
      │
      ▼
plugin.apply(ctx)  → 注册服务、注册回调
      │
      ▼
verify_dependencies  → 检查所有插件声明的依赖
      │
      ▼
plugin.start(ctx)  → 初始化资源
      │
      ▼
ready hooks        → 所有插件已启动完毕
```

### 6.2 停止流程

```text
stop 阶段
      │
      ▼
plugin.stop(ctx)   → 按注册顺序逆序执行
      │
      ▼
dispose hooks      → 最终清理
```

### 6.3 生命周期职责划分

| 阶段 | 允许做什么 | 禁止做什么 |
|---|---|---|
| `apply` | 注册服务、注册回调、校验配置 | 连接数据库、启动网络请求、耗时初始化 |
| `start` | 初始化资源、连接外部服务 | 依赖其他未启动的服务 |
| `on_ready` | 访问所有已启动的服务 | 修改已注册服务结构（除显式允许外） |
| `stop` | 清理资源、释放连接 | 再次启动新插件 |
| `on_dispose` | 最终清理 | 访问已销毁的服务 |

### 6.4 生命周期原则

- `apply` 必须幂等。
- `start` 必须幂等。
- `stop` 必须幂等。
- 框架必须保证 `start` 前所有 `apply` 已结束。
- 框架必须保证 `stop` 逆序调用插件。
- 任何生命周期回调都必须返回 `Result<(), Error>`。

### 6.5 启动失败语义

- `start()` 前会先执行依赖检查。
- 依赖检查失败时，`start()` 返回错误，且允许补充服务后重新调用 `start()`。
- 某个插件的 `start()` 失败时，**立即停止启动**，后续插件不再 start。
- `start()` 失败后不自动停止已经启动的插件，调用方必须显式调用 `stop()` 清理。
- `stop()` 必须在“部分插件尚未启动”时也能安全调用；`stop()` 对未启动插件执行默认空操作是被允许的。

### 6.6 停止失败语义

- 某个插件的 `stop()` 失败时，**必须继续**逆序停止剩余插件。
- 即使插件停止失败，**dispose hooks 仍然必须全部执行**。
- 停止阶段的所有错误聚合为 `Error::Multiple` 返回。

### 6.7 重复调用语义

- `start()` 可重复调用，但只有第一次会真正执行。
- `stop()` 可重复调用，但只有第一次会真正执行。
- 重复调用 `start()` / `stop()` 时必须成为安全 no-op。
- ready hooks / dispose hooks 只执行一次。

### 6.8 插件注册失败语义

- 单个 `plugin()` 是事务性的：`apply` 失败时，本次新增的服务、hooks、嵌套插件必须回滚。
- `plugins()` 不是整体事务：某个插件失败时，之前已注册插件保持有效，后续插件不再注册。

---

## 7. 服务注册规范

### 7.1 服务唯一性

同一 `TypeId` 只允许注册一次。

重复注册必须返回：

```text
Error::ServiceAlreadyRegistered
```

### 7.2 服务类型

服务可以是：

- 具体类型
- `trait object`
- 共享状态容器
- 其他任何 `'static` 类型

### 7.3 共享服务

当多个插件需要共享同一份可变状态时，必须使用共享容器：

```rust
type Shared<T> = Arc<RwLock<T>>;
```

禁止直接在多个插件之间传递裸可变引用。

### 7.4 接口服务

跨模块暴露能力时，应优先注册接口类型：

```rust
pub trait Logger: Send + Sync + 'static {
    fn log(&self, msg: &str);
}

ctx.provide::<Arc<dyn Logger>>(Arc::new(MyLogger));
ctx.require::<Arc<dyn Logger>>();
```

### 7.5 服务命名

使用 `TypeId` 作为服务键，不使用字符串键。

如果同一接口需要多个实现，应使用 newtype 包装：

```rust
pub struct PrimaryLogger(pub Arc<dyn Logger>);
pub struct SecondaryLogger(pub Arc<dyn Logger>);
```

---

## 8. 插件开发规范

### 8.1 插件应该做什么

1. 在 `dependencies()` 中声明需要的外部服务。
2. 在 `apply` 中注册自身提供的服务。
3. 在 `apply` 中注册生命周期回调。
4. 在 `start` 中初始化外部资源。
5. 在 `stop` 中释放外部资源。
6. 通过 `ctx.require` 获取依赖。
7. 通过 `ctx.provide` 暴露能力。

### 8.2 插件禁止做什么

- 禁止直接持有其他插件的具体类型。
- 禁止绕过 `Context` 直接访问其他插件内部状态。
- 禁止在 `apply` 中做耗时启动。
- 禁止使用 `unsafe`。
- 禁止在插件内部手动管理全局生命周期。
- 禁止 `panic`，所有异常必须通过 `Result<(), Error>` 返回。

### 8.3 插件示例

```rust
impl Plugin for LoggerPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Logger::new("app"))?;

        ctx.on_ready(|ctx| {
            let logger = ctx.require::<Logger>()?;
            logger.log("ready");
            Ok(())
        });

        Ok(())
    }

    fn start(&self, ctx: &Context) -> Result<(), Error> {
        let logger = ctx.require::<Logger>()?;
        logger.connect();
        Ok(())
    }

    fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
        // 清理资源
        Ok(())
    }
}
```

---

## 9. 错误处理规范

统一使用：

```rust
pub enum Error {
    ServiceAlreadyRegistered(String),
    ServiceNotFound(String),
    ServiceTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
    PluginApply(String),
    PluginStart(String),
    PluginStop(String),
    Multiple(Vec<Error>),
}
```

规范：

- 所有框架方法返回 `Result<(), Error>` 或 `Result<T, Error>`。
- 插件不得吞掉错误。
- 错误信息应包含具体的服务名或插件名。
- `ServiceTypeMismatch` 是防御性错误；正常公共 API 不应触发，若触发说明内部不变量被破坏。
- `Error::Multiple` 只用于聚合停止阶段的多个错误。
- 框架层错误不允许 `panic`。

---

## 10. 代码规范

1. 所有公共 API 必须有 rustdoc 注释。
2. 所有公共类型必须使用统一 `Error`。
3. 新增插件必须附带单元测试。
4. 新增服务必须测试重复注册行为。
5. 生命周期顺序变化必须测试。
6. 不使用 `unsafe`。
7. 保持模块小、职责单一。
8. 先写文档，再写实现；实现必须符合本文档。

---

## 11. 当前实现状态

| 能力 | 状态 |
|---|---|
| `Context` | 已实现 |
| `Plugin` | 已实现 |
| `ServiceRegistry` | 已实现 |
| `provide` / `require` | 已实现 |
| `start` / `stop` | 已实现 |
| 逆序销毁 | 已实现 |
| 依赖声明 | 已实现 |
| 依赖检查 | 已实现 |
| 插件 apply 失败回滚 | 已实现 |
| 停止失败继续清理 | 已实现 |
| 重复 start / stop no-op | 已实现 |
| Scope / 子 Context | 未实现 |
| EventBus | 未实现 |
| 异步生命周期 | 未实现（后续另行定义） |

---

## 12. 非目标

目前明确不做：

- 运行时动态加载插件
- 热更新
- 插件市场
- 跨语言插件
- 字符串键服务
- 反射式依赖注入
- 不预置任何具体业务层

未来具体业务（无论是什么）应通过 Plugin / Service 直接构建在本架构之上，
不应要求修改底层 Cordis 骨架。

---

## 13. 开发流程

后续开发必须遵循：

```text
1. 修改本文档，明确规范
2. 根据规范实现
3. 编写单元测试
4. cargo test 全部通过
5. 更新文档状态
```

---

## 14. 结论

本文档是本项目的底层宪法。

所有代码都向本文档看齐。

先把 Cordis 哲学做扎实，再谈其他。
