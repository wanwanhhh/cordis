# cordis

Rust 实现的 Cordis 风格插件化基础架构：`Builder`（装配期）/ `Context`（只读句柄）/ `Runtime`（生命周期唯一所有者）三段式，服务是插件之间的唯一契约。

当前处于早期阶段（0.x），核心目标是构建一套可复用的 Rust 插件化 / 依赖注入底座。0.x 阶段公开类型不做向后兼容承诺。

**适合**：单进程内把功能拆成可组合插件、用服务作为插件间契约、希望框架统一管理依赖解析与启动/停止顺序的项目。
**不适合**：需要插件热加载、运行时动态卸载插件、跨进程事件总线的场景（见「当前限制」与[使用指南](docs/USAGE.md) §10）。

## 快速开始

```toml
[dependencies]
cordis = { path = "../cordis" }
futures = "0.3"       # 示例用 futures::executor::block_on 驱动
async-trait = "0.1"   # 异步插件实现 start/stop 需要
```

```rust
use cordis::{Builder, Configurator, Error, Plugin};

struct Greeter(&'static str);

struct GreeterPlugin;

impl Plugin for GreeterPlugin {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Greeter("hello"))?;
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut builder = Builder::new();
        builder.plugin(GreeterPlugin)?;

        let mut rt = builder.build()?;
        rt.start().await?;

        let ctx = rt.handle();
        println!("{}", ctx.require::<Greeter>()?.0);

        rt.stop().await?;
        Ok(())
    })
}
```

## 文档

- [使用指南](docs/USAGE.md)——面向使用者，建议从这里开始
- [底层架构](docs/architecture.md)——实现细节与设计决策
- [开发规范](docs/development.md)——维护者必读

## 当前实现

- `Builder`（装配期独占 `&mut`）/ `Context`（只读数据句柄，Clone + Send + Sync）/ `Runtime`（生命周期唯一所有者）
- `Plugin`（异步 start / stop，`apply(&Configurator)`）
- `ServiceRegistry`（Send + Sync 服务）
- `LifecycleHook` / `SyncHook` / `AsyncHook`
- `FnEventHandler` / `AsyncFnEventHandler`
- 服务注册 / 获取
- `try_require` / `require_all` / `require_all_recursive`
- 集合服务 `provide_collect` / `require_all` / `require_all_recursive` 多实现
- `provide_factory` 懒加载服务
- 父级服务继承与局部遮蔽
- `PluginScope` 插件作用域约束
- `provide_dynamic` / `require_dynamic` 运行时动态配置
- 插件依赖声明与检查
- 可选依赖
- 插件间依赖 `PluginDependency`
- 插件配置注入 `plugin_with_config`
- 插件元信息 / 优先级
- `Configurator` 窄接口与 `apply` 失败回滚
- 异步 ready / dispose 生命周期
- 生命周期为显式状态机（`Built` / `Starting` / `Running` / `Failed` / `Stopping` / `Stopped`），与子作用域租约计数由 `Gate`（`Mutex` 保护的 `{ lifecycle, leases }` + `AtomicBool` 单向闩）统一管理
- 启动失败 fail-fast；失败后进入失败态，重入 `start` 返回 `StartFailed`（不再静默成功），根因可由 `Runtime::start_error()` 查回
- 停止失败继续清理
- `stop` 可续跑：中途丢弃 stop future 后重入从断点继续，不谎报成功
- 重复 `start`（`Running` 态）/ `stop`（`Stopped` 态）安全 no-op；start-after-stop 是 no-op
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用 plugin.stop
- 嵌套 `Context::scope() -> Result<Builder, Error>`
- c-lite 租约：父 `Runtime::stop` 会拒绝仍有活跃子 Builder / Runtime 的停止
- 作用域 id：`Context::id()` / `Builder::id()`，与父 `Context::children()` 清单同源（`Builder::id()` 在 build 之前就能取到）
- 停止可观测性：`Context::children()` / `is_stopping()` / `parent()`，`ActiveScopes` 错误携带活跃子 id 清单
- 可等待的停止信号 `Context::cancelled()`：电平触发，进入关闭流程或收到停止请求时立即完成，无需轮询 `is_stopping()`
- `Runtime::stop_handle()` 返回可 `Clone` 的 `StopHandle`：把「请求停止」的能力显式授出（信号处理 / 管理端点 / 测试兜底），请求只广播取消，关闭仍由 owner 执行
- `Context::spawn` 后台任务绑定作用域生命周期（`tokio` feature，默认启用），返回 `TaskHandle`（`id()` / `abort()` / `wait()` / `is_finished()`）；任务返回 `Err` 时发 `TaskFailed` 事件旁路通知，任务 panic 则在 `stop()` 排空时以 `ErrorKind::TaskFailed` 上报（`TaskHandle::wait()` 也会返回它）
- 优雅停止：`stop()` 排空任务；`stop_with_timeout` 超时强制取消并上报 `TaskAborted`；owner 通过 `TaskHandle::abort()` 的主动取消不计入停止错误
- 事件系统（类型化、父链冒泡、serial / parallel、notify 旁路）
- 结构化 `Error { phase, plugin, kind }`，`source` 保留错误链
- 默认分层并行 `start`，`start_serial()` 保留旧串行语义
- 逆序销毁

## 当前限制

- **父 stop vs 子存活**：采用 c-lite 租约模型。父 `Runtime::stop()` 只检查本层活跃子 `Runtime`/`Builder` 计数；仅子 `Context` 句柄存活不阻塞父 stop，但子 `Runtime` 即使已 `stop` 未 `drop` 仍占租约阻塞父 stop。
- **无 `Drop` 兜底**：忘记调用 `stop()` 不会自动销毁插件，dispose 钩子也不会运行。这是为了避免异步析构陷阱；资源清理责任在创建者，请显式调用 `stop()`。
- **debug 构建下漏调 `stop()` 会 panic**：`Runtime` 的 `Drop` 在 debug 构建下会 `debug_assert!` 检查「曾进入启动流程却没走到 `Stopped`」，命中即硬失败（release 下静默）。这是护栏不是清理——它只负责让泄漏在开发/测试期立刻暴露。
- **`require_all` 只查本层**：它返回当前 Context/Builder 局部集合，不沿父链冒泡。需要读取父级集合时请使用 `require_all_recursive`。
- **c-lite 不防御「不 `stop` 直接 `drop` 子 Runtime」**：drop 会释放租约，但深层异步清理可能未完成，符合显式 `stop` + 无 `Drop` 兜底的原则。
- **`Drop` 不排空 spawn 任务**：忘记 `stop()` 直接 drop `Runtime` 时，`Context::spawn` 的未完成任务随句柄丢弃而脱离框架管理（`Drop` 不做异步清理）。请经 `stop()` / `stop_with_timeout()` 收口。
- **任务取消尽力而为**：`stop_with_timeout` 超时后对未完成任务执行 tokio 协作式取消；卡在同步阻塞调用中的任务无法被强制杀死。被动取消只中断任务在 await 点的执行，任务体里同步阻塞的代码仍会跑完。
- **`stop_with_timeout` 需要 tokio time driver（存在待排空任务时）**：超时排空用 `tokio::time` 计时，要求当前 runtime 启用了 time driver（标准 `new_multi_thread`，或显式 `enable_time` 的 `new_current_thread`）。另外预算大到 `Instant` 无法表示时（如 `Duration::MAX`）按「不设超时」处理，不会 panic——需要真正的上界就别传天文数字。
- **`TaskHandle` 丢弃不等于取消**：`TaskHandle` 不是 guard，drop 它任务照常运行；取消必须显式 `abort()`，或由 `Runtime::stop` 的排空收口。

## 破坏性变更记录

- `Context::spawn` 返回值由 `u64` 改为 `TaskHandle`：任务 id 用 `TaskHandle::id()` 取回（`let id = ctx.spawn(fut)?.id();`）。
- `ErrorKind::ServiceTypeMismatch { expected, found }`：**删除**。服务与工厂只存在于泛型 `provide*` 插入路径，键与值的类型由 `TypeId` 保证一致，这个变体不可能被构造；取出时的 `downcast` 失败改为内部不变式 `expect`（真出错会明确 panic，而不是伪装成可恢复的服务错误）。
- `ErrorKind::StartFailed`：新增变体。`start()` 在**启动失败**后重入时返回它，失败根因挂在 `source` 链上；在**被中断**（start future 被丢弃）后重入同样返回它，但没有 `source`。旧行为是两种情况都静默返回 `Ok` 且不启动任何插件。对 `ErrorKind` 做穷尽匹配的代码需补分支。
- `ErrorKind::ActiveScopes { count, ids }`：新增 `ids` 字段（活跃子作用域 id 清单）。旧代码的模式匹配需补 `..` 或 `ids` 绑定。
- `Phase::Require`：新增变体。运行期 `Context::require` 未命中时返回它；装配期 `Builder::require` 仍是 `Phase::Build`。对 `Phase` 做穷尽匹配的代码需补分支。
- `ErrorKind::TooManyScopes`：**删除**。作用域租约计数是 `usize`，不存在可实际触及的上限，该变体已不可构造。
- `Configurator` / `Builder` 的 `apply` 失败回滚由整表快照改为增量撤销日志；`plugin_with_config` 现在对 `apply` panic 也会回滚注入的配置服务。

自本版起生效；0.x 阶段该类型不做向后兼容承诺。

## 运行示例

```bash
cargo run --example basic
cargo run --example full
cargo run --example scopes
cargo run --example events
cargo run --example dynamic
cargo run --example services
cargo run --example tasks
```

## 测试

```bash
cargo test
cargo test --no-default-features   # 无 tokio 绑定路径（spawn/排空相关用例自动跳过）
```
