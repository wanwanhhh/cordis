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

        rt.stop().await.into_result()?;
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
- 生命周期为显式状态机（`Built` / `Starting` / `Running` / `Failed` / `Closing` / `Stopping` / `Stopped`），与活跃子作用域集合由 `ScopeCore`（`Mutex<ScopeInner>` 保护 `{ lifecycle, requested, abandoned, children }` + 派生 `AtomicU64` tag 供免锁快路径）统一管理
- 启动失败 fail-fast；失败后进入失败态，重入 `start` 返回 `StartFailed`（不再静默成功），根因可由 `Runtime::start_error()` 查回
- 停止失败继续清理
- `stop` 的清理由 `Runtime` 自持的 future 驱动：中途丢弃 `stop()` 的 await 不会中断清理，重入继续 poll 同一个 future（插件 `stop` / dispose 只被调用一次）
- 重复 `start`（`Running` 态）/ `stop`（`Stopped` 态）安全 no-op；start-after-stop 是 no-op
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用 plugin.stop
- 嵌套 `Context::scope() -> Result<Builder, Error>`
- c-lite 租约：父 `Runtime::stop` 发现仍有活跃子 Builder / Runtime 时返回 `StopOutcome::Blocked`（已提交停止意图、清理未开始，阻塞方回收后重试续跑）
- 作用域 id：不透明 newtype `ScopeId`，`Context::id()` / `Builder::id()` 与父 `Context::children()` 清单同源（`Builder::id()` 在 build 之前就能取到）
- 停止可观测性：`Context::children()` / `is_stopping()` / `is_stopped()` / `parent()`；`StopOutcome::Blocked(Blockers)` 携带阻塞方 `ScopeId` 清单
- 两个层级的可等待信号：`Context::cancelled()`（提交停止时触发，早于插件 stop 与排空）与 `Context::stopped()`（已停稳，返回 `Settlement::{Stopped, Abandoned}`），无需轮询
- `Runtime::stop_handle()` 返回可 `Clone` 的 `StopHandle`：把「请求停止」的能力显式授出（信号处理 / 管理端点 / 测试兜底），请求只广播取消，关闭仍由 owner 执行
- `Context::spawn` 后台任务绑定作用域生命周期（`tokio` feature，默认启用），返回 `TaskHandle`（`id()` / `abort()` / `wait()` / `is_finished()`）；任务返回 `Err` 时发 `TaskFailed` 事件旁路通知（上报被截断时由 `stop()` 排空兜底补报），任务 panic 则在 `stop()` 排空时以 `ErrorKind::TaskFailed` 上报。`TaskHandle::wait()` 返回一等结局 `TaskOutcome`（`Completed` / `Failed` / `Panicked(PanicInfo)` / `Aborted(AbortReason)`），跑完与被取消可区分
- 优雅停止：`stop()` 排空任务；`stop_with_timeout` 超时强制取消并上报 `TaskAborted`；owner 通过 `TaskHandle::abort()` 的主动取消（`Aborted(Owner)`）不计入停止错误，超时取消（`Aborted(Timeout)`）才计入
- 事件系统（类型化、父链冒泡、serial / parallel、notify 旁路）
- 结构化 `Error { phase, plugin, kind }`，`source` 保留错误链
- 默认分层并行 `start`，`start_serial()` 保留旧串行语义
- 逆序销毁

## 当前限制

- **父 stop vs 子存活**：采用 c-lite 租约模型。父 `Runtime::stop()` 只检查本层活跃子 `Runtime`/`Builder` 计数；仅子 `Context` 句柄存活不阻塞父 stop。子 `Runtime` 走完停止流程时归还租约（归还后随即进入 `Stopped`）、不再阻塞父 stop（即使尚未 `drop`）；只有未 `stop` 就被 `drop` 的子仍占租约。
- **无 `Drop` 兜底**：忘记调用 `stop()` 不会自动销毁插件，dispose 钩子也不会运行。这是为了避免异步析构陷阱；资源清理责任在创建者，请显式调用 `stop()`。
- **debug 构建下未走完停止会 panic**：`Runtime` 的 `Drop` 在 debug 构建下会 `debug_assert!` 检查「曾进入启动流程却没走到 `Stopped`」，命中即硬失败（release 下静默）。诊断文字区分「从未 stop」「stop 被活跃子作用域阻塞（列出 ids）」「stop 已提交但清理未走完」三种成因。未到 `Stopped` 就 drop 时还会让 `Context::stopped()` 的等待者拿到 `Settlement::Abandoned`，且本层随即拒绝新 `scope` / `spawn`。
- **`require_all` 只查本层**：它返回当前 Context/Builder 局部集合，不沿父链冒泡。需要读取父级集合时请使用 `require_all_recursive`。
- **c-lite 不防御「不 `stop` 直接 `drop` 子 Runtime」**：drop 会释放租约，但深层异步清理可能未完成，符合显式 `stop` + 无 `Drop` 兜底的原则。
- **`Drop` 不排空 spawn 任务**：忘记 `stop()` 直接 drop `Runtime` 时，`Context::spawn` 的未完成任务随句柄丢弃而脱离框架管理（`Drop` 不做异步清理）。请经 `stop()` / `stop_with_timeout()` 收口。
- **任务取消尽力而为**：`stop_with_timeout` 超时后对未完成任务执行 tokio 协作式取消；卡在同步阻塞调用中的任务无法被强制杀死。被动取消只中断任务在 await 点的执行，任务体里同步阻塞的代码仍会跑完。
- **`stop_with_timeout` 需要 tokio time driver（存在待排空任务时）**：超时排空用 `tokio::time` 计时，要求当前 runtime 启用了 time driver（标准 `new_multi_thread`，或显式 `enable_time` 的 `new_current_thread`）。另外预算大到 `Instant` 无法表示时（如 `Duration::MAX`）按「不设超时」处理，不会 panic——需要真正的上界就别传天文数字。
- **`TaskHandle` 丢弃不等于取消**：`TaskHandle` 不是 guard，drop 它任务照常运行；取消必须显式 `abort()`，或由 `Runtime::stop` 的排空收口。

## 破坏性变更记录

- `Runtime::stop` / `stop_with_timeout` 返回值由 `Result<(), Error>` 改为 `StopOutcome`（`Stopped { errors }` / `Blocked(Blockers)`）。`ErrorKind::ActiveScopes { count, ids }` **删除**，被 `StopOutcome::Blocked`（及折叠用的 `ErrorKind::StopBlocked(Blockers)`）取代。
- `TaskHandle::wait()` 返回值由 `Result<(), Error>` 改为 `TaskOutcome`：取消不再伪装成 `Ok(())`，panic 带 `PanicInfo::message`，取消成因分 `AbortReason::{Owner, Timeout, HostShutdown}`。`TaskHandle::id()` 返回不透明 `TaskId`；`Context::id()` / `Builder::id()` / `Context::children()` 返回或包含不透明 `ScopeId`；`TaskFailed::task_id` 与 `ErrorKind::{TaskFailed,TaskAborted}` 的 `task_id` 同改。旧值用 `.get()` 取回。
- 新增 `Lifecycle::Closing`：`stop()` 一提交就进入（拒绝新 `scope`/`spawn`、广播取消、清理未开始）；被活跃子作用域阻塞时返回 `Blocked` 且意图不可逆，不再回退状态。
- 新增 `Context::is_stopped()` / `Context::stopped()`（`Settlement::{Stopped, Abandoned}`）；公开此前私有的 `Cancelled`，新增公开类型 `Settled` / `Blockers` / `StopOutcome` / `TaskOutcome` / `AbortReason` / `PanicInfo`。
- 子 `Runtime` 的租约改为走完停止流程时归还（归还后随即进入 `Stopped`；原先绑定在 `drop` 上）；`Context::children()` 语义随之变为「尚未归还租约的子作用域」（列出的必未 `Stopped`）。
- `stop_with_timeout` 的预算改为「进入清理时算定、随清理 future 存续，重入不重置」。
- `Plugin::stop` 不再要求可重入：清理由自持 future 驱动，每个被回收项只调用一次；但仍可能在 `start` 未成功甚至未调用时被调用。

- `Context::spawn` 返回值由 `u64` 改为 `TaskHandle`：任务 id 用 `TaskHandle::id()` 取回（`let id = ctx.spawn(fut)?.id();`）。
- `ErrorKind::ServiceTypeMismatch { expected, found }`：**删除**。服务与工厂只存在于泛型 `provide*` 插入路径，键与值的类型由 `TypeId` 保证一致，这个变体不可能被构造；取出时的 `downcast` 失败改为内部不变式 `expect`（真出错会明确 panic，而不是伪装成可恢复的服务错误）。
- `ErrorKind::StartFailed`：新增变体。`start()` 在**启动失败**后重入时返回它，失败根因挂在 `source` 链上；在**被中断**（start future 被丢弃）后重入同样返回它，但没有 `source`。旧行为是两种情况都静默返回 `Ok` 且不启动任何插件。对 `ErrorKind` 做穷尽匹配的代码需补分支。
- `Phase::Require`：新增变体。运行期 `Context::require` 未命中时返回它；装配期 `Builder::require` 仍是 `Phase::Build`。对 `Phase` 做穷尽匹配的代码需补分支。
- `ErrorKind::TooManyScopes`：**删除**。作用域租约计数是 `usize`，不存在可实际触及的上限，该变体已不可构造。
- `Configurator` / `Builder` 的 `apply` 失败回滚由整表快照改为增量撤销日志；`plugin_with_config` 现在对 `apply` panic 也会回滚注入的配置服务。
- `ErrorKind::CloseHookPanicked { message }` **改名**为 `ErrorKind::LifecyclePanicked { message }`：它现在同时表达关闭回调、插件 `stop`、dispose 三类生命周期用户代码的 panic（用 `phase` 区分），因此不再叫「close hook」。对 `ErrorKind` 做穷尽匹配的代码必须改分支名。
- `ErrorKind` 新增 `CloseHookDispatchOverflow`，并标注 `#[non_exhaustive]`：错误分类表后续仍会增补，下游 `match` 请保留兜底分支。
- `ErrorKind::TaskFailed { task_id }` 的语义收紧：任务体返回 `Err` 且其 `TaskFailed` **上报未送达**（handler panic、排空超时在 await 点取消上报、宿主丢弃 future）时也会在排空时上报，不再因为「结局是 `Failed`」就当作已上报。
- `RegistryError::Busy(String)`：新增变体。名字正被在飞的 `open`/`close` 占用（`Reserved`/`Closing`）时返回它；此前这类情况会被谎报成 `NotFound`。`ScopeRegistry::close` 对被孙作用域阻塞的子级改为如实返回 `CloseStatus::Blocked` 并**保留** owner（此前会析构它），回收孙级后可重试 `close`。
- `CloseReport::attempts`：**删除**。`close` 单次调用只跑一轮 `stop`（恒为 1），「被阻塞则重试」是调用方的决定，该字段不表达任何信息。
- `RegistryError::Poisoned`：**删除**。所有锁都 `into_inner` 解毒，该变体没有构造点、不可达。
- `Context::on_closing` / 关闭回调的链式注册现在有**三道上界**并记一条 `CloseHookDispatchOverflow`（每层至多一条）：派发轮次 `MAX_CLOSING_HOOK_ROUNDS`（16）、执行的**嵌套深度**（16）、**链式注册**的总数 `MAX_CHAINED_CLOSING_HOOKS`（4096，按层计）。「链式」指注册发生在关闭钩子执行中、或层已开始派发之后；装配期声明与**层存活期间普通代码**的动态注册都不计（含反复注册+注销）。触顶的回调不再登记、也不执行（轮次触顶例外：收尾补跑剩余项）。此前该路径是同步无界递归：「每次注册自身」的回调会栈溢出 abort，一次注册多个副本的回调会按分支因子指数膨胀，关闭钩子向尚未开始派发的另一层注册还能绕过预算并跨层放大。
- `ScopeRegistry::open` 的启动失败回收若被活跃孙级 `Blocked`：owner 以 `Closing(Some)` 保留、**名字不释放**，同名 `open` 得到 `AlreadyExists`，须先回收孙级再 `close` 续跑（此前「名字必释放」的表述不成立）。
- `ScopeRegistry::open` / `close` 在回收 `stop` 的 await 点被取消时不再析构 `Runtime`（由 `ClosingGuard` 放回 `Closing(Some)`，可重试）；`open` 的 `start().await` 仍是明文契约上的未兜底窗口（owner 应让 `open` 跑完）。

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
