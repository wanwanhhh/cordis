//! 核心错误类型。

use std::fmt;
use std::sync::Arc;

use crate::id::{Blockers, TaskId};
use crate::plugin::PluginScope;

/// 错误发生阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Apply,
    Verify,
    Build,
    /// 运行期服务查询失败（`Context::require` 未命中）。
    ///
    /// 与 `Build` 区分开：同一个 `ServiceNotFound` 在装配期是注册错误、在运行期
    /// 是查询失败，混用一个阶段会让排障无法定位。
    Require,
    Start,
    Ready,
    /// 本层刚提交关闭（进入 `Closing`）时同步执行的关闭回调。
    ///
    /// 与 `Stop` 分开：关闭回调早于插件 `stop`、任务排空与 dispose，是「开始收尾」
    /// 阶段的错误，不该和清理失败混在一个阶段里。
    Close,
    Stop,
    Dispose,
    Event,
}

/// 结构化错误种类。
///
/// `#[non_exhaustive]`：这是框架错误分类表，后续版本可能继续增补。下游 `match`
/// 请保留兜底分支——它让新增变体不再是破坏性变更。
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ErrorKind {
    /// 服务已被注册过。
    ServiceAlreadyRegistered(String),
    /// 服务不存在。
    ServiceNotFound(String),
    /// 插件名重复注册。
    PluginNameAlreadyRegistered(String),
    /// 插件依赖缺失。
    PluginDependencyNotFound(String),
    /// 插件依赖存在循环。
    PluginDependencyCycle,
    /// 插件安装作用域不匹配。
    PluginScopeMismatch {
        plugin_name: String,
        expected: PluginScope,
        actual: PluginScope,
    },
    /// `StopOutcome::Blocked` 被折叠成 `Error` 时使用；表达「停止被活跃子作用域
    /// 挡住，前置条件未满足、可重试」，不是清理失败。
    ///
    /// 清单由 `ScopeCore` 在同一临界区内与租约计数一起取出，因此与实际阻塞集合一致。
    StopBlocked(Blockers),
    /// 父已进入停止，拒绝新 scope / spawn。
    Stopping,
    /// 事件订阅不存在或不属于当前 Context。
    SubscriptionNotFound,
    /// `Context::spawn` 时不存在可用的 tokio runtime 上下文。
    NoTaskRuntime,
    /// `Context::spawn` 的后台任务 panic。
    ///
    /// 两条上报路径：`Runtime::stop` 排空时计入停止错误；`TaskHandle::wait` 直接
    /// 作为返回值。
    TaskFailed { task_id: TaskId },
    /// 优雅停止超时，后台任务被强制取消。
    TaskAborted { task_id: TaskId },
    /// 生命周期用户代码 panic。
    ///
    /// 覆盖三类站点，由 `phase` 区分（`Close` / `Stop` / `Dispose`）：关闭回调、
    /// 插件 `stop`、dispose hook。它们在 owner `stop()` 的清理路径上同步或被
    /// `catch_unwind` 隔离执行，panic 绝不能逃出——逃出会让本层停在不可逆的
    /// `Closing`/`Stopping` 却永远没有清理 future（`stop_future` 会被 poison）。
    LifecyclePanicked { message: Option<String> },
    /// 关闭回调链式注册超过收敛上界：某个回调在每次被调用时又注册新的回调。
    ///
    /// 三条上界共用这一个出口（每层至多记一条）：派发轮次、执行的嵌套深度、以及
    /// **链式注册**（关闭钩子执行中触发、或目标层已开始派发后注册）的总数。触顶时
    /// 停止收敛、把这一条计入停止错误，而不是静默丢弃或失控——同步递归无界会栈溢出
    /// abort，分支式自增殖会按分支因子指数膨胀。
    CloseHookDispatchOverflow,
    /// `start` 未能成功完成：重入一个已失败的运行时，或上一次启动被中断。
    ///
    /// 有根因时，聚合错误挂在 `source` 链上，可按
    /// `err.source().and_then(|s| s.downcast_ref::<Error>())` 取回；
    /// 启动被中途丢弃（没有记录到失败）时没有 `source`。
    StartFailed,
    /// 承载插件自定义来源。
    Other,
    /// 停止/并行 start 的聚合错误。
    Multiple(Vec<Error>),
}

/// Cordis 底层框架错误。
///
/// 可 `Clone`：`source` 是 `Arc`，克隆只增加引用计数。这让同一个失败可以在
/// 交给调用方之外，再被 `Runtime` 保留一份（见 `ErrorKind::StartFailed`）。
#[derive(Clone)]
pub struct Error {
    /// 错误发生阶段。
    pub phase: Phase,
    /// 关联插件名（若有）。
    pub plugin: Option<&'static str>,
    /// 错误种类。
    pub kind: ErrorKind,
    /// 底层错误链。用 `Arc` 而非 `Box`：既让首错可被 Runtime 与重入错误共享，
    /// 又保持 `with_source` 的公开签名与 `source()` 的返回类型不变。
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    /// 创建一个没有底层来源的错误。
    pub fn new(phase: Phase, kind: ErrorKind) -> Self {
        Self {
            phase,
            plugin: None,
            kind,
            source: None,
        }
    }

    /// 创建带底层来源的错误。
    pub fn with_source(
        phase: Phase,
        kind: ErrorKind,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            phase,
            plugin: None,
            kind,
            source: Some(Arc::new(source)),
        }
    }

    /// 将错误标记到指定阶段和插件（不改变 `kind`，保留错误链）。
    ///
    /// 如果错误已经携带了内层插件名，则保留内层插件名；仅当尚未指定插件时
    /// 才写入当前外层插件名。
    pub fn into_phase(mut self, phase: Phase, plugin: Option<&'static str>) -> Self {
        self.phase = phase;
        if self.plugin.is_none() {
            self.plugin = plugin;
        }
        self
    }

    /// 返回错误种类。
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// 判断是否是聚合错误。
    pub fn is_multiple(&self) -> bool {
        matches!(self.kind, ErrorKind::Multiple(_))
    }

    /// 返回底层错误来源。
    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(self)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("phase", &self.phase)
            .field("plugin", &self.plugin)
            .field("kind", &self.kind)
            .field("source", &self.source.as_ref().map(|s| s.to_string()))
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::ServiceAlreadyRegistered(name) => {
                write!(f, "{:?}: service already registered: {name}", self.phase)
            }
            ErrorKind::ServiceNotFound(name) => {
                write!(f, "{:?}: service not found: {name}", self.phase)
            }
            ErrorKind::PluginNameAlreadyRegistered(name) => {
                write!(
                    f,
                    "{:?}: plugin name already registered: {name}",
                    self.phase
                )
            }
            ErrorKind::PluginDependencyNotFound(name) => {
                write!(f, "{:?}: plugin dependency not found: {name}", self.phase)
            }
            ErrorKind::PluginDependencyCycle => {
                write!(f, "{:?}: plugin dependency cycle detected", self.phase)
            }
            ErrorKind::PluginScopeMismatch {
                plugin_name,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "{:?}: plugin scope mismatch for {plugin_name}: expected {expected:?}, actual {actual:?}",
                    self.phase
                )
            }
            ErrorKind::StopBlocked(blockers) => {
                write!(
                    f,
                    "{:?}: stop blocked by {}",
                    self.phase,
                    blockers.describe()
                )
            }
            ErrorKind::Stopping => write!(f, "{:?}: stopping", self.phase),
            ErrorKind::SubscriptionNotFound => {
                write!(f, "{:?}: event subscription not found", self.phase)
            }
            ErrorKind::NoTaskRuntime => {
                write!(f, "{:?}: no tokio runtime context for spawn", self.phase)
            }
            ErrorKind::TaskFailed { task_id } => {
                write!(f, "{:?}: background {task_id} panicked", self.phase)
            }
            ErrorKind::TaskAborted { task_id } => {
                write!(
                    f,
                    "{:?}: background {task_id} aborted after drain timeout",
                    self.phase
                )
            }
            ErrorKind::LifecyclePanicked { message } => match message {
                Some(message) => write!(
                    f,
                    "{:?}: lifecycle callback panicked: {message}",
                    self.phase
                ),
                None => write!(f, "{:?}: lifecycle callback panicked", self.phase),
            },
            ErrorKind::CloseHookDispatchOverflow => {
                write!(
                    f,
                    "{:?}: close hook dispatch round limit exceeded",
                    self.phase
                )
            }
            ErrorKind::StartFailed => write!(f, "{:?}: start did not complete", self.phase),
            ErrorKind::Other => write!(f, "{:?}: other error", self.phase),
            ErrorKind::Multiple(errors) => {
                write!(f, "{:?}: multiple errors:", self.phase)?;
                for (index, error) in errors.iter().enumerate() {
                    write!(f, "\n  {}. {error}", index + 1)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
