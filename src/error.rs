//! 核心错误类型。

use std::fmt;

/// 错误发生阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Apply,
    Verify,
    Build,
    Start,
    Ready,
    Stop,
    Dispose,
    Event,
}

/// 结构化错误种类。
#[derive(Debug)]
pub enum ErrorKind {
    /// 服务已被注册过。
    ServiceAlreadyRegistered(String),
    /// 服务不存在。
    ServiceNotFound(String),
    /// 服务类型不匹配。
    ServiceTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
    /// 插件名重复注册。
    PluginNameAlreadyRegistered(String),
    /// 插件依赖缺失。
    PluginDependencyNotFound(String),
    /// 插件依赖存在循环。
    PluginDependencyCycle,
    /// 父 Runtime 停止时仍有活跃子 Runtime。
    ActiveScopes { count: u64 },
    /// 父已进入停止，拒绝新 scope。
    Stopping,
    /// scope 数量超限。
    TooManyScopes,
    /// 事件订阅不存在或不属于当前 Context。
    SubscriptionNotFound,
    /// 承载插件自定义来源。
    Other,
    /// 停止/并行 start 的聚合错误。
    Multiple(Vec<Error>),
}

/// Cordis 底层框架错误。
pub struct Error {
    /// 错误发生阶段。
    pub phase: Phase,
    /// 关联插件名（若有）。
    pub plugin: Option<&'static str>,
    /// 错误种类。
    pub kind: ErrorKind,
    /// 底层错误链。
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
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
            source: Some(Box::new(source)),
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
            ErrorKind::ServiceTypeMismatch { expected, found } => {
                write!(
                    f,
                    "{:?}: service type mismatch: expected {expected}, found {found}",
                    self.phase
                )
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
            ErrorKind::ActiveScopes { count } => {
                write!(f, "{:?}: active scopes: {count}", self.phase)
            }
            ErrorKind::Stopping => write!(f, "{:?}: stopping", self.phase),
            ErrorKind::TooManyScopes => write!(f, "{:?}: too many scopes", self.phase),
            ErrorKind::SubscriptionNotFound => {
                write!(f, "{:?}: event subscription not found", self.phase)
            }
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
