//! 核心错误类型。

use std::fmt;

/// Cordis 底层框架错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// 服务已被注册过。
    ServiceAlreadyRegistered(String),

    /// 服务不存在。
    ServiceNotFound(String),

    /// 服务类型不匹配。
    ServiceTypeMismatch {
        expected: &'static str,
        found: &'static str,
    },

    /// 插件注册阶段失败。
    PluginApply(String),

    /// 插件启动阶段失败。
    PluginStart(String),

    /// 插件销毁阶段失败。
    PluginStop(String),

    /// 停止阶段发生多个错误。
    Multiple(Vec<Error>),

    /// 上下文已被 Scope 共享，当前不允许可变操作。
    ContextShared,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ServiceAlreadyRegistered(name) => {
                write!(f, "service already registered: {name}")
            }
            Error::ServiceNotFound(name) => write!(f, "service not found: {name}"),
            Error::ServiceTypeMismatch { expected, found } => {
                write!(
                    f,
                    "service type mismatch: expected {expected}, found {found}"
                )
            }
            Error::PluginApply(msg) => write!(f, "plugin apply failed: {msg}"),
            Error::PluginStart(msg) => write!(f, "plugin start failed: {msg}"),
            Error::PluginStop(msg) => write!(f, "plugin stop failed: {msg}"),
            Error::Multiple(errors) => {
                write!(f, "multiple errors during shutdown:")?;
                for (index, error) in errors.iter().enumerate() {
                    write!(f, "\n  {}. {error}", index + 1)?;
                }
                Ok(())
            }
            Error::ContextShared => write!(
                f,
                "context is shared by a scope; mutable operations are not allowed"
            ),
        }
    }
}

impl std::error::Error for Error {}
