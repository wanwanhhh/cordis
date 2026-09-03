//! 插件定义。

use std::any::TypeId;

use async_trait::async_trait;

use crate::{Configurator, Context, Error};

/// 服务依赖声明。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dependency {
    /// 依赖服务的类型标识。
    pub type_id: TypeId,
    /// 依赖服务的人类可读类型名。
    pub name: &'static str,
    /// 是否为可选依赖。
    pub optional: bool,
}

impl Dependency {
    /// 必选依赖。
    pub fn of<T: 'static>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
            optional: false,
        }
    }

    /// 可选依赖。
    pub fn optional_of<T: 'static>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
            optional: true,
        }
    }
}

/// 插件间依赖声明。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PluginDependency {
    /// 依赖的目标插件名。
    pub plugin_name: &'static str,
    /// 是否为可选依赖。
    pub optional: bool,
}

impl PluginDependency {
    /// 必选插件依赖。
    pub fn of(plugin_name: &'static str) -> Self {
        Self {
            plugin_name,
            optional: false,
        }
    }

    /// 可选插件依赖。
    pub fn optional_of(plugin_name: &'static str) -> Self {
        Self {
            plugin_name,
            optional: true,
        }
    }
}

/// 插件 trait。
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// 插件名称。
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// 插件版本。
    fn version(&self) -> &'static str {
        "0.0.0"
    }

    /// 插件优先级；同一层内的调度提示。
    fn priority(&self) -> i32 {
        0
    }

    /// 插件声明的服务依赖。
    ///
    /// 返回 `Vec`，由 `Builder::plugin()` 在注册时求值并缓存。
    fn dependencies(&self) -> Vec<Dependency> {
        Vec::new()
    }

    /// 插件声明的插件间依赖。
    ///
    /// 返回 `Vec`，由 `Builder::plugin()` 在注册时求值并缓存。
    fn plugin_dependencies(&self) -> Vec<PluginDependency> {
        Vec::new()
    }

    /// 装载阶段。
    ///
    /// 入参为窄接口 `Configurator`：可以注册服务/插件/hook，但不能修改运行期
    /// 已冻结的既有服务，也不能调用 `start`/`stop`。
    fn apply(&self, _cfg: &mut Configurator<'_>) -> Result<(), Error> {
        Ok(())
    }

    /// 启动阶段。
    async fn start(&self, _ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    /// 销毁阶段。
    async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
        Ok(())
    }
}

/// 允许闭包作为插件。
#[async_trait]
impl<F> Plugin for F
where
    F: Fn(&mut Configurator<'_>) -> Result<(), Error> + Send + Sync + 'static,
{
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        self(cfg)
    }
}
