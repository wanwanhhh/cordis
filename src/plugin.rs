//! 插件定义。

use std::any::TypeId;

use async_trait::async_trait;

use crate::{Context, Error};

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

    /// 插件优先级；越大越先启动。
    fn priority(&self) -> i32 {
        0
    }

    /// 插件声明的服务依赖。
    fn dependencies(&self) -> &'static [Dependency] {
        &[]
    }

    /// 插件声明的插件间依赖。
    fn plugin_dependencies(&self) -> &'static [PluginDependency] {
        &[]
    }

    /// 装载阶段。
    fn apply(&self, _ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }

    /// 启动阶段。
    async fn start(&self, _ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    /// 销毁阶段。
    async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

/// 允许闭包作为插件。
#[async_trait]
impl<F> Plugin for F
where
    F: Fn(&mut Context) -> Result<(), Error> + Send + Sync + 'static,
{
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        self(ctx)
    }
}
