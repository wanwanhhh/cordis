//! 插件定义。

use std::any::TypeId;

use async_trait::async_trait;

use crate::{Context, Error};

/// 插件依赖声明。
///
/// 携带 `TypeId` 和类型名，供依赖检查使用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dependency {
    /// 依赖服务的类型标识。
    pub type_id: TypeId,
    /// 依赖服务的人类可读类型名。
    pub name: &'static str,
}

impl Dependency {
    /// 根据类型构造依赖声明。
    pub fn of<T: 'static>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }
}

/// 插件 trait。
///
/// 插件是 Cordis 架构的基本扩展单元。
///
/// # 生命周期
///
/// - `apply`：同步，用于注册服务和回调。
/// - `start`：异步，用于初始化外部资源。
/// - `stop`：异步，用于释放资源。
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    /// 插件声明的依赖。
    ///
    /// 返回的 slice 必须在整个生命周期内稳定。
    fn dependencies(&self) -> &'static [Dependency] {
        &[]
    }

    /// 装载阶段。此时可以向 `Context` 注册服务或生命周期回调。
    fn apply(&self, _ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }

    /// 启动阶段。所有插件已完成 apply。
    async fn start(&self, _ctx: &Context) -> Result<(), Error> {
        Ok(())
    }

    /// 销毁阶段。按注册顺序的逆序调用。
    async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
        Ok(())
    }
}

/// 允许直接传入一个 `Fn(&mut Context)` 作为插件。
#[async_trait]
impl<F> Plugin for F
where
    F: Fn(&mut Context) -> Result<(), Error> + Send + Sync + 'static,
{
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        self(ctx)
    }
}
