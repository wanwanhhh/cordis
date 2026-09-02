//! 核心上下文与 Scope。

use std::any::TypeId;
use std::mem;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{Error, Plugin, ServiceRegistry};

type ReadyHook = Box<dyn LifecycleHook>;
type DisposeHook = Box<dyn LifecycleHook>;

/// 异步生命周期回调。
///
/// 用 trait object 替代裸 `Pin<Box<dyn Future>>`，简化注册和存储。
#[async_trait]
pub trait LifecycleHook: Send + Sync + 'static {
    /// 执行回调。
    async fn call(&mut self, ctx: &mut Context) -> Result<(), Error>;
}

/// 同步闭包适配器，便于将普通 `FnMut(&mut Context) -> Result<(), Error>` 注册为异步钩子。
pub struct SyncHook<F>(pub F);

#[async_trait]
impl<F> LifecycleHook for SyncHook<F>
where
    F: for<'a> FnMut(&'a mut Context) -> Result<(), Error> + Send + Sync + 'static,
{
    async fn call(&mut self, ctx: &mut Context) -> Result<(), Error> {
        (self.0)(ctx)
    }
}

/// 内部可共享状态。
struct ContextInner {
    parent: Option<Arc<ContextInner>>,
    services: ServiceRegistry,
    plugins: Vec<Box<dyn Plugin>>,
    ready_hooks: Mutex<Vec<ReadyHook>>,
    dispose_hooks: Mutex<Vec<DisposeHook>>,
    start_called: bool,
    stopped: bool,
}

impl ContextInner {
    fn new() -> Self {
        Self {
            parent: None,
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            ready_hooks: Mutex::new(Vec::new()),
            dispose_hooks: Mutex::new(Vec::new()),
            start_called: false,
            stopped: false,
        }
    }

    fn child(parent: &Arc<ContextInner>) -> Self {
        Self {
            parent: Some(parent.clone()),
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            ready_hooks: Mutex::new(Vec::new()),
            dispose_hooks: Mutex::new(Vec::new()),
            start_called: false,
            stopped: false,
        }
    }

    fn require<T: 'static>(&self) -> Result<&T, Error> {
        match self.services.get::<T>() {
            Ok(value) => Ok(value),
            Err(Error::ServiceNotFound(_)) => match &self.parent {
                Some(parent) => parent.require::<T>(),
                None => Err(Error::ServiceNotFound(
                    std::any::type_name::<T>().to_string(),
                )),
            },
            Err(err) => Err(err),
        }
    }

    fn contains<T: 'static>(&self) -> bool {
        self.services.contains::<T>() || self.contains_type(TypeId::of::<T>())
    }

    fn contains_type(&self, type_id: TypeId) -> bool {
        if self.services.contains_type(type_id) {
            return true;
        }
        match &self.parent {
            Some(parent) => parent.contains_type(type_id),
            None => false,
        }
    }

    fn verify_dependencies(&self) -> Result<(), Error> {
        for plugin in &self.plugins {
            for dependency in plugin.dependencies() {
                if !self.contains_type(dependency.type_id) {
                    return Err(Error::ServiceNotFound(dependency.name.to_string()));
                }
            }
        }
        Ok(())
    }
}

/// Cordis 核心上下文句柄。
///
/// 内部使用 `Arc`，支持 `Clone` 和跨线程共享。
#[derive(Clone)]
pub struct Context {
    inner: Arc<ContextInner>,
}

impl Context {
    /// 创建一个空的根上下文。
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ContextInner::new()),
        }
    }

    /// 创建一个子上下文（Scope）。
    ///
    /// Scope 共享父级服务，但在任一后代存活期间父级不能执行可变操作。
    pub fn scope(&self) -> Scope {
        Scope {
            ctx: Context {
                inner: Arc::new(ContextInner::child(&self.inner)),
            },
        }
    }

    /// 注册一个插件。
    ///
    /// 如果 `apply` 失败，该插件本次产生的副作用会被回滚。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        let snapshot = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            (
                inner.services.type_ids(),
                inner.plugins.len(),
                inner.ready_hooks.lock().unwrap().len(),
                inner.dispose_hooks.lock().unwrap().len(),
            )
        };

        let (service_keys, plugin_len, ready_len, dispose_len) = snapshot;

        if let Err(err) = plugin.apply(self) {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.services.retain(&service_keys);
            inner.plugins.truncate(plugin_len);
            inner.ready_hooks.lock().unwrap().truncate(ready_len);
            inner.dispose_hooks.lock().unwrap().truncate(dispose_len);
            return Err(Error::PluginApply(err.to_string()));
        }

        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.plugins.push(Box::new(plugin));
        Ok(())
    }

    /// 批量注册插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin,
    {
        for plugin in plugins {
            self.plugin(plugin)?;
        }
        Ok(())
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.provide(value)
    }

    /// 获取服务引用。
    ///
    /// 查找顺序：当前上下文局部服务 -> 父级上下文服务（递归）。
    pub fn require<T: 'static>(&self) -> Result<&T, Error> {
        self.inner.require()
    }

    /// 获取本地服务可变引用。
    pub fn require_mut<T: 'static>(&mut self) -> Result<&mut T, Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.get_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: 'static>(&self) -> bool {
        self.inner.contains::<T>()
    }

    /// 注册一个 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.ready_hooks.lock().unwrap().push(Box::new(hook));
        Ok(())
    }

    /// 注册一个 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.dispose_hooks.lock().unwrap().push(Box::new(hook));
        Ok(())
    }

    /// 检查所有插件的依赖是否满足。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        self.inner.verify_dependencies()
    }

    /// 异步启动上下文。
    ///
    /// 只启动当前上下文内注册的插件，不影响父级。
    pub async fn start(&mut self) -> Result<(), Error> {
        {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.start_called {
                return Ok(());
            }
            inner.verify_dependencies()?;
            inner.start_called = true;
        }

        for plugin in &self.inner.plugins {
            plugin.start(&*self).await?;
        }

        let mut hooks = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            mem::take(&mut *inner.ready_hooks.lock().unwrap())
        };
        for hook in &mut hooks {
            hook.call(&mut *self).await?;
        }

        Ok(())
    }

    /// 异步停止上下文。
    ///
    /// 只停止当前上下文内注册的插件，不影响父级。
    pub async fn stop(&mut self) -> Result<(), Error> {
        let mut errors = Vec::new();

        let plugins = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.stopped {
                return Ok(());
            }
            inner.stopped = true;
            mem::take(&mut inner.plugins)
        };

        for plugin in plugins.iter().rev() {
            if let Err(err) = plugin.stop(self).await {
                errors.push(err);
            }
        }

        {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.plugins = plugins;
        }

        let mut hooks = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            mem::take(&mut *inner.dispose_hooks.lock().unwrap())
        };
        for hook in &mut hooks {
            if let Err(err) = hook.call(&mut *self).await {
                errors.push(err);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::Multiple(errors))
        }
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

/// 子上下文。
///
/// 它是共享父级服务的局部 `Context`，用于隔离插件组。
#[derive(Clone)]
pub struct Scope {
    ctx: Context,
}

impl Scope {
    /// 创建孙级 Scope。
    pub fn scope(&self) -> Scope {
        self.ctx.scope()
    }

    /// 注册插件。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        self.ctx.plugin(plugin)
    }

    /// 批量注册插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin,
    {
        self.ctx.plugins(plugins)
    }

    /// 注册局部服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.ctx.provide(value)
    }

    /// 查询服务：局部优先，父级兜底。
    pub fn require<T: 'static>(&self) -> Result<&T, Error> {
        self.ctx.require()
    }

    /// 获取本地服务可变引用。
    pub fn require_mut<T: 'static>(&mut self) -> Result<&mut T, Error> {
        self.ctx.require_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: 'static>(&self) -> bool {
        self.ctx.contains::<T>()
    }

    /// 检查依赖。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        self.ctx.verify_dependencies()
    }

    /// 注册 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.ctx.on_ready(hook)
    }

    /// 注册 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.ctx.on_dispose(hook)
    }

    /// 异步启动 scope。
    pub async fn start(&mut self) -> Result<(), Error> {
        self.ctx.start().await
    }

    /// 异步停止 scope。
    pub async fn stop(&mut self) -> Result<(), Error> {
        self.ctx.stop().await
    }
}
