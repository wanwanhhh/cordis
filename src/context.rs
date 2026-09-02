//! 核心上下文与 Scope。

use std::any::TypeId;
use std::mem;
use std::rc::Rc;

use crate::{Error, Plugin, ServiceRegistry};

type ReadyHook = Box<dyn FnMut(&mut Context) -> Result<(), Error>>;
type DisposeHook = Box<dyn FnMut(&mut Context) -> Result<(), Error>>;

/// 内部可共享状态。
///
/// `Rc` 让 Scope 可以在不引入生命周期泛型的情况下共享父级服务。
struct ContextInner {
    parent: Option<Rc<ContextInner>>,
    services: ServiceRegistry,
    plugins: Vec<Box<dyn Plugin>>,
    ready_hooks: Vec<ReadyHook>,
    dispose_hooks: Vec<DisposeHook>,
    start_called: bool,
    stopped: bool,
}

impl ContextInner {
    fn new() -> Self {
        Self {
            parent: None,
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            ready_hooks: Vec::new(),
            dispose_hooks: Vec::new(),
            start_called: false,
            stopped: false,
        }
    }

    fn child(parent: &Rc<ContextInner>) -> Self {
        Self {
            parent: Some(parent.clone()),
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            ready_hooks: Vec::new(),
            dispose_hooks: Vec::new(),
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
/// 内部使用 `Rc` 实现父级服务共享，避免把生命周期暴露给用户。
pub struct Context {
    inner: Rc<ContextInner>,
}

impl Context {
    /// 创建一个空的根上下文。
    pub fn new() -> Self {
        Self {
            inner: Rc::new(ContextInner::new()),
        }
    }

    /// 创建一个子上下文（Scope）。
    ///
    /// Scope 共享父级服务，但在 scope 存活期间父上下文不能执行可变操作。
    pub fn scope(&self) -> Scope {
        Scope {
            ctx: Context {
                inner: Rc::new(ContextInner::child(&self.inner)),
            },
        }
    }

    /// 注册一个插件。
    ///
    /// 插件会在注册时调用 `apply`，之后被保存到内部。
    ///
    /// 如果 `apply` 失败，该插件本次产生的副作用会被回滚：
    ///
    /// - 新注册的服务会被移除
    /// - 新注册的 ready / dispose 回调会被丢弃
    /// - 嵌套注册的插件会被移除
    ///
    /// 已经成功注册的插件不受影响。
    pub fn plugin<P: Plugin + 'static>(&mut self, plugin: P) -> Result<(), Error> {
        let snapshot = {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            (
                inner.services.type_ids(),
                inner.plugins.len(),
                inner.ready_hooks.len(),
                inner.dispose_hooks.len(),
            )
        };

        let (service_keys, plugin_len, ready_len, dispose_len) = snapshot;

        if let Err(err) = plugin.apply(self) {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.services.retain(&service_keys);
            inner.plugins.truncate(plugin_len);
            inner.ready_hooks.truncate(ready_len);
            inner.dispose_hooks.truncate(dispose_len);
            return Err(Error::PluginApply(err.to_string()));
        }

        let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.plugins.push(Box::new(plugin));
        Ok(())
    }

    /// 批量注册插件。
    ///
    /// 不是事务性操作：如果第 N 个插件失败，
    /// 第 N 个插件自身会回滚，前 N-1 个插件保持已注册状态，
    /// 后续插件不再继续注册。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin + 'static,
    {
        for plugin in plugins {
            self.plugin(plugin)?;
        }
        Ok(())
    }

    /// 注册服务。
    pub fn provide<T: 'static>(&mut self, value: T) -> Result<(), Error> {
        let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.provide(value)
    }

    /// 获取服务引用。
    ///
    /// 查找顺序：当前上下文局部服务 -> 父级上下文服务（递归）。
    pub fn require<T: 'static>(&self) -> Result<&T, Error> {
        self.inner.require()
    }

    /// 获取本地服务可变引用。
    ///
    /// 父级服务通过子上下文不可变共享，因此这里只访问当前上下文局部服务。
    pub fn require_mut<T: 'static>(&mut self) -> Result<&mut T, Error> {
        let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.get_mut()
    }

    /// 判断服务是否存在，同时检查局部和父级上下文。
    pub fn contains<T: 'static>(&self) -> bool {
        self.inner.contains::<T>()
    }

    /// 注册一个 ready 回调。
    ///
    /// 在 `start()` 中，所有插件的 `start()` 执行完毕后调用。
    pub fn on_ready(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    ) -> Result<(), Error> {
        let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.ready_hooks.push(Box::new(hook));
        Ok(())
    }

    /// 注册一个 dispose 回调。
    ///
    /// 在 `stop()` 中，所有插件的 `stop()` 执行完毕后调用。
    pub fn on_dispose(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    ) -> Result<(), Error> {
        let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.dispose_hooks.push(Box::new(hook));
        Ok(())
    }

    /// 检查所有插件的依赖是否满足。
    ///
    /// 依赖检查包含局部服务和父级服务。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        self.inner.verify_dependencies()
    }

    /// 启动上下文。
    ///
    /// 只启动当前上下文内注册的插件，不影响父级。
    pub fn start(&mut self) -> Result<(), Error> {
        {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.start_called {
                return Ok(());
            }
            inner.verify_dependencies()?;
            inner.start_called = true;
        }

        for plugin in &self.inner.plugins {
            plugin.start(&*self)?;
        }

        let mut hooks = {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            mem::take(&mut inner.ready_hooks)
        };
        for hook in &mut hooks {
            hook(self)?;
        }

        Ok(())
    }

    /// 停止上下文。
    ///
    /// 只停止当前上下文内注册的插件，不影响父级。
    pub fn stop(&mut self) -> Result<(), Error> {
        let mut errors = Vec::new();

        let plugins = {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.stopped {
                return Ok(());
            }
            inner.stopped = true;
            mem::take(&mut inner.plugins)
        };

        for plugin in plugins.iter().rev() {
            if let Err(err) = plugin.stop(self) {
                errors.push(err);
            }
        }

        {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.plugins = plugins;
        }

        let mut hooks = {
            let inner = Rc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            mem::take(&mut inner.dispose_hooks)
        };
        for hook in &mut hooks {
            if let Err(err) = hook(self) {
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
pub struct Scope {
    ctx: Context,
}

impl Scope {
    /// 注册插件。
    pub fn plugin<P: Plugin + 'static>(&mut self, plugin: P) -> Result<(), Error> {
        self.ctx.plugin(plugin)
    }

    /// 批量注册插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin + 'static,
    {
        self.ctx.plugins(plugins)
    }

    /// 注册局部服务。
    pub fn provide<T: 'static>(&mut self, value: T) -> Result<(), Error> {
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
    pub fn on_ready(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    ) -> Result<(), Error> {
        self.ctx.on_ready(hook)
    }

    /// 注册 dispose 回调。
    pub fn on_dispose(
        &mut self,
        hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static,
    ) -> Result<(), Error> {
        self.ctx.on_dispose(hook)
    }

    /// 启动 scope。
    pub fn start(&mut self) -> Result<(), Error> {
        self.ctx.start()
    }

    /// 停止 scope。
    pub fn stop(&mut self) -> Result<(), Error> {
        self.ctx.stop()
    }
}
