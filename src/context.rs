//! 核心上下文与 Scope。

use std::any::TypeId;
use std::future::Future;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::event::{ErasedEventHandler, Subscription, TypedEventHandler};
use crate::{Error, Event, EventControl, EventHandler, Plugin, ServiceRegistry};

static NEXT_CONTEXT_ID: AtomicUsize = AtomicUsize::new(0);

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

/// 异步闭包适配器，用于注册异步生命周期回调。
///
/// 回调接收一个克隆后的 `Context`，便于在异步任务中读取服务并向 runtime 共享。
///
/// 用法：
///
/// ```ignore
/// ctx.on_ready(AsyncHook(|ctx: Context| async move {
///     let service = ctx.require::<MyService>()?;
///     service.init().await?;
///     Ok(())
/// }))?;
/// ```
pub struct AsyncHook<F>(pub F);

#[async_trait]
impl<F, Fut> LifecycleHook for AsyncHook<F>
where
    F: FnMut(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + 'static,
{
    async fn call(&mut self, ctx: &mut Context) -> Result<(), Error> {
        let ctx = ctx.clone();
        (self.0)(ctx).await
    }
}

/// 内部可共享状态。
struct ContextInner {
    context_id: usize,
    parent: Option<Arc<ContextInner>>,
    services: ServiceRegistry,
    plugins: Vec<Box<dyn Plugin>>,
    plugin_names: Vec<&'static str>,
    ready_hooks: Mutex<Vec<ReadyHook>>,
    dispose_hooks: Mutex<Vec<DisposeHook>>,
    event_handlers: Mutex<Vec<Arc<dyn ErasedEventHandler>>>,
    next_subscription_id: usize,
    start_order: Vec<usize>,
    start_called: bool,
    stopped: bool,
}

impl ContextInner {
    fn new() -> Self {
        Self {
            context_id: NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: None,
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            plugin_names: Vec::new(),
            ready_hooks: Mutex::new(Vec::new()),
            dispose_hooks: Mutex::new(Vec::new()),
            event_handlers: Mutex::new(Vec::new()),
            next_subscription_id: 0,
            start_order: Vec::new(),
            start_called: false,
            stopped: false,
        }
    }

    fn child(parent: &Arc<ContextInner>) -> Self {
        Self {
            context_id: NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: Some(parent.clone()),
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            plugin_names: Vec::new(),
            ready_hooks: Mutex::new(Vec::new()),
            dispose_hooks: Mutex::new(Vec::new()),
            event_handlers: Mutex::new(Vec::new()),
            next_subscription_id: 0,
            start_order: Vec::new(),
            start_called: false,
            stopped: false,
        }
    }

    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
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

    fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        match self.services.try_get::<T>()? {
            Some(value) => Ok(Some(value)),
            None => match &self.parent {
                Some(parent) => parent.try_require::<T>(),
                None => Ok(None),
            },
        }
    }

    fn all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.services.all()
    }

    fn contains<T: Send + Sync + 'static>(&self) -> bool {
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

    fn event_handlers_for<E: Event>(&self) -> Vec<Arc<dyn ErasedEventHandler>> {
        self.event_handlers
            .lock()
            .unwrap()
            .iter()
            .filter(|handler| handler.event_type_id() == std::any::TypeId::of::<E>())
            .cloned()
            .collect()
    }

    fn has_plugin(&self, name: &str) -> bool {
        if self.plugin_names.contains(&name) {
            return true;
        }
        match &self.parent {
            Some(parent) => parent.has_plugin(name),
            None => false,
        }
    }

    fn compute_start_order(&self) -> Result<Vec<usize>, Error> {
        let n = self.plugins.len();
        let mut indegree = vec![0usize; n];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

        for (i, plugin) in self.plugins.iter().enumerate() {
            for plugin_dependency in plugin.plugin_dependencies() {
                match self
                    .plugin_names
                    .iter()
                    .position(|name| *name == plugin_dependency.plugin_name)
                {
                    Some(j) => {
                        dependents[j].push(i);
                        indegree[i] += 1;
                    }
                    None => {
                        // 目标插件可能位于父级 Scope：依赖检查已通过时不需要本地排序边。
                        if !plugin_dependency.optional
                            && !self.has_plugin(plugin_dependency.plugin_name)
                        {
                            return Err(Error::PluginDependencyNotFound(
                                plugin_dependency.plugin_name.to_string(),
                            ));
                        }
                    }
                }
            }
        }

        let mut order = Vec::with_capacity(n);
        let mut remaining: Vec<usize> = (0..n).collect();

        while !remaining.is_empty() {
            let candidates: Vec<usize> = remaining
                .iter()
                .copied()
                .filter(|&index| indegree[index] == 0)
                .collect();

            if candidates.is_empty() {
                return Err(Error::PluginDependencyCycle);
            }

            let next = *candidates
                .iter()
                .max_by(|&&a, &&b| {
                    self.plugins[a]
                        .priority()
                        .cmp(&self.plugins[b].priority())
                        .then(b.cmp(&a))
                })
                .expect("candidates is not empty");

            order.push(next);

            for &dependent in &dependents[next] {
                indegree[dependent] -= 1;
            }

            remaining.retain(|&index| index != next);
        }

        Ok(order)
    }

    fn verify_dependencies(&self) -> Result<(), Error> {
        for plugin in &self.plugins {
            for dependency in plugin.dependencies() {
                if !dependency.optional && !self.contains_type(dependency.type_id) {
                    return Err(Error::ServiceNotFound(dependency.name.to_string()));
                }
            }

            for plugin_dependency in plugin.plugin_dependencies() {
                if !plugin_dependency.optional && !self.has_plugin(plugin_dependency.plugin_name) {
                    return Err(Error::PluginDependencyNotFound(
                        plugin_dependency.plugin_name.to_string(),
                    ));
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
        let name = plugin.name();

        let snapshot = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.plugin_names.contains(&name) {
                return Err(Error::PluginNameAlreadyRegistered(name.to_string()));
            }
            (
                inner.services.type_ids(),
                inner.plugins.len(),
                inner.plugin_names.len(),
                inner.ready_hooks.lock().unwrap().len(),
                inner.dispose_hooks.lock().unwrap().len(),
            )
        };

        let (service_keys, plugin_len, plugin_names_len, ready_len, dispose_len) = snapshot;

        if let Err(err) = plugin.apply(self) {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.services.retain(&service_keys);
            inner.plugins.truncate(plugin_len);
            inner.plugin_names.truncate(plugin_names_len);
            inner.ready_hooks.lock().unwrap().truncate(ready_len);
            inner.dispose_hooks.lock().unwrap().truncate(dispose_len);
            return Err(Error::PluginApply(err.to_string()));
        }

        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.plugins.push(Box::new(plugin));
        inner.plugin_names.push(name);
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

    /// 注册插件并注入配置。
    ///
    /// 配置会以 `C` 类型作为当前 Context 的服务注入；插件可通过 `require::<C>()` 读取。
    /// 如果插件 `apply` 失败，配置服务也会一起回滚。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        let snapshot = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.services.type_ids()
        };

        self.provide(config)?;

        if let Err(err) = self.plugin(plugin) {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            inner.services.retain(&snapshot);
            return Err(err);
        }

        Ok(())
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.provide(value)
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.provide_factory(factory)
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.provide_collect(value)
    }

    /// 尝试获取服务（普通服务或工厂，含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.inner.try_require()
    }

    /// 获取当前 Context 局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.inner.all()
    }

    /// 获取服务引用。
    ///
    /// 查找顺序：当前上下文局部服务 -> 父级上下文服务（递归）。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.inner.require()
    }

    /// 获取本地服务可变引用。
    pub fn require_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        inner.services.get_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.inner.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.inner.has_plugin(name)
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

    /// 注册一个事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        let id = inner.next_subscription_id;
        inner.next_subscription_id += 1;
        inner
            .event_handlers
            .lock()
            .unwrap()
            .push(Arc::new(TypedEventHandler::new(id, handler)));
        Ok(Subscription {
            context_id: inner.context_id,
            handler_id: id,
        })
    }

    /// 取消一个事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
        if subscription.context_id != inner.context_id {
            return Err(Error::SubscriptionNotFound);
        }

        let mut handlers = inner.event_handlers.lock().unwrap();
        if let Some(index) = handlers
            .iter()
            .position(|handler| handler.id() == subscription.handler_id)
        {
            handlers.remove(index);
            Ok(())
        } else {
            Err(Error::SubscriptionNotFound)
        }
    }

    /// 串行发出事件，并沿 Scope 向上冒泡。
    pub async fn emit<E: Event>(&self, event: E) -> Result<(), Error> {
        self.emit_impl(event, false).await
    }

    /// 并发发出事件，并沿 Scope 向上冒泡。
    pub async fn emit_parallel<E: Event>(&self, event: E) -> Result<(), Error> {
        self.emit_impl(event, true).await
    }

    async fn emit_impl<E: Event>(&self, event: E, parallel: bool) -> Result<(), Error> {
        let mut current = Some(self.inner.clone());

        while let Some(inner) = current {
            let handlers = inner.event_handlers_for::<E>();

            if parallel {
                let results = futures::future::join_all(
                    handlers.iter().map(|handler| handler.call(&event, &*self)),
                )
                .await;

                let mut errors = Vec::new();
                let mut bail = false;
                for result in results {
                    match result {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => bail = true,
                        Err(err) => errors.push(err),
                    }
                }

                if !errors.is_empty() {
                    return Err(Error::Multiple(errors));
                }
                if bail {
                    return Ok(());
                }
            } else {
                for handler in &handlers {
                    match handler.call(&event, self).await {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => return Ok(()),
                        Err(err) => return Err(err),
                    }
                }
            }

            current = inner.parent.clone();
        }

        Ok(())
    }

    /// 异步启动上下文。
    ///
    /// 只启动当前上下文内注册的插件，不影响父级。
    pub async fn start(&mut self) -> Result<(), Error> {
        let order = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.start_called {
                return Ok(());
            }
            inner.verify_dependencies()?;
            let order = inner.compute_start_order()?;
            inner.start_order = order.clone();
            inner.start_called = true;
            order
        };

        for index in order {
            self.inner.plugins[index].start(&*self).await?;
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

        let (plugins, order) = {
            let inner = Arc::get_mut(&mut self.inner).ok_or(Error::ContextShared)?;
            if inner.stopped {
                return Ok(());
            }
            inner.stopped = true;
            let order = if inner.start_order.is_empty() {
                (0..inner.plugins.len()).collect::<Vec<_>>()
            } else {
                inner.start_order.clone()
            };
            (mem::take(&mut inner.plugins), order)
        };

        for &index in order.iter().rev() {
            if let Some(plugin) = plugins.get(index)
                && let Err(err) = plugin.stop(self).await
            {
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

    /// 注册插件并注入配置。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        self.ctx.plugin_with_config(plugin, config)
    }

    /// 注册局部服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.ctx.provide(value)
    }

    /// 注册懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        self.ctx.provide_factory(factory)
    }

    /// 注册集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.ctx.provide_collect(value)
    }

    /// 尝试获取服务（含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.ctx.try_require()
    }

    /// 获取 Scope 局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.ctx.require_all()
    }

    /// 查询服务：局部优先，父级兜底。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.ctx.require()
    }

    /// 获取本地服务可变引用。
    pub fn require_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        self.ctx.require_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.ctx.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.ctx.has_plugin(name)
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

    /// 注册事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        self.ctx.on(handler)
    }

    /// 取消事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        self.ctx.off(subscription)
    }

    /// 串行发出事件。
    pub async fn emit<E: Event>(&self, event: E) -> Result<(), Error> {
        self.ctx.emit(event).await
    }

    /// 并发发出事件。
    pub async fn emit_parallel<E: Event>(&self, event: E) -> Result<(), Error> {
        self.ctx.emit_parallel(event).await
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
