//! 三段式上下文模型：Builder / Context / Runtime。

use std::any::TypeId;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::event::{ErasedEventHandler, Subscription, TypedEventHandler};
use crate::{
    DynamicValue, Error, ErrorKind, Event, EventControl, EventHandler, Phase, Plugin, PluginScope,
    ServiceRegistry,
};

static NEXT_CONTEXT_ID: AtomicUsize = AtomicUsize::new(0);

const STOPPED: u64 = 1 << 63;
const COUNT_MASK: u64 = STOPPED - 1;

type ReadyHook = Box<dyn LifecycleHook>;
type DisposeHook = Box<dyn LifecycleHook>;

/// 异步生命周期回调。
#[async_trait]
pub trait LifecycleHook: Send + Sync + 'static {
    /// 执行回调。回调只获得只读 `Context`。
    async fn call(&mut self, ctx: &Context) -> Result<(), Error>;
}

/// 同步闭包适配器，便于将普通 `FnMut(&Context) -> Result<(), Error>` 注册为异步钩子。
pub struct SyncHook<F>(pub F);

#[async_trait]
impl<F> LifecycleHook for SyncHook<F>
where
    F: for<'a> FnMut(&'a Context) -> Result<(), Error> + Send + Sync + 'static,
{
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        (self.0)(ctx)
    }
}

/// 异步闭包适配器，用于注册异步生命周期回调。
pub struct AsyncHook<F>(pub F);

#[async_trait]
impl<F, Fut> LifecycleHook for AsyncHook<F>
where
    F: FnMut(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + 'static,
{
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        let ctx = ctx.clone();
        (self.0)(ctx).await
    }
}

/// 数据面：build 后只读，由 `Context` 的 `Arc` 共享。
struct Data {
    context_id: usize,
    parent: Option<Arc<Data>>,
    services: ServiceRegistry,
    plugin_names: Vec<&'static str>,
    event_handlers: Vec<Arc<dyn ErasedEventHandler>>,
    next_subscription_id: usize,
    state: AtomicU64,
}

impl Data {
    fn root() -> Self {
        Self {
            context_id: NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: None,
            services: ServiceRegistry::new(),
            plugin_names: Vec::new(),
            event_handlers: Vec::new(),
            next_subscription_id: 0,
            state: AtomicU64::new(0),
        }
    }

    fn child(parent: Arc<Data>) -> Self {
        Self {
            context_id: NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: Some(parent),
            services: ServiceRegistry::new(),
            plugin_names: Vec::new(),
            event_handlers: Vec::new(),
            next_subscription_id: 0,
            state: AtomicU64::new(0),
        }
    }

    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        match self.services.get::<T>() {
            Ok(value) => Ok(value),
            Err(err) => {
                if matches!(err.kind, ErrorKind::ServiceNotFound(_)) {
                    match &self.parent {
                        Some(parent) => parent.require::<T>(),
                        None => Err(Error::new(
                            Phase::Build,
                            ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
                        )),
                    }
                } else {
                    Err(err)
                }
            }
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

    fn all_with_parents<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        let mut values = self.services.all::<T>()?;
        if let Some(parent) = &self.parent {
            values.extend(parent.all_with_parents::<T>()?);
        }
        Ok(values)
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
}

type Snapshot = (
    HashSet<TypeId>,
    usize,
    usize,
    usize,
    usize,
    Vec<Arc<dyn ErasedEventHandler>>,
    usize,
);

/// 缓存插件注册时求值的依赖信息。
struct PluginRecord {
    plugin: Box<dyn Plugin>,
    deps: Vec<crate::Dependency>,
    plugin_deps: Vec<crate::PluginDependency>,
}

impl PluginRecord {
    fn name(&self) -> &'static str {
        self.plugin.name()
    }

    fn priority(&self) -> i32 {
        self.plugin.priority()
    }
}

/// 统一的插件依赖拓扑排序。
///
/// `local_names` 是本层已注册插件名；`has_plugin` 用于检查父级插件是否存在。
/// 可选插件依赖只放宽“必须存在”的校验，不改变“存在则必须按依赖顺序启动”的语义。
fn compute_start_order(
    plugins: &[PluginRecord],
    local_names: &[&'static str],
    has_plugin: impl Fn(&str) -> bool,
) -> Result<Vec<usize>, Error> {
    let n = plugins.len();
    let mut indegree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (i, record) in plugins.iter().enumerate() {
        for plugin_dependency in &record.plugin_deps {
            match local_names
                .iter()
                .position(|name| *name == plugin_dependency.plugin_name)
            {
                Some(j) => {
                    dependents[j].push(i);
                    indegree[i] += 1;
                }
                None => {
                    // 目标插件可能位于父级：依赖检查已通过时不需要本地排序边。
                    if !plugin_dependency.optional && !has_plugin(plugin_dependency.plugin_name) {
                        return Err(Error::new(
                            Phase::Verify,
                            ErrorKind::PluginDependencyNotFound(
                                plugin_dependency.plugin_name.to_string(),
                            ),
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
            return Err(Error::new(Phase::Verify, ErrorKind::PluginDependencyCycle));
        }

        let next = *candidates
            .iter()
            .max_by(|&&a, &&b| {
                plugins[a]
                    .priority()
                    .cmp(&plugins[b].priority())
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

/// 把拓扑序切分为可并发的层。
///
/// 同一层内没有尚未启动的本地插件依赖；父级插件依赖不产生本地层间边。
fn compute_start_layers(
    plugins: &[PluginRecord],
    order: &[usize],
) -> Result<Vec<Vec<usize>>, Error> {
    let mut remaining: Vec<usize> = order.to_vec();
    let mut layers = Vec::new();

    while !remaining.is_empty() {
        let mut layer = Vec::new();
        let mut next_remaining = Vec::new();

        for &index in &remaining {
            let depends_on_remaining = plugins[index].plugin_deps.iter().any(|dep| {
                remaining
                    .iter()
                    .any(|&r| plugins[r].name() == dep.plugin_name)
            });

            if depends_on_remaining {
                next_remaining.push(index);
            } else {
                layer.push(index);
            }
        }

        if layer.is_empty() {
            // 该状态与拓扑排序结果矛盾，说明内部依赖解析不一致；应直接暴露，避免静默死循环。
            return Err(Error::new(Phase::Start, ErrorKind::PluginDependencyCycle));
        }

        layers.push(layer);
        remaining = next_remaining;
    }

    Ok(layers)
}

/// 子作用域租约：持有父 `Data` 强引用，`Drop` 时归还父计数。
struct ScopeLease(Arc<Data>);

impl Drop for ScopeLease {
    fn drop(&mut self) {
        self.0.state.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 装配阶段：独占 `&mut`，不 `Clone`。
pub struct Builder {
    data: Data,
    plugins: Vec<PluginRecord>,
    ready: Vec<ReadyHook>,
    dispose: Vec<DisposeHook>,
    lease: Option<ScopeLease>,
}

impl Builder {
    /// 创建一个空的根 Builder。
    pub fn new() -> Self {
        Self {
            data: Data::root(),
            plugins: Vec::new(),
            ready: Vec::new(),
            dispose: Vec::new(),
            lease: None,
        }
    }

    fn child(parent: Arc<Data>, lease: ScopeLease) -> Self {
        Self {
            data: Data::child(parent),
            plugins: Vec::new(),
            ready: Vec::new(),
            dispose: Vec::new(),
            lease: Some(lease),
        }
    }

    /// 当前 Builder 在作用域树中的深度；根 Builder 为 0。
    ///
    /// 该方法是公开的辅助查询接口，可与 [`Builder::is_root`] 配合使用。
    pub fn depth(&self) -> usize {
        let mut depth = 0;
        let mut current = &self.data;
        while let Some(parent) = &current.parent {
            depth += 1;
            current = parent.as_ref();
        }
        depth
    }

    /// 当前 Builder 是否为根 Builder。
    pub fn is_root(&self) -> bool {
        self.data.parent.is_none()
    }

    fn snapshot(&self) -> Snapshot {
        (
            self.data.services.type_ids(),
            self.plugins.len(),
            self.data.plugin_names.len(),
            self.ready.len(),
            self.dispose.len(),
            self.data.event_handlers.clone(),
            self.data.next_subscription_id,
        )
    }

    fn restore(&mut self, snapshot: Snapshot) {
        let (
            service_keys,
            plugin_len,
            plugin_names_len,
            ready_len,
            dispose_len,
            event_handlers,
            next_subscription_id,
        ) = snapshot;
        self.data.services.retain(&service_keys);
        self.plugins.truncate(plugin_len);
        self.data.plugin_names.truncate(plugin_names_len);
        self.ready.truncate(ready_len);
        self.dispose.truncate(dispose_len);
        self.data.event_handlers = event_handlers;
        self.data.next_subscription_id = next_subscription_id;
    }

    /// 注册一个插件。
    ///
    /// 插件依赖在注册时求值并缓存。如果 `apply` 失败，本插件产生的所有副作用
    /// 会回滚到进入 `apply` 之前的状态。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        let name = plugin.name();
        let declared_scope = plugin.scope();
        let actual_scope = if self.is_root() {
            PluginScope::Root
        } else {
            PluginScope::Child
        };

        if declared_scope != PluginScope::Any && declared_scope != actual_scope {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::PluginScopeMismatch {
                    plugin_name: name.to_string(),
                    expected: declared_scope,
                    actual: actual_scope,
                },
            ));
        }

        let deps = plugin.dependencies();
        let plugin_deps = plugin.plugin_dependencies();

        if self.data.plugin_names.contains(&name) {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::PluginNameAlreadyRegistered(name.to_string()),
            ));
        }

        let snapshot = self.snapshot();

        let apply_result = {
            let mut cfg = Configurator { builder: self };
            plugin.apply(&mut cfg)
        };

        if let Err(err) = apply_result {
            self.restore(snapshot);
            return Err(err.into_phase(Phase::Apply, Some(name)));
        }

        self.plugins.push(PluginRecord {
            plugin: Box::new(plugin),
            deps,
            plugin_deps,
        });
        self.data.plugin_names.push(name);
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
    /// 配置会以 `C` 类型作为当前 Builder 的服务注入；插件可通过 `require::<C>()` 读取。
    /// 如果插件 `apply` 失败，配置服务也会一起回滚。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        let snapshot = self.data.services.type_ids();
        self.provide(config)?;

        if let Err(err) = self.plugin(plugin) {
            self.data.services.retain(&snapshot);
            return Err(err);
        }

        Ok(())
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.data.services.provide(value)
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        self.data.services.provide_factory(factory)
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.data.services.provide_collect(value)
    }

    /// 注册一个运行时动态配置服务。
    ///
    /// 实际服务类型为 `Arc<DynamicValue<T>>`，不会占用原 `T` 的类型槽位。
    pub fn provide_dynamic<T: Send + Sync + 'static>(&mut self, initial: T) -> Result<(), Error> {
        let value = Arc::new(DynamicValue::new(initial));
        self.provide(value)
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.require::<Arc<DynamicValue<T>>>().map(Arc::clone)
    }

    /// 尝试获取服务（普通服务或工厂，含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.data.try_require()
    }

    /// 获取本层局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.data.all()
    }

    /// 获取本层及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.data.all_with_parents()
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.data.require()
    }

    /// 获取本地服务可变引用（仅构建期可调用）。
    pub fn require_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        self.data.services.get_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.data.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.data.has_plugin(name)
    }

    /// 注册一个 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.ready.push(Box::new(hook));
        Ok(())
    }

    /// 注册一个 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.dispose.push(Box::new(hook));
        Ok(())
    }

    /// 注册一个事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        let id = self.data.next_subscription_id;
        self.data.next_subscription_id += 1;
        self.data
            .event_handlers
            .push(Arc::new(TypedEventHandler::new(id, handler)));
        Ok(Subscription {
            context_id: self.data.context_id,
            handler_id: id,
        })
    }

    /// 取消一个事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        if subscription.context_id != self.data.context_id {
            return Err(Error::new(Phase::Build, ErrorKind::SubscriptionNotFound));
        }

        if let Some(index) = self
            .data
            .event_handlers
            .iter()
            .position(|handler| handler.id() == subscription.handler_id)
        {
            self.data.event_handlers.remove(index);
            Ok(())
        } else {
            Err(Error::new(Phase::Build, ErrorKind::SubscriptionNotFound))
        }
    }

    /// 校验依赖：服务依赖、插件依赖和环。
    pub fn verify(&self) -> Result<(), Error> {
        self.verify_dependencies()
    }

    /// 校验所有插件的依赖是否满足。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        for record in &self.plugins {
            for dependency in &record.deps {
                if !dependency.optional && !self.data.contains_type(dependency.type_id) {
                    return Err(Error::new(
                        Phase::Verify,
                        ErrorKind::ServiceNotFound(dependency.name.to_string()),
                    ));
                }
            }

            for plugin_dependency in &record.plugin_deps {
                if !plugin_dependency.optional
                    && !self.data.has_plugin(plugin_dependency.plugin_name)
                {
                    return Err(Error::new(
                        Phase::Verify,
                        ErrorKind::PluginDependencyNotFound(
                            plugin_dependency.plugin_name.to_string(),
                        ),
                    ));
                }
            }
        }

        self.compute_start_order()?;
        Ok(())
    }

    fn compute_start_order(&self) -> Result<Vec<usize>, Error> {
        compute_start_order(&self.plugins, &self.data.plugin_names, |name| {
            self.data.has_plugin(name)
        })
    }

    /// 消费 Builder 并产出唯一冻结的 Runtime。
    ///
    /// 冻结点是唯一一次 `Arc::new(data)`。
    pub fn build(self) -> Result<Runtime, Error> {
        self.verify()?;

        let Builder {
            data,
            plugins,
            ready,
            dispose,
            lease,
        } = self;

        Ok(Runtime {
            ctx: Context {
                inner: Arc::new(data),
            },
            plugins,
            ready,
            dispose,
            started: false,
            stopped: false,
            started_plugins: Vec::new(),
            _lease: lease,
        })
    }

    /// 消费 Builder；校验失败时把 Builder（含租约）完整带回。
    #[allow(clippy::result_large_err)]
    pub fn try_build(self) -> Result<Runtime, (Builder, Error)> {
        if let Err(err) = self.verify() {
            return Err((self, err));
        }

        Ok(self.build().expect("verify() already succeeded"))
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// 插件 `apply` 阶段使用的窄接口。
pub struct Configurator<'a> {
    builder: &'a mut Builder,
}

impl Configurator<'_> {
    /// 尝试获取服务（普通服务或工厂，含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.builder.try_require()
    }

    /// 获取本层局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.builder.require_all()
    }

    /// 获取本层及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.builder.require_all_recursive()
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.builder.require_dynamic()
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.builder.require()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.builder.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.builder.has_plugin(name)
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.builder.provide(value)
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        self.builder.provide_factory(factory)
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.builder.provide_collect(value)
    }

    /// 注册一个运行时动态配置服务。
    pub fn provide_dynamic<T: Send + Sync + 'static>(&mut self, initial: T) -> Result<(), Error> {
        self.builder.provide_dynamic(initial)
    }

    /// 注册子插件。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        self.builder.plugin(plugin)
    }

    /// 批量注册子插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin,
    {
        self.builder.plugins(plugins)
    }

    /// 注册子插件并注入配置。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        self.builder.plugin_with_config(plugin, config)
    }

    /// 注册 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.builder.on_ready(hook)
    }

    /// 注册 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.builder.on_dispose(hook)
    }

    /// 注册事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        self.builder.on(handler)
    }

    /// 取消事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        self.builder.off(subscription)
    }
}

/// 只读数据句柄；`Clone + Send + Sync`。
#[derive(Clone)]
pub struct Context {
    inner: Arc<Data>,
}

impl Context {
    /// 创建一个子 Builder。
    ///
    /// 该方法会在父 `Data.state` 上原子的递增 child 计数；若父已进入停止，
    /// 返回 `ErrorKind::Stopping`。
    pub fn scope(&self) -> Result<Builder, Error> {
        loop {
            let s = self.inner.state.load(Ordering::Acquire);
            if s & STOPPED != 0 {
                return Err(Error::new(Phase::Build, ErrorKind::Stopping));
            }
            if s & COUNT_MASK == COUNT_MASK {
                return Err(Error::new(Phase::Build, ErrorKind::TooManyScopes));
            }
            match self.inner.state.compare_exchange_weak(
                s,
                s + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Ok(Builder::child(
                        self.inner.clone(),
                        ScopeLease(self.inner.clone()),
                    ));
                }
                Err(_) => continue,
            }
        }
    }

    /// 尝试获取服务（普通服务或工厂，含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.inner.try_require()
    }

    /// 获取当前 Context 局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.inner.all()
    }

    /// 获取当前 Context 及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.inner.all_with_parents()
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.require::<Arc<DynamicValue<T>>>().map(Arc::clone)
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.inner.require()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.inner.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.inner.has_plugin(name)
    }

    /// 测试用：读取本层活跃子 Runtime/Builder 的租约计数。
    #[cfg(test)]
    pub(crate) fn child_count(&self) -> u64 {
        self.inner.state.load(Ordering::SeqCst) & COUNT_MASK
    }

    /// 串行发出事件，并沿父链向上冒泡。
    pub async fn emit<E: Event>(&self, event: E) -> Result<(), Error> {
        let errors = self.emit_impl(event, false, false).await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors
                .into_iter()
                .next()
                .expect("strict serial emit returns at most one error"))
        }
    }

    /// 并发发出事件，并沿父链向上冒泡。
    pub async fn emit_parallel<E: Event>(&self, event: E) -> Result<(), Error> {
        let errors = self.emit_impl(event, true, false).await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Event, ErrorKind::Multiple(errors)))
        }
    }

    /// 旁路通知：串行执行事件，handler 错误不阻断后续 handler 和父链冒泡。
    ///
    /// 返回所有收集到的 handler 错误，由调用方决定如何记录。
    pub async fn emit_notify<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, false, true).await
    }

    /// 旁路通知：并行执行事件，handler 错误不阻断父链冒泡。
    ///
    /// 返回所有收集到的 handler 错误，由调用方决定如何记录。
    pub async fn emit_notify_parallel<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, true, true).await
    }

    async fn emit_impl<E: Event>(&self, event: E, parallel: bool, notify: bool) -> Vec<Error> {
        let mut current = Some(self.inner.clone());
        let mut all_errors = Vec::new();

        while let Some(inner) = current {
            let handlers = inner.event_handlers_for::<E>();
            let ctx = Context {
                inner: inner.clone(),
            };

            let mut layer_errors = Vec::new();
            let mut bail = false;

            if parallel {
                let results = futures::future::join_all(
                    handlers.iter().map(|handler| handler.call(&event, &ctx)),
                )
                .await;

                for result in results {
                    match result {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => bail = true,
                        Err(err) => layer_errors.push(err),
                    }
                }
            } else if notify {
                for handler in &handlers {
                    match handler.call(&event, &ctx).await {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => {
                            bail = true;
                            break;
                        }
                        Err(err) => layer_errors.push(err),
                    }
                }
            } else {
                // 严格串行：保留旧语义，第一个错误立即停止。
                for handler in &handlers {
                    match handler.call(&event, &ctx).await {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => return all_errors,
                        Err(err) => {
                            all_errors.push(err);
                            return all_errors;
                        }
                    }
                }
            }

            // 严格并行模式：错误会聚合返回，不再继续向父层冒泡。
            if !notify && parallel && !layer_errors.is_empty() {
                all_errors.extend(layer_errors);
                return all_errors;
            }

            all_errors.extend(layer_errors);

            if bail {
                return all_errors;
            }

            current = inner.parent.clone();
        }

        all_errors
    }
}

/// 生命周期唯一所有者；不 `Clone`，`#[must_use]`。
#[must_use]
pub struct Runtime {
    ctx: Context,
    plugins: Vec<PluginRecord>,
    ready: Vec<ReadyHook>,
    dispose: Vec<DisposeHook>,
    started: bool,
    stopped: bool,
    started_plugins: Vec<usize>,
    /// 私有租约必须作为最后一个字段声明，确保在插件字段析构之后归还父计数。
    #[allow(dead_code)]
    _lease: Option<ScopeLease>,
}

impl Runtime {
    /// 获取只读 `Context` 句柄。
    pub fn handle(&self) -> Context {
        self.ctx.clone()
    }

    async fn start_with(&mut self, serial: bool) -> Result<(), Error> {
        if self.started || self.stopped {
            return Ok(());
        }

        let order = self.compute_start_order()?;
        self.started = true;

        let mut errors = Vec::new();
        if serial {
            for &index in &order {
                self.started_plugins.push(index);
                let record = &self.plugins[index];
                if let Err(err) = record.plugin.start(&self.ctx).await {
                    errors.push(err.into_phase(Phase::Start, Some(record.name())));
                    break;
                }
            }
        } else {
            // 分层并行：拓扑同层无依赖边，可并发启动。
            let layers = match compute_start_layers(&self.plugins, &order) {
                Ok(layers) => layers,
                Err(err) => {
                    self.started = false;
                    errors.push(err);
                    return Err(Error::new(Phase::Start, ErrorKind::Multiple(errors)));
                }
            };

            for layer in layers {
                self.started_plugins.extend(layer.iter().copied());
                let layer_plugins: Vec<(usize, &dyn Plugin)> = layer
                    .iter()
                    .map(|&index| (index, self.plugins[index].plugin.as_ref()))
                    .collect();
                let ctx = &self.ctx;
                let results = futures::future::join_all(
                    layer_plugins.iter().map(|(_, plugin)| plugin.start(ctx)),
                )
                .await;

                for ((index, _plugin), result) in layer_plugins.into_iter().zip(results) {
                    if let Err(err) = result {
                        errors.push(err.into_phase(Phase::Start, Some(self.plugins[index].name())));
                    }
                }

                if !errors.is_empty() {
                    break;
                }
            }
        }

        if errors.is_empty() {
            let ctx = self.ctx.clone();
            for hook in &mut self.ready {
                if let Err(err) = hook.call(&ctx).await {
                    errors.push(err.into_phase(Phase::Ready, None));
                    break;
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Start, ErrorKind::Multiple(errors)))
        }
    }

    /// 默认分层并行启动。
    pub async fn start(&mut self) -> Result<(), Error> {
        self.start_with(false).await
    }

    /// 保留旧语义的串行启动。
    pub async fn start_serial(&mut self) -> Result<(), Error> {
        self.start_with(true).await
    }

    fn compute_start_order(&self) -> Result<Vec<usize>, Error> {
        compute_start_order(&self.plugins, &self.ctx.inner.plugin_names, |name| {
            self.ctx.inner.has_plugin(name)
        })
    }

    /// 异步停止。
    ///
    /// 父/自身 `Runtime::stop` 只接受本层没有活跃子 Runtime；若仍有活跃子，
    /// 返回 `ActiveScopes`，且不进入停止状态。
    pub async fn stop(&mut self) -> Result<(), Error> {
        if self.stopped {
            return Ok(());
        }

        match self
            .ctx
            .inner
            .state
            .compare_exchange(0, STOPPED, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => self.stopped = true,
            Err(s) => {
                return Err(Error::new(
                    Phase::Stop,
                    ErrorKind::ActiveScopes {
                        count: s & COUNT_MASK,
                    },
                ));
            }
        }

        let mut errors = Vec::new();

        let order = if self.started {
            std::mem::take(&mut self.started_plugins)
        } else {
            // 未开始启动时，stop 不调用插件自身 stop；只执行 dispose hooks。
            Vec::new()
        };

        for &index in order.iter().rev() {
            let record = &self.plugins[index];
            if let Err(err) = record.plugin.stop(&self.ctx).await {
                errors.push(err.into_phase(Phase::Stop, Some(record.name())));
            }
        }

        let ctx = self.ctx.clone();
        for hook in &mut self.dispose {
            if let Err(err) = hook.call(&ctx).await {
                errors.push(err.into_phase(Phase::Dispose, None));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Stop, ErrorKind::Multiple(errors)))
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // 不做异步清理。租约释放由最后一个字段 `ScopeLease` 在字段析构阶段完成。
    }
}
