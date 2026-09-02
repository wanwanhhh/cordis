//! 核心上下文。

use std::mem;

use crate::{Error, Plugin, ServiceRegistry};

type ReadyHook = Box<dyn FnMut(&mut Context) -> Result<(), Error>>;
type DisposeHook = Box<dyn FnMut(&mut Context) -> Result<(), Error>>;

/// Cordis 核心上下文。
///
/// 负责管理插件、服务和生命周期。
pub struct Context {
    services: ServiceRegistry,
    plugins: Vec<Box<dyn Plugin>>,
    ready_hooks: Vec<ReadyHook>,
    dispose_hooks: Vec<DisposeHook>,
    /// 是否已经调用过 `start()`（无论成功与否）。
    start_called: bool,
    /// 是否已经调用过 `stop()`。
    stopped: bool,
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}

impl Context {
    /// 创建一个空上下文。
    pub fn new() -> Self {
        Self {
            services: ServiceRegistry::new(),
            plugins: Vec::new(),
            ready_hooks: Vec::new(),
            dispose_hooks: Vec::new(),
            start_called: false,
            stopped: false,
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
        let service_keys = self.services.type_ids();
        let plugin_len = self.plugins.len();
        let ready_len = self.ready_hooks.len();
        let dispose_len = self.dispose_hooks.len();

        if let Err(err) = plugin.apply(self) {
            self.services.retain(&service_keys);
            self.plugins.truncate(plugin_len);
            self.ready_hooks.truncate(ready_len);
            self.dispose_hooks.truncate(dispose_len);
            return Err(Error::PluginApply(err.to_string()));
        }

        self.plugins.push(Box::new(plugin));
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
        self.services.provide(value)
    }

    /// 获取服务引用。
    pub fn require<T: 'static>(&self) -> Result<&T, Error> {
        self.services.get()
    }

    /// 获取服务可变引用。
    ///
    /// # 注意
    ///
    /// 该 API 适合在启动前、或明确持有独占访问的场景使用。
    /// 多插件共享状态建议使用 `Arc<RwLock<T>>`。
    pub fn require_mut<T: 'static>(&mut self) -> Result<&mut T, Error> {
        self.services.get_mut()
    }

    /// 判断服务是否存在。
    pub fn contains<T: 'static>(&self) -> bool {
        self.services.contains::<T>()
    }

    /// 注册一个 ready 回调。
    ///
    /// 在 `start()` 中，所有插件的 `start()` 执行完毕后调用。
    pub fn on_ready(&mut self, hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static) {
        self.ready_hooks.push(Box::new(hook));
    }

    /// 注册一个 dispose 回调。
    ///
    /// 在 `stop()` 中，所有插件的 `stop()` 执行完毕后调用。
    pub fn on_dispose(&mut self, hook: impl FnMut(&mut Context) -> Result<(), Error> + 'static) {
        self.dispose_hooks.push(Box::new(hook));
    }

    /// 检查所有插件的依赖是否满足。
    ///
    /// 在 `start()` 前调用；也可以在业务逻辑中主动调用。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        for plugin in &self.plugins {
            for dependency in plugin.dependencies() {
                if !self.services.contains_type(dependency.type_id) {
                    return Err(Error::ServiceNotFound(dependency.name.to_string()));
                }
            }
        }
        Ok(())
    }

    /// 启动上下文。
    ///
    /// # 语义
    ///
    /// - 多次调用 `start()` 只有第一次会真正执行，后续调用为 no-op。
    /// - 如果依赖检查失败，本次调用返回错误且不标记为“已调用”，允许补充服务后重试。
    /// - 如果某个插件的 `start()` 失败，之后不再启动剩余插件，且不再执行 ready hooks。
    /// - 失败后应显式调用 `stop()` 清理已经启动的插件。
    pub fn start(&mut self) -> Result<(), Error> {
        if self.start_called {
            return Ok(());
        }

        self.verify_dependencies()?;
        self.start_called = true;

        for plugin in &self.plugins {
            plugin.start(self)?;
        }

        let mut hooks = mem::take(&mut self.ready_hooks);
        for hook in &mut hooks {
            hook(self)?;
        }

        Ok(())
    }

    /// 停止上下文。
    ///
    /// # 语义
    ///
    /// - 多次调用 `stop()` 只有第一次会真正执行，后续调用为 no-op。
    /// - 即使某个插件的 `stop()` 失败，也会继续逆序停止剩余插件。
    /// - 即使插件停止失败，dispose hooks 仍然会全部执行。
    /// - 所有错误会聚合在 [`Error::Multiple`] 中返回。
    pub fn stop(&mut self) -> Result<(), Error> {
        if self.stopped {
            return Ok(());
        }
        self.stopped = true;

        let mut errors = Vec::new();

        let plugins = mem::take(&mut self.plugins);
        for plugin in plugins.iter().rev() {
            if let Err(err) = plugin.stop(self) {
                errors.push(err);
            }
        }
        self.plugins = plugins;

        let mut hooks = mem::take(&mut self.dispose_hooks);
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
