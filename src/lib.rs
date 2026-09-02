//! Cordis 风格的插件化基础架构。
//!
//! 本 crate 是通用插件化基础架构，采用以下核心思想：
//!
//! - 一切插件化
//! - Context 是插件边界
//! - 服务是插件之间的唯一契约
//! - 生命周期由框架管理
//! - 静态集成，不做热加载

mod context;
mod error;
mod plugin;
mod service;

pub use context::{Context, Scope};
pub use error::Error;
pub use plugin::{Dependency, Plugin};
pub use service::ServiceRegistry;

#[cfg(test)]
mod tests {
    use super::*;

    struct Logger {
        name: String,
    }

    struct LoggerPlugin;

    impl Plugin for LoggerPlugin {
        fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
            ctx.provide(Logger {
                name: "main".to_string(),
            })?;

            ctx.on_ready(|ctx| {
                let logger = ctx.require::<Logger>()?;
                assert_eq!(logger.name, "main");
                Ok(())
            })?;

            Ok(())
        }
    }

    #[test]
    fn plugin_and_service_workflow() {
        let mut ctx = Context::new();
        ctx.plugin(LoggerPlugin).unwrap();
        ctx.start().unwrap();
        ctx.stop().unwrap();
    }

    #[test]
    fn duplicate_service_is_rejected() {
        let mut ctx = Context::new();
        ctx.provide(42_u32).unwrap();
        let err = ctx.provide(7_u32).unwrap_err();
        assert_eq!(err, Error::ServiceAlreadyRegistered("u32".to_string()));
    }

    #[test]
    fn closure_can_be_a_plugin() {
        let mut ctx = Context::new();
        ctx.plugin(|ctx: &mut Context| {
            ctx.provide("hello".to_string())?;
            Ok(())
        })
        .unwrap();

        assert_eq!(ctx.require::<String>().unwrap(), "hello");
    }

    #[test]
    fn stop_runs_in_reverse_order() {
        use std::sync::Arc;

        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct P(Arc<std::sync::Mutex<Vec<usize>>>, u8);

        impl Plugin for P {
            fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(self.1 as usize);
                Ok(())
            }

            fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(10 + self.1 as usize);
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(P(observed.clone(), 1)).unwrap();
        ctx.plugin(P(observed.clone(), 2)).unwrap();
        ctx.start().unwrap();
        ctx.stop().unwrap();

        let observed = observed.lock().unwrap();
        // start order: 1, 2
        assert_eq!(&observed[..2], &[1, 2]);
        // stop order is reverse: 2 then 1
        assert_eq!(&observed[2..], &[12, 11]);
    }

    #[test]
    fn dependency_check_runs_before_start() {
        #[derive(Debug)]
        struct Missing;

        struct NeedsMissing;

        impl Plugin for NeedsMissing {
            fn dependencies(&self) -> &'static [Dependency] {
                static DEPENDENCY: std::sync::OnceLock<Dependency> = std::sync::OnceLock::new();
                let dependency = DEPENDENCY.get_or_init(Dependency::of::<Missing>);
                std::slice::from_ref(dependency)
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(NeedsMissing).unwrap();

        // 依赖缺失时 start 应失败，且不会消费掉一次 start 机会。
        let err = ctx.start().unwrap_err();
        assert!(matches!(
            err,
            Error::ServiceNotFound(name) if name.contains("Missing")
        ));

        // 补上依赖后可以重新 start。
        ctx.provide(Missing).unwrap();
        ctx.start().unwrap();
        ctx.stop().unwrap();
    }

    #[test]
    fn plugin_apply_failure_rolls_back_partial_side_effects() {
        struct BadPlugin;

        impl Plugin for BadPlugin {
            fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
                ctx.provide(42_u32)?;
                ctx.on_ready(|_| Ok(()))?;
                Err(Error::PluginApply("boom".to_string()))
            }
        }

        let mut ctx = Context::new();
        assert!(ctx.plugin(BadPlugin).is_err());

        // apply 中已经注册的服务必须在失败后回滚。
        assert!(!ctx.contains::<u32>());
    }

    #[test]
    fn stop_continues_and_dispose_hook_always_runs() {
        use std::sync::Arc;

        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct OkPlugin(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl Plugin for OkPlugin {
            fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("ok_stop");
                Ok(())
            }
        }

        struct BadPlugin(Arc<std::sync::Mutex<Vec<&'static str>>>);

        impl Plugin for BadPlugin {
            fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("bad_stop");
                Err(Error::PluginStop("boom".to_string()))
            }
        }

        let mut ctx = Context::new();
        ctx.on_dispose(|ctx| {
            let observed = ctx.require::<Arc<std::sync::Mutex<Vec<&'static str>>>>()?;
            observed.lock().unwrap().push("dispose");
            Ok(())
        })
        .unwrap();
        ctx.provide(observed.clone()).unwrap();

        ctx.plugin(OkPlugin(observed.clone())).unwrap();
        ctx.plugin(BadPlugin(observed.clone())).unwrap();

        // 即使 bad_stop 失败，也要继续执行 ok_stop 和 dispose。
        assert!(matches!(ctx.stop(), Err(Error::Multiple(_))));

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["bad_stop", "ok_stop", "dispose"]);
    }

    #[test]
    fn repeated_start_stop_are_noop() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let start_count = Arc::new(AtomicUsize::new(0));
        let stop_count = Arc::new(AtomicUsize::new(0));
        let ready_count = Arc::new(AtomicUsize::new(0));
        let dispose_count = Arc::new(AtomicUsize::new(0));

        struct OncePlugin(Arc<AtomicUsize>, Arc<AtomicUsize>);

        impl Plugin for OncePlugin {
            fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }

            fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(OncePlugin(start_count.clone(), stop_count.clone()))
            .unwrap();

        let ready_counter = ready_count.clone();
        ctx.on_ready(move |_| {
            ready_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        let dispose_counter = dispose_count.clone();
        ctx.on_dispose(move |_| {
            dispose_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();

        ctx.start().unwrap();
        ctx.start().unwrap();
        ctx.stop().unwrap();
        ctx.stop().unwrap();

        assert_eq!(start_count.load(Ordering::SeqCst), 1);
        assert_eq!(stop_count.load(Ordering::SeqCst), 1);
        assert_eq!(ready_count.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_service_reports_type_name() {
        let ctx = Context::new();
        let err = ctx.require::<u32>().unwrap_err();
        assert!(matches!(
            err,
            Error::ServiceNotFound(name) if name.contains("u32")
        ));
    }

    #[test]
    fn scope_plugin_can_access_parent_service() {
        struct ParentService(&'static str);

        struct ScopePlugin;

        impl Plugin for ScopePlugin {
            fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
                let service = ctx.require::<ParentService>()?;
                assert_eq!(service.0, "parent");
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.provide(ParentService("parent")).unwrap();

        let mut scope = ctx.scope();
        scope.plugin(ScopePlugin).unwrap();
        scope.start().unwrap();
        scope.stop().unwrap();
    }

    #[test]
    fn scope_service_isolation_and_shadowing() {
        struct ParentService;
        struct ChildService;

        let mut ctx = Context::new();
        ctx.provide(ParentService).unwrap();

        let mut scope = ctx.scope();
        scope.provide(ChildService).unwrap();

        // scope 可以看到父级服务，也可以看到自己的服务
        assert!(scope.contains::<ParentService>());
        assert!(scope.contains::<ChildService>());

        // 父级看不到 scope 的局部服务
        assert!(!ctx.contains::<ChildService>());
        assert!(ctx.contains::<ParentService>());
    }

    #[test]
    fn scope_dependency_check_sees_parent_service() {
        struct ParentService;
        struct NeedsParent;

        impl Plugin for NeedsParent {
            fn dependencies(&self) -> &'static [Dependency] {
                static DEPS: std::sync::OnceLock<Dependency> = std::sync::OnceLock::new();
                let dependency = DEPS.get_or_init(Dependency::of::<ParentService>);
                std::slice::from_ref(dependency)
            }
        }

        let mut ctx = Context::new();
        ctx.provide(ParentService).unwrap();

        let mut scope = ctx.scope();
        scope.plugin(NeedsParent).unwrap();

        // 依赖检查应能看到父级服务
        scope.verify_dependencies().unwrap();
        scope.start().unwrap();
        scope.stop().unwrap();
    }

    #[test]
    fn scope_blocks_parent_mutation_until_dropped() {
        struct ParentService;
        struct ChildService;
        struct DummyPlugin;

        impl Plugin for DummyPlugin {}

        let mut ctx = Context::new();
        ctx.provide(ParentService).unwrap();

        let mut scope = ctx.scope();
        scope.provide(ChildService).unwrap();

        // scope 存活期间父级不能添加服务、注册插件或停止。
        assert!(matches!(
            ctx.provide(ChildService),
            Err(Error::ContextShared)
        ));
        assert!(matches!(ctx.plugin(DummyPlugin), Err(Error::ContextShared)));
        assert!(matches!(ctx.stop(), Err(Error::ContextShared)));

        // scope 销毁后，父级恢复可变能力。
        drop(scope);
        ctx.provide(ChildService).unwrap();
        ctx.stop().unwrap();
    }

    #[test]
    fn explicit_context_typing_does_not_break_scope() {
        // 不依赖泛型生命周期推断，显式声明 Context 也能直接创建 scope。
        let ctx: Context = Context::new();
        let _scope = ctx.scope();
    }
}
