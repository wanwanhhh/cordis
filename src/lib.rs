//! Cordis 风格的插件化基础架构。
//!
//! 本 crate 是通用插件化基础架构，采用以下核心思想：
//!
//! - 一切插件化
//! - Context 是插件边界
//! - 服务是插件之间的唯一契约
//! - 生命周期由框架管理
//! - 静态集成，不做热加载
//!
//! # Example
//!
//! ```rust
//! use cordis::{Context, Error, Plugin};
//!
//! struct MyService;
//!
//! struct MyPlugin;
//!
//! impl Plugin for MyPlugin {
//!     fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
//!         ctx.provide(MyService)?;
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Error> {
//! let mut ctx = Context::new();
//! ctx.plugin(MyPlugin)?;
//! let future = async {
//!     ctx.start().await?;
//!     ctx.stop().await?;
//!     Ok::<(), Error>(())
//! };
//! // 实际使用时用任意 runtime 执行 future。
//! # Ok(())
//! # }
//! ```

mod context;
mod error;
mod plugin;
mod service;

pub use context::{Context, LifecycleHook, Scope, SyncHook};
pub use error::Error;
pub use plugin::{Dependency, Plugin};
pub use service::ServiceRegistry;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        futures::executor::block_on(future)
    }

    struct Logger {
        name: String,
    }

    struct LoggerPlugin;

    #[async_trait]
    impl Plugin for LoggerPlugin {
        fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
            ctx.provide(Logger {
                name: "main".to_string(),
            })?;

            ctx.on_ready(SyncHook(|ctx: &mut Context| {
                let logger = ctx.require::<Logger>()?;
                assert_eq!(logger.name, "main");
                Ok(())
            }))?;

            Ok(())
        }
    }

    #[test]
    fn plugin_and_service_workflow() {
        let mut ctx = Context::new();
        ctx.plugin(LoggerPlugin).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();
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
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct P(Arc<std::sync::Mutex<Vec<usize>>>, u8);

    #[async_trait]
        impl Plugin for P {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(self.1 as usize);
                Ok(())
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(10 + self.1 as usize);
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(P(observed.clone(), 1)).unwrap();
        ctx.plugin(P(observed.clone(), 2)).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&observed[..2], &[1, 2]);
        assert_eq!(&observed[2..], &[12, 11]);
    }

    #[test]
    fn dependency_check_runs_before_start() {
        #[derive(Debug)]
        struct Missing;

        struct NeedsMissing;

    #[async_trait]
        impl Plugin for NeedsMissing {
            fn dependencies(&self) -> &'static [Dependency] {
                static DEPENDENCY: std::sync::OnceLock<Dependency> = std::sync::OnceLock::new();
                let dependency = DEPENDENCY.get_or_init(Dependency::of::<Missing>);
                std::slice::from_ref(dependency)
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(NeedsMissing).unwrap();

        let err = block_on(ctx.start()).unwrap_err();
        assert!(matches!(
            err,
            Error::ServiceNotFound(name) if name.contains("Missing")
        ));

        ctx.provide(Missing).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn plugin_apply_failure_rolls_back_partial_side_effects() {
        struct BadPlugin;

    #[async_trait]
        impl Plugin for BadPlugin {
            fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
                ctx.provide(42_u32)?;
                ctx.on_ready(SyncHook(|_: &mut Context| Ok(())))?;
                Err(Error::PluginApply("boom".to_string()))
            }
        }

        let mut ctx = Context::new();
        assert!(ctx.plugin(BadPlugin).is_err());
        assert!(!ctx.contains::<u32>());
    }

    #[test]
    fn stop_continues_and_dispose_hook_always_runs() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct OkPlugin(Arc<std::sync::Mutex<Vec<&'static str>>>);

    #[async_trait]
        impl Plugin for OkPlugin {
            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("ok_stop");
                Ok(())
            }
        }

        struct BadPlugin(Arc<std::sync::Mutex<Vec<&'static str>>>);

    #[async_trait]
        impl Plugin for BadPlugin {
            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("bad_stop");
                Err(Error::PluginStop("boom".to_string()))
            }
        }

        let mut ctx = Context::new();
        ctx.on_dispose(SyncHook(|ctx: &mut Context| {
            let observed = ctx.require::<Arc<std::sync::Mutex<Vec<&'static str>>>>()?;
            observed.lock().unwrap().push("dispose");
            Ok(())
        }))
        .unwrap();
        ctx.provide(observed.clone()).unwrap();

        ctx.plugin(OkPlugin(observed.clone())).unwrap();
        ctx.plugin(BadPlugin(observed.clone())).unwrap();

        assert!(matches!(block_on(ctx.stop()), Err(Error::Multiple(_))));

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["bad_stop", "ok_stop", "dispose"]);
    }

    #[test]
    fn repeated_start_stop_are_noop() {
        let start_count = Arc::new(AtomicUsize::new(0));
        let stop_count = Arc::new(AtomicUsize::new(0));
        let ready_count = Arc::new(AtomicUsize::new(0));
        let dispose_count = Arc::new(AtomicUsize::new(0));

        struct OncePlugin(Arc<AtomicUsize>, Arc<AtomicUsize>);

    #[async_trait]
        impl Plugin for OncePlugin {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(OncePlugin(start_count.clone(), stop_count.clone()))
            .unwrap();

        let ready_counter = ready_count.clone();
        ctx.on_ready(SyncHook(move |_: &mut Context| {
            ready_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
        .unwrap();
        let dispose_counter = dispose_count.clone();
        ctx.on_dispose(SyncHook(move |_: &mut Context| {
            dispose_counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
        .unwrap();

        block_on(ctx.start()).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();
        block_on(ctx.stop()).unwrap();

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

    #[async_trait]
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
        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();
    }

    #[test]
    fn scope_service_isolation_and_shadowing() {
        struct ParentService;
        struct ChildService;

        let mut ctx = Context::new();
        ctx.provide(ParentService).unwrap();

        let mut scope = ctx.scope();
        scope.provide(ChildService).unwrap();

        assert!(scope.contains::<ParentService>());
        assert!(scope.contains::<ChildService>());
        assert!(!ctx.contains::<ChildService>());
        assert!(ctx.contains::<ParentService>());
    }

    #[test]
    fn scope_dependency_check_sees_parent_service() {
        struct ParentService;
        struct NeedsParent;

    #[async_trait]
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

        scope.verify_dependencies().unwrap();
        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();
    }

    #[test]
    fn scope_blocks_parent_mutation_until_dropped() {
        struct ParentService;
        struct ChildService;
        struct DummyPlugin;

    #[async_trait]
        impl Plugin for DummyPlugin {}

        let mut ctx = Context::new();
        ctx.provide(ParentService).unwrap();

        let mut scope = ctx.scope();
        scope.provide(ChildService).unwrap();

        assert!(matches!(
            ctx.provide(ChildService),
            Err(Error::ContextShared)
        ));
        assert!(matches!(ctx.plugin(DummyPlugin), Err(Error::ContextShared)));
        assert!(matches!(
            block_on(ctx.stop()),
            Err(Error::ContextShared)
        ));

        drop(scope);
        ctx.provide(ChildService).unwrap();
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn explicit_context_typing_does_not_break_scope() {
        let ctx: Context = Context::new();
        let _scope = ctx.scope();
    }

    #[test]
    fn nested_scope_inherits_and_shadows() {
        #[derive(Debug, PartialEq)]
        struct RootService(&'static str);
        struct SessionService;
        struct AnotherSessionService;
        struct SubflowService;

        let mut ctx = Context::new();
        ctx.provide(RootService("root")).unwrap();

        let mut session = ctx.scope();
        session.provide(RootService("session")).unwrap();
        session.provide(SessionService).unwrap();

        let mut subflow = session.scope();
        subflow.provide(SubflowService).unwrap();

        assert!(subflow.contains::<RootService>());
        assert!(subflow.contains::<SessionService>());
        assert!(subflow.contains::<SubflowService>());

        assert_eq!(subflow.require::<RootService>().unwrap().0, "session");
        assert_eq!(session.require::<RootService>().unwrap().0, "session");
        assert_eq!(ctx.require::<RootService>().unwrap().0, "root");

        assert!(!session.contains::<SubflowService>());
        assert!(!ctx.contains::<SubflowService>());

        assert!(matches!(
            session.provide(SessionService),
            Err(Error::ContextShared)
        ));
        assert!(matches!(
            ctx.provide(SessionService),
            Err(Error::ContextShared)
        ));

        drop(subflow);

        session.provide(AnotherSessionService).unwrap();
        block_on(session.stop()).unwrap();
        drop(session);
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn scope_stop_does_not_clear_local_services() {
        struct LocalService;

        let ctx = Context::new();
        let mut scope = ctx.scope();
        scope.provide(LocalService).unwrap();

        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();

        assert!(scope.contains::<LocalService>());
        assert!(scope.require::<LocalService>().is_ok());
    }

    #[test]
    fn multiple_scopes_block_parent_mutation_until_all_dropped() {
        struct Service;

        let mut ctx = Context::new();
        let first = ctx.scope();
        let second = ctx.scope();

        assert!(matches!(ctx.provide(Service), Err(Error::ContextShared)));

        drop(first);
        assert!(matches!(ctx.provide(Service), Err(Error::ContextShared)));

        drop(second);
        ctx.provide(Service).unwrap();
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn scope_start_failure_cleanup_via_stop() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ready_count = Arc::new(AtomicUsize::new(0));

        struct FailingPlugin(Arc<std::sync::Mutex<Vec<&'static str>>>, Arc<AtomicUsize>);

    #[async_trait]
        impl Plugin for FailingPlugin {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Err(Error::PluginStart("boom".to_string()))
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("stop");
                Ok(())
            }
        }

        let ctx = Context::new();
        let mut scope = ctx.scope();
        scope.provide(observed.clone()).unwrap();
        scope.provide(ready_count.clone()).unwrap();
        let ready_counter = ready_count.clone();
        scope
            .on_ready(SyncHook(move |_: &mut Context| {
                ready_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        scope
            .plugin(FailingPlugin(observed.clone(), ready_count.clone()))
            .unwrap();

        assert!(matches!(
            block_on(scope.start()),
            Err(Error::PluginStart(_))
        ));
        assert_eq!(ready_count.load(Ordering::SeqCst), 1);

        block_on(scope.stop()).unwrap();
        assert_eq!(*observed.lock().unwrap(), vec!["stop"]);
    }

    #[test]
    fn ready_hook_failure_is_fail_fast() {
        let run_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

    #[async_trait]
        impl Plugin for Dummy {}

        let ctx = Context::new();
        let mut scope = ctx.scope();
        let counter = run_count.clone();
        scope
            .on_ready(SyncHook(move |_: &mut Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        let counter = run_count.clone();
        scope
            .on_ready(SyncHook(move |_: &mut Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::PluginApply("ready failure".to_string()))
            }))
            .unwrap();
        let counter = run_count.clone();
        scope
            .on_ready(SyncHook(move |_: &mut Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        scope.plugin(Dummy).unwrap();
        assert!(matches!(
            block_on(scope.start()),
            Err(Error::PluginApply(_))
        ));
        assert_eq!(run_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dispose_hook_failure_continues_and_aggregates() {
        let run_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

    #[async_trait]
        impl Plugin for Dummy {}

        let ctx = Context::new();
        let mut scope = ctx.scope();
        let counter = run_count.clone();
        scope
            .on_dispose(SyncHook(move |_: &mut Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::PluginStop("dispose failure".to_string()))
            }))
            .unwrap();
        let counter = run_count.clone();
        scope
            .on_dispose(SyncHook(move |_: &mut Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        scope.plugin(Dummy).unwrap();
        block_on(scope.start()).unwrap();
        assert!(matches!(block_on(scope.stop()), Err(Error::Multiple(_))));
        assert_eq!(run_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn scope_on_ready_dispose_run_once() {
        let ready_count = Arc::new(AtomicUsize::new(0));
        let dispose_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

    #[async_trait]
        impl Plugin for Dummy {}

        let ctx = Context::new();
        let mut scope = ctx.scope();

        let ready_counter = ready_count.clone();
        scope
            .on_ready(SyncHook(move |_: &mut Context| {
                ready_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        let dispose_counter = dispose_count.clone();
        scope
            .on_dispose(SyncHook(move |_: &mut Context| {
                dispose_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        scope.plugin(Dummy).unwrap();

        block_on(scope.start()).unwrap();
        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();
        block_on(scope.stop()).unwrap();

        assert_eq!(ready_count.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn context_clone_blocks_mutable_operations() {
        struct Service;

        let mut ctx = Context::new();
        let shared = ctx.clone();

        assert!(matches!(ctx.provide(Service), Err(Error::ContextShared)));

        drop(shared);
        ctx.provide(Service).unwrap();
        block_on(ctx.stop()).unwrap();
    }
}
