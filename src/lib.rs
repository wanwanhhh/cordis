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
mod event;
mod plugin;
mod service;

pub use context::{AsyncHook, Context, LifecycleHook, Scope, SyncHook};
pub use error::Error;
pub use event::{
    AsyncFnEventHandler, Event, EventControl, EventHandler, FnEventHandler, Subscription,
};
pub use plugin::{Dependency, Plugin, PluginDependency};
pub use service::ServiceRegistry;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            fn name(&self) -> &'static str {
                if self.1 == 1 { "p1" } else { "p2" }
            }

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
        assert!(matches!(block_on(ctx.stop()), Err(Error::ContextShared)));

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

    #[test]
    fn async_hook_registration_works() {
        struct Dummy;

        impl Plugin for Dummy {}

        let ctx = Context::new();
        let mut scope = ctx.scope();

        scope
            .on_ready(AsyncHook(|ctx: Context| async move {
                let _ = ctx;
                Ok(())
            }))
            .unwrap();

        scope.plugin(Dummy).unwrap();

        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();
    }

    #[test]
    fn context_can_be_shared_across_threads() {
        use std::thread;

        struct Service(u32);

        let mut ctx = Context::new();
        ctx.provide(Service(42)).unwrap();
        block_on(ctx.start()).unwrap();

        let shared = ctx.clone();

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let ctx = shared.clone();
                thread::spawn(move || {
                    block_on(async move {
                        let service = ctx.require::<Service>()?;
                        assert_eq!(service.0, 42);
                        Ok::<(), Error>(())
                    })
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        drop(shared);
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn event_serial_emit_runs_in_order() {
        struct Ping(u32);

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let first = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |event: &Ping, _: &Context| {
            first.lock().unwrap().push(event.0);
            Ok(EventControl::Continue)
        }))
        .unwrap();

        let second = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |event: &Ping, _: &Context| {
            second.lock().unwrap().push(event.0 + 10);
            Ok(EventControl::Continue)
        }))
        .unwrap();

        block_on(ctx.emit(Ping(1))).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &[1, 11]);
    }

    #[test]
    fn event_scope_bubbles_to_parent() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
            root_handler.lock().unwrap().push("root");
            Ok(EventControl::Continue)
        }))
        .unwrap();

        let mut scope = ctx.scope();
        let child_handler = observed.clone();
        scope
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        block_on(scope.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child", "root"]);
    }

    #[test]
    fn event_bail_stops_bubbling() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
            root_handler.lock().unwrap().push("root");
            Ok(EventControl::Continue)
        }))
        .unwrap();

        let mut scope = ctx.scope();
        let child_handler = observed.clone();
        scope
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Bail)
            }))
            .unwrap();

        block_on(scope.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child"]);
    }

    #[test]
    fn event_off_unsubscribes() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let handler = observed.clone();
        let subscription = ctx
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                handler.lock().unwrap().push("called");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        block_on(ctx.emit(Ping)).unwrap();
        ctx.off(subscription).unwrap();
        block_on(ctx.emit(Ping)).unwrap();

        assert_eq!(*observed.lock().unwrap(), vec!["called"]);
    }

    #[test]
    fn event_parallel_runs_all_handlers() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let handler = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
            handler.lock().unwrap().push(1);
            Ok(EventControl::Continue)
        }))
        .unwrap();

        let handler = observed.clone();
        ctx.on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
            handler.lock().unwrap().push(2);
            Ok(EventControl::Continue)
        }))
        .unwrap();

        block_on(ctx.emit_parallel(Ping)).unwrap();

        let mut observed = observed.lock().unwrap();
        observed.sort_unstable();
        assert_eq!(&*observed, &[1, 2]);
    }

    #[test]
    fn event_on_and_off_blocked_after_clone() {
        struct Ping;

        let mut ctx = Context::new();
        let _shared = ctx.clone();

        assert!(matches!(
            ctx.on::<Ping, _>(FnEventHandler(|_: &Ping, _: &Context| {
                Ok(EventControl::Continue)
            })),
            Err(Error::ContextShared)
        ));

        assert!(matches!(
            ctx.off(Subscription {
                context_id: 0,
                handler_id: 0
            }),
            Err(Error::ContextShared)
        ));
    }

    #[test]
    fn event_off_does_not_leak_across_contexts() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        let root_sub = ctx
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                root_handler.lock().unwrap().push("root");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let mut child = ctx.scope();
        let child_handler = observed.clone();
        child
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        // 用 root 的订阅去 child 里取消，必须失败。
        assert!(matches!(
            child.off(root_sub),
            Err(Error::SubscriptionNotFound)
        ));

        block_on(child.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child", "root"]);
    }

    #[test]
    fn try_require_handles_missing_and_present() {
        #[derive(Debug)]
        struct Present;
        struct Missing;

        let mut ctx = Context::new();
        assert!(ctx.try_require::<Missing>().unwrap().is_none());

        ctx.provide(Present).unwrap();
        assert!(ctx.try_require::<Present>().unwrap().is_some());
    }

    #[test]
    fn collection_is_local_only() {
        struct Tool(&'static str);

        let mut ctx = Context::new();
        ctx.provide_collect(Tool("root")).unwrap();

        let mut scope = ctx.scope();
        scope.provide_collect(Tool("child")).unwrap();

        let root_all = ctx.require_all::<Tool>().unwrap();
        assert_eq!(root_all.len(), 1);
        assert_eq!(root_all[0].0, "root");

        let child_all = scope.require_all::<Tool>().unwrap();
        assert_eq!(child_all.len(), 1);
        assert_eq!(child_all[0].0, "child");
    }

    #[test]
    fn factory_is_lazy_and_retries_on_error() {
        struct Lazy(String);

        let mut ctx = Context::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = calls.clone();
        ctx.provide_factory(move || -> Result<Lazy, Error> {
            inside.fetch_add(1, Ordering::SeqCst);
            Err(Error::PluginStart("boom".to_string()))
        })
        .unwrap();

        assert!(ctx.require::<Lazy>().is_err());
        assert!(ctx.require::<Lazy>().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let mut ctx = Context::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = calls.clone();
        ctx.provide_factory(move || -> Result<Lazy, Error> {
            inside.fetch_add(1, Ordering::SeqCst);
            Ok(Lazy("ok".to_string()))
        })
        .unwrap();

        assert_eq!(ctx.require::<Lazy>().unwrap().0, "ok");
        assert_eq!(ctx.require::<Lazy>().unwrap().0, "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn factory_works_with_require_mut_and_contains() {
        let mut ctx = Context::new();
        ctx.provide_factory(|| Ok::<u32, Error>(0)).unwrap();

        assert!(ctx.contains::<u32>());
        *ctx.require_mut::<u32>().unwrap() += 1;
        assert_eq!(*ctx.require::<u32>().unwrap(), 1);
    }

    #[test]
    fn async_fn_event_handler_works() {
        struct Ping;

        let mut ctx = Context::new();
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        let handler = observed.clone();
        ctx.on::<Ping, _>(AsyncFnEventHandler(move |_: &Ping, _ctx: Context| {
            let handler = handler.clone();
            async move {
                handler.lock().unwrap().push(1);
                Ok(EventControl::Continue)
            }
        }))
        .unwrap();

        block_on(ctx.emit(Ping)).unwrap();
        assert_eq!(*observed.lock().unwrap(), vec![1]);
    }

    #[test]
    fn optional_dependency_missing_does_not_block_start() {
        struct Missing;

        struct OptionalPlugin;

        impl Plugin for OptionalPlugin {
            fn dependencies(&self) -> &'static [Dependency] {
                static DEPS: std::sync::OnceLock<[Dependency; 1]> = std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [Dependency::optional_of::<Missing>()]);
                &deps[..]
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(OptionalPlugin).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn priority_controls_start_and_stop_order() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct P(Arc<std::sync::Mutex<Vec<usize>>>, i32, usize);

        #[async_trait]
        impl Plugin for P {
            fn name(&self) -> &'static str {
                if self.2 == 1 { "p1" } else { "p2" }
            }

            fn priority(&self) -> i32 {
                self.1
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(self.2);
                Ok(())
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(10 + self.2);
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(P(observed.clone(), 0, 1)).unwrap();
        ctx.plugin(P(observed.clone(), 10, 2)).unwrap();

        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();

        let observed = observed.lock().unwrap();
        // start: high priority 2 first, then 1; stop reverse: 1 then 2
        assert_eq!(&*observed, &[2, 1, 11, 12]);
    }

    #[test]
    fn service_and_factory_cannot_coexist() {
        let mut ctx = Context::new();
        ctx.provide_factory(|| Ok::<u32, Error>(1)).unwrap();
        assert!(matches!(
            ctx.provide(2_u32),
            Err(Error::ServiceAlreadyRegistered(_))
        ));
    }

    #[test]
    fn plugin_dependency_missing_blocks_start() {
        struct NeedsMissing;

        impl Plugin for NeedsMissing {
            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> =
                    std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::of("missing")]);
                &deps[..]
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(NeedsMissing).unwrap();
        assert!(matches!(
            block_on(ctx.start()),
            Err(Error::PluginDependencyNotFound(_))
        ));
    }

    #[test]
    fn optional_plugin_dependency_missing_does_not_block() {
        struct OptionalPlugin;

        impl Plugin for OptionalPlugin {
            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> =
                    std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::optional_of("missing")]);
                &deps[..]
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(OptionalPlugin).unwrap();
        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();
    }

    #[test]
    fn plugin_dependency_topological_order() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));

        struct A(Arc<std::sync::Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for A {
            fn name(&self) -> &'static str {
                "a"
            }

            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> =
                    std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::of("b")]);
                &deps[..]
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("a-start");
                Ok(())
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("a-stop");
                Ok(())
            }
        }

        struct B(Arc<std::sync::Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for B {
            fn name(&self) -> &'static str {
                "b"
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("b-start");
                Ok(())
            }

            async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("b-stop");
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(A(observed.clone())).unwrap();
        ctx.plugin(B(observed.clone())).unwrap();

        block_on(ctx.start()).unwrap();
        block_on(ctx.stop()).unwrap();

        let observed = observed.lock().unwrap();
        // b 必须先于 a 启动；停车时逆序：a 先停，b 后停
        assert_eq!(&*observed, &["b-start", "a-start", "a-stop", "b-stop"]);
    }

    #[test]
    fn plugin_dependency_cycle_detected() {
        struct A;
        struct B;

        impl Plugin for A {
            fn name(&self) -> &'static str {
                "a"
            }

            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> =
                    std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::of("b")]);
                &deps[..]
            }
        }

        impl Plugin for B {
            fn name(&self) -> &'static str {
                "b"
            }

            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> =
                    std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::of("a")]);
                &deps[..]
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(A).unwrap();
        ctx.plugin(B).unwrap();
        assert!(matches!(
            block_on(ctx.start()),
            Err(Error::PluginDependencyCycle)
        ));
    }

    #[test]
    fn plugin_name_uniqueness_enforced() {
        struct First;
        struct Second;

        impl Plugin for First {
            fn name(&self) -> &'static str {
                "duplicate"
            }
        }

        impl Plugin for Second {
            fn name(&self) -> &'static str {
                "duplicate"
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(First).unwrap();
        assert!(matches!(
            ctx.plugin(Second),
            Err(Error::PluginNameAlreadyRegistered(_))
        ));
    }

    #[test]
    fn has_plugin_sees_parent() {
        struct RootPlugin;

        impl Plugin for RootPlugin {
            fn name(&self) -> &'static str {
                "root-plugin"
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(RootPlugin).unwrap();

        let scope = ctx.scope();
        assert!(scope.has_plugin("root-plugin"));
        assert!(!scope.has_plugin("missing"));
    }

    #[test]
    fn plugin_with_config_injects_and_rolls_back() {
        #[derive(Clone)]
        struct MyConfig;

        struct ConfigPlugin;

        impl Plugin for ConfigPlugin {
            fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
                let _ = ctx.require::<MyConfig>()?;
                Ok(())
            }
        }

        let mut ctx = Context::new();
        ctx.plugin_with_config(ConfigPlugin, MyConfig).unwrap();
        assert!(ctx.contains::<MyConfig>());

        struct BadPlugin;

        impl Plugin for BadPlugin {
            fn apply(&self, _ctx: &mut Context) -> Result<(), Error> {
                Err(Error::PluginApply("boom".to_string()))
            }
        }

        let mut ctx = Context::new();
        assert!(ctx.plugin_with_config(BadPlugin, MyConfig).is_err());
        assert!(!ctx.contains::<MyConfig>());
    }


    #[test]
    fn child_plugin_can_depend_on_parent_plugin() {
        struct RootPlugin;

        impl Plugin for RootPlugin {
            fn name(&self) -> &'static str {
                "root"
            }
        }

        struct ChildPlugin;

        impl Plugin for ChildPlugin {
            fn name(&self) -> &'static str {
                "child"
            }

            fn plugin_dependencies(&self) -> &'static [PluginDependency] {
                static DEPS: std::sync::OnceLock<[PluginDependency; 1]> = std::sync::OnceLock::new();
                let deps = DEPS.get_or_init(|| [PluginDependency::of("root")]);
                &deps[..]
            }
        }

        let mut ctx = Context::new();
        ctx.plugin(RootPlugin).unwrap();
        block_on(ctx.start()).unwrap();

        let mut scope = ctx.scope();
        scope.plugin(ChildPlugin).unwrap();
        block_on(scope.start()).unwrap();
        block_on(scope.stop()).unwrap();

        drop(scope);
        block_on(ctx.stop()).unwrap();
    }
}
