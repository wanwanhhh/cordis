//! Cordis 风格的插件化基础架构。
//!
//! 本 crate 是通用插件化基础架构，采用以下核心思想：
//!
//! - 一切插件化
//! - Builder / Context / Runtime 三段式生命周期
//! - 服务是插件之间的唯一契约
//! - 生命周期由框架管理
//! - 静态集成，不做热加载
//!
//! # Example
//!
//! ```rust
//! use cordis::{Builder, Configurator, Error, Plugin};
//!
//! struct MyService;
//!
//! struct MyPlugin;
//!
//! impl Plugin for MyPlugin {
//!     fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
//!         cfg.provide(MyService)?;
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<(), Error> {
//! let mut builder = Builder::new();
//! builder.plugin(MyPlugin)?;
//! let mut rt = builder.build()?;
//! let future = async {
//!     rt.start().await?;
//!     rt.stop().await?;
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

pub use context::{AsyncHook, Builder, Configurator, Context, LifecycleHook, Runtime, SyncHook};
pub use error::{Error, ErrorKind, Phase};
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
    use std::sync::Mutex;
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
        fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
            cfg.provide(Logger {
                name: "main".to_string(),
            })?;

            cfg.on_ready(SyncHook(|ctx: &Context| {
                let logger = ctx.require::<Logger>()?;
                assert_eq!(logger.name, "main");
                Ok(())
            }))?;

            Ok(())
        }
    }

    #[test]
    fn plugin_and_service_workflow() {
        let mut builder = Builder::new();
        builder.plugin(LoggerPlugin).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn duplicate_service_is_rejected() {
        let mut builder = Builder::new();
        builder.provide(42_u32).unwrap();
        let err = builder.provide(7_u32).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::ServiceAlreadyRegistered(_)));
    }

    #[test]
    fn closure_can_be_a_plugin() {
        let mut builder = Builder::new();
        builder
            .plugin(|cfg: &mut Configurator<'_>| {
                cfg.provide("hello".to_string())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(builder.require::<String>().unwrap(), "hello");
    }

    #[test]
    fn stop_runs_in_reverse_order() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct P(Arc<Mutex<Vec<usize>>>, u8);

        #[async_trait]
        impl Plugin for P {
            fn name(&self) -> &'static str {
                if self.1 == 1 { "p1" } else { "p2" }
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(self.1 as usize);
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(10 + self.1 as usize);
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin(P(observed.clone(), 1)).unwrap();
        builder.plugin(P(observed.clone(), 2)).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start_serial()).unwrap();
        block_on(rt.stop()).unwrap();

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
            fn dependencies(&self) -> Vec<Dependency> {
                vec![Dependency::of::<Missing>()]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(NeedsMissing).unwrap();

        let err = builder.verify().unwrap_err();
        assert!(matches!(err.kind, ErrorKind::ServiceNotFound(_)));

        builder.provide(Missing).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn plugin_apply_failure_rolls_back_partial_side_effects() {
        struct BadPlugin;

        #[async_trait]
        impl Plugin for BadPlugin {
            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                cfg.provide(42_u32)?;
                cfg.on_ready(SyncHook(|_: &Context| Ok(())))?;
                Err(Error::new(Phase::Apply, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        assert!(builder.plugin(BadPlugin).is_err());
        assert!(!builder.contains::<u32>());
    }

    #[test]
    fn plugin_apply_failure_rolls_back_event_handlers_and_subscription_ids() {
        struct Ping;
        struct BadPlugin(Arc<Mutex<usize>>);

        #[async_trait]
        impl Plugin for BadPlugin {
            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                let calls = self.0.clone();
                cfg.on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                    *calls.lock().unwrap() += 1;
                    Ok(EventControl::Continue)
                }))?;
                Err(Error::new(Phase::Apply, ErrorKind::Other))
            }
        }

        let observed = Arc::new(Mutex::new(0usize));
        let mut builder = Builder::new();
        assert!(builder.plugin(BadPlugin(observed.clone())).is_err());

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping)).unwrap();
        assert_eq!(*observed.lock().unwrap(), 0);

        let mut builder = Builder::new();
        let subscription = builder
            .on::<Ping, _>(FnEventHandler(|_: &Ping, _: &Context| {
                Ok(EventControl::Continue)
            }))
            .unwrap();
        assert_eq!(subscription.handler_id, 0);
    }

    #[test]
    fn plugin_apply_failure_rolls_back_event_off_side_effect() {
        struct Ping;
        struct BadPlugin {
            subscription: Subscription,
        }

        #[async_trait]
        impl Plugin for BadPlugin {
            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                cfg.off(self.subscription)?;
                Err(Error::new(Phase::Apply, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(0usize));

        let observed_handler = observed.clone();
        let subscription = builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                *observed_handler.lock().unwrap() += 1;
                Ok(EventControl::Continue)
            }))
            .unwrap();

        assert!(builder.plugin(BadPlugin { subscription }).is_err());

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping)).unwrap();
        assert_eq!(*observed.lock().unwrap(), 1);
    }

    #[test]
    fn stop_continues_and_dispose_hook_always_runs() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct OkPlugin(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for OkPlugin {
            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("ok_stop");
                Ok(())
            }
        }

        struct BadPlugin(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for BadPlugin {
            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("bad_stop");
                Err(Error::new(Phase::Stop, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        builder
            .on_dispose(SyncHook(|ctx: &Context| {
                let observed = ctx.require::<Arc<Mutex<Vec<&'static str>>>>()?;
                observed.lock().unwrap().push("dispose");
                Ok(())
            }))
            .unwrap();
        builder.provide(observed.clone()).unwrap();

        builder.plugin(OkPlugin(observed.clone())).unwrap();
        builder.plugin(BadPlugin(observed.clone())).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::Multiple(_),
                ..
            })
        ));

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

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder
            .plugin(OncePlugin(start_count.clone(), stop_count.clone()))
            .unwrap();

        let ready_counter = ready_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                ready_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        let dispose_counter = dispose_count.clone();
        builder
            .on_dispose(SyncHook(move |_: &Context| {
                dispose_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
        block_on(rt.stop()).unwrap();

        assert_eq!(start_count.load(Ordering::SeqCst), 1);
        assert_eq!(stop_count.load(Ordering::SeqCst), 1);
        assert_eq!(ready_count.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn start_after_stop_is_noop() {
        let start_count = Arc::new(AtomicUsize::new(0));
        let stop_count = Arc::new(AtomicUsize::new(0));

        struct Counter(Arc<AtomicUsize>, Arc<AtomicUsize>);

        #[async_trait]
        impl Plugin for Counter {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder
            .plugin(Counter(start_count.clone(), stop_count.clone()))
            .unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.stop()).unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.start()).unwrap();

        assert_eq!(start_count.load(Ordering::SeqCst), 0);
        assert_eq!(stop_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn stop_without_start_only_runs_dispose_hooks() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct NeverStarted(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for NeverStarted {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("start");
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("stop");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        let dispose_log = observed.clone();
        builder
            .on_dispose(SyncHook(move |_ctx: &Context| {
                dispose_log.lock().unwrap().push("dispose");
                Ok(())
            }))
            .unwrap();
        builder.plugin(NeverStarted(observed.clone())).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.stop()).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["dispose"]);
    }

    #[test]
    fn nested_plugin_apply_failure_preserves_inner_plugin_name() {
        struct InnerBad;

        impl Plugin for InnerBad {
            fn name(&self) -> &'static str {
                "inner-bad"
            }

            fn apply(&self, _cfg: &mut Configurator<'_>) -> Result<(), Error> {
                Err(Error::new(Phase::Apply, ErrorKind::Other))
            }
        }

        struct Outer;

        impl Plugin for Outer {
            fn name(&self) -> &'static str {
                "outer"
            }

            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                cfg.provide(1_u32)?;
                cfg.plugin(InnerBad)?;
                Ok(())
            }
        }

        let mut builder = Builder::new();
        let err = builder.plugin(Outer).unwrap_err();
        assert_eq!(err.plugin, Some("inner-bad"));
        // 外层副作用应回滚，内层错误名保留。
        assert!(!builder.contains::<u32>());
    }

    #[test]
    fn missing_service_reports_type_name() {
        let builder = Builder::new();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let err = ctx.require::<u32>().unwrap_err();
        assert!(matches!(err.kind, ErrorKind::ServiceNotFound(_)));
    }

    #[test]
    fn scope_plugin_can_access_parent_service() {
        struct ParentService(&'static str);

        struct ScopePlugin;

        #[async_trait]
        impl Plugin for ScopePlugin {
            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                let service = cfg.require::<ParentService>()?;
                assert_eq!(service.0, "parent");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.provide(ParentService("parent")).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let mut scope = ctx.scope().unwrap();
        scope.plugin(ScopePlugin).unwrap();
        let mut scope_rt = scope.build().unwrap();
        block_on(scope_rt.start()).unwrap();
        block_on(scope_rt.stop()).unwrap();
    }

    #[test]
    fn scope_service_isolation_and_shadowing() {
        struct ParentService;
        struct ChildService;

        let mut builder = Builder::new();
        builder.provide(ParentService).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let mut scope = ctx.scope().unwrap();
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
            fn dependencies(&self) -> Vec<Dependency> {
                vec![Dependency::of::<ParentService>()]
            }
        }

        let mut builder = Builder::new();
        builder.provide(ParentService).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let mut scope = ctx.scope().unwrap();
        scope.plugin(NeedsParent).unwrap();

        scope.verify().unwrap();
        let mut scope_rt = scope.build().unwrap();
        block_on(scope_rt.start()).unwrap();
        block_on(scope_rt.stop()).unwrap();
    }

    #[test]
    fn active_scope_blocks_parent_stop_until_dropped() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let ctx = rt.handle();

        let scope_builder = ctx.scope().unwrap();

        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1 },
                ..
            })
        ));

        drop(scope_builder);
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn active_child_runtime_blocks_parent_stop_until_dropped() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let ctx = rt.handle();

        let scope = ctx.scope().unwrap();
        let mut scope_rt = scope.build().unwrap();
        block_on(scope_rt.start()).unwrap();

        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1 },
                ..
            })
        ));

        block_on(scope_rt.stop()).unwrap();
        // Runtime 已 stop 但未 drop 仍占租约，按文档设计父 stop 仍被阻塞。
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1 },
                ..
            })
        ));

        drop(scope_rt);
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn scope_lease_release_happens_after_child_plugin_drop() {
        let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        struct ProbePlugin {
            log: Arc<Mutex<Vec<&'static str>>>,
            parent_ctx: Context,
        }

        impl Drop for ProbePlugin {
            fn drop(&mut self) {
                // 插件字段析构时，父 Data 的 child_count 必须仍然 > 0。
                // 如果 `_lease` 被重排到 `plugins` 之前，这里会观察到 count 已归零。
                if self.parent_ctx.child_count() == 0 {
                    self.log.lock().unwrap().push("lease-released-too-early");
                } else {
                    self.log
                        .lock()
                        .unwrap()
                        .push("child-plugin-dropped-while-lease-held");
                }
            }
        }

        #[async_trait]
        impl Plugin for ProbePlugin {}

        let mut root_builder = Builder::new();
        let root_log = log.clone();
        root_builder
            .on_dispose(SyncHook(move |_ctx: &Context| {
                root_log.lock().unwrap().push("parent-dispose");
                Ok(())
            }))
            .unwrap();

        let mut root_rt = root_builder.build().unwrap();
        let root_ctx = root_rt.handle();

        let mut child_builder = root_ctx.scope().unwrap();
        child_builder
            .plugin(ProbePlugin {
                log: log.clone(),
                parent_ctx: root_ctx.clone(),
            })
            .unwrap();
        let child_rt = child_builder.build().unwrap();

        drop(child_rt);
        block_on(root_rt.stop()).unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            &*log,
            &["child-plugin-dropped-while-lease-held", "parent-dispose"]
        );
    }

    #[test]
    fn nested_scope_inherits_and_shadows() {
        #[derive(Debug, PartialEq)]
        struct RootService(&'static str);
        struct SessionService;
        struct AnotherSessionService;
        struct SubflowService;

        let mut builder = Builder::new();
        builder.provide(RootService("root")).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let mut session_builder = ctx.scope().unwrap();
        session_builder.provide(RootService("session")).unwrap();
        session_builder.provide(SessionService).unwrap();

        let session_rt = session_builder.build().unwrap();
        let session_ctx = session_rt.handle();

        let mut subflow_builder = session_ctx.scope().unwrap();
        subflow_builder.provide(SubflowService).unwrap();

        assert!(subflow_builder.contains::<RootService>());
        assert!(subflow_builder.contains::<SessionService>());
        assert!(subflow_builder.contains::<SubflowService>());

        assert_eq!(
            subflow_builder.require::<RootService>().unwrap().0,
            "session"
        );
        assert_eq!(session_ctx.require::<RootService>().unwrap().0, "session");
        assert_eq!(ctx.require::<RootService>().unwrap().0, "root");

        assert!(!session_ctx.contains::<SubflowService>());
        assert!(!ctx.contains::<SubflowService>());

        drop(subflow_builder);
        let mut another_session_builder = ctx.scope().unwrap();
        another_session_builder
            .provide(AnotherSessionService)
            .unwrap();
        drop(another_session_builder);

        // 局部服务在 stop 后仍可从 Runtime 读句柄读到。
        let mut session_builder = ctx.scope().unwrap();
        session_builder.provide(SessionService).unwrap();
        let session_rt = session_builder.build().unwrap();
        let session_ctx = session_rt.handle();
        assert!(session_ctx.contains::<SessionService>());
    }

    #[test]
    fn multiple_scopes_block_parent_mutation_until_all_dropped() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let ctx = rt.handle();

        let first = ctx.scope().unwrap();
        let second = ctx.scope().unwrap();

        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 2 },
                ..
            })
        ));

        drop(first);
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1 },
                ..
            })
        ));

        drop(second);
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn scope_start_failure_cleanup_via_stop() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let ready_count = Arc::new(AtomicUsize::new(0));

        struct FailingPlugin(Arc<Mutex<Vec<&'static str>>>, Arc<AtomicUsize>);

        #[async_trait]
        impl Plugin for FailingPlugin {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Err(Error::new(Phase::Start, ErrorKind::Other))
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("stop");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.provide(observed.clone()).unwrap();
        builder.provide(ready_count.clone()).unwrap();
        let ready_counter = ready_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                ready_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        builder
            .plugin(FailingPlugin(observed.clone(), ready_count.clone()))
            .unwrap();

        let mut rt = builder.build().unwrap();
        assert!(block_on(rt.start()).is_err());
        assert_eq!(ready_count.load(Ordering::SeqCst), 1);

        block_on(rt.stop()).unwrap();
        assert_eq!(*observed.lock().unwrap(), vec!["stop"]);
    }

    #[test]
    fn ready_hook_failure_is_fail_fast() {
        let run_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        let counter = run_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();
        let counter = run_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::new(Phase::Ready, ErrorKind::Other))
            }))
            .unwrap();
        let counter = run_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        builder.plugin(Dummy).unwrap();
        let mut rt = builder.build().unwrap();
        assert!(block_on(rt.start()).is_err());
        assert_eq!(run_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn dispose_hook_failure_continues_and_aggregates() {
        let run_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        let counter = run_count.clone();
        builder
            .on_dispose(SyncHook(move |_: &Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(Error::new(Phase::Dispose, ErrorKind::Other))
            }))
            .unwrap();
        let counter = run_count.clone();
        builder
            .on_dispose(SyncHook(move |_: &Context| {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        builder.plugin(Dummy).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::Multiple(_),
                ..
            })
        ));
        assert_eq!(run_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn scope_on_ready_dispose_run_once() {
        let ready_count = Arc::new(AtomicUsize::new(0));
        let dispose_count = Arc::new(AtomicUsize::new(0));

        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        let ready_counter = ready_count.clone();
        builder
            .on_ready(SyncHook(move |_: &Context| {
                ready_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        let dispose_counter = dispose_count.clone();
        builder
            .on_dispose(SyncHook(move |_: &Context| {
                dispose_counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }))
            .unwrap();

        builder.plugin(Dummy).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
        block_on(rt.stop()).unwrap();

        assert_eq!(ready_count.load(Ordering::SeqCst), 1);
        assert_eq!(dispose_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn context_clone_does_not_block_stop() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let _shared = rt.handle();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn async_hook_registration_works() {
        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        builder
            .on_ready(AsyncHook(|ctx: Context| async move {
                let _ = ctx;
                Ok(())
            }))
            .unwrap();

        builder.plugin(Dummy).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn context_can_be_shared_across_threads() {
        use std::thread;

        struct Service(u32);

        let mut builder = Builder::new();
        builder.provide(Service(42)).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();

        let shared = rt.handle();

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
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn event_serial_emit_runs_in_order() {
        struct Ping(u32);

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let first = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |event: &Ping, _: &Context| {
                first.lock().unwrap().push(event.0);
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let second = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |event: &Ping, _: &Context| {
                second.lock().unwrap().push(event.0 + 10);
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping(1))).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &[1, 11]);
    }

    #[test]
    fn event_scope_bubbles_to_parent() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                root_handler.lock().unwrap().push("root");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut scope = ctx.scope().unwrap();
        let child_handler = observed.clone();
        scope
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let scope_rt = scope.build().unwrap();
        let child_ctx = scope_rt.handle();
        block_on(child_ctx.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child", "root"]);
    }

    #[test]
    fn event_handlers_receive_their_registration_scope_context() {
        struct Ping;

        let observed = Arc::new(Mutex::new(Vec::new()));

        let root_observed = observed.clone();
        let mut root_builder = Builder::new();
        root_builder.provide(1_u32).unwrap();
        root_builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, ctx: &Context| {
                root_observed
                    .lock()
                    .unwrap()
                    .push(*ctx.require::<u32>().unwrap());
                Ok(EventControl::Continue)
            }))
            .unwrap();
        let root_rt = root_builder.build().unwrap();
        let root = root_rt.handle();

        let mut child = root.scope().unwrap();
        child.provide(2_u32).unwrap();
        let child_observed = observed.clone();
        child
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, ctx: &Context| {
                child_observed
                    .lock()
                    .unwrap()
                    .push(*ctx.require::<u32>().unwrap());
                Ok(EventControl::Continue)
            }))
            .unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        block_on(child_ctx.emit(Ping)).unwrap();
        block_on(child_ctx.emit_parallel(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &[2, 1, 2, 1]);
    }

    #[test]
    fn event_bail_stops_bubbling() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                root_handler.lock().unwrap().push("root");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut scope = ctx.scope().unwrap();
        let child_handler = observed.clone();
        scope
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Bail)
            }))
            .unwrap();

        let scope_rt = scope.build().unwrap();
        let child_ctx = scope_rt.handle();
        block_on(child_ctx.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child"]);
    }

    #[test]
    fn event_off_unsubscribes() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let handler = observed.clone();
        let _subscription = builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                handler.lock().unwrap().push("called");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping)).unwrap();
        // off 只能在 Builder 阶段使用；这里再建一个 Builder 验证取消机制。
        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let handler = observed.clone();
        let subscription = builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                handler.lock().unwrap().push("called");
                Ok(EventControl::Continue)
            }))
            .unwrap();
        builder.off(subscription).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping)).unwrap();

        assert!(observed.lock().unwrap().is_empty());
    }

    #[test]
    fn event_parallel_runs_all_handlers() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let handler = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                handler.lock().unwrap().push(1);
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let handler = observed.clone();
        builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                handler.lock().unwrap().push(2);
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit_parallel(Ping)).unwrap();

        let mut observed = observed.lock().unwrap();
        observed.sort_unstable();
        assert_eq!(&*observed, &[1, 2]);
    }

    #[test]
    fn event_off_does_not_leak_across_contexts() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let root_handler = observed.clone();
        let root_sub = builder
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                root_handler.lock().unwrap().push("root");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        let child_handler = observed.clone();
        child
            .on::<Ping, _>(FnEventHandler(move |_: &Ping, _: &Context| {
                child_handler.lock().unwrap().push("child");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        assert!(matches!(
            child.off(root_sub),
            Err(Error {
                kind: ErrorKind::SubscriptionNotFound,
                ..
            })
        ));

        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();
        block_on(child_ctx.emit(Ping)).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["child", "root"]);
    }

    #[test]
    fn try_require_handles_missing_and_present() {
        #[derive(Debug)]
        struct Present;
        struct Missing;

        let mut builder = Builder::new();
        assert!(builder.try_require::<Missing>().unwrap().is_none());

        builder.provide(Present).unwrap();
        assert!(builder.try_require::<Present>().unwrap().is_some());
    }

    #[test]
    fn collection_is_local_only() {
        struct Tool(&'static str);

        let mut builder = Builder::new();
        builder.provide_collect(Tool("root")).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut scope = ctx.scope().unwrap();
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

        let mut builder = Builder::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = calls.clone();
        builder
            .provide_factory(move || -> Result<Lazy, Error> {
                inside.fetch_add(1, Ordering::SeqCst);
                Err(Error::new(Phase::Start, ErrorKind::Other))
            })
            .unwrap();

        assert!(builder.require::<Lazy>().is_err());
        assert!(builder.require::<Lazy>().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let mut builder = Builder::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = calls.clone();
        builder
            .provide_factory(move || -> Result<Lazy, Error> {
                inside.fetch_add(1, Ordering::SeqCst);
                Ok(Lazy("ok".to_string()))
            })
            .unwrap();

        assert_eq!(builder.require::<Lazy>().unwrap().0, "ok");
        assert_eq!(builder.require::<Lazy>().unwrap().0, "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn factory_works_with_require_mut_and_contains() {
        let mut builder = Builder::new();
        builder.provide_factory(|| Ok::<u32, Error>(0)).unwrap();

        assert!(builder.contains::<u32>());
        *builder.require_mut::<u32>().unwrap() += 1;
        assert_eq!(*builder.require::<u32>().unwrap(), 1);
    }

    #[test]
    fn async_fn_event_handler_works() {
        struct Ping;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let handler = observed.clone();
        builder
            .on::<Ping, _>(AsyncFnEventHandler(move |_: &Ping, _ctx: Context| {
                let handler = handler.clone();
                async move {
                    handler.lock().unwrap().push(1);
                    Ok(EventControl::Continue)
                }
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        block_on(ctx.emit(Ping)).unwrap();
        assert_eq!(*observed.lock().unwrap(), vec![1]);
    }

    #[test]
    fn optional_dependency_missing_does_not_block_start() {
        struct Missing;

        struct OptionalPlugin;

        impl Plugin for OptionalPlugin {
            fn dependencies(&self) -> Vec<Dependency> {
                vec![Dependency::optional_of::<Missing>()]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(OptionalPlugin).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn priority_controls_start_and_stop_order() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct P(Arc<Mutex<Vec<usize>>>, i32, usize);

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

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push(10 + self.2);
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin(P(observed.clone(), 0, 1)).unwrap();
        builder.plugin(P(observed.clone(), 10, 2)).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start_serial()).unwrap();
        block_on(rt.stop()).unwrap();

        let observed = observed.lock().unwrap();
        // start: high priority 2 first, then 1; stop reverse: 1 then 2
        assert_eq!(&*observed, &[2, 1, 11, 12]);
    }

    #[test]
    fn service_and_factory_cannot_coexist() {
        let mut builder = Builder::new();
        builder.provide_factory(|| Ok::<u32, Error>(1)).unwrap();
        assert!(matches!(
            builder.provide(2_u32),
            Err(Error {
                kind: ErrorKind::ServiceAlreadyRegistered(_),
                ..
            })
        ));
    }

    #[test]
    fn plugin_dependency_missing_blocks_start() {
        struct NeedsMissing;

        impl Plugin for NeedsMissing {
            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::of("missing")]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(NeedsMissing).unwrap();
        assert!(matches!(
            builder.verify(),
            Err(Error {
                kind: ErrorKind::PluginDependencyNotFound(_),
                ..
            })
        ));
    }

    #[test]
    fn optional_plugin_dependency_missing_does_not_block() {
        struct OptionalPlugin;

        impl Plugin for OptionalPlugin {
            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::optional_of("missing")]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(OptionalPlugin).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn optional_plugin_dependency_present_respects_order_even_in_parallel_start() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct A(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for A {
            fn name(&self) -> &'static str {
                "a"
            }

            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::optional_of("b")]
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("a-start");
                Ok(())
            }
        }

        struct B(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for B {
            fn name(&self) -> &'static str {
                "b"
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("b-start");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin(A(observed.clone())).unwrap();
        builder.plugin(B(observed.clone())).unwrap();

        let mut rt = builder.build().unwrap();
        // 即使使用默认并行路径，可选但存在的依赖仍然产生层间顺序。
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(&*observed, &["b-start", "a-start"]);
    }

    #[test]
    fn plugin_dependency_topological_order() {
        let observed = Arc::new(Mutex::new(Vec::new()));

        struct A(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for A {
            fn name(&self) -> &'static str {
                "a"
            }

            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::of("b")]
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("a-start");
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("a-stop");
                Ok(())
            }
        }

        struct B(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for B {
            fn name(&self) -> &'static str {
                "b"
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("b-start");
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("b-stop");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin(A(observed.clone())).unwrap();
        builder.plugin(B(observed.clone())).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start_serial()).unwrap();
        block_on(rt.stop()).unwrap();

        let observed = observed.lock().unwrap();
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

            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::of("b")]
            }
        }

        impl Plugin for B {
            fn name(&self) -> &'static str {
                "b"
            }

            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::of("a")]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(A).unwrap();
        builder.plugin(B).unwrap();
        assert!(matches!(
            builder.verify(),
            Err(Error {
                kind: ErrorKind::PluginDependencyCycle,
                ..
            })
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

        let mut builder = Builder::new();
        builder.plugin(First).unwrap();
        assert!(matches!(
            builder.plugin(Second),
            Err(Error {
                kind: ErrorKind::PluginNameAlreadyRegistered(_),
                ..
            })
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

        let mut builder = Builder::new();
        builder.plugin(RootPlugin).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let scope = ctx.scope().unwrap();
        assert!(scope.has_plugin("root-plugin"));
        assert!(!scope.has_plugin("missing"));
    }

    #[test]
    fn plugin_with_config_injects_and_rolls_back() {
        #[derive(Clone)]
        struct MyConfig;

        struct ConfigPlugin;

        impl Plugin for ConfigPlugin {
            fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
                let _ = cfg.require::<MyConfig>()?;
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin_with_config(ConfigPlugin, MyConfig).unwrap();
        assert!(builder.contains::<MyConfig>());

        struct BadPlugin;

        impl Plugin for BadPlugin {
            fn apply(&self, _cfg: &mut Configurator<'_>) -> Result<(), Error> {
                Err(Error::new(Phase::Apply, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        assert!(builder.plugin_with_config(BadPlugin, MyConfig).is_err());
        assert!(!builder.contains::<MyConfig>());
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

            fn plugin_dependencies(&self) -> Vec<PluginDependency> {
                vec![PluginDependency::of("root")]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(RootPlugin).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();

        let ctx = rt.handle();
        let mut scope = ctx.scope().unwrap();
        scope.plugin(ChildPlugin).unwrap();
        let mut scope_rt = scope.build().unwrap();
        block_on(scope_rt.start()).unwrap();
        block_on(scope_rt.stop()).unwrap();

        drop(scope_rt);
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn try_build_failure_returns_builder() {
        struct Missing;

        struct NeedsMissing;

        #[async_trait]
        impl Plugin for NeedsMissing {
            fn dependencies(&self) -> Vec<Dependency> {
                vec![Dependency::of::<Missing>()]
            }
        }

        let mut builder = Builder::new();
        builder.plugin(NeedsMissing).unwrap();
        let (builder, err) = match builder.try_build() {
            Ok(_) => panic!("try_build should fail"),
            Err(pair) => pair,
        };
        assert!(matches!(err.kind, ErrorKind::ServiceNotFound(_)));
        assert!(!builder.verify().is_ok());
    }

    #[test]
    fn stopping_scope_returns_stopping_error() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let ctx = rt.handle();

        block_on(rt.stop()).unwrap();
        assert!(matches!(
            ctx.scope(),
            Err(Error {
                kind: ErrorKind::Stopping,
                ..
            })
        ));
    }
}
