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

#[cfg(feature = "tokio")]
pub use context::TaskHandle;
pub use context::{
    AsyncHook, Builder, Configurator, Context, LifecycleHook, Runtime, StopHandle, SyncHook,
    TaskFailed,
};
pub use error::{Error, ErrorKind, Phase};
pub use event::{
    AsyncFnEventHandler, Event, EventControl, EventHandler, FnEventHandler, Subscription,
};
pub use plugin::{Dependency, Plugin, PluginDependency, PluginScope};
pub use service::{DynamicValue, ServiceRegistry};

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

        let err = block_on(rt.stop()).unwrap_err();
        match err.kind {
            ErrorKind::ActiveScopes { count: 1, ids } => {
                assert_eq!(ids, ctx.children());
                assert_eq!(ids.len(), 1);
            }
            other => panic!("unexpected error kind: {other:?}"),
        }

        drop(scope_builder);
        assert!(ctx.children().is_empty());
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
                kind: ErrorKind::ActiveScopes { count: 1, .. },
                ..
            })
        ));

        block_on(scope_rt.stop()).unwrap();
        // Runtime 已 stop 但未 drop 仍占租约，按文档设计父 stop 仍被阻塞。
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1, .. },
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
                kind: ErrorKind::ActiveScopes { count: 2, .. },
                ..
            })
        ));

        drop(first);
        assert!(matches!(
            block_on(rt.stop()),
            Err(Error {
                kind: ErrorKind::ActiveScopes { count: 1, .. },
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
    fn partial_start_failure_reclaims_every_entered_plugin() {
        let observed = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        struct OkPlugin(Arc<Mutex<Vec<&'static str>>>);
        struct BadPlugin(Arc<Mutex<Vec<&'static str>>>);

        #[async_trait]
        impl Plugin for OkPlugin {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("ok_start");
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("ok_stop");
                Ok(())
            }
        }

        #[async_trait]
        impl Plugin for BadPlugin {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("bad_start");
                Err(Error::new(Phase::Start, ErrorKind::Other))
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.lock().unwrap().push("bad_stop");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder.plugin(OkPlugin(observed.clone())).unwrap();
        builder.plugin(BadPlugin(observed.clone())).unwrap();

        let mut rt = builder.build().unwrap();
        assert!(block_on(rt.start_serial()).is_err());
        block_on(rt.stop()).unwrap();

        // 串行序为 [ok, bad]，失败插件也已被记入，故逆序回收两者。
        assert_eq!(
            &*observed.lock().unwrap(),
            &["ok_start", "bad_start", "bad_stop", "ok_stop"]
        );
    }

    #[test]
    fn start_failure_blocks_reentry_and_preserves_first_error() {
        let starts = Arc::new(AtomicUsize::new(0));

        struct Failing(Arc<AtomicUsize>);

        #[async_trait]
        impl Plugin for Failing {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(Error::new(Phase::Start, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        builder.plugin(Failing(starts.clone())).unwrap();
        let mut rt = builder.build().unwrap();

        // 首次失败按原样返回聚合错误，调用方不必先扒 source 链。
        let err = block_on(rt.start()).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::Multiple(_)));
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        // start_error() 暴露同一份聚合错误。
        let queried = rt.start_error().expect("失败后应可查询首错");
        assert!(matches!(queried.kind, ErrorKind::Multiple(_)));

        // 重入被明确拒绝，而不是伪装成成功；根因挂在 source 链上。
        let reentry = block_on(rt.start()).unwrap_err();
        assert!(matches!(reentry.kind, ErrorKind::StartFailed));
        let aggregate = reentry
            .source()
            .and_then(|source| source.downcast_ref::<Error>())
            .expect("重入错误应把首错挂在 source 链上");
        assert!(matches!(aggregate.kind, ErrorKind::Multiple(_)));
        assert_eq!(starts.load(Ordering::SeqCst), 1);

        // 失败态仍可 stop 回收；进入终态后 start 回到 no-op。
        block_on(rt.stop()).unwrap();
        block_on(rt.start()).unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropped_stop_future_resumes_instead_of_reporting_success() {
        use std::future::Future;
        use std::task::{Context as TaskContext, Poll};

        let stops = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let entered = Arc::new(AtomicUsize::new(0));

        struct SlowStop {
            log: Arc<Mutex<Vec<&'static str>>>,
            entered: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl Plugin for SlowStop {
            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                // 首次进入就挂起，模拟 stop future 在插件 await 中被丢弃。
                if self.entered.fetch_add(1, Ordering::SeqCst) == 0 {
                    futures::future::pending::<()>().await;
                }
                self.log.lock().unwrap().push("stopped");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder
            .plugin(SlowStop {
                log: stops.clone(),
                entered: entered.clone(),
            })
            .unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();

        // 手动 poll 一次 stop，让它停在插件的 await 上，然后整体丢弃。
        let waker = futures::task::noop_waker();
        let mut task_cx = TaskContext::from_waker(&waker);
        {
            let mut fut = Box::pin(rt.stop());
            assert!(matches!(fut.as_mut().poll(&mut task_cx), Poll::Pending));
        }

        // 状态停在 Stopping，插件 stop 尚未完成。
        assert!(rt.handle().is_stopping());
        assert!(stops.lock().unwrap().is_empty());

        // 重入续跑：游标未推进，该项被重试并完成，而不是被当作已停止。
        block_on(rt.stop()).unwrap();
        assert_eq!(&*stops.lock().unwrap(), &["stopped"]);
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn dropped_stop_future_keeps_inflight_task_tracked() {
        use futures::FutureExt;

        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        tokio_rt.block_on(async {
            let mut builder = Builder::new();
            builder.plugin(Dummy).unwrap();
            let mut rt = builder.build().unwrap();
            rt.start().await.unwrap();

            let ctx = rt.handle();
            ctx.spawn(async {
                futures::future::pending::<()>().await;
                Ok(())
            })
            .unwrap();

            // 首次 stop 停在任务排空的 await 上，然后被整体丢弃。
            assert!(rt.stop().now_or_never().is_none());
            assert!(ctx.is_stopping());

            // 重入时那个在飞任务必须仍被跟踪：给一个短预算，它应被 abort 并计入
            // 聚合错误，而不是因为句柄已随上一次 future detach 而被当作「已排空」。
            let err = rt
                .stop_with_timeout(std::time::Duration::from_millis(10))
                .await
                .unwrap_err();
            let ErrorKind::Multiple(errors) = &err.kind else {
                panic!("expected aggregated error, got {:?}", err.kind);
            };
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                errors[0].kind,
                ErrorKind::TaskAborted { task_id: 0 }
            ));
        });
    }

    #[test]
    fn dropped_start_future_blocks_reentry_and_stays_reclaimable() {
        use std::future::Future;
        use std::task::{Context as TaskContext, Poll};

        let entered = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        struct SlowStart {
            entered: Arc<AtomicUsize>,
            stops: Arc<Mutex<Vec<&'static str>>>,
        }

        #[async_trait]
        impl Plugin for SlowStart {
            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                // 首次进入就挂起，模拟 start future 被丢弃。
                if self.entered.fetch_add(1, Ordering::SeqCst) == 0 {
                    futures::future::pending::<()>().await;
                }
                Ok(())
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.stops.lock().unwrap().push("stopped");
                Ok(())
            }
        }

        let mut builder = Builder::new();
        builder
            .plugin(SlowStart {
                entered: entered.clone(),
                stops: stops.clone(),
            })
            .unwrap();
        let mut rt = builder.build().unwrap();

        // 手动 poll 一次 start，让它停在插件的 await 上，然后整体丢弃。
        let waker = futures::task::noop_waker();
        let mut task_cx = TaskContext::from_waker(&waker);
        {
            let mut fut = Box::pin(rt.start());
            assert!(matches!(fut.as_mut().poll(&mut task_cx), Poll::Pending));
        }

        // 被中断的启动不得谎报成功：重入返回 StartFailed，且因没有记录到失败而不带 source。
        let err = block_on(rt.start()).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::StartFailed));
        assert!(err.source().is_none());
        assert!(rt.start_error().is_none());
        assert_eq!(entered.load(Ordering::SeqCst), 1);

        // 已进入启动流程的插件仍可被 stop 回收。
        block_on(rt.stop()).unwrap();
        assert_eq!(&*stops.lock().unwrap(), &["stopped"]);
    }

    #[test]
    fn dropped_stop_future_resumes_dispose_and_keeps_earlier_errors() {
        use std::future::Future;
        use std::task::{Context as TaskContext, Poll};

        let dispose_runs = Arc::new(AtomicUsize::new(0));

        struct BadStop;

        #[async_trait]
        impl Plugin for BadStop {
            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                Err(Error::new(Phase::Stop, ErrorKind::Other))
            }
        }

        let mut builder = Builder::new();
        builder.plugin(BadStop).unwrap();

        // dispose 首次调用挂起，好让 stop 停在 dispose 段的中途。
        let counter = dispose_runs.clone();
        builder
            .on_dispose(AsyncHook(move |_ctx: Context| {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        futures::future::pending::<()>().await;
                    }
                    Ok(())
                }
            }))
            .unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();

        // 首次 stop：插件 stop 已经报错（记入累积错误），随后停在 dispose 的 await 上。
        let waker = futures::task::noop_waker();
        let mut task_cx = TaskContext::from_waker(&waker);
        {
            let mut fut = Box::pin(rt.stop());
            assert!(matches!(fut.as_mut().poll(&mut task_cx), Poll::Pending));
        }

        // 重入：dispose 被重试并完成，而上一轮记录的插件错误必须仍在——不能被吞掉
        // （若累积错误退回局部 Vec，这里会得到 Ok）。
        let err = block_on(rt.stop()).unwrap_err();
        let ErrorKind::Multiple(errors) = &err.kind else {
            panic!("expected aggregated error, got {:?}", err.kind);
        };
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].phase, Phase::Stop));
        assert!(matches!(errors[0].kind, ErrorKind::Other));
        assert_eq!(dispose_runs.load(Ordering::SeqCst), 2);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "dropped without stop")]
    fn dropping_running_runtime_without_stop_is_caught() {
        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        builder.plugin(Dummy).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        // 故意不 stop：Drop 护栏应在 debug 构建下硬失败。
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
        // 启动失败后仍处于待清理状态，必须显式 stop 回收；
        // 否则 Drop 护栏会（正确地）报出生命周期未归还。
        block_on(rt.stop()).unwrap();
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
    fn context_clone_does_not_block_stop() {
        let builder = Builder::new();
        let mut rt = builder.build().unwrap();
        let _shared = rt.handle();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
    }

    #[test]
    fn async_hook_registration_works() {
        let runs = Arc::new(AtomicUsize::new(0));

        struct Dummy;

        #[async_trait]
        impl Plugin for Dummy {}

        let mut builder = Builder::new();
        let counter = runs.clone();
        builder
            .on_ready(AsyncHook(move |ctx: Context| {
                let counter = counter.clone();
                async move {
                    let _ = ctx;
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }))
            .unwrap();

        builder.plugin(Dummy).unwrap();

        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();

        // AsyncHook 必须真的被执行，而不是只注册成功。
        assert_eq!(runs.load(Ordering::SeqCst), 1);
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

        // off 只能在 Builder 阶段使用。
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
        let (mut builder, err) = match builder.try_build() {
            Ok(_) => panic!("try_build should fail"),
            Err(pair) => pair,
        };
        assert!(matches!(err.kind, ErrorKind::ServiceNotFound(_)));
        builder.verify().unwrap_err();

        // try_build 的契约：校验失败把 Builder 完整带回；补齐依赖后应能照常构建运行。
        builder.provide(Missing).unwrap();
        let mut rt = builder.build().unwrap();
        block_on(rt.start()).unwrap();
        block_on(rt.stop()).unwrap();
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

    #[test]
    fn plugin_scope_enforces_root_and_child_restrictions() {
        struct RootOnly;

        impl Plugin for RootOnly {
            fn name(&self) -> &'static str {
                "root-only"
            }

            fn scope(&self) -> PluginScope {
                PluginScope::Root
            }
        }

        struct ChildOnly;

        impl Plugin for ChildOnly {
            fn name(&self) -> &'static str {
                "child-only"
            }

            fn scope(&self) -> PluginScope {
                PluginScope::Child
            }
        }

        let mut builder = Builder::new();
        builder.plugin(RootOnly).unwrap();

        let err = builder.plugin(ChildOnly).unwrap_err();
        assert!(matches!(
            err.kind,
            ErrorKind::PluginScopeMismatch {
                plugin_name,
                expected: PluginScope::Child,
                actual: PluginScope::Root,
            } if plugin_name == "child-only"
        ));

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        child.plugin(ChildOnly).unwrap();

        let err = child.plugin(RootOnly).unwrap_err();
        assert!(matches!(
            err.kind,
            ErrorKind::PluginScopeMismatch {
                plugin_name,
                expected: PluginScope::Root,
                actual: PluginScope::Child,
            } if plugin_name == "root-only"
        ));
    }

    #[test]
    fn require_all_recursive_inherits_parent_collections() {
        struct Tool(&'static str);

        let mut builder = Builder::new();
        builder.provide_collect(Tool("root")).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let mut child = ctx.scope().unwrap();
        child.provide_collect(Tool("child-1")).unwrap();
        child.provide_collect(Tool("child-2")).unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let local = child_ctx.require_all::<Tool>().unwrap();
        assert_eq!(local.len(), 2);
        assert_eq!(local[0].0, "child-1");
        assert_eq!(local[1].0, "child-2");

        let recursive = child_ctx.require_all_recursive::<Tool>().unwrap();
        let names: Vec<&str> = recursive.iter().map(|tool| tool.0).collect();
        assert_eq!(names, ["child-1", "child-2", "root"]);

        drop(child_rt);
        drop(rt);
    }

    #[test]
    fn emit_notify_continues_after_handler_errors() {
        struct Notify;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let root_seen = observed.clone();
        builder
            .on::<Notify, _>(FnEventHandler(move |_: &Notify, _: &Context| {
                root_seen.lock().unwrap().push("root");
                Err(Error::new(Phase::Event, ErrorKind::Other))
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        let child_seen = observed.clone();
        child
            .on::<Notify, _>(FnEventHandler(move |_: &Notify, _: &Context| {
                child_seen.lock().unwrap().push("child");
                Err(Error::new(Phase::Event, ErrorKind::Other))
            }))
            .unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let errors = block_on(child_ctx.emit_notify(Notify));
        assert_eq!(errors.len(), 2);

        let mut seen = observed.lock().unwrap();
        seen.sort_unstable();
        assert_eq!(&*seen, &["child", "root"]);
    }

    #[test]
    fn emit_notify_bail_stops_bubbling() {
        struct NotifyBail;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let root_seen = observed.clone();
        builder
            .on::<NotifyBail, _>(FnEventHandler(move |_: &NotifyBail, _: &Context| {
                root_seen.lock().unwrap().push("root");
                Ok(EventControl::Continue)
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        let child_seen = observed.clone();
        child
            .on::<NotifyBail, _>(FnEventHandler(move |_: &NotifyBail, _: &Context| {
                child_seen.lock().unwrap().push("child");
                Ok(EventControl::Bail)
            }))
            .unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let errors = block_on(child_ctx.emit_notify(NotifyBail));
        assert!(errors.is_empty());
        let seen = observed.lock().unwrap();
        assert_eq!(&*seen, &["child"]);
    }

    #[test]
    fn emit_notify_parallel_continues_after_handler_errors() {
        struct NotifyParallel;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let root_seen = observed.clone();
        builder
            .on::<NotifyParallel, _>(FnEventHandler(move |_: &NotifyParallel, _: &Context| {
                root_seen.lock().unwrap().push("root");
                Err(Error::new(Phase::Event, ErrorKind::Other))
            }))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        let child_seen = observed.clone();
        child
            .on::<NotifyParallel, _>(FnEventHandler(move |_: &NotifyParallel, _: &Context| {
                child_seen.lock().unwrap().push("child");
                Err(Error::new(Phase::Event, ErrorKind::Other))
            }))
            .unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let errors = block_on(child_ctx.emit_notify_parallel(NotifyParallel));
        assert_eq!(errors.len(), 2);

        let mut seen = observed.lock().unwrap();
        seen.sort_unstable();
        assert_eq!(&*seen, &["child", "root"]);
    }

    #[test]
    fn emit_notify_parallel_bail_stops_bubbling() {
        struct NotifyParallelBail;

        let mut builder = Builder::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let root_seen = observed.clone();
        builder
            .on::<NotifyParallelBail, _>(FnEventHandler(
                move |_: &NotifyParallelBail, _: &Context| {
                    root_seen.lock().unwrap().push("root");
                    Ok(EventControl::Continue)
                },
            ))
            .unwrap();

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut child = ctx.scope().unwrap();
        let child_seen = observed.clone();
        child
            .on::<NotifyParallelBail, _>(FnEventHandler(
                move |_: &NotifyParallelBail, _: &Context| {
                    child_seen.lock().unwrap().push("child");
                    Ok(EventControl::Bail)
                },
            ))
            .unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let errors = block_on(child_ctx.emit_notify_parallel(NotifyParallelBail));
        assert!(errors.is_empty());
        let seen = observed.lock().unwrap();
        assert_eq!(&*seen, &["child"]);
    }

    #[test]
    fn builder_depth_and_is_root_for_nested_scopes() {
        let root = Builder::new();
        assert_eq!(root.depth(), 0);
        assert!(root.is_root());

        let rt = root.build().unwrap();
        let ctx = rt.handle();
        let child = ctx.scope().unwrap();
        assert_eq!(child.depth(), 1);
        assert!(!child.is_root());

        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();
        let grandchild = child_ctx.scope().unwrap();
        assert_eq!(grandchild.depth(), 2);
        assert!(!grandchild.is_root());
    }

    #[test]
    fn dynamic_value_can_be_updated_at_runtime() {
        let mut builder = Builder::new();
        builder.provide_dynamic(1_u32).unwrap();

        // 动态配置不占用原 T 的类型槽位。
        assert!(!builder.contains::<u32>());
        assert!(builder.contains::<Arc<DynamicValue<u32>>>());

        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let dynamic = ctx.require_dynamic::<u32>().unwrap();
        assert_eq!(*dynamic.read(), 1);

        dynamic.set(2);
        assert_eq!(*dynamic.read(), 2);

        dynamic.update(|value| *value += 3);
        assert_eq!(*dynamic.read(), 5);
    }

    #[test]
    fn dynamic_value_is_inherited_by_child_scope() {
        let mut builder = Builder::new();
        builder.provide_dynamic("boot".to_string()).unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();

        let child = ctx.scope().unwrap();
        let child_rt = child.build().unwrap();
        let child_ctx = child_rt.handle();

        let dynamic = child_ctx.require_dynamic::<String>().unwrap();
        assert_eq!(*dynamic.read(), "boot");

        dynamic.set("child-updated".to_string());
        assert_eq!(
            *ctx.require_dynamic::<String>().unwrap().read(),
            "child-updated"
        );
    }

    struct RollbackTool(&'static str);

    struct FailingCollect;

    impl Plugin for FailingCollect {
        fn name(&self) -> &'static str {
            "failing-collect"
        }

        fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
            cfg.provide_collect(RollbackTool("bad"))?;
            Err(Error::new(Phase::Apply, ErrorKind::Other))
        }
    }

    #[test]
    fn contains_sees_ancestor_collection() {
        let mut builder = Builder::new();
        builder.provide_collect(RollbackTool("root")).unwrap();
        let rt = builder.build().unwrap();
        let scope = rt.handle().scope().unwrap();
        assert!(scope.contains::<RollbackTool>());
        assert!(rt.handle().contains::<RollbackTool>());
    }

    #[test]
    fn failed_plugin_rolls_back_collection_elements() {
        let mut builder = Builder::new();
        builder.provide_collect(RollbackTool("root")).unwrap();
        let err = builder.plugin(FailingCollect).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::Other));
        let all = builder.require_all::<RollbackTool>().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "root");
    }

    struct ConcurrentLazy(u64);

    #[test]
    fn concurrent_factory_runs_at_most_once_on_success() {
        let mut builder = Builder::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let inside = calls.clone();
        builder
            .provide_factory(move || -> Result<ConcurrentLazy, Error> {
                inside.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(20));
                Ok(ConcurrentLazy(42))
            })
            .unwrap();
        let rt = builder.build().unwrap();
        let ctx = rt.handle();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = ctx.clone();
            handles.push(std::thread::spawn(move || {
                c.require::<ConcurrentLazy>().unwrap().0
            }));
        }
        for handle in handles {
            assert_eq!(handle.join().unwrap(), 42);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stopped_child_scope_is_observable() {
        let rt = Builder::new().build().unwrap();
        let ctx = rt.handle();
        let scope = ctx.scope().unwrap();
        let mut scope_rt = scope.build().unwrap();
        let scope_ctx = scope_rt.handle();
        assert!(!scope_ctx.is_stopping());
        assert_eq!(ctx.children().len(), 1);
        assert!(ctx.parent().is_none());
        assert!(scope_ctx.parent().is_some());

        block_on(scope_rt.stop()).unwrap();
        assert!(scope_ctx.is_stopping());
        // 子 Runtime 停止后未 drop 前仍占租约，出现在父清单中。
        assert_eq!(ctx.children().len(), 1);

        drop(scope_ctx);
        drop(scope_rt);
        assert!(ctx.children().is_empty());
    }

    #[cfg(feature = "tokio")]
    fn tokio_block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn stop_waits_for_spawned_tasks() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
            ctx.spawn(async move {
                let _ = release_rx.await;
                Ok::<(), Error>(())
            })
            .unwrap();
            assert_eq!(ctx.task_count(), 1);

            // 关键断言：手动 poll 一次 `stop()` 必须停在「等在飞任务」的 await 上。
            // 若排空不等待（取出即返回），这里已经 Ready，本测试变红——靠
            // `join!` + 时间差做不到这一点，那只能证明「注册表不再报告它」。
            let mut stopper = std::pin::pin!(rt.stop());
            assert!(futures::poll!(stopper.as_mut()).is_pending());
            assert_eq!(ctx.task_count(), 1);

            let _ = release_tx.send(());
            stopper.await.unwrap();
            assert_eq!(ctx.task_count(), 0);
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn panicking_task_failed_handler_does_not_hang_stop() {
        // 上报路径（emit_notify → 用户 handler）在任务体之外、仍在这个任务里。它
        // panic 时 wrapper 会在写结局之前展开——若没有兜底，完成信号永不触发，
        // `wait()` 与 `stop()` 会一起挂死，超时排空还会把它误报成 TaskAborted。
        let mut builder = Builder::new();
        builder
            .on::<TaskFailed, _>(FnEventHandler(|_: &TaskFailed, _: &Context| {
                panic!("TaskFailed handler boom")
            }))
            .unwrap();
        tokio_block_on(async move {
            let mut rt = builder.build().unwrap();
            rt.start().await.unwrap();
            let task = rt
                .handle()
                .spawn(async { Err::<(), Error>(Error::new(Phase::Start, ErrorKind::Other)) })
                .unwrap();

            let waited = tokio::time::timeout(std::time::Duration::from_secs(5), task.wait())
                .await
                .expect("完成信号丢失：上报路径 panic 未被兜住");
            assert!(waited.is_err(), "结局应被记为 panic");
            assert!(task.is_finished());

            let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), rt.stop())
                .await
                .expect("stop 挂起：完成信号丢失")
                .unwrap_err();
            let ErrorKind::Multiple(errors) = stopped.kind else {
                panic!("expected aggregated error, got {:?}", stopped.kind);
            };
            assert!(matches!(
                errors[0].kind,
                ErrorKind::TaskFailed { task_id: 0 }
            ));
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn stop_with_timeout_aborts_and_reports() {
        let started = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = futures::channel::oneshot::channel::<()>();
        let task_started = started.clone();
        tokio_block_on(async move {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();
            ctx.spawn(async move {
                task_started.fetch_add(1, Ordering::SeqCst);
                let _ = rx.await;
                Ok::<(), Error>(())
            })
            .unwrap();
            // 先确认任务真的被调度过，否则慢机器上 50ms 预算可能先到、任务还没开始，
            // `started == 1` 会假红。这里等待的是「已开始」，不是「已完成」。
            for _ in 0..1000 {
                if started.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(started.load(Ordering::SeqCst), 1, "任务必须先真正开始");
            std::mem::forget(tx);
            let err = rt
                .stop_with_timeout(std::time::Duration::from_millis(50))
                .await
                .unwrap_err();
            match err.kind {
                ErrorKind::Multiple(errors) => {
                    assert_eq!(errors.len(), 1);
                    assert!(matches!(
                        errors[0].kind,
                        ErrorKind::TaskAborted { task_id: 0 }
                    ));
                }
                other => panic!("unexpected error kind: {other:?}"),
            }
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn dropping_task_handle_wait_future_unregisters_waiter() {
        // `TaskHandle::wait()` 会被反复丢弃（`select!` 里另一个分支先就绪、被
        // `timeout` 包裹等）。每次丢弃都必须摘掉自己的注册项，否则该 cell 的等待者
        // 列表会随这类尝试单调增长。
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();
            let task = ctx
                .spawn(async {
                    futures::future::pending::<()>().await;
                    Ok::<(), Error>(())
                })
                .unwrap();

            for _ in 0..3 {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_millis(1), task.wait()).await;
            }
            assert!(
                ctx.pending_task_waiters() <= 1,
                "被丢弃的 wait future 残留了注册项: {}",
                ctx.pending_task_waiters()
            );

            task.abort();
            assert!(task.wait().await.is_ok());
            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn stop_with_timeout_accepts_absurd_budget() {
        // `Instant::now() + Duration::MAX` 会溢出 panic；实现用 `checked_add` 把它
        // 降级为「不设超时」。这个用例钉住那条降级路径不被改回裸加法。
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            rt.handle().spawn(async { Ok::<(), Error>(()) }).unwrap();
            rt.stop_with_timeout(std::time::Duration::MAX)
                .await
                .unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn host_runtime_shutdown_does_not_fabricate_task_failure() {
        // 宿主 runtime 关闭会把在飞任务的 future 直接丢掉（非展开）。这不等于 panic：
        // 兜底守卫若一律记 `Panicked`，`stop()` 就会凭空多出一条 `TaskFailed`。
        let host = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (mut rt, ctx) = host.block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();
            ctx.spawn(async {
                futures::future::pending::<()>().await;
                Ok::<(), Error>(())
            })
            .unwrap();
            // 先让它真的被 poll 一次并 park（兜底守卫只在任务体开始执行后才存在）。
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            assert_eq!(ctx.task_count(), 1);
            (rt, ctx)
        });
        drop(host);

        tokio_block_on(async move {
            let stopped = tokio::time::timeout(std::time::Duration::from_secs(5), rt.stop())
                .await
                .expect("完成信号丢失：非展开丢弃未被兜住");
            assert!(
                stopped.is_ok(),
                "宿主 runtime 关闭被误报成任务失败: {stopped:?}"
            );
            assert_eq!(ctx.task_count(), 0);
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn spawn_rejected_after_stop() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            assert!(!ctx.is_stopping());
            rt.stop().await.unwrap();
            assert!(ctx.is_stopping());
            let err = ctx.spawn(async { Ok::<(), Error>(()) }).unwrap_err();
            assert!(matches!(err.kind, ErrorKind::Stopping));
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn spawn_without_runtime_context_fails() {
        let rt = Builder::new().build().unwrap();
        let ctx = rt.handle();
        let err = ctx.spawn(async { Ok::<(), Error>(()) }).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::NoTaskRuntime));
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn finished_tasks_are_pruned_from_registry() {
        tokio_block_on(async {
            let rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            ctx.spawn(async { Ok::<(), Error>(()) }).unwrap();
            ctx.spawn(async { Ok::<(), Error>(()) }).unwrap();
            for _ in 0..1000 {
                if ctx.task_count() == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            // `task_count` 会过滤已结束的，所以它证明不了「剪除」这件事——必须看
            // 注册表原长：此刻两个已结束的 cell 都还在表里。
            assert_eq!(ctx.registered_task_count(), 2);
            // 再 spawn 一次才触发剪除，表里只剩这个新任务。
            ctx.spawn(async { Ok::<(), Error>(()) }).unwrap();
            assert_eq!(ctx.registered_task_count(), 1);
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn pruning_keeps_unreported_panic_outcome() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let panicked = ctx.spawn(async { panic!("boom") }).unwrap();
            // 等 panic 结局落定：它只等排空上报，没有事件出口。
            assert!(panicked.wait().await.is_err());

            // 之后再 spawn 会触发剪除。未上报的 panic 结局必须被保住，否则 `stop`
            // 会因为它之后又有人 spawn 过而静默报成功。
            ctx.spawn(async { Ok::<(), Error>(()) }).unwrap();

            let err = rt.stop().await.unwrap_err();
            let ErrorKind::Multiple(errors) = err.kind else {
                panic!("expected aggregated error, got {:?}", err.kind);
            };
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                errors[0].kind,
                ErrorKind::TaskFailed { task_id: 0 }
            ));
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn task_handle_wait_returns_ok_on_success() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let task = rt.handle().spawn(async { Ok::<(), Error>(()) }).unwrap();
            assert!(task.wait().await.is_ok());
            assert!(task.is_finished());
            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn task_ids_match_failed_event() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let handler_observed = observed.clone();
        tokio_block_on(async move {
            let mut builder = Builder::new();
            builder
                .on::<TaskFailed, _>(FnEventHandler(move |event: &TaskFailed, _: &Context| {
                    handler_observed.lock().unwrap().push(event.task_id);
                    Ok(EventControl::Continue)
                }))
                .unwrap();
            let mut rt = builder.build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let ok = ctx.spawn(async { Ok::<(), Error>(()) }).unwrap();
            let failing = ctx
                .spawn(async { Err::<(), Error>(Error::new(Phase::Start, ErrorKind::Other)) })
                .unwrap();
            // id 在同一作用域内从 0 单调递增，不是恒为 0 的默认值。
            assert_eq!((ok.id(), failing.id()), (0, 1));

            let _ = failing.wait().await.unwrap_err();
            assert_eq!(*observed.lock().unwrap(), vec![failing.id()]);
            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn task_failure_emits_task_failed_event() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let handler_observed = observed.clone();
        tokio_block_on(async {
            let mut builder = Builder::new();
            builder
                .on::<TaskFailed, _>(FnEventHandler(move |event: &TaskFailed, _: &Context| {
                    handler_observed
                        .lock()
                        .unwrap()
                        .push((event.task_id, matches!(event.error.kind, ErrorKind::Other)));
                    Ok(EventControl::Continue)
                }))
                .unwrap();
            let mut rt = builder.build().unwrap();
            rt.start().await.unwrap();
            let scope = rt.handle().scope().unwrap();
            let mut scope_rt = scope.build().unwrap();
            scope_rt
                .handle()
                .spawn(async { Err::<(), Error>(Error::new(Phase::Start, ErrorKind::Other)) })
                .unwrap();
            for _ in 0..1000 {
                if !observed.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(*observed.lock().unwrap(), vec![(0, true)]);
            scope_rt.stop().await.unwrap();
            drop(scope_rt);
            rt.stop().await.unwrap();
        });
    }

    #[test]
    fn cancelled_is_level_triggered() {
        use futures::FutureExt;

        block_on(async {
            let mut rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            // 未停止：等待点保持挂起（不能立即就绪，否则等待形同虚设）。
            assert!(ctx.cancelled().now_or_never().is_none());
            rt.stop().await.unwrap();
            // 已停止：立即就绪。晚到的等待者不会永远挂起。
            assert!(ctx.cancelled().now_or_never().is_some());
            // 「请求」与「停止」是两件事，没人请求时请求标记保持 false。
            assert!(!rt.stop_handle().is_stop_requested());
        });
    }

    /// 手工 waker 探针：只关心「有没有被唤醒」与「唤醒时刻请求位是否已可见」，
    /// 因此不需要 tokio，也不受 `--no-default-features` 影响——这样 `StopHandle`
    /// 的契约在无 tokio 配置下也有真实覆盖。
    #[test]
    fn stop_handle_request_wakes_registered_waiter_and_keeps_scope_usable() {
        use futures::task::ArcWake;
        use std::task::Context as TaskContext;

        struct FlagWaker {
            woken: Arc<std::sync::atomic::AtomicBool>,
            /// 唤醒时刻读到的请求位。`request_stop` 必须先写请求位再广播，否则被
            /// 唤醒的等待者会看到「已取消但还没请求」的矛盾状态。
            request_visible: Arc<std::sync::atomic::AtomicBool>,
            handle: StopHandle,
        }

        impl ArcWake for FlagWaker {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.woken.store(true, Ordering::SeqCst);
                arc_self
                    .request_visible
                    .store(arc_self.handle.is_stop_requested(), Ordering::SeqCst);
            }
        }

        block_on(async {
            let rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            let handle = rt.stop_handle();

            let woken = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let request_visible = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let waker = futures::task::waker(Arc::new(FlagWaker {
                woken: woken.clone(),
                request_visible: request_visible.clone(),
                handle: handle.clone(),
            }));
            let mut cx = TaskContext::from_waker(&waker);

            let mut waiting = Box::pin(handle.cancelled());
            assert!(waiting.as_mut().poll(&mut cx).is_pending());
            assert!(!woken.load(Ordering::SeqCst));
            assert_eq!(ctx.cancellation_waiters(), 1);

            handle.clone().request_stop();
            handle.request_stop(); // 幂等
            assert!(handle.is_stop_requested());
            assert!(woken.load(Ordering::SeqCst), "请求必须唤醒已注册的等待者");
            assert!(
                request_visible.load(Ordering::SeqCst),
                "请求位必须在广播之前写入"
            );
            assert!(waiting.as_mut().poll(&mut cx).is_ready());
            assert_eq!(ctx.cancellation_waiters(), 0);

            // 请求不等于进入清理：`is_stopping` 仍为 false，`scope` 也不被拒绝。
            assert!(!ctx.is_stopping());
            drop(ctx.scope().unwrap());
            drop(waiting);

            // 未 start，Drop 护栏不会触发。
            drop(rt);
        });
    }

    #[test]
    fn cancelled_future_drop_unregisters_waiter() {
        block_on(async {
            let rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);

            let mut waiting = Box::pin(ctx.cancelled());
            assert!(waiting.as_mut().poll(&mut cx).is_pending());
            assert_eq!(ctx.cancellation_waiters(), 1);

            // 等待者主动放弃（典型：`select!` 里先等到别的事件）必须摘掉自己的
            // waker，否则长生命周期作用域的等待者列表会随这类任务单调增长。
            drop(waiting);
            assert_eq!(ctx.cancellation_waiters(), 0);
            drop(rt);
        });
    }

    #[test]
    fn dropping_one_cancelled_waiter_keeps_the_other_registered() {
        block_on(async {
            let rt = Builder::new().build().unwrap();
            let ctx = rt.handle();
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);

            // 同一任务/同一 waker 下的两个等待者。按 waker 相等去重会让他们共享一条
            // 注册项，drop 其中一个就把另一个的唤醒源一起摘掉——摘除必须按注册身份
            // （token）进行，谁注册谁负责摘自己。
            let mut first = Box::pin(ctx.cancelled());
            let mut second = Box::pin(ctx.cancelled());
            assert!(first.as_mut().poll(&mut cx).is_pending());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            assert_eq!(ctx.cancellation_waiters(), 2);

            drop(second);
            assert_eq!(ctx.cancellation_waiters(), 1, "只应摘掉被丢弃的那一个");

            rt.stop_handle().request_stop();
            assert!(
                first.as_mut().poll(&mut cx).is_ready(),
                "存活等待者的唤醒源被误摘了"
            );
            drop(first);
            drop(rt);
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn cancelled_wakes_waiter_across_worker_threads() {
        let observed = Arc::new(AtomicUsize::new(0));
        let task_observed = observed.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();

        rt.block_on(async move {
            let mut cordis_rt = Builder::new().build().unwrap();
            cordis_rt.start().await.unwrap();
            let ctx = cordis_rt.handle();

            let wait_ctx = ctx.clone();
            ctx.spawn(async move {
                wait_ctx.cancelled().await;
                task_observed.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Error>(())
            })
            .unwrap();

            // 注册 waker 与触发停止可能落在不同 worker 线程上——`Signal` 的
            // 「同锁置位/复查」正是为这种交错准备的。
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            cordis_rt.stop().await.unwrap();
        });

        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn cancelled_fires_before_task_drain() {
        let observed = Arc::new(AtomicUsize::new(0));
        let task_observed = observed.clone();
        tokio_block_on(async move {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            // 该任务只在收到取消信号后才结束。
            let wait_ctx = ctx.clone();
            ctx.spawn(async move {
                wait_ctx.cancelled().await;
                task_observed.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Error>(())
            })
            .unwrap();

            // 关键：必须等任务真的把 waker 注册进 `Signal` 再 stop。否则它会在 stop
            // 之后才被首次 poll，走 `poll_cancelled` 的状态快路径直接 Ready——那样
            // 这个测试就测不到「广播是否真的发出、是否早于排空」（把广播移到排空
            // 之后、或删掉唤醒循环，都仍然会绿）。
            for _ in 0..1000 {
                if ctx.cancellation_waiters() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(ctx.cancellation_waiters(), 1, "任务未注册取消等待者");

            tokio::time::timeout(std::time::Duration::from_secs(5), rt.stop())
                .await
                .expect("stop 未在 5s 内完成：广播晚于排空，或唤醒丢失")
                .unwrap();
        });
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn stop_handle_request_wakes_waiters_without_starting_cleanup() {
        let observed = Arc::new(AtomicUsize::new(0));
        let task_observed = observed.clone();
        tokio_block_on(async move {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();
            let handle = rt.stop_handle();
            assert!(!handle.is_stop_requested());

            let wait_ctx = ctx.clone();
            ctx.spawn(async move {
                wait_ctx.cancelled().await;
                task_observed.fetch_add(1, Ordering::SeqCst);
                Ok::<(), Error>(())
            })
            .unwrap();

            let cloned = handle.clone();
            cloned.request_stop();
            handle.request_stop(); // 幂等
            assert!(handle.is_stop_requested());

            // 请求不等于进入清理：拒绝新工作仍要等真正 `stop`。
            assert!(!ctx.is_stopping());
            assert!(ctx.spawn(async { Ok::<(), Error>(()) }).is_ok());

            for _ in 0..1000 {
                if observed.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(observed.load(Ordering::SeqCst), 1);

            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn owner_aborted_task_is_not_a_stop_error() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let task = ctx
                .spawn(async {
                    futures::future::pending::<()>().await;
                    Ok::<(), Error>(())
                })
                .unwrap();
            assert_eq!(task.id(), 0);
            assert!(!task.is_finished());

            task.abort();
            // 取消是请求，不是失败。
            assert!(task.wait().await.is_ok());
            assert!(task.is_finished());

            // 排空看到的是「owner 主动取消」，不得计入停止错误。
            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn task_handle_wait_returns_body_error() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let task = ctx
                .spawn(async { Err::<(), Error>(Error::new(Phase::Start, ErrorKind::Other)) })
                .unwrap();
            let err = task.wait().await.unwrap_err();
            assert!(matches!(err.kind, ErrorKind::Other));

            rt.stop().await.unwrap();
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn panicking_task_is_reported_at_drain() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            rt.handle().spawn(async { panic!("boom") }).unwrap();

            let err = rt.stop().await.unwrap_err();
            let ErrorKind::Multiple(errors) = err.kind else {
                panic!("expected aggregated error, got {:?}", err.kind);
            };
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                errors[0].kind,
                ErrorKind::TaskFailed { task_id: 0 }
            ));
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn abort_after_completion_does_not_mask_panic_outcome() {
        tokio_block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let task = ctx.spawn(async { panic!("boom") }).unwrap();
            // 等它真的结束：panic 的结局对 wait() 是 TaskFailed。
            assert!(matches!(
                task.wait().await.unwrap_err().kind,
                ErrorKind::TaskFailed { task_id: 0 }
            ));

            // 迟到的 abort 不得把结局改写成「被取消」而让 panic 在排空时消失。
            task.abort();

            let err = rt.stop().await.unwrap_err();
            let ErrorKind::Multiple(errors) = err.kind else {
                panic!("expected aggregated error, got {:?}", err.kind);
            };
            assert_eq!(errors.len(), 1);
            assert!(matches!(
                errors[0].kind,
                ErrorKind::TaskFailed { task_id: 0 }
            ));
        });
    }

    #[test]
    fn cancelled_is_per_layer_and_not_fired_by_rejected_stop() {
        use futures::FutureExt;

        block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();

            let scope = ctx.scope().unwrap();
            let mut child_rt = scope.build().unwrap();
            child_rt.start().await.unwrap();
            let child_ctx = child_rt.handle();

            // 被活跃子租约拒绝的 stop 不算「进入关闭流程」：两层的信号都不该触发。
            rt.stop().await.unwrap_err();
            assert!(ctx.cancelled().now_or_never().is_none());
            assert!(child_ctx.cancelled().now_or_never().is_none());

            // 子层自己的 stop 只触发子层信号，父层不代子层广播。
            child_rt.stop().await.unwrap();
            assert!(child_ctx.cancelled().now_or_never().is_some());
            assert!(ctx.cancelled().now_or_never().is_none());

            drop(child_rt);
            rt.stop().await.unwrap();
            assert!(ctx.cancelled().now_or_never().is_some());
        });
    }

    #[test]
    fn context_id_matches_parent_children_list() {
        block_on(async {
            let mut rt = Builder::new().build().unwrap();
            rt.start().await.unwrap();
            let ctx = rt.handle();
            let root_id = ctx.id();
            assert!(ctx.children().is_empty());

            let scope = ctx.scope().unwrap();
            let child_id = scope.id();
            assert_ne!(child_id, root_id);
            assert_eq!(ctx.children(), vec![child_id]);

            let child_rt = scope.build().unwrap();
            assert_eq!(child_rt.handle().id(), child_id);
            assert_eq!(child_rt.handle().parent().unwrap().id(), root_id);

            // 未启动的子 Runtime 可以只 drop；租约随最后一个字段归还。
            drop(child_rt);
            assert!(ctx.children().is_empty());

            rt.stop().await.unwrap();
        });
    }
}
