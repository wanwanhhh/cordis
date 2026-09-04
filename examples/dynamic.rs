use async_trait::async_trait;
use cordis::{
    Builder, Configurator, Context, Dependency, Error, ErrorKind, Phase, Plugin, PluginDependency,
    PluginScope,
};

// ---------- 配置注入：plugin_with_config ----------
struct PrinterConfig {
    title: &'static str,
}

struct Printer {
    title: &'static str,
}

struct PrinterPlugin;

impl Plugin for PrinterPlugin {
    fn name(&self) -> &'static str {
        "printer"
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        let title = cfg.require::<PrinterConfig>()?.title;
        cfg.provide(Printer { title })?;
        Ok(())
    }
}

// ---------- PluginScope 门禁 ----------
struct Metrics;

struct RootOnlyPlugin;

impl Plugin for RootOnlyPlugin {
    fn name(&self) -> &'static str {
        "root-only"
    }

    fn scope(&self) -> PluginScope {
        PluginScope::Root
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Metrics)?;
        Ok(())
    }
}

struct ChildOnlyPlugin;

impl Plugin for ChildOnlyPlugin {
    fn name(&self) -> &'static str {
        "child-only"
    }

    fn scope(&self) -> PluginScope {
        PluginScope::Child
    }
}

// ---------- 动态配置 + 可选依赖消费方 ----------
struct Telemetry;

struct FeatureFlags {
    verbose: bool,
}

struct ConsumerPlugin;

#[async_trait]
impl Plugin for ConsumerPlugin {
    fn name(&self) -> &'static str {
        "consumer"
    }

    fn dependencies(&self) -> Vec<Dependency> {
        vec![
            Dependency::of::<Printer>(),
            Dependency::optional_of::<Telemetry>(),
        ]
    }

    fn plugin_dependencies(&self) -> Vec<PluginDependency> {
        vec![
            PluginDependency::of("printer"),
            PluginDependency::optional_of("root-only"),
        ]
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        let telemetry = ctx.try_require::<Telemetry>()?.is_some();
        println!("consumer start: telemetry 可选服务存在 = {telemetry}");
        if ctx.has_plugin("root-only") {
            println!("consumer start: 可选插件 root-only 已启用");
        }
        let flags = ctx.require_dynamic::<FeatureFlags>()?;
        println!("consumer start: verbose = {}", flags.read().verbose);
        Ok(())
    }
}

// ---------- apply 失败回滚 ----------
struct Volatile;

struct BrokenPlugin;

impl Plugin for BrokenPlugin {
    fn name(&self) -> &'static str {
        "broken"
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Volatile)?;
        Err(Error::new(Phase::Apply, ErrorKind::Other))
    }
}

// ---------- try_build 失败回传修正 ----------
struct MissingService;

struct NeedsMissingPlugin;

impl Plugin for NeedsMissingPlugin {
    fn name(&self) -> &'static str {
        "needs-missing"
    }

    fn dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::of::<MissingService>()]
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        // 1. apply 失败：副作用整体回滚，错误带阶段与内层插件名
        let mut probe = Builder::new();
        let err = probe.plugin(BrokenPlugin).unwrap_err();
        assert_eq!(err.phase, Phase::Apply);
        assert_eq!(err.plugin, Some("broken"));
        assert!(probe.try_require::<Volatile>()?.is_none());
        println!("[1] apply 失败已回滚: {err}");

        // 2. try_build 校验失败把 Builder 完整带回，修正后可再次构建
        let mut recover = Builder::new();
        recover.plugin(NeedsMissingPlugin)?;
        let (mut recover, err) = recover.try_build().err().expect("校验应失败");
        println!("[2] try_build 失败: {err}");
        recover.provide(MissingService)?;
        let _rt = recover.build()?;
        println!("[2] 修正后重新 build 成功");

        // 3. PluginScope 门禁：Child-only 插件装在根上被拒
        let mut scope_check = Builder::new();
        let err = scope_check.plugin(ChildOnlyPlugin).unwrap_err();
        assert!(matches!(err.kind, ErrorKind::PluginScopeMismatch { .. }));
        println!("[3] 根作用域拒绝 child-only 插件: {err}");

        // 4. 主装配：配置注入 + 根插件 + 动态配置 + 可选依赖
        let mut builder = Builder::new();
        builder.plugin_with_config(PrinterPlugin, PrinterConfig { title: "cordis" })?;
        builder.plugin(RootOnlyPlugin)?;
        builder.provide_dynamic(FeatureFlags { verbose: false })?;
        builder.plugin(ConsumerPlugin)?;
        assert_eq!(builder.require::<Printer>()?.title, "cordis");

        let mut rt = builder.build()?;
        rt.start_serial().await?;

        // 5. 运行期动态配置：读 / set / update / write 多字段一次改完
        let ctx = rt.handle();
        let flags = ctx.require_dynamic::<FeatureFlags>()?;
        flags.update(|f| f.verbose = true);
        assert!(flags.read().verbose);
        {
            let mut guard = flags.write();
            guard.verbose = false;
        }
        println!("[5] 动态配置读改写: verbose = {}", flags.read().verbose);

        // 6. 子作用域：has_plugin 沿父链；child-only 插件装在子级；租约回收后才可停父
        let mut child = ctx.scope()?;
        child.plugin(ChildOnlyPlugin)?;
        assert!(child.has_plugin("root-only"));
        let mut child_rt = child.build()?;
        child_rt.start_serial().await?;
        child_rt.stop().await?;
        drop(child_rt);
        println!("[6] child-only 插件在子作用域运行完毕");

        rt.stop().await?;

        // 7. 父 Runtime 停止后 scope() 被拒
        let err = match ctx.scope() {
            Err(err) => err,
            Ok(_) => unreachable!("停止后不应允许新作用域"),
        };
        assert!(matches!(err.kind, ErrorKind::Stopping));
        println!("[7] stop 后 scope 被拒: {err}");

        Ok(())
    })
}
