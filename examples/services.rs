//! 服务注册的几种形态：普通服务、集合服务（多实现）、懒加载工厂、运行时动态配置。
//!
//! 运行：`cargo run --example services`

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cordis::{Builder, Configurator, Error, Plugin};

/// 多实现用的 trait。
trait LlmProvider: Send + Sync {
    fn name(&self) -> &'static str;
}

struct OpenAiProvider;
struct ClaudeProvider;

impl LlmProvider for OpenAiProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
}

impl LlmProvider for ClaudeProvider {
    fn name(&self) -> &'static str {
        "claude"
    }
}

/// 懒加载服务：构造有代价，首次访问才创建。
struct Expensive {
    value: u32,
}

impl Expensive {
    fn new() -> Self {
        println!("  (Expensive 首次构造)");
        Self { value: 42 }
    }
}

/// 记录工厂被调用次数，用来演示「成功路径至多执行一次」。
#[derive(Clone)]
struct FactoryCalls(Arc<AtomicUsize>);

/// 在 apply 阶段读取各类服务。
struct Reporter;

impl Plugin for Reporter {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        // 集合服务：按注册时的精确类型取回全部实现
        let providers = cfg.require_all::<Arc<dyn LlmProvider>>()?;
        let names: Vec<_> = providers.iter().map(|p| p.name()).collect();
        println!("  集合服务实现: {names:?}");

        // 懒加载：第一次 require 触发构造，第二次复用同一实例
        let expensive = cfg.require::<Expensive>()?;
        println!("  懒加载服务: {}", expensive.value);
        let _again = cfg.require::<Expensive>()?;

        Ok(())
    }
}

/// 事件：任何 `Send + Sync + 'static` 类型都可直接作为事件，无需额外实现。
struct ConfigChanged;

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let calls = FactoryCalls(Arc::new(AtomicUsize::new(0)));

        let mut builder = Builder::new();

        // 1. 集合服务：多实现必须统一成同一个 trait object 类型，
        //    否则 require_all::<Arc<dyn LlmProvider>> 查不到（静默返回空集合）。
        builder.provide_collect(Arc::new(OpenAiProvider) as Arc<dyn LlmProvider>)?;
        builder.provide_collect(Arc::new(ClaudeProvider) as Arc<dyn LlmProvider>)?;

        // 2. 懒加载工厂
        let counter = calls.clone();
        builder.provide_factory(move || {
            counter.0.fetch_add(1, Ordering::SeqCst);
            Ok(Expensive::new())
        })?;

        // 3. 运行时动态配置（注册为 Arc<DynamicValue<T>>，不占 T 的类型槽位）
        builder.provide_dynamic(7_u32)?;

        // 4. 普通服务
        builder.provide(String::from("hello"))?;

        builder.plugin(Reporter)?;

        let mut rt = builder.build()?;
        rt.start().await?;

        let ctx = rt.handle();

        let all = ctx.require_all::<Arc<dyn LlmProvider>>()?;
        println!("启动后集合服务实现数: {}", all.len());

        // 动态配置可在运行期改写
        let dynamic = ctx.require_dynamic::<u32>()?;
        dynamic.set(9);
        println!("动态配置: {}", *dynamic.read());

        // notify 形式的事件：handler 失败不阻断主流程（本例无 handler）
        let _ = ctx.emit_notify(ConfigChanged).await;

        println!(
            "工厂调用次数（应恰好为 1）: {}",
            calls.0.load(Ordering::SeqCst)
        );

        rt.stop().await
    })
}
