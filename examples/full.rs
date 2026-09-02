use std::sync::{Arc, LazyLock, RwLock};

use cordis::{Context, Dependency, Error, Plugin};

struct Logger {
    prefix: String,
}

impl Logger {
    fn log(&self, msg: &str) {
        println!("[{}] {}", self.prefix, msg);
    }
}

struct LoggerPlugin;

impl Plugin for LoggerPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Arc::new(RwLock::new(Logger {
            prefix: "logger".to_string(),
        })))?;

        ctx.on_ready(|ctx| {
            let logger = ctx.require::<Arc<RwLock<Logger>>>()?;
            logger.read().unwrap().log("ready");
            Ok(())
        })?;

        Ok(())
    }
}

struct AppPlugin;

impl Plugin for AppPlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        static DEPS: LazyLock<[Dependency; 1]> =
            LazyLock::new(|| [Dependency::of::<Arc<RwLock<Logger>>>()]);
        &DEPS[..]
    }

    fn start(&self, ctx: &Context) -> Result<(), Error> {
        let logger = ctx.require::<Arc<RwLock<Logger>>>()?;
        logger.read().unwrap().log("app start");
        Ok(())
    }

    fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
        println!("app stop");
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    let mut ctx = Context::new();

    ctx.plugin(LoggerPlugin)?;
    ctx.plugin(AppPlugin)?;

    // start 前会先检查 AppPlugin 声明的依赖是否满足
    ctx.start()?;
    ctx.stop()?;

    Ok(())
}
