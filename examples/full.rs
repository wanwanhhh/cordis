use std::sync::{Arc, LazyLock, RwLock};

use async_trait::async_trait;
use cordis::{Context, Dependency, Error, Plugin, SyncHook};

struct Logger {
    prefix: String,
}

impl Logger {
    fn log(&self, msg: &str) {
        println!("[{}] {}", self.prefix, msg);
    }
}

struct LoggerPlugin;

#[async_trait]
impl Plugin for LoggerPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Arc::new(RwLock::new(Logger {
            prefix: "logger".to_string(),
        })))?;

        ctx.on_ready(SyncHook(|ctx: &mut Context| {
            let logger = ctx.require::<Arc<RwLock<Logger>>>()?;
            logger.read().unwrap().log("ready");
            Ok(())
        }))?;

        Ok(())
    }
}

struct AppPlugin;

#[async_trait]
impl Plugin for AppPlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        static DEPS: LazyLock<[Dependency; 1]> =
            LazyLock::new(|| [Dependency::of::<Arc<RwLock<Logger>>>()]);
        &DEPS[..]
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        let logger = ctx.require::<Arc<RwLock<Logger>>>()?;
        logger.read().unwrap().log("app start");
        Ok(())
    }

    async fn stop(&self, _ctx: &mut Context) -> Result<(), Error> {
        println!("app stop");
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut ctx = Context::new();

        ctx.plugin(LoggerPlugin)?;
        ctx.plugin(AppPlugin)?;

        ctx.start().await?;
        ctx.stop().await?;

        Ok(())
    })
}
