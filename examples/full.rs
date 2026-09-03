use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use cordis::{Builder, Configurator, Context, Dependency, Error, Plugin, SyncHook};

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
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Arc::new(RwLock::new(Logger {
            prefix: "logger".to_string(),
        })))?;

        cfg.on_ready(SyncHook(|ctx: &Context| {
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
    fn dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::of::<Arc<RwLock<Logger>>>()]
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        let logger = ctx.require::<Arc<RwLock<Logger>>>()?;
        logger.read().unwrap().log("app start");
        Ok(())
    }

    async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
        println!("app stop");
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut builder = Builder::new();

        builder.plugin(LoggerPlugin)?;
        builder.plugin(AppPlugin)?;

        let mut rt = builder.build()?;
        rt.start().await?;
        rt.stop().await?;

        Ok(())
    })
}
