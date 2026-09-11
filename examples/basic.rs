use async_trait::async_trait;
use cordis::{Builder, Configurator, Error, Plugin, SyncHook};

#[derive(Default)]
struct Logger {
    name: String,
}

struct LoggerPlugin;

#[async_trait]
impl Plugin for LoggerPlugin {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Logger {
            name: "basic".to_string(),
        })?;

        cfg.on_ready(SyncHook(|ctx: &cordis::Context| {
            let logger = ctx.require::<Logger>()?;
            println!("ready, logger.name = {}", logger.name);
            Ok(())
        }))?;

        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut builder = Builder::new();

        builder.plugin(LoggerPlugin)?;
        let mut rt = builder.build()?;
        rt.start().await?;
        rt.stop().await.into_result()?;

        Ok(())
    })
}
