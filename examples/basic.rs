use async_trait::async_trait;
use cordis::{Context, Error, Plugin, SyncHook};

#[derive(Default)]
struct Logger {
    name: String,
}

struct LoggerPlugin;

#[async_trait]
impl Plugin for LoggerPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Logger {
            name: "basic".to_string(),
        })?;

        ctx.on_ready(SyncHook(|ctx: &mut Context| {
            let logger = ctx.require::<Logger>()?;
            println!("ready, logger.name = {}", logger.name);
            Ok(())
        }))?;

        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut ctx = Context::new();

        ctx.plugin(LoggerPlugin)?;
        ctx.start().await?;
        ctx.stop().await?;

        Ok(())
    })
}
