use cordis::{Context, Error, Plugin};

#[derive(Default)]
struct Logger {
    name: String,
}

struct LoggerPlugin;

impl Plugin for LoggerPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Logger {
            name: "basic".to_string(),
        })?;

        ctx.on_ready(|ctx| {
            let logger = ctx.require::<Logger>()?;
            println!("ready, logger.name = {}", logger.name);
            Ok(())
        })?;

        Ok(())
    }
}

fn main() -> Result<(), Error> {
    let mut ctx = Context::new();

    ctx.plugin(LoggerPlugin)?;
    ctx.start()?;
    ctx.stop()?;

    Ok(())
}
