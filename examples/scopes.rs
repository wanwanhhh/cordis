use async_trait::async_trait;
use cordis::{Builder, Configurator, Context, Dependency, Error, Plugin};

struct Database;

impl Database {
    fn query(&self, sql: &str) -> &str {
        println!("query: {sql}");
        "ok"
    }
}

struct RootPlugin;

#[async_trait]
impl Plugin for RootPlugin {
    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        cfg.provide(Database)?;
        Ok(())
    }
}

struct SessionService;

struct SessionPlugin;

#[async_trait]
impl Plugin for SessionPlugin {
    fn dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::of::<Database>()]
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        let db = cfg.require::<Database>()?;
        db.query("create session");
        cfg.provide(SessionService)?;
        Ok(())
    }
}

struct SubflowService;

struct SubflowPlugin;

#[async_trait]
impl Plugin for SubflowPlugin {
    fn dependencies(&self) -> Vec<Dependency> {
        vec![
            Dependency::of::<Database>(),
            Dependency::of::<SessionService>(),
        ]
    }

    fn apply(&self, cfg: &mut Configurator<'_>) -> Result<(), Error> {
        let db = cfg.require::<Database>()?;
        let _session = cfg.require::<SessionService>()?;
        db.query("start subflow");
        cfg.provide(SubflowService)?;
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut builder = Builder::new();
        builder.plugin(RootPlugin)?;
        let mut rt = builder.build()?;
        rt.start().await?;

        let ctx: Context = rt.handle();
        let mut session = ctx.scope()?;
        session.plugin(SessionPlugin)?;
        let mut session_rt = session.build()?;
        session_rt.start().await?;

        let session_ctx = session_rt.handle();
        let mut subflow = session_ctx.scope()?;
        subflow.plugin(SubflowPlugin)?;

        assert!(subflow.contains::<Database>());
        assert!(subflow.contains::<SessionService>());

        let mut subflow_rt = subflow.build()?;
        subflow_rt.start().await?;
        subflow_rt.stop().await?;
        drop(subflow_rt);

        session_rt.stop().await?;
        drop(session_rt);

        rt.stop().await?;

        Ok(())
    })
}
