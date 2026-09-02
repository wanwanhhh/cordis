use std::sync::OnceLock;

use async_trait::async_trait;
use cordis::{Context, Dependency, Error, Plugin};

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
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        ctx.provide(Database)?;
        Ok(())
    }
}

struct SessionService;

struct SessionPlugin;

#[async_trait]
impl Plugin for SessionPlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        static DEPS: OnceLock<Dependency> = OnceLock::new();
        let dependency = DEPS.get_or_init(Dependency::of::<Database>);
        std::slice::from_ref(dependency)
    }

    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        let db = ctx.require::<Database>()?;
        db.query("create session");
        ctx.provide(SessionService)?;
        Ok(())
    }
}

struct SubflowService;

struct SubflowPlugin;

#[async_trait]
impl Plugin for SubflowPlugin {
    fn dependencies(&self) -> &'static [Dependency] {
        static DEPS: OnceLock<[Dependency; 2]> = OnceLock::new();
        let deps = DEPS.get_or_init(|| {
            [
                Dependency::of::<Database>(),
                Dependency::of::<SessionService>(),
            ]
        });
        &deps[..]
    }

    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        let db = ctx.require::<Database>()?;
        let _session = ctx.require::<SessionService>()?;
        db.query("start subflow");
        ctx.provide(SubflowService)?;
        Ok(())
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut ctx = Context::new();
        ctx.plugin(RootPlugin)?;
        ctx.start().await?;

        let mut session = ctx.scope();
        session.plugin(SessionPlugin)?;
        session.start().await?;

        let mut subflow = session.scope();
        subflow.plugin(SubflowPlugin)?;

        assert!(subflow.contains::<Database>());
        assert!(subflow.contains::<SessionService>());

        subflow.start().await?;
        subflow.stop().await?;
        drop(subflow);

        session.stop().await?;
        drop(session);

        ctx.stop().await?;

        Ok(())
    })
}
