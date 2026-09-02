use std::sync::{Arc, Mutex};

use cordis::{Context, Error, EventControl, FnEventHandler};

#[derive(Debug)]
struct UserMessage(String);

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        let mut ctx = Context::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        let root = observed.clone();
        ctx.on::<UserMessage, _>(FnEventHandler(move |event: &UserMessage, _: &Context| {
            root.lock().unwrap().push(format!("root: {}", event.0));
            Ok(EventControl::Continue)
        }))?;

        let mut session = ctx.scope();
        let child = observed.clone();
        session.on::<UserMessage, _>(FnEventHandler(move |event: &UserMessage, _: &Context| {
            child.lock().unwrap().push(format!("session: {}", event.0));
            Ok(EventControl::Continue)
        }))?;

        session.emit(UserMessage("hello".to_string())).await?;

        for line in observed.lock().unwrap().iter() {
            println!("{line}");
        }

        Ok(())
    })
}
