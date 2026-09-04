use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cordis::{
    AsyncFnEventHandler, Builder, Context, Error, ErrorKind, EventControl, EventHandler,
    FnEventHandler, Phase,
};

#[derive(Debug)]
struct UserMessage(String);

struct FailingHandler;

#[async_trait]
impl EventHandler<UserMessage> for FailingHandler {
    async fn handle(&self, _event: &UserMessage, _ctx: &Context) -> Result<EventControl, Error> {
        Err(Error::new(Phase::Event, ErrorKind::Other))
    }
}

fn main() -> Result<(), Error> {
    futures::executor::block_on(async {
        // 1. 串行 emit：子层 handler 先执行，再沿父链冒泡
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut builder = Builder::new();
        let root = observed.clone();
        builder.on::<UserMessage, _>(FnEventHandler(move |event: &UserMessage, _: &Context| {
            root.lock().unwrap().push(format!("root: {}", event.0));
            Ok(EventControl::Continue)
        }))?;
        let mut rt = builder.build()?;
        let ctx = rt.handle();
        let mut session = ctx.scope()?;
        let child = observed.clone();
        session.on::<UserMessage, _>(FnEventHandler(move |event: &UserMessage, _: &Context| {
            child.lock().unwrap().push(format!("session: {}", event.0));
            Ok(EventControl::Continue)
        }))?;
        let session_rt = session.build()?;
        session_rt
            .handle()
            .emit(UserMessage("hello".to_string()))
            .await?;
        println!("[1] 冒泡顺序: {:?}", observed.lock().unwrap());
        drop(session_rt);
        rt.stop().await?;

        // 2. off 取消订阅后 handler 不再收到事件
        let mut builder = Builder::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let sub = builder.on::<UserMessage, _>(FnEventHandler(
            move |event: &UserMessage, _: &Context| {
                sink.lock().unwrap().push(event.0.clone());
                Ok(EventControl::Continue)
            },
        ))?;
        builder.off(sub)?;
        let ctx = builder.build()?.handle();
        ctx.emit(UserMessage("dropped".to_string())).await?;
        assert!(seen.lock().unwrap().is_empty());
        println!("[2] off 之后订阅者未收到事件");

        // 3. Bail：子层 handler 返回 Bail 后停止冒泡，父层不再执行
        let mut builder = Builder::new();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let parent = trace.clone();
        builder.on::<UserMessage, _>(FnEventHandler(move |_: &UserMessage, _: &Context| {
            parent.lock().unwrap().push("parent".to_string());
            Ok(EventControl::Continue)
        }))?;
        let mut rt = builder.build()?;
        let mut gated = rt.handle().scope()?;
        gated.on::<UserMessage, _>(FnEventHandler(|_: &UserMessage, _: &Context| {
            Ok(EventControl::Bail)
        }))?;
        let gated_rt = gated.build()?;
        gated_rt
            .handle()
            .emit(UserMessage("bailed".to_string()))
            .await?;
        assert!(trace.lock().unwrap().is_empty());
        println!("[3] Bail 阻止了向父链冒泡");
        drop(gated_rt);
        rt.stop().await?;

        // 4. 严格 emit 首个错误即中止；emit_notify 收集错误且不阻断父链冒泡
        let mut builder = Builder::new();
        let notified = Arc::new(Mutex::new(Vec::new()));
        let parent = notified.clone();
        builder.on::<UserMessage, _>(FnEventHandler(move |event: &UserMessage, _: &Context| {
            parent
                .lock()
                .unwrap()
                .push(format!("parent 仍收到: {}", event.0));
            Ok(EventControl::Continue)
        }))?;
        let mut rt = builder.build()?;
        let mut faulty = rt.handle().scope()?;
        faulty.on::<UserMessage, _>(FailingHandler)?;
        let faulty_rt = faulty.build()?;
        let faulty_ctx = faulty_rt.handle();
        let err = faulty_ctx
            .emit(UserMessage("strict".to_string()))
            .await
            .unwrap_err();
        println!("[4] 严格 emit 返回首个错误: {err}");
        let errors = faulty_ctx
            .emit_notify(UserMessage("notify".to_string()))
            .await;
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].phase, Phase::Event);
        println!(
            "[4] notify 收集 {} 个错误且父链照常冒泡: {:?}",
            errors.len(),
            notified.lock().unwrap()
        );
        drop(faulty_rt);
        rt.stop().await?;

        // 5. emit_parallel：同层 handler 并发执行，全部完成后再冒泡父链
        let mut builder = Builder::new();
        let done = Arc::new(Mutex::new(0_usize));
        let parent = done.clone();
        builder.on::<UserMessage, _>(FnEventHandler(move |_: &UserMessage, _: &Context| {
            *parent.lock().unwrap() += 1;
            Ok(EventControl::Continue)
        }))?;
        let mut rt = builder.build()?;
        let mut worker = rt.handle().scope()?;
        for _ in 0..2 {
            let sink = done.clone();
            worker.on::<UserMessage, _>(AsyncFnEventHandler(
                move |_: &UserMessage, _: Context| {
                    let sink = sink.clone();
                    async move {
                        *sink.lock().unwrap() += 1;
                        Ok(EventControl::Continue)
                    }
                },
            ))?;
        }
        let worker_rt = worker.build()?;
        worker_rt
            .handle()
            .emit_parallel(UserMessage("parallel".to_string()))
            .await?;
        assert_eq!(*done.lock().unwrap(), 3);
        println!("[5] emit_parallel 同层 2 个 + 父链 1 个 handler 全部执行");
        drop(worker_rt);
        rt.stop().await?;

        Ok(())
    })
}
