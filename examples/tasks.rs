use std::time::Duration;

use async_trait::async_trait;
use cordis::{
    Builder, Context, Error, ErrorKind, EventControl, FnEventHandler, Phase, Plugin, TaskFailed,
};

/// 优雅 worker：等取消信号，而不是轮询 `is_stopping`。
struct WorkerPlugin;

#[async_trait]
impl Plugin for WorkerPlugin {
    fn name(&self) -> &'static str {
        "worker"
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        let wait_ctx = ctx.clone();
        ctx.spawn(async move {
            let mut ticks = 0u64;
            loop {
                // 取消信号在 `stop` 进入清理之前触发，因此这里总能先收到信号，
                // 不必担心「排空已经在等我了」。
                tokio::select! {
                    () = wait_ctx.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_millis(1)) => ticks += 1,
                }
            }
            println!("worker 收到取消信号后退出，共 {ticks} ticks");
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut builder = Builder::new();
    builder.on::<TaskFailed, _>(FnEventHandler(|event: &TaskFailed, _: &Context| {
        println!("TaskFailed 事件: task {} -> {}", event.task_id, event.error);
        Ok(EventControl::Continue)
    }))?;
    builder.plugin(WorkerPlugin)?;
    let mut rt = builder.build()?;
    rt.start().await?;
    let ctx = rt.handle();

    // 1. 任务返回 Err → TaskFailed 旁路事件（沿父链冒泡，由根 handler 收到）
    ctx.spawn(async { Err(Error::new(Phase::Start, ErrorKind::Other)) })?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 2. StopHandle：把「请求停止」的能力交给别人（信号处理、管理端点、测试兜底）。
    //    请求只广播取消信号，不开始清理；关闭仍由持有 Runtime 的 owner 执行。
    let stop_handle = rt.stop_handle();
    let requester = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        println!("外部请求停止");
        stop_handle.request_stop();
    });

    // owner 侧等信号后收口。请求不等于 Stopping：此刻 spawn / scope 仍然可用。
    assert!(!ctx.is_stopping());
    rt.handle().cancelled().await;
    rt.stop().await?;
    requester.await.unwrap();
    println!("root 已停止（worker 由取消信号唤醒后自然退出）");

    // 3. 会话子作用域：TaskHandle 提供单任务取消 + 等终态
    let mut rt = Builder::new().build()?;
    rt.start().await?;
    let ctx = rt.handle();
    let session = ctx.scope()?;
    // 子作用域 id 就是父 children() 里列出的值，可直接和应用自己的表对齐。
    let session_id = session.id();
    assert_eq!(ctx.children(), vec![session_id]);

    let mut session_rt = session.build()?;
    session_rt.start().await?;
    let run_task = session_rt.handle().spawn(async {
        futures::future::pending::<()>().await;
        Ok::<(), Error>(())
    })?;

    // owner 主动取消并等它落定：这是请求，不是失败，不计入停止错误。
    run_task.abort();
    run_task.wait().await?;
    assert!(run_task.is_finished());

    // 4. 与「主动取消」对照：排空预算耗尽触发的取消会作为停止错误上报。
    session_rt.handle().spawn(async {
        futures::future::pending::<()>().await;
        Ok::<(), Error>(())
    })?;
    match session_rt
        .stop_with_timeout(Duration::from_millis(50))
        .await
    {
        Ok(()) => println!("session {session_id} 干净停止"),
        Err(err) => println!("session {session_id} 停止报告: {err}"),
    }
    drop(session_rt);
    rt.stop().await?;
    Ok(())
}
