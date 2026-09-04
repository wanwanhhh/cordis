use std::time::Duration;

use async_trait::async_trait;
use cordis::{
    Builder, Context, Error, ErrorKind, EventControl, FnEventHandler, Phase, Plugin, TaskFailed,
};

/// 优雅 worker：看到 is_stopping 后自然收尾，stop 排空时无需取消。
struct WorkerPlugin;

#[async_trait]
impl Plugin for WorkerPlugin {
    fn name(&self) -> &'static str {
        "worker"
    }

    async fn start(&self, ctx: &Context) -> Result<(), Error> {
        let watcher = ctx.clone();
        ctx.spawn(async move {
            let mut ticks = 0u64;
            while !watcher.is_stopping() {
                tokio::task::yield_now().await;
                ticks += 1;
            }
            println!("worker 自然退出，共 {ticks} ticks");
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

    // 1. 任务失败 → TaskFailed 旁路事件（沿父链冒泡，这里由根 handler 收到）
    ctx.spawn(async { Err(Error::new(Phase::Start, ErrorKind::Other)) })?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. 会话子作用域：登记一个永不退出的任务，只能超时取消
    let scope = ctx.scope()?;
    let mut session = scope.build()?;
    session.start().await?;
    session.handle().spawn(async move {
        loop {
            tokio::task::yield_now().await;
        }
        #[allow(unreachable_code)]
        Ok::<(), Error>(())
    })?;

    // 3. 停止可观测性：父 stop 被活跃子租约拒绝，ids 指明阻塞方
    println!("活跃子作用域: {:?}", ctx.children());
    if let Err(err) = rt.stop().await {
        println!("父 stop 被拒绝: {err}");
    }

    // 4. 子作用域优雅停止：200ms 预算后顽固任务被强制取消并上报
    match session.stop_with_timeout(Duration::from_millis(200)).await {
        Ok(()) => println!("session 干净停止"),
        Err(err) => println!("session 停止报告: {err}"),
    }
    drop(session);

    // 5. 父停止：worker 任务看到停止标志后自然收尾，排空即完成
    rt.stop().await?;
    println!("root 已停止");
    Ok(())
}
