//! 类型化事件系统。

use std::any::{Any, TypeId};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use async_trait::async_trait;

use crate::id::ScopeId;
use crate::{Context, Error, ErrorKind, Phase};

/// 事件 marker。
///
/// 任何满足 `Send + Sync + 'static` 的类型都自动实现 `Event`。
pub trait Event: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> Event for T {}

/// 事件处理结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventControl {
    /// 继续执行后续 handler 和向上冒泡。
    Continue,
    /// 停止当前 Scope 内后续 handler，并停止向上冒泡。
    Bail,
}

/// 异步事件 handler。
#[async_trait]
pub trait EventHandler<E>: Send + Sync + 'static {
    async fn handle(&self, event: &E, ctx: &Context) -> Result<EventControl, Error>;
}

/// 同步闭包事件 handler 适配器。
pub struct FnEventHandler<F>(pub F);

#[async_trait]
impl<E, F> EventHandler<E> for FnEventHandler<F>
where
    E: Event,
    F: for<'a, 'b> Fn(&'a E, &'b Context) -> Result<EventControl, Error> + Send + Sync + 'static,
{
    async fn handle(&self, event: &E, ctx: &Context) -> Result<EventControl, Error> {
        (self.0)(event, ctx)
    }
}

/// 异步闭包事件 handler 适配器。
pub struct AsyncFnEventHandler<F>(pub F);

#[async_trait]
impl<E, F, Fut> EventHandler<E> for AsyncFnEventHandler<F>
where
    E: Event,
    F: Fn(&E, Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<EventControl, Error>> + Send + 'static,
{
    async fn handle(&self, event: &E, ctx: &Context) -> Result<EventControl, Error> {
        let ctx = ctx.clone();
        (self.0)(event, ctx).await
    }
}

/// 类型擦除的异步事件 handler。
///
/// `call` 手写为返回 boxed future，而不是 `#[async_trait]`：内层
/// [`EventHandler::handle`] 本身已返回 boxed future，手写签名可以直接透传它，
/// 省掉 async_trait 在外层再包的那一层 box（每个 handler 每次调用少一次堆分配；
/// 实测由 2 次/88 B 降到 1 次/32 B）。
pub trait ErasedEventHandler: Send + Sync + 'static {
    fn id(&self) -> usize;
    fn event_type_id(&self) -> TypeId;
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        ctx: &'a Context,
    ) -> Pin<Box<dyn Future<Output = Result<EventControl, Error>> + Send + 'a>>;
}

/// 具体类型的事件 handler 包装。
pub(crate) struct TypedEventHandler<E, H> {
    id: usize,
    handler: H,
    _marker: PhantomData<fn() -> E>,
}

impl<E, H> TypedEventHandler<E, H> {
    pub(crate) fn new(id: usize, handler: H) -> Self {
        Self {
            id,
            handler,
            _marker: PhantomData,
        }
    }
}

impl<E, H> ErasedEventHandler for TypedEventHandler<E, H>
where
    E: Event,
    H: EventHandler<E>,
{
    fn id(&self) -> usize {
        self.id
    }

    fn event_type_id(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        ctx: &'a Context,
    ) -> Pin<Box<dyn Future<Output = Result<EventControl, Error>> + Send + 'a>> {
        // 派发路径（含冻结后的按 TypeId 分组）已保证类型匹配；该分支实际
        // 不可达。即使触达也以错误上报，绝不 panic 击穿用户的 await 点。
        match event.downcast_ref::<E>() {
            Some(event) => self.handler.handle(event, ctx),
            None => Box::pin(async { Err(Error::new(Phase::Event, ErrorKind::Other)) }),
        }
    }
}

/// 事件订阅句柄。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub(crate) context_id: ScopeId,
    pub(crate) handler_id: usize,
}
