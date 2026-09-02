//! 类型化事件系统。

use std::any::{Any, TypeId};
use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Context, Error};

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
#[async_trait]
pub trait ErasedEventHandler: Send + Sync + 'static {
    fn id(&self) -> usize;
    fn event_type_id(&self) -> TypeId;
    async fn call(
        &self,
        event: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> Result<EventControl, Error>;
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

#[async_trait]
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

    async fn call(
        &self,
        event: &(dyn Any + Send + Sync),
        ctx: &Context,
    ) -> Result<EventControl, Error> {
        let event = event
            .downcast_ref::<E>()
            .expect("internal event type mismatch");
        self.handler.handle(event, ctx).await
    }
}

/// 事件订阅句柄。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Subscription {
    pub(crate) context_id: usize,
    pub(crate) handler_id: usize,
}
