//! 旁路通知的异步投递：**每订阅者一条有界 FIFO lane**。
//!
//! 语义契约（与 `Context::emit` 的严格管线相对）：
//!
//! - **发射方不等任何人**：`notify` 只做「沿父链找到各层的 lane 并 `try_send`」，
//!   没有任何 await 点；订阅者慢、卡住都不进入发射方的时间。
//! - **每订阅者内部有序，跨订阅者互不干扰**：每条 lane 是独立的有界队列 + 独立
//!   worker，一个慢订阅者只堆自己那条。
//! - **有界、满了丢、可观测**：丢**新来的**（丢队首会打乱 FIFO 语义），按订阅者
//!   分别累计 `dropped`。
//! - **不提供跨订阅者/跨层的短路**：每个订阅者独立排队之后，「让后面的人别再收到」
//!   已无确定含义。需要确定性的短路请用 `Context::emit`（严格管线，调用方 await）。
//!
//! 关闭语义：`begin_drain` 之后 `enqueue` 一律计入 `rejected`，worker 把各自队列里
//! 已入队的事件处理完再退出；**带预算的** `Runtime::stop_with_timeout` 在预算内
//! join 它们。无预算的 `Runtime::stop()` 会一直等到 worker 收工——一个卡住的异步
//! handler 会让它挂住，需要可终止就用带预算的停止。
//!
//! 无 `tokio` 构建没有可驱动 worker 的执行器，`Context::notify` 退化为「调用方
//! 同步跑完 handler」——切片与计数仍然成立，只是不再有「不等」这一性质。

/// 每条 lane 的默认容量。
///
/// 上限的作用是让慢订阅者不至于无界占用内存；它不是背压——满了就丢并计数，绝不
/// 反过来阻塞发射方。
#[cfg(feature = "tokio")]
pub(crate) const DEFAULT_LANE_CAPACITY: usize = 256;

/// 一次 `notify` 的入队回执。
///
/// `delivered` 是成功入队的份数（不是「已处理完」），`dropped` 是 lane 满被丢弃的，
/// `rejected` 是 hub 已进入排空后拒绝的。handler 自身的错误在异步世界里无法同步
/// 聚合，通过 [`Context::notify_stats`](crate::Context::notify_stats) 观测。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use = "回执携带丢弃计数，忽略它就等于放弃了「哪个订阅者丢了」的可观测性"]
pub struct Receipt {
    /// 成功入队的份数。
    pub delivered: usize,
    /// lane 满被丢弃的份数（丢新来的）。
    pub dropped: usize,
    /// hub 已排空、拒绝接收的份数。
    pub rejected: usize,
}

impl Receipt {
    /// 合并一次逐层投递的回执；仅异步投递路径使用。
    #[cfg(feature = "tokio")]
    pub(crate) fn merge(&mut self, other: Receipt) {
        self.delivered += other.delivered;
        self.dropped += other.dropped;
        self.rejected += other.rejected;
    }
}

/// 一个订阅者的积压与丢弃观测。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backlog {
    /// 该订阅者在本层注册表里的 handler id。
    pub handler_id: usize,
    /// 已入队、尚未被 worker 取走的份数。
    pub queued: usize,
    /// 因 lane 满被丢弃的累计份数。
    pub dropped: u64,
    /// hub 排空后拒绝的累计份数。
    pub rejected: u64,
}

#[cfg(feature = "tokio")]
mod hub {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use tokio::sync::{mpsc, watch};

    use super::{Backlog, Receipt};
    use crate::event::ErasedEventHandler;
    use crate::service::TypeMap;

    /// 交给 worker 的一条 lane 输入：handler + 接收端。
    pub(crate) struct LaneInput {
        pub(crate) handler: Arc<dyn ErasedEventHandler>,
        pub(crate) rx: mpsc::Receiver<Arc<dyn Any + Send + Sync>>,
    }

    /// 单条 lane 的发送端与计数。
    struct Lane {
        handler_id: usize,
        tx: mpsc::Sender<Arc<dyn Any + Send + Sync>>,
        dropped: AtomicU64,
        rejected: AtomicU64,
    }

    /// 一个作用域层的通知 hub：本层全部订阅者的 lane。
    ///
    /// lane 在冻结点一次建好，此后只读——因此 `enqueue` 不需要任何锁。仅有的共享
    /// 可变状态是几个原子（`closed` 与每 lane 的计数）和一个 `watch` 关闭信号。
    ///
    /// **已知取舍（评估过、刻意保留）**：lane 与事件 handler 一一对应，包含只 `emit`
    /// 从不 `notify` 的 handler——它们也会各占一个有界队列与一个 worker 任务。代价换
    /// 来的是 `enqueue` 完全无锁（lane 在冻结点定形，通知热路径上没有任何同步）。
    /// 改成「首次 `notify` 时按需建 lane」会把锁或 CAS 引入热路径，得不偿失，故不采用；
    /// 该常数成本由单个作用域内的 handler 数界定。
    pub(crate) struct NotifyHub {
        lanes: Vec<Lane>,
        /// 事件类型 → 本层订阅了该类型的 lane 下标。冻结点建好，此后只读。
        by_type: TypeMap<Vec<usize>>,
        /// 排空信号：置位后 worker 处理完积压即退出。
        shutdown: watch::Sender<bool>,
        /// 是否已进入排空。置位后 `enqueue` 一律拒绝。
        closed: AtomicBool,
        /// 尚未交给 worker 的接收端；`Runtime::start` 一次性取走。
        inputs: Mutex<Vec<LaneInput>>,
    }

    impl NotifyHub {
        /// 由「本层的全部事件 handler（注册序）」构建。
        pub(crate) fn build(handlers: &[Arc<dyn ErasedEventHandler>]) -> Self {
            let (shutdown, _) = watch::channel(false);
            let mut lanes = Vec::with_capacity(handlers.len());
            let mut inputs = Vec::with_capacity(handlers.len());
            let mut by_type: TypeMap<Vec<usize>> = TypeMap::default();

            for handler in handlers {
                let (tx, rx) = mpsc::channel(super::DEFAULT_LANE_CAPACITY);
                let index = lanes.len();
                by_type
                    .entry(handler.event_type_id())
                    .or_default()
                    .push(index);
                inputs.push(LaneInput {
                    handler: Arc::clone(handler),
                    rx,
                });
                lanes.push(Lane {
                    handler_id: handler.id(),
                    tx,
                    dropped: AtomicU64::new(0),
                    rejected: AtomicU64::new(0),
                });
            }

            Self {
                lanes,
                by_type,
                shutdown,
                closed: AtomicBool::new(false),
                inputs: Mutex::new(inputs),
            }
        }

        /// 取走全部接收端；只应调用一次（`Runtime::start`），之后为空表。
        pub(crate) fn take_inputs(&self) -> Vec<LaneInput> {
            let mut inputs = self
                .inputs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *inputs)
        }

        /// 排空信号；worker 侧 `clone` 一个持有。
        pub(crate) fn shutdown_signal(&self) -> watch::Receiver<bool> {
            self.shutdown.subscribe()
        }

        /// 进入排空：拒绝新的投递，并通知 worker 处理完积压后退出。
        pub(crate) fn begin_drain(&self) {
            self.closed.store(true, Ordering::Release);
            let _ = self.shutdown.send_replace(true);
        }

        /// 同步入队一个事件；返回本次的入队回执。**无 await 点**。
        ///
        /// `type_id` 由调用方以 `TypeId::of::<E>()` 给出，而不是从 `event` 上取：
        /// 对 `Arc<dyn Any + Send + Sync>` 直接调 `.type_id()` 会命中 `Arc` 自身
        /// 经 blanket impl 得到的 `Any`，返回的是 `Arc<...>` 的类型 id。
        pub(crate) fn enqueue(
            &self,
            type_id: std::any::TypeId,
            event: &Arc<dyn Any + Send + Sync>,
        ) -> Receipt {
            if self.closed.load(Ordering::Acquire) {
                return Receipt {
                    delivered: 0,
                    dropped: 0,
                    rejected: 1,
                };
            }
            let Some(indices) = self.by_type.get(&type_id) else {
                return Receipt::default();
            };

            let mut receipt = Receipt::default();
            for &index in indices {
                let lane = &self.lanes[index];
                match lane.tx.try_send(Arc::clone(event)) {
                    Ok(()) => receipt.delivered += 1,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // 丢新来的：保住已入队项的 FIFO 顺序。
                        lane.dropped.fetch_add(1, Ordering::Relaxed);
                        receipt.dropped += 1;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // worker 已退出：与排空同样按「拒绝」计。
                        lane.rejected.fetch_add(1, Ordering::Relaxed);
                        receipt.rejected += 1;
                    }
                }
            }
            receipt
        }

        /// 本层各订阅者的积压与丢弃观测。
        pub(crate) fn backlog(&self) -> Vec<Backlog> {
            self.lanes
                .iter()
                .map(|lane| Backlog {
                    handler_id: lane.handler_id,
                    queued: lane.tx.max_capacity().saturating_sub(lane.tx.capacity()),
                    dropped: lane.dropped.load(Ordering::Relaxed),
                    rejected: lane.rejected.load(Ordering::Relaxed),
                })
                .collect()
        }
    }
}

#[cfg(feature = "tokio")]
pub(crate) use hub::{LaneInput, NotifyHub};
