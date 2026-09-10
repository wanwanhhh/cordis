//! 三段式上下文模型：Builder / Context / Runtime。

use std::any::TypeId;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};
use std::time::Duration;

#[cfg(feature = "tokio")]
use std::sync::OnceLock;
#[cfg(feature = "tokio")]
use std::time::Instant;

use async_trait::async_trait;

use crate::event::{ErasedEventHandler, Subscription, TypedEventHandler};
use crate::service::{NameMap, TypeMap};
use crate::{
    DynamicValue, Error, ErrorKind, Event, EventControl, EventHandler, Phase, Plugin, PluginScope,
    ServiceRegistry,
};

static NEXT_CONTEXT_ID: AtomicUsize = AtomicUsize::new(0);

/// 生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    /// 已构建，尚未启动，也没有启动尝试。
    Built,
    /// 正在启动：已进入启动流程（`started_plugins` 开始记录）。
    Starting,
    /// 启动成功。
    Running,
    /// 启动失败：已启动的插件仍待 `stop` 回收。与 `Running` 平行，不是终态。
    Failed,
    /// owner 正在清理。可续跑：被丢弃的 `stop` future 会留在此态，重入继续。
    Stopping,
    /// 终态。
    Stopped,
}

impl Lifecycle {
    /// 拒绝新子作用域 / 新任务的状态。
    const fn is_shutting_down(self) -> bool {
        matches!(self, Self::Stopping | Self::Stopped)
    }
}

/// 作用域门：生命周期状态与子作用域租约计数。
///
/// 两个判定必须一起成立：`Context::scope()` 的「父未停止 + 租约加一」，以及
/// `Runtime::stop` 的「租约为零 + 转入 `Stopping`」。用两个独立原子会在它们之间
/// 裂开「父停止的同时长出子作用域」的窗口。早期实现把状态与计数压进同一个
/// `AtomicU64`、用一次 CAS 完成，正确但耦合了状态位与计数位，并由此长出「非法编码
/// 兜底」「租约下溢断言」等一串不可达防御。`scope()` 与 `stop` 都是低频路径，改用
/// 一把互斥锁在同一临界区内判定；热路径 `is_stopping()` 仍是单原子读。
struct Gate {
    inner: Mutex<GateState>,
    /// `Stopping` / `Stopped` 的单向闩：置位后不再清除，供热路径无锁读取。
    stopping: AtomicBool,
}

struct GateState {
    lifecycle: Lifecycle,
    leases: usize,
}

impl Gate {
    fn new() -> Self {
        Self {
            inner: Mutex::new(GateState {
                lifecycle: Lifecycle::Built,
                leases: 0,
            }),
            stopping: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lifecycle(&self) -> Lifecycle {
        self.lock().lifecycle
    }

    /// 热路径判定：本层是否已进入关闭流程。单原子读，不拿锁。
    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn lease_count(&self) -> usize {
        self.lock().leases
    }

    fn set_lifecycle(&self, next: Lifecycle) {
        let mut guard = self.lock();
        guard.lifecycle = next;
        if next.is_shutting_down() {
            self.stopping.store(true, Ordering::Release);
        }
    }

    /// 父未停止则登记一个子作用域租约。返回是否成功；失败意味着父已进入关闭流程。
    ///
    /// 判定与自增在同一临界区内完成，并与 `enter_stopping` 的「租约为零才迁移」
    /// 互斥，因此不存在「父停止的同时长出子作用域」的窗口。
    fn acquire_lease(&self) -> bool {
        let mut guard = self.lock();
        if guard.lifecycle.is_shutting_down() {
            return false;
        }
        guard.leases += 1;
        true
    }

    /// 归还租约。与 `acquire_lease` 由 `ScopeLease` 的 RAII 配对保证一一对应。
    fn release_lease(&self) {
        self.lock().leases -= 1;
    }

    /// 转入 `Stopping`；仍有租约时返回当前计数且不改动状态。
    fn enter_stopping(&self) -> Result<(), usize> {
        let mut guard = self.lock();
        if guard.leases != 0 {
            return Err(guard.leases);
        }
        guard.lifecycle = Lifecycle::Stopping;
        drop(guard);
        self.stopping.store(true, Ordering::Release);
        Ok(())
    }
}

/// 一次性、**电平触发**的信号。
///
/// 触发不可逆，触发后 `poll` 恒为 `Ready`——晚到的等待者因此不会永远挂起。这是
/// 取消语义的硬要求：边沿触发要求「先注册再等待」，而停止可能在任何注册之前就已
/// 发生。
///
/// 同时服务两处：`Data` 的停止信号（`Context::cancelled`）与 `TaskCell` 的完成
/// 信号（排空与 `TaskHandle::wait` 共用）。
struct Signal {
    fired: AtomicBool,
    /// 本信号自己的等待者 token 分配器。token 只需在单个信号内唯一；独立计数
    /// 避免全局原子在多核大量注册时争用同一缓存行。
    token_seq: AtomicU64,
    waiters: Mutex<Vec<Waiter>>,
}

/// 一个等待者注册项。
///
/// `token` 标识「哪个 future 注册的」，**不能用 waker 相等当身份**：同一任务里的
/// 两个 `cancelled()` 等待者由 executor 用同一个 waker 轮询，按 waker 去重会让它们
/// 共享一条记录，其中一个 future 被丢弃时就把另一个仍然存活的等待者的唤醒源一起
/// 摘掉了。按 token 注册/摘除则「谁注册谁负责摘自己」。
struct Waiter {
    token: u64,
    waker: Waker,
}

impl Signal {
    const fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
            token_seq: AtomicU64::new(0),
            waiters: Mutex::new(Vec::new()),
        }
    }

    /// 分配本信号内唯一的等待者 token。
    fn next_token(&self) -> u64 {
        self.token_seq.fetch_add(1, Ordering::Relaxed)
    }

    #[cfg(feature = "tokio")]
    fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    /// 触发并唤醒全部等待者。幂等，可从任意线程调用。
    fn fire(&self) {
        // 置位与清空同在一把锁内：`poll` 的「复查 + 注册」也在同一把锁下进行，
        // 两边因此不会交错出「先查后注册」的丢唤醒窗口。唤醒放到锁外做，避免
        // 在持锁期间调用外部代码。
        let waiters = {
            let mut guard = self.lock_waiters();
            self.fired.store(true, Ordering::Release);
            std::mem::take(&mut *guard)
        };
        for waiter in waiters {
            waiter.waker.wake();
        }
    }

    /// 摘掉 `token` 那个 future 的注册项。
    ///
    /// 等待者主动放弃等待时调用（见 [`Cancelled`] 的 `Drop`）。不这么做的话，
    /// `select! { _ = ctx.cancelled() => …, _ = work => … }` 这种「先等到 work 就
    /// 不再关心停止」的写法会把注册项永久留下，长生命周期作用域的等待者列表会随
    /// 这类任务单调增长。
    fn unregister(&self, token: u64) {
        if self.fired.load(Ordering::Acquire) {
            // 已触发：列表已被 `fire` 清空，无需再上锁。
            return;
        }
        self.lock_waiters().retain(|waiter| waiter.token != token);
    }

    /// 注册/更新 `token` 的等待者。
    ///
    /// 同一个 future 被反复 poll 时（典型：`select!` 的另一个分支把它唤醒）
    /// 只更新 waker 而不新增条目，因此列表长度 = 活着的等待 future 数。
    fn poll(&self, token: u64, waker: &Waker) -> Poll<()> {
        if self.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let mut waiters = self.lock_waiters();
        if self.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        match waiters.iter_mut().find(|waiter| waiter.token == token) {
            Some(waiter) => waiter.waker = waker.clone(),
            None => waiters.push(Waiter {
                token,
                waker: waker.clone(),
            }),
        }
        Poll::Pending
    }

    /// 等待信号触发；已触发时立即返回。返回的 future 被丢弃时会摘掉自己的注册项。
    #[cfg(feature = "tokio")]
    fn wait(&self) -> Waiting<'_> {
        Waiting {
            signal: self,
            token: self.next_token(),
            registered: false,
        }
    }

    fn lock_waiters(&self) -> MutexGuard<'_, Vec<Waiter>> {
        self.waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 测试用：当前注册的等待者数量。
    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.lock_waiters().len()
    }
}

/// [`Signal::wait`] 返回的 future；被丢弃时摘掉自己的注册项。
///
/// 与 [`Cancelled`] 同理：`TaskHandle::wait()` 可能被反复丢弃（`select!` 里另一个
/// 分支先就绪、被 `timeout` 包裹等），不摘除的话注册项会一直累积到该 cell 结束。
#[cfg(feature = "tokio")]
struct Waiting<'a> {
    signal: &'a Signal,
    token: u64,
    registered: bool,
}

#[cfg(feature = "tokio")]
impl Future for Waiting<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.signal.poll(this.token, cx.waker()).is_ready() {
            return Poll::Ready(());
        }
        this.registered = true;
        Poll::Pending
    }
}

#[cfg(feature = "tokio")]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if self.registered {
            self.signal.unregister(self.token);
        }
    }
}

/// [`Context::cancelled`] / [`StopHandle::cancelled`] 返回的 future。
///
/// 必须自己管好注册项：等待者常常写成
/// `select! { _ = ctx.cancelled() => …, _ = work => … }`，先等到 `work` 之后就不再
/// 关心停止信号了。若把注册项一直留在 `Data` 的等待者列表里，长生命周期作用域的
/// 列表会随「曾等待过取消、但自己先结束」的任务单调增长。
struct Cancelled {
    inner: Arc<Data>,
    /// 本 future 在 `Signal` 里的注册身份。摘除只按它进行，不会误伤别的等待者。
    token: u64,
    registered: bool,
}

impl Future for Cancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.inner.poll_cancelled(this.token, cx.waker()).is_ready() {
            return Poll::Ready(());
        }
        this.registered = true;
        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        if self.registered {
            self.inner.cancellation.unregister(self.token);
        }
    }
}

type ReadyHook = Box<dyn LifecycleHook>;
type DisposeHook = Box<dyn LifecycleHook>;

/// 异步生命周期回调。
#[async_trait]
pub trait LifecycleHook: Send + Sync + 'static {
    /// 执行回调。回调只获得只读 `Context`。
    async fn call(&mut self, ctx: &Context) -> Result<(), Error>;
}

/// 同步闭包适配器，便于将普通 `FnMut(&Context) -> Result<(), Error>` 注册为异步钩子。
pub struct SyncHook<F>(pub F);

#[async_trait]
impl<F> LifecycleHook for SyncHook<F>
where
    F: for<'a> FnMut(&'a Context) -> Result<(), Error> + Send + Sync + 'static,
{
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        (self.0)(ctx)
    }
}

/// 异步闭包适配器，用于注册异步生命周期回调。
pub struct AsyncHook<F>(pub F);

#[async_trait]
impl<F, Fut> LifecycleHook for AsyncHook<F>
where
    F: FnMut(Context) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + 'static,
{
    async fn call(&mut self, ctx: &Context) -> Result<(), Error> {
        let ctx = ctx.clone();
        (self.0)(ctx).await
    }
}

/// 数据面：build 后只读，由 `Context` 的 `Arc` 共享。
struct Data {
    context_id: usize,
    parent: Option<Arc<Data>>,
    services: ServiceRegistry,
    /// 插件名 -> `plugins` 索引。注册期查重与调度共用这一份索引。
    plugin_index: NameMap<usize>,
    /// 装配阶段的可变 handler 列表；`build` 时按事件类型分组进 `handlers_by_type`。
    event_handlers: Vec<Arc<dyn ErasedEventHandler>>,
    /// 冻结后的按事件类型分组表：emit 直接查表，避免每次全表扫描。
    /// 只在 `Runtime` 暴露的 `Context` 上读取，读取前必已冻结。
    handlers_by_type: TypeMap<Vec<Arc<dyn ErasedEventHandler>>>,
    next_subscription_id: usize,
    gate: Gate,
    children: Mutex<Vec<usize>>,
    /// 本层停止信号：进入关闭流程（`enter_stopping`）或被显式请求时触发。
    cancellation: Signal,
    /// 是否收到过显式停止请求（[`StopHandle::request_stop`]）。
    ///
    /// 刻意与生命周期状态分开：请求不等于已进入清理，`is_stopping` 的语义不能被它
    /// 污染，否则「拒绝新工作」的依据会在租约检查之前就被置位。
    stop_requested: AtomicBool,
    #[cfg(feature = "tokio")]
    tasks: TaskRegistry,
}

#[cfg(feature = "tokio")]
struct TaskRegistry {
    tasks: Mutex<Vec<Arc<TaskCell>>>,
    next_id: AtomicU64,
    /// 下一次压缩的触发长度。`spawn` 只在表长跨过它时做一次 O(n) 剪除，然后按
    /// 实际长度翻倍；因此 n 次 spawn 的累计剪除代价是 O(n)，而不是每次 O(n) 的
    /// O(n²)。
    compact_at: AtomicUsize,
}

#[cfg(feature = "tokio")]
impl Default for TaskRegistry {
    fn default() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            compact_at: AtomicUsize::new(COMPACT_BASE),
        }
    }
}

/// 任务表压缩阈值的初值与下限。
#[cfg(feature = "tokio")]
const COMPACT_BASE: usize = 4;

/// 单个后台任务的共享控制块。
///
/// 任务表与 [`TaskHandle`] 各持一份 `Arc`，因此「注册表里的任务」和「调用方手里的
/// 句柄」看的是同一份完成信号。刻意不保存 `JoinHandle`：完成信号足以表达结局，
/// 取消只需要 `AbortHandle`。这消掉了旧实现为了「stop future 被丢弃时不 detach」
/// 而在 `Runtime` 上保留 `draining` 句柄的整套补丁——被丢弃的 future 不会从表里
/// 移除任何东西，重入直接重新处理同一个 cell。
#[cfg(feature = "tokio")]
struct TaskCell {
    id: u64,
    /// `spawn` 之后才能取到，因此在发布到任务表之前写入（见 `Context::spawn`）。
    abort: OnceLock<tokio::task::AbortHandle>,
    /// 结局落定信号。电平触发，可重复等待。
    finished: Signal,
    /// 任务结局；先写者胜（正常结束由任务体写，被取消由取消方补写）。
    ///
    /// 「谁取消的」写进结局本身而不是旁边一个独立原子：排空只读一次结局就能正确
    /// 归类，不存在「结局已是取消、来源标记尚未写入」的窗口。
    outcome: Mutex<Option<TaskOutcome>>,
}

/// 后台任务的结局。
#[cfg(feature = "tokio")]
#[derive(Debug, Clone)]
enum TaskOutcome {
    /// 任务体跑完；`Some` 是它返回的错误（完成时已经通过 `TaskFailed` 事件上报过）。
    Completed(Option<Error>),
    /// 任务 panic。**排空必须上报**，因此持有这个结局的 cell 不允许被剪除。
    Panicked,
    /// 被 owner 通过 [`TaskHandle::abort`] 主动取消：取消是请求，不计入停止错误。
    AbortedByOwner,
    /// 排空预算耗尽触发的取消：计入停止错误。
    AbortedByTimeout,
}

/// `TaskOutcome` 的判别式视图。用于只判类别、不取错误内容的热路径（剪除扫描、
/// 排空归类），避免克隆可能很深（`ErrorKind::Multiple`）的 `Error`。
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskOutcomeKind {
    Completed,
    Panicked,
    AbortedByOwner,
    AbortedByTimeout,
}

#[cfg(feature = "tokio")]
impl TaskOutcome {
    fn kind(&self) -> TaskOutcomeKind {
        match self {
            Self::Completed(_) => TaskOutcomeKind::Completed,
            Self::Panicked => TaskOutcomeKind::Panicked,
            Self::AbortedByOwner => TaskOutcomeKind::AbortedByOwner,
            Self::AbortedByTimeout => TaskOutcomeKind::AbortedByTimeout,
        }
    }
}

#[cfg(feature = "tokio")]
impl TaskCell {
    fn is_finished(&self) -> bool {
        self.finished.is_fired()
    }

    /// 落定结局；先写者胜。返回是否由本次调用写入（false 表示已有结局）。
    ///
    /// 只写结局、不唤醒：调用方可在 `fire` 之前再补一次标记，等待者一旦被唤醒
    /// 就必定能看到完整状态。
    fn set_outcome(&self, outcome: TaskOutcome) -> bool {
        let mut slot = self
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if slot.is_none() {
            *slot = Some(outcome);
            true
        } else {
            false
        }
    }

    /// 落定结局并唤醒等待者。重复调用是 no-op（结局不变，信号幂等）。
    fn finish(&self, outcome: TaskOutcome) {
        self.set_outcome(outcome);
        self.finished.fire();
    }

    /// 主动取消：请求 abort、落定结局、唤醒等待者。返回是否由本次取消结束。
    ///
    /// 被 abort 的任务不会再执行任务体的收尾代码，完成信号必须由取消方补发，
    /// 否则排空与 `TaskHandle::wait` 会等一个永远不会触发的信号。
    ///
    /// 只有本次真的落定了结局，才把它算作「这个取消造成的」：任务已经跑完（甚至
    /// panic）之后再 `abort`，不得掩盖原有结局而漏掉 panic 上报。
    fn abort(&self, by_owner: bool) -> bool {
        let outcome = if by_owner {
            TaskOutcome::AbortedByOwner
        } else {
            TaskOutcome::AbortedByTimeout
        };
        let initiated = self.set_outcome(outcome);
        if let Some(abort) = self.abort.get() {
            abort.abort();
        }
        self.finished.fire();
        initiated
    }

    /// 结局是否已经落定。只判存在性，不克隆结局。
    fn is_outcome_written(&self) -> bool {
        self.outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    /// 读取尚未落定的结局。
    fn peek_outcome(&self) -> Option<TaskOutcome> {
        self.outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// 读取结局。只允许在完成信号已触发之后调用（`fire` 必在 `set_outcome` 之后，
    /// 所以此处必有值；缺失说明本类型的内部不变式被破坏，不该静默兜底）。
    fn outcome(&self) -> TaskOutcome {
        self.peek_outcome()
            .expect("finished implies outcome written")
    }

    /// 只读取结局类别，不克隆 `Error`；供剪除扫描与排空归类使用。
    fn outcome_kind(&self) -> Option<TaskOutcomeKind> {
        self.outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(TaskOutcome::kind)
    }

    /// 是否有「排空必须上报、但还没上报」的结局。
    ///
    /// 两个来源：panic（没有事件出口）与排空超时取消（只有排空上报）。`spawn` 的
    /// 剪除必须保住这类 cell，否则一条 panic 会因为它之后又有人 `spawn` 过而被静默
    /// 丢掉，`stop` 反而报成功。任务体 `Err` 在完成时已通过 `TaskFailed` 事件上报、
    /// owner 取消按语义不上报，二者都不在此列。
    fn needs_drain_report(&self) -> bool {
        matches!(
            self.outcome_kind(),
            Some(TaskOutcomeKind::Panicked | TaskOutcomeKind::AbortedByTimeout)
        )
    }
}

/// 保证任务结局一定有写者。
///
/// `catch_unwind` 只兜住任务体；结局的**上报路径**（`emit_notify` → 用户 handler）
/// 与任务体同在一个任务里，它 panic 时 wrapper 会在写结局前展开，完成信号就永远不
/// 会触发。本守卫负责「结局没写就被丢弃」这条兜底路径，并按成因落定不同的结局：
/// 正在展开（真 panic）记 `Panicked`；宿主直接把 future 丢掉（runtime 关闭、未记录
/// 的 abort）记取消——把后者也报成 panic，会让一次 `stop()` 凭空多出 `TaskFailed`
/// 假失败。两种都必须 `fire`，否则排空会等一个永远不来的信号。
#[cfg(feature = "tokio")]
struct FinishOnUnwind(Arc<TaskCell>);

#[cfg(feature = "tokio")]
impl Drop for FinishOnUnwind {
    fn drop(&mut self) {
        if self.0.is_outcome_written() {
            return;
        }
        let outcome = if std::thread::panicking() {
            TaskOutcome::Panicked
        } else {
            TaskOutcome::AbortedByOwner
        };
        self.0.finish(outcome);
    }
}

impl Data {
    fn root() -> Self {
        Self::with_parent(None)
    }

    fn child(parent: Arc<Data>) -> Self {
        Self::with_parent(Some(parent))
    }

    /// 唯一的字段初始化处：`root` 与 `child` 只差 `parent`，各自抄一份字段列表
    /// 意味着新增字段要改两处、漏一处即行为分歧。
    fn with_parent(parent: Option<Arc<Data>>) -> Self {
        Self {
            context_id: NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent,
            services: ServiceRegistry::new(),
            plugin_index: NameMap::default(),
            event_handlers: Vec::new(),
            handlers_by_type: TypeMap::default(),
            next_subscription_id: 0,
            gate: Gate::new(),
            children: Mutex::new(Vec::new()),
            cancellation: Signal::new(),
            stop_requested: AtomicBool::new(false),
            #[cfg(feature = "tokio")]
            tasks: TaskRegistry::default(),
        }
    }

    /// 冻结点：把装配阶段的 handler 列表按事件类型分组。`build` 在唯一一次
    /// `Arc::new(data)` 之前调用；此后 emit 路径只读分组表。
    fn freeze_event_handlers(&mut self) {
        let handlers = std::mem::take(&mut self.event_handlers);
        let mut grouped: TypeMap<Vec<Arc<dyn ErasedEventHandler>>> = TypeMap::default();
        for handler in handlers {
            grouped
                .entry(handler.event_type_id())
                .or_default()
                .push(handler);
        }
        self.handlers_by_type = grouped;
    }

    /// 沿父链自下向上的迭代器，含本层。
    ///
    /// 所有「逐层尝试、到顶收口」的查询都建立在这一个迭代器上——它们此前各自手写
    /// 了一份循环/递归，改一处漏一处。
    fn chain(&self) -> impl Iterator<Item = &Data> {
        std::iter::successors(Some(self), |data| data.parent.as_deref())
    }

    /// 沿父链查找服务。miss 路径无分配；仅在整条链都未命中时构造一次错误。
    ///
    /// 类型不匹配与工厂初始化失败一律直接传播，不再被当作“本层缺失”继续向上
    /// （旧实现中工厂返回的 `ServiceNotFound` 类错误会被误判为缺失而被跳过）。
    fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        for data in self.chain() {
            if let Some(value) = data.services.get_ref::<T>()? {
                return Ok(value);
            }
        }
        Err(Error::new(
            Phase::Build,
            ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
        ))
    }

    /// 沿父链尝试查找服务；链顶仍未命中返回 `Ok(None)`。
    ///
    /// 注意语义：服务不存在 → `Ok(None)`；但槽位存在且工厂初始化失败 →
    /// `Err` 传播（工厂失败不等于“不存在”）。
    fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        for data in self.chain() {
            if let Some(value) = data.services.get_ref::<T>()? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.services.all()
    }

    fn all_with_parents<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        let mut values = Vec::new();
        for data in self.chain() {
            values.extend(data.services.all::<T>()?);
        }
        Ok(values)
    }

    /// 局部 + 父链的完整存在性检查（普通服务 / 工厂 / 集合）。
    fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.chain().any(|data| data.services.contains::<T>())
    }

    /// 单例依赖语义：集合服务不满足依赖，仅普通服务与工厂参与校验。
    fn contains_type(&self, type_id: TypeId) -> bool {
        self.chain()
            .any(|data| data.services.contains_type(type_id))
    }

    /// 返回本层注册了事件 `E` 的 handler（注册序）。要求 `Data` 已冻结。
    fn event_handlers_for<E: Event>(&self) -> &[Arc<dyn ErasedEventHandler>] {
        self.handlers_by_type
            .get(&TypeId::of::<E>())
            .map_or(&[], Vec::as_slice)
    }

    fn has_plugin(&self, name: &str) -> bool {
        self.chain()
            .any(|data| data.plugin_index.contains_key(name))
    }

    /// [`Context::cancelled`] / [`StopHandle::cancelled`] 的轮询实现。
    ///
    /// `Signal` 自身已经不丢唤醒（`enter_stopping` 先写状态、后 `fire`，而 `fire`
    /// 必定唤醒已注册的等待者），所以这里复查状态是一个**免锁快路径**：已经进入
    /// 停止的等待者不必去抢 `Signal` 的锁就能立刻就绪。
    fn poll_cancelled(&self, token: u64, waker: &Waker) -> Poll<()> {
        if self.gate.is_stopping() {
            return Poll::Ready(());
        }
        self.cancellation.poll(token, waker)
    }

    /// 记录显式停止请求并唤醒等待者。幂等。
    ///
    /// 先写请求位再 `fire`：被唤醒的等待者读 [`Data::is_stop_requested`] 时必定
    /// 已经可见。
    fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        self.cancellation.fire();
    }

    fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }
}

/// 装配期回滚检查点：`undo` 的截断位置 + 订阅号计数。
///
/// 只记录「从哪里开始撤销」，不复制任何表。回滚代价与本次 `plugin()` 期间的改动量
/// 成正比，而不是与注册表 / 事件表的总规模成正比。
#[derive(Clone, Copy)]
struct Checkpoint {
    undo: usize,
    next_subscription_id: usize,
}

/// 单条装配期逆操作。
///
/// 每条由一次具体的注册动作产生，回滚时逆序重放。逆操作对「已被内层回滚删掉的
/// 条目」幂等（`remove` 已删键、`pop` 空集合都是 no-op），因此嵌套 `plugin()` 各自
/// 持检查点时不会互相误伤。`provide_collect` 的逆操作是「弹出最后一个元素」而非
/// 「删除整个槽位」，所以回滚一个插件不会误删其他插件向同一集合追加的元素。
enum UndoOp {
    RemoveService(TypeId),
    RemoveFactory(TypeId),
    PopCollection(TypeId),
    PopHandler,
    /// `off` 从中间删除的 handler：连下标一起记下，回滚时原位放回。
    RestoreHandler {
        index: usize,
        handler: Arc<dyn ErasedEventHandler>,
    },
    PopPlugin,
    RemovePluginName(&'static str),
    PopReady,
    PopDispose,
}

/// 缓存插件注册时求值的依赖信息。
struct PluginRecord {
    plugin: Box<dyn Plugin>,
    deps: Vec<crate::Dependency>,
    plugin_deps: Vec<crate::PluginDependency>,
}

impl PluginRecord {
    fn name(&self) -> &'static str {
        self.plugin.name()
    }

    fn priority(&self) -> i32 {
        self.plugin.priority()
    }
}

/// 从同一张依赖图一次算出的启动调度：串行拓扑序与可并发分层。
#[derive(Default)]
struct Schedule {
    /// 串行启动顺序（`start_serial` 与停止逆序的基准）。
    order: Vec<usize>,
    /// 分层并行启动的层序列；同层内无尚未启动的本地依赖边。
    layers: Vec<Vec<usize>>,
}

/// 统一依赖图的调度计算。
///
/// `local_names` 是本层已注册插件名（与 `plugins` 索引对齐）；`has_plugin` 用于
/// 检查父级插件是否存在。可选插件依赖只放宽“必须存在”的校验，不改变“存在则
/// 必须按依赖顺序启动”的语义。
///
/// 串行序与分层共享同一张 `indegree`/`dependents` 图，二者不可能发散；
/// 环检测只在 Kahn 贪心选点阶段发生一次。
fn compute_schedule(
    plugins: &[PluginRecord],
    local_index: &NameMap<usize>,
    has_plugin: impl Fn(&str) -> bool,
) -> Result<Schedule, Error> {
    let n = plugins.len();

    let mut indegree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (i, record) in plugins.iter().enumerate() {
        for plugin_dependency in &record.plugin_deps {
            match local_index.get(plugin_dependency.plugin_name) {
                Some(&j) => {
                    dependents[j].push(i);
                    indegree[i] += 1;
                }
                None => {
                    // 目标插件可能位于父级：依赖检查已通过时不需要本地排序边。
                    if !plugin_dependency.optional && !has_plugin(plugin_dependency.plugin_name) {
                        return Err(Error::new(
                            Phase::Verify,
                            ErrorKind::PluginDependencyNotFound(
                                plugin_dependency.plugin_name.to_string(),
                            ),
                        ));
                    }
                }
            }
        }
    }

    // 串行序：Kahn 贪心，零入度候选中优先取高 priority、同 priority 取注册序靠前。
    // 二叉堆 + 每节点仅在入度归零时入堆一次，复杂度 O(P log P + E)。
    let mut order = Vec::with_capacity(n);
    {
        let mut indeg = indegree.clone();
        let mut heap: BinaryHeap<(i32, Reverse<usize>)> = BinaryHeap::with_capacity(n);
        for (index, &deg) in indeg.iter().enumerate() {
            if deg == 0 {
                heap.push((plugins[index].priority(), Reverse(index)));
            }
        }
        while let Some((_, Reverse(index))) = heap.pop() {
            order.push(index);
            for &dependent in &dependents[index] {
                indeg[dependent] -= 1;
                if indeg[dependent] == 0 {
                    heap.push((plugins[dependent].priority(), Reverse(dependent)));
                }
            }
        }
        if order.len() != n {
            return Err(Error::new(Phase::Verify, ErrorKind::PluginDependencyCycle));
        }
    }

    // 分层：最长路径深度（Kahn 逐轮取全部零入度节点的等价形式）。
    // 图已确认无环，每个节点的深度良定义，层内容与非空性无需防御分支。
    let mut depth = vec![0usize; n];
    for &index in &order {
        let next_depth = depth[index] + 1;
        for &dependent in &dependents[index] {
            if depth[dependent] < next_depth {
                depth[dependent] = next_depth;
            }
        }
    }
    let layer_count = depth.iter().copied().max().map_or(0, |max| max + 1);
    let mut layers: Vec<Vec<usize>> = vec![Vec::new(); layer_count];
    for &index in &order {
        layers[depth[index]].push(index);
    }
    // 层内保持拓扑序输出，与旧实现的 remaining 过滤次序一致。

    Ok(Schedule { order, layers })
}

/// 子作用域租约：持有父 `Data` 强引用，`Drop` 时从父注册表摘除子 id 并归还父计数。
struct ScopeLease {
    parent: Arc<Data>,
    child_id: usize,
}

impl Drop for ScopeLease {
    fn drop(&mut self) {
        // 先摘除子 id 再归还租约：父 `stop` 只会观察到「计数已归零」这一种完成态，
        // 不会看到「计数为零但清单里仍有条目」。
        self.parent
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|id| *id != self.child_id);
        self.parent.gate.release_lease();
    }
}

/// 装配阶段：独占 `&mut`，不 `Clone`。
pub struct Builder {
    data: Data,
    plugins: Vec<PluginRecord>,
    ready: Vec<ReadyHook>,
    dispose: Vec<DisposeHook>,
    lease: Option<ScopeLease>,
    /// 装配期逆操作日志；见 [`UndoOp`]。
    undo: Vec<UndoOp>,
}

impl Builder {
    /// 创建一个空的根 Builder。
    pub fn new() -> Self {
        Self {
            data: Data::root(),
            plugins: Vec::new(),
            ready: Vec::new(),
            dispose: Vec::new(),
            lease: None,
            undo: Vec::new(),
        }
    }

    fn child(parent: Arc<Data>) -> Self {
        let data = Data::child(parent.clone());
        let child_id = data.context_id;
        parent
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(child_id);
        Self {
            data,
            plugins: Vec::new(),
            ready: Vec::new(),
            dispose: Vec::new(),
            lease: Some(ScopeLease { parent, child_id }),
            undo: Vec::new(),
        }
    }

    /// 当前 Builder 在作用域树中的深度；根 Builder 为 0。
    ///
    /// 该方法是公开的辅助查询接口，可与 [`Builder::is_root`] 配合使用。
    pub fn depth(&self) -> usize {
        // `chain()` 含自身，层数 = 链长 - 1。
        self.data.chain().count().saturating_sub(1)
    }

    /// 当前 Builder 是否为根 Builder。
    pub fn is_root(&self) -> bool {
        self.data.parent.is_none()
    }

    /// 当前 Builder 的 context id（进程内全局唯一）。
    ///
    /// 与子作用域建立时写入父 `Context::children()` 的是同一个值，因此可以在
    /// `build()` 之前就把它登记进应用自己的表里。
    pub fn id(&self) -> usize {
        self.data.context_id
    }

    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            undo: self.undo.len(),
            next_subscription_id: self.data.next_subscription_id,
        }
    }

    /// 逆序重放 `undo` 到检查点，并恢复订阅号计数。
    fn rollback(&mut self, checkpoint: Checkpoint) {
        while self.undo.len() > checkpoint.undo {
            match self
                .undo
                .pop()
                .expect("undo log non-empty above checkpoint")
            {
                UndoOp::RemoveService(key) => self.data.services.remove_service_key(key),
                UndoOp::RemoveFactory(key) => self.data.services.remove_factory_key(key),
                UndoOp::PopCollection(key) => self.data.services.pop_collection(key),
                UndoOp::PopHandler => {
                    self.data.event_handlers.pop();
                }
                UndoOp::RestoreHandler { index, handler } => {
                    self.data.event_handlers.insert(index, handler);
                }
                UndoOp::PopPlugin => {
                    self.plugins.pop();
                }
                UndoOp::RemovePluginName(name) => {
                    self.data.plugin_index.remove(name);
                }
                UndoOp::PopReady => {
                    self.ready.pop();
                }
                UndoOp::PopDispose => {
                    self.dispose.pop();
                }
            }
        }
        self.data.next_subscription_id = checkpoint.next_subscription_id;
    }

    /// 注册一个插件。
    ///
    /// 插件依赖在注册时求值并缓存。如果 `apply` 失败，本插件产生的所有副作用
    /// 会回滚到进入 `apply` 之前的状态。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        let name = plugin.name();
        let declared_scope = plugin.scope();
        let actual_scope = if self.is_root() {
            PluginScope::Root
        } else {
            PluginScope::Child
        };

        if declared_scope != PluginScope::Any && declared_scope != actual_scope {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::PluginScopeMismatch {
                    plugin_name: name.to_string(),
                    expected: declared_scope,
                    actual: actual_scope,
                },
            ));
        }

        // 重名检查放在求值依赖之前：重名时不必为两个 `Vec` 白白分配。
        if self.data.plugin_index.contains_key(name) {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::PluginNameAlreadyRegistered(name.to_string()),
            ));
        }

        let deps = plugin.dependencies();
        let plugin_deps = plugin.plugin_dependencies();

        let checkpoint = self.checkpoint();

        // `apply` 是同步的纯注册过程。用 `catch_unwind` 让「回滚」对返回 `Err` 与
        // panic 展开两种失败方式都成立：panic 仍原样向上传播（`resume_unwind`），
        // 不降级成 `Err`、不吞掉 panic 语义，只是多跑一次与 `Err` 路径完全相同的
        // undo。回滚的正确性依赖「undo 覆盖 apply 的全部副作用」，因此每个
        // `Configurator` 写方法都必须同步记录逆操作。
        let apply_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            plugin.apply(&mut Configurator { builder: self })
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.rollback(checkpoint);
                std::panic::resume_unwind(payload);
            }
        };

        if let Err(err) = apply_result {
            self.rollback(checkpoint);
            return Err(err.into_phase(Phase::Apply, Some(name)));
        }

        let index = self.plugins.len();
        self.plugins.push(PluginRecord {
            plugin: Box::new(plugin),
            deps,
            plugin_deps,
        });
        self.data.plugin_index.insert(name, index);
        self.undo.push(UndoOp::PopPlugin);
        self.undo.push(UndoOp::RemovePluginName(name));
        Ok(())
    }

    /// 批量注册插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin,
    {
        for plugin in plugins {
            self.plugin(plugin)?;
        }
        Ok(())
    }

    /// 注册插件并注入配置。
    ///
    /// 配置会以 `C` 类型作为当前 Builder 的服务注入；插件可通过 `require::<C>()` 读取。
    /// 插件 `apply` 失败（返回 `Err` 或 panic 展开）时，配置服务也会一起回滚。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        let checkpoint = self.checkpoint();

        // 配置注入与插件装载必须同属一个事务：`plugin()` 内部的 `catch_unwind` 只会
        // 回滚到它自己的检查点（在 `provide(config)` 之后），因此配置的 panic 回滚
        // 必须由这里统一负责，否则 `apply` panic 会留下注入的配置服务。
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.provide(config)?;
            self.plugin(plugin)
        }));

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                self.rollback(checkpoint);
                Err(err)
            }
            Err(payload) => {
                self.rollback(checkpoint);
                std::panic::resume_unwind(payload);
            }
        }
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.data.services.provide(value)?;
        self.undo.push(UndoOp::RemoveService(TypeId::of::<T>()));
        Ok(())
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        self.data.services.provide_factory(factory)?;
        self.undo.push(UndoOp::RemoveFactory(TypeId::of::<T>()));
        Ok(())
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.data.services.provide_collect(value)?;
        self.undo.push(UndoOp::PopCollection(TypeId::of::<T>()));
        Ok(())
    }

    /// 注册一个运行时动态配置服务。
    ///
    /// 实际服务类型为 `Arc<DynamicValue<T>>`，不会占用原 `T` 的类型槽位。
    pub fn provide_dynamic<T: Send + Sync + 'static>(&mut self, initial: T) -> Result<(), Error> {
        let value = Arc::new(DynamicValue::new(initial));
        self.provide(value)
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.require::<Arc<DynamicValue<T>>>().map(Arc::clone)
    }

    /// 尝试获取服务（普通服务或工厂，含父级）。
    ///
    /// 语义：服务不存在返回 `Ok(None)`；但槽位存在（如工厂）且初始化失败时
    /// 返回 `Err`——工厂失败不等于“不存在”，调用方不应把 `Err` 当作缺失处理。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.data.try_require()
    }

    /// 获取本层局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.data.all()
    }

    /// 获取本层及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.data.all_with_parents()
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.data.require()
    }

    /// 获取本地服务可变引用（仅构建期可调用）。
    pub fn require_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        self.data.services.get_mut()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.data.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.data.has_plugin(name)
    }

    /// 注册一个 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.ready.push(Box::new(hook));
        self.undo.push(UndoOp::PopReady);
        Ok(())
    }

    /// 注册一个 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.dispose.push(Box::new(hook));
        self.undo.push(UndoOp::PopDispose);
        Ok(())
    }

    /// 注册一个事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        let id = self.data.next_subscription_id;
        self.data.next_subscription_id += 1;
        self.data
            .event_handlers
            .push(Arc::new(TypedEventHandler::new(id, handler)));
        self.undo.push(UndoOp::PopHandler);
        Ok(Subscription {
            context_id: self.data.context_id,
            handler_id: id,
        })
    }

    /// 取消一个事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        if subscription.context_id != self.data.context_id {
            return Err(Error::new(Phase::Build, ErrorKind::SubscriptionNotFound));
        }

        if let Some(index) = self
            .data
            .event_handlers
            .iter()
            .position(|handler| handler.id() == subscription.handler_id)
        {
            let handler = self.data.event_handlers.remove(index);
            self.undo.push(UndoOp::RestoreHandler { index, handler });
            Ok(())
        } else {
            Err(Error::new(Phase::Build, ErrorKind::SubscriptionNotFound))
        }
    }

    /// 校验依赖：服务依赖、插件依赖和环。
    pub fn verify(&self) -> Result<(), Error> {
        self.validate().map(|_| ())
    }

    /// 校验所有插件的依赖是否满足。
    pub fn verify_dependencies(&self) -> Result<(), Error> {
        self.validate().map(|_| ())
    }

    /// 校验服务依赖并算出启动调度。
    ///
    /// 调度只在这里算一次，随 [`Runtime`] 保存；`start` 直接取用，既不会重复计算，
    /// 也不存在「已通过校验的 Runtime 在 start 时调度失败」的不可达错误分支。
    fn validate(&self) -> Result<Schedule, Error> {
        for record in &self.plugins {
            for dependency in &record.deps {
                if !dependency.optional && !self.data.contains_type(dependency.type_id) {
                    return Err(Error::new(
                        Phase::Verify,
                        ErrorKind::ServiceNotFound(dependency.name.to_string()),
                    ));
                }
            }
        }

        // 插件依赖的存在性检查不在这里做：`compute_schedule` 是唯一依赖解析入口，
        // 它已经对每个插件的每条 plugin_dep 做了同样判断、报同样的错误。
        self.compute_schedule()
    }

    fn compute_schedule(&self) -> Result<Schedule, Error> {
        compute_schedule(&self.plugins, &self.data.plugin_index, |name| {
            self.data.has_plugin(name)
        })
    }

    /// 消费 Builder 并产出唯一冻结的 Runtime。
    ///
    /// 冻结点是唯一一次 `Arc::new(data)`。
    pub fn build(self) -> Result<Runtime, Error> {
        let schedule = self.validate()?;
        Ok(self.build_validated(schedule))
    }

    /// 假定校验已通过的内部构造；`build` 与 `try_build` 共用，校验只跑一遍。
    fn build_validated(self, schedule: Schedule) -> Runtime {
        let Builder {
            data,
            plugins,
            ready,
            dispose,
            lease,
            undo: _,
        } = self;

        let mut data = data;
        data.freeze_event_handlers();

        Runtime {
            ctx: Context {
                inner: Arc::new(data),
            },
            plugins,
            ready,
            dispose,
            started_plugins: Vec::new(),
            stop_progress: None,
            start_error: None,
            stop_errors: Vec::new(),
            schedule,
            active: AtomicBool::new(false),
            _lease: lease,
        }
    }

    /// 消费 Builder；校验失败时把 Builder（含租约）完整带回。
    #[allow(clippy::result_large_err)]
    pub fn try_build(self) -> Result<Runtime, (Builder, Error)> {
        match self.validate() {
            Ok(schedule) => Ok(self.build_validated(schedule)),
            Err(err) => Err((self, err)),
        }
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// 插件 `apply` 阶段使用的窄接口。
pub struct Configurator<'a> {
    builder: &'a mut Builder,
}

impl Configurator<'_> {
    /// 尝试获取服务（普通服务或工厂，含父级）。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.builder.try_require()
    }

    /// 获取本层局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.builder.require_all()
    }

    /// 获取本层及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.builder.require_all_recursive()
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.builder.require_dynamic()
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.builder.require()
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.builder.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.builder.has_plugin(name)
    }

    /// 注册服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.builder.provide(value)
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        self.builder.provide_factory(factory)
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.builder.provide_collect(value)
    }

    /// 注册一个运行时动态配置服务。
    pub fn provide_dynamic<T: Send + Sync + 'static>(&mut self, initial: T) -> Result<(), Error> {
        self.builder.provide_dynamic(initial)
    }

    /// 注册子插件。
    pub fn plugin<P: Plugin>(&mut self, plugin: P) -> Result<(), Error> {
        self.builder.plugin(plugin)
    }

    /// 批量注册子插件。
    pub fn plugins<I, P>(&mut self, plugins: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = P>,
        P: Plugin,
    {
        self.builder.plugins(plugins)
    }

    /// 注册子插件并注入配置。
    pub fn plugin_with_config<P, C>(&mut self, plugin: P, config: C) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static,
    {
        self.builder.plugin_with_config(plugin, config)
    }

    /// 注册 ready 回调。
    pub fn on_ready(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.builder.on_ready(hook)
    }

    /// 注册 dispose 回调。
    pub fn on_dispose(&mut self, hook: impl LifecycleHook) -> Result<(), Error> {
        self.builder.on_dispose(hook)
    }

    /// 注册事件 handler。
    pub fn on<E, H>(&mut self, handler: H) -> Result<Subscription, Error>
    where
        E: Event,
        H: EventHandler<E>,
    {
        self.builder.on(handler)
    }

    /// 取消事件订阅。
    pub fn off(&mut self, subscription: Subscription) -> Result<(), Error> {
        self.builder.off(subscription)
    }
}

/// 只读数据句柄；`Clone + Send + Sync`。
#[derive(Clone)]
pub struct Context {
    inner: Arc<Data>,
}

impl Context {
    /// 创建一个子 Builder。
    ///
    /// 该方法会在父 `Gate` 的临界区内登记一个子作用域租约；若父已进入停止，
    /// 返回 `ErrorKind::Stopping`。
    pub fn scope(&self) -> Result<Builder, Error> {
        if self.inner.gate.acquire_lease() {
            Ok(Builder::child(self.inner.clone()))
        } else {
            Err(Error::new(Phase::Build, ErrorKind::Stopping))
        }
    }

    /// 尝试获取服务（普通服务或工厂，含父级）。
    ///
    /// 语义：服务不存在返回 `Ok(None)`；但槽位存在（如工厂）且初始化失败时
    /// 返回 `Err`——工厂失败不等于“不存在”，调用方不应把 `Err` 当作缺失处理。
    pub fn try_require<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.inner.try_require()
    }

    /// 获取当前 Context 局部集合中的所有实现。
    pub fn require_all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.inner.all()
    }

    /// 获取当前 Context 及所有父层集合中的所有实现；先本层，再沿父链向上。
    pub fn require_all_recursive<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        self.inner.all_with_parents()
    }

    /// 获取运行时动态配置服务的共享句柄。
    pub fn require_dynamic<T: Send + Sync + 'static>(&self) -> Result<Arc<DynamicValue<T>>, Error> {
        self.require::<Arc<DynamicValue<T>>>().map(Arc::clone)
    }

    /// 获取服务引用。
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        // 用 `try_require` 区分「真正未命中」与「工厂失败」：`Ok(None)` 只可能是
        // 沿整条父链都没找到，由这里构造 `Phase::Require` 的错误；工厂初始化失败
        // 以 `Err` 透传，phase/kind 原样保留，不被运行期查询语义覆盖。
        match self.inner.try_require::<T>()? {
            Some(value) => Ok(value),
            None => Err(Error::new(
                Phase::Require,
                ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
            )),
        }
    }

    /// 判断服务是否存在（局部 + 父级）。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.inner.contains::<T>()
    }

    /// 判断某个插件是否已注册（局部 + 父级）。
    pub fn has_plugin(&self, name: &str) -> bool {
        self.inner.has_plugin(name)
    }

    /// 本层是否已经拒绝新工作（`Stopping` / `Stopped`）。
    fn shutting_down(&self) -> bool {
        self.inner.gate.is_stopping()
    }

    /// 本层 Context 的稳定 id（进程内全局唯一）。
    ///
    /// 子作用域的 id 就是父 [`Context::children`] 里列出的那个值，因此它让「框架的
    /// 子作用域清单」与「应用自己的会话表」可以直接对上。
    pub fn id(&self) -> usize {
        self.inner.context_id
    }

    /// 本层是否已进入停止流程。
    pub fn is_stopping(&self) -> bool {
        self.shutting_down()
    }

    /// 等待本层进入停止流程或被显式请求停止。
    ///
    /// 电平触发：本层已处于 `Stopping` / `Stopped`，或已有人调用
    /// [`StopHandle::request_stop`] 时立即完成；否则挂起，直到上述任一条件成立。
    ///
    /// 这是给长驻任务用的等待点，替代「轮询 [`Context::is_stopping`] + 猜间隔」。
    /// 触发点在框架内部（进入关闭流程那一刻）与显式请求两处，因此任何停止路径都会
    /// 唤醒它，不会漏——而且触发**早于**插件 `stop` 与任务排空，长驻任务因此有窗口
    /// 在 dispose 之前自己收尾。
    ///
    /// 不要求 tokio 上下文，可在任意 executor 上使用。
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        Cancelled {
            token: self.inner.cancellation.next_token(),
            inner: self.inner.clone(),
            registered: false,
        }
    }

    /// 父级 Context 句柄；根级为 `None`。
    pub fn parent(&self) -> Option<Context> {
        self.inner.parent.clone().map(|inner| Context { inner })
    }

    /// 本层活跃子作用域的 context id 清单。
    ///
    /// 包含尚未 build 的子 Builder 与尚未 drop 的子 Runtime；子作用域停止后
    /// 仍需 drop 才会从清单中消失。
    pub fn children(&self) -> Vec<usize> {
        match self.inner.children.lock() {
            Ok(children) => children.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// 注册并启动绑定到本作用域生命周期的后台任务，返回可控制的任务句柄。
    ///
    /// 任务在停止阶段被排空（见 [`Runtime::stop`] / [`Runtime::stop_with_timeout`]）：
    /// - `Runtime::stop` 会等待全部已注册任务完成；
    /// - `Runtime::stop_with_timeout` 在预算内等待，超时后强制取消并上报
    ///   [`ErrorKind::TaskAborted`]。
    ///
    /// 任务返回 `Err` 时，会以旁路通知方式向本作用域发出 [`TaskFailed`] 事件
    /// （沿父链冒泡）；任务 panic 会被捕获为该事件的同款结局，并在停止排空时以
    /// [`ErrorKind::TaskFailed`] 上报。
    ///
    /// [`TaskFailed`] 的发出有意不受停止标志限制：排空阶段结束的任务即使本层
    /// 已进入 stopping，其失败事件仍会冒泡，失败报告不会因停止时序而丢失。
    ///
    /// 长驻任务应等待 [`Context::cancelled`] 而不是轮询 [`Context::is_stopping`]；
    /// 需要单独取消某个任务时用返回的 [`TaskHandle::abort`]。
    ///
    /// 要求当前线程处于 tokio runtime 上下文；本层进入停止后调用返回
    /// [`ErrorKind::Stopping`]。
    #[cfg(feature = "tokio")]
    pub fn spawn<F>(&self, fut: F) -> Result<TaskHandle, Error>
    where
        F: Future<Output = Result<(), Error>> + Send + 'static,
    {
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::new(Phase::Build, ErrorKind::NoTaskRuntime))?;
        // 停止态检查与 push 必须同在一次持锁内（见 `lock_tasks` 的不变式论证）。
        let mut tasks = self.lock_tasks();
        if self.shutting_down() {
            return Err(Error::new(Phase::Build, ErrorKind::Stopping));
        }
        // 摊销剪除：只在表长跨过阈值时做一次 O(n) 清理，然后按实际长度翻倍，n 次
        // spawn 的累计代价是 O(n)。剪除必须保住「排空仍需上报」的结局（panic 与
        // 排空超时取消）：按 `is_finished()` 盲删会让一条 panic 因为它之后又有人
        // `spawn` 过而被静默丢掉，`stop` 反而报成功。
        if tasks.len() >= self.inner.tasks.compact_at.load(Ordering::Relaxed) {
            tasks.retain(|task| !task.is_finished() || task.needs_drain_report());
            let next = (tasks.len().saturating_mul(2)).max(COMPACT_BASE);
            self.inner.tasks.compact_at.store(next, Ordering::Relaxed);
        }
        let id = self.inner.tasks.next_id.fetch_add(1, Ordering::Relaxed);

        let cell = Arc::new(TaskCell {
            id,
            abort: OnceLock::new(),
            finished: Signal::new(),
            outcome: Mutex::new(None),
        });

        let task_cell = cell.clone();
        let task_ctx = self.clone();
        let join = handle.spawn(async move {
            // 任务体的 panic 由 `catch_unwind` 收口；但**上报路径本身**（`emit_notify`
            // 会跑用户 handler）也在本任务里，它 panic 会让整个 wrapper 在写结局之前
            // 展开，完成信号就永远不会触发——排空与 `TaskHandle::wait` 会一起挂死，
            // 超时排空还会把它误报成 `TaskAborted`。用 Drop 兜住最后一道：只要结局还没
            // 写，就按 panic 落定。
            let _finisher = FinishOnUnwind(task_cell.clone());
            let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(fut)).await;
            match result {
                Ok(Ok(())) => task_cell.finish(TaskOutcome::Completed(None)),
                Ok(Err(err)) => {
                    let _ = task_ctx
                        .emit_notify(TaskFailed {
                            task_id: id,
                            error: Arc::new(err.clone()),
                        })
                        .await;
                    task_cell.finish(TaskOutcome::Completed(Some(err)));
                }
                Err(_panic) => task_cell.finish(TaskOutcome::Panicked),
            }
        });

        // abort 句柄只能在 spawn 之后取到，因此必须在释放任务表锁之前写入：排空
        // 路径可能立刻取到这个 cell 并调用 abort，空 `OnceLock` 会让取消变成静默
        // 无效。句柄本身随即丢弃——任务因此 detach，但完成信号与 abort 句柄已经
        // 足够管理它，不再需要 `JoinHandle`。
        if cell.abort.set(join.abort_handle()).is_err() {
            // cell 刚创建、尚未发布，`abort` 只会被写入一次。
            debug_assert!(false, "abort handle set exactly once before publish");
        }
        drop(join);
        tasks.push(cell.clone());

        Ok(TaskHandle { cell })
    }

    /// 当前已注册且仍活跃的后台任务数量。
    ///
    /// 尚未落定结局的任务（含 `stop` 正在等待排空的那一个）都计入。
    #[cfg(feature = "tokio")]
    pub fn task_count(&self) -> usize {
        // 只统计、不剪除：剪除会销毁尚未上报的结局（panic / 取消），让排空失去
        // 本该上报的错误。表的内存边界由 `spawn` 的剪除负责。
        self.lock_tasks()
            .iter()
            .filter(|task| !task.is_finished())
            .count()
    }

    /// 任务表锁。关键不变式（`spawn` 与 `drain_tasks` 共用此锁）：
    /// `spawn` 的「停止态检查 + push」在同一次持锁内完成，而 `stop_impl` 先
    /// 转入 `Stopping`、之后才逐个取走任务表——因此任何通过检查的任务必在
    /// 被取走之前入表，不存在「停止竞态丢任务」。修改任一侧的持锁顺序前必须先
    /// 推翻此论证。
    #[cfg(feature = "tokio")]
    fn lock_tasks(&self) -> MutexGuard<'_, Vec<Arc<TaskCell>>> {
        self.inner
            .tasks
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 测试用：注册表原长（不经 `task_count` 的过滤），用于验证剪除行为。
    #[cfg(all(feature = "tokio", test))]
    pub(crate) fn registered_task_count(&self) -> usize {
        self.lock_tasks().len()
    }

    /// 测试用：当前注册的取消等待者数量，用于验证等待者是否被摘除。
    #[cfg(test)]
    pub(crate) fn cancellation_waiters(&self) -> usize {
        self.inner.cancellation.waiter_count()
    }

    /// 测试用：任务完成信号上注册的等待者数量上限（验证 `wait()` future 被丢弃后
    /// 是否真的摘掉了自己的注册项）。
    #[cfg(all(feature = "tokio", test))]
    pub(crate) fn pending_task_waiters(&self) -> usize {
        self.lock_tasks()
            .iter()
            .map(|cell| cell.finished.waiter_count())
            .max()
            .unwrap_or(0)
    }

    /// 测试用：读取本层活跃子 Runtime/Builder 的租约计数。
    #[cfg(test)]
    pub(crate) fn child_count(&self) -> u64 {
        self.inner.gate.lease_count() as u64
    }

    /// 串行发出事件，并沿父链向上冒泡。
    pub async fn emit<E: Event>(&self, event: E) -> Result<(), Error> {
        let errors = self.emit_impl(event, false, false).await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors
                .into_iter()
                .next()
                .expect("strict serial emit returns at most one error"))
        }
    }

    /// 并发发出事件，并沿父链向上冒泡。
    pub async fn emit_parallel<E: Event>(&self, event: E) -> Result<(), Error> {
        let errors = self.emit_impl(event, true, false).await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Event, ErrorKind::Multiple(errors)))
        }
    }

    /// 旁路通知：串行执行事件，handler 错误不阻断后续 handler 和父链冒泡。
    ///
    /// 返回所有收集到的 handler 错误，由调用方决定如何记录。
    pub async fn emit_notify<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, false, true).await
    }

    /// 旁路通知：并行执行事件，handler 错误不阻断父链冒泡。
    ///
    /// 返回所有收集到的 handler 错误，由调用方决定如何记录。
    pub async fn emit_notify_parallel<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, true, true).await
    }

    async fn emit_impl<E: Event>(&self, event: E, parallel: bool, notify: bool) -> Vec<Error> {
        // 沿父链借用推进，不做任何 `Arc` 克隆；只有真正要构造 `Context` 交给
        // handler 的那一层才 clone 一次。
        let mut current: Option<&Arc<Data>> = Some(&self.inner);
        let mut all_errors = Vec::new();

        while let Some(inner) = current {
            let handlers = inner.event_handlers_for::<E>();
            if handlers.is_empty() {
                // 绝大多数层没有该事件类型的 handler，直接跳过。
                current = inner.parent.as_ref();
                continue;
            }
            let ctx = Context {
                inner: Arc::clone(inner),
            };

            let mut layer_errors = Vec::new();
            let mut bail = false;

            if parallel {
                let results = futures::future::join_all(
                    handlers.iter().map(|handler| handler.call(&event, &ctx)),
                )
                .await;

                for result in results {
                    match result {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => bail = true,
                        Err(err) => layer_errors.push(err),
                    }
                }
            } else if notify {
                for handler in handlers {
                    match handler.call(&event, &ctx).await {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => {
                            bail = true;
                            break;
                        }
                        Err(err) => layer_errors.push(err),
                    }
                }
            } else {
                // 严格串行：保留旧语义，第一个错误立即停止。
                for handler in handlers {
                    match handler.call(&event, &ctx).await {
                        Ok(EventControl::Continue) => {}
                        Ok(EventControl::Bail) => return all_errors,
                        Err(err) => {
                            all_errors.push(err);
                            return all_errors;
                        }
                    }
                }
            }

            // 严格并行模式：错误会聚合返回，不再继续向父层冒泡。
            if !notify && parallel && !layer_errors.is_empty() {
                all_errors.extend(layer_errors);
                return all_errors;
            }

            all_errors.extend(layer_errors);

            if bail {
                return all_errors;
            }

            current = inner.parent.as_ref();
        }

        all_errors
    }
}

/// 后台任务失败事件。
///
/// 由 [`Context::spawn`] 的任务在返回 `Err` 时以旁路通知发出，沿父链冒泡；
/// handler 错误不会反向影响任务。
#[derive(Debug)]
pub struct TaskFailed {
    /// 任务 id（[`TaskHandle::id`]，作用域内唯一）。
    pub task_id: u64,
    /// 任务返回的错误。
    pub error: Arc<Error>,
}

/// 停止请求句柄；由 [`Runtime::stop_handle`] 显式分发。
///
/// 只负责「请求」：唤醒本层 [`Context::cancelled`] 的全部等待者并置位请求标记。
/// 真正执行关闭（插件 `stop`、任务排空、dispose）的仍然是持有 `Runtime` 的 owner
/// ——请求不是关闭，也不会让本层跳过子作用域租约检查。
///
/// `Clone` 且可跨任务持有，适合信号处理、管理端点、测试超时兜底等「就近发起
/// 请求」的位置。刻意不放在 `Context` 上：那等于给每个插件环境权限，而「谁能停」
/// 应当是 owner 显式授出的能力。
#[derive(Clone)]
pub struct StopHandle {
    inner: Arc<Data>,
}

impl StopHandle {
    /// 请求本层停止。幂等；已请求或已进入停止流程时为 no-op。
    ///
    /// 不会把 [`Context::is_stopping`] 翻成 `true`，也不会让 `scope` / `spawn`
    /// 开始拒绝——那两件事只在真正进入清理（`Stopping`）后才发生。
    pub fn request_stop(&self) {
        self.inner.request_stop();
    }

    /// 是否收到过显式停止请求。
    pub fn is_stop_requested(&self) -> bool {
        self.inner.is_stop_requested()
    }

    /// 等待停止信号；语义与 [`Context::cancelled`] 相同。
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + 'static {
        Cancelled {
            token: self.inner.cancellation.next_token(),
            inner: self.inner.clone(),
            registered: false,
        }
    }
}

/// 后台任务句柄；[`Context::spawn`] 的返回值。
///
/// `Clone` 共享同一任务。退出作用域或丢弃句柄都**不会**取消任务——取消只能显式
/// [`TaskHandle::abort`]，或者由 `Runtime::stop` 的排空收口。
#[cfg(feature = "tokio")]
#[derive(Clone)]
pub struct TaskHandle {
    cell: Arc<TaskCell>,
}

#[cfg(feature = "tokio")]
impl TaskHandle {
    /// 任务 id（本作用域内唯一，从 0 递增）。
    pub fn id(&self) -> u64 {
        self.cell.id
    }

    /// 结局是否已落定（正常结束、panic 或被取消）。
    pub fn is_finished(&self) -> bool {
        self.cell.is_finished()
    }

    /// 主动取消该任务。幂等。
    ///
    /// 这是 owner 意图，因此**不计入** `stop` 的停止错误——与排空预算耗尽触发的
    /// [`ErrorKind::TaskAborted`] 明确区分。
    pub fn abort(&self) {
        self.cell.abort(true);
    }

    /// 等待任务结局。
    ///
    /// - 正常结束：`Ok(())`；
    /// - 任务返回 `Err`：原样返回该错误（`TaskFailed` 事件同时也已发出）；
    /// - panic：[`ErrorKind::TaskFailed`]；
    /// - 被取消（不管是 owner `abort` 还是排空超时）：`Ok(())`——取消是请求，不是失败。
    pub async fn wait(&self) -> Result<(), Error> {
        let cell = self.cell.clone();
        cell.finished.wait().await;
        match cell.outcome() {
            TaskOutcome::Completed(None) => Ok(()),
            TaskOutcome::Completed(Some(err)) => Err(err),
            TaskOutcome::Panicked => Err(Error::new(
                Phase::Stop,
                ErrorKind::TaskFailed { task_id: cell.id },
            )),
            TaskOutcome::AbortedByOwner | TaskOutcome::AbortedByTimeout => Ok(()),
        }
    }
}

#[cfg(feature = "tokio")]
impl std::fmt::Debug for TaskHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("task_id", &self.cell.id)
            .field("finished", &self.cell.is_finished())
            .finish()
    }
}

/// `stop` 的可续跑进度。
///
/// 被丢弃的 stop future 会在 `Stopping` 态留下这个进度，重入时从这里继续，
/// 因此每个已进入的插件与 dispose hook 至少被处理一次，不会出现「清理漏项却
/// 报成功」。代价是插件 `stop` 需要能吃下一次中断后重入。
#[derive(Debug, Clone, Copy)]
struct StopProgress {
    /// `started_plugins` 中从末尾起尚未回收的项数。
    plugins: usize,
    /// `dispose` 中下一个待执行的下标。
    dispose: usize,
}

/// 生命周期唯一所有者；不 `Clone`，`#[must_use]`。
///
/// 生命周期状态不存在本地副本：真值源是 `Gate` 里 `Mutex<GateState>` 保护的
/// `{ lifecycle, leases }`。本地 bool 镜像一旦存在，就会在状态被 owner 之外的
/// 路径推进时变成陈旧值——那正是「启动失败被记成已启动」的同一形态。
#[must_use]
pub struct Runtime {
    ctx: Context,
    plugins: Vec<PluginRecord>,
    ready: Vec<ReadyHook>,
    dispose: Vec<DisposeHook>,
    /// 启动流程中已经进入的插件索引，`stop` 逆序回收。这是 owner 的执行资源，
    /// 不参与「当前处于什么状态」的判断。
    started_plugins: Vec<usize>,
    /// `stop` 的可续跑进度；`None` 表示尚未进入清理。
    stop_progress: Option<StopProgress>,
    /// 首次启动失败的聚合错误：供 [`Runtime::start_error`] 查询，并作为重入
    /// 错误的 `source`。`Error` 可 `Clone`（`source` 是 `Arc`），因此这份副本与
    /// 交给调用方的那份共享同一条错误链。
    start_error: Option<Error>,
    /// `stop` 累积的错误，跨重入保留，进入 `Stopped` 时随返回值交出。
    stop_errors: Vec<Error>,
    /// `build()` 一次算出的启动调度；`start` 取用后清空。
    schedule: Schedule,
    /// 是否处于「已进入启动流程但尚未到 `Stopped`」。只服务 `Drop` 诊断，不参与
    /// 状态判定，因此是单原子读写、不拿 `Gate` 锁。
    active: AtomicBool,
    /// 私有租约必须作为最后一个字段声明，确保在插件字段析构之后归还父计数。
    #[allow(dead_code)]
    _lease: Option<ScopeLease>,
}

impl Runtime {
    /// 获取只读 `Context` 句柄。
    pub fn handle(&self) -> Context {
        self.ctx.clone()
    }

    /// 取一个可 `Clone` 的停止请求句柄。
    ///
    /// 句柄只携带「请求停止」的能力（见 [`StopHandle`]），关闭仍由本 `Runtime`
    /// 执行。适合交给信号处理、管理端点或测试兜底这些无法持有 `Runtime` 的位置。
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            inner: self.ctx.inner.clone(),
        }
    }

    /// 当前生命周期状态。真值源是 `Gate`，本地不保留副本。
    fn lifecycle(&self) -> Lifecycle {
        self.ctx.inner.gate.lifecycle()
    }

    /// 更新生命周期状态（`Gate` 在锁内同时处理 `stopping` 单向闩）。
    fn set_lifecycle(&self, next: Lifecycle) {
        self.ctx.inner.gate.set_lifecycle(next);
    }

    /// 首次启动失败的聚合错误；从未失败时为 `None`。
    ///
    /// 返回的是原始聚合错误（`ErrorKind::Multiple`），可直接遍历子错误。
    pub fn start_error(&self) -> Option<&Error> {
        self.start_error.as_ref()
    }

    /// 构造「上次启动未成功完成」的错误，根因挂在 `source` 链上。
    ///
    /// `Failed` 只由 `start_with` 在写入 `start_error` 之后设置，且生命周期是一个
    /// 普通枚举、不会被破坏成非法值，因此这里必有根因——不再需要「无根因兜底」。
    fn start_failed_error(&self) -> Error {
        let first = self
            .start_error
            .as_ref()
            .expect("Failed implies start_error recorded");
        Error::with_source(Phase::Start, ErrorKind::StartFailed, first.clone())
    }

    async fn start_with(&mut self, serial: bool) -> Result<(), Error> {
        match self.lifecycle() {
            // 已经停稳：start-after-stop 是 no-op。
            Lifecycle::Stopped => return Ok(()),
            // 停止流程进行中：不接受启动。
            Lifecycle::Stopping => return Ok(()),
            // 启动成功后的重复调用是幂等 no-op。
            Lifecycle::Running => return Ok(()),
            // 上一次启动被中途丢弃：部分插件已启动但流程未完成。
            // 不谎报成功，也不续跑——部分启动的状态不该被「接着启动」，
            // 调用方应当 `stop` 回收后重建。
            Lifecycle::Starting => return Err(Error::new(Phase::Start, ErrorKind::StartFailed)),
            // 启动失败后重入：明确报错，不再伪装成成功。
            Lifecycle::Failed => return Err(self.start_failed_error()),
            Lifecycle::Built => {}
        }

        // 调度在 `build()` 已算好并随 Runtime 保存；`start` 直接取用，没有失败分支，
        // 也不存在「已通过校验的 Runtime 在 start 时调度失败」的不可达错误路径。
        let schedule = std::mem::take(&mut self.schedule);
        self.active.store(true, Ordering::Release);
        self.set_lifecycle(Lifecycle::Starting);

        let mut errors = Vec::new();
        if serial {
            for &index in &schedule.order {
                self.started_plugins.push(index);
                let record = &self.plugins[index];
                if let Err(err) = record.plugin.start(&self.ctx).await {
                    errors.push(err.into_phase(Phase::Start, Some(record.name())));
                    break;
                }
            }
        } else {
            // 分层并行：拓扑同层无依赖边，可并发启动。
            for layer in schedule.layers {
                self.started_plugins.extend(layer.iter().copied());
                let layer_plugins: Vec<(usize, &dyn Plugin)> = layer
                    .iter()
                    .map(|&index| (index, self.plugins[index].plugin.as_ref()))
                    .collect();
                let ctx = &self.ctx;
                let results = futures::future::join_all(
                    layer_plugins.iter().map(|(_, plugin)| plugin.start(ctx)),
                )
                .await;

                for ((index, _plugin), result) in layer_plugins.into_iter().zip(results) {
                    if let Err(err) = result {
                        errors.push(err.into_phase(Phase::Start, Some(self.plugins[index].name())));
                    }
                }

                if !errors.is_empty() {
                    break;
                }
            }
        }

        if errors.is_empty() {
            let ctx = self.ctx.clone();
            for hook in &mut self.ready {
                if let Err(err) = hook.call(&ctx).await {
                    errors.push(err.into_phase(Phase::Ready, None));
                    break;
                }
            }
        }

        if errors.is_empty() {
            self.set_lifecycle(Lifecycle::Running);
            Ok(())
        } else {
            // 首次失败按原样返回聚合错误，同时留一份供 `start_error()` 查询与
            // 重入挂 `source`（`Error: Clone` 让两份共享同一条链）。
            // 只有重入才包装成 `StartFailed`。
            let aggregate = Error::new(Phase::Start, ErrorKind::Multiple(errors));
            self.start_error = Some(aggregate.clone());
            self.set_lifecycle(Lifecycle::Failed);
            Err(aggregate)
        }
    }

    /// 默认分层并行启动。
    pub async fn start(&mut self) -> Result<(), Error> {
        self.start_with(false).await
    }

    /// 保留旧语义的串行启动。
    pub async fn start_serial(&mut self) -> Result<(), Error> {
        self.start_with(true).await
    }

    /// 异步停止。
    ///
    /// 父/自身 `Runtime::stop` 只接受本层没有活跃子 Runtime；若仍有活跃子，
    /// 返回 `ActiveScopes`（含子作用域 id 清单），且不进入停止状态。
    ///
    /// 已注册后台任务的排空不设超时，等待全部任务自然完成。
    pub async fn stop(&mut self) -> Result<(), Error> {
        self.stop_impl(None).await
    }

    /// 带总预算的优雅停止。
    ///
    /// 与 [`Runtime::stop`] 相同，但 `Context::spawn` 注册的任务排空共享
    /// `timeout` 预算：预算耗尽仍未完成的任务会被强制取消，并以
    /// `ErrorKind::TaskAborted` 计入聚合错误。
    ///
    /// 仅在启用 `tokio` feature 且使用过 `Context::spawn` 时有实际差异；
    /// 插件 `stop` 与 dispose 回调本身不受该预算约束。
    ///
    /// 超时排空使用 `tokio::time` 定时器，要求当前 runtime 启用了 time
    /// driver（标准 `new_multi_thread` / 显式 `enable_time` 的
    /// `new_current_thread` 均满足）。
    pub async fn stop_with_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        self.stop_impl(Some(timeout)).await
    }

    async fn stop_impl(&mut self, task_drain_timeout: Option<Duration>) -> Result<(), Error> {
        // 入口状态机。`Stopping` 只可能来自上一次 stop future 被丢弃（owner 唯一，
        // `&mut self` 排除了并发 stop）：此时租约检查已经通过、清理已有进度，
        // 直接续跑，不重新检查租约、也不重复转换状态。
        match self.lifecycle() {
            Lifecycle::Stopped => return Ok(()),
            Lifecycle::Stopping => {}
            _ => {
                self.enter_stopping()?;
                // 首次进入：固定待回收范围。续跑时不动这份进度。
                self.stop_progress = Some(StopProgress {
                    plugins: self.started_plugins.len(),
                    dispose: 0,
                });
            }
        }

        // 错误累积在字段里而非局部变量：中断后重入时，早先一轮已记录的错误不会
        // 随局部变量消失（否则可能在最终返回 `Ok` 时被静默吞掉）。
        let mut progress = self
            .stop_progress
            .expect("Stopping implies stop_progress is initialized");

        // 插件逆序回收。游标只在 await 真正返回后才推进并写回，因此 stop future
        // 被丢弃时该项保留、下次重入重试，而不是被静默跳过。
        while progress.plugins > 0 {
            let index = self.started_plugins[progress.plugins - 1];
            let record = &self.plugins[index];
            if let Err(err) = record.plugin.stop(&self.ctx).await {
                self.stop_errors
                    .push(err.into_phase(Phase::Stop, Some(record.name())));
            }
            progress.plugins -= 1;
            self.stop_progress = Some(progress);
        }

        #[cfg(feature = "tokio")]
        self.drain_tasks(task_drain_timeout).await;

        #[cfg(not(feature = "tokio"))]
        let _ = task_drain_timeout;

        // dispose 保持注册序，同样是完成一个才推进游标。
        let ctx = self.ctx.clone();
        while progress.dispose < self.dispose.len() {
            let next = progress.dispose;
            if let Err(err) = self.dispose[next].call(&ctx).await {
                self.stop_errors.push(err.into_phase(Phase::Dispose, None));
            }
            progress.dispose += 1;
            self.stop_progress = Some(progress);
        }

        // 清理全部走完才进入终态：中途丢弃 stop future 时状态留在 `Stopping`，
        // 下次调用从这里继续，而不是被当作已完成。
        self.stop_progress = None;
        self.started_plugins.clear();
        self.set_lifecycle(Lifecycle::Stopped);
        self.active.store(false, Ordering::Release);

        let errors = std::mem::take(&mut self.stop_errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Stop, ErrorKind::Multiple(errors)))
        }
    }

    /// 转入 `Stopping`：在 `Gate` 的临界区内一次完成「确认无活跃子作用域 + 状态迁移」。
    fn enter_stopping(&mut self) -> Result<(), Error> {
        match self.ctx.inner.gate.enter_stopping() {
            Ok(()) => {
                // 进入关闭流程即广播，且必须早于插件 `stop` 与任务排空：长驻任务
                // 因此有窗口在 dispose 之前自己收尾，而不是等排空来踢。`stopping`
                // 闩已在 `enter_stopping` 内先于本行置位（Release）。
                self.ctx.inner.cancellation.fire();
                Ok(())
            }
            Err(count) => Err(Error::new(
                Phase::Stop,
                ErrorKind::ActiveScopes {
                    count: count as u64,
                    ids: self.ctx.children(),
                },
            )),
        }
    }

    /// 排空本层登记的后台任务：插件 stop 之后、dispose 之前执行。
    ///
    /// 每个 cell 都是「先 await 结局、再取出」：被丢弃的 stop future 因此不会把
    /// 在飞任务从表里摘走，重入会重新看到它——而完成信号是电平触发的，已结束的
    /// 立即返回，重入不会空转。这替换了旧实现把 `JoinHandle` 挂在 `Runtime` 字段上
    /// 的整套补丁。
    #[cfg(feature = "tokio")]
    async fn drain_tasks(&mut self, task_drain_timeout: Option<Duration>) {
        // `checked_add`：`Instant + Duration` 在越过可表示范围时会 panic，而
        // `stop_with_timeout(Duration::MAX)` 是合法输入。溢出按「实际无限预算」
        // 处理（等价于不设超时），不 panic。
        let deadline = task_drain_timeout.and_then(|budget| Instant::now().checked_add(budget));

        loop {
            // 锁只在取表尾这一瞬间持有。这里不能写成
            // `while let Some(cell) = ...pop()`：scrutinee 的 `MutexGuard` 临时值会
            // 活到整个循环体结束，await 期间仍持锁，而任务体自己要用同一把锁——死锁。
            let Some(cell) = self.ctx.lock_tasks().last().cloned() else {
                break;
            };
            let id = cell.id;

            match deadline {
                None => cell.finished.wait().await,
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    // 用 `timeout` 而不是 `select(Box::pin(sleep), Box::pin(wait))`：
                    // 语义相同，且省掉每个任务两次堆分配。任务恰好同时结束时 `timeout`
                    // 返回 `Ok`，下面读到的就是它的真实结局，不会被算成超时。
                    if tokio::time::timeout(remaining, cell.finished.wait())
                        .await
                        .is_err()
                    {
                        cell.abort(false);
                    }
                }
            }

            // 结局已定，从表中取出。若上一行被取消，这一行不执行，cell 仍在表里，
            // 重入会重新处理（此时完成信号已触发，立即返回）。
            {
                let mut tasks = self.ctx.lock_tasks();
                // 排空期间 `spawn` 已被拒绝，表尾不可能被并发替换。若将来放开
                // 「Stopping 期间可 spawn」，必须先改掉这里的取出方式。
                debug_assert!(matches!(tasks.last(), Some(tail) if Arc::ptr_eq(tail, &cell)));
                tasks.pop();
            }

            // 从这里到本次循环结束没有 await 点，因此「取出 - 判断 - 上报」相对
            // 取消是原子的：被丢弃的 stop future 不会停在这中间造成漏报或重报。
            // 取消来源直接取自结局本身，不需要额外的旁路状态。
            match cell
                .outcome_kind()
                .expect("finished implies outcome written")
            {
                // owner 主动取消不是失败，不计入停止错误。
                TaskOutcomeKind::AbortedByOwner => {}
                // 任务体返回的错误已在完成时通过 `TaskFailed` 事件上报过一次，
                // 排空不重复上报。
                TaskOutcomeKind::Completed => {}
                TaskOutcomeKind::AbortedByTimeout => {
                    self.stop_errors.push(Error::new(
                        Phase::Stop,
                        ErrorKind::TaskAborted { task_id: id },
                    ));
                }
                TaskOutcomeKind::Panicked => {
                    self.stop_errors.push(Error::new(
                        Phase::Stop,
                        ErrorKind::TaskFailed { task_id: id },
                    ));
                }
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // 不做异步清理。租约释放由最后一个字段 `ScopeLease` 在字段析构阶段完成。
        //
        // 生命周期护栏：曾经进入过启动流程却没走到 `Stopped`，说明插件资源与
        // dispose hooks 都没有回收。debug 构建下硬失败，让这类泄漏在开发/测试期
        // 立刻暴露；release 下静默——正确性不应依赖这条诊断。
        //
        // `panicking()` 守卫：调用方可能因别的原因在持有本值时 panic，此时栈正在
        // 展开。Drop 里再 panic 会二次 panic 并 abort，连原始 panic 信息一起吞掉，
        // 因此展开路径只放行。
        debug_assert!(
            std::thread::panicking() || !self.active.load(Ordering::Acquire),
            "Runtime dropped without stop(): plugin resources and dispose hooks \
             were not reclaimed"
        );
    }
}
