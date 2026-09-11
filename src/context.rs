//! 三段式上下文模型：Builder / Context / Runtime。

use std::any::TypeId;
use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
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
use crate::id::{Blockers, ScopeId, TaskId};
#[cfg(feature = "tokio")]
use crate::notify::NotifyHub;
use crate::notify::{Backlog, Receipt};
use crate::service::{NameMap, TypeMap};
use crate::waiters::{WaitId, Waiters};
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
    /// owner 已提交停止：拒绝新 `scope` / `spawn`，取消信号已广播，正在等待子
    /// 作用域租约归零。**清理尚未开始**。
    ///
    /// 提交不可逆：`stop` 一旦被调用就进入本态，即使因活跃子作用域被阻塞也不会
    /// 退回——只返回 [`StopOutcome::Blocked`]，等子回收后重试即可续跑。
    Closing,
    /// 租约已归零，正在清理（插件 `stop`、任务排空、dispose）。可续跑：被丢弃的
    /// `stop` future 会留在此态，重入继续。
    Stopping,
    /// 终态。
    Stopped,
}

impl Lifecycle {
    /// 拒绝新子作用域 / 新任务的判定（热路径闩的语义）。
    const fn is_shutting_down(self) -> bool {
        matches!(self, Self::Closing | Self::Stopping | Self::Stopped)
    }
}

/// 作用域核心：生命周期状态、租约计数、请求位与遗弃位，以及两路电平信号。
///
/// 生命周期此前是 `Data` 上的**六份并行表示**：阶段枚举、`shutting_down` /
/// `stop_requested` / `abandoned` 三个原子，外加 `cancellation` / `settled` 两个信号。
/// 分成多份会在快路径上产生**撕裂读**——`poll_settled` 先读一次阶段枚举（拿锁）、
/// 再读一次 `abandoned`（原子），两次之间状态可能已推进。
///
/// 现在收敛为「**一个锁内权威快照 + 一个派生 tag**」：
/// - `inner` 是唯一真值源，所有迁移都在它的临界区内完成；
/// - `tag` 在同一次持锁内由 `commit` 刷新，供热路径与免锁快路径单次读取。
///
/// 判定与计数必须一起成立：`Context::scope()` 的「父未停止 + 租约加一」，以及
/// `Runtime::stop` 的「租约为零 + 转入 `Stopping`」。用两个独立原子会在它们之间
/// 裂开「父停止的同时长出子作用域」的窗口，因此二者同在 `inner` 的临界区内。
struct ScopeCore {
    inner: Mutex<ScopeInner>,
    /// 派生标志位（`SHUTTING` / `STOPPED` / `REQUESTED` / `ABANDONED`）。
    ///
    /// 只是派生缓存，不是第二真值源：每次迁移都在 `inner` 的同一临界区内重算并
    /// `Release` 写入，读取侧 `Acquire` 一次即得到自洽快照。
    tag: AtomicU64,
    /// 本层「开始收尾」信号：owner 提交停止或被显式请求时触发。
    cancellation: Waiters,
    /// 本层「已停稳」信号：进入 `Stopped` 或被拥有者遗弃时触发。
    ///
    /// 与 `cancellation` 是两个层级的电平：前者是「请你收尾」，后者是「资源已经
    /// 回收完」。
    settled: Waiters,
}

struct ScopeInner {
    lifecycle: Lifecycle,
    /// 是否收到过显式停止请求（[`StopHandle::request_stop`]）。
    ///
    /// 刻意与 `lifecycle` 分开：请求不等于已进入清理。它是 `cancellation` 的另一个
    /// 触发源，但**不**让 `is_stopping()` 为真，也不拒绝 `scope` / `spawn`。
    requested: bool,
    /// 拥有者未走完停止流程就丢弃了 `Runtime`（`Closing`/`Stopping` 等中途态）。
    ///
    /// `settled` 被唤醒后据此区分「走完流程」与「被遗弃」：没有它，`Context::stopped`
    /// 在 owner 直接 drop 时会永久挂起。
    abandoned: bool,
    /// 活跃子作用域 id 集合。它同时是「阻塞清单」与「租约计数」：`ScopeId` 进程内
    /// 唯一，一个 id 至多一个租约，因此集合大小恒等于活跃租约数，无需另存计数。
    ///
    /// 用 `BTreeSet` 而不是 `Vec`：`acquire`/`release` 是 O(log k) 而非 `Vec::retain`
    /// 的 O(k)——后者在「一个父作用域下挂很多会话子作用域并批量关闭」时是 O(k²)。
    /// 迭代顺序恒为 `ScopeId` 升序（优于 `Vec` 在并发 acquire 下的乱序），
    /// `Context::children()` 因此返回确定顺序。
    children: BTreeSet<ScopeId>,
}

// `ScopeCore.tag` 的位定义。
const SHUTTING: u64 = 1 << 0;
const STOPPED: u64 = 1 << 1;
const REQUESTED: u64 = 1 << 2;
const ABANDONED: u64 = 1 << 3;

impl ScopeInner {
    /// 由权威状态重算派生标志位；只在持锁期间调用。
    fn derive_tag(&self) -> u64 {
        let mut tag = 0;
        if self.lifecycle.is_shutting_down() {
            tag |= SHUTTING;
        }
        if matches!(self.lifecycle, Lifecycle::Stopped) {
            tag |= STOPPED;
        }
        if self.requested {
            tag |= REQUESTED;
        }
        if self.abandoned {
            tag |= ABANDONED;
        }
        tag
    }
}

impl ScopeCore {
    fn new() -> Self {
        Self {
            inner: Mutex::new(ScopeInner {
                lifecycle: Lifecycle::Built,
                requested: false,
                abandoned: false,
                children: BTreeSet::new(),
            }),
            tag: AtomicU64::new(0),
            cancellation: Waiters::new(),
            settled: Waiters::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, ScopeInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 所有状态迁移的唯一出口：改 `inner` 后在同一次持锁内刷新派生 tag。
    fn commit<R>(&self, mutate: impl FnOnce(&mut ScopeInner) -> R) -> R {
        let mut guard = self.lock();
        let result = mutate(&mut guard);
        self.tag.store(guard.derive_tag(), Ordering::Release);
        result
    }

    fn lifecycle(&self) -> Lifecycle {
        self.lock().lifecycle
    }

    /// 热路径判定：本层是否已提交关闭（不再接受新工作）。单原子读，不拿锁。
    fn is_stopping(&self) -> bool {
        self.tag.load(Ordering::Acquire) & SHUTTING != 0
    }

    /// 是否已进入 `Stopped` 终态。单原子读，不拿锁。
    fn is_stopped(&self) -> bool {
        self.tag.load(Ordering::Acquire) & STOPPED != 0
    }

    /// 是否收到过显式停止请求。单原子读。
    fn is_stop_requested(&self) -> bool {
        self.tag.load(Ordering::Acquire) & REQUESTED != 0
    }

    /// 是否被拥有者遗弃。单原子读。
    fn is_abandoned(&self) -> bool {
        self.tag.load(Ordering::Acquire) & ABANDONED != 0
    }

    /// 是否已有停稳结局（走完流程或被遗弃）。单原子读——`poll_settled` 靠它一次
    /// 拿到自洽快照，不再分别读 `lifecycle` 与 `abandoned`。
    fn is_settled(&self) -> bool {
        self.tag.load(Ordering::Acquire) & (STOPPED | ABANDONED) != 0
    }

    /// 活跃子作用域 id 清单，恒为 `ScopeId` 升序。
    fn children(&self) -> Vec<ScopeId> {
        self.lock().children.iter().copied().collect()
    }

    #[cfg(test)]
    fn lease_count(&self) -> usize {
        self.lock().children.len()
    }

    fn set_lifecycle(&self, next: Lifecycle) {
        self.commit(|inner| inner.lifecycle = next);
    }

    /// 父未停止则登记一个子作用域租约，并把子 id 记入阻塞集合。返回是否成功；
    /// 失败意味着父已进入关闭流程。
    ///
    /// 判定与登记在同一临界区内完成，并与 `begin_close` / `enter_cleanup`
    /// 的「提交关闭」「租约为零才转入清理」互斥，因此不存在「父停止的同时长出
    /// 子作用域」的窗口。
    fn acquire_lease(&self, child_id: ScopeId) -> bool {
        self.commit(|inner| {
            if inner.lifecycle.is_shutting_down() {
                return false;
            }
            inner.children.insert(child_id);
            true
        })
    }

    /// 归还租约并摘除阻塞集合条目。与 `acquire_lease` 由 `ScopeLease` 的 RAII
    /// 配对保证一一对应；同锁完成，外部观察不到中间态。
    fn release_lease(&self, child_id: ScopeId) {
        self.commit(|inner| {
            inner.children.remove(&child_id);
        });
    }

    /// 提交停止意图：转入 `Closing` 并置单向闩。首次提交返回 `true`；已是
    /// `Closing` / `Stopping` / `Stopped` 时返回 `false`（幂等）。
    ///
    /// 提交即拒绝新的 `scope` / `spawn`，但**不代表清理已经开始**——清理仍要等
    /// 子租约归零（见 [`ScopeCore::enter_cleanup`]）。
    fn begin_close(&self) -> bool {
        self.commit(|inner| {
            if inner.lifecycle.is_shutting_down() {
                return false;
            }
            inner.lifecycle = Lifecycle::Closing;
            true
        })
    }

    /// 在 `Closing` 且租约为零时转入 `Stopping`（真正开始清理）。
    ///
    /// 仍有租约时返回阻塞方清单且不改动状态。`Stopping` / `Stopped` 是**幂等
    /// 兜底**：重入续跑并不经过这里——`stop_impl` 入口把 `Stopped` 提前返回、
    /// 把 `Stopping` 直接分流去 poll 自持 future。保留该分支只是为了让单向终态
    /// 不会被误调回退成 `Stopping`，不是因为它承担重入。
    fn enter_cleanup(&self) -> Result<(), Blockers> {
        self.commit(|inner| match inner.lifecycle {
            Lifecycle::Stopping | Lifecycle::Stopped => Ok(()),
            _ if !inner.children.is_empty() => {
                Err(Blockers::new(inner.children.iter().copied().collect()))
            }
            _ => {
                inner.lifecycle = Lifecycle::Stopping;
                Ok(())
            }
        })
    }

    /// 记录显式停止请求并唤醒 `cancelled()` 等待者。幂等。
    ///
    /// 先发布标志位再唤醒：被唤醒的等待者读 [`ScopeCore::is_stop_requested`] 时
    /// 必定已经可见。
    fn request_stop(&self) {
        self.commit(|inner| inner.requested = true);
        self.cancellation.fire_all();
    }

    /// 标记「拥有者未走完停止流程就丢弃了 `Runtime`」，然后投递停稳信号。
    ///
    /// 先置位再唤醒：被唤醒的等待者读 [`ScopeCore::is_abandoned`] 时必定可见。
    fn abandon(&self) {
        self.commit(|inner| inner.abandoned = true);
        self.settled.fire_all();
    }

    /// 投递「已停稳」；进入 `Stopped` 时调用。幂等。
    fn settle(&self) {
        self.settled.fire_all();
    }
}

/// [`Context::cancelled`] / [`StopHandle::cancelled`] 返回的 future。
///
/// 必须自己管好注册项：等待者常常写成
/// `select! { _ = ctx.cancelled() => …, _ = work => … }`，先等到 `work` 之后就不再
/// 关心停止信号了。若把注册项一直留在 `Data` 的等待者列表里，长生命周期作用域的
/// 列表会随「曾等待过取消、但自己先结束」的任务单调增长。
///
/// 类型是公开的（而不是 `impl Future`），因此可以命名、存进结构体字段或放进
/// trait object；`Drop` 时自动注销自己的等待者注册项。
#[must_use = "`cancelled()` 返回的 future 需要被 await 或显式保存才有意义"]
pub struct Cancelled {
    inner: Arc<Data>,
    /// 本 future 在 `Data::cancellation` 里的注册身份；首次 poll 时才分配。
    /// 摘除只按它进行，不会误伤别的等待者。
    wait: Option<WaitId>,
}

impl Future for Cancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        this.inner.poll_cancelled(&mut this.wait, cx.waker())
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(id) = self.wait {
            self.inner.core.cancellation.release(id);
        }
    }
}

/// [`Context::stopped`] 的结局。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// 本层走完了停止流程，进入 `Stopped`；插件、任务、dispose 均已回收。
    Stopped,
    /// 拥有者未走完停止流程就丢弃了 `Runtime`；资源未被回收。
    ///
    /// 有了这个结局，[`Context::stopped`] 的等待者不会因为对方永远不 `stop` 而
    /// 永久挂起。
    Abandoned,
}

/// [`Context::stopped`] 返回的 future。
///
/// 与 [`Cancelled`] 同理，`Drop` 时按自己的注册身份注销等待者：调用方可能用
/// `select!` 先等到别的分支而不再关心停止完成。
#[must_use = "`stopped()` 返回的 future 需要被 await 或显式保存才有意义"]
pub struct Settled {
    inner: Arc<Data>,
    wait: Option<WaitId>,
}

impl Future for Settled {
    type Output = Settlement;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Settlement> {
        let this = self.get_mut();
        this.inner.poll_settled(&mut this.wait, cx.waker())
    }
}

impl Drop for Settled {
    fn drop(&mut self) {
        if let Some(id) = self.wait {
            self.inner.core.settled.release(id);
        }
    }
}

/// 本层**开始关闭**时同步调用的回调。
///
/// 触发时机是生命周期首次从非关闭态提交为 `Closing` 的那一刻——早于插件 `stop`、
/// 任务排空与 dispose。它是「请你收尾」信号（[`Context::cancelled`]）的 push 形态：
/// 不必专门养一个常驻任务去 `await`，因此适合「作用域一关就开始收尾」的场景。
///
/// 刻意是同步的：回调运行在 `begin_close` 之后、`ScopeCore` 锁之外，但仍在 owner
/// `stop()` 的 future 里。签名不留 await 位置，是因为在回调里等待
/// [`Context::stopped`] 等于等待自己造成的关闭完成，会构成自死锁。
///
/// **遗弃路径不触发**：owner 未调用 `stop` 就丢弃 `Runtime` 时只置状态、唤醒 pull
/// 等待者，不执行任何用户代码——`Drop` 可能在栈展开中运行，此时调用户代码会二次
/// panic 并 abort。要保证收尾就依赖 owner 调用 `stop`。
///
/// panic 由框架 `catch_unwind` 兜住并计入停止错误，不会打挂 owner 的 `stop()`。
pub trait CloseHook: Send + Sync + 'static {
    /// 执行回调。回调只获得只读 `Context`；本层此刻已拒绝新的 `scope` / `spawn`。
    fn call(&self, ctx: &Context);
}

impl<F> CloseHook for F
where
    F: Fn(&Context) + Send + Sync + 'static,
{
    fn call(&self, ctx: &Context) {
        (self)(ctx)
    }
}

/// [`Context::on_closing`] 的注册句柄；**丢弃即注销**。
///
/// 装配期注册（`Builder::on_closing` / [`Configurator::on_closing`]）的回调随本层
/// 存续、不返回句柄；运行期注册返回本句柄，供「运行中才建立、之后可能撤下」的组件
/// 控制回调生命周期。
#[must_use = "丢弃句柄会立即注销该关闭回调"]
pub struct CloseHandle {
    inner: Arc<Data>,
    id: usize,
}

impl Drop for CloseHandle {
    fn drop(&mut self) {
        self.inner
            .lock_closing_hooks()
            .hooks
            .retain(|(id, _)| *id != self.id);
    }
}

/// 关闭回调表与派发状态；**同一把锁**管理。
///
/// 这两件事若分属两个同步域，「取快照」与「迁移派发状态」之间会裂开窗口，使同一个
/// 回调既被即时补齐、又被快照各跑一次（两次），或落在窗口里的注册既不补齐也不在
/// 快照（永不触发）。放进同一临界区后，任一注册只可能被判为「未派发」（并入后续
/// 轮次）或「已派发」（锁外自跑一次），次数恒为一。
#[derive(Default)]
struct ClosingHooks {
    hooks: Vec<(usize, Arc<dyn CloseHook>)>,
    dispatch: CloseDispatch,
    /// 已派发过的最大 hook id。hook id 单调递增，用它取「尚未派发」的增量；注销
    /// （[`CloseHandle::drop`]）只是从表中移除条目，不会打乱游标。
    dispatched_upto: Option<usize>,
    /// **链式注册**的回调计数：在关闭钩子执行上下文内触发（任何层），或目标层自身
    /// 已开始派发时注册的那些。层存活期间由普通代码做的动态注册**不计**。
    ///
    /// 口径不能只看目标层派发状态：运行期回调可以向一个尚未开始派发的另一层注册，
    /// 那样就绕过了预算并跨层指数放大。也不能对所有运行期注册一律计费：长生命周期
    /// 层反复注册/注销会误耗尽预算。见 [`MAX_CHAINED_CLOSING_HOOKS`]。
    chained: u32,
    /// 是否已为「链式注册未收敛」记过溢出错误：每层至多一条（轮次触顶与预算/深度
    /// 触顶共用一个出口）。
    cutoff_reported: bool,
}

impl ClosingHooks {
    /// 取走「尚未派发」的回调。游标在**取走时**推进：某个回调随后 panic 也被记为
    /// 已派发（panic 会作为错误记录），不会重跑。
    fn take_pending(&mut self) -> Vec<Arc<dyn CloseHook>> {
        let mut batch = Vec::new();
        let mut upto = self.dispatched_upto;
        for (id, hook) in &self.hooks {
            if Some(*id) > upto {
                batch.push(hook.clone());
                upto = Some(*id);
            }
        }
        self.dispatched_upto = upto;
        batch
    }

    /// 原子收尾：在一次持锁内取走剩余未派发项并置 `Dispatched`。
    ///
    /// 「取走剩余」与「置 `Dispatched`」若分两次加锁，会裂开一个缝隙：期间注册的项
    /// 读到 `Dispatching` 因而不会自跑，又不在已取走的批次里，于是被静默丢弃。
    fn take_final(&mut self) -> Vec<Arc<dyn CloseHook>> {
        self.dispatch = CloseDispatch::Dispatched;
        self.take_pending()
    }

    /// 首次记录「链式注册未收敛」返回 `true`；此后恒 `false`（每层至多一条）。
    fn mark_cutoff(&mut self) -> bool {
        let first = !self.cutoff_reported;
        self.cutoff_reported = true;
        first
    }
}

/// 关闭回调的派发状态。见 [`ClosingHooks`]。
#[derive(Default, Clone, Copy, PartialEq, Eq)]
enum CloseDispatch {
    /// 尚未开始派发。此刻已注册的回调必然被即将到来的快照包含，其后的注册也不会
    /// 被漏掉——两者都不需要即时补齐。
    #[default]
    NotStarted,
    /// 派发进行中。新注册进下一轮，不需要即时补齐。
    Dispatching,
    /// 派发已结束（或本层被遗弃、不会再派发）。此刻注册的回调不会被任何快照覆盖，
    /// 必须即时自跑一次。
    Dispatched,
}

thread_local! {
    /// 关闭回调执行的同步嵌套深度。
    ///
    /// 用线程局部而非层内字段：同步执行天然发生在单线程上，且**跨层**的回调互注册
    /// （子层回调注册到父层、父层再注册回子层）共用这一个计数，因此总嵌套深度有全局
    /// 上界，而不只是每层各 16；同时不受其他线程并发注册的干扰。
    ///
    /// 它同时充当「此刻正在执行关闭钩子」的判据（`> 0`）：[`Context::on_closing`] 据此
    /// 区分**链式注册**（钩子执行中触发）与层存活期间的普通动态注册，只对前者计预算。
    static CLOSING_HOOK_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// [`CLOSING_HOOK_DEPTH`] 的自增/自减守卫：即使 `run_close_hook` 意外展开也把深度
/// 减回（它正常由 `catch_unwind` 兜住，不展开）。
struct ClosingHookDepth;

impl ClosingHookDepth {
    fn enter() -> Self {
        CLOSING_HOOK_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }

    fn current() -> u32 {
        CLOSING_HOOK_DEPTH.with(std::cell::Cell::get)
    }
}

impl Drop for ClosingHookDepth {
    fn drop(&mut self) {
        CLOSING_HOOK_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// 关闭回调链式注册的**收敛上界**，同一个概念的两种形态：
///
/// - 派发进行中：新回调并入下一轮，最多再追这么多轮；
/// - 派发已结束：新回调**即时同步自跑**，最多再嵌套这么多层（否则自增殖回调会
///   栈溢出）。
///
/// 两处共用同一个数，因为回答的是同一个问题——「一次注册引发的新注册，最多再追
/// 多少层」。深度触顶后不再执行并记一条 [`ErrorKind::CloseHookDispatchOverflow`]；
/// 轮次触顶是例外——收尾会把**那一刻已在表内**的剩余项各补跑一次，再记一条。
pub(crate) const MAX_CLOSING_HOOK_ROUNDS: u32 = 16;

/// **链式注册**（关闭钩子执行中触发、或层已开始派发后注册）的回调总数上限，按层计。
///
/// 轮次与嵌套深度各自封顶，只约束「一条链有多深、多少轮」；一个每次被调用都注册
/// `k` 个自身副本的回调，会让待注册集合按 `k^轮次` 膨胀（`k=2`、16 轮即约 6.5 万，
/// `k=10` 等价挂死）。这里再加一道**宽度**上界：超过后不再登记、也不执行，记一条
/// [`ErrorKind::CloseHookDispatchOverflow`] 收口。
///
/// 计数口径是「链式」而非「所有运行期注册」：只看目标层派发状态会被「向尚未开始
/// 派发的另一层注册」绕过并跨层放大；而把所有运行期注册都计入，又会让长生命周期层
/// 反复注册/注销时误耗尽预算。因此以「是否发生在关闭钩子执行上下文内」为准。
///
/// 装配期（[`Builder::on_closing`] / [`Configurator::on_closing`]）声明、以及层存活
/// 期间普通代码的动态注册都**不计入**，可任意多——正常用法不受影响。
pub(crate) const MAX_CHAINED_CLOSING_HOOKS: u32 = 4096;

/// 执行单个关闭回调并把 panic 转成错误。
///
/// 绝不让 panic 逃出：这段代码运行在 owner `stop()` 的 future 里，一旦展开会让本层
/// 停在不可逆的 `Closing` 却永远没有清理 future。
fn run_close_hook(hook: &dyn CloseHook, ctx: &Context) -> Option<Error> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.call(ctx)));
    match outcome {
        Ok(()) => None,
        Err(payload) => Some(Error::new(
            Phase::Close,
            ErrorKind::LifecyclePanicked {
                message: panic_message(&*payload),
            },
        )),
    }
}

/// 隔离执行一个生命周期用户单元（插件 `stop` / dispose hook），把 panic 转成结构化
/// 错误。
///
/// 与 [`run_close_hook`] 是同一条策略的异步形态，目的都是让清理 future 体在构造上
/// **不可展开**：用户代码的 panic 一旦逃出，本层会停在不可逆的 `Stopping`，
/// `stop_future` 被 poison，后续插件与全部 dispose 被跳过，重入还会 poll 已 panic
/// 的 future。调用次数与「重入续跑同一调用」的语义不变——`catch_unwind` 只是包在
/// 该次 poll 的外层，是可正常挂起/恢复的组合子。
async fn run_lifecycle_unit<F>(
    phase: Phase,
    plugin: Option<&'static str>,
    unit: F,
) -> Result<(), Error>
where
    F: Future<Output = Result<(), Error>>,
{
    match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(unit)).await {
        Ok(result) => result.map_err(|err| err.into_phase(phase, plugin)),
        Err(payload) => Err(Error::new(
            phase,
            ErrorKind::LifecyclePanicked {
                message: panic_message(&*payload),
            },
        )
        .into_phase(phase, plugin)),
    }
}

/// 从 `catch_unwind` 载荷提取可读消息；提取不到时为 `None`（panic hook 仍会输出）。
///
/// 关闭回调的 panic 与后台任务的 [`PanicInfo`] 共用这一份提取逻辑：支持的载荷
/// 类型（`&'static str` 优先，其次 `String`）只在这里定义一次。
fn panic_message(payload: &(dyn std::any::Any + Send)) -> Option<String> {
    if let Some(text) = payload.downcast_ref::<&'static str>() {
        Some((*text).to_string())
    } else {
        payload.downcast_ref::<String>().cloned()
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
    context_id: ScopeId,
    parent: Option<Arc<Data>>,
    services: ServiceRegistry,
    /// 插件名 -> `plugins` 索引。注册期查重与调度共用这一份索引。
    plugin_index: NameMap<usize>,
    /// 已声明（已认领）的插件名。**唯一性权威**：名字在进入 `apply` 之前就先落这里，
    /// 因为 `apply` 内可以经 `Configurator::plugin` 再注册，是一个可重入窗口。若把
    /// 查重建立在 `plugin_index`（`apply` 成功之后才写入）上，同名嵌套注册会看到
    /// 陈旧表而被接受。与 `plugin_index` 的区别是两件真实不同的事：本集合答「此名
    /// 是否已被占用」，`plugin_index` 答「已安装的插件在哪个下标」。
    declared_names: NameMap<()>,
    /// 装配阶段的可变 handler 列表；`build` 时按事件类型分组进 `handlers_by_type`。
    event_handlers: Vec<Arc<dyn ErasedEventHandler>>,
    /// 冻结后的按事件类型分组表：emit 直接查表，避免每次全表扫描。
    /// 只在 `Runtime` 暴露的 `Context` 上读取，读取前必已冻结。
    handlers_by_type: TypeMap<Vec<Arc<dyn ErasedEventHandler>>>,
    next_subscription_id: usize,
    /// 生命周期状态、租约、请求位、遗弃位与两路信号的唯一归宿。
    core: ScopeCore,
    /// 本层「开始关闭」同步回调表与派发状态。装配期与运行期都可注册，故内部可变。
    ///
    /// 存句柄 id 是为了运行期注销（[`CloseHandle`]）；装配期注册的项 id 只增不减。
    /// 派发状态与表同锁，见 [`ClosingHooks`]。
    closing_hooks: Mutex<ClosingHooks>,
    /// 关闭回调注册 id 分配器。
    closing_hook_seq: AtomicUsize,
    /// 关闭回调 panic 的暂存；清理 future 起手与末尾各并入一次停止错误。
    closing_errors: Mutex<Vec<Error>>,
    /// 本层旁路通知的投递 hub（每订阅者一条有界 FIFO lane）。
    #[cfg(feature = "tokio")]
    notify: NotifyHub,
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
    id: TaskId,
    /// `spawn` 之后才能取到，因此在发布到任务表之前写入（见 `Context::spawn`）。
    abort: OnceLock<tokio::task::AbortHandle>,
    /// 结局落定信号。电平触发，可重复等待。
    finished: Waiters,
    /// 任务结局；先写者胜（正常结束由任务体写，被取消由取消方补写）。
    ///
    /// 「谁取消的」写进结局本身而不是旁边一个独立原子：排空只读一次结局就能正确
    /// 归类，不存在「结局已是取消、来源标记尚未写入」的窗口。
    outcome: Mutex<Option<TaskOutcome>>,
    /// 任务体 `Err` 的 `TaskFailed` 事件**是否已完整送达**。
    ///
    /// 记在旁边的原子而不是改结局：真实失败（`Failed(err)`）必须先于上报写入结局
    /// 且不被覆盖。它是「结局是 `Failed`」与「事件已送达」这两件事的桥——只在
    /// `emit_notify` 正常返回后置位，因此上报被 panic、被排空超时 abort、或宿主
    /// 丢弃 future 等**任何**截断都表现为「`Failed` 而本标记为 false」，排空据此以
    /// [`ErrorKind::TaskFailed`] 兜底。没有它，一次 fire-and-forget 的失败会消失。
    report_settled: AtomicBool,
}

/// 任务 panic 的可读根因。
///
/// panic hook（默认打到 stderr）之外，框架把载荷里的消息也留存下来，供
/// `TaskHandle::wait` 与停止排空两条出口引用；否则两条路径都只剩一个 task id，
/// 排障时拿不到 panic 说了什么。
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Default)]
pub struct PanicInfo {
    /// `panic!` 的载荷消息。
    ///
    /// `&'static str` 与 `String` 载荷会被提取；其他类型（自定义 payload、无载荷）
    /// 为 `None`，此时仍可从 panic hook 的输出拿到信息。
    pub message: Option<String>,
}

#[cfg(feature = "tokio")]
impl PanicInfo {
    /// 从 `catch_unwind` 捕获的载荷提取可读消息。
    fn from_payload(payload: &(dyn std::any::Any + Send)) -> Self {
        Self {
            message: panic_message(payload),
        }
    }
}

/// 后台任务的结局。
///
/// [`TaskHandle::wait`] 直接返回它，停止排空也从它投影上报错误。四种归宿不再被
/// 折叠成 `Result`：「跑完」与「被取消」在类型上分开，取消的成因也保留下来。
#[cfg(feature = "tokio")]
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// 任务体正常跑完。
    Completed,
    /// 任务体返回 `Err`（完成时已经通过 [`TaskFailed`] 事件上报过）。
    Failed(Error),
    /// 任务 panic。**排空必须上报**，因此持有这个结局的 cell 不允许被剪除。
    Panicked(PanicInfo),
    /// 任务被取消；成因见 [`AbortReason`]。取消是请求，不计入停止错误。
    Aborted(AbortReason),
}

/// 任务被取消的成因。
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    /// owner 通过 [`TaskHandle::abort`] 主动取消。
    Owner,
    /// 排空预算耗尽，由框架取消；计入停止错误。
    Timeout,
    /// 宿主直接丢弃了任务 future（runtime 关闭等），框架按取消落定。
    ///
    /// 单列而不是并入 [`AbortReason::Owner`]：它不是 owner 的显式决定，报成
    /// panic 又会让一次 `stop()` 凭空多出 `TaskFailed` 假失败。
    HostShutdown,
}

/// `TaskOutcome` 的判别式视图。用于只判类别、不取错误内容的热路径（剪除扫描、
/// 排空归类），避免克隆可能很深（`ErrorKind::Multiple`）的 `Error`。
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TaskOutcomeKind {
    /// 正常跑完。
    Completed,
    /// 任务体返回 `Err`。与 `Completed` 分开：`Failed` 的 `TaskFailed` 上报可能是
    /// 未送达成（见 `TaskCell::report_settled`），排空要据此兜底；两者合并会让
    /// 「未送达的失败」与「成功」无法区分。
    Failed,
    Panicked,
    AbortedByOwner,
    AbortedByTimeout,
    AbortedByHost,
}

#[cfg(feature = "tokio")]
impl TaskOutcome {
    fn kind(&self) -> TaskOutcomeKind {
        match self {
            Self::Completed => TaskOutcomeKind::Completed,
            Self::Failed(_) => TaskOutcomeKind::Failed,
            Self::Panicked(_) => TaskOutcomeKind::Panicked,
            Self::Aborted(AbortReason::Owner) => TaskOutcomeKind::AbortedByOwner,
            Self::Aborted(AbortReason::Timeout) => TaskOutcomeKind::AbortedByTimeout,
            Self::Aborted(AbortReason::HostShutdown) => TaskOutcomeKind::AbortedByHost,
        }
    }

    /// 结局是否为「成功跑完」；`Failed`、panic、任何取消都是 `false`。
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// 排空上报用的错误投影；不需要上报的结局返回 `None`。
    ///
    /// 只按**结局类别**归类，不含「送达与否」这一维度：任务体返回 `Err` 恒为 `None`，
    /// 因为该失败预期已由 `TaskFailed` 事件出口送达；若送达被超时 abort 截断、上报
    /// 自身 panic 或宿主丢弃 future，则由框架**内部的排空兜底**补报一次
    /// [`ErrorKind::TaskFailed`]，这一情形不体现在本方法上。要取 `Failed` 携带的
    /// 具体错误请直接 match [`TaskOutcome`]。
    ///
    /// panic 投影为 [`ErrorKind::TaskFailed`]、排空超时取消投影为
    /// [`ErrorKind::TaskAborted`]，其余（`Completed` 与 owner/host 取消）按语义返回 `None`。
    pub fn drain_error(&self, task_id: TaskId) -> Option<Error> {
        self.kind().drain_error(task_id)
    }

    /// 把结局折叠回 `Result`：`Failed` 原样返回其错误，panic 折叠为
    /// [`ErrorKind::TaskFailed`]，**任何取消都返回 `Ok(())`**（取消是请求，
    /// 不是失败）。需要区分取消成因时直接 match 本枚举。
    pub fn into_result(self, task_id: TaskId) -> Result<(), Error> {
        match self {
            Self::Completed => Ok(()),
            Self::Failed(err) => Err(err),
            Self::Panicked(_) => Err(Error::new(Phase::Stop, ErrorKind::TaskFailed { task_id })),
            Self::Aborted(_) => Ok(()),
        }
    }
}

#[cfg(feature = "tokio")]
impl TaskOutcomeKind {
    fn drain_error(self, task_id: TaskId) -> Option<Error> {
        match self {
            Self::Panicked => Some(Error::new(Phase::Stop, ErrorKind::TaskFailed { task_id })),
            Self::AbortedByTimeout => {
                Some(Error::new(Phase::Stop, ErrorKind::TaskAborted { task_id }))
            }
            // `Failed` 的排空上报由 `TaskCell::drain_report_error` 依「送达与否」决定，
            // 不走这里；`Completed` 与 owner/host 取消按语义不上报。
            Self::Completed | Self::Failed | Self::AbortedByOwner | Self::AbortedByHost => None,
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
        self.finished.fire_all();
    }

    /// 主动取消：请求 abort、落定结局、唤醒等待者。返回是否由本次取消结束。
    ///
    /// 被 abort 的任务不会再执行任务体的收尾代码，完成信号必须由取消方补发，
    /// 否则排空与 `TaskHandle::wait` 会等一个永远不会触发的信号。
    ///
    /// 只有本次真的落定了结局，才把它算作「这个取消造成的」：任务已经跑完（甚至
    /// panic）之后再 `abort`，不得掩盖原有结局而漏掉 panic 上报。
    fn abort(&self, by_owner: bool) -> bool {
        let outcome = TaskOutcome::Aborted(if by_owner {
            AbortReason::Owner
        } else {
            AbortReason::Timeout
        });
        let initiated = self.set_outcome(outcome);
        if let Some(abort) = self.abort.get() {
            abort.abort();
        }
        self.finished.fire_all();
        initiated
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
    /// 三个来源：panic（没有事件出口）、排空超时取消（只有排空上报）、以及任务体
    /// 返回 `Err` 但 `TaskFailed` 上报**未送达**（被超时 abort 截断、上报自身 panic、
    /// 或宿主丢弃 future）。`spawn` 的剪除必须保住这类 cell，否则一条失败会因为它
    /// 之后又有人 `spawn` 过而被静默丢掉，`stop` 反而报成功。任务体 `Err` 且上报
    /// 已送达、owner 取消按语义不上报，二者都不在此列。
    fn needs_drain_report(&self) -> bool {
        match self.outcome_kind() {
            Some(TaskOutcomeKind::Failed) => !self.report_settled.load(Ordering::Acquire),
            Some(TaskOutcomeKind::Panicked | TaskOutcomeKind::AbortedByTimeout) => true,
            _ => false,
        }
    }

    /// 排空时该任务的错误投影。
    ///
    /// 与 [`TaskOutcomeKind::drain_error`] 的唯一差别是「任务体 `Err` 但上报未送达」
    /// 这一情形：结局里是真实失败，但 `TaskFailed` 事件没送到，必须以
    /// [`ErrorKind::TaskFailed`] 兜住，否则 fire-and-forget 的失败无人知晓。
    fn drain_report_error(&self, id: TaskId) -> Option<Error> {
        let kind = self
            .outcome_kind()
            .expect("finished implies outcome written");
        if matches!(kind, TaskOutcomeKind::Failed) && !self.report_settled.load(Ordering::Acquire) {
            return Some(Error::new(
                Phase::Stop,
                ErrorKind::TaskFailed { task_id: id },
            ));
        }
        kind.drain_error(id)
    }
}

/// 保证任务结局一定有写者、且完成信号一定发出。
///
/// `catch_unwind` 只兜住任务体；结局的**上报路径**（`emit_notify` → 用户 handler）
/// 与任务体同在一个任务里，它 panic 时 wrapper 会在补发完成信号前展开。本守卫兜的
/// 是**完成信号**：没有它，排空会等一个永远不来的信号。
///
/// 判据是「信号是否已发出」而不是「结局是否已写」：失败分支会先写 `Failed` 结局、
/// 后跑上报、最后才发信号，上游 panic 时结局已写但信号未发——若按结局判据提前返回，
/// 排空就会挂死。`finish` 的结局写入本身是先写者胜，因此这里不会用 `Panicked`
/// 覆盖已经记下的真实失败。
///
/// 结局缺失时按成因补写：正在展开（真 panic）记 `Panicked`；宿主直接把 future
/// 丢掉（runtime 关闭、未记录的 abort）记取消——把后者也报成 panic，会让一次
/// `stop()` 凭空多出 `TaskFailed` 假失败。
#[cfg(feature = "tokio")]
struct FinishOnUnwind(Arc<TaskCell>);

#[cfg(feature = "tokio")]
impl Drop for FinishOnUnwind {
    fn drop(&mut self) {
        if self.0.is_finished() {
            return;
        }
        let outcome = if std::thread::panicking() {
            TaskOutcome::Panicked(PanicInfo { message: None })
        } else {
            TaskOutcome::Aborted(AbortReason::HostShutdown)
        };
        self.0.finish(outcome);
    }
}

impl Data {
    fn root() -> Self {
        Self::with_parent(
            None,
            ScopeId::new(NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed)),
        )
    }

    fn child(parent: Arc<Data>, context_id: ScopeId) -> Self {
        Self::with_parent(Some(parent), context_id)
    }

    /// 唯一的字段初始化处：`root` 与 `child` 只差 `parent` 与 id 的来源，各自抄一份
    /// 字段列表意味着新增字段要改两处、漏一处即行为分歧。
    fn with_parent(parent: Option<Arc<Data>>, context_id: ScopeId) -> Self {
        Self {
            context_id,
            parent,
            services: ServiceRegistry::new(),
            plugin_index: NameMap::default(),
            declared_names: NameMap::default(),
            event_handlers: Vec::new(),
            handlers_by_type: TypeMap::default(),
            next_subscription_id: 0,
            core: ScopeCore::new(),
            closing_hooks: Mutex::new(ClosingHooks::default()),
            closing_hook_seq: AtomicUsize::new(0),
            closing_errors: Mutex::new(Vec::new()),
            #[cfg(feature = "tokio")]
            notify: NotifyHub::build(&[]),
            #[cfg(feature = "tokio")]
            tasks: TaskRegistry::default(),
        }
    }

    /// 冻结点：把装配阶段的 handler 列表按事件类型分组。`build` 在唯一一次
    /// `Arc::new(data)` 之前调用；此后 emit 路径只读分组表。
    fn freeze_event_handlers(&mut self) {
        let handlers = std::mem::take(&mut self.event_handlers);
        // 通知 hub 与分组表同源同一次冻结：lane 在冻结点一次建好，此后 `notify`
        // 路径无需任何锁（见 `NotifyHub` 的字段说明）。
        #[cfg(feature = "tokio")]
        {
            self.notify = NotifyHub::build(&handlers);
        }
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
    /// `Waiters` 自身已经不丢唤醒（`begin_close` 在同一锁内置闩，`stop` 随后
    /// `fire_all`，而 `fire_all` 必定唤醒已注册的等待者），所以这里复查闩是一个
    /// **免锁快路径**：已提交关闭的等待者不必去抢 `Waiters` 的锁就能立刻就绪。
    fn poll_cancelled(&self, wait: &mut Option<WaitId>, waker: &Waker) -> Poll<()> {
        if self.core.is_stopping() {
            return Poll::Ready(());
        }
        if self.core.cancellation.is_fired() {
            return Poll::Ready(());
        }
        match *wait {
            Some(id) => self.core.cancellation.update(id, waker),
            None => {
                *wait = self.core.cancellation.register(waker);
                if wait.is_none() {
                    return Poll::Ready(());
                }
            }
        }
        Poll::Pending
    }

    /// 记录显式停止请求并唤醒等待者。幂等。委托给 [`ScopeCore::request_stop`]。
    fn request_stop(&self) {
        self.core.request_stop();
    }

    fn is_stop_requested(&self) -> bool {
        self.core.is_stop_requested()
    }

    /// [`Context::stopped`] 的轮询实现。
    ///
    /// 快路径先查「已有结局」：走完流程（`STOPPED`）与被遗弃（`ABANDONED`）在同一个
    /// tag 里，因此一次原子读就得到自洽快照——不像分成两份时会在两次读之间撕裂。
    /// 命中即立即就绪，否则挂到 `settled` 信号上等待。
    ///
    /// 这里**不**像 [`Data::poll_cancelled`] 那样另查 `settled.is_fired()`：`settled`
    /// 的两个触发点（`settle`、`abandon`）都在**更新 tag 之后**才 `fire_all`，
    /// `Release` 存与 `fire_all` 的 `Release` 存有 program order，因此
    /// 「读者 `Acquire` 到 `fired` ⇒ tag 位已可见」。即使出现读侧陈旧，`register`
    /// 也会在锁内复查 `fired` 并返回 `None`，调用方照样立刻就绪。
    fn poll_settled(&self, wait: &mut Option<WaitId>, waker: &Waker) -> Poll<Settlement> {
        if self.core.is_settled() {
            return Poll::Ready(self.settlement());
        }
        match *wait {
            Some(id) => self.core.settled.update(id, waker),
            None => {
                *wait = self.core.settled.register(waker);
                if wait.is_none() {
                    return Poll::Ready(self.settlement());
                }
            }
        }
        Poll::Pending
    }

    /// 当前「停稳」结局；只在 `settled` 触发后调用才有意义（触发前尚无结局）。
    fn settlement(&self) -> Settlement {
        if self.core.is_abandoned() {
            Settlement::Abandoned
        } else {
            Settlement::Stopped
        }
    }

    /// 投递「已停稳」；进入 `Stopped` 时调用。幂等。委托给 [`ScopeCore::settle`]。
    fn settle(&self) {
        self.core.settle();
    }

    /// 标记「拥有者未走完停止流程就丢弃了 Runtime」并投递停稳信号。
    fn abandon(&self) {
        self.core.abandon();
    }

    fn lock_closing_hooks(&self) -> MutexGuard<'_, ClosingHooks> {
        self.closing_hooks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 标记关闭回调派发已结束。被遗弃的层不会再派发，晚到的注册只能自跑，
    /// 因此遗弃路径也调用它。
    fn settle_closing_dispatch(&self) {
        self.lock_closing_hooks().dispatch = CloseDispatch::Dispatched;
    }

    fn lock_closing_errors(&self) -> MutexGuard<'_, Vec<Error>> {
        self.closing_errors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 取出并清空关闭回调的 panic 记录；清理 future 起手时调用一次。
    fn take_closing_errors(&self) -> Vec<Error> {
        std::mem::take(&mut *self.lock_closing_errors())
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
    PopClosingHook,
}

/// 缓存插件注册时求值的依赖信息。
struct PluginRecord {
    plugin: Box<dyn Plugin>,
    /// 注册时一次取名。`Plugin::name()` 是用户代码，缓存它使启动/停止的错误上报与
    /// 依赖解析都不必在可能正在展开的路径上再调一次用户代码。
    name: &'static str,
    deps: Vec<crate::Dependency>,
    plugin_deps: Vec<crate::PluginDependency>,
}

impl PluginRecord {
    fn name(&self) -> &'static str {
        self.name
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
    child_id: ScopeId,
}

impl Drop for ScopeLease {
    fn drop(&mut self) {
        // 摘除阻塞清单条目与归还计数在同一把 `ScopeCore` 锁内完成，父 `stop` 因此只
        // 会观察到「计数已归零且清单已摘除」这一种完成态。
        self.parent.core.release_lease(self.child_id);
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

    /// `child_id` 由调用方在父 `ScopeCore` 临界区内登记租约时一并给出，这里只负责
    /// 构造数据面与租约载体。
    fn child(parent: Arc<Data>, child_id: ScopeId) -> Self {
        let data = Data::child(parent.clone(), child_id);
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
    pub fn id(&self) -> ScopeId {
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
                    self.data.declared_names.remove(name);
                }
                UndoOp::PopReady => {
                    self.ready.pop();
                }
                UndoOp::PopDispose => {
                    self.dispose.pop();
                }
                UndoOp::PopClosingHook => {
                    self.data.lock_closing_hooks().hooks.pop();
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

        // 重名检查放在求值依赖之前：重名时不必为两个 `Vec` 白白分配。查的是
        // `declared_names`——包含正在 `apply` 中、尚未安装的外层插件名。
        if self.data.declared_names.contains_key(name) {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::PluginNameAlreadyRegistered(name.to_string()),
            ));
        }

        let deps = plugin.dependencies();
        let plugin_deps = plugin.plugin_dependencies();

        let checkpoint = self.checkpoint();

        // 预声明名字：进入 `apply` 之前就占住，使 `apply` 内经 `Configurator::plugin`
        // 的嵌套注册（含自己注册自己）在查重处立即被拒。注册失败或后续回滚由 undo
        // 撤销这次声明。
        self.data.declared_names.insert(name, ());
        self.undo.push(UndoOp::RemovePluginName(name));

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
            name,
            deps,
            plugin_deps,
        });
        self.data.plugin_index.insert(name, index);
        self.undo.push(UndoOp::PopPlugin);
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

    /// 注册一个「本层开始关闭」的同步回调。
    ///
    /// 装配期注册的回调随本层存续，不返回句柄（需要运行期撤下请用
    /// [`Context::on_closing`]）。触发时机见 [`CloseHook`]：早于插件 `stop`、
    /// 任务排空与 dispose，且只在提交关闭那一次触发。
    pub fn on_closing(&mut self, hook: impl CloseHook) -> Result<(), Error> {
        let id = self.data.closing_hook_seq.fetch_add(1, Ordering::Relaxed);
        self.data
            .lock_closing_hooks()
            .hooks
            .push((id, Arc::new(hook)));
        self.undo.push(UndoOp::PopClosingHook);
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
            stop_future: None,
            start_error: None,
            schedule,
            active: AtomicBool::new(false),
            #[cfg(feature = "tokio")]
            notify_workers: Vec::new(),
            lease,
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

    /// 注册「本层开始关闭」的同步回调。
    pub fn on_closing(&mut self, hook: impl CloseHook) -> Result<(), Error> {
        self.builder.on_closing(hook)
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
    /// 该方法会在父 `ScopeCore` 的临界区内登记一个子作用域租约；若父已进入停止，
    /// 返回 `ErrorKind::Stopping`。
    pub fn scope(&self) -> Result<Builder, Error> {
        // 先取 id 再登记：`acquire_lease` 在同一临界区内把 id 记入阻塞清单，
        // 因此父 `stop` 报告的阻塞方与租约计数永远一致。
        let child_id = ScopeId::new(NEXT_CONTEXT_ID.fetch_add(1, Ordering::Relaxed));
        if self.inner.core.acquire_lease(child_id) {
            Ok(Builder::child(self.inner.clone(), child_id))
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

    /// 本层是否已经拒绝新工作（`Closing` / `Stopping` / `Stopped`）。
    fn shutting_down(&self) -> bool {
        self.inner.core.is_stopping()
    }

    /// 本层 Context 的稳定 id（进程内全局唯一）。
    ///
    /// 子作用域的 id 就是父 [`Context::children`] 里列出的那个值，因此它让「框架的
    /// 子作用域清单」与「应用自己的会话表」可以直接对上。
    pub fn id(&self) -> ScopeId {
        self.inner.context_id
    }

    /// 本层是否已提交关闭（`Closing` / `Stopping` / `Stopped`）。
    ///
    /// 语义是「不再接受新工作」：`scope` / `spawn` 从此刻起被拒绝。它**不**表示
    /// 清理已经开始，也不表示已停稳——owner 调用 `stop()` 后即使因活跃子作用域被
    /// 阻塞，本方法也为 `true`（此时清理尚未开始，见 `Runtime::stop` 的
    /// `StopOutcome::Blocked`）。要等清理完成用 [`Context::stopped`]。
    pub fn is_stopping(&self) -> bool {
        self.shutting_down()
    }

    /// 本层是否已走完停止流程、进入 `Stopped` 终态。
    ///
    /// 免锁：`STOPPED` 在 `ScopeCore` 的派生 tag 里，单次原子读即可。
    pub fn is_stopped(&self) -> bool {
        self.inner.core.is_stopped()
    }

    /// 等待本层「开始收尾」。
    ///
    /// 电平触发：本层已提交关闭时立即完成；否则挂起。
    ///
    /// 触发点有三处：owner 调用 `stop()` 提交停止、显式请求
    /// （[`StopHandle::request_stop`]）、以及 owner 丢弃未走完停止流程的 `Runtime`
    /// （此时没有后续清理，资源不再被回收）。即使父 `stop` 因活跃子作用域被阻塞
    /// （`StopOutcome::Blocked`），信号也已经广播——owner 表达停止意图本身就该唤醒
    /// 长驻任务收尾。
    ///
    /// 这是给长驻任务用的等待点，替代「轮询 [`Context::is_stopping`] + 猜间隔」。
    /// 触发**早于**插件 `stop` 与任务排空，长驻任务因此有窗口在 dispose 之前自己
    /// 收尾。要等清理真正完成用 [`Context::stopped`]。
    ///
    /// 不要求 tokio 上下文，可在任意 executor 上使用。
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            inner: self.inner.clone(),
            wait: None,
        }
    }

    /// 等待本层「已停稳」（进入 `Stopped`）。
    ///
    /// 与 [`Context::cancelled`] 是不同层级的电平：`cancelled` 在 owner 提交停止
    /// 时触发，本方法等到清理全部完成、资源已回收。非 owner 的组件用它等停止完成，
    /// 不必轮询 `is_stopping`。
    ///
    /// 结局是 [`Settlement`]：正常走完为 `Stopped`；拥有者在半途丢弃 `Runtime`
    /// 则为 `Abandoned`，后者保证等待者不会永久挂起。
    ///
    /// 不要求 tokio 上下文，可在任意 executor 上使用。
    pub fn stopped(&self) -> Settled {
        Settled {
            inner: self.inner.clone(),
            wait: None,
        }
    }

    /// 注册「本层**开始关闭**时同步调用」的回调，返回丢弃即注销的句柄。
    ///
    /// 触发时机早于插件 `stop`、任务排空与 dispose，是 owner 提交停止（`begin_close`）
    /// 的那一刻——正是 [`Context::cancelled`] 的电平沿，只是以 push 回调形式交付，
    /// 因此不需要为它养一个常驻任务。
    ///
    /// 已提交关闭之后再注册时，按电平语义**立即同步调用一次**，避免「检查时还没关、
    /// 注册完已经关了」的窗口让回调永远不触发。判定与派发状态在同一把锁内完成：
    /// 每个注册恰好执行一次——要么并入尚未结束的派发轮次，要么在派发已结束后自跑，
    /// 不存在既被补齐又被快照重跑的窗口。
    ///
    /// 回调整体是同步的，不能 `await`；不能在这里等待本层 [`Context::stopped`]。
    ///
    /// 已知残留：本层到达 `Stopped` 之后再注册的回调仍会立即执行（电平语义），但那时
    /// 清理 future 已结束、没有消费者再读错误暂存，因此该回调若 panic、或此后的注册
    /// 触顶产生的 [`ErrorKind::CloseHookDispatchOverflow`]，都只会留在暂存里（panic
    /// 另由 panic hook 输出），不会出现在任何 `StopOutcome` 里。本层被遗弃、从不
    /// `stop` 时同理。在终态之后再注册关闭回调属于边界用法，这是刻意保留的低危缺口，
    /// 不是「错误被吞」的一般性承诺。
    ///
    /// 关闭回调链式注册有**三道上界**（框架内部常量）：派发轮次
    /// `MAX_CLOSING_HOOK_ROUNDS`（16）、执行的嵌套深度（同为 16）、**链式注册**（关闭
    /// 钩子执行中触发、或层已开始派发后注册）的总数 `MAX_CHAINED_CLOSING_HOOKS`（4096）。
    /// 轮次触顶时收尾会把此刻已在表内的剩余项各补跑一次；其余两处触顶则该回调不再
    /// 登记、也不执行。任一触顶都记一条 [`ErrorKind::CloseHookDispatchOverflow`]
    /// （每层至多一条）。三闸缺一不可：没有深度闸，自增殖回调会栈溢出（abort，
    /// `catch_unwind` 拦不住）；没有总数闸，一次注册 `k` 个副本的回调会按 `k^16` 膨胀。
    /// 层存活期间由普通代码做的动态注册不计入总数上界，不受影响。
    pub fn on_closing(&self, hook: impl CloseHook) -> CloseHandle {
        let id = self.inner.closing_hook_seq.fetch_add(1, Ordering::Relaxed);
        let hook: Arc<dyn CloseHook> = Arc::new(hook);
        // 判定「是否即时自跑 / 是否触顶」与入表在同一临界区：派发状态已结束（或本层
        // 被遗弃、不会再派发）才自跑；未派发/派发中会被后续轮次覆盖。链式总量在锁内
        // 计数，嵌套深度用线程局部量（递归天然单线程，跨层共用，无需入锁）。
        let (run_now, cutoff) = {
            let mut hooks = self.inner.lock_closing_hooks();
            let over_depth = hooks.dispatch == CloseDispatch::Dispatched
                && ClosingHookDepth::current() >= MAX_CLOSING_HOOK_ROUNDS;
            // 只有**链式注册**才消耗总数预算：即在关闭钩子执行上下文内触发（任何层，
            // 含向尚未开始派发的另一层注册），或目标层自身已开始派发。层存活期间由
            // 普通代码做的动态注册不计——否则长生命周期层累计注册会误耗尽预算。
            let chained =
                ClosingHookDepth::current() > 0 || hooks.dispatch != CloseDispatch::NotStarted;
            if (chained && hooks.chained >= MAX_CHAINED_CLOSING_HOOKS) || over_depth {
                (false, hooks.mark_cutoff())
            } else {
                if chained {
                    hooks.chained += 1;
                }
                hooks.hooks.push((id, hook.clone()));
                (hooks.dispatch == CloseDispatch::Dispatched, false)
            }
        };
        if cutoff {
            self.inner.lock_closing_errors().push(Error::new(
                Phase::Close,
                ErrorKind::CloseHookDispatchOverflow,
            ));
        }
        if run_now && let Some(err) = self.run_hook_one(&hook) {
            self.inner.lock_closing_errors().push(err);
        }

        CloseHandle {
            inner: self.inner.clone(),
            id,
        }
    }

    /// 派发本层全部关闭回调。只在 `begin_close` 首次提交后调用一次。
    ///
    /// 结构上保证三件事：回调在 `ScopeCore` 锁**外**执行（`begin_close` 已返回、锁已
    /// 释放）；每个回调的 panic 都被隔离成错误——绝不让用户代码把 `stop()` 打挂；
    /// 派发是**有界收敛**的——链式注册有三道上界（轮次、即时自跑嵌套深度、链式注册
    /// 总数，见 [`MAX_CLOSING_HOOK_ROUNDS`] / [`MAX_CHAINED_CLOSING_HOOKS`]）：派发
    /// 期间新注册的回调在下一轮执行，超过轮次上限后收尾把剩余项各执行一次；派发结束
    /// 后（`Dispatched`）新注册的即时自跑受嵌套深度与总数约束，触顶不再执行。任一
    /// 触顶都经 `mark_cutoff` 记**一条** `CloseHookDispatchOverflow`（每层至多一条）。
    /// 收尾（取走剩余 + 置 `Dispatched`）在**一次持锁**内完成，因此不存在「刚注册的项
    /// 既不在批次、又因已置 `Dispatched` 而不自跑」的缝隙。
    ///
    /// 「各执行一次」仅覆盖**收尾那一刻已在表内**的剩余项；此后由即时自跑驱动、
    /// 继续链式注册的回调，触顶后不登记也不执行（否则同步递归会栈溢出，或按分支因子
    /// 指数膨胀）。
    fn run_closing_hooks(&self) {
        {
            let mut hooks = self.inner.lock_closing_hooks();
            if hooks.dispatch != CloseDispatch::NotStarted {
                // 本层只提交一次 `begin_close`，正常不会重入。
                return;
            }
            hooks.dispatch = CloseDispatch::Dispatching;
        }

        let mut errors = Vec::new();
        for _ in 0..MAX_CLOSING_HOOK_ROUNDS {
            // 取一批「尚未派发」并**立即释放锁**：用户回调在锁外执行。
            let batch = self.inner.lock_closing_hooks().take_pending();
            if batch.is_empty() {
                break;
            }
            self.run_hook_batch(&batch, &mut errors);
        }

        // 原子收尾：同一次持锁内「取走剩余 pending + 置 `Dispatched`」。此后新注册
        // 由 `on_closing` 自跑；已经注册的剩余项在这里各执行一次，不滞留。轮次触顶
        // 与预算/深度触顶共用 `mark_cutoff`，因此「链式注册未收敛」每层至多记一条。
        let (remaining, first_cutoff) = {
            let mut hooks = self.inner.lock_closing_hooks();
            let remaining = hooks.take_final();
            let first = !remaining.is_empty() && hooks.mark_cutoff();
            (remaining, first)
        };
        if first_cutoff {
            errors.push(Error::new(
                Phase::Close,
                ErrorKind::CloseHookDispatchOverflow,
            ));
        }
        self.run_hook_batch(&remaining, &mut errors);

        if !errors.is_empty() {
            self.inner.lock_closing_errors().extend(errors);
        }
    }

    /// 执行单个关闭回调：**深度维护的唯一出处**——`ClosingHookDepth` 在执行外围
    /// 自增/自减，并隔离 panic（返回 `Some(error)`）。
    ///
    /// 深度不只是嵌套上限，也是「此刻正在执行关闭钩子」这一上下文的载体：`on_closing`
    /// 据此判定一次注册是否属于**链式注册**（在钩子执行中被触发），从而只对链式部分
    /// 计预算——层存活期间由普通代码注册的动态回调不受影响。
    fn run_hook_one(&self, hook: &Arc<dyn CloseHook>) -> Option<Error> {
        let _depth = ClosingHookDepth::enter();
        run_close_hook(hook.as_ref(), self)
    }

    /// 依次执行一批关闭回调；深度维护与 panic 隔离统一在 [`Self::run_hook_one`]。
    fn run_hook_batch(&self, batch: &[Arc<dyn CloseHook>], errors: &mut Vec<Error>) {
        for hook in batch {
            if let Some(err) = self.run_hook_one(hook) {
                errors.push(err);
            }
        }
    }

    /// 父级 Context 句柄；根级为 `None`。
    pub fn parent(&self) -> Option<Context> {
        self.inner.parent.clone().map(|inner| Context { inner })
    }

    /// 旁路通知：把事件**同步入队**到各订阅者的 lane，发完即返回，不等任何人。
    ///
    /// 沿父链自下而上投递（与 [`Context::emit`] 一致：本层先、祖先后），跨订阅者、
    /// 跨层不保证处理顺序——只保证**每个订阅者内部**按收到先后处理。订阅者慢或卡住
    /// 只堆自己那条 lane，满了丢新来的并计入回执。
    ///
    /// 与 [`Context::emit`] 的分工：需要确定性、可短路、要拿 handler 错误就用 `emit`
    /// （调用方 await 整条链）；只是「顺带看一眼」的审计/监控就用 `notify`。
    ///
    /// `TaskFailed` 不走这里：它是任务失败的**唯一**出口，走内联上报（见
    /// [`Context::spawn`] 的说明），绝不进可丢的 lane。
    ///
    /// 无 `tokio` 构建没有执行器驱动 worker，此处退化为「调用方同步跑完本层及祖先
    /// 的全部 handler」（仍保持 handler 错误不阻断他人），回执的 `delivered` 即实跑数。
    #[cfg(feature = "tokio")]
    pub async fn notify<E: Event>(&self, event: E) -> Receipt {
        let event: Arc<dyn std::any::Any + Send + Sync> = Arc::new(event);
        let type_id = TypeId::of::<E>();
        let mut receipt = Receipt::default();
        for data in self.inner.chain() {
            receipt.merge(data.notify.enqueue(type_id, &event));
        }
        receipt
    }

    /// 无 `tokio` 的退化实现：没有 worker 可投递，只能调用方同步跑完。
    #[cfg(not(feature = "tokio"))]
    pub async fn notify<E: Event>(&self, event: E) -> Receipt {
        let mut receipt = Receipt::default();
        let mut current = Some(&self.inner);
        while let Some(inner) = current {
            let handlers = inner.event_handlers_for::<E>();
            if handlers.is_empty() {
                // 多数层没有该事件类型的 handler：不构造 `Context`、不做无谓的
                // `Arc` 自增就跳过。
                current = inner.parent.as_ref();
                continue;
            }
            let ctx = Context {
                inner: Arc::clone(inner),
            };
            for handler in handlers {
                // 旁路语义：单个 handler 出错不阻断后续，也不影响返回。
                let _ = handler.call(&event, &ctx).await;
                receipt.delivered += 1;
            }
            current = inner.parent.as_ref();
        }
        receipt
    }

    /// 本层及祖先各订阅者的积压与丢弃观测，用于回答「是哪个订阅者、丢了多少」。
    ///
    /// 无 `tokio` 时为无 lane 可查，返回空表。
    pub fn notify_stats(&self) -> Vec<Backlog> {
        #[cfg(feature = "tokio")]
        {
            let mut out = Vec::new();
            for data in self.inner.chain() {
                out.extend(data.notify.backlog());
            }
            out
        }
        #[cfg(not(feature = "tokio"))]
        {
            Vec::new()
        }
    }

    /// 启动本层通知 worker；由 `Runtime::start` 调用一次。
    #[cfg(feature = "tokio")]
    fn start_notify_workers(&self, workers: &mut Vec<tokio::task::JoinHandle<()>>) {
        // 只取一次：接收端一旦被取出又没交给 worker，通道会立即关闭，后续投递全部
        // 落进 `rejected`。
        let inputs: Vec<crate::notify::LaneInput> = self.inner.notify.take_inputs();
        if inputs.is_empty() {
            return;
        }
        // 每个订阅者一个 worker；共同的排空信号驱动它们处理完积压后退出。
        //
        // worker 只持 `Weak<Data>`：它手里若有强引用，`Data → hub → lane` 与
        // `worker → Data` 会成环，owner 未 `stop` 就丢弃 `Runtime` 时 Data 永远
        // 不释放，通道也不会关闭。弱引用让「Data 一释放 → 通道断开 → worker 退出」
        // 这条链自然成立。
        let shutdown = self.inner.notify.shutdown_signal();
        for input in inputs {
            let handler = input.handler;
            let mut rx = input.rx;
            let mut shutdown = shutdown.clone();
            let weak = Arc::downgrade(&self.inner);
            workers.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        maybe = rx.recv() => match maybe {
                            Some(event) => {
                                let Some(data) = weak.upgrade() else { break };
                                let ctx = Context { inner: data };
                                let _ = handler.call(event.as_ref(), &ctx).await;
                            }
                            None => break,
                        },
                        _ = shutdown.changed() => {
                            // 排空：处理完已入队项再退出，避免丢积压。
                            while let Ok(event) = rx.try_recv() {
                                let Some(data) = weak.upgrade() else { break };
                                let ctx = Context { inner: data };
                                let _ = handler.call(event.as_ref(), &ctx).await;
                            }
                            break;
                        }
                    }
                }
            }));
        }
    }

    /// 本层**尚未归还租约**的子作用域 `ScopeId` 清单，恒为 `ScopeId` 升序。
    ///
    /// 包含尚未 `build` 的子 `Builder` 与尚未走完停止流程的子 `Runtime`。不变式是
    /// 单向的：列出的必未 `Stopped`；不在清单 ⇒ 租约已归还（即使 `is_stopped()`
    /// 因跨层可见性晚一拍为真）。
    pub fn children(&self) -> Vec<ScopeId> {
        self.inner.core.children()
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
        let id = TaskId::new(self.inner.tasks.next_id.fetch_add(1, Ordering::Relaxed));

        let cell = Arc::new(TaskCell {
            id,
            abort: OnceLock::new(),
            finished: Waiters::new(),
            outcome: Mutex::new(None),
            report_settled: AtomicBool::new(false),
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
                Ok(Ok(())) => task_cell.finish(TaskOutcome::Completed),
                Ok(Err(err)) => {
                    // 顺序是硬约束：**先写权威结局，再上报**。
                    //
                    // `Ok(Err)` 没有第二条出口——`drain_error` 对 `Failed` 返回 `None`
                    // （任务体返回的 `Err` 不进 `stop` 的汇总错误），`TaskFailed` 事件
                    // 是唯一通路。把记录先落进结局，上报路径即使 panic 也没有丢：结局
                    // 已经是 `Failed`，`FinishOnUnwind` 只负责补发完成信号，不会再用
                    // `Panicked` 覆盖它。
                    let shared = Arc::new(err.clone());
                    task_cell.set_outcome(TaskOutcome::Failed(err));
                    // 上报路径可能被两种方式截断：自身 panic（`catch_unwind` 收口），
                    // 或排空预算耗尽时任务被 abort、上报 future 在 await 点被取消。
                    // 两种都让 `report_settled` 保持 false，排空据结局类别兜底上报一次
                    // ——真实失败已在结局里，不会被覆盖。
                    let reported = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                        task_ctx.emit_notify(TaskFailed {
                            task_id: id,
                            error: shared,
                        }),
                    ))
                    .await;
                    if reported.is_ok() {
                        task_cell.report_settled.store(true, Ordering::Release);
                    }
                    // 公开结局：与旧顺序一样在上报之后唤醒；`report_settled` 必须在
                    // `fire_all` **之前**置位，等待者/排空一旦看到完成信号就必然看到
                    // 「已送达」，不会把已送达的失败重复上报。
                    task_cell.finished.fire_all();
                }
                Err(payload) => {
                    task_cell.finish(TaskOutcome::Panicked(PanicInfo::from_payload(&*payload)))
                }
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
        self.inner.core.cancellation.waiter_count()
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
        self.inner.core.lease_count() as u64
    }

    /// 串行发出事件，并沿父链向上冒泡。
    pub async fn emit<E: Event>(&self, event: E) -> Result<(), Error> {
        let errors = self
            .emit_impl(event, Scheduling::Ordered, ErrorPolicy::Abort)
            .await;
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
        let errors = self
            .emit_impl(event, Scheduling::Concurrent, ErrorPolicy::Abort)
            .await;
        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::new(Phase::Event, ErrorKind::Multiple(errors)))
        }
    }

    /// **内联**旁路通知：串行执行事件，handler 错误不阻断后续 handler 和父链冒泡；
    /// **但调用方要等全部 handler 跑完**。返回所有收集到的 handler 错误。
    ///
    /// 三个投递档位的分工，按「要不要等订阅者」选：
    ///
    /// | 用途 | 用哪个 |
    /// |---|---|
    /// | 要确定性顺序、要短路、要同步拿到 handler 错误 | [`Context::emit`] / [`Context::emit_parallel`] |
    /// | 失败上报等**不能丢**、且能接受等待 | 本方法（内联、无损） |
    /// | 审计 / 监控等「顺带看一眼」，**不能让订阅者拖住主流程** | [`Context::notify`]（异步、有界、可丢） |
    ///
    /// `TaskFailed` 走本方法而不是 [`Context::notify`]：它是任务失败的**唯一**出口，
    /// 丢不起，所以在任务体内内联上报。
    pub async fn emit_notify<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, Scheduling::Ordered, ErrorPolicy::Collect)
            .await
    }

    /// [`Context::emit_notify`] 的并行版本；同样要等全部 handler 跑完。
    ///
    /// 并行下 `EventControl::Bail` 只能阻止向父层冒泡，拦不住同层已经启动的 handler。
    pub async fn emit_notify_parallel<E: Event>(&self, event: E) -> Vec<Error> {
        self.emit_impl(event, Scheduling::Concurrent, ErrorPolicy::Collect)
            .await
    }

    async fn emit_impl<E: Event>(
        &self,
        event: E,
        scheduling: Scheduling,
        policy: ErrorPolicy,
    ) -> Vec<Error> {
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

            let outcome = dispatch_layer(handlers, &event, &ctx, scheduling, policy).await;

            // 严格策略下「本层有错」与 `Bail` 一样终止冒泡；旁路策略只把错误记下来。
            let abort = matches!(policy, ErrorPolicy::Abort) && !outcome.errors.is_empty();
            all_errors.extend(outcome.errors);

            if outcome.bail || abort {
                return all_errors;
            }

            current = inner.parent.as_ref();
        }

        all_errors
    }
}

/// 同一层内 handler 的调度方式。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scheduling {
    /// 按注册序逐个执行。
    Ordered,
    /// 同层并发执行（`join_all`）。
    ///
    /// `Bail` 只能阻止向父层冒泡：并发下它返回时同层其他 handler 早已启动，
    /// 拦不住它们。
    Concurrent,
}

/// handler 出错后的处理策略。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ErrorPolicy {
    /// 严格：首个错误即停止本层剩余 handler 与向上冒泡，最终只返回这一个错误。
    Abort,
    /// 旁路：收集错误、继续执行本层其余 handler，并继续向上冒泡。
    Collect,
}

/// 单层投递的结果。
struct LayerOutcome {
    /// 本层是否出现了 `EventControl::Bail`。
    bail: bool,
    /// 本层收集到的 handler 错误。
    errors: Vec<Error>,
}

/// 执行单层全部 handler。
///
/// 两个维度正交：`Scheduling` 决定怎么排、`ErrorPolicy` 决定出错后收不收手。
/// 拆开写而不是塞进一个布尔分支树，是为了让四种组合的语义各自只有一处定义。
async fn dispatch_layer<E: Event>(
    handlers: &[Arc<dyn ErasedEventHandler>],
    event: &E,
    ctx: &Context,
    scheduling: Scheduling,
    policy: ErrorPolicy,
) -> LayerOutcome {
    let mut outcome = LayerOutcome {
        bail: false,
        errors: Vec::new(),
    };

    match scheduling {
        Scheduling::Concurrent => {
            let results =
                futures::future::join_all(handlers.iter().map(|handler| handler.call(event, ctx)))
                    .await;
            for result in results {
                match result {
                    Ok(EventControl::Continue) => {}
                    Ok(EventControl::Bail) => outcome.bail = true,
                    Err(err) => outcome.errors.push(err),
                }
            }
        }
        Scheduling::Ordered => {
            for handler in handlers {
                match handler.call(event, ctx).await {
                    Ok(EventControl::Continue) => {}
                    Ok(EventControl::Bail) => {
                        outcome.bail = true;
                        break;
                    }
                    Err(err) => {
                        outcome.errors.push(err);
                        // 严格策略下首个错误即收手，本层后面的 handler 不再执行。
                        if matches!(policy, ErrorPolicy::Abort) {
                            break;
                        }
                    }
                }
            }
        }
    }

    outcome
}

/// 后台任务失败事件。
///
/// 由 [`Context::spawn`] 的任务在返回 `Err` 时以旁路通知发出，沿父链冒泡；
/// handler 错误不会反向影响任务。
#[derive(Debug)]
pub struct TaskFailed {
    /// 任务 id（[`TaskHandle::id`]，作用域内唯一）。
    pub task_id: TaskId,
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
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            inner: self.inner.clone(),
            wait: None,
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
    pub fn id(&self) -> TaskId {
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

    /// 等待任务结局，返回完整结局。
    ///
    /// 不再把结局折叠成 `Result`：[`TaskOutcome::Completed`]（跑完）、
    /// [`TaskOutcome::Failed`]（任务体返回 `Err`）、[`TaskOutcome::Panicked`] 与
    /// [`TaskOutcome::Aborted`]（取消，含 [`AbortReason`] 成因）彼此可区分。
    /// panic 与取消都**不再**被伪装成 `Ok(())`，owner 因此能分辨「跑完了」与
    /// 「被取消了」。需要旧的 `Result<(), Error>` 视图时用
    /// [`TaskOutcome::into_result`]。
    pub async fn wait(&self) -> TaskOutcome {
        let cell = self.cell.clone();
        cell.finished.wait().await;
        cell.outcome()
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

/// [`Runtime::stop`] / [`Runtime::stop_with_timeout`] 的结局。
///
/// 判别式只表达**进度**：`Blocked` 是非终态（清理尚未开始），`Stopped` 是终态。
/// 「有没有错」是终态上的正交属性，放在 `errors` 里——不把两个维度压进一个判别式。
#[derive(Debug)]
#[must_use = "`Blocked` 表示停止被活跃子作用域挡住，静默丢弃即漏处理"]
pub enum StopOutcome {
    /// 已进入 `Stopped` 终态；`errors` 为插件 `stop`、任务排空、dispose 累积的错误。
    Stopped { errors: Vec<Error> },
    /// 已提交停止（本层进入 `Closing`：拒绝新工作、取消已广播），但仍有活跃子
    /// 作用域，清理尚未开始。等阻塞方回收后再次调用 `stop` 即可续跑。
    Blocked(Blockers),
}

impl StopOutcome {
    /// 是否已到终态。
    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }

    /// 终态错误表；`Blocked` 时为空。
    pub fn errors(&self) -> &[Error] {
        match self {
            Self::Stopped { errors } => errors,
            Self::Blocked(_) => &[],
        }
    }

    /// 阻塞方清单；已停稳时为 `None`。
    pub fn blockers(&self) -> Option<&Blockers> {
        match self {
            Self::Blocked(blockers) => Some(blockers),
            Self::Stopped { .. } => None,
        }
    }

    /// 折叠成 `Result<(), Error>`，便于在「确定没有活跃子作用域」的调用点用 `?`
    /// 传播。`Blocked` 折叠为 [`ErrorKind::StopBlocked`]——注意它表达的是「前置条件
    /// 未满足、可重试」，不是清理失败。
    pub fn into_result(self) -> Result<(), Error> {
        match self {
            Self::Stopped { errors } if errors.is_empty() => Ok(()),
            Self::Stopped { errors } => Err(Error::new(Phase::Stop, ErrorKind::Multiple(errors))),
            Self::Blocked(blockers) => {
                Err(Error::new(Phase::Stop, ErrorKind::StopBlocked(blockers)))
            }
        }
    }

    /// 断言已停稳且无清理错误；`Blocked` 或有错误时 panic，panic 消息带错误内容。
    ///
    /// 供测试与「确定本层没有活跃子作用域」的调用点使用，语义与
    /// `Result::unwrap` 对应。
    #[track_caller]
    pub fn unwrap(self) {
        if let Err(err) = self.into_result() {
            panic!("stop did not complete cleanly: {err}");
        }
    }

    /// 断言停止不干净（被阻塞或有清理错误）并取出错误。
    #[track_caller]
    pub fn unwrap_err(self) -> Error {
        match self.into_result() {
            Ok(()) => panic!("stop completed cleanly"),
            Err(err) => err,
        }
    }
}

/// 自持清理 future：拥有插件、dispose、待回收范围与累积错误，`Output` 是错误表。
///
/// 存放在 [`Runtime`] 里而不是调用方的栈上，因此调用方丢弃 `stop()` 的 await
/// **不会**中断清理——重入继续 poll 同一个 future，进度由 future 自身保存，
/// 每个插件 `stop` 只被调用一次，不需要可重入。
type StopFuture = futures::future::BoxFuture<'static, Vec<Error>>;

/// 生命周期唯一所有者；不 `Clone`，`#[must_use]`。
///
/// 生命周期状态不存在本地副本：真值源是 `ScopeCore` 里 `Mutex<ScopeInner>` 保护的
/// `{ lifecycle, requested, abandoned, children }`。本地 bool 镜像一旦存在，就会在状态被 owner 之外的
/// 路径推进时变成陈旧值——那正是「启动失败被记成已启动」的同一形态。
#[must_use]
pub struct Runtime {
    ctx: Context,
    plugins: Vec<PluginRecord>,
    ready: Vec<ReadyHook>,
    dispose: Vec<DisposeHook>,
    /// 启动流程中已经进入的插件索引，`stop` 逆序回收。这是 owner 的执行资源，
    /// 不参与「当前处于什么状态」的判断。进入清理时被移入 `stop_future`。
    started_plugins: Vec<usize>,
    /// 自持清理 future；仅在首次进入 `Stopping` 时构造一次，之后每次 `stop` 只
    /// poll 它。`Some` 表示清理已开始但未完成。
    ///
    /// 它拥有插件、dispose、待回收范围与累积错误，因此调用方丢弃 `stop()` 的
    /// await **不会**中断清理：重入继续 poll 同一个 future，插件 `stop` 只被调用
    /// 一次，不需要可重入。
    stop_future: Option<StopFuture>,
    /// 首次启动失败的聚合错误：供 [`Runtime::start_error`] 查询，并作为重入
    /// 错误的 `source`。`Error` 可 `Clone`（`source` 是 `Arc`），因此这份副本与
    /// 交给调用方的那份共享同一条错误链。
    start_error: Option<Error>,
    /// `build()` 一次算出的启动调度；`start` 取用后清空。
    schedule: Schedule,
    /// 是否处于「已进入启动流程但尚未到 `Stopped`」。只服务 `Drop` 诊断，不参与
    /// 状态判定，因此是单原子读写、不拿 `ScopeCore` 锁。
    active: AtomicBool,
    /// 本层通知 worker 的句柄；`start` 时启动，停止排空阶段 join。
    ///
    /// 不放进 `TaskRegistry`：任务表是用户 `spawn` 的语义，排空策略（超时即 abort）
    /// 也不适用——通知 worker 只需处理完积压后自然退出。
    #[cfg(feature = "tokio")]
    notify_workers: Vec<tokio::task::JoinHandle<()>>,
    /// 子作用域租约。[`Runtime::stop`] 走到 `Stopped` 时提前归还，因此父 `stop`
    /// 不必再等本对象析构；未 stop 就被 drop 的 Runtime 仍由字段析构归还。
    ///
    /// 必须作为最后一个字段声明：未 stop 的路径上要确保插件字段先于租约析构。
    lease: Option<ScopeLease>,
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

    /// 当前生命周期状态。真值源是 `ScopeCore`，本地不保留副本。
    fn lifecycle(&self) -> Lifecycle {
        self.ctx.inner.core.lifecycle()
    }

    /// 更新生命周期状态（`ScopeCore` 在锁内同时处理 `stopping` 单向闩）。
    fn set_lifecycle(&self, next: Lifecycle) {
        self.ctx.inner.core.set_lifecycle(next);
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
            // 已经停稳、正在停止、或已提交停止：start-after-stop 是 no-op。
            // 停止意图不可逆，因此 `Closing` 也不接受启动。
            Lifecycle::Stopped | Lifecycle::Stopping | Lifecycle::Closing => return Ok(()),
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

        // 通知 worker 先于插件 `start` 起来：插件启动期发出的通知会被投递，而不是
        // 攒在 lane 里等所有插件就绪。此刻已在 tokio 上下文内（`start` 是 async）。
        #[cfg(feature = "tokio")]
        {
            let workers = &mut self.notify_workers;
            self.ctx.start_notify_workers(workers);
        }

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
    /// 调用即**提交**停止意图（不可逆）：本层转入 `Closing`，此后拒绝 `scope` /
    /// `spawn` 并广播 [`Context::cancelled`]。若本层仍有活跃子 `Runtime` / `Builder`，
    /// 返回 [`StopOutcome::Blocked`]（含阻塞方清单），**清理尚未开始**；等阻塞方
    /// 回收后再次调用即续跑——意图不可回退，因此不存在「Blocked 后恢复运行」。
    ///
    /// 与 [`StopHandle::request_stop`] 的区别：那是外部的弱请求，只广播、不改变
    /// 生命周期；本方法是 owner 的强决定。
    ///
    /// 已注册后台任务的排空不设超时，等待全部任务自然完成。
    pub async fn stop(&mut self) -> StopOutcome {
        self.stop_impl(None).await
    }

    /// 带总预算的优雅停止。
    ///
    /// 与 [`Runtime::stop`] 相同，但 `Context::spawn` 注册的任务排空共享
    /// `timeout` 预算：预算耗尽仍未完成的任务会被强制取消，并以
    /// `ErrorKind::TaskAborted` 计入 `StopOutcome::Stopped { errors }`。
    ///
    /// 仅在启用 `tokio` feature 且使用过 `Context::spawn` 时有实际差异；
    /// 插件 `stop` 与 dispose 回调本身不受该预算约束。
    ///
    /// 预算在**进入清理时**一次算定并随清理 future 存续：中途丢弃 `stop` future
    /// 后重入不会重置预算，那才是真正的总上界。反过来，若本层已带着「不设超时」
    /// 的预算进入清理，之后再用本方法也不会收紧——要让超时生效，须用本方法首次
    /// 进入清理。
    ///
    /// 超时排空使用 `tokio::time` 定时器，要求当前 runtime 启用了 time
    /// driver（标准 `new_multi_thread` / 显式 `enable_time` 的
    /// `new_current_thread` 均满足）。
    pub async fn stop_with_timeout(&mut self, timeout: Duration) -> StopOutcome {
        self.stop_impl(Some(timeout)).await
    }

    async fn stop_impl(&mut self, task_drain_timeout: Option<Duration>) -> StopOutcome {
        // 入口状态机。`Closing` 表示上一次 `stop` 已提交意图但被活跃子作用域挡住，
        // 这里重新检查租约；`Stopping` 表示上一次 stop future 被丢弃、清理仍有进度，
        // 直接续跑。两条路径都不重复广播（`Waiters::fire_all` 幂等）。
        match self.lifecycle() {
            Lifecycle::Stopped => return StopOutcome::Stopped { errors: Vec::new() },
            Lifecycle::Stopping => {}
            _ => {
                // 提交意图（幂等）。只有**首次**提交返回 `true`——这正是「本层刚开始
                // 关闭」那一次的原子沿，关闭回调挂在它上面，天然每层至多一次。
                if self.ctx.inner.core.begin_close() {
                    self.ctx.run_closing_hooks();
                }
                // 提交意图后**无条件**广播取消：即使因活跃子作用域被阻塞、清理尚未
                // 开始，owner 的停止意图也应当唤醒长驻任务收尾。这正是 `cancelled()`
                // 文档所说「任何停止路径都会唤醒它」的实现。
                self.ctx.inner.core.cancellation.fire_all();

                if let Err(blockers) = self.ctx.inner.core.enter_cleanup() {
                    return StopOutcome::Blocked(blockers);
                }
                // 首次进入清理：构造自持 future，此后每次 stop 只是 poll 它。
                self.start_stop_future(task_drain_timeout);
            }
        }

        // 调用方 await 的只是对自持 future 的 poll：外层 await 被丢弃不影响
        // future 自身的进度（它仍在 `self.stop_future` 里），重入继续 poll。
        let errors = {
            let future = self
                .stop_future
                .as_mut()
                .expect("Stopping implies stop future initialized");
            std::future::poll_fn(|cx| future.as_mut().poll(cx)).await
        };
        self.stop_future = None;

        // 归还父租约必须**早于**标记 `Stopped`：`children()` 与 `is_stopped()` 的
        // 不变式是「列出的子都未 Stopped」「已 Stopped 的子都已归还」。反过来会
        // 留下一个可被多线程观测的窗口——`is_stopped()` 已为真而父 `children()`
        // 仍列出该子。plugins/dispose 已随 `stop_future` 析构，租约仍最后释放。
        drop(self.lease.take());

        self.set_lifecycle(Lifecycle::Stopped);
        self.active.store(false, Ordering::Release);

        // 通知 `Context::stopped` 的等待者：本层已停稳、资源已回收。
        self.ctx.inner.settle();

        StopOutcome::Stopped { errors }
    }

    /// 构造自持清理 future。
    ///
    /// 把插件、dispose、待回收范围与累积错误一起 move 进去；此后 `Runtime` 只剩
    /// 一个 future 句柄，清理进度由 future 自身保存。timeout 的 deadline 在**进入
    /// 清理时**一次算定并随 future 存续，中途重入不会重置预算。
    fn start_stop_future(&mut self, task_drain_timeout: Option<Duration>) {
        let plugins = std::mem::take(&mut self.plugins);
        let mut dispose = std::mem::take(&mut self.dispose);
        let started_plugins = std::mem::take(&mut self.started_plugins);
        #[cfg(feature = "tokio")]
        let notify_workers = std::mem::take(&mut self.notify_workers);
        let ctx = self.ctx.clone();

        // `checked_add`：`Instant + Duration` 在越过可表示范围时会 panic，而
        // `stop_with_timeout(Duration::MAX)` 是合法输入。溢出按「实际无限预算」
        // 处理（等价于不设超时），不 panic。
        #[cfg(feature = "tokio")]
        let deadline = task_drain_timeout.and_then(|budget| Instant::now().checked_add(budget));
        #[cfg(not(feature = "tokio"))]
        let _ = task_drain_timeout;

        self.stop_future = Some(Box::pin(async move {
            // 关闭回调的错误分两处并入：提交点同步派发的那批在这里（清理起手）取走，
            // 顺序排在插件/排空/dispose 错误之前；清理期间经 `on_closing` 注册并立即
            // 执行的那批在末尾再取一次——单点起手取走会让后写入的错误永远没有读者。
            let mut errors = ctx.inner.take_closing_errors();

            // 插件逆序回收。future 停在某个 `stop` 的 await 上时，重入继续 poll
            // 的是同一个调用，不会从该项开头重放。每个调用都经 `run_lifecycle_unit`
            // 隔离：插件 `stop` panic 不得逃出本 future（否则后续插件、任务排空、
            // dispose 全被跳过，且 `stop_future` 被 poison 导致重入二次 panic）。
            for &index in started_plugins.iter().rev() {
                let record = &plugins[index];
                if let Err(err) =
                    run_lifecycle_unit(Phase::Stop, Some(record.name()), record.plugin.stop(&ctx))
                        .await
                {
                    errors.push(err);
                }
            }

            #[cfg(feature = "tokio")]
            drain_tasks_into(&ctx, deadline, &mut errors).await;

            // 通知排空必须排在任务排空之后：任务体可能在返回前发出通知，先关 hub
            // 会把它们判成「拒绝」。与任务共享同一 deadline 预算与错误聚合口径。
            #[cfg(feature = "tokio")]
            drain_notify_into(&ctx, notify_workers, deadline).await;

            // dispose 保持注册序，同样逐个隔离。
            for hook in dispose.iter_mut() {
                if let Err(err) = run_lifecycle_unit(Phase::Dispose, None, hook.call(&ctx)).await {
                    errors.push(err);
                }
            }

            // 清理期间写入的关闭回调错误（见起手处的两处并入说明）。放在这里读，
            // 覆盖 `plugin.stop` / dispose 期间经 `on_closing` 注册并立即执行的回调。
            errors.extend(ctx.inner.take_closing_errors());

            errors
        }));
    }
}

/// 排空 `ctx` 登记的后台任务：插件 stop 之后、dispose 之前执行。
///
/// 与自持 future 配合：每个 cell 都是「先 await 结局、再取出」，因此被丢弃的
/// `stop()` 外层 await 不会把在飞任务从表里摘走——future 自身留在 `Runtime` 里，
/// 重入继续 poll。完成信号是电平触发的，已结束的立即返回。`deadline` 在进入清理
/// 时算定并随 future 存续，中途重入不会重置预算。
#[cfg(feature = "tokio")]
async fn drain_tasks_into(ctx: &Context, deadline: Option<Instant>, errors: &mut Vec<Error>) {
    loop {
        // 锁只在取表尾这一瞬间持有。这里不能写成
        // `while let Some(cell) = ...pop()`：scrutinee 的 `MutexGuard` 临时值会
        // 活到整个循环体结束，await 期间仍持锁，而任务体自己要用同一把锁——死锁。
        let Some(cell) = ctx.lock_tasks().last().cloned() else {
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

        // 结局已定，从表中取出。若上一行被 await 取消，这一行不执行，cell 仍在
        // 表里，future 重入会重新处理（此时完成信号已触发，立即返回）。
        {
            let mut tasks = ctx.lock_tasks();
            // 排空期间 `spawn` 已被拒绝，表尾不可能被并发替换。若将来放开
            // 「Stopping 期间可 spawn」，必须先改掉这里的取出方式。
            debug_assert!(matches!(tasks.last(), Some(tail) if Arc::ptr_eq(tail, &cell)));
            tasks.pop();
        }

        // 从这里到本次循环结束没有 await 点，因此「取出 - 判断 - 上报」相对
        // 取消是原子的。取消来源直接取自结局本身，不需要额外的旁路状态。
        if let Some(err) = cell.drain_report_error(id) {
            errors.push(err);
        }
    }
}

/// 排空通知 hub：置排空位、等各 lane worker 处理完**已入队**的积压后退出。
///
/// 与任务排空的区别在于超时处置：通知是旁路、可丢的，预算耗尽后直接 detach 剩余
/// worker（不再 sink 成任务那类错误），因为「有通知没送完」不是停止失败。
#[cfg(feature = "tokio")]
async fn drain_notify_into(
    ctx: &Context,
    workers: Vec<tokio::task::JoinHandle<()>>,
    deadline: Option<Instant>,
) {
    ctx.inner.notify.begin_drain();
    for worker in workers {
        match deadline {
            None => {
                let _ = worker.await;
            }
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, worker).await.is_err() {
                    // 预算耗尽：剩余未 join 的 worker 随句柄 detach，不再等待。
                    return;
                }
            }
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // 不做异步清理。租约释放由最后一个字段 `ScopeLease` 在字段析构阶段完成。

        // 诊断文字必须在遗弃处理**之前**算：遗弃会把生命周期推进到 `Closing`，
        // 之后再读就分不清「原本没调 stop」与「stop 被阻塞」。
        //
        // 只在**真的会 panic** 时才构造它：`active` 不被遗弃处理改动，因此这里先
        // 拿到与下面护栏同一个判据，正常 `stop()` 过的 Runtime（`active == false`）
        // 与正在展开栈的路径都不必付一次 `ScopeCore` 锁 + `children` 克隆 + `format!`。
        #[cfg(debug_assertions)]
        let diagnostic = (!std::thread::panicking() && self.active.load(Ordering::Acquire))
            .then(|| self.drop_diagnostic());

        let unstopped = self.lifecycle() != Lifecycle::Stopped;

        // 未到 `Stopped` 就被丢弃：本层不能再作为可管理工作层存在。先提交关闭
        // （拒绝新的 `scope` / `spawn`）并广播取消，再投递停稳信号——否则幸存的
        // `Context` 会成为一个「没有 owner 的僵尸层」：新建的子作用域与后台任务
        // 不再有人排空，等 `cancelled()` 的长驻任务也永远不会被唤醒。
        if unstopped {
            self.ctx.inner.core.begin_close();
            // 本层被遗弃、不会再走正常停止流程，因此关闭回调派发也就「结束」了：
            // 此刻起注册的回调不会被任何快照覆盖，只能自跑（与 `on_closing` 的电平
            // 语义一致）。已派发过的层重复置位是无害的。
            self.ctx.inner.settle_closing_dispatch();
            self.ctx.inner.core.cancellation.fire_all();
            self.ctx.inner.abandon();
        }

        // 生命周期护栏：曾经进入过启动流程却没走到 `Stopped`，说明插件资源与
        // dispose hooks 都没有回收。debug 构建下硬失败，让这类泄漏在开发/测试期
        // 立刻暴露；release 下静默——正确性不应依赖这条诊断。
        //
        // `panicking()` 守卫：调用方可能因别的原因在持有本值时 panic，此时栈正在
        // 展开。Drop 里再 panic 会二次 panic 并 abort，连原始 panic 信息一起吞掉，
        // 因此展开路径只放行。
        #[cfg(debug_assertions)]
        if let Some(diagnostic) = diagnostic {
            panic!("{diagnostic}");
        }
    }
}

impl Runtime {
    /// `Drop` 护栏的根因叙事，区分三种成因：被活跃子作用域阻塞、已提交停止但
    /// 清理未走完、从未调用 `stop`。
    ///
    /// 阻塞方按**当前** `children()` 现读，而不是缓存上一次 `stop` 的清单——否则
    /// 「先被阻塞、阻塞方随后被回收、owner 直接 drop」会把根因指向已经不存在的
    /// 子作用域。
    #[cfg(debug_assertions)]
    fn drop_diagnostic(&self) -> String {
        match self.lifecycle() {
            Lifecycle::Stopping => {
                "Runtime dropped while stopping: cleanup did not complete and plugin resources \
                 or dispose hooks were not fully reclaimed"
                    .to_string()
            }
            Lifecycle::Closing => {
                let children = self.ctx.children();
                if children.is_empty() {
                    "Runtime dropped after stop() was blocked: the blocking scopes were \
                     reclaimed but stop() was not retried, so cleanup never ran"
                        .to_string()
                } else {
                    format!(
                        "Runtime dropped after stop() was blocked by active child scopes (ids: {children:?}); \
                         drop or stop those scopes, then retry stop()"
                    )
                }
            }
            _ => "Runtime dropped without stop(): plugin resources and dispose hooks \
                  were not reclaimed"
                .to_string(),
        }
    }
}

#[cfg(test)]
mod scope_core_tests {
    use super::*;

    #[test]
    fn tag_tracks_each_transition() {
        let core = ScopeCore::new();
        assert!(!core.is_stopping() && !core.is_stopped() && !core.is_settled());
        assert!(!core.is_stop_requested() && !core.is_abandoned());

        // 显式请求：只置请求位，不改「已提交关闭」。
        core.request_stop();
        assert!(core.is_stop_requested());
        assert!(!core.is_stopping(), "请求不等于已提交关闭");
        assert!(!core.is_settled());

        // 提交关闭：单向一次。
        assert!(core.begin_close());
        assert!(!core.begin_close(), "提交是单向一次");
        assert!(core.is_stopping());
        assert!(!core.is_stopped());
        assert!(!core.is_settled());

        // 走到终态：STOPPED 与「停稳」在同一次 tag 读里一致。
        core.set_lifecycle(Lifecycle::Stopping);
        core.set_lifecycle(Lifecycle::Stopped);
        assert!(core.is_stopped());
        assert!(core.is_settled());
        assert_eq!(core.lifecycle(), Lifecycle::Stopped);
    }

    #[test]
    fn abandon_is_a_settled_outcome() {
        let core = ScopeCore::new();
        core.abandon();

        assert!(core.is_abandoned());
        assert!(core.is_settled(), "遗弃也算停稳：等待者不能永久挂起");
        assert!(!core.is_stopped(), "遗弃不是走完流程");
    }

    #[test]
    fn leases_block_cleanup_and_are_reported() {
        let core = ScopeCore::new();
        let child = ScopeId::new(7);
        assert!(core.acquire_lease(child));

        assert!(core.begin_close());
        let blockers = core.enter_cleanup().expect_err("有活跃租约时必须阻塞");
        assert_eq!(blockers.ids(), &[child]);

        core.release_lease(child);
        assert!(core.enter_cleanup().is_ok());
        assert_eq!(core.lifecycle(), Lifecycle::Stopping);
        assert!(core.children().is_empty());
    }

    #[test]
    fn lease_is_refused_after_close_commit() {
        let core = ScopeCore::new();
        assert!(core.begin_close());
        assert!(
            !core.acquire_lease(ScopeId::new(1)),
            "父已提交关闭，不再长出新子作用域"
        );
    }
}
