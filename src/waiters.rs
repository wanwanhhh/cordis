//! 一次性、**电平触发**的等待者注册表。
//!
//! 触发不可逆：`fire_all` 之后 `register` 一律返回 `None`、`is_fired` 恒为
//! `true`——晚到的等待者因此不会永远挂起。这是取消语义的硬要求：边沿触发要求
//! 「先注册再等待」，而停止可能在任何注册之前就已发生。
//!
//! 三处信号共用本原语：`Data` 的「开始收尾」与「已停稳」，以及 `TaskCell` 的完成
//! 信号（排空与 `TaskHandle::wait` 共用）。
//!
//! 注册项用**带代际的下标槽位**而不是「全局自增 token + 线性扫描」：注册与注销都是
//! `O(1)`、槽位按需复用，长生命周期作用域上反复 `select!`（等到别的事件就丢弃
//! `cancelled()` future）不会退化成每次注销一遍扫全部等待者。
//!
//! 唤醒一律在**锁外**做：waker 是 executor 或用户代码，不能在持锁期间调用。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::Waker;

/// 一个等待者在注册表里的身份。
///
/// `generation` 防槽位复用后的 ABA：槽位被释放又分配给新等待者后，旧持有者的
/// `release` / `update` 会因代际不符而被忽略，不会误伤新注册项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WaitId {
    index: u32,
    generation: u32,
}

struct Slot {
    generation: u32,
    waker: Option<Waker>,
}

#[derive(Default)]
struct State {
    slots: Vec<Slot>,
    /// 空闲槽位下标；复用使槽位总数收敛到「并发等待者峰值」。
    free: Vec<u32>,
}

/// 一次性电平信号 + 等待者注册表。
pub(crate) struct Waiters {
    fired: AtomicBool,
    state: Mutex<State>,
}

impl Waiters {
    pub(crate) const fn new() -> Self {
        Self {
            fired: AtomicBool::new(false),
            state: Mutex::new(State {
                slots: Vec::new(),
                free: Vec::new(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 是否已触发。免锁快路径，供高频存在性检查使用。
    pub(crate) fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    /// 注册一个等待者。
    ///
    /// 已触发时返回 `None`，调用方应直接就绪——「复查 + 注册」在同一把锁内完成，
    /// 与 `fire_all` 的「置位 + 取走」互斥，因此不存在「先查后注册」的丢唤醒窗口。
    pub(crate) fn register(&self, waker: &Waker) -> Option<WaitId> {
        let mut state = self.lock();
        if self.fired.load(Ordering::Acquire) {
            return None;
        }
        let id = match state.free.pop() {
            Some(index) => {
                let slot = &mut state.slots[index as usize];
                slot.waker = Some(waker.clone());
                WaitId {
                    index,
                    generation: slot.generation,
                }
            }
            None => {
                let index = state.slots.len() as u32;
                state.slots.push(Slot {
                    generation: 0,
                    waker: Some(waker.clone()),
                });
                WaitId {
                    index,
                    generation: 0,
                }
            }
        };
        Some(id)
    }

    /// 同一个 future 被反复 poll 时只更新 waker。
    ///
    /// 身份过期（槽位已释放或已换主）时为 no-op，不会误改别人的注册项。
    pub(crate) fn update(&self, id: WaitId, waker: &Waker) {
        let mut state = self.lock();
        if let Some(slot) = state.slots.get_mut(id.index as usize)
            && slot.generation == id.generation
            && slot.waker.is_some()
        {
            slot.waker = Some(waker.clone());
        }
    }

    /// 摘掉注册项。
    ///
    /// 等待者主动放弃等待时调用：`select! { _ = cancelled() => …, _ = work => … }`
    /// 先等到 `work` 就不再关心停止信号，不摘除的话注册项会随这类任务单调累积。
    ///
    /// 已触发时是 no-op——`fire_all` 已把全部注册项取走，槽位不再需要归还。
    pub(crate) fn release(&self, id: WaitId) {
        if self.fired.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.lock();
        let Some(slot) = state.slots.get_mut(id.index as usize) else {
            return;
        };
        if slot.generation != id.generation || slot.waker.is_none() {
            return;
        }
        slot.waker = None;
        slot.generation = slot.generation.wrapping_add(1);
        state.free.push(id.index);
    }

    /// 触发并唤醒全部等待者。幂等，可从任意线程调用。
    pub(crate) fn fire_all(&self) {
        // 置位与取走同在一把锁内：`register` 的「复查 + 注册」也在同一把锁下进行，
        // 两边因此不会交错出「先查后注册」的丢唤醒窗口。唤醒放到锁外做，避免在
        // 持锁期间调用外部代码。
        let wakers = {
            let mut state = self.lock();
            self.fired.store(true, Ordering::Release);
            let mut wakers = Vec::new();
            for slot in &mut state.slots {
                if let Some(waker) = slot.waker.take() {
                    wakers.push(waker);
                }
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }

    /// 等待信号触发；已触发时立即返回。返回的 future 被丢弃时会摘掉自己的注册项。
    #[cfg(feature = "tokio")]
    pub(crate) fn wait(&self) -> Waiting<'_> {
        Waiting {
            signal: self,
            wait: None,
        }
    }

    /// 测试用：当前注册的等待者数量。
    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        self.lock()
            .slots
            .iter()
            .filter(|slot| slot.waker.is_some())
            .count()
    }

    /// 测试用：已分配的槽位总数（验证复用而非单调增长）。
    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.lock().slots.len()
    }
}

/// [`Waiters::wait`] 返回的 future；被丢弃时摘掉自己的注册项。
///
/// 与 [`crate::context::Cancelled`] 同理：`TaskHandle::wait()` 可能被反复丢弃
/// （`select!` 里另一个分支先就绪、被 `timeout` 包裹等），不摘除的话注册项会一直
/// 累积到该 cell 结束。
#[cfg(feature = "tokio")]
pub(crate) struct Waiting<'a> {
    signal: &'a Waiters,
    wait: Option<WaitId>,
}

#[cfg(feature = "tokio")]
impl std::future::Future for Waiting<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use std::task::Poll;

        let this = self.get_mut();
        if this.signal.is_fired() {
            return Poll::Ready(());
        }
        match this.wait {
            Some(id) => this.signal.update(id, cx.waker()),
            None => {
                this.wait = this.signal.register(cx.waker());
                if this.wait.is_none() {
                    return Poll::Ready(());
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(feature = "tokio")]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.wait {
            self.signal.release(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker_ref;

    #[test]
    fn register_then_release_returns_slot() {
        let waiters = Waiters::new();
        let waker = noop_waker_ref();

        let id = waiters.register(waker).expect("未触发时可注册");
        assert_eq!(waiters.waiter_count(), 1);

        waiters.release(id);
        assert_eq!(waiters.waiter_count(), 0);
    }

    #[test]
    fn slots_are_reused_not_accumulated() {
        let waiters = Waiters::new();
        let waker = noop_waker_ref();

        for _ in 0..1000 {
            let id = waiters.register(waker).expect("未触发时可注册");
            waiters.release(id);
        }

        // 顺序注册/注销一千次，槽位应复用为 1，而不是随历史累计。
        assert_eq!(waiters.slot_count(), 1);
    }

    #[test]
    fn register_after_fire_returns_none() {
        let waiters = Waiters::new();
        waiters.fire_all();

        assert!(waiters.is_fired());
        assert!(waiters.register(noop_waker_ref()).is_none());
    }

    #[test]
    fn release_after_fire_is_noop() {
        let waiters = Waiters::new();
        let id = waiters.register(noop_waker_ref()).expect("可注册");

        waiters.fire_all();
        waiters.release(id);

        // 触发后全部注册项已由 `fire_all` 取走，不应再被释放路径影响。
        assert_eq!(waiters.waiter_count(), 0);
    }

    #[test]
    fn stale_release_does_not_clear_reused_slot() {
        let waiters = Waiters::new();
        let waker = noop_waker_ref();

        let first = waiters.register(waker).expect("可注册");
        waiters.release(first);
        let second = waiters.register(waker).expect("可注册");
        assert_eq!(waiters.waiter_count(), 1);

        // 旧身份的释放请求必须因代际不符被忽略，不能摘掉复用同一槽位的新等待者。
        waiters.release(first);
        assert_eq!(waiters.waiter_count(), 1);

        waiters.release(second);
        assert_eq!(waiters.waiter_count(), 0);
    }

    #[test]
    fn same_slot_update_keeps_single_entry() {
        let waiters = Waiters::new();
        let waker = noop_waker_ref();

        let id = waiters.register(waker).expect("可注册");
        waiters.update(id, waker);
        waiters.update(id, waker);

        // 同一个 future 被反复 poll 只更新 waker，不新增条目。
        assert_eq!(waiters.waiter_count(), 1);
        assert_eq!(waiters.slot_count(), 1);
    }
}
