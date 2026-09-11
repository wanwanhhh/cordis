//! 受管子作用域注册表（可选工具，不属核心）。
//!
//! 解决的问题：常驻服务里会不停按名字开关子作用域（一个会话一个），而框架只给
//! 「开一个」「停一个」。下面这些并发正确性，每个应用都得自己重写一遍：
//!
//! - 同一个名字并发创建只能有一个赢家，其余拿到确定结果（不互相顶掉）；
//! - 每次关闭要带预算，并区分「正常 / 超时强杀 / 错误」；
//! - 启动失败的半成品要收干净，不留欠账；
//! - 停机时必须**先关光所有子级**，父级才停得下来（子级就是父级的欠账）。
//!
//! ## 所有权
//!
//! 子 `Runtime` 被 [`Mutex`] 里的 slot **独占**持有，从不以 `Arc<Runtime>` 形式外泄
//! ——`Runtime` 非 `Sync`、`stop` 需要 `&mut self`，共享执行权会直接破坏「所有权唯一」。
//! 对外只授出只读的 [`Context`] 句柄（`open` 的返回值），停止权永远留在注册表内。
//! 注册表本身因此可以按 `&self` 跨任务使用，但**不可 `Clone`**：它是 owner，不是
//! 一个到处拷贝的服务。
//!
//! ## 认领顺序（关键）
//!
//! `open` 先在锁内用 `HashMap::entry` 占住名字，**占住了才**调用 `ctx.scope()`。
//! 输家在 `entry` 阶段就失败、从未 `scope()`，因此不产生租约、更不用回滚一个已经
//! `start` 过的 `Runtime`——这正是手写版最难收敛的一段并发。
//!
//! 借出去的占位由 [`Reservation`] 的 `Drop` 兜底：`open` 的 future 若在
//! `build`/`start` 中途被丢弃，占位会被摘掉，不会永久泄漏那个名字。
//!
//! ## 已知边界：`open` 中途被丢弃
//!
//! 占位能收回；回收路径上的 `stop().await` 也由 [`ClosingGuard`] 兜底——future 在此
//! 被丢弃时 owner 以 `Closing(Some)` 留在表里，可 `close` 续跑。唯一没有兜底的是
//! `runtime.start().await`：`open` 的 future 在此期间被丢弃时，那个已进入启动流程的
//! `Runtime` 会随栈展开被 drop。框架的 `Runtime::drop` 在 debug 构建下把「进过启动
//! 流程却没到 `Stopped`」视为泄漏并硬失败（release 下静默）。这是 `Runtime` 的既有
//! 契约（owner 应当 `stop`），不是注册表引入的：要让创建具备取消安全性，请让 `open`
//! 跑完，或用外层超时把「创建」当成一个整体而不是丢弃它。

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::{Builder, Context, Error, ErrorKind, Runtime, StopOutcome};

/// 管理一批命名子作用域的 owner。
///
/// 生命周期：`new()` → `bind(parent)`（通常在父层 `on_ready` 里）→ `open` / `close`
/// / `close_all`。父层 `stop` 之前必须先 `close_all`，否则父层会被活跃子租约
/// `Blocked`。
pub struct ScopeRegistry {
    /// 绑定父 `Context`；只应绑定一次。
    parent: OnceLock<Context>,
    /// 登记表与停机闩**共用一把锁**。
    ///
    /// 这是本模块的核心线性化点：`open` 的「查停机 + 认领」与「发布」、`close_all`
    /// 的「置停机 + 取快照」都在同一临界区内，因此「停机开始之后仍发布出 `Running`」
    /// 不可表达——不存在跨两个同步域的检查窗口。
    state: Mutex<RegistryState>,
}

struct RegistryState {
    entries: HashMap<String, Slot>,
    /// 停机收口已开始：拒绝新 `open`，让收口有终点。
    shutting_down: bool,
}

enum Slot {
    /// 名字已认领，`Runtime` 尚未构建完成。
    Reserved,
    /// 已启动的成品；owner 独占持有。
    ///
    /// 装箱：`Runtime` 比 `Reserved` 大得多，放进 `HashMap` 的槽位会让整个表按最大
    /// 变体撑开。作用域数量少且长驻，一次装箱换来表内元素变小是划算的。
    Running(Box<Runtime>),
    /// 关闭进行中或上一轮被阻塞。名字**仍被占住**：`Running -> Closing` 是状态迁移，
    /// 不是把名字让出去，因此并发 `open` 同名只会得到 `AlreadyExists`。
    ///
    /// - `None`：本轮的 `Runtime` 已被取出，正在 `stop`（有 close 在飞）；
    /// - `Some`：上一轮 `stop` 返回 `Blocked`（子级还活着），owner 已放回，可重试。
    Closing(Option<Box<Runtime>>),
}

/// 注册表层面的失败。
#[derive(Debug)]
pub enum RegistryError {
    /// 尚未 `bind`，或重复 `bind`。
    Unbound,
    /// 该名字已存在（并发创建时的确定输家结果）。
    AlreadyExists(String),
    /// 名字不存在。
    NotFound(String),
    /// 该名字正被在飞的 `open`/`close` 占用（`Reserved` 或 `Closing`），不是「不存在」，
    /// 也不可立刻重试成功。
    Busy(String),
    /// 收口已开始，拒绝新作用域。
    ShuttingDown,
    /// 子作用域自身的装配/启动失败。
    Scope(Error),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unbound => write!(f, "registry is not bound to a parent context"),
            Self::AlreadyExists(name) => write!(f, "scope `{name}` already exists"),
            Self::NotFound(name) => write!(f, "scope `{name}` not found"),
            Self::Busy(name) => write!(f, "scope `{name}` is being opened or closed"),
            Self::ShuttingDown => write!(f, "registry is shutting down"),
            Self::Scope(err) => write!(f, "scope error: {err}"),
        }
    }
}

impl std::error::Error for RegistryError {}

/// 一次关闭的三分结局。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseStatus {
    /// 正常收尾，无错误。
    Clean,
    /// 预算耗尽，有任务被强制取消（`ErrorKind::TaskAborted`）。
    Aborted,
    /// 清理过程中有错误（插件 `stop` / dispose / panic 上报等）。
    Failed,
    /// 仍被活跃子作用域挡住（子级还没停）。这些子级不在本注册表内，需调用方处理。
    ///
    /// 这不是终态：被阻塞的子作用域**仍是注册表的条目**（`Closing`），owner 未被
    /// 析构，回收那些孙级后重试 `close` 即可续跑。
    Blocked,
}

/// 一次关闭的结局：进度（`StopOutcome`）+ 互斥摘要（`CloseStatus`）。
///
/// 摘要由内部 `status_of` 单点从 `StopOutcome` 推导，不新增第二套分类
/// 真相（「超时强杀」的唯一定义仍是被强制取消的 `ErrorKind::TaskAborted`）。
#[derive(Debug)]
pub struct CloseReport {
    /// 最终一轮 `stop` 的原始结局，保留完整错误链与阻塞方清单。
    pub outcome: StopOutcome,
    /// 从 `outcome` 推导的三分摘要。
    pub status: CloseStatus,
}

impl CloseReport {
    fn derive(outcome: StopOutcome) -> Self {
        let status = Self::status_of(&outcome);
        Self { outcome, status }
    }

    fn status_of(outcome: &StopOutcome) -> CloseStatus {
        if outcome.blockers().is_some() {
            return CloseStatus::Blocked;
        }
        let errors = outcome.errors();
        if errors
            .iter()
            .any(|err| matches!(err.kind, ErrorKind::TaskAborted { .. }))
        {
            // 超时强杀优先于「有错」：它解释了那些错误里的取消成因。
            return CloseStatus::Aborted;
        }
        if errors.is_empty() {
            CloseStatus::Clean
        } else {
            CloseStatus::Failed
        }
    }

    /// 是否干净收尾。
    pub fn is_clean(&self) -> bool {
        self.status == CloseStatus::Clean
    }

    /// 被预算强制取消的任务 id。
    pub fn aborted_tasks(&self) -> Vec<crate::TaskId> {
        self.outcome
            .errors()
            .iter()
            .filter_map(|err| match err.kind {
                ErrorKind::TaskAborted { task_id } => Some(task_id),
                _ => None,
            })
            .collect()
    }
}

/// 批量收口的汇总。
#[derive(Debug, Default)]
pub struct CloseAllReport {
    /// 逐个名字的结局（成功取到 owner 并执行了 `stop` 的）。
    ///
    /// 包含 `CloseStatus::Blocked` 的条目——它们**不计为失败**：owner 仍留在注册表
    /// 里，回收孙级后重试即可。
    pub closed: Vec<(String, CloseReport)>,
    /// 连 `stop` 都没能执行的（例如遇到在飞的 `open`/`close` 而拿到 `Busy`）。
    pub failed: Vec<(String, RegistryError)>,
}

impl CloseAllReport {
    /// 是否全部干净关闭。
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty() && self.closed.iter().all(|(_, report)| report.is_clean())
    }
}

/// 名字的占位守卫：`open` 中途被丢弃时摘掉占位，避免名字永久泄漏。
struct Reservation<'a> {
    registry: &'a ScopeRegistry,
    name: &'a str,
    claimed: bool,
}

impl Reservation<'_> {
    /// 占位已转为成品，不再需要摘除。
    fn disarm(&mut self) {
        self.claimed = false;
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.claimed {
            self.registry.lock().entries.remove(self.name);
        }
    }
}

/// 取出 `Runtime` 执行 `stop` 期间的**归还守卫**（`close` 与 `open` 的回收路径共用）。
///
/// 调用方必须把 `Runtime` 移出登记表才能调用 `&mut self` 的 `stop`；这段「已移出、
/// 尚未到终态」的所有权若不设防，future 在 `await` 点被取消时会随栈展开析构
/// `Runtime`（debug 命中 Drop 护栏、release 静默跳过插件 `stop`），并在表里留下
/// 无人可取的 `Closing(None)` —— 该名字此后永久 `Busy`、不可再关闭。这与 `open`
/// 的 [`Reservation`] 是同一个问题的两种形态。
///
/// 守卫在取消时把 `Runtime` 放回 `Closing(Some)`，使状态等价于 `Blocked`：owner 仍在
/// 注册表内、`stop` 的自持 future 也还在 `Runtime` 里，重试 `close` 即续跑。
struct ClosingGuard<'a> {
    registry: &'a ScopeRegistry,
    name: &'a str,
    runtime: Option<Runtime>,
    armed: bool,
}

impl<'a> ClosingGuard<'a> {
    fn new(registry: &'a ScopeRegistry, name: &'a str, runtime: Runtime) -> Self {
        Self {
            registry,
            name,
            runtime: Some(runtime),
            armed: true,
        }
    }

    fn runtime_mut(&mut self) -> &mut Runtime {
        self.runtime
            .as_mut()
            .expect("closing guard holds the runtime until finalized")
    }

    /// 非终态收尾：把 owner 放回槽位，守卫不再兜底。
    ///
    /// 用 `insert` 而不是「就地覆盖已存在槽位」：`open` 在发布被拒后已把条目摘掉，
    /// 取消点还原时必须**重新占住名字**，否则 owner 无处安放会随守卫析构（正是本
    /// 守卫要消除的）并把名字让给别人。
    fn restore(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            self.registry.lock().entries.insert(
                self.name.to_string(),
                Slot::Closing(Some(Box::new(runtime))),
            );
        }
        self.armed = false;
    }

    /// 终态收尾：放开名字，owner 随本守卫析构（此时已 `Stopped`，析构正常）。
    fn discard(&mut self) {
        self.registry.lock().entries.remove(self.name);
        self.armed = false;
    }
}

impl Drop for ClosingGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            // 取消：等价于 `Blocked`，把 owner 放回供重试。
            self.restore();
        }
    }
}

impl Default for ScopeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeRegistry {
    /// 创建一个空注册表。
    pub fn new() -> Self {
        Self {
            parent: OnceLock::new(),
            state: Mutex::new(RegistryState {
                entries: HashMap::new(),
                shutting_down: false,
            }),
        }
    }

    /// 绑定父 `Context`；只应调用一次（通常在父层 `on_ready`）。
    pub fn bind(&self, parent: &Context) -> Result<(), RegistryError> {
        self.parent
            .set(parent.clone())
            .map_err(|_| RegistryError::Unbound)
    }

    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn parent(&self) -> Result<&Context, RegistryError> {
        self.parent.get().ok_or(RegistryError::Unbound)
    }

    /// 按名字创建并启动一个子作用域，返回**只读**句柄。
    ///
    /// 并发同名创建只有一个赢家：输家在认领阶段就拿到 [`RegistryError::AlreadyExists`]，
    /// 且从未调用 `scope()`（不产生租约）。
    ///
    /// `configure` 用于在子 `Builder` 上注册会话级服务与插件。构建或启动失败时，
    /// 半成品会被回收（`stop` + drop），名字被释放，可重试。
    ///
    /// 例外：回收 `stop` 若被活跃孙作用域阻塞（`Blocked`，插件在 `start` 里泄漏了
    /// 子作用域），owner 以 `Closing(Some)` 保留、**名字不释放**，再 `open` 同名会得
    /// [`RegistryError::AlreadyExists`]；调用方须先回收孙级，再对同一名字 `close` 续跑。
    pub async fn open<F>(
        &self,
        name: impl Into<String>,
        configure: F,
    ) -> Result<Context, RegistryError>
    where
        F: FnOnce(&mut Builder) -> Result<(), Error>,
    {
        let name = name.into();
        let parent = self.parent()?.clone();

        // 第一步：锁内「查停机 + 认领名字」。占住才继续——输家到此为止，不产生
        // 任何作用域。停机闩与登记表同锁，因此这里与 `close_all` 的置位互为线性化点。
        {
            let mut state = self.lock();
            if state.shutting_down {
                return Err(RegistryError::ShuttingDown);
            }
            if state.entries.contains_key(&name) {
                return Err(RegistryError::AlreadyExists(name));
            }
            state.entries.insert(name.clone(), Slot::Reserved);
        }
        let mut reservation = Reservation {
            registry: self,
            name: &name,
            claimed: true,
        };

        // 第二步：构建并启动。中途被丢弃时由 `Reservation::drop` 摘掉占位；即使
        // `build`/`start` 失败，占位也随 `reservation` 析构归还。
        let mut builder = match parent.scope() {
            Ok(builder) => builder,
            Err(err) => return Err(RegistryError::Scope(err)),
        };
        if let Err(err) = configure(&mut builder) {
            return Err(RegistryError::Scope(err));
        }
        let mut runtime = match builder.build() {
            Ok(runtime) => runtime,
            Err(err) => return Err(RegistryError::Scope(err)),
        };
        if let Err(start_err) = runtime.start().await {
            // 启动失败的半成品：已启动的插件仍待回收，必须 stop，否则 `Runtime::drop`
            // 的 debug 护栏会因「进过启动流程却没到 Stopped」硬失败。
            //
            // 名字的处置权交给守卫：`stop` 返回 `Blocked`（插件在 `start` 里创建并
            // 泄漏了子作用域）时放回 `Closing(Some)`（调用方可 `close` 续跑），终态才
            // 摘名；`stop` 的 await 点被取消时守卫的 Drop 同样放回，不析构 owner。
            // 因此必须先 `disarm` 占位守卫，否则它在 `open` 返回时会把条目又摘掉。
            reservation.disarm();
            let mut guard = ClosingGuard::new(self, &name, runtime);
            let outcome = guard.runtime_mut().stop().await;
            if outcome.blockers().is_some() {
                guard.restore();
            } else {
                guard.discard();
            }
            return Err(RegistryError::Scope(start_err));
        }

        let ctx = runtime.handle();

        // 第三步：锁内复查停机后再发布。这是收口线性化点的另一半——若 `close_all`
        // 已在本次 `start().await` 期间置位，就**不能**把 `Running` 发布进一个已经
        // 停机的注册表；就地摘掉占位并取回刚建好的 `Runtime`，**锁外**再回收。
        let rejected: Option<Runtime> = {
            let mut state = self.lock();
            if state.shutting_down {
                state.entries.remove(&name);
                Some(runtime)
            } else {
                match state.entries.get_mut(&name) {
                    Some(slot @ Slot::Reserved) => {
                        *slot = Slot::Running(Box::new(runtime));
                        None
                    }
                    // 不可能：占位只有本流程会替换，且 `Reservation` 尚未 disarm。
                    Some(Slot::Running(_) | Slot::Closing(_)) => {
                        debug_assert!(false, "reserved slot replaced before publish");
                        None
                    }
                    None => {
                        debug_assert!(false, "reservation vanished before publish");
                        None
                    }
                }
            }
        };
        reservation.disarm();
        if let Some(runtime) = rejected {
            // 回收刚建好的 `Runtime`；同样由守卫持有——`stop` 返回 `Blocked` 时保留为
            // `Closing(Some)` 供调用方 `close` 续跑（此时条目已被摘掉，守卫会重新占
            // 住名字），await 点被取消时也不析构 owner。
            let mut guard = ClosingGuard::new(self, &name, runtime);
            let outcome = guard.runtime_mut().stop().await;
            if outcome.blockers().is_some() {
                guard.restore();
            } else {
                guard.discard();
            }
            return match outcome.into_result() {
                Ok(()) => Err(RegistryError::ShuttingDown),
                // 收尾失败不静默：宁可把它作为本次 `open` 的错误上报。
                Err(err) => Err(RegistryError::Scope(err)),
            };
        }

        Ok(ctx)
    }

    /// 关闭并回收一个子作用域，返回三分结局。
    ///
    /// 锁内把 `Running` 迁到 `Closing(None)`（名字全程被占住）并取出 owned
    /// `Runtime` → 释放锁 → `stop_with_timeout`（不在持锁期间 await）。
    ///
    /// 结局**非终态**时不得析构 owner：`Blocked` 表示子级还活着，`Runtime` 会被
    /// 放回 `Closing(Some(..))` 供调用方回收孙级后重试——把它当终态 `drop` 会命中
    /// `Runtime` 的 Drop 护栏（debug）或静默跳过插件 `stop`（release）。
    pub async fn close(&self, name: &str, budget: Duration) -> Result<CloseReport, RegistryError> {
        let runtime = {
            let mut state = self.lock();
            match state.entries.get_mut(name) {
                None => return Err(RegistryError::NotFound(name.to_string())),
                Some(Slot::Reserved | Slot::Closing(None)) => {
                    return Err(RegistryError::Busy(name.to_string()));
                }
                Some(slot) => match std::mem::replace(slot, Slot::Closing(None)) {
                    Slot::Running(runtime) => *runtime,
                    Slot::Closing(Some(runtime)) => *runtime,
                    // 上面两个 arm 已排除。
                    Slot::Reserved | Slot::Closing(None) => unreachable!(),
                },
            }
        };

        // 由守卫持有 owner：`close` 的 future 若在 `stop` 的 await 点被丢弃，守卫把
        // `Runtime` 放回 `Closing(Some)`（等价 `Blocked`，可重试），而不是随栈展开
        // 析构它并留下永久 `Busy` 的空槽。
        let mut guard = ClosingGuard::new(self, name, runtime);
        let outcome = guard.runtime_mut().stop_with_timeout(budget).await;
        let report = CloseReport::derive(outcome);

        if report.status == CloseStatus::Blocked {
            guard.restore();
        } else {
            guard.discard();
        }
        Ok(report)
    }

    /// 停机收口：拒绝新 `open`，逐个关闭全部子作用域。
    ///
    /// **串行**关闭：子作用域是父层的租约，收口的正确顺序就是「子级全停 → 父级才停」。
    /// 本方法只负责把注册表管着的直接子级关光；它们各自的孙级由它们自己负责。
    ///
    /// 返回后保证：注册表里**不存在 `Running` 条目**。被孙级阻塞的子级以
    /// `Blocked` 报告并保留 owner（`Closing(Some)`），调用方须先回收那些孙级再重试
    /// `close`；在飞 `open` 的占位（`Reserved`）会因发布时复查停机而自行清理，不会
    /// 在返回后变成新的 `Running`。
    ///
    /// 但「返回后立刻 `stop` 父级不会 `Blocked`」只在**没有在飞 `open`** 时成立：
    /// 一个已认领、尚未发布的 `open` 可能已经 `ctx.scope()` 出了子作用域，即已持有
    /// 父级租约，而它要等到发布复查停机、走回滚 `stop` 才归还。若父级在这段窗口里
    /// `stop`，会瞬时返回 `Blocked`（等那个 `open` 回滚完成即消失）。要严格保证一次
    /// 停干净，调用方应先确认所有 `open` future 已结束，或对父级 `stop` 的 `Blocked`
    /// 做一次重试。
    pub async fn close_all(&self, budget: Duration) -> CloseAllReport {
        // 置停机与取快照在同一临界区：这是「返回后不再出现可管理子级」的线性化点。
        let names: Vec<String> = {
            let mut state = self.lock();
            state.shutting_down = true;
            state.entries.keys().cloned().collect()
        };

        let mut report = CloseAllReport::default();
        for name in names {
            match self.close(&name, budget).await {
                Ok(item) => report.closed.push((name, item)),
                Err(err) => report.failed.push((name, err)),
            }
        }
        report
    }

    /// 取出一个已存在作用域的只读句柄。
    ///
    /// 只有 `Running` 才授出：`Reserved` 尚未启动，`Closing` 已进入不可逆的关闭流程
    /// （拒绝新 `scope`/`spawn`），把它的句柄交给应用会造成误用。
    pub fn get(&self, name: &str) -> Option<Context> {
        self.lock().entries.get(name).and_then(|slot| match slot {
            Slot::Running(runtime) => Some(runtime.handle()),
            Slot::Reserved | Slot::Closing(_) => None,
        })
    }

    /// 当前登记的名字清单（含仍在构建中 / 正在关闭的条目）。
    pub fn names(&self) -> Vec<String> {
        self.lock().entries.keys().cloned().collect()
    }

    /// 是否已开始停机收口。
    pub fn is_shutting_down(&self) -> bool {
        self.lock().shutting_down
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::{Phase, Plugin};

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(future)
    }

    /// 启动即失败的插件，用来验证「半成品回收 + 名字释放」。
    struct FailStart;

    #[async_trait]
    impl Plugin for FailStart {
        fn name(&self) -> &'static str {
            "fail-start"
        }

        async fn start(&self, _ctx: &Context) -> Result<(), Error> {
            Err(Error::new(Phase::Start, ErrorKind::Other))
        }
    }

    /// 启动时挂一个长驻任务，用来验证超时强杀被归类为 `Aborted`。
    struct HangingTask;

    #[async_trait]
    impl Plugin for HangingTask {
        fn name(&self) -> &'static str {
            "hanging-task"
        }

        async fn start(&self, ctx: &Context) -> Result<(), Error> {
            ctx.spawn(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(())
            })?;
            Ok(())
        }
    }

    /// 建一个已启动的根作用域 + 绑定到它的注册表。
    async fn fixture() -> (Runtime, ScopeRegistry) {
        let mut root = crate::Builder::new().build().unwrap();
        root.start().await.unwrap();
        let registry = ScopeRegistry::new();
        registry.bind(&root.handle()).unwrap();
        (root, registry)
    }

    #[test]
    fn open_then_close_is_clean() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            let ctx = registry.open("s1", |_| Ok(())).await.unwrap();
            assert!(ctx.id() == registry.get("s1").unwrap().id());
            assert_eq!(root.handle().children().len(), 1);

            let report = registry.close("s1", Duration::from_secs(1)).await.unwrap();
            assert_eq!(report.status, CloseStatus::Clean);
            assert!(report.is_clean());
            // 关闭后父层欠账归零。
            assert!(root.handle().children().is_empty());

            root.stop().await.into_result().unwrap();
        });
    }

    #[test]
    fn duplicate_open_is_rejected_without_leaking_a_lease() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            registry.open("s1", |_| Ok(())).await.unwrap();

            let err = match registry.open("s1", |_| Ok(())).await {
                Err(err) => err,
                Ok(_) => panic!("duplicate name must be rejected"),
            };
            assert!(matches!(err, RegistryError::AlreadyExists(ref n) if n == "s1"));
            // 输家从未 scope()，因此没有多出任何租约。
            assert_eq!(root.handle().children().len(), 1);

            registry.close_all(Duration::from_secs(1)).await;
            root.stop().await.into_result().unwrap();
        });
    }

    #[test]
    fn concurrent_open_has_exactly_one_winner() {
        block_on(async {
            let (mut root, registry) = fixture().await;

            let (a, b) = tokio::join!(
                registry.open("s1", |_| Ok(())),
                registry.open("s1", |_| Ok(())),
            );
            assert!(a.is_ok() ^ b.is_ok(), "同名并发创建必须恰好一个赢家");
            let loser = if a.is_ok() { b } else { a };
            assert!(matches!(loser, Err(RegistryError::AlreadyExists(_))));

            registry.close_all(Duration::from_secs(1)).await;
            root.stop().await.into_result().unwrap();
        });
    }

    #[test]
    fn failed_start_releases_the_name_for_retry() {
        block_on(async {
            let (mut root, registry) = fixture().await;

            let err = match registry
                .open("s1", |builder| builder.plugin(FailStart))
                .await
            {
                Err(err) => err,
                Ok(_) => panic!("failing plugin start must surface as an error"),
            };
            assert!(matches!(err, RegistryError::Scope(_)));
            // 半成品被收干净，名字已释放，可以重试。
            assert!(registry.names().is_empty());
            assert!(root.handle().children().is_empty());

            registry.open("s1", |_| Ok(())).await.unwrap();
            registry.close_all(Duration::from_secs(1)).await;
            root.stop().await.into_result().unwrap();
        });
    }

    #[test]
    fn close_all_lets_parent_stop() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            for i in 0..3 {
                registry.open(format!("s{i}"), |_| Ok(())).await.unwrap();
            }
            assert_eq!(root.handle().children().len(), 3);

            let report = registry.close_all(Duration::from_secs(1)).await;
            assert_eq!(report.closed.len(), 3);
            assert!(report.is_clean(), "{report:?}");

            // 子级全关光，父级不会被 Blocked。
            let outcome = root.stop().await;
            assert!(outcome.is_stopped(), "{outcome:?}");
        });
    }

    #[test]
    fn open_after_shutdown_is_rejected() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            registry.close_all(Duration::from_secs(1)).await;
            assert!(registry.is_shutting_down());

            assert!(matches!(
                registry.open("late", |_| Ok(())).await,
                Err(RegistryError::ShuttingDown)
            ));
            root.stop().await.into_result().unwrap();
        });
    }

    #[test]
    fn close_reports_aborted_when_a_task_exceeds_budget() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            registry
                .open("hangs", |builder| builder.plugin(HangingTask))
                .await
                .unwrap();

            let report = registry
                .close("hangs", Duration::from_millis(20))
                .await
                .unwrap();
            assert_eq!(report.status, CloseStatus::Aborted, "{report:?}");
            assert_eq!(report.aborted_tasks().len(), 1);

            root.stop().await.into_result().unwrap();
        });
    }

    /// 被孙作用域阻塞的子级：`close` 必须如实返回 `Blocked` 并**保留 owner**，
    /// 而不是把仍在 `Closing` 的 `Runtime` 析构掉（那会命中 Drop 护栏或静默跳过
    /// 插件 `stop`）；回收孙级后重试即可续跑。
    #[test]
    fn close_child_with_active_grandchild_reports_blocked() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            let child_ctx = registry.open("child", |_| Ok(())).await.unwrap();
            // 孙级 Builder 持有 child 的租约；不 build、不 drop 即保持活跃。
            let grandchild = child_ctx.scope().unwrap();

            let report = registry
                .close("child", Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(report.status, CloseStatus::Blocked, "{report:?}");
            // owner 未被析构：名字仍在册，但句柄不再授出（已进入关闭流程）。
            assert!(registry.names().iter().any(|n| n == "child"));
            assert!(registry.get("child").is_none());
            assert_eq!(root.handle().children().len(), 1, "child 仍持有父租约");

            // 回收孙级后重试：同一 owner 续跑清理。
            drop(grandchild);
            let report = registry
                .close("child", Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(report.status, CloseStatus::Clean, "{report:?}");
            assert!(root.handle().children().is_empty());

            root.stop().await.into_result().unwrap();
        });
    }

    /// 在飞 `open` 与 `close_all` 交错：`close_all` 置停机后，那个已认领但尚未发布
    /// 的 `open` 必须拒绝发布并自行回收，否则父层会被一条无人收口的租约永久阻塞。
    #[test]
    fn open_during_close_all_cannot_publish() {
        struct SlowStart {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl Plugin for SlowStart {
            fn name(&self) -> &'static str {
                "slow-start"
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            }
        }

        block_on(async {
            let (mut root, registry) = fixture().await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());

            // 先让 `open` 跑到插件的 `start`（此时名字是 `Reserved`、租约已存在），
            // 再在同一任务里调 `close_all`——`Notify` 会保存 permit，顺序确定。
            let (opened, _closed) = tokio::join!(
                registry.open("late", {
                    let entered = entered.clone();
                    let release = release.clone();
                    move |builder: &mut Builder| builder.plugin(SlowStart { entered, release })
                }),
                async {
                    entered.notified().await;
                    let report = registry.close_all(Duration::from_secs(1)).await;
                    release.notify_one();
                    report
                }
            );

            assert!(
                matches!(opened, Err(RegistryError::ShuttingDown)),
                "停机后不得再发布作用域"
            );
            assert!(registry.get("late").is_none());
            assert!(
                root.handle().children().is_empty(),
                "被拒绝的 open 必须自行回收，父层租约归零"
            );
            root.stop().await.into_result().unwrap();
        });
    }

    /// `close` 的 future 在 `stop` 的 await 点被取消：owner 不得被析构，名字不得
    /// 永久 `Busy`——守卫把 `Runtime` 放回 `Closing(Some)`，重试即续跑。
    #[test]
    fn close_cancellation_keeps_scope_recoverable() {
        struct HangingStop {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl Plugin for HangingStop {
            fn name(&self) -> &'static str {
                "hanging-stop"
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            }
        }

        block_on(async {
            let (mut root, registry) = fixture().await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            registry
                .open("s", {
                    let entered = entered.clone();
                    let release = release.clone();
                    move |builder: &mut Builder| builder.plugin(HangingStop { entered, release })
                })
                .await
                .unwrap();

            // `close` 停在插件 `stop` 上时被超时取消（future 被丢弃）。
            let cancelled = tokio::time::timeout(
                Duration::from_millis(50),
                registry.close("s", Duration::from_secs(3600)),
            )
            .await;
            assert!(cancelled.is_err(), "插件 stop 挂起，close 应被取消");
            // 确认取消确实发生在插件 `stop` 内部（permit 未被消费）。
            assert!(
                futures::FutureExt::now_or_never(entered.notified()).is_some(),
                "close 必须曾进入插件 stop 才谈得上取消清理"
            );

            // 取消后 owner 仍在：不是永久 Busy，重试可续跑同一次清理。
            release.notify_one();
            let report = registry.close("s", Duration::from_secs(1)).await.unwrap();
            assert_eq!(report.status, CloseStatus::Clean, "{report:?}");
            assert!(root.handle().children().is_empty());

            root.stop().await.into_result().unwrap();
        });
    }

    /// `open` 的启动失败清理遇到被活跃子作用域阻塞的 `stop`：不得析构 owner
    /// （debug 会命中 Drop 护栏），应保留在 `Closing` 供调用方回收孙级后 `close`。
    #[test]
    fn failed_start_with_leaked_child_does_not_destroy_owner() {
        use std::sync::Mutex as StdMutex;

        struct LeakyStart {
            child: Arc<StdMutex<Option<Builder>>>,
        }

        #[async_trait]
        impl Plugin for LeakyStart {
            fn name(&self) -> &'static str {
                "leaky-start"
            }

            async fn start(&self, ctx: &Context) -> Result<(), Error> {
                // 在 start 里创建子作用域并持有（模拟泄漏），随后让 start 失败。
                *self.child.lock().unwrap() = Some(ctx.scope().unwrap());
                Err(Error::new(Phase::Start, crate::ErrorKind::Other))
            }
        }

        block_on(async {
            let (mut root, registry) = fixture().await;
            let child = Arc::new(StdMutex::new(None));
            let open_child = child.clone();

            let err = match registry
                .open("leaky", move |builder: &mut Builder| {
                    builder.plugin(LeakyStart { child: open_child })
                })
                .await
            {
                Err(err) => err,
                Ok(_) => panic!("start 失败必须返回错误"),
            };
            assert!(matches!(err, RegistryError::Scope(_)), "{err:?}");
            // owner 未被析构：名字仍占住，但不是永久不可关闭。
            assert!(registry.names().iter().any(|n| n == "leaky"));
            assert!(registry.get("leaky").is_none());

            // 回收被泄漏的孙级后，重试 `close` 即可续跑。
            drop(child.lock().unwrap().take());
            let report = registry
                .close("leaky", Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(report.status, CloseStatus::Clean, "{report:?}");
            assert!(root.handle().children().is_empty());

            root.stop().await.into_result().unwrap();
        });
    }

    /// `open` 的启动失败回收停在 `stop` 的 await 点被取消：owner 同样不得被析构，
    /// 名字也不得丢失——守卫把它放回 `Closing(Some)`，`close` 可续跑同一次清理。
    #[test]
    fn failed_start_cancellation_keeps_owner_recoverable() {
        struct FailThenHang {
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl Plugin for FailThenHang {
            fn name(&self) -> &'static str {
                "fail-then-hang"
            }

            async fn start(&self, _ctx: &Context) -> Result<(), Error> {
                Err(Error::new(Phase::Start, crate::ErrorKind::Other))
            }

            async fn stop(&self, _ctx: &Context) -> Result<(), Error> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(())
            }
        }

        block_on(async {
            let (mut root, registry) = fixture().await;
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());

            let cancelled = tokio::time::timeout(
                Duration::from_millis(50),
                registry.open("s", {
                    let entered = entered.clone();
                    let release = release.clone();
                    move |builder: &mut Builder| builder.plugin(FailThenHang { entered, release })
                }),
            )
            .await;
            assert!(cancelled.is_err(), "回收 stop 挂起，open 应被取消");
            assert!(
                futures::FutureExt::now_or_never(entered.notified()).is_some(),
                "必须曾进入插件 stop 才谈得上取消清理"
            );

            // 取消后 owner 仍在册（不是丢名字、也不是永久 Reserved）。
            assert!(registry.names().iter().any(|n| n == "s"), "取消不得丢名字");
            assert!(registry.get("s").is_none());

            release.notify_one();
            let report = registry.close("s", Duration::from_secs(1)).await.unwrap();
            assert_eq!(report.status, CloseStatus::Clean, "{report:?}");
            assert!(root.handle().children().is_empty());

            root.stop().await.into_result().unwrap();
        });
    }

    /// 注册表必须能跨 `spawn` 的任务共享 `&self`：内部可变性 + 只授只读句柄。
    #[test]
    fn registry_shared_by_reference_across_tasks() {
        block_on(async {
            let (mut root, registry) = fixture().await;
            let counter = Arc::new(AtomicUsize::new(0));
            let tasks: Vec<_> = (0..4)
                .map(|i| {
                    let counter = counter.clone();
                    let registry = &registry;
                    async move {
                        registry.open(format!("s{i}"), |_| Ok(())).await.unwrap();
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                })
                .collect();
            futures::future::join_all(tasks).await;
            assert_eq!(counter.load(Ordering::SeqCst), 4);

            registry.close_all(Duration::from_secs(1)).await;
            root.stop().await.into_result().unwrap();
        });
    }
}
