//! 框架身份类型与停止阻塞报告。
//!
//! 本模块是全 crate 的依赖叶子：只有不透明 newtype 与纯数据，不 import 任何框架
//! 概念。身份类型放这里而不是 [`crate::context`]，是为了让最底层的
//! [`crate::error`] / [`crate::event`] 不必反向依赖高层模块的装配过程。

/// 作用域身份；进程内全局唯一。
///
/// 由 [`crate::Context::id`] / [`crate::Builder::id`] 返回，也与
/// [`crate::Context::children`] 列出的值同源。
///
/// 不透明 newtype：与 [`TaskId`] 在类型上不可互换，把作用域 id 误传到任务 id 的
/// 位置会直接编译失败。构造器不公开，只能由框架产生；需要读取数值时用
/// [`ScopeId::get`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct ScopeId(usize);

impl ScopeId {
    /// 框架内构造；对外不公开。
    pub(crate) const fn new(raw: usize) -> Self {
        Self(raw)
    }

    /// 取底层数值。
    pub const fn get(self) -> usize {
        self.0
    }
}

impl std::fmt::Display for ScopeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scope#{}", self.0)
    }
}

/// 后台任务身份；在所属作用域内唯一。
///
/// 由 [`crate::TaskHandle::id`] 返回，并与 [`crate::TaskFailed::task_id`]、
/// [`crate::ErrorKind::TaskFailed`] / [`crate::ErrorKind::TaskAborted`] 携带的值一致。
///
/// 与 [`ScopeId`] 同理是不透明 newtype，两者不可互换。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TaskId(u64);

impl TaskId {
    /// 框架内构造；对外不公开。
    ///
    /// 无 `tokio` 构建下不存在任何构造路径（`spawn` 不编译），因此整条构造器按
    /// feature 收窄——「未使用」在这里是编译期可知的，不需要 `allow(dead_code)`。
    #[cfg(feature = "tokio")]
    pub(crate) const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// 取底层数值。
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "task#{}", self.0)
    }
}

/// 停止被活跃子作用域阻塞时，框架报告的阻塞方清单。
///
/// 每一项是「尚未归还租约」的子作用域 [`ScopeId`]——包含尚未 `build` 的子
/// `Builder`，以及尚未走完停止流程的子 `Runtime`。不变式是单向的：列出的必未
/// `Stopped`；不在清单 ⇒ 租约已归还（父 `stop` 不会再被它阻塞）。清单与
/// [`crate::Context::children`] 同源、在同一临界区内取，因此与实际阻塞集合一致。
///
/// [`crate::Runtime::stop`] 的阻塞报告用 [`Blockers::describe`]；`Runtime` 的 drop
/// 护栏叙事更细（区分「阻塞方已回收但没重试 stop」与「仍有活跃子级」，后者附带
/// 处置指引），由 `Runtime::drop_diagnostic` 单独实现，**不**复用 `describe`。
#[derive(Debug, Clone)]
pub struct Blockers {
    ids: Vec<ScopeId>,
}

impl Blockers {
    pub(crate) fn new(ids: Vec<ScopeId>) -> Self {
        Self { ids }
    }

    /// 阻塞方的作用域 id 清单。
    pub fn ids(&self) -> &[ScopeId] {
        &self.ids
    }

    /// 阻塞方数量。
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// 是否为空；`Blockers` 不会以空清单形态被构造，此方法仅为 API 完整。
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// 统一的根因叙事。
    pub fn describe(&self) -> String {
        format!("active scopes: {}, ids: {:?}", self.ids.len(), self.ids)
    }
}

impl std::fmt::Display for Blockers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.describe())
    }
}
