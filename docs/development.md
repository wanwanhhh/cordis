# Cordis 开发规范

> 给维护者：改动本 crate 时必须遵守的约束、流程与测试要求。

---

## 1. 模块结构

```text
src/
  lib.rs        # 公共导出与文档
  context.rs    # Builder / Context / Runtime / Configurator / ScopeLease
  error.rs      # Error / ErrorKind / Phase
  event.rs      # Event / EventHandler / Subscription
  plugin.rs     # Plugin / Dependency / PluginDependency
  service.rs    # ServiceRegistry
```

新增能力优先放在对应模块；跨模块类型通过 crate 根导出统一公开。

---

## 2. 核心不变量

1. **冻结点唯一**
   - `Data` 只有在其所属 `Builder::build()` 中 `Arc::new` 一次。
   - 冻结后不存在框架可见的 `&mut Data` 路径。

2. **`Context` 只读**
   - `Context` 上没有 `provide` / `plugin` / `on` / `off` / `start` / `stop` / `require_mut`。
   - `scope()` 是 `Context` 上唯一的构建入口，但它只递增父计数并返回新 `Builder`，不修改既有 `Data`。
   - 所有写操作只存在于 `Builder` 或 `Configurator`。

3. **`Runtime` 不 Clone**
   - `Runtime` 是生命周期唯一所有者。
   - 控制方法必须是 `&mut self`。

4. **零 `unsafe`**
   - 禁止引入 `unsafe`。
   - 不用 `ManuallyDrop` 等绕开析构顺序的工具。

5. **内部可变白名单**
   - 只允许 `OnceLock`（懒工厂）、`Data.state: AtomicU64`（scope 计数/停止位）与 `DynamicValue` 服务内部的 `RwLock`。
   - 不允许在 `Data` 本身放 `Mutex<Vec<...>>` 作为通用可变通道；运行期可变配置必须通过 `DynamicValue` 暴露。

6. **租约字段序**
   - `ScopeLease` 必须是 `Runtime` 最后一个声明字段。
   - 不在 `impl Drop for Runtime` 方法体里做 `fetch_sub`。
   - 必须保留 `scope_lease_release_happens_after_child_plugin_drop` 探针测试。

---

## 3. 生命周期状态机

- 仅当 `!started && !stopped` 时真正启动；`started || stopped` 时 `start` 为 no-op。
- `ActiveScopes`（CAS 失败）不得置 `stopped`。
- CAS 一旦成功，即使后续 plugin.stop / dispose 返回错误，`stopped` 仍保持 true；重复 `stop` 为 no-op。
- 未 `start` 的 `stop` 只跑 dispose hooks，不调用插件 stop。

---

## 4. 依赖与排序

- `compute_start_order` 与 `compute_start_layers` 是唯一依赖解析入口。
- Builder 与 Runtime 不得各自复制一份拓扑实现。
- 可选依赖语义固定为：缺失可容忍，存在则必须按序启动。
- 分层切分必须包含空层防御，出现空层应返回 `PluginDependencyCycle`，禁止静默死循环。

---

## 5. 错误处理

- 所有框架错误使用结构化 `Error { phase, plugin, kind, source }`。
- 不新增 `to_string()` 压平的旧式错误变体。
- 单点错误用 `matches!(err.kind(), ...)`。
- 聚合错误使用 `ErrorKind::Multiple`，不要另造 `Error::Multiple` 变体。
- 嵌套插件失败时保留内层插件名；外层只补 phase，不覆盖已有 plugin。

---

## 6. 异步与取消安全

- 生命周期方法统一接收 `&Context`。
- `ready` / `dispose` 遍历时不得 `mem::take` + `await` 后放回，避免取消丢 hook。
- 事件 handler 不持有框架可变引用。

---

## 7. 测试要求

提交前必须通过：

```bash
cargo fmt --check
cargo clippy --all-targets
cargo test --all-targets
cargo test --doc
```

必须维持的测试类别：

- 插件/服务基本工作流
- `apply` 失败回滚（含事件、订阅 ID、嵌套插件）
- c-lite 租约四路：Builder drop / Runtime drop / try_build 失败回传 / build 成功转移
- 父 stop 与子 Runtime 存活
- ScopeLease 时序探针
- start-after-stop no-op
- 未 start 的 stop 只跑 dispose
- 可选依赖存在时仍按序启动
- 插件依赖环检测
- 事件串行/并行/冒泡/Bail/取消订阅
- 结构化错误 kind/phase/plugin

---

## 8. 公共 API 纪律

- 对外 API 变更需同步：
  - `README.md`
  - `docs/architecture.md`
  - `docs/USAGE.md`
- 删除旧 API 前确认无内部引用，且示例与测试全部迁移。
- 不导出 `Data` / `PluginRecord` / `ScopeLease` 等内部实现类型。
