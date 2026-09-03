# cordis

Rust 实现的 Cordis 风格插件化基础架构。

当前处于早期阶段，核心目标是构建一套可复用的 Rust 插件化/依赖注入底座。

## 文档

- [底层架构](docs/architecture.md)
- [开发规范](docs/development.md)
- [面向使用者的使用指南](docs/USAGE.md)

## 当前实现

- `Builder`（装配期独占 `&mut`）/ `Context`（只读数据句柄，Clone + Send + Sync）/ `Runtime`（生命周期唯一所有者）
- `Plugin`（异步 start / stop，`apply(&Configurator)`）
- `ServiceRegistry`（Send + Sync 服务）
- `LifecycleHook` / `SyncHook` / `AsyncHook`
- `FnEventHandler` / `AsyncFnEventHandler`
- 服务注册 / 获取
- `try_require` / `require_all` / `require_all_recursive`
- `Collection<T>` 多实现服务
- `provide_factory` 懒加载服务
- 父级服务继承与局部遮蔽
- `PluginScope` 插件作用域约束
- `provide_dynamic` / `require_dynamic` 运行时动态配置
- 插件依赖声明与检查
- 可选依赖
- 插件间依赖 `PluginDependency`
- 插件配置注入 `plugin_with_config`
- 插件元信息 / 优先级
- `Configurator` 窄接口与 `apply` 失败回滚
- 异步 ready / dispose 生命周期
- 启动失败 fail-fast
- 停止失败继续清理
- 重复 start / stop 安全 no-op；start-after-stop 也是 no-op
- 未 `start` 就 `stop` 时只执行 dispose hooks，不调用 plugin.stop
- 嵌套 `Context::scope() -> Result<Builder, Error>`
- c-lite 租约：父 `Runtime::stop` 会拒绝仍有活跃子 Runtime 的停止
- 事件系统（类型化、父链冒泡、serial / parallel、notify 旁路）
- 结构化 `Error { phase, plugin, kind }`
- 默认分层并行 `start`，`start_serial()` 保留旧串行语义
- 逆序销毁

## 当前限制

- **父 stop vs 子存活**：采用 c-lite 租约模型。父 `Runtime::stop()` 只检查本层活跃子 `Runtime`/`Builder` 计数；仅子 `Context` 句柄存活不阻塞父 stop，但子 `Runtime` 即使已 `stop` 未 `drop` 仍占租约阻塞父 stop。
- **无 `Drop` 兜底**：忘记调用 `stop()` 不会自动销毁插件，dispose 钩子也不会运行。这是为了避免异步析构陷阱；资源清理责任在创建者，请显式调用 `stop()`。
- **`require_all` 只查本层**：它返回当前 Context/Builder 局部集合，不沿父链冒泡。需要读取父级集合时请使用 `require_all_recursive`。
- **c-lite 不防御「不 `stop` 直接 `drop` 子 Runtime」**：drop 会释放租约，但深层异步清理可能未完成，符合显式 `stop` + 无 `Drop` 兜底的原则。

## 运行示例

```bash
cargo run --example basic
cargo run --example full
cargo run --example scopes
cargo run --example events
```

## 测试

```bash
cargo test
```
