# cordis

Rust 实现的 Cordis 风格插件化基础架构。

当前处于早期阶段，核心目标是构建一套可复用的 Rust 插件化/依赖注入底座。

## 文档

- [Cordis 底层约束与开发指导](docs/00-cordis-architecture.md)
- [异步生命周期与线程安全设计](docs/01-async-lifecycle.md)
- [EventBus / 事件系统设计](docs/02-event-bus.md)
- [v0.3 服务与插件增强设计](docs/03-service-plugin-enhancements.md)
- [插件系统增强设计](docs/04-plugin-system-enhancements.md)

## 当前实现

- `Context` / `Scope`（支持 Clone、Arc、Send + Sync）
- `Plugin`（异步 start / stop）
- `ServiceRegistry`（Send + Sync 服务）
- `LifecycleHook` / `SyncHook` / `AsyncHook`
- `FnEventHandler` / `AsyncFnEventHandler`
- 服务注册 / 获取
- `try_require` / `require_all`
- `Collection<T>` 多实现服务
- `provide_factory` 懒加载服务
- 父级服务继承与局部遮蔽
- 插件依赖声明与检查
- 可选依赖
- 插件间依赖 `PluginDependency`
- 插件配置注入 `plugin_with_config`
- 插件元信息 / 优先级
- `apply` 失败回滚
- 异步 ready / dispose 生命周期
- 启动失败 fail-fast
- 停止失败继续清理
- 重复 start / stop 安全 no-op
- 嵌套 Scope
- 事件系统（类型化、Scope 冒泡、serial / parallel）
- 逆序销毁

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
