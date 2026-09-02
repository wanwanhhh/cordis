# cordis

Rust 实现的 Cordis 风格插件化基础架构。

当前处于早期阶段，核心目标是构建一套可复用的 Rust 插件化/依赖注入底座。

## 文档

- [Cordis 底层约束与开发指导](docs/00-cordis-architecture.md)

## 当前实现

- `Context`
- `Plugin`
- `ServiceRegistry`
- 服务注册 / 获取
- 插件依赖声明与检查
- `apply` 失败回滚
- ready / dispose 生命周期
- 启动失败 fail-fast
- 停止失败继续清理
- 重复 start / stop 安全 no-op
- 逆序销毁

## 运行示例

```bash
cargo run --example basic
cargo run --example full
```

## 测试

```bash
cargo test
```
