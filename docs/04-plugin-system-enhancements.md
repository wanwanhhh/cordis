# 插件系统增强设计：插件依赖与插件配置

> 版本：0.1（已实现）
> 定位：补充插件间依赖和插件配置注入能力。

---

## 1. 目标

本阶段实现两块能力：

1. **插件间依赖** `PluginDependency`
2. **插件配置注入** `plugin_with_config`

---

## 2. PluginDependency

### 2.1 定义

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PluginDependency {
    pub plugin_name: &'static str,
    pub optional: bool,
}

impl PluginDependency {
    pub fn of(plugin_name: &'static str) -> Self;
    pub fn optional_of(plugin_name: &'static str) -> Self;
}
```

### 2.2 Plugin trait 扩展

```rust
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    fn plugin_dependencies(&self) -> &'static [PluginDependency] {
        &[]
    }
}
```

### 2.3 依赖标识

插件依赖通过插件 `name()` 标识：

- 插件注册时，`ContextInner` 保存 `plugin_names: Vec<&'static str>`
- `plugin()` 失败回滚时，必须同步截断 `plugin_names`，避免残留脏名字
- 依赖检查在 `start()` 前进行
- 缺失的必选插件依赖返回 `Error::PluginDependencyNotFound`

### 2.3.1 插件名唯一性

- 同一 `Context` / `Scope` 中，插件 `name()` 必须唯一。
- 重复注册同名插件返回 `Error::PluginNameAlreadyRegistered`。
- `plugin_names` 保存的是注册成功的插件名。

### 2.4 查询插件是否存在

`Context` / `Scope` 增加：

```rust
pub fn has_plugin(&self, name: &str) -> bool;
```

- 用于插件查询某个插件是否已注册
- `try_require` 查询的是服务，不能替代插件查询
- 可选插件依赖通常这样使用：

```rust
if ctx.has_plugin("MemoryPlugin") {
    // 启用可选能力
}
```

### 2.5 可选依赖

- `optional = true` 时，目标插件缺失不报错
- 插件可以通过 `has_plugin` 判断目标插件是否存在
- 可选依赖存在时，会参与启动排序

### 2.6 启动排序

启动顺序规则：

1. 先满足插件依赖：如果 A 依赖 B，则 B 必须在 A 之前启动
2. 在依赖约束下，`priority` 越大越先启动
3. 相同 `priority` 按注册顺序启动
4. 停止顺序为启动顺序的逆序

实现采用 **Kahn 拓扑排序**：

- 以插件索引为节点
- 依赖边：`B -> A` 表示 B 先于 A
- 可选依赖缺失时，从图中排除该边
- 目标插件不在当前 Scope 时，如果父级插件存在，则跳过本地排序边，不报错
- 必选依赖缺失（局部与父级都没有）时，`start()` 前返回 `Error::PluginDependencyNotFound`
- 每次从“入度为 0 的节点”中选择：
  - `priority` 最高
  - 注册序号最小
- 如果无法生成完整拓扑序列，返回 `Error::PluginDependencyCycle`
- 排序结果是“插件索引序列”
- 该序列保存为 `start_order`
- `stop()` 按该序列逆序清理

---

## 3. plugin_with_config

### 3.1 API

```rust
impl Context {
    pub fn plugin_with_config<P, C>(
        &mut self,
        plugin: P,
        config: C,
    ) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static;
}

impl Scope {
    pub fn plugin_with_config<P, C>(
        &mut self,
        plugin: P,
        config: C,
    ) -> Result<(), Error>
    where
        P: Plugin,
        C: Send + Sync + 'static;
}
```

### 3.2 当前语义

v1 采用“配置作为服务注入”的简化模型：

```text
1. 将 config 以 C 类型注册到当前 Context
2. 再注册 plugin
3. 插件在 apply / start 中通过 ctx.require::<C>() 获取配置
```

```rust
ctx.plugin_with_config(MyAgentPlugin, MyConfig {
    model: "gpt-4o".into(),
})?;
```

插件内部：

```rust
impl Plugin for MyAgentPlugin {
    fn apply(&self, ctx: &mut Context) -> Result<(), Error> {
        let config = ctx.require::<MyConfig>()?;
        ctx.provide(MyService::new(config.clone()))?;
        Ok(())
    }
}
```

### 3.3 冲突规则

- 如果当前 Context 已经有同类型 `C` 的服务，返回 `Error::ServiceAlreadyRegistered`
- 同一插件配置类型只能注入一次
- 如果同一个插件需要多个配置，建议使用不同配置类型或 newtype

### 3.4 回滚语义

`plugin_with_config` 是原子操作：

```rust
let snapshot = ctx.services_snapshot();
ctx.provide(config)?;

if let Err(err) = ctx.plugin(plugin) {
    ctx.services_restore(snapshot);
    return Err(err);
}
```

注意：

- `plugin()` 内部的快照发生在 `config` 注入之后
- 因此必须在外层记录 `services_snapshot`
- 如果 `plugin.apply()` 失败，恢复外层快照即可移除本次注入的配置
- 插件自身注册会由 `plugin()` 回滚

### 3.5 配置可见范围

- 配置作为服务注册到当前 Context/Scope
- 子级 Scope 是否继承，与普通服务规则一致
- 父级 Scope 看不到子级局部配置
- 同一插件需要多个配置时，使用不同配置类型或 newtype 隔离

### 3.5 与 `provide_factory` 兼容

不要求配置使用工厂。  
配置可以是具体实例：

```rust
ctx.plugin_with_config(MyPlugin, MyConfig::default())
```

### 3.6 未来扩展

后续可以升级为：

```rust
ctx.plugin_with_config_schema::<P>(schema, config)
```

支持：

- 配置默认值
- 环境变量覆盖
- 配置校验
- Schema 声明

---

## 4. 错误类型

新增：

```rust
Error::PluginNameAlreadyRegistered(String)
Error::PluginDependencyNotFound(String)
Error::PluginDependencyCycle
```

---

## 5. 预期测试

- 插件依赖存在 / 缺失
- 可选插件依赖缺失不阻塞
- 插件依赖拓扑排序
- priority 与插件依赖共同排序
- 循环依赖检测
- `plugin_with_config` 注入配置
- `plugin_with_config` 配置冲突
- `plugin_with_config` apply 失败回滚
- Scope 级 `plugin_with_config`

---

## 6. 非目标

- 不实现插件动态卸载
- 不实现完整 ConfigSchema
- 不改变插件 `apply` / `start` / `stop` 基础语义
- 不引入热加载
