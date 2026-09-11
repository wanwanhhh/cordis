//! 服务注册表。

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{Error, ErrorKind, Phase};

/// FxHash 风格的快速 hasher，专为 `TypeId` 键设计。
///
/// `TypeId` 本身已是高质量哈希值，默认 SipHash 属于重复付费；
/// 本实现只做透传混合，热路径每次查找节省约 10–15ns。
#[derive(Default)]
pub(crate) struct TypeIdHasher(u64);

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl TypeIdHasher {
    #[inline]
    fn mix(&mut self, value: u64) {
        self.0 = (self.0.rotate_left(5) ^ value).wrapping_mul(SEED);
    }
}

impl Hasher for TypeIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.mix(u64::from_le_bytes(chunk.try_into().expect("8-byte chunk")));
        }
        if !chunks.remainder().is_empty() {
            let mut tail = [0u8; 8];
            tail[..chunks.remainder().len()].copy_from_slice(chunks.remainder());
            self.mix(u64::from_le_bytes(tail));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.mix(i as u64);
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.mix(i as u64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.mix(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.mix(i);
    }

    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.mix(i as u64);
        self.mix((i >> 64) as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.mix(i as u64);
    }
}

/// `TypeId` 键哈希表专用构建器。
pub(crate) type TypeMap<V> = HashMap<TypeId, V, BuildHasherDefault<TypeIdHasher>>;

/// FNV-1a hasher，用于插件名（`&'static str`）这类可信键。
///
/// 插件名是编译期常量、没有不可信输入，默认 SipHash 的抗 DoS 属于白付；编译期
/// 字符串键用 FNV-1a 更快（注册路径实测约快 15%）。
pub(crate) struct FnvHasher(u64);

impl Default for FnvHasher {
    fn default() -> Self {
        // 直接以 FNV offset basis 起始，不用 0 作「未初始化」哨兵。
        Self(Self::OFFSET)
    }
}

impl FnvHasher {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
}

impl Hasher for FnvHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 ^= byte as u64;
            self.0 = self.0.wrapping_mul(Self::PRIME);
        }
    }
}

/// `&'static str` 键哈希表专用构建器。
pub(crate) type NameMap<V> = HashMap<&'static str, V, BuildHasherDefault<FnvHasher>>;

/// 存储的服务实例 / 集合元素：类型擦除后的值。
///
/// `services` / `collections` 只由泛型 `provide*` 路径写入，键恒为
/// `TypeId::of::<T>()`、值恒为同一个 `T` 装箱而来，所以取出时的 `downcast` 不可能
/// 失败；`factories` 同理（键 `TypeId::of::<T>()`、值 `TypedFactory<T>`）。类型不
/// 匹配是类型层面的不可能事件，不构成运行的错误分支。
type StoredValue = Box<dyn Any + Send + Sync>;

/// 类型擦除的服务工厂。
trait ErasedFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// 具体类型服务工厂。
struct TypedFactory<T: Send + Sync + 'static> {
    factory: Box<dyn Fn() -> Result<T, Error> + Send + Sync>,
    value: OnceLock<T>,
    init_lock: Mutex<()>,
}

impl<T: Send + Sync + 'static> TypedFactory<T> {
    fn get(&self) -> Result<&T, Error> {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }

        // 串行化初始化：成功路径至多执行一次工厂，返回值始终是首个成功值。
        // 锁跨工厂调用持有，要求工厂为非阻塞纯计算；失败保留可重试语义。
        let _guard = self
            .init_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(value) = self.value.get() {
            return Ok(value);
        }

        let value = (self.factory)()?;
        if self.value.set(value).is_err() {
            // 双检后槽位必为空，`set` 不可能失败；走到这里说明本类型内部不变式被
            // 破坏，不该伪装成可恢复的业务错误。
            debug_assert!(false, "factory value set under init_lock");
        }
        Ok(self.value.get().expect("factory value set under init_lock"))
    }

    fn get_mut(&mut self) -> Result<&mut T, Error> {
        if self.value.get().is_none() {
            self.get()?;
        }
        // `get()` 成功后 OnceLock 必已置位。
        Ok(self
            .value
            .get_mut()
            .expect("factory value set under init_lock"))
    }
}

impl<T: Send + Sync + 'static> ErasedFactory for TypedFactory<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// 运行时动态配置值。
///
/// 以普通服务的形式注册为 `Arc<DynamicValue<T>>`，运行期可通过只读 `Context`
/// 取得同一个 `Arc` 句柄，在保持服务注册表冻结语义的同时安全地修改配置。
pub struct DynamicValue<T> {
    value: Arc<RwLock<T>>,
}

impl<T> DynamicValue<T> {
    /// 创建动态配置值。
    pub fn new(initial: T) -> Self {
        Self {
            value: Arc::new(RwLock::new(initial)),
        }
    }

    /// 读锁获取：**中毒策略的唯一出口**。
    ///
    /// 持锁线程 panic 导致的 `RwLock` 中毒被容忍，返回中毒时刻的数据快照，不会把
    /// 单次用户 panic 放大为读路径崩溃。四个公开方法都走这里，策略只此一份。
    fn read_guard(&self) -> RwLockReadGuard<'_, T> {
        self.value
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 写锁获取；中毒容忍同 `read_guard`。
    fn write_guard(&self) -> RwLockWriteGuard<'_, T> {
        self.value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 读取当前配置值。
    ///
    /// 中毒策略见 `read_guard`。
    pub fn read(&self) -> impl Deref<Target = T> + '_ {
        self.read_guard()
    }

    /// 获取当前配置值的可变写锁。
    ///
    /// 中毒被容忍，见 `read_guard`。
    pub fn write(&self) -> impl DerefMut<Target = T> + '_ {
        self.write_guard()
    }

    /// 整体替换配置值。
    ///
    /// 中毒被容忍，见 `read_guard`。
    pub fn set(&self, value: T) {
        *self.write_guard() = value;
    }

    /// 通过闭包更新配置值。
    ///
    /// 中毒被容忍，见 `read_guard`。
    pub fn update(&self, f: impl FnOnce(&mut T)) {
        f(&mut self.write_guard());
    }
}

/// 服务注册表。
#[derive(Default)]
pub struct ServiceRegistry {
    services: TypeMap<StoredValue>,
    collections: TypeMap<Vec<StoredValue>>,
    factories: TypeMap<Box<dyn ErasedFactory>>,
}

impl ServiceRegistry {
    /// 创建一个空的服务注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个普通服务。
    pub fn provide<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        let key = TypeId::of::<T>();
        if self.contains_type(key) {
            return Err(Self::duplicate_slot_error::<T>());
        }
        self.services.insert(key, Box::new(value));
        Ok(())
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let key = TypeId::of::<T>();
        if self.contains_type(key) {
            return Err(Self::duplicate_slot_error::<T>());
        }
        self.factories.insert(
            key,
            Box::new(TypedFactory {
                factory: Box::new(factory),
                value: OnceLock::new(),
                init_lock: Mutex::new(()),
            }),
        );
        Ok(())
    }

    /// 普通服务与工厂共用同一个类型槽位；「已被占用」的判定与文案只此一处。
    fn duplicate_slot_error<T: Send + Sync + 'static>() -> Error {
        Error::new(
            Phase::Build,
            ErrorKind::ServiceAlreadyRegistered(std::any::type_name::<T>().to_string()),
        )
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.collections
            .entry(TypeId::of::<T>())
            .or_default()
            .push(Box::new(value));
        Ok(())
    }

    /// 类型查找的内部通道：`Ok(Some)` 命中，`Ok(None)` 本层未注册该类型槽位，
    /// `Err` 为工厂初始化失败。
    ///
    /// miss 路径不分配任何内存，供父链循环使用；工厂初始化失败一律以错误
    /// 传播，不会被降级为“不存在”。
    pub(crate) fn get_ref<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        let key = TypeId::of::<T>();

        if let Some(service) = self.services.get(&key) {
            return Ok(Some(
                service
                    .downcast_ref::<T>()
                    .expect("service key type matches stored value type"),
            ));
        }

        if let Some(factory) = self.factories.get(&key) {
            return factory
                .as_any()
                .downcast_ref::<TypedFactory<T>>()
                .expect("factory key type matches stored factory type")
                .get()
                .map(Some);
        }

        Ok(None)
    }

    /// 获取普通服务，兼容工厂。
    pub fn get<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        self.get_ref::<T>()?.ok_or_else(|| {
            Error::new(
                Phase::Build,
                ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
            )
        })
    }

    /// 尝试获取普通服务，兼容工厂。
    ///
    /// 服务不存在返回 `Ok(None)`；工厂初始化失败返回 `Err`。
    pub fn try_get<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        self.get_ref::<T>()
    }

    /// 获取本地集合中的所有实现。
    pub fn all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        let mut result = Vec::new();
        if let Some(values) = self.collections.get(&TypeId::of::<T>()) {
            for stored in values {
                result.push(
                    stored
                        .downcast_ref::<T>()
                        .expect("collection key type matches stored value type"),
                );
            }
        }
        Ok(result)
    }

    /// 获取普通服务可变引用，兼容工厂。
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        let key = TypeId::of::<T>();

        if let Some(service) = self.services.get_mut(&key) {
            return Ok(service
                .downcast_mut::<T>()
                .expect("service key type matches stored value type"));
        }

        if let Some(factory) = self.factories.get_mut(&key) {
            return factory
                .as_any_mut()
                .downcast_mut::<TypedFactory<T>>()
                .expect("factory key type matches stored factory type")
                .get_mut();
        }

        Err(Error::new(
            Phase::Build,
            ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
        ))
    }

    /// 判断是否有普通服务、工厂或集合实现。
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        let key = TypeId::of::<T>();
        self.services.contains_key(&key)
            || self.factories.contains_key(&key)
            || self.collections.contains_key(&key)
    }

    /// 按 `TypeId` 判断是否有普通服务或工厂。
    pub(crate) fn contains_type(&self, type_id: TypeId) -> bool {
        self.services.contains_key(&type_id) || self.factories.contains_key(&type_id)
    }

    /// 移除普通服务。
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Result<T, Error> {
        let key = TypeId::of::<T>();
        let service = self.services.remove(&key).ok_or_else(|| {
            Error::new(
                Phase::Build,
                ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
            )
        })?;

        Ok(*service
            .downcast::<T>()
            .expect("service key type matches stored value type"))
    }

    // 以下三个方法是装配期增量回滚的唯一入口：注册表在插件 `apply` 期间的每次
    // 新增都由调用方记录成一条逆操作，失败时按序撤销。此前用的「整表快照 + 重建」
    // 需要复制全部键与长度，注册 N 个插件是 O(N·S)；增量撤销只与本次插件的改动量
    // 相关。三个逆操作对「已删除的条目」都是幂等的，因此嵌套插件各自回滚不会误伤。

    /// 撤销一次 `provide`：移除对应普通服务槽位。
    pub(crate) fn remove_service_key(&mut self, key: TypeId) {
        self.services.remove(&key);
    }

    /// 撤销一次 `provide_factory`：移除对应工厂槽位。
    pub(crate) fn remove_factory_key(&mut self, key: TypeId) {
        self.factories.remove(&key);
    }

    /// 撤销一次 `provide_collect`：弹出该类型最后压入的元素，集合空后移除槽位。
    pub(crate) fn pop_collection(&mut self, key: TypeId) {
        if let Some(values) = self.collections.get_mut(&key) {
            values.pop();
            if values.is_empty() {
                self.collections.remove(&key);
            }
        }
    }
}
