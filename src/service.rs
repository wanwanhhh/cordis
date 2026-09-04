//! 服务注册表。

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

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

/// 存储的服务实例。
struct StoredService {
    type_name: &'static str,
    value: Box<dyn Any + Send + Sync>,
}

/// 存储的集合服务元素。
struct StoredCollection {
    type_name: &'static str,
    value: Box<dyn Any + Send + Sync>,
}

/// 类型擦除的服务工厂。
trait ErasedFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// 工厂产出值的真实类型名，用于类型不匹配诊断。
    fn type_name(&self) -> &'static str;
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
        let _ = self.value.set(value);
        // `set` 在 `init_lock` 下执行，紧接的 `get` 必有值；此处不 panic，走错误通道。
        self.value
            .get()
            .ok_or_else(|| Error::new(Phase::Build, ErrorKind::Other))
    }

    fn get_mut(&mut self) -> Result<&mut T, Error> {
        if self.value.get().is_none() {
            self.get()?;
        }
        // `get()` 成功后 OnceLock 必已置位；此处不 panic，走错误通道。
        self.value
            .get_mut()
            .ok_or_else(|| Error::new(Phase::Build, ErrorKind::Other))
    }
}

impl<T: Send + Sync + 'static> ErasedFactory for TypedFactory<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
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

    /// 读取当前配置值。
    ///
    /// 中毒策略与框架其余部分一致：持锁线程 panic 导致的 `RwLock` 中毒被容忍，
    /// 返回中毒时刻的数据快照，不会把单次用户 panic 放大为读路径崩溃。
    pub fn read(&self) -> impl Deref<Target = T> + '_ {
        self.value
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 获取当前配置值的可变写锁。
    ///
    /// 中毒被容忍，见 [`DynamicValue::read`]。
    pub fn write(&self) -> impl DerefMut<Target = T> + '_ {
        self.value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 整体替换配置值。
    ///
    /// 中毒被容忍，见 [`DynamicValue::read`]。
    pub fn set(&self, value: T) {
        let mut guard = self
            .value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = value;
    }

    /// 通过闭包更新配置值。
    ///
    /// 中毒被容忍，见 [`DynamicValue::read`]。
    pub fn update(&self, f: impl FnOnce(&mut T)) {
        let mut guard = self
            .value
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&mut guard);
    }
}

/// 服务注册表。
#[derive(Default)]
pub struct ServiceRegistry {
    services: TypeMap<StoredService>,
    collections: TypeMap<Vec<StoredCollection>>,
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
        if self.services.contains_key(&key) || self.factories.contains_key(&key) {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::ServiceAlreadyRegistered(std::any::type_name::<T>().to_string()),
            ));
        }
        self.services.insert(
            key,
            StoredService {
                type_name: std::any::type_name::<T>(),
                value: Box::new(value),
            },
        );
        Ok(())
    }

    /// 注册一个懒加载服务工厂。
    pub fn provide_factory<T: Send + Sync + 'static>(
        &mut self,
        factory: impl Fn() -> Result<T, Error> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        let key = TypeId::of::<T>();
        if self.services.contains_key(&key) || self.factories.contains_key(&key) {
            return Err(Error::new(
                Phase::Build,
                ErrorKind::ServiceAlreadyRegistered(std::any::type_name::<T>().to_string()),
            ));
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

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.collections
            .entry(TypeId::of::<T>())
            .or_default()
            .push(StoredCollection {
                type_name: std::any::type_name::<T>(),
                value: Box::new(value),
            });
        Ok(())
    }

    /// 类型查找的内部通道：`Ok(Some)` 命中，`Ok(None)` 本层未注册该类型槽位，
    /// `Err` 为类型不匹配或工厂初始化失败。
    ///
    /// miss 路径不分配任何内存，供父链循环使用；工厂初始化失败一律以错误
    /// 传播，不会被降级为“不存在”。
    pub(crate) fn get_ref<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        let key = TypeId::of::<T>();

        if let Some(service) = self.services.get(&key) {
            return service
                .value
                .downcast_ref::<T>()
                .map(Some)
                .ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found: service.type_name,
                        },
                    )
                });
        }

        if let Some(factory) = self.factories.get(&key) {
            return factory
                .as_any()
                .downcast_ref::<TypedFactory<T>>()
                .ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found: factory.type_name(),
                        },
                    )
                })?
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
                let value = stored.value.downcast_ref::<T>().ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found: stored.type_name,
                        },
                    )
                })?;
                result.push(value);
            }
        }
        Ok(result)
    }

    /// 获取普通服务可变引用，兼容工厂。
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Result<&mut T, Error> {
        let key = TypeId::of::<T>();

        if let Some(service) = self.services.get_mut(&key) {
            return service.value.downcast_mut::<T>().ok_or_else(|| {
                Error::new(
                    Phase::Build,
                    ErrorKind::ServiceTypeMismatch {
                        expected: std::any::type_name::<T>(),
                        found: service.type_name,
                    },
                )
            });
        }

        if let Some(factory) = self.factories.get_mut(&key) {
            let found = factory.type_name();
            return factory
                .as_any_mut()
                .downcast_mut::<TypedFactory<T>>()
                .ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found,
                        },
                    )
                })?
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

        service
            .value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| {
                Error::new(
                    Phase::Build,
                    ErrorKind::ServiceTypeMismatch {
                        expected: std::any::type_name::<T>(),
                        found: service.type_name,
                    },
                )
            })
    }

    /// 注册表回滚快照：记录键集合与各集合当前长度。
    pub(crate) fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            service_keys: self.services.keys().copied().collect(),
            factory_keys: self.factories.keys().copied().collect(),
            collection_lens: self
                .collections
                .iter()
                .map(|(key, values)| (*key, values.len()))
                .collect(),
        }
    }

    /// 回滚到指定快照：删除快照外的键，并将既有集合截断回快照长度。
    pub(crate) fn restore(&mut self, snapshot: RegistrySnapshot) {
        let RegistrySnapshot {
            service_keys,
            factory_keys,
            collection_lens,
        } = snapshot;
        self.services.retain(|key, _| service_keys.contains(key));
        self.factories.retain(|key, _| factory_keys.contains(key));
        self.collections
            .retain(|key, _| collection_lens.contains_key(key));
        for (key, len) in collection_lens {
            if let Some(values) = self.collections.get_mut(&key) {
                values.truncate(len);
            }
        }
    }
}

/// 服务注册表回滚快照。
pub(crate) struct RegistrySnapshot {
    service_keys: HashSet<TypeId>,
    factory_keys: HashSet<TypeId>,
    collection_lens: HashMap<TypeId, usize>,
}
