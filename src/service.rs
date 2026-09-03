//! 服务注册表。

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, OnceLock, RwLock};

use crate::{Error, ErrorKind, Phase};

/// 存储的服务实例。
struct StoredService {
    type_name: &'static str,
    value: Box<dyn Any + Send + Sync>,
}

/// 类型擦除的服务工厂。
trait ErasedFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// 具体类型服务工厂。
struct TypedFactory<T: Send + Sync + 'static> {
    factory: Box<dyn Fn() -> Result<T, Error> + Send + Sync>,
    value: OnceLock<T>,
}

impl<T: Send + Sync + 'static> TypedFactory<T> {
    fn get(&self) -> Result<&T, Error> {
        if let Some(value) = self.value.get() {
            return Ok(value);
        }

        let value = (self.factory)()?;
        let _ = self.value.set(value);
        Ok(self.value.get().expect("factory just set"))
    }

    fn get_mut(&mut self) -> Result<&mut T, Error> {
        if self.value.get().is_none() {
            self.get()?;
        }
        Ok(self.value.get_mut().expect("factory just set"))
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

    /// 读取当前配置值。
    ///
    /// 如果内部的 `RwLock` 已被写线程 panic 污染，会直接 panic。
    pub fn read(&self) -> impl Deref<Target = T> + '_ {
        self.value.read().expect("dynamic value poisoned")
    }

    /// 获取当前配置值的可变写锁。
    ///
    /// 如果内部的 `RwLock` 已被写线程 panic 污染，会直接 panic。
    pub fn write(&self) -> impl DerefMut<Target = T> + '_ {
        self.value.write().expect("dynamic value poisoned")
    }

    /// 整体替换配置值。
    ///
    /// 如果内部的 `RwLock` 已被写线程 panic 污染，会直接 panic。
    pub fn set(&self, value: T) {
        *self.value.write().expect("dynamic value poisoned") = value;
    }

    /// 通过闭包更新配置值。
    ///
    /// 如果内部的 `RwLock` 已被写线程 panic 污染，会直接 panic。
    pub fn update(&self, f: impl FnOnce(&mut T)) {
        f(&mut self.value.write().expect("dynamic value poisoned"));
    }
}

/// 服务注册表。
#[derive(Default)]
pub struct ServiceRegistry {
    services: HashMap<TypeId, StoredService>,
    collections: HashMap<TypeId, Vec<Box<dyn Any + Send + Sync>>>,
    factories: HashMap<TypeId, Box<dyn ErasedFactory>>,
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
            }),
        );
        Ok(())
    }

    /// 注册一个集合服务实现。
    pub fn provide_collect<T: Send + Sync + 'static>(&mut self, value: T) -> Result<(), Error> {
        self.collections
            .entry(TypeId::of::<T>())
            .or_default()
            .push(Box::new(value));
        Ok(())
    }

    /// 获取普通服务，兼容工厂。
    pub fn get<T: Send + Sync + 'static>(&self) -> Result<&T, Error> {
        let key = TypeId::of::<T>();

        if let Some(service) = self.services.get(&key) {
            return service.value.downcast_ref::<T>().ok_or_else(|| {
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
                            found: std::any::type_name::<T>(),
                        },
                    )
                })?
                .get();
        }

        Err(Error::new(
            Phase::Build,
            ErrorKind::ServiceNotFound(std::any::type_name::<T>().to_string()),
        ))
    }

    /// 尝试获取普通服务，兼容工厂。
    pub fn try_get<T: Send + Sync + 'static>(&self) -> Result<Option<&T>, Error> {
        match self.get::<T>() {
            Ok(value) => Ok(Some(value)),
            Err(Error {
                kind: ErrorKind::ServiceNotFound(_),
                ..
            }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// 获取本地集合中的所有实现。
    pub fn all<T: Send + Sync + 'static>(&self) -> Result<Vec<&T>, Error> {
        let mut result = Vec::new();
        if let Some(values) = self.collections.get(&TypeId::of::<T>()) {
            for value in values {
                let value = value.downcast_ref::<T>().ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found: std::any::type_name::<T>(),
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
            return factory
                .as_any_mut()
                .downcast_mut::<TypedFactory<T>>()
                .ok_or_else(|| {
                    Error::new(
                        Phase::Build,
                        ErrorKind::ServiceTypeMismatch {
                            expected: std::any::type_name::<T>(),
                            found: std::any::type_name::<T>(),
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

    /// 返回当前所有已注册服务类型的集合（普通服务 + 工厂 + 集合）。
    pub(crate) fn type_ids(&self) -> HashSet<TypeId> {
        self.services
            .keys()
            .chain(self.factories.keys())
            .chain(self.collections.keys())
            .copied()
            .collect()
    }

    /// 仅保留指定 `TypeId` 集合中的服务。
    pub(crate) fn retain(&mut self, keep: &HashSet<TypeId>) {
        self.services.retain(|key, _| keep.contains(key));
        self.factories.retain(|key, _| keep.contains(key));
        self.collections.retain(|key, _| keep.contains(key));
    }
}
