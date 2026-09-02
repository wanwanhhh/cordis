//! 服务注册表。

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};

use crate::Error;

/// 存储的服务实例。
///
/// 同时保存类型名，用于提供更准确的错误诊断。
struct StoredService {
    type_name: &'static str,
    value: Box<dyn Any>,
}

/// 服务注册表。
///
/// 服务以 [`TypeId`] 为键，以 [`StoredService`] 存储。
/// 只允许同一类型注册一次。
#[derive(Default)]
pub struct ServiceRegistry {
    services: HashMap<TypeId, StoredService>,
}

impl ServiceRegistry {
    /// 创建一个空的服务注册表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个服务。
    ///
    /// 如果该类型已经注册过，则返回 [`Error::ServiceAlreadyRegistered`]。
    pub fn provide<T: 'static>(&mut self, value: T) -> Result<(), Error> {
        let key = TypeId::of::<T>();
        if self.services.contains_key(&key) {
            return Err(Error::ServiceAlreadyRegistered(
                std::any::type_name::<T>().to_string(),
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

    /// 获取一个服务的不可变引用。
    pub fn get<T: 'static>(&self) -> Result<&T, Error> {
        let key = TypeId::of::<T>();
        let service = self
            .services
            .get(&key)
            .ok_or_else(|| Error::ServiceNotFound(std::any::type_name::<T>().to_string()))?;

        service
            .value
            .downcast_ref::<T>()
            .ok_or_else(|| Error::ServiceTypeMismatch {
                expected: std::any::type_name::<T>(),
                found: service.type_name,
            })
    }

    /// 获取一个服务的可变引用。
    pub fn get_mut<T: 'static>(&mut self) -> Result<&mut T, Error> {
        let key = TypeId::of::<T>();
        let service = self
            .services
            .get_mut(&key)
            .ok_or_else(|| Error::ServiceNotFound(std::any::type_name::<T>().to_string()))?;

        service
            .value
            .downcast_mut::<T>()
            .ok_or_else(|| Error::ServiceTypeMismatch {
                expected: std::any::type_name::<T>(),
                found: service.type_name,
            })
    }

    /// 判断服务是否存在。
    pub fn contains<T: 'static>(&self) -> bool {
        self.services.contains_key(&TypeId::of::<T>())
    }

    /// 按 `TypeId` 判断服务是否存在。
    pub(crate) fn contains_type(&self, type_id: TypeId) -> bool {
        self.services.contains_key(&type_id)
    }

    /// 移除并返回一个服务。
    pub fn remove<T: 'static>(&mut self) -> Result<T, Error> {
        let key = TypeId::of::<T>();
        let service = self
            .services
            .remove(&key)
            .ok_or_else(|| Error::ServiceNotFound(std::any::type_name::<T>().to_string()))?;

        service
            .value
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| Error::ServiceTypeMismatch {
                expected: std::any::type_name::<T>(),
                found: service.type_name,
            })
    }

    /// 返回当前所有已注册服务类型的集合。
    pub(crate) fn type_ids(&self) -> HashSet<TypeId> {
        self.services.keys().copied().collect()
    }

    /// 仅保留指定 `TypeId` 集合中的服务。
    ///
    /// 用于插件 `apply` 失败时的副作用回滚。
    pub(crate) fn retain(&mut self, keep: &HashSet<TypeId>) {
        self.services.retain(|key, _| keep.contains(key));
    }
}
