use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::error::{Result, TsError};

/// 锁模式
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockMode {
    /// 共享锁（用于读取）
    Shared,
    /// 排他锁（用于写入）
    Exclusive,
}

/// 锁信息
#[derive(Debug, Clone)]
struct LockInfo {
    /// 持有锁的事务ID
    tx_id: u64,
    /// 锁模式
    mode: LockMode,
    /// 获取锁的时间
    acquired_at: Instant,
}

/// 锁管理器
/// 支持共享锁和排他锁，用于实现 Serializable 隔离级别
pub struct LockManager {
    /// 键 -> 锁信息列表（一个键可以被多个共享锁持有）
    locks: RwLock<HashMap<Vec<u8>, Vec<LockInfo>>>,
    /// 锁等待超时（毫秒）
    lock_timeout_ms: u64,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            lock_timeout_ms: 10000, // 默认10秒
        }
    }

    pub fn with_timeout(timeout_ms: u64) -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            lock_timeout_ms: timeout_ms,
        }
    }

    /// 尝试获取锁
    pub fn try_acquire_lock(
        &self,
        tx_id: u64,
        key: Vec<u8>,
        mode: LockMode,
        timeout_ms: u64,
    ) -> Result<()> {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);

        loop {
            // 检查超时
            if start.elapsed() > timeout {
                return Err(TsError::Timeout(format!(
                    "事务 {} 获取锁超时（键: {:?})",
                    tx_id,
                    String::from_utf8_lossy(&key)
                )));
            }

            let mut locks = self
                .locks
                .write()
                .map_err(|_| TsError::Unknown("获取锁管理器写锁失败".into()))?;

            let key_locks = locks.entry(key.clone()).or_insert_with(Vec::new);

            match mode {
                LockMode::Shared => {
                    // 共享锁：如果没有排他锁，则可以获取
                    let has_exclusive = key_locks
                        .iter()
                        .any(|l| matches!(l.mode, LockMode::Exclusive) && l.tx_id != tx_id);

                    if !has_exclusive {
                        key_locks.push(LockInfo {
                            tx_id,
                            mode: LockMode::Shared,
                            acquired_at: Instant::now(),
                        });
                        return Ok(());
                    }
                }
                LockMode::Exclusive => {
                    // 排他锁：如果没有其他任何锁，则可以获取
                    let has_other_locks = key_locks.iter().any(|l| l.tx_id != tx_id);

                    if !has_other_locks {
                        key_locks.push(LockInfo {
                            tx_id,
                            mode: LockMode::Exclusive,
                            acquired_at: Instant::now(),
                        });
                        return Ok(());
                    }
                }
            }

            // 锁被占用，释放写锁并等待
            drop(locks);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// 释放事务持有的所有锁
    pub fn release_locks(&self, tx_id: u64) {
        if let Ok(mut locks) = self.locks.write() {
            locks.retain(|_, lock_list| {
                lock_list.retain(|l| l.tx_id != tx_id);
                !lock_list.is_empty()
            });
        }
    }

    /// 检查键是否被锁定
    pub fn is_locked(&self, key: &[u8]) -> bool {
        if let Ok(locks) = self.locks.read() {
            if let Some(key_locks) = locks.get(key) {
                return !key_locks.is_empty();
            }
        }
        false
    }

    /// 获取锁定键的数量
    pub fn locked_keys_count(&self) -> usize {
        if let Ok(locks) = self.locks.read() {
            return locks.len();
        }
        0
    }

    /// 获取指定键的锁持有者列表
    pub fn get_lock_holders(&self, key: &[u8]) -> Vec<(u64, LockMode)> {
        if let Ok(locks) = self.locks.read() {
            if let Some(key_locks) = locks.get(key) {
                return key_locks.iter().map(|l| (l.tx_id, l.mode)).collect();
            }
        }
        Vec::new()
    }

    /// 检查事务是否持有指定键的排他锁
    pub fn has_exclusive_lock(&self, tx_id: u64, key: &[u8]) -> bool {
        if let Ok(locks) = self.locks.read() {
            if let Some(key_locks) = locks.get(key) {
                return key_locks
                    .iter()
                    .any(|l| l.tx_id == tx_id && matches!(l.mode, LockMode::Exclusive));
            }
        }
        false
    }

    /// 检查事务是否持有指定键的共享锁
    pub fn has_shared_lock(&self, tx_id: u64, key: &[u8]) -> bool {
        if let Ok(locks) = self.locks.read() {
            if let Some(key_locks) = locks.get(key) {
                return key_locks
                    .iter()
                    .any(|l| l.tx_id == tx_id && matches!(l.mode, LockMode::Shared));
            }
        }
        false
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}
