use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::transaction::context::TransactionState;

#[derive(Debug, Clone)]
pub struct ConflictResult {
    pub has_conflict: bool,
    pub conflict_key: Option<Vec<u8>>,
    pub conflict_version: Option<u64>,
    pub conflict_type: String,
    pub message: String,
}

impl ConflictResult {
    pub fn no_conflict() -> Self {
        Self {
            has_conflict: false,
            conflict_key: None,
            conflict_version: None,
            conflict_type: String::new(),
            message: String::new(),
        }
    }

    pub fn read_write_conflict(key: Vec<u8>, write_version: u64, snapshot_version: u64) -> Self {
        Self {
            has_conflict: true,
            conflict_key: Some(key.clone()),
            conflict_version: Some(write_version),
            conflict_type: "ReadWrite".to_string(),
            message: format!(
                "读-写冲突: 键 {:?} 在事务开始(快照版本={})后被版本 {} 修改",
                String::from_utf8_lossy(&key),
                snapshot_version,
                write_version
            ),
        }
    }

    pub fn write_write_conflict(key: Vec<u8>, write_version: u64, snapshot_version: u64) -> Self {
        Self {
            has_conflict: true,
            conflict_key: Some(key.clone()),
            conflict_version: Some(write_version),
            conflict_type: "WriteWrite".to_string(),
            message: format!(
                "写-写冲突: 键 {:?} 在事务开始(快照版本={})后被版本 {} 修改",
                String::from_utf8_lossy(&key),
                snapshot_version,
                write_version
            ),
        }
    }
}

#[derive(Debug)]
pub struct ConflictDetector {
    // 存储每个键的最后写入版本
    // 结构: graph_id -> key -> last_written_version
    key_versions: RwLock<HashMap<u32, HashMap<Vec<u8>, u64>>>,

    // 存储活跃事务的信息
    active_transactions: RwLock<HashMap<u64, ActiveTransactionInfo>>,
}

#[derive(Debug, Clone)]
struct ActiveTransactionInfo {
    pub graph_id: u32,
    pub snapshot_version: u64,
    pub read_keys: Vec<Vec<u8>>,
    pub write_keys: Vec<Vec<u8>>,
    pub state: TransactionState,
}

impl ConflictDetector {
    pub fn new() -> Self {
        Self {
            key_versions: RwLock::new(HashMap::new()),
            active_transactions: RwLock::new(HashMap::new()),
        }
    }

    /// 注册新事务
    pub fn register_transaction(&self, tx_id: u64, graph_id: u32, snapshot_version: u64) {
        let mut active_txs = self.active_transactions.write().unwrap();
        active_txs.insert(
            tx_id,
            ActiveTransactionInfo {
                graph_id,
                snapshot_version,
                read_keys: Vec::new(),
                write_keys: Vec::new(),
                state: TransactionState::Active,
            },
        );
    }

    /// 记录事务读操作
    pub fn record_read(&self, tx_id: u64, key: Vec<u8>) -> Result<(), String> {
        let mut active_txs = self.active_transactions.write().unwrap();
        if let Some(info) = active_txs.get_mut(&tx_id) {
            if !info.read_keys.contains(&key) {
                info.read_keys.push(key.clone());
            }
            Ok(())
        } else {
            Err(format!("事务 {} 不存在", tx_id))
        }
    }

    /// 记录事务写操作
    pub fn record_write(&self, tx_id: u64, key: Vec<u8>) -> Result<(), String> {
        let mut active_txs = self.active_transactions.write().unwrap();
        if let Some(info) = active_txs.get_mut(&tx_id) {
            if !info.write_keys.contains(&key) {
                info.write_keys.push(key.clone());
            }
            Ok(())
        } else {
            Err(format!("事务 {} 不存在", tx_id))
        }
    }

    /// 更新事务状态
    pub fn update_transaction_state(
        &self,
        tx_id: u64,
        state: TransactionState,
    ) -> Result<(), String> {
        let mut active_txs = self.active_transactions.write().unwrap();
        if let Some(info) = active_txs.get_mut(&tx_id) {
            info.state = state;
            Ok(())
        } else {
            Err(format!("事务 {} 不存在", tx_id))
        }
    }

    /// 检查事务是否可以提交
    pub fn check_transaction_conflicts(
        &self,
        tx_id: u64,
        isolation_level: rpc::ts::IsolationLevel,
    ) -> ConflictResult {
        // 获取事务信息
        let active_txs = self.active_transactions.read().unwrap();
        let tx_info = match active_txs.get(&tx_id) {
            Some(info) => info,
            None => {
                return ConflictResult {
                    has_conflict: true,
                    conflict_key: None,
                    conflict_version: None,
                    conflict_type: "TransactionNotFound".to_string(),
                    message: format!("事务 {} 不存在", tx_id),
                };
            }
        };

        // 检查事务状态
        if !matches!(tx_info.state, TransactionState::Active) {
            return ConflictResult {
                has_conflict: true,
                conflict_key: None,
                conflict_version: None,
                conflict_type: "InvalidState".to_string(),
                message: format!("事务 {} 不处于活跃状态", tx_id),
            };
        }

        // 根据隔离级别检查冲突
        match isolation_level {
            rpc::ts::IsolationLevel::ReadCommitted => {
                // 读已提交：只检查写-写冲突
                self.check_write_write_conflicts(tx_info)
            }
            rpc::ts::IsolationLevel::Snapshot => {
                // 快照隔离：检查读-写冲突和写-写冲突
                let read_write_result = self.check_read_write_conflicts(tx_info);
                if read_write_result.has_conflict {
                    return read_write_result;
                }
                self.check_write_write_conflicts(tx_info)
            }
            rpc::ts::IsolationLevel::Serializable => {
                // 可序列化：检查读-写、写-写和写-读冲突
                let read_write_result = self.check_read_write_conflicts(tx_info);
                if read_write_result.has_conflict {
                    return read_write_result;
                }

                let write_write_result = self.check_write_write_conflicts(tx_info);
                if write_write_result.has_conflict {
                    return write_write_result;
                }

                self.check_write_read_conflicts(tx_id, tx_info, &active_txs)
            }
        }
    }

    /// 检查读-写冲突（快照隔离的核心）
    fn check_read_write_conflicts(&self, tx_info: &ActiveTransactionInfo) -> ConflictResult {
        // 如果没有读取数据，直接返回无冲突
        if tx_info.read_keys.is_empty() {
            return ConflictResult::no_conflict();
        }

        let key_versions = self.key_versions.read().unwrap();
        let graph_versions = match key_versions.get(&tx_info.graph_id) {
            Some(versions) => versions,
            None => return ConflictResult::no_conflict(),
        };

        // 检查每个读取的键是否在事务开始后被修改
        for key in &tx_info.read_keys {
            if let Some(&last_written_version) = graph_versions.get(key) {
                if last_written_version > tx_info.snapshot_version {
                    return ConflictResult::read_write_conflict(
                        key.clone(),
                        last_written_version,
                        tx_info.snapshot_version,
                    );
                }
            }
        }

        ConflictResult::no_conflict()
    }

    /// 检查写-写冲突
    fn check_write_write_conflicts(&self, tx_info: &ActiveTransactionInfo) -> ConflictResult {
        // 如果没有写入数据，直接返回无冲突
        if tx_info.write_keys.is_empty() {
            return ConflictResult::no_conflict();
        }

        let key_versions = self.key_versions.read().unwrap();
        let graph_versions = match key_versions.get(&tx_info.graph_id) {
            Some(versions) => versions,
            None => return ConflictResult::no_conflict(),
        };

        // 检查每个要写入的键是否在事务开始后被其他事务修改
        for key in &tx_info.write_keys {
            if let Some(&last_written_version) = graph_versions.get(key) {
                if last_written_version > tx_info.snapshot_version {
                    return ConflictResult::write_write_conflict(
                        key.clone(),
                        last_written_version,
                        tx_info.snapshot_version,
                    );
                }
            }
        }

        ConflictResult::no_conflict()
    }

    /// 检查写-读冲突（可序列化隔离级别需要）
    fn check_write_read_conflicts(
        &self,
        tx_id: u64,
        tx_info: &ActiveTransactionInfo,
        active_txs: &HashMap<u64, ActiveTransactionInfo>,
    ) -> ConflictResult {
        // 检查当前事务的写集是否与其他事务的读集冲突
        for (other_tx_id, other_info) in active_txs {
            if *other_tx_id == tx_id || other_info.state != TransactionState::Active {
                continue;
            }

            // 如果其他事务的开始时间晚于当前事务，检查是否有写-读冲突
            if other_info.snapshot_version > tx_info.snapshot_version {
                for key in &tx_info.write_keys {
                    if other_info.read_keys.contains(key) {
                        return ConflictResult {
                            has_conflict: true,
                            conflict_key: Some(key.clone()),
                            conflict_version: None,
                            conflict_type: "WriteRead".to_string(),
                            message: format!(
                                "写-读冲突: 键 {:?} 被事务 {} 读取，但当前事务修改了它",
                                String::from_utf8_lossy(key),
                                other_tx_id
                            ),
                        };
                    }
                }
            }
        }

        ConflictResult::no_conflict()
    }

    /// 更新键的版本信息（事务提交成功后调用）
    pub fn update_key_versions(
        &self,
        tx_id: u64,
        graph_id: u32,
        commit_version: u64,
        write_keys: &[Vec<u8>],
    ) {
        // 更新键的版本
        if !write_keys.is_empty() {
            let mut key_versions = self.key_versions.write().unwrap();
            let graph_versions = key_versions.entry(graph_id).or_insert_with(HashMap::new);

            for key in write_keys {
                graph_versions.insert(key.clone(), commit_version);
            }
        }

        // 清理事务信息
        let mut active_txs = self.active_transactions.write().unwrap();
        active_txs.remove(&tx_id);
    }

    /// 清理事务（事务回滚或中止时调用）
    pub fn cleanup_transaction(&self, tx_id: u64) {
        let mut active_txs = self.active_transactions.write().unwrap();
        active_txs.remove(&tx_id);
    }

    /// 获取键的最后写入版本（用于调试和测试）
    pub fn get_key_last_version(&self, graph_id: u32, key: &[u8]) -> Option<u64> {
        let key_versions = self.key_versions.read().unwrap();
        key_versions
            .get(&graph_id)
            .and_then(|graph_versions| graph_versions.get(key).copied())
    }

    /// 获取活跃事务数量
    pub fn get_active_count(&self) -> usize {
        self.active_transactions.read().unwrap().len()
    }

    /// 清理过期的键版本信息（可以定期调用）
    pub fn cleanup_old_versions(&self, min_version_to_keep: u64) {
        let mut key_versions = self.key_versions.write().unwrap();

        for (_graph_id, graph_versions) in key_versions.iter_mut() {
            // 移除版本小于 min_version_to_keep 的键
            // 注意：这里我们保留所有版本，因为实际使用中应该有MVCC清理机制
            // 这里只是示例，实际实现应该更复杂
            graph_versions.retain(|_, &mut version| version >= min_version_to_keep);
        }
    }
}
