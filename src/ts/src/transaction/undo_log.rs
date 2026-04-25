use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use rpc::common::WriteSet;

/// Undo 操作类型
#[derive(Debug, Clone, PartialEq)]
pub enum UndoOp {
    /// 插入操作（undo时需要删除）
    Insert(Vec<u8>, Vec<u8>),
    /// 更新操作（undo时需要恢复旧值）
    Update(Vec<u8>, Vec<u8>),
    /// 删除操作（undo时需要恢复被删的值）
    Delete(Vec<u8>, Vec<u8>),
    /// 清空操作
    Clear(Vec<u8>),
}

/// Undo 日志条目
#[derive(Debug, Clone)]
pub struct UndoLogEntry {
    /// 事务ID
    pub tx_id: u64,
    /// 操作序列号
    pub seq_no: u64,
    /// Undo 操作
    pub op: UndoOp,
}

/// Undo 日志
/// 用于事务回滚时撤销已执行的修改
pub struct UndoLog {
    /// 事务ID -> 该事务的所有 Undo 条目列表
    entries: Arc<Mutex<HashMap<u64, Vec<UndoLogEntry>>>>,
    /// 全局序列号计数器
    seq_counter: Arc<Mutex<u64>>,
}

impl UndoLog {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            seq_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// 记录一个 Undo 操作
    pub fn record(&self, tx_id: u64, op: UndoOp) -> Result<()> {
        let mut entries = self.entries.lock().unwrap();
        let mut counter = self.seq_counter.lock().unwrap();
        *counter += 1;

        let tx_entries = entries.entry(tx_id).or_insert_with(Vec::new);
        tx_entries.push(UndoLogEntry {
            tx_id,
            seq_no: *counter,
            op,
        });

        Ok(())
    }

    /// 获取指定事务的所有 Undo 条目（按序列号顺序，用于回滚时反向执行）
    pub fn get_entries_for_tx(&self, tx_id: u64) -> Vec<UndoLogEntry> {
        let entries = self.entries.lock().unwrap();
        entries.get(&tx_id).cloned().unwrap_or_default()
    }

    /// 清除指定事务的 Undo 日志（事务提交后调用）
    pub fn clear_tx(&self, tx_id: u64) {
        let mut entries = self.entries.lock().unwrap();
        entries.remove(&tx_id);
    }

    /// 获取当前记录的 Undo 条目总数
    pub fn total_entries(&self) -> usize {
        let entries = self.entries.lock().unwrap();
        entries.values().map(|v| v.len()).sum()
    }

    /// 获取活跃的 Undo 事务数量
    pub fn active_tx_count(&self) -> usize {
        let entries = self.entries.lock().unwrap();
        entries.len()
    }

    /// 检查指定事务是否有 Undo 条目
    pub fn is_empty(&self, tx_id: u64) -> bool {
        let entries = self.entries.lock().unwrap();
        entries.get(&tx_id).map(|v| v.is_empty()).unwrap_or(true)
    }
}

/// 根据 Undo 日志条目构建回滚 WriteSet
/// 将 UndoOp 列表转换为 LS 可执行的 WriteSet
pub fn build_rollback_write_set(undo_log: &UndoLog, tx_id: u64) -> WriteSet {
    let mut upsert_kvs = Vec::new();
    let mut deleted_keys = Vec::new();

    let entries = undo_log.get_entries_for_tx(tx_id);
    for entry in entries.iter().rev() {
        match &entry.op {
            UndoOp::Insert(key, _) => {
                // 插入操作回滚：删除该键
                deleted_keys.push(key.clone());
            }
            UndoOp::Update(key, old_value) => {
                // 更新操作回滚：恢复旧值
                upsert_kvs.push(rpc::common::Kv {
                    key: key.clone(),
                    value: old_value.clone(),
                });
            }
            UndoOp::Delete(key, old_value) => {
                // 删除操作回滚：恢复被删的值
                upsert_kvs.push(rpc::common::Kv {
                    key: key.clone(),
                    value: old_value.clone(),
                });
            }
            UndoOp::Clear(key) => {
                // 清空操作回滚：不做特殊处理
                // 因为 Clear 是批量操作的兜底，需要由调用方自行处理
            }
        }
    }

    WriteSet {
        upsert_kvs,
        deleted_keys,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_log_record_and_clear() {
        let log = UndoLog::new();
        let tx_id = 1;

        log.record(tx_id, UndoOp::Insert(b"key1".to_vec(), b"val1".to_vec()))
            .unwrap();
        log.record(tx_id, UndoOp::Update(b"key2".to_vec(), b"old_val".to_vec()))
            .unwrap();
        log.record(
            tx_id,
            UndoOp::Delete(b"key3".to_vec(), b"deleted_val".to_vec()),
        )
        .unwrap();

        assert_eq!(log.total_entries(), 3);

        let entries = log.get_entries_for_tx(tx_id);
        assert_eq!(entries.len(), 3);

        // 清除
        log.clear_tx(tx_id);
        assert_eq!(log.total_entries(), 0);
    }

    #[test]
    fn test_undo_log_multiple_tx() {
        let log = UndoLog::new();

        log.record(1, UndoOp::Insert(b"k1".to_vec(), b"v1".to_vec()))
            .unwrap();
        log.record(2, UndoOp::Insert(b"k2".to_vec(), b"v2".to_vec()))
            .unwrap();

        assert_eq!(log.total_entries(), 2);
        assert_eq!(log.active_tx_count(), 2);

        log.clear_tx(1);
        assert_eq!(log.active_tx_count(), 1);
        assert_eq!(log.total_entries(), 1);
    }
}
