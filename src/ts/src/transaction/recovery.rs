use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::error::{Result, TsError};
use crate::transaction::wal::{WalEntry, WalEntryType, WalManager};

/// 崩溃恢复管理器
/// 用于在系统重启后从 WAL 中恢复未完成的事务
pub struct RecoveryManager {
    wal_manager: Arc<WalManager>,
}

impl RecoveryManager {
    pub fn new(wal_manager: Arc<WalManager>) -> Self {
        Self { wal_manager }
    }

    /// 执行崩溃恢复
    /// 遍历 WAL 日志，根据事务状态决定：
    /// - 只有 Begin 没有 Commit → 需要回滚
    /// - 有 Prepare 没有 Commit → 需要回滚
    /// - 有 Prepare 和 Commit → 事务已完成
    pub async fn recover(&self) -> Result<RecoveryResult> {
        info!("开始崩溃恢复...");

        let entries = self.wal_manager.read_all_entries()?;
        info!("读取到 {} 条 WAL 条目", entries.len());

        let mut tx_states: HashMap<u64, TxRecoveryState> = HashMap::new();

        for entry in &entries {
            let state = tx_states.entry(entry.tx_id).or_insert(TxRecoveryState {
                tx_id: entry.tx_id,
                graph_id: entry.graph_id,
                has_begin: false,
                has_prepare: false,
                has_commit: false,
                has_rollback: false,
                writes: Vec::new(),
            });

            match entry.entry_type {
                WalEntryType::Begin => {
                    state.has_begin = true;
                }
                WalEntryType::Write => {
                    if let (Some(key), Some(value)) = (&entry.key, &entry.value) {
                        state.writes.push((key.clone(), value.clone()));
                    }
                }
                WalEntryType::Prepare => {
                    state.has_prepare = true;
                }
                WalEntryType::Commit => {
                    state.has_commit = true;
                }
                WalEntryType::Rollback => {
                    state.has_rollback = true;
                }
            }
        }

        let mut recovered_txs = 0;
        let mut rolled_back_txs = 0;

        for (_tx_id, state) in &tx_states {
            if state.has_commit {
                // 已提交，无需恢复
                info!("事务 {} 已提交，跳过恢复", state.tx_id);
                continue;
            }

            if state.has_rollback {
                // 已回滚，无需处理
                info!("事务 {} 已回滚，跳过恢复", state.tx_id);
                continue;
            }

            if state.has_begin {
                // 只有 Begin → 或 Prepare → 需要回滚
                warn!("事务 {} 未完成（begin={}, prepare={}, commit={}），执行回滚",
                    state.tx_id, state.has_begin, state.has_prepare, state.has_commit);

                // 记录回滚日志
                let rollback_entry = WalEntry {
                    entry_type: WalEntryType::Rollback,
                    tx_id: state.tx_id,
                    graph_id: state.graph_id,
                    key: None,
                    value: None,
                    old_value: None,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                };
                self.wal_manager.append_entry(&rollback_entry)?;
                self.wal_manager.flush()?;

                rolled_back_txs += 1;
                info!("事务 {} 已从 WAL 恢复（主动回滚）", state.tx_id);
            }

            recovered_txs += 1;
        }

        // 创建检查点
        self.wal_manager.create_checkpoint()?;

        let result = RecoveryResult {
            total_entries: entries.len(),
            recovered_transactions: recovered_txs,
            rolled_back_transactions: rolled_back_txs,
        };

        info!(
            "崩溃恢复完成: 共{}条条目, 恢复{}个事务, 回滚{}个事务",
            result.total_entries, result.recovered_transactions, result.rolled_back_transactions
        );

        Ok(result)
    }

    /// 验证 WAL 完整性
    pub fn validate_wal_integrity(&self) -> Result<bool> {
        let entries = self.wal_manager.read_all_entries()?;

        for entry in &entries {
            // 验证条目格式
            if entry.tx_id == 0 {
                return Ok(false);
            }
            // 验证时间戳合理性
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if entry.timestamp > now + 86400 {
                // 时间戳不会超过未来一天
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[derive(Debug, Clone)]
struct TxRecoveryState {
    tx_id: u64,
    graph_id: u32,
    has_begin: bool,
    has_prepare: bool,
    has_commit: bool,
    has_rollback: bool,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct RecoveryResult {
    pub total_entries: usize,
    pub recovered_transactions: u64,
    pub rolled_back_transactions: u64,
}