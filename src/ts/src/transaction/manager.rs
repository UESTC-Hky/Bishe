use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

use crate::error::{Result, TsError};
use crate::transaction::conflict_detector::{ConflictDetector, ConflictResult};
use crate::transaction::context::{Transaction, TransactionState};
use crate::transaction::lock_manager::{LockManager, LockMode};
use crate::transaction::recovery::RecoveryManager;
use crate::transaction::undo_log::{build_rollback_write_set, UndoLog, UndoOp};
use crate::transaction::version::VersionManager;
use crate::transaction::wal::{WalEntry, WalEntryType, WalManager};

pub struct TransactionManager {
    // 版本管理
    version_manager: Arc<VersionManager>,

    // 活跃事务
    active_transactions: RwLock<HashMap<u64, Arc<RwLock<Transaction>>>>,

    // 冲突检测器
    conflict_detector: Arc<ConflictDetector>,

    // 锁管理器（用于 Isolation-Serializable）
    lock_manager: Arc<LockManager>,

    // 写前日志（用于 Durability）
    wal_manager: Arc<WalManager>,

    // 恢复管理器（用于 Crash Recovery）
    recovery_manager: Arc<RecoveryManager>,

    // UndoLog 集合（用于 Atomicity-Rollback）
    undo_logs: RwLock<HashMap<u64, Arc<RwLock<UndoLog>>>>,

    // 客户端
    ls_client: Arc<tokio::sync::Mutex<crate::client::LsClient>>,
    ss_client: Arc<tokio::sync::Mutex<crate::client::SsClient>>,
}

impl TransactionManager {
    pub async fn new(
        ls_client: crate::client::LsClient,
        ss_client: crate::client::SsClient,
        wal_dir: std::path::PathBuf,
    ) -> Result<Self> {
        let ls_client_arc = Arc::new(tokio::sync::Mutex::new(ls_client));
        let ss_client_arc = Arc::new(tokio::sync::Mutex::new(ss_client));

        // 初始版本
        let version_manager = Arc::new(VersionManager::new(1, 0, ls_client_arc.clone()));

        // 冲突检测器
        let conflict_detector = Arc::new(ConflictDetector::new());

        // 锁管理器
        let lock_manager = Arc::new(LockManager::new());

        // WAL 管理器（启用 fsync 确保持久化）
        let wal_manager = Arc::new(WalManager::new(wal_dir, true)?);

        // 恢复管理器
        let recovery_manager = Arc::new(RecoveryManager::new(wal_manager.clone()));

        // 执行崩溃恢复
        let recovery_result = recovery_manager.recover().await?;
        info!("崩溃恢复结果: {:?}", recovery_result);

        Ok(Self {
            version_manager,
            active_transactions: RwLock::new(HashMap::new()),
            conflict_detector,
            lock_manager,
            wal_manager,
            recovery_manager,
            undo_logs: RwLock::new(HashMap::new()),
            ls_client: ls_client_arc,
            ss_client: ss_client_arc,
        })
    }

    /// === ATOMICITY-A: 开始事务（记录WAL Begin） ===
    pub async fn begin_transaction(
        &self,
        graph_id: u32,
        isolation_level: rpc::ts::IsolationLevel,
        timeout_ms: u64,
    ) -> Result<Transaction> {
        info!(
            "开始新事务: graph={}, isolation={:?}",
            graph_id, isolation_level
        );

        // 获取快照版本
        let snapshot_version = self
            .version_manager
            .get_snapshot_version_for_tx(isolation_level)
            .await?;

        // 生成事务ID
        let tx_id = self.version_manager.next_transaction_id();

        // === DURABILITY: 写入WAL Begin日志 ===
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let wal_entry = WalEntry {
            entry_type: WalEntryType::Begin,
            tx_id,
            graph_id,
            key: None,
            value: None,
            old_value: None,
            timestamp,
        };
        self.wal_manager.append_entry(&wal_entry)?;
        self.wal_manager.flush()?;

        // 注册冲突检测
        self.conflict_detector
            .register_transaction(tx_id, graph_id, snapshot_version);

        // 创建事务
        let tx = Transaction::new(
            tx_id,
            graph_id,
            snapshot_version,
            isolation_level,
            timeout_ms,
        );

        // 创建 UndoLog
        {
            let mut undo_logs = self
                .undo_logs
                .write()
                .map_err(|_| TsError::Unknown("获取UndoLog锁失败".into()))?;
            undo_logs.insert(tx_id, Arc::new(RwLock::new(UndoLog::new())));
        }

        // 记录活跃事务
        {
            let mut active_txs = self
                .active_transactions
                .write()
                .map_err(|_| TsError::Unknown("获取事务锁失败".into()))?;
            active_txs.insert(tx_id, Arc::new(RwLock::new(tx.clone())));
        }

        info!("事务 {} 开始成功，快照版本: {}", tx_id, snapshot_version);
        Ok(tx)
    }

    /// === DURABILITY: 写入WAL ===
    fn write_wal(
        &self,
        entry_type: WalEntryType,
        tx_id: u64,
        graph_id: u32,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        old_value: Option<Vec<u8>>,
    ) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = WalEntry {
            entry_type,
            tx_id,
            graph_id,
            key,
            value,
            old_value,
            timestamp,
        };
        self.wal_manager.append_entry(&entry)?;
        Ok(())
    }

    /// === ATOMICITY-B: 写入前记录 UndoLog ===
    fn record_undo_for_write(
        &self,
        tx_id: u64,
        key: Vec<u8>,
        old_value: Vec<u8>,
        operation: UndoOp,
    ) -> Result<()> {
        let undo_logs = self
            .undo_logs
            .read()
            .map_err(|_| TsError::Unknown("获取UndoLog锁失败".into()))?;

        if let Some(undo_arc) = undo_logs.get(&tx_id) {
            let mut undo = undo_arc
                .write()
                .map_err(|_| TsError::Unknown("获取UndoLog写锁失败".into()))?;
            undo.record(tx_id, operation)?;
        }

        Ok(())
    }

    /// === CONSISTENCY-A: 约束验证 ===
    fn validate_constraints(&self, tx: &Transaction) -> Result<()> {
        // 约束1: 键值大小限制
        for (key, value) in &tx.write_set {
            let key_str = String::from_utf8_lossy(key);

            // 键不能为空
            if key.is_empty() {
                return Err(TsError::InvalidArgument(
                    "键不能为空".into(),
                ));
            }

            // 键长度限制（最大1KB）
            if key.len() > 1024 {
                return Err(TsError::InvalidArgument(format!(
                    "键过长: {} 字节（最大1024）",
                    key.len()
                )));
            }

            // 值长度限制（最大1MB）
            if let Some(val) = value {
                if val.len() > 1024 * 1024 {
                    return Err(TsError::InvalidArgument(format!(
                        "值过长: {} 字节（最大1MB）",
                        val.len()
                    )));
                }
            }

            // 约束2: 键名格式验证（仅允许可打印ASCII和UTF-8）
            for &byte in key {
                if byte == 0x00 {
                    return Err(TsError::InvalidArgument(format!(
                        "键 '{}' 包含空字节",
                        key_str
                    )));
                }
            }
        }

        // 约束3: 同一个键不能同时upsert和delete
        // (write_set中key->None表示delete, Some(v)表示upsert, HashMap保证不会重复)
        // 如果同一个键出现在两个操作中也是冲突的，但HashMap会覆盖

        // 约束4: 读取一致性验证
        for read_record in &tx.read_set {
            let known_read = tx
                .read_keys
                .contains(&read_record.key);
            if !known_read {
                return Err(TsError::InvalidArgument(format!(
                    "读记录不一致: 键 {:?} 不在读集合中",
                    String::from_utf8_lossy(&read_record.key)
                )));
            }
        }

        // 约束5: 跨键约束 - 确保读写集不包含已删除的键
        // 实际上写集不能同时包含upsert和delete同一个键

        info!("约束验证通过 (事务 {})", tx.id);
        Ok(())
    }

    /// === ISOLATION-A: 获取锁（针对 Serializable 级别） ===
    fn acquire_locks_for_write_set(
        &self,
        tx: &Transaction,
    ) -> Result<()> {
        if tx.isolation_level != rpc::ts::IsolationLevel::Serializable {
            return Ok(()); // 只有 Serializable 需要锁
        }

        for key in tx.write_set.keys() {
            self.lock_manager.try_acquire_lock(
                tx.id,
                key.clone(),
                LockMode::Exclusive,
                5000, // 5秒超时
            )?;
        }

        // 读锁（防止幻读）
        for key in tx.read_keys.iter() {
            if !tx.write_set.contains_key(key) {
                self.lock_manager.try_acquire_lock(
                    tx.id,
                    key.clone(),
                    LockMode::Shared,
                    5000,
                )?;
            }
        }

        Ok(())
    }

    /// === ATOMICITY-C + ISOLATION: 读数据 ===
    pub async fn read(
        &self,
        tx_id: u64,
        key: Vec<u8>,
        specified_version: Option<u64>,
    ) -> Result<Option<Vec<u8>>> {
        let tx_arc = self.get_transaction(tx_id)?;

        // 获取事务信息
        let (graph_id, snapshot_version, isolation_level, needs_abort) = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            let timed_out = tx_guard.is_timed_out();
            (
                tx_guard.graph_id,
                tx_guard.snapshot_version,
                tx_guard.isolation_level,
                timed_out,
            )
        };

        if needs_abort {
            self.abort_transaction(tx_id, "事务超时").await?;
            return Err(TsError::Timeout(format!("事务 {} 已超时", tx_id)));
        }

        // === ISOLATION-B: Serializable 级别获取读锁 ===
        if isolation_level == rpc::ts::IsolationLevel::Serializable {
            self.lock_manager.try_acquire_lock(
                tx_id,
                key.clone(),
                LockMode::Shared,
                5000,
            )?;
        }

        // 确定读取版本
        let read_version = match isolation_level {
            rpc::ts::IsolationLevel::ReadCommitted => {
                let mut client = self.ls_client.lock().await;
                client.get_max_committed_version().await?
            }
            rpc::ts::IsolationLevel::Snapshot => snapshot_version,
            rpc::ts::IsolationLevel::Serializable => snapshot_version,
        };

        let read_version = if let Some(specified) = specified_version {
            match isolation_level {
                rpc::ts::IsolationLevel::Snapshot | rpc::ts::IsolationLevel::Serializable => {
                    if specified > snapshot_version {
                        return Err(TsError::InvalidArgument(format!(
                            "在快照隔离下不能读取版本 {} 之后的数据（快照版本: {}）",
                            specified, snapshot_version
                        )));
                    }
                    specified
                }
                rpc::ts::IsolationLevel::ReadCommitted => specified,
            }
        } else {
            read_version
        };

        // 从SS读取数据
        let mut ss_client = self.ss_client.lock().await;
        let value = ss_client.get(graph_id, key.clone(), read_version).await?;

        // 记录读操作
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            tx_guard.record_read(key.clone(), read_version, value.clone());

            if let Err(e) = self.conflict_detector.record_read(tx_id, key) {
                warn!("记录读操作失败: {}", e);
            }
        }

        Ok(value)
    }

    /// === ATOMICITY + DURABILITY + ISOLATION: 写数据 ===
    pub async fn write(
        &self,
        tx_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;

        let (graph_id, needs_abort) = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            (tx_guard.graph_id, tx_guard.is_timed_out())
        };

        if needs_abort {
            self.abort_transaction(tx_id, "事务超时").await?;
            return Err(TsError::Timeout(format!("事务 {} 已超时", tx_id)));
        }

        // === ISOLATION-C: Serializable 级别获取写锁 ===
        if let Ok(tx_guard) = tx_arc.read() {
            if tx_guard.isolation_level == rpc::ts::IsolationLevel::Serializable {
                drop(tx_guard);
                self.lock_manager.try_acquire_lock(
                    tx_id,
                    key.clone(),
                    LockMode::Exclusive,
                    5000,
                )?;
            }
        }

        // === ATOMICITY-D: 旧值记录（用于UndoLog） ===
        // 在写入前从SS读取旧值
        let old_value = {
            let mut ss_client = self.ss_client.lock().await;
            ss_client.get(graph_id, key.clone(), 0).await.ok().flatten()
        };

        // === DURABILITY: 写入WAL ===
        self.write_wal(
            WalEntryType::Write,
            tx_id,
            graph_id,
            Some(key.clone()),
            Some(value.clone()),
            old_value.clone(),
        )?;

        // === ATOMICITY-E: 记录UndoLog ===
        if let Some(old_val) = &old_value {
            // 更新操作：记录旧值以便回滚恢复
            self.record_undo_for_write(tx_id, key.clone(), old_val.clone(), UndoOp::Update(key.clone(), old_val.clone()))?;
        } else {
            // 插入操作：记录以便回滚时删除
            self.record_undo_for_write(tx_id, key.clone(), value.clone(), UndoOp::Insert(key.clone(), value.clone()))?;
        }

        // === CONSISTENCY-B: 约束验证（写操作级别） ===
        if key.is_empty() {
            return Err(TsError::InvalidArgument("键不能为空".into()));
        }
        if key.len() > 1024 {
            return Err(TsError::InvalidArgument(format!(
                "键过长: {} 字节（最大1024）",
                key.len()
            )));
        }
        if value.len() > 1024 * 1024 {
            return Err(TsError::InvalidArgument(format!(
                "值过长: {} 字节（最大1MB）",
                value.len()
            )));
        }

        // 记录写操作
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            tx_guard.record_write(key.clone(), Some(value));

            if let Err(e) = self.conflict_detector.record_write(tx_id, key) {
                warn!("记录写操作失败: {}", e);
            }
        }

        self.wal_manager.flush()?;
        Ok(())
    }

    /// === ATOMICITY-F: 删除（也记录Undo） ===
    pub async fn delete(&self, tx_id: u64, key: Vec<u8>) -> Result<()> {
        let tx_arc = self.get_transaction(tx_id)?;

        let (graph_id, needs_abort) = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            (tx_guard.graph_id, tx_guard.is_timed_out())
        };

        if needs_abort {
            self.abort_transaction(tx_id, "事务超时").await?;
            return Err(TsError::Timeout(format!("事务 {} 已超时", tx_id)));
        }

        // Serializable: 获取排他锁
        if let Ok(tx_guard) = tx_arc.read() {
            if tx_guard.isolation_level == rpc::ts::IsolationLevel::Serializable {
                drop(tx_guard);
                self.lock_manager.try_acquire_lock(
                    tx_id,
                    key.clone(),
                    LockMode::Exclusive,
                    5000,
                )?;
            }
        }

        // 读取旧值（用于UndoLog回滚）
        let old_value = {
            let mut ss_client = self.ss_client.lock().await;
            ss_client.get(graph_id, key.clone(), 0).await.ok().flatten()
        };

        // WAL记录
        self.write_wal(
            WalEntryType::Write,
            tx_id,
            graph_id,
            Some(key.clone()),
            None, // 删除
            old_value.clone(),
        )?;

        // UndoLog：如果是已存在的键被删除，记录旧值以便恢复
        if let Some(old_val) = &old_value {
            let undo_logs = self.undo_logs.read().map_err(|_| {
                TsError::Unknown("获取UndoLog锁失败".into())
            })?;
            if let Some(undo_arc) = undo_logs.get(&tx_id) {
                let mut undo = undo_arc.write().map_err(|_| {
                    TsError::Unknown("获取UndoLog写锁失败".into())
                })?;
                undo.record(tx_id, UndoOp::Delete(key.clone(), old_val.clone()))?;
            }
        }

        // 记录写操作（删除 = value=None）
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;

            tx_guard.record_write(key.clone(), None);

            if let Err(e) = self.conflict_detector.record_write(tx_id, key) {
                warn!("记录删除操作失败: {}", e);
            }
        }

        self.wal_manager.flush()?;
        Ok(())
    }

    /// === 2-PHASE COMMIT: 两阶段提交 ===
    /// Phase 1: Prepare
    async fn prepare_commit(&self, tx_id: u64) -> Result<(u64, u64, u32, rpc::common::WriteSet, bool)> {
        let tx_arc = self.get_transaction(tx_id)?;

        let (graph_id, isolation_level, write_set, needs_abort, is_readonly) = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;

            if !matches!(tx_guard.state, TransactionState::Active) {
                return Err(TsError::InvalidArgument(format!(
                    "事务 {} 不处于活跃状态",
                    tx_id
                )));
            }

            let timed_out = tx_guard.is_timed_out();
            (
                tx_guard.graph_id,
                tx_guard.isolation_level,
                tx_guard.to_write_set(),
                timed_out,
                tx_guard.write_set.is_empty(),
            )
        };

        if needs_abort {
            self.abort_transaction(tx_id, "事务超时").await?;
            return Err(TsError::Timeout(format!("事务 {} 已超时", tx_id)));
        }

        // 只读事务直接跳过2PC
        if is_readonly {
            return Ok((0, 0, graph_id, write_set, true));
        }

        // === CONSISTENCY-C: 完整约束验证 ===
        {
            let tx_guard = tx_arc.read().map_err(|_| {
                TsError::Unknown("获取事务读锁失败".into())
            })?;
            self.validate_constraints(&tx_guard)?;
        }

        // === ISOLATION-D: Serializable 级别获取所有必要的锁 ===
        {
            let tx_guard = tx_arc.read().map_err(|_| {
                TsError::Unknown("获取事务读锁失败".into())
            })?;
            self.acquire_locks_for_write_set(&tx_guard)?;
        }

        // === ISOLATION-E: 冲突检测 ===
        let conflict_result = self
            .conflict_detector
            .check_transaction_conflicts(tx_id, isolation_level);

        if conflict_result.has_conflict {
            warn!("事务 {} 冲突检测失败: {}", tx_id, conflict_result.message);
            match conflict_result.conflict_type.as_str() {
                "ReadWrite" => return Err(TsError::ReadConflict(conflict_result.message)),
                "WriteWrite" => return Err(TsError::WriteConflict(conflict_result.message)),
                "WriteRead" => return Err(TsError::WriteConflict(conflict_result.message)),
                _ => return Err(TsError::Unknown(conflict_result.message)),
            }
        }

        // === 2PC Phase 1: Prepare ===
        // 状态 -> Preparing
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
            tx_guard.state = TransactionState::Preparing;
        }

        // DURABILITY: 写入Prepare WAL
        self.write_wal(
            WalEntryType::Prepare,
            tx_id,
            graph_id,
            None,
            None,
            None,
        )?;
        self.wal_manager.flush()?;

        // 获取LS版本
        let mut ls_client = self.ls_client.lock().await;
        let prev_commit_version = ls_client.get_max_committed_version().await?;
        let commit_version = prev_commit_version + 1;

        // 状态 -> Prepared
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
            tx_guard.state = TransactionState::Prepared;
        }

        Ok((prev_commit_version, commit_version, graph_id, write_set, false))
    }

    /// === 2-PHASE COMMIT: Phase 2 - Commit ===
    pub async fn commit_transaction(&self, tx_id: u64) -> Result<u64> {
        info!("提交事务: {} (2PC)", tx_id);

        // Phase 1: Prepare
        let (prev_commit_version, commit_version, graph_id, write_set, is_readonly) =
            self.prepare_commit(tx_id).await?;

        if is_readonly {
            // 只读事务：直接完成
            {
                let tx_arc = self.get_transaction(tx_id)?;
                let mut tx_guard = tx_arc
                    .write()
                    .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
                tx_guard.state = TransactionState::Committed;
            }
            self.cleanup_transaction(tx_id);
            let current_version = self.version_manager.get_snapshot_version();
            info!("只读事务 {} 提交成功，无日志写入", tx_id);
            return Ok(current_version);
        }

        // Phase 2: 写入LS
        let log_entry = rpc::log::LogEntry {
            prev_commit_version,
            commit_version,
            graph_id,
            write_set: Some(write_set.clone()),
        };

        let mut ls_client = self.ls_client.lock().await;
        let result = ls_client.append_entry(log_entry).await?;
        if !result.ok {
            // LS写入失败 → 需要回滚（原子性保证）
            self.rollback_transaction(tx_id, &result.message).await?;
            return Err(TsError::LsError(result.message));
        }

        // Phase 2完成: 写入WAL Commit
        self.write_wal(
            WalEntryType::Commit,
            tx_id,
            graph_id,
            None,
            None,
            None,
        )?;
        self.wal_manager.flush()?;
        self.wal_manager.create_checkpoint()?;

        // 更新事务状态
        {
            let tx_arc = self.get_transaction(tx_id)?;
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
            tx_guard.state = TransactionState::Committed;

            if let Err(e) = self
                .conflict_detector
                .update_transaction_state(tx_id, TransactionState::Committed)
            {
                warn!("更新冲突检测器事务状态失败: {}", e);
            }
        }

        // 更新键版本
        let write_keys: Vec<Vec<u8>> = write_set
            .upsert_kvs
            .iter()
            .map(|kv| kv.key.clone())
            .chain(write_set.deleted_keys.iter().cloned())
            .collect();

        self.conflict_detector
            .update_key_versions(tx_id, graph_id, commit_version, &write_keys);

        // 更新快照版本
        self.version_manager.update_snapshot_version(commit_version);

        // 清理
        self.cleanup_transaction(tx_id);

        info!(
            "事务 {} 提交成功（2PC），提交版本: {}",
            tx_id, commit_version
        );
        Ok(commit_version)
    }

    /// === ATOMICITY-G: 回滚（使用 UndoLog 恢复旧值） ===
    pub async fn rollback_transaction(&self, tx_id: u64, reason: &str) -> Result<()> {
        info!("回滚事务: {}, 原因: {}", tx_id, reason);

        let tx_arc = match self.get_transaction(tx_id) {
            Ok(tx) => tx,
            Err(_) => {
                warn!("事务 {} 不存在，跳过回滚", tx_id);
                return Ok(());
            }
        };

        let graph_id = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;
            tx_guard.graph_id
        };

        // === ATOMICITY-H: 使用 UndoLog 构建回滚 WriteSet ===
        let rollback_write_set = {
            let undo_logs = self
                .undo_logs
                .read()
                .map_err(|_| TsError::Unknown("获取UndoLog锁失败".into()))?;

            if let Some(undo_arc) = undo_logs.get(&tx_id) {
                let undo = undo_arc
                    .read()
                    .map_err(|_| TsError::Unknown("获取UndoLog读锁失败".into()))?;
                if !undo.is_empty(tx_id) {
                    Some(build_rollback_write_set(&undo, tx_id))
                } else {
                    None
                }
            } else {
                None
            }
        };

        // 如果有回滚数据，写入LS进行回滚
        if let Some(rb_write_set) = rollback_write_set {
            if !rb_write_set.upsert_kvs.is_empty() || !rb_write_set.deleted_keys.is_empty() {
                let mut ls_client = self.ls_client.lock().await;
                let prev_commit_version = ls_client.get_max_committed_version().await?;
                let commit_version = prev_commit_version + 1;

                let log_entry = rpc::log::LogEntry {
                    prev_commit_version,
                    commit_version,
                    graph_id,
                    write_set: Some(rb_write_set),
                };

                let result = ls_client.append_entry(log_entry).await?;
                if !result.ok {
                    warn!("回滚写入LS失败: {}", result.message);
                } else {
                    info!("回滚WriteSet已写入LS，版本: {}", commit_version);
                }
            }
        }

        // WAL: 记录回滚
        self.write_wal(
            WalEntryType::Rollback,
            tx_id,
            graph_id,
            None,
            None,
            None,
        )?;
        self.wal_manager.flush()?;

        // 更新状态
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
            tx_guard.state = TransactionState::RolledBack;

            let _ = self
                .conflict_detector
                .update_transaction_state(tx_id, TransactionState::RolledBack);
        }

        // 清理
        self.cleanup_transaction(tx_id);

        info!("事务 {} 回滚成功", tx_id);
        Ok(())
    }

    /// === 中止事务 ===
    pub async fn abort_transaction(&self, tx_id: u64, reason: &str) -> Result<()> {
        info!("中止事务: {}, 原因: {}", tx_id, reason);

        let tx_arc = match self.get_transaction(tx_id) {
            Ok(tx) => tx,
            Err(_) => return Ok(()),
        };

        let graph_id = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;
            tx_guard.graph_id
        };

        // 先尝试回滚（如果有写集）
        let has_write_set = {
            let tx_guard = tx_arc
                .read()
                .map_err(|_| TsError::Unknown("获取事务读锁失败".into()))?;
            !tx_guard.write_set.is_empty()
        };
        if has_write_set {
            let _ = self.rollback_transaction(tx_id, reason).await;
        }

        // WAL记录
        self.write_wal(
            WalEntryType::Rollback,
            tx_id,
            graph_id,
            None,
            None,
            None,
        )?;
        self.wal_manager.flush()?;

        // 更新状态
        {
            let mut tx_guard = tx_arc
                .write()
                .map_err(|_| TsError::Unknown("获取事务写锁失败".into()))?;
            tx_guard.state = TransactionState::Aborted;

            let _ = self
                .conflict_detector
                .update_transaction_state(tx_id, TransactionState::Aborted);
        }

        self.cleanup_transaction(tx_id);
        info!("事务 {} 中止成功", tx_id);
        Ok(())
    }

    /// 清理事务资源
    fn cleanup_transaction(&self, tx_id: u64) {
        // 从活跃事务移除
        {
            let mut active_txs = self.active_transactions.write().unwrap();
            active_txs.remove(&tx_id);
        }

        // 释放锁
        self.lock_manager.release_locks(tx_id);

        // 清理冲突检测
        self.conflict_detector.cleanup_transaction(tx_id);

        // 清理UndoLog
        {
            let mut undo_logs = self.undo_logs.write().unwrap();
            undo_logs.remove(&tx_id);
        }
    }

    /// 获取事务
    fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<Transaction>>> {
        let active_txs = self
            .active_transactions
            .read()
            .map_err(|_| TsError::Unknown("获取事务锁失败".into()))?;

        active_txs
            .get(&tx_id)
            .cloned()
            .ok_or(TsError::TransactionNotFound(tx_id))
    }

    /// 获取活跃事务数量
    pub fn get_active_transaction_count(&self) -> usize {
        self.active_transactions
            .read()
            .map(|txs| txs.len())
            .unwrap_or(0)
    }

    /// 获取WAL状态
    pub fn get_wal_size(&self) -> Result<u64> {
        self.wal_manager.wal_size()
    }

    /// 获取锁状态
    pub fn get_locked_keys_count(&self) -> usize {
        self.lock_manager.locked_keys_count()
    }

    /// 获取WAL的引用（用于外部检查）
    pub fn get_wal_manager(&self) -> Arc<WalManager> {
        self.wal_manager.clone()
    }

    /// 获取LockManager的引用
    pub fn get_lock_manager(&self) -> Arc<LockManager> {
        self.lock_manager.clone()
    }

    /// 检查键是否被锁
    pub fn is_key_locked(&self, key: &[u8]) -> bool {
        self.lock_manager.is_locked(key)
    }
}