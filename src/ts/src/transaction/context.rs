use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::transaction::undo_log::{UndoLog, UndoLogEntry, UndoOp};

#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,      // 活跃状态
    Preparing,   // 准备提交
    Prepared,    // 已准备（2PC第一阶段完成）
    Committed,   // 已提交
    RollingBack, // 正在回滚
    RolledBack,  // 已回滚
    Aborted,     // 已中止（系统强制）
}

#[derive(Debug, Clone)]
pub struct ReadRecord {
    pub key: Vec<u8>,
    pub version: u64,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct WriteRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    // 基本属性
    pub id: u64,
    pub graph_id: u32,
    pub state: TransactionState,
    pub snapshot_version: u64,
    pub start_time: Instant,
    pub timeout: Duration,

    // 隔离级别
    pub isolation_level: rpc::ts::IsolationLevel,

    // 读写集（用于冲突检测和提交）
    pub read_set: Vec<ReadRecord>,
    pub write_set: HashMap<Vec<u8>, Option<Vec<u8>>>, // key -> value (None表示删除)

    // 用于快速查找
    pub read_keys: HashSet<Vec<u8>>,
}

impl Transaction {
    pub fn new(
        id: u64,
        graph_id: u32,
        snapshot_version: u64,
        isolation_level: rpc::ts::IsolationLevel,
        timeout_ms: u64,
    ) -> Self {
        Self {
            id,
            graph_id,
            state: TransactionState::Active,
            snapshot_version,
            start_time: Instant::now(),
            timeout: Duration::from_millis(timeout_ms),
            isolation_level,
            read_set: Vec::new(),
            write_set: HashMap::new(),
            read_keys: HashSet::new(),
        }
    }

    /// 检查事务是否超时
    pub fn is_timed_out(&self) -> bool {
        self.start_time.elapsed() > self.timeout
    }

    /// 记录读操作
    pub fn record_read(&mut self, key: Vec<u8>, version: u64, value: Option<Vec<u8>>) {
        if !self.read_keys.contains(&key) {
            self.read_set.push(ReadRecord {
                key: key.clone(),
                version,
                value,
            });
            self.read_keys.insert(key);
        }
    }

    /// 记录写操作
    pub fn record_write(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.write_set.insert(key, value);
    }

    /// 检查是否读取了指定键
    pub fn has_read_key(&self, key: &[u8]) -> bool {
        self.read_keys.contains(key)
    }

    /// 获取读取的版本号
    pub fn get_read_version(&self, key: &[u8]) -> Option<u64> {
        self.read_set
            .iter()
            .find(|r| r.key == key)
            .map(|r| r.version)
    }

    /// 将写集转换为LS的WriteSet格式
    pub fn to_write_set(&self) -> rpc::common::WriteSet {
        let mut upsert_kvs = Vec::new();
        let mut deleted_keys = Vec::new();

        for (key, value) in &self.write_set {
            match value {
                Some(value) => {
                    upsert_kvs.push(rpc::common::Kv {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                None => {
                    deleted_keys.push(key.clone());
                }
            }
        }

        rpc::common::WriteSet {
            upsert_kvs,
            deleted_keys,
        }
    }

    /// 转换为LS的LogEntry格式
    pub fn to_log_entry(
        &self,
        prev_commit_version: u64,
        commit_version: u64,
    ) -> rpc::log::LogEntry {
        rpc::log::LogEntry {
            prev_commit_version,
            commit_version,
            graph_id: self.graph_id,
            write_set: Some(self.to_write_set()),
        }
    }
}
