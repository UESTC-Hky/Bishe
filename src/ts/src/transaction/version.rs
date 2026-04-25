use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::client::LsClient;
use crate::error::Result;

pub struct VersionManager {
    // 当前事务ID计数器
    current_tx_id: AtomicU64,

    // 当前快照版本（从LS获取并维护）
    current_snapshot_version: RwLock<u64>,

    // LS客户端用于获取最新版本
    ls_client: Arc<tokio::sync::Mutex<LsClient>>,
}

impl VersionManager {
    pub fn new(
        initial_tx_id: u64,
        initial_snapshot_version: u64,
        ls_client_arg: Arc<tokio::sync::Mutex<LsClient>>, // 使用不同名称的参数
    ) -> Self {
        Self {
            current_tx_id: AtomicU64::new(initial_tx_id),
            current_snapshot_version: RwLock::new(initial_snapshot_version),
            ls_client: ls_client_arg, // 显式赋值给字段
        }
    }

    /// 获取下一个事务ID
    pub fn next_transaction_id(&self) -> u64 {
        self.current_tx_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 获取当前快照版本
    pub fn get_snapshot_version(&self) -> u64 {
        *self.current_snapshot_version.read().unwrap()
    }

    /// 更新快照版本
    pub fn update_snapshot_version(&self, new_version: u64) {
        let mut version = self.current_snapshot_version.write().unwrap();
        if new_version > *version {
            *version = new_version;
        }
    }

    /// 从LS刷新快照版本（事务开始时调用）
    pub async fn refresh_snapshot_version(&self) -> Result<u64> {
        let mut client = self.ls_client.lock().await;
        let latest_version = client.get_max_committed_version().await?;

        let mut version = self.current_snapshot_version.write().unwrap();
        if latest_version > *version {
            *version = latest_version;
        }

        Ok(*version)
    }

    /// 获取新事务的快照版本
    pub async fn get_snapshot_version_for_tx(
        &self,
        isolation_level: rpc::ts::IsolationLevel,
    ) -> Result<u64> {
        match isolation_level {
            rpc::ts::IsolationLevel::ReadCommitted => {
                // 读已提交：总是获取最新版本
                let mut client = self.ls_client.lock().await;
                client.get_max_committed_version().await
            }
            rpc::ts::IsolationLevel::Snapshot => {
                // 快照隔离：使用事务开始时的版本
                Ok(self.get_snapshot_version())
            }
            rpc::ts::IsolationLevel::Serializable => {
                // 可序列化：也使用快照版本，但需要更强的冲突检测
                Ok(self.get_snapshot_version())
            }
        }
    }
}
