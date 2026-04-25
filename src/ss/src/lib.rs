#[macro_use]
mod error;
mod storage;
mod log2storage;
pub mod service;

use crate::{error::Result, service::StorageServerImpl};
use rpc::storage::storage_server::StorageServer;
use std::{
    net::IpAddr,
    net::Ipv4Addr,
    net::SocketAddr,
    sync::Arc,
    sync::Mutex,
};
use tokio::sync::oneshot;
use tracing::{info, warn};

#[derive(Clone, Debug, serde::Deserialize)]
pub struct StorageServerConfig {
    #[serde(default = "default_ip")]
    pub ip: IpAddr,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }
fn default_port() -> u16 { 25001 }


struct StopSignal {
    sender: Mutex<Option<oneshot::Sender<()>>>,
}

impl Default for StorageServerConfig {
    fn default() -> Self {
        Self {
            ip: default_ip(),
            port: default_port(),
        }
    }
}

impl StopSignal {
    fn new() -> Self {
        Self {
            sender: Mutex::new(None),
        }
    }

    async fn send(&self) {
        if let Some(sender) = self.sender.lock().unwrap().take() {
            sender.send(()).unwrap_or_default();
        }
    }
}

pub struct StoreServerManager {
    rpc_stop: StopSignal,
    log2storage_stop: StopSignal,
    config: StorageServerConfig,
    data_path: String,
}
// 外部接口
impl StoreServerManager {
    pub fn new(data_path: Option<String>) -> Self {
        let data_path = data_path.unwrap_or_else(|| "/tmp/ss_unit_test".into());
        
        Self {
            rpc_stop: StopSignal::new(),
            log2storage_stop: StopSignal::new(),
            config: StorageServerConfig::default(),
            data_path,
        }
    }

    pub async fn start(&self) -> Result<()> {
        // 构造关闭信号
        let (rpc_sender, rpc_receiver) = oneshot::channel();
        let (log_sender, log_receiver) = oneshot::channel();
        *self.rpc_stop.sender.lock().unwrap() = Some(rpc_sender);
        *self.log2storage_stop.sender.lock().unwrap() = Some(log_sender);

        let server = self.init_server().await?;
        self.start_services(server, rpc_receiver, log_receiver).await
    }

    pub async fn stop(&self) -> Result<()> {
        warn!("ss: Storage initiating shutdown sequence...");
        
        self.rpc_stop.send().await;
        self.log2storage_stop.send().await;
        
        warn!("ss: All storage services stopped");
        Ok(())
    }
}

// 内部函数
impl StoreServerManager {
    async fn init_server(&self) -> Result<Arc<StorageServerImpl>> {
        let server = StorageServerImpl::new(self.data_path.clone()).await;//?;
        Ok(Arc::new(server))
    }

    async fn start_services(
        &self,
        server: Arc<StorageServerImpl>,
        rpc_receiver: oneshot::Receiver<()>,
        log_receiver: oneshot::Receiver<()>,
    ) -> Result<()> {
        let listen_addr = SocketAddr::new(self.config.ip, self.config.port);
        
        // 启动 log2storage 服务 - 返回 JoinHandle
        let log2storage_handle = self.start_log2storage(server.clone(), log_receiver);
        
        // 启动 RPC 服务 - 返回 JoinHandle
        let rpc_handle = self.start_rpc_service(server, listen_addr, rpc_receiver);
        
        // 等待任意一个服务停止
        tokio::select! {
            _ = log2storage_handle => {
                info!("ss: log2storage service stopped");
            }
            _ = rpc_handle => {
                info!("ss: RPC service stopped");
            }
        }
        
        Ok(())
    }

    fn start_log2storage(
        &self,
        server: Arc<StorageServerImpl>,
        receiver: oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let log2storage = server.log2storage.clone();
        
        tokio::spawn(async move {
            // 启动 log2storage
            log2storage.start().await;
            
            // 等待停止信号
            let _ = receiver.await;
            log2storage.stop().await;
            info!("ss: log2storage service shutdown complete");
        })
    }

    fn start_rpc_service(
        &self,
        server: Arc<StorageServerImpl>,
        addr: SocketAddr,
        receiver: oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        let service = rpc::storage::storage_server::StorageServer::new(server.as_ref().clone());
        
        tokio::spawn(async move {
            info!("ss: Starting RPC service on {}", addr);
            
            let server = tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    receiver.await.ok();
                    info!("ss: RPC service received shutdown signal");
                });
            
            if let Err(e) = server.await {
                tracing::error!("ss: RPC server error: {}", e);
            }
            
            info!("ss: RPC service shutdown complete");
        })
    }


}

#[cfg(test)]
mod tests {
    use super::*;
    
    use tempfile::TempDir;
    use std::time::Duration;

    // 开启info日志
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();
    }

    // 短时间运行，用于内部功能测试
    #[tokio::test]
    async fn test_service_lifecycle() {
        init_tracing();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let ssm = StoreServerManager::new(Some(temp_dir.path().to_string_lossy().into()));
        // 运行2s
        ssm.start().await.expect("Failed to start");
        tokio::time::sleep(Duration::from_secs(5)).await;
        ssm.stop().await.expect("Failed to stop");
    }

    // 持续运行，用于对外服务测试
    #[tokio::test]
    async fn test_long_time() {
        init_tracing();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let ssm = StoreServerManager::new(Some(temp_dir.path().to_string_lossy().into()));
        // 持续运行
        ssm.start().await.unwrap();
        let mut count = 0;
        loop{
            tokio::time::sleep(Duration::from_millis(1000)).await;
            count += 1;
            tracing::info!("StoreServer已运行 {}s……",{count});
        }
    }   
}