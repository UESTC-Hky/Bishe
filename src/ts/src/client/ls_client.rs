use tonic::transport::Channel;
use tracing::{info, warn, error};

use crate::error::{Result, TsError};
use rpc::log::log_client::LogClient;
use rpc::log::{
    AppendEntryRequest, AppendEntryResponse,
    GetMaxCommittedVersionRequest, 
};

#[derive(Clone)]
pub struct LsClient {
    client: LogClient<Channel>,
    addr: String,
}

impl LsClient {
    /// 创建新的LS客户端
    pub async fn new(addr: String) -> Result<Self> {
        info!("连接LS服务: {}", addr);
        
        let client = LogClient::connect(addr.clone())
            .await
            .map_err(|e| TsError::LsError(format!("连接LS失败: {}", e)))?;
        
        info!("LS连接成功");
        Ok(Self { client, addr })
    }
    
    /// 追加日志条目
    pub async fn append_entry(
        &mut self,
        log_entry: rpc::log::LogEntry,
    ) -> Result<AppendEntryResponse> {
        info!("向LS追加日志条目: version={}", log_entry.commit_version);
        
        let request = AppendEntryRequest { log_entry: Some(log_entry) };
        
        match self.client.append_entry(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.ok {
                    info!("LS日志追加成功");
                } else {
                    warn!("LS日志追加失败: {}", resp.message);
                }
                Ok(resp)
            }
            Err(e) => {
                error!("LS日志追加RPC错误: {}", e);
                Err(TsError::LsError(format!("RPC错误: {}", e)))
            }
        }
    }
    
    /// 获取当前最大提交版本（从LS磁盘读取）
    pub async fn get_max_committed_version(&mut self) -> Result<u64> {
        info!("获取LS最大提交版本");
        
        let request = GetMaxCommittedVersionRequest {};
        
        match self.client.get_max_committed_version(request).await {
            Ok(response) => {
                let version = response.into_inner().max_committed_version;
                info!("LS当前最大提交版本: {}", version);
                Ok(version)
            }
            Err(e) => {
                error!("获取LS最大提交版本失败: {}", e);
                Err(TsError::LsError(format!("获取最大提交版本失败: {}", e)))
            }
        }
    }
    
    /// 健康检查
    pub async fn ping(&mut self) -> Result<bool> {
        // 简单的ping操作，尝试获取版本号
        match self.get_max_committed_version().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}