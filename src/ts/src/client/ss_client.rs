use tonic::transport::Channel;
use tracing::{info, error}; 

use crate::error::{Result, TsError};
use rpc::storage::storage_client::StorageClient;
use rpc::storage::{
    CreateRequest, GetRequest, MultiGetRequest, 
    FlushRequest, DropRequest,
};

#[derive(Clone)]
pub struct SsClient {
    client: StorageClient<Channel>,
    addr: String,
}

impl SsClient {
    /// 创建新的SS客户端
    pub async fn new(addr: String) -> Result<Self> {
        info!("连接SS服务: {}", addr);
        
        let client = StorageClient::connect(addr.clone())
            .await
            .map_err(|e| TsError::SsError(format!("连接SS失败: {}", e)))?;
        
        info!("SS连接成功");
        Ok(Self { client, addr })
    }
    
    /// 读取数据
    pub async fn get(
        &mut self,
        graph_id: u32,
        key: Vec<u8>,
        version: u64,
    ) -> Result<Option<Vec<u8>>> {
        info!("从SS读取数据: graph={}, version={}", graph_id, version);
        
        let request = GetRequest {
            graph_id,
            key,
            version,
        };
        
        match self.client.get(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                Ok(resp.value)
            }
            Err(e) => {
                error!("SS读取数据失败: {}", e);
                Err(TsError::SsError(format!("读取失败: {}", e)))
            }
        }
    }
    
    /// 批量读取数据
    pub async fn multi_get(
        &mut self,
        graph_id: u32,
        keys: Vec<Vec<u8>>,
        version: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        info!("从SS批量读取数据: graph={}, keys={}, version={}", 
              graph_id, keys.len(), version);
        
        let request = MultiGetRequest {
            graph_id,
            keys,
            version,
        };
        
        match self.client.multi_get(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                let values: Vec<Option<Vec<u8>>> = resp.values
                    .into_iter()
                    .map(|r| r.value)
                    .collect();
                Ok(values)
            }
            Err(e) => {
                error!("SS批量读取数据失败: {}", e);
                Err(TsError::SsError(format!("批量读取失败: {}", e)))
            }
        }
    }
    
    /// 创建图
    pub async fn create_graph(&mut self, graph_id: u32) -> Result<bool> {
        info!("在SS创建图: {}", graph_id);
        
        let request = CreateRequest { graph_id };
        
        match self.client.create(request).await {
            Ok(response) => {
                let success = response.into_inner(); // 直接获取 bool
                Ok(success)
            }
            Err(e) => {
                error!("SS创建图失败: {}", e);
                Err(TsError::SsError(format!("创建图失败: {}", e)))
            }
        }
    }

    /// 删除图
    pub async fn drop_graph(&mut self, graph_id: u32) -> Result<bool> {
        info!("在SS删除图: {}", graph_id);
        
        let request = DropRequest { graph_id };
        
        match self.client.drop(request).await {
            Ok(response) => {
                let success = response.into_inner(); // 直接获取 bool
                Ok(success)
            }
            Err(e) => {
                error!("SS删除图失败: {}", e);
                Err(TsError::SsError(format!("删除图失败: {}", e)))
            }
        }
    }

    /// 刷新图数据到磁盘
    pub async fn flush_graph(&mut self, graph_id: u32) -> Result<bool> {
        info!("刷新图数据到磁盘: {}", graph_id);
        
        let request = FlushRequest { graph_id };
        
        match self.client.flush(request).await {
            Ok(response) => {
                let success = response.into_inner(); // 直接获取 bool
                Ok(success)
            }
            Err(e) => {
                error!("SS刷新图失败: {}", e);
                Err(TsError::SsError(format!("刷新图失败: {}", e)))
            }
        }
    }
}