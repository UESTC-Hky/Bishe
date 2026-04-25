use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::transaction::TransactionManager;
use rpc::ts::transaction_service_server::TransactionService;
use rpc::ts::*;

pub struct TransactionServiceImpl {
    tx_manager: Arc<TransactionManager>,
}

impl TransactionServiceImpl {
    pub fn new(tx_manager: Arc<TransactionManager>) -> Self {
        Self { tx_manager }
    }
}

#[tonic::async_trait]
impl TransactionService for TransactionServiceImpl {
    /// 开始事务
    async fn begin_transaction(
        &self,
        request: Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        info!("收到BeginTransaction请求");
        
        let req = request.into_inner();
        
        // 使用 TryFrom 替代已弃用的 from_i32
        let isolation_level = rpc::ts::IsolationLevel::try_from(req.isolation_level)
            .unwrap_or(rpc::ts::IsolationLevel::Snapshot);
        
        // 超时时间，默认5秒
        let timeout_ms = req.timeout_ms.unwrap_or(5000);
        
        match self.tx_manager.begin_transaction(
            req.graph_id,
            isolation_level,
            timeout_ms,
        ).await {
            Ok(tx) => {
                let response = BeginTransactionResponse {
                    transaction_id: tx.id,
                    snapshot_version: tx.snapshot_version,
                    success: true,
                    message: "事务开始成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("开始事务失败: {}", e);
                let response = BeginTransactionResponse {
                    transaction_id: 0,
                    snapshot_version: 0,
                    success: false,
                    message: format!("开始事务失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 提交事务
    async fn commit_transaction(
        &self,
        request: Request<CommitTransactionRequest>,
    ) -> Result<Response<CommitTransactionResponse>, Status> {
        info!("收到CommitTransaction请求");
        
        let req = request.into_inner();
        
        match self.tx_manager.commit_transaction(req.transaction_id).await {
            Ok(commit_version) => {
                let response = CommitTransactionResponse {
                    success: true,
                    commit_version,
                    message: "事务提交成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("提交事务失败: {}", e);
                let response = CommitTransactionResponse {
                    success: false,
                    commit_version: 0,
                    message: format!("提交事务失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 回滚事务
    async fn rollback_transaction(
        &self,
        request: Request<RollbackTransactionRequest>,
    ) -> Result<Response<RollbackTransactionResponse>, Status> {
        info!("收到RollbackTransaction请求");
        
        let req = request.into_inner();
        
        match self.tx_manager.rollback_transaction(req.transaction_id, "客户端请求回滚").await {
            Ok(()) => {
                let response = RollbackTransactionResponse {
                    success: true,
                    message: "事务回滚成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("回滚事务失败: {}", e);
                let response = RollbackTransactionResponse {
                    success: false,
                    message: format!("回滚事务失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 读取数据
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        info!("收到Get请求");
        
        let req = request.into_inner();
        
        match self.tx_manager.read(
            req.transaction_id,
            req.key,
            req.version,
        ).await {
            Ok(value) => {
                let response = GetResponse {
                    value,
                    read_version: 0, // TODO: 从事务中获取实际读取版本
                    success: true,
                    message: "读取成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("读取数据失败: {}", e);
                let response = GetResponse {
                    value: None,
                    read_version: 0,
                    success: false,
                    message: format!("读取失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 写入数据
    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        info!("收到Set请求");
        
        let req = request.into_inner();
        
        match self.tx_manager.write(
            req.transaction_id,
            req.key,
            req.value,
        ).await {
            Ok(()) => {
                let response = SetResponse {
                    success: true,
                    message: "写入成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("写入数据失败: {}", e);
                let response = SetResponse {
                    success: false,
                    message: format!("写入失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 删除数据
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        info!("收到Delete请求");
        
        let req = request.into_inner();
        
        match self.tx_manager.delete(
            req.transaction_id,
            req.key,
        ).await {
            Ok(()) => {
                let response = DeleteResponse {
                    success: true,
                    message: "删除成功".into(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                warn!("删除数据失败: {}", e);
                let response = DeleteResponse {
                    success: false,
                    message: format!("删除失败: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }
    
    /// 批量读取
    async fn multi_get(
        &self,
        request: Request<MultiGetRequest>,
    ) -> Result<Response<MultiGetResponse>, Status> {
        info!("收到MultiGet请求");
        
        let req = request.into_inner();
        
        // 简化实现：逐个读取
        let mut values = Vec::new();
        let mut all_success = true;
        let mut error_message = String::new();
        
        for key in req.keys {
            match self.tx_manager.read(
                req.transaction_id,
                key,
                req.version,
            ).await {
                Ok(value) => {
                    values.push(GetResponse {
                        value,
                        read_version: 0, // TODO: 实际读取版本
                        success: true,
                        message: "".into(),
                    });
                }
                Err(e) => {
                    all_success = false;
                    error_message = format!("批量读取失败: {}", e);
                    break;
                }
            }
        }
        
        let response = MultiGetResponse {
            values,
            success: all_success,
            message: error_message,
        };
        Ok(Response::new(response))
    }
    
    /// 批量写入
    async fn batch_write(
        &self,
        request: Request<BatchWriteRequest>,
    ) -> Result<Response<BatchWriteResponse>, Status> {
        info!("收到BatchWrite请求");
        
        let req = request.into_inner();
        
        // 处理设置操作
        let mut sets_applied = 0;
        for set_op in req.sets {
            match self.tx_manager.write(
                req.transaction_id,
                set_op.key,
                set_op.value,
            ).await {
                Ok(()) => sets_applied += 1,
                Err(e) => {
                    warn!("批量写入失败: {}", e);
                    let response = BatchWriteResponse {
                        success: false,
                        sets_applied,
                        deletes_applied: 0,
                        message: format!("批量写入失败: {}", e),
                    };
                    return Ok(Response::new(response));
                }
            }
        }
        
        // 处理删除操作
        let mut deletes_applied = 0;
        for key in req.deletes {
            match self.tx_manager.delete(
                req.transaction_id,
                key,
            ).await {
                Ok(()) => deletes_applied += 1,
                Err(e) => {
                    warn!("批量删除失败: {}", e);
                    let response = BatchWriteResponse {
                        success: false,
                        sets_applied,
                        deletes_applied,
                        message: format!("批量删除失败: {}", e),
                    };
                    return Ok(Response::new(response));
                }
            }
        }
        
        let response = BatchWriteResponse {
            success: true,
            sets_applied,
            deletes_applied,
            message: "批量操作成功".into(),
        };
        Ok(Response::new(response))
    }
    
    /// 创建图
    async fn create_graph(
        &self,
        _request: Request<CreateGraphRequest>,
    ) -> Result<Response<bool>, Status> {
        // 根据生成的代码，这里应该返回 bool
        // TODO: 实现实际逻辑
        Ok(Response::new(true))
    }
    
    /// 删除图
    async fn drop_graph(
        &self,
        _request: Request<DropGraphRequest>,
    ) -> Result<Response<bool>, Status> {
        info!("收到DropGraph请求");
        
        // TODO: 实现实际逻辑
        Ok(Response::new(true))
    }
    
    /// 刷新图
    async fn flush_graph(
        &self,
        _request: Request<FlushGraphRequest>,
    ) -> Result<Response<bool>, Status> {
        info!("收到FlushGraph请求");
        
        // TODO: 实现实际逻辑
        Ok(Response::new(true))
    }
    
    /// 健康检查
    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        info!("收到Ping请求");
        
        let server_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        
        let response = PingResponse {
            alive: true,
            server_time_ms,
            version: "0.1.0".into(),
        };
        Ok(Response::new(response))
    }
    
    /// 获取状态
    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        info!("收到Status请求");
        
        let req = request.into_inner();
        
        let active_transactions = self.tx_manager.get_active_transaction_count() as u32;
        
        let mut metrics = std::collections::HashMap::new();
        if req.include_metrics {
            metrics.insert("active_transactions".to_string(), active_transactions.to_string());
        }
        
        let response = StatusResponse {
            active: true,
            active_transactions,
            total_transactions: 0,
            uptime_ms: 0,
            metrics,
            connected_servers: vec!["LS".to_string(), "SS".to_string()],
        };
        Ok(Response::new(response))
    }
}