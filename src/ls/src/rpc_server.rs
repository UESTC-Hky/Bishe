use tonic::{Request, Response, Status};
use crate::log_store::LogStore;
use crate::error::Result as LsResult;
// Add these imports
use rpc::log::log_server::Log;
use rpc::log::{AppendEntryRequest, AppendEntryResponse, FetchEntriesRequest, FetchEntriesResponse,GetMaxCommittedVersionRequest,GetMaxCommittedVersionResponse};
use rpc::log::log_server::LogServer;

pub struct LogRpcServer {
    store: LogStore,
}

impl LogRpcServer {
    pub fn new(store: LogStore) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl Log for LogRpcServer {  
    async fn get_max_committed_version(
        &self,
        _request: Request<GetMaxCommittedVersionRequest>,  // 使用下划线忽略未使用的参数
    ) -> Result<Response<GetMaxCommittedVersionResponse>, Status> {
        // 从store获取最大提交版本
        let max_version = self.store.get_max_committed_version().await
            .map_err(|e| Status::internal(format!("Failed to get max committed version: {}", e)))?;
        
        let response = GetMaxCommittedVersionResponse {
            max_committed_version: max_version,
        };
        
        Ok(Response::new(response))
        }
    

    async fn append_entry(
        &self,
        request: Request<AppendEntryRequest>,  
    ) -> std::result::Result<Response<AppendEntryResponse>, Status> {  
        let req = request.into_inner();
        let log_entry = req.log_entry.ok_or_else(|| {
            Status::invalid_argument("LogEntry is required")
        })?;
        
        match self.store.append_entry(log_entry).await {
            Ok(_) => {
                let response = AppendEntryResponse {  
                    ok: true,
                    message: "Success".to_string(),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                let response = AppendEntryResponse { 
                    ok: false,
                    message: format!("Failed to append entry: {}", e),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn fetch_entries(
        &self,
        request: Request<FetchEntriesRequest>,  
    ) -> std::result::Result<Response<FetchEntriesResponse>, Status> {  
        let req = request.into_inner();
        
        let entries = self.store
            .fetch_entries(req.prev_commit_version, req.max_entries)
            .await;
        
        let response = FetchEntriesResponse {  
            log_entries: entries,
        };
        
        Ok(Response::new(response))
    }

}
pub async fn start_rpc_server(store: LogStore, addr: &str) -> LsResult<()> {
    let server = LogRpcServer::new(store);
    
    let addr = addr.parse()?;
    
    tonic::transport::Server::builder()
        .add_service(LogServer::new(server))  
        .serve(addr)
        .await?;
        
    Ok(())
}