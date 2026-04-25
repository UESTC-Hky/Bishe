// rpc 服务实现
use rpc::common::Kv;
use rpc::storage::storage_server::Storage;
use rpc::storage::CreateRequest;
use rpc::storage::{GetRequest, GetResponse};
use rpc::storage::{MultiGetRequest, MultiGetResponse};
use rpc::storage::FlushRequest;
use rpc::storage::DropRequest;


use crate::storage::RocksDBStorage;
use crate::log2storage::Log2Storage;


use std::sync::Arc;
use tonic::{async_trait, Code, Request, Response, Status};



#[derive(Clone)]
pub struct StorageServerImpl {
    storage: Arc<RocksDBStorage>,
    pub log2storage: Arc<Log2Storage>,
}

// 基础函数接口
impl StorageServerImpl {
    pub async fn new(path: String) -> StorageServerImpl {
        let storage = Arc::new(RocksDBStorage::new(path.clone()));
        let log2storage = Arc::new(
            Log2Storage::new(storage.clone(), path.clone()).await
        );

        StorageServerImpl {
            storage,
            log2storage
        }
    }
}


// storage相关RPC接口
#[async_trait]
impl Storage for StorageServerImpl {
    async fn create(&self,request: Request<CreateRequest>) -> Result<Response<bool>, Status> {
        let request = request.into_inner();
        self
            .storage
            .create_cf(&request.graph_id.to_string())
            .map_err(|err| Status::new(Code::Internal, format!("{}", err)))?;

        Ok(Response::new(true))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let request = request.into_inner();
        
        let value = self.storage
            .get(request.graph_id, request.key, request.version)
            .map_err(|err| Status::new(Code::Internal, format!("{}", err)))?;

        Ok(Response::new(GetResponse { value }))
    }

    async fn multi_get(&self, request: Request<MultiGetRequest>) -> Result<Response<MultiGetResponse>, Status> {
        let request = request.into_inner();
        
        let values = self.storage
            .multi_get(request.graph_id, request.keys, request.version)
            .map_err(|err| Status::new(Code::Internal, format!("{}", err)))?;

        // 将 Vec<Option<Vec<u8>>> 转换为 Vec<GetResponse>
        let responses: Vec<GetResponse> = values
            .into_iter()
            .map(|value| GetResponse { value })
            .collect();

        Ok(Response::new(MultiGetResponse { values: responses }))
    }

    async fn flush(&self, request: Request<FlushRequest>) -> Result<Response<bool>, Status> {
        let request = request.into_inner();
        
        self.storage
            .flush(request.graph_id)
            .await
            .map_err(|err| Status::new(Code::Internal, format!("{}", err)))?;

        Ok(Response::new(true))
    }

    async fn drop(&self, request: Request<DropRequest>) -> Result<Response<bool>, Status> {
        let request = request.into_inner();
        
        self.storage
            .drop_cf(&request.graph_id.to_string())
            .map_err(|err| Status::new(Code::Internal, format!("{}", err)))?;

        Ok(Response::new(true))
    }

}

/*
    以下为RPC服务测试
    注意：测试前应启动服务端，如lib.rs中的test_long_time()
 */
#[cfg(test)]
mod tests {
    use core::{assert_eq, unreachable};

    use super::*;
    use tonic::Request;
    use rpc::storage::storage_client::StorageClient;
    // 基础功能测试
    #[tokio::test]
    async fn test_basic() {

        let mut client = StorageClient::connect("http://127.0.0.1:25001").await.unwrap();

        /* ========================== Get测试  START ==========================*/
        let request = GetRequest {
            graph_id: 43,
            key : b"keyA".to_vec(),
            version : 3
        };
        let resp = client.get(Request::new(request)).await.unwrap();
        // println!("{:#?}",resp);
        assert_eq!(resp.into_inner().value, Some(b"valueA".to_vec()));
        /* ========================== Get测试  END ==========================*/

        /* ========================== MultiGet测试  START ==========================*/
        let request = MultiGetRequest {
            graph_id: 42,
            keys : vec![b"key2".to_vec(), b"key3".to_vec()],
            version : 3
        };
        let resp = client.multi_get(Request::new(request)).await.unwrap();
        // println!("{:#?}",resp);
        assert_eq!(resp.into_inner().values,
            vec![
                GetResponse {
                    value : Some(7i64.to_be_bytes().to_vec())
                },
                GetResponse {
                    value : Some(b"value3".to_vec())
                },
            ] 
        );
        /* ========================== MultiGet测试  END ==========================*/
    }

    // 并发读取测试
    #[tokio::test]
    async fn test_concurrent() {
        // 并发数
        let concurrent_request = 10000;
        // 任务类型数
        let task_types = 2;
        let mut handles = vec![];

        for i in 0..concurrent_request{
            let handle = tokio::spawn(async move {

                let mut client = StorageClient::connect("http://127.0.0.1:25001").await.unwrap();

                match i%task_types {
                    0 => {
                        let request = GetRequest {
                            graph_id: 43,
                            key : b"keyA".to_vec(),
                            version : 3
                        };
                        let resp = client.get(Request::new(request)).await.unwrap();
                        assert_eq!(resp.into_inner().value, Some(b"valueA".to_vec()), "Graph 43 KeyA Version3对应值应为'valueA'");
                    },
                    1 => {
                        let request = MultiGetRequest {
                            graph_id: 42,
                            keys : vec![b"key2".to_vec(), b"key3".to_vec()],
                            version : 3
                        };
                        let resp = client.multi_get(Request::new(request)).await.unwrap();
                        assert_eq!(resp.into_inner().values,
                            vec![
                                GetResponse {
                                    value : Some(7i64.to_be_bytes().to_vec())
                                },
                                GetResponse {
                                    value : Some(b"value3".to_vec())
                                },
                            ] 
                        );                        
                    },
                    _ => {
                        unreachable!()
                    }
                }
            });
            
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
//测试办法
//终端1 - 启动服务器：cargo test test_long_time -- --nocapture
//终端2 - 直接运行编译好的测试程序（不重新编译）：.\target\debug\deps\ss-*.exe test_basic --nocapture