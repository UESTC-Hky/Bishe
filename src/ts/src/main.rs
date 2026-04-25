// src/bin/ts.rs
use std::sync::Arc;
use clap::Parser;

mod config;
mod error;
mod transaction;
mod client;
mod service;

use error::Result;
use transaction::TransactionManager;
use service::TransactionServiceImpl;
use rpc::ts::transaction_service_server::TransactionServiceServer;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:25003")]
    addr: String,
    
    #[arg(long, default_value = "127.0.0.1:25002")]
    ls_addr: String,
    
    #[arg(long, default_value = "127.0.0.1:25001")]
    ss_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    tracing::info!("启动 TS 服务...");
    tracing::info!("监听地址: {}", args.addr);
    tracing::info!("LS地址: {}", args.ls_addr);
    //tracing::info!("SS地址: {}", args.ss_addr);
    
    // 解析监听地址
    let addr = args.addr.parse()
        .map_err(|e| error::TsError::InvalidArgument(format!("地址解析失败: {}", e)))?;
    
    // 初始化 LS 客户端 
    let ls_addr = format!("http://{}", args.ls_addr);
    let ls_client = client::LsClient::new(ls_addr).await
        .expect("TS: 连接LS服务失败");
    
    // 初始化 SS 客户端
    let ss_addr = format!("http://{}", args.ss_addr);
    let ss_client = client::SsClient::new(ss_addr).await
        .expect("TS: 连接SS服务失败");
    
    // 创建 WAL 目录
    let wal_dir = std::path::PathBuf::from("./wal_logs");
    std::fs::create_dir_all(&wal_dir).ok();

    // 创建事务管理器（支持ACID）
    let tx_manager = TransactionManager::new(ls_client, ss_client, wal_dir)
        .await
        .expect("TS: 创建事务管理器失败");
    
    let tx_manager_arc = Arc::new(tx_manager);
    
    // 创建事务服务实现
    let tx_service = TransactionServiceImpl::new(tx_manager_arc);
    
    tracing::info!("TS 服务启动成功，等待连接...");
    
    // 启动 gRPC 服务器
    tonic::transport::Server::builder()
        .add_service(TransactionServiceServer::new(tx_service))
        .serve(addr)
        .await
        .map_err(|e| error::TsError::Grpc(e.to_string()))?;
    
    Ok(())
}