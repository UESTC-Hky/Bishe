mod error;
mod log_store;
mod rpc_server;

use crate::error::Result;
use log_store::LogStore;
use rpc_server::start_rpc_server;
use std::path::Path;
use tracing::{info, warn};

#[derive(clap::Parser)]
#[command(version, about)]
struct Args {
    /// 数据存储路径
    #[arg(short, long, default_value = "/tmp/ls_data")]
    data_path: String,
    
    /// 服务监听地址
    #[arg(short, long, default_value = "0.0.0.0:25002")]
    listen_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt::init();
    
    // 解析命令行参数
    let args = <Args as clap::Parser>::parse();
    
    info!("启动单机LS系统...");
    info!("数据路径: {}", args.data_path);
    info!("监听地址: {}", args.listen_addr);
    
    // 创建存储目录
    if let Err(e) = std::fs::create_dir_all(&args.data_path) {
        warn!("创建目录失败: {}, 错误: {}", args.data_path, e);
    }
    
    // 创建存储引擎
    let store = LogStore::new(Path::new(&args.data_path))?;
    info!("LS存储引擎初始化完成");
    
    // 启动RPC服务器
    info!("启动gRPC服务...");
    start_rpc_server(store, &args.listen_addr).await?;
    
    Ok(())
}