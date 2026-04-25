// ss/src/main.rs
use ss::StoreServerManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    println!("启动 Storage Server (SS)...");
    
    // 创建存储服务器管理器
    let ssm = StoreServerManager::new(Some("/tmp/ss_data".to_string()));
    
    // 启动服务
    ssm.start().await?;
    
    Ok(())
}
