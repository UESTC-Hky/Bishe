// ss/src/main.rs
use ss::StoreServerManager;

#[derive(clap::Parser)]
struct Args {
    /// 数据存储路径
    #[arg(short, long, default_value = "./ss_data")]
    data_path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = <Args as clap::Parser>::parse();
    println!("启动 Storage Server (SS)... 数据路径: {}", args.data_path);

    // 创建存储服务器管理器
    let ssm = StoreServerManager::new(Some(args.data_path));

    // 启动服务
    ssm.start().await?;

    Ok(())
}
