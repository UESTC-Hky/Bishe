// src/bin/ss_ls.rs
use std::env;
use tonic::transport::Channel;
use tonic::Request;

use rpc::ts::transaction_service_client::TransactionServiceClient;
use rpc::ts::{
    BeginTransactionRequest, CommitTransactionRequest, DeleteRequest, GetRequest, IsolationLevel,
    SetRequest,
};

// ---------- TS 客户端 ----------
#[derive(Clone)]
struct TsClient {
    client: TransactionServiceClient<Channel>,
}

impl TsClient {
    async fn new(addr: String) -> Result<Self, String> {
        let client = TransactionServiceClient::connect(addr.clone())
            .await
            .map_err(|e| format!("连接 TS 失败: {}", e))?;
        Ok(Self { client })
    }

    async fn begin_transaction(
        &mut self,
        graph_id: u32,
        timeout_ms: u64,
    ) -> Result<rpc::ts::BeginTransactionResponse, String> {
        let request = BeginTransactionRequest {
            graph_id,
            isolation_level: IsolationLevel::Snapshot as i32,
            timeout_ms: Some(timeout_ms),
        };
        match self.client.begin_transaction(Request::new(request)).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(format!("开始事务 RPC 错误: {}", e)),
        }
    }

    async fn commit_transaction(
        &mut self,
        transaction_id: u64,
    ) -> Result<rpc::ts::CommitTransactionResponse, String> {
        let request = CommitTransactionRequest { transaction_id };
        match self.client.commit_transaction(Request::new(request)).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(format!("提交事务 RPC 错误: {}", e)),
        }
    }

    async fn get(
        &mut self,
        transaction_id: u64,
        key: Vec<u8>,
        version: Option<u64>,
    ) -> Result<rpc::ts::GetResponse, String> {
        let request = GetRequest {
            transaction_id,
            key,
            version,
        };
        match self.client.get(Request::new(request)).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(format!("读取数据 RPC 错误: {}", e)),
        }
    }

    async fn set(
        &mut self,
        transaction_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<rpc::ts::SetResponse, String> {
        let request = SetRequest {
            transaction_id,
            key,
            value,
        };
        match self.client.set(Request::new(request)).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(format!("写入数据 RPC 错误: {}", e)),
        }
    }

    async fn delete(
        &mut self,
        transaction_id: u64,
        key: Vec<u8>,
    ) -> Result<rpc::ts::DeleteResponse, String> {
        let request = DeleteRequest {
            transaction_id,
            key,
        };
        match self.client.delete(Request::new(request)).await {
            Ok(response) => Ok(response.into_inner()),
            Err(e) => Err(format!("删除数据 RPC 错误: {}", e)),
        }
    }
}

// ---------- 辅助函数 ----------
fn str_to_vec(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// 定义操作类型
#[derive(Debug)]
enum Operation {
    Add { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Read { key: Vec<u8> },
}

// 提交模式
enum CommitMode {
    Post,   // 立即提交
    Unpost, // 暂不提交，等待后续 commit 命令
}

// 解析命令行参数，返回 (图ID, 操作列表, 提交模式)
fn parse_args(args: &[String]) -> Result<(u32, Vec<Operation>, CommitMode), String> {
    if args.len() < 3 {
        return Err("至少需要图ID和一个操作".to_string());
    }

    // 检测模式标志（-post 或 -unpost）可能在最后，也可能在中间？我们假设在最后，先检查最后几个参数
    let mut mode = CommitMode::Post;
    let mut effective_args = args.to_vec();
    if effective_args.last() == Some(&"-post".to_string()) {
        mode = CommitMode::Post;
        effective_args.pop();
    } else if effective_args.last() == Some(&"-unpost".to_string()) {
        mode = CommitMode::Unpost;
        effective_args.pop();
    }

    // 第一个参数是操作类型（add/delete/read）
    let first_cmd = &effective_args[0];
    let graph_id: u32 = effective_args[1].parse().map_err(|_| "图ID必须为数字")?;

    let mut ops = Vec::new();
    let mut i = 2; // 当前索引指向第一个操作的参数

    // 解析第一个操作
    match first_cmd.as_str() {
        "add" => {
            if i + 1 >= effective_args.len() {
                return Err("add 命令需要键和值".to_string());
            }
            let key = str_to_vec(&effective_args[i]);
            let value = str_to_vec(&effective_args[i + 1]);
            ops.push(Operation::Add { key, value });
            i += 2;
        }
        "delete" => {
            if i >= effective_args.len() {
                return Err("delete 命令需要键".to_string());
            }
            let key = str_to_vec(&effective_args[i]);
            ops.push(Operation::Delete { key });
            i += 1;
        }
        "read" => {
            if i >= effective_args.len() {
                return Err("read 命令需要键".to_string());
            }
            let key = str_to_vec(&effective_args[i]);
            ops.push(Operation::Read { key });
            i += 1;
        }
        _ => return Err(format!("未知操作: {}", first_cmd)),
    }

    // 继续解析后续操作，遇到 "+" 分隔符
    while i < effective_args.len() {
        let token = &effective_args[i];
        if token == "+" {
            i += 1;
            if i >= effective_args.len() {
                break;
            }
            // 下一个应该是操作类型
            let cmd = &effective_args[i];
            i += 1;
            match cmd.as_str() {
                "add" => {
                    if i + 1 >= effective_args.len() {
                        return Err("add 命令需要键和值".to_string());
                    }
                    let key = str_to_vec(&effective_args[i]);
                    let value = str_to_vec(&effective_args[i + 1]);
                    ops.push(Operation::Add { key, value });
                    i += 2;
                }
                "delete" => {
                    if i >= effective_args.len() {
                        return Err("delete 命令需要键".to_string());
                    }
                    let key = str_to_vec(&effective_args[i]);
                    ops.push(Operation::Delete { key });
                    i += 1;
                }
                "read" => {
                    if i >= effective_args.len() {
                        return Err("read 命令需要键".to_string());
                    }
                    let key = str_to_vec(&effective_args[i]);
                    ops.push(Operation::Read { key });
                    i += 1;
                }
                _ => return Err(format!("未知操作: {}", cmd)),
            }
        } else {
            return Err(format!("意外的参数: {}，期望 '+' 分隔符", token));
        }
    }

    if ops.is_empty() {
        return Err("没有有效的操作".to_string());
    }

    Ok((graph_id, ops, mode))
}

// ---------- 主函数 ----------
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("用法: {} <命令> [参数...]", args[0]);
        eprintln!("命令:");
        eprintln!("  add/delete/read 图ID 参数... [+ 更多操作...] [-post|-unpost]");
        eprintln!("    例如: add 1 key1 value1 + add 1 key2 value2");
        eprintln!("  commit <事务ID>");
        return Ok(());
    }

    let mut client = TsClient::new("http://127.0.0.1:25003".to_string()).await?;

    // 检查是否为 commit 命令
    if args[1] == "commit" {
        if args.len() != 3 {
            eprintln!("用法: {} commit <事务ID>", args[0]);
            return Ok(());
        }
        let tx_id: u64 = args[2].parse().map_err(|_| "事务ID必须为数字")?;
        let commit = client.commit_transaction(tx_id).await?;
        if !commit.success {
            eprintln!("提交事务失败: {}", commit.message);
        } else {
            println!("事务 {} 提交成功，版本: {}", tx_id, commit.commit_version);
        }
        return Ok(());
    }

    // 否则，解析 add/delete/read 命令
    // 传入从 args[1] 开始的切片（即跳过程序名）
    let (graph_id, ops, mode) = parse_args(&args[1..])?;

    // 开启事务
    let begin = client.begin_transaction(graph_id, 30000).await?;
    if !begin.success {
        eprintln!("开始事务失败: {}", begin.message);
        return Ok(());
    }
    println!("事务开始，ID: {}", begin.transaction_id);

    // 执行所有操作
    for op in ops {
        match op {
            Operation::Add { key, value } => {
                let resp = client
                    .set(begin.transaction_id, key.clone(), value.clone())
                    .await?;
                if !resp.success {
                    eprintln!("add 操作失败: {}", resp.message);
                    return Ok(());
                }
                println!(
                    "add 成功: {} = {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
            }
            Operation::Delete { key } => {
                let resp = client.delete(begin.transaction_id, key.clone()).await?;
                if !resp.success {
                    eprintln!("delete 操作失败: {}", resp.message);
                    return Ok(());
                }
                println!("delete 成功: {}", String::from_utf8_lossy(&key));
            }
            Operation::Read { key } => {
                let resp = client.get(begin.transaction_id, key.clone(), None).await?;
                if !resp.success {
                    eprintln!("read 操作失败: {}", resp.message);
                    return Ok(());
                }
                match resp.value {
                    Some(v) => println!(
                        "读取到: {} = {}",
                        String::from_utf8_lossy(&key),
                        String::from_utf8_lossy(&v)
                    ),
                    None => println!("读取到: {} 不存在", String::from_utf8_lossy(&key)),
                }
            }
        }
    }

    // 根据模式提交
    match mode {
        CommitMode::Post => {
            let commit = client.commit_transaction(begin.transaction_id).await?;
            if !commit.success {
                eprintln!("提交事务失败: {}", commit.message);
            } else {
                println!("事务提交成功，版本: {}", commit.commit_version);
            }
        }
        CommitMode::Unpost => {
            println!(
                "事务未提交，请使用 `commit {}` 手动提交",
                begin.transaction_id
            );
        }
    }

    Ok(())
}
