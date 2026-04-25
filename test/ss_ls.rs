// src/bin/ss_ls.rs
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tonic::transport::Channel;
use tonic::Request;

use rpc::ts::transaction_service_client::TransactionServiceClient;
use rpc::ts::{
    BeginTransactionRequest, CommitTransactionRequest, GetRequest, IsolationLevel, SetRequest,
};

// 辅助函数：将 Option<Vec<u8>> 转换为可显示的字符串
fn opt_vec_display(val: &Option<Vec<u8>>) -> String {
    match val {
        Some(v) => String::from_utf8_lossy(v).to_string(),
        None => "None".to_string(),
    }
}

// ---------- 客户端定义 ----------
#[derive(Clone)]
struct TsClient {
    client: TransactionServiceClient<Channel>,
    addr: String,
}

impl TsClient {
    async fn new(addr: String) -> Result<Self, String> {
        println!("连接TS服务: {}", addr);
        let client = TransactionServiceClient::connect(addr.clone())
            .await
            .map_err(|e| format!("连接TS失败: {}", e))?;
        println!("TS连接成功");
        Ok(Self { client, addr })
    }

    async fn begin_transaction(
        &mut self,
        graph_id: u32,
        isolation_level: IsolationLevel,
        timeout_ms: u64,
    ) -> Result<rpc::ts::BeginTransactionResponse, String> {
        println!(
            "开始事务: graph={}, isolation={:?}",
            graph_id, isolation_level
        );
        let request = BeginTransactionRequest {
            graph_id,
            isolation_level: isolation_level as i32,
            timeout_ms: Some(timeout_ms),
        };
        match self.client.begin_transaction(Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    println!(
                        "事务开始成功: id={}, snapshot={}",
                        resp.transaction_id, resp.snapshot_version
                    );
                } else {
                    println!("事务开始失败: {}", resp.message);
                }
                Ok(resp)
            }
            Err(e) => {
                println!("开始事务RPC错误: {}", e);
                Err(format!("RPC错误: {}", e))
            }
        }
    }

    async fn commit_transaction(
        &mut self,
        transaction_id: u64,
    ) -> Result<rpc::ts::CommitTransactionResponse, String> {
        println!("提交事务: {}", transaction_id);
        let request = CommitTransactionRequest { transaction_id };
        match self.client.commit_transaction(Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    println!("事务提交成功: version={}", resp.commit_version);
                } else {
                    println!("事务提交失败: {}", resp.message);
                }
                Ok(resp)
            }
            Err(e) => {
                println!("提交事务RPC错误: {}", e);
                Err(format!("RPC错误: {}", e))
            }
        }
    }

    async fn get(
        &mut self,
        transaction_id: u64,
        key: Vec<u8>,
        version: Option<u64>,
    ) -> Result<rpc::ts::GetResponse, String> {
        println!("读取数据: tx={}, key_len={}", transaction_id, key.len());
        let request = GetRequest {
            transaction_id,
            key,
            version,
        };
        match self.client.get(Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                Ok(resp)
            }
            Err(e) => {
                println!("读取数据RPC错误: {}", e);
                Err(format!("RPC错误: {}", e))
            }
        }
    }

    async fn set(
        &mut self,
        transaction_id: u64,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<rpc::ts::SetResponse, String> {
        println!(
            "写入数据: tx={}, key_len={}, value_len={}",
            transaction_id,
            key.len(),
            value.len()
        );
        let request = SetRequest {
            transaction_id,
            key,
            value,
        };
        match self.client.set(Request::new(request)).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.success {
                    println!("写入成功");
                } else {
                    println!("写入失败: {}", resp.message);
                }
                Ok(resp)
            }
            Err(e) => {
                println!("写入数据RPC错误: {}", e);
                Err(format!("RPC错误: {}", e))
            }
        }
    }
}

// ---------- 辅助函数：重试读取直到成功 ----------
async fn retry_get(
    ts_client: &mut TsClient,
    graph_id: u32,
    key: Vec<u8>,
    expected: Vec<u8>,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let begin = ts_client
            .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
            .await?;
        let get = ts_client
            .get(begin.transaction_id, key.clone(), None)
            .await?;
        ts_client.commit_transaction(begin.transaction_id).await?;

        if get.value == Some(expected.clone()) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(format!(
        "重试超时 {} 秒，未获取到期望值。期望: {:?}",
        timeout.as_secs(),
        String::from_utf8_lossy(&expected)
    ))
}

// ---------- 测试1：原子性 ----------
async fn test_atomicity() -> Result<(), String> {
    println!("\n=== 测试1：原子性（多键写入） ===");
    let mut client = TsClient::new("http://127.0.0.1:25003".to_string()).await?;
    let graph_id = 1;

    let begin = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    let tx_id = begin.transaction_id;

    client
        .set(tx_id, b"atom_key1".to_vec(), b"value1".to_vec())
        .await?;
    client
        .set(tx_id, b"atom_key2".to_vec(), b"value2".to_vec())
        .await?;

    let commit = client.commit_transaction(tx_id).await?;
    if !commit.success {
        return Err(format!("事务提交失败: {}", commit.message));
    }
    println!("  提交成功，版本: {}", commit.commit_version);

    // 重试验证两个键都存在
    retry_get(
        &mut client,
        graph_id,
        b"atom_key1".to_vec(),
        b"value1".to_vec(),
        Duration::from_secs(10),
    )
    .await?;
    retry_get(
        &mut client,
        graph_id,
        b"atom_key2".to_vec(),
        b"value2".to_vec(),
        Duration::from_secs(10),
    )
    .await?;
    println!("✓ 原子性测试通过");
    Ok(())
}

// ---------- 测试2：冲突检测（写-写冲突） ----------
async fn test_conflict_detection() -> Result<(), String> {
    println!("\n=== 测试2：冲突检测（写-写冲突） ===");
    let mut client = TsClient::new("http://127.0.0.1:25003".to_string()).await?;
    let graph_id = 1;

    // 事务 A 写入
    let tx_a = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    client
        .set(tx_a.transaction_id, b"conflict_key".to_vec(), b"A".to_vec())
        .await?;

    // 事务 B 写入同一键
    let tx_b = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    client
        .set(tx_b.transaction_id, b"conflict_key".to_vec(), b"B".to_vec())
        .await?;

    // 提交 A（应成功）
    let commit_a = client.commit_transaction(tx_a.transaction_id).await?;
    if !commit_a.success {
        return Err("事务 A 提交失败".into());
    }
    println!("  事务 A 提交成功");

    // 提交 B（应失败，因为写-写冲突）
    let commit_b = client.commit_transaction(tx_b.transaction_id).await;
    match commit_b {
        Ok(resp) if !resp.success => println!("  事务 B 提交失败（预期）: {}", resp.message),
        Ok(_) => return Err("事务 B 应该失败但成功了".into()),
        Err(e) if e.contains("冲突") || e.contains("Conflict") => {
            println!("  事务 B 提交失败（预期）: {}", e)
        }
        Err(e) => return Err(format!("事务 B 提交时发生错误: {}", e)),
    }

    // 验证最终值为 A
    let read_tx = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    let read = client
        .get(read_tx.transaction_id, b"conflict_key".to_vec(), None)
        .await?;
    if read.value != Some(b"A".to_vec()) {
        return Err(format!(
            "最终值应为 A, 实际: {}",
            opt_vec_display(&read.value)
        ));
    }
    println!("✓ 冲突检测测试通过");
    Ok(())
}

// ---------- 测试3：多版本读取 ----------
async fn test_multiversion() -> Result<(), String> {
    println!("\n=== 测试3：多版本读取 ===");
    let mut client = TsClient::new("http://127.0.0.1:25003".to_string()).await?;
    let graph_id = 1;

    // 写入版本 1
    let tx1 = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    client
        .set(tx1.transaction_id, b"mv_key".to_vec(), b"v1".to_vec())
        .await?;
    let commit1 = client.commit_transaction(tx1.transaction_id).await?;
    if !commit1.success {
        return Err("版本1提交失败".into());
    }
    println!("  版本1提交，版本号: {}", commit1.commit_version);

    // 等待 SS 应用
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 写入版本 2
    let tx2 = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    client
        .set(tx2.transaction_id, b"mv_key".to_vec(), b"v2".to_vec())
        .await?;
    let commit2 = client.commit_transaction(tx2.transaction_id).await?;
    if !commit2.success {
        return Err("版本2提交失败".into());
    }
    println!("  版本2提交，版本号: {}", commit2.commit_version);

    // 等待 SS 应用
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 在快照版本1下读取，应得到 v1
    let read_tx_v1 = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    let read_v1 = client
        .get(
            read_tx_v1.transaction_id,
            b"mv_key".to_vec(),
            Some(commit1.commit_version),
        )
        .await?;
    if read_v1.value != Some(b"v1".to_vec()) {
        return Err(format!(
            "版本1读取错误，期望 v1, 实际: {}",
            opt_vec_display(&read_v1.value)
        ));
    }
    println!("  在版本1下读取正确");

    // 在快照版本2下读取，应得到 v2
    let read_tx_v2 = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    let read_v2 = client
        .get(
            read_tx_v2.transaction_id,
            b"mv_key".to_vec(),
            Some(commit2.commit_version),
        )
        .await?;
    if read_v2.value != Some(b"v2".to_vec()) {
        return Err(format!(
            "版本2读取错误，期望 v2, 实际: {}",
            opt_vec_display(&read_v2.value)
        ));
    }
    println!("  在版本2下读取正确");

    // 在最新版本（不指定版本）下读取，应得到 v2
    let read_tx_latest = client
        .begin_transaction(graph_id, IsolationLevel::Snapshot, 5000)
        .await?;
    let read_latest = client
        .get(read_tx_latest.transaction_id, b"mv_key".to_vec(), None)
        .await?;
    if read_latest.value != Some(b"v2".to_vec()) {
        return Err(format!(
            "最新版本读取错误，期望 v2, 实际: {}",
            opt_vec_display(&read_latest.value)
        ));
    }
    println!("  最新版本读取正确");

    println!("✓ 多版本读取测试通过");
    Ok(())
}

// ---------- 主函数 ----------
type TestFunc = Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>>>>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 分布式事务键值存储系统核心功能测试 ===");
    println!("注意：请确保 LS、SS、TS 服务已在 127.0.0.1 相应端口启动。");

    let tests: Vec<(&str, TestFunc)> = vec![
        ("原子性", Box::new(|| Box::pin(test_atomicity()))),
        ("冲突检测", Box::new(|| Box::pin(test_conflict_detection()))),
        ("多版本读取", Box::new(|| Box::pin(test_multiversion()))),
    ];

    let mut passed = 0;
    let mut failed = 0;

    for (name, test_fn) in tests {
        print!("运行测试: {} ... ", name);
        match test_fn().await {
            Ok(_) => {
                println!("通过");
                passed += 1;
            }
            Err(e) => {
                println!("失败: {}", e);
                failed += 1;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("\n测试结果: 通过 {}, 失败 {}", passed, failed);
    if failed == 0 {
        println!("✓ 所有核心功能测试通过");
        Ok(())
    } else {
        Err("存在测试失败".into())
    }
}
