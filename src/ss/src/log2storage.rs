// #[allow(unused_imports)]
// use rpc::common::WriteSet;
// use rpc::log::{LogEntry, FetchEntriesRequest, FetchEntriesResponse};
// #[allow(unused_imports)]
// use rpc::log::log_client::LogClient;
// #[allow(unused_imports)]
// use rpc::storage::storage_server::Storage;
// #[allow(unused_imports)]
// use tonic::transport::Channel;
// #[allow(unused_imports)]
// use tonic::{Code, Request, Status};
// use std::{
//     net::IpAddr,
//     net::Ipv4Addr
// };

// use crate::storage::RocksDBStorage;

// use core::time::Duration;
// use std::collections::{HashMap, HashSet, VecDeque};
// #[allow(unused_imports)]
// use std::{
//     sync::{Arc, Mutex}
// };
// use tokio::{sync::RwLock, task::JoinHandle, time::Instant};
// use tracing::warn;
// #[cfg(test)]
// use rpc::common::Kv;
// #[derive(Clone, Debug, serde::Deserialize)]
// struct LogServerConfig {
//     #[allow(dead_code)]
//     #[serde(default = "default_ip")]
//     pub ip: IpAddr,
//     #[allow(dead_code)]
//     #[serde(default = "default_port")]
//     pub port: u16,
// }

// fn default_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }
// fn default_port() -> u16 { 25002 }

// impl Default for LogServerConfig {
//     fn default() -> Self {
//         Self {
//             ip: default_ip(),
//             port: default_port(),
//         }
//     }
// }


// pub const FETCH_LOG_ENTRY_LIMIT: u32 = 500;
// // 拉取间隔
// const FETCH_INTERVAL: u64 = 1000;

// #[cfg(debug_assertions)]
// const MAX_CACHED_ENTRIES: usize = 500; 
// #[cfg(not(debug_assertions))]
// const MAX_CACHED_ENTRIES: usize = 50;

// macro_rules! wait {
//     ($interval:expr) => {{
//         if $interval > 0 {
//             tokio::time::sleep(Duration::from_millis($interval)).await;
//         }
//     }};
// }

// #[derive(Clone)]
// pub struct Log2Storage {
//     mem_state: Arc<RwLock<MemState>>,
//     pub storage: Arc<RocksDBStorage>,
//     handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
//     // // 单元测试需先注释
//     log_client: LogClient<Channel>,
// }

// // state in memory, recover before start
// struct MemState {
//     /// 当前已落盘（已应用）的最大版本号
//     applied_commit_version: u64,
//     /// 当前缓存中最后一个日志的版本号（尚未全部落盘）
//     max_commit_version: u64,
//     log_entries: VecDeque<LogEntry>,
// }

// impl MemState {
//     fn new(persist_version: u64) -> Self {
//         MemState {
//             applied_commit_version: persist_version,
//             max_commit_version: persist_version,
//             log_entries: VecDeque::with_capacity(MAX_CACHED_ENTRIES), // 预分配内存
//         }
//     }

//     fn cache_full(&self) -> bool {
//         if self.log_entries.len() >= MAX_CACHED_ENTRIES {
//             return true;
//         } else {
//             return false;
//         }
//     }

//     // update applied_commit_version and remove useless caches
//     fn update_applied_commit_version(&mut self, new_applied_commit_version: u64) {
//         self.applied_commit_version = new_applied_commit_version;
        
//         // 移除所有已应用的日志条目
//         while let Some(front) = self.log_entries.front() {
//             if front.commit_version <= new_applied_commit_version {
//                 self.log_entries.pop_front();
//             } else {
//                 break;
//             }
//         }
//     }
// }


// // 对外接口
// impl Log2Storage {
//     pub async fn new(
//         storage: Arc<RocksDBStorage>,
//         data_path: String,
//     ) -> Log2Storage {
//         let mut applied_commit_version: u64 = 0;
//         if let Ok(Some(persist_version)) = storage.get_applied_commit_version() {
//             applied_commit_version = persist_version;
//         }

//         // // 单元测试需先注释
//         let log_configure = LogServerConfig::default();
//         let log_configure= format!("http://{}:{}", log_configure.ip, log_configure.port);
//         let log_client = LogClient::connect(log_configure).await.expect("ss: failed to connetct logserver");

//         let log2storage = Log2Storage {
//             mem_state: Arc::new(RwLock::new(MemState::new(applied_commit_version))),
//             storage: storage.clone(),
//             handles: Arc::new(Mutex::new(vec![])),
//             // // 单元测试需先注释
//             log_client: log_client,
//         };
//         log2storage
//     }


//     pub async fn start(self: &Arc<Self>) {
//         let log2storage_clone = Arc::clone(&self);

//         tracing::info!("ss: log2storage 任务启动");
//         let handle = tokio::spawn(async move {
//             let mut interval = 0;
//             let mut loop_count = 0;

//             loop {
//                 loop_count += 1;
//                 tracing::info!("-------------------------------");
//                 tracing::info!("[ss: Log2Storage::start] 第{}次loop", loop_count);
//                 wait!(interval);
//                 interval = FETCH_INTERVAL;
                
//                 // 添加详细的步骤日志
//                 tracing::info!("[ss: Log2Storage] 步骤1: 检查内存状态");
//                 if let Some(begin) = log2storage_clone.check_mem_state().await {
//                     tracing::info!("[ss: Log2Storage] 内存状态检查通过，开始版本: {}", begin);
                    
//                     tracing::info!("[ss: Log2Storage] 步骤2: 从LS拉取日志");
//                     if let Some(resp) = log2storage_clone.fetch_log_entries(begin).await {
//                         tracing::info!("[ss: Log2Storage] 成功拉取到 {} 条日志", resp.log_entries.len());
                        
//                         tracing::info!("[ss: Log2Storage] 步骤3: 追加日志到内存");
//                         if !log2storage_clone.append_log_entries(begin, resp).await { 
//                             tracing::warn!("[ss: Log2Storage] 日志追加失败，继续下一次循环");
//                             continue; 
//                         }
//                         tracing::info!("[ss: Log2Storage] 日志追加成功");
//                     } else {
//                         tracing::warn!("[ss: Log2Storage] 拉取日志失败或无数据，继续下一次循环");
//                         continue; // 无数据或拉取失败
//                     }
//                 } else {
//                     tracing::warn!("[ss: Log2Storage] 内存状态检查失败，缓存可能已满");
//                 }
                
//                 tracing::info!("[ss: Log2Storage] 步骤4: 应用日志到存储");
//                 log2storage_clone.apply_entries().await;
                
//                 tracing::info!("[ss: Log2Storage] 当前循环完成");
//             }
//         });

//         self.handles.lock().unwrap().push(handle);
//     }

//     pub async fn stop(&self) {
//         let handles = self.handles.lock().unwrap();
//         for handle in handles.iter() {
//             handle.abort();
//         }
//     }
// }

// // 内部函数
// impl Log2Storage {

//     /* ==================================== start()子函数 END ====================================*/

//     // 单元测试需先注释
//     //通过RPC获取日志信息
//     async fn fetch_entries(&mut self, prev_commit_version: u64) -> Option<FetchEntriesResponse> {
//         let resp = self.log_client
//             .fetch_entries(Request::new(FetchEntriesRequest {
//                 prev_commit_version,
//                 max_entries: FETCH_LOG_ENTRY_LIMIT,
//             }))
//             .await
//             .map_err(|e| {
//                 warn!("ss: Fetch log entries error. Desc: {}.", e);
//                 Status::new(Code::Internal, "ss: Fetch log entries error")
//             });
//         if resp.is_err() {
//             None
//         } else {
//             Some(resp.unwrap().into_inner())
//         }
//     }
    

//     /// 检查内存状态以判断是否可以继续写入日志。
//     ///
//     /// 功能说明：
//     /// - 异步获取内存状态的读锁（`mem_state`）
//     /// - 判断日志缓存是否已满：
//     ///     - 若已满，返回 `None`
//     ///     - 若未满，返回`Some(max_commit_version)`
//     ///
//     /// 返回值：
//     /// - `Some(max_commit_version)`：表示可以继续写入日志
//     /// - `None`：表示内存缓存已满，暂不可写入
//     async fn check_mem_state(&self) -> Option<u64> {
//         let mem_state = self.mem_state.read().await;
//         if mem_state.cache_full() {
//             None
//         } else {
//             Some(mem_state.max_commit_version)
//         }
//     }

//     /// 异步拉取日志条目，用于从指定的日志序列号开始同步日志。
//     ///
//     /// 功能说明：
//     /// - 记录操作开始时间以统计耗时
//     /// - 使用实际拉取方法（`fetch_entries`）
//     /// - 如果未拉取到日志（返回 `None`），打印提示（info）
//     /// - 如果拉取耗时超过 1 秒，打印警告（warn），包括耗时与返回的日志数量
//     /// - 最终返回拉取到的日志响应（`Some(response)`）或 `None`
//     ///
//     /// 参数：
//     /// - `prev_commit_version`：上一个日志序列号，从该位置之后开始拉取
//     ///
//     /// 返回值：
//     /// - `Some(FetchEntriesResponse)`：拉取成功，包含日志条目
//     /// - `None`：未获取到日志条目
//     // 修改 fetch_log_entries 方法，添加条件编译
//     #[allow(unused_variables)]
//     async fn fetch_log_entries(&self, prev_commit_version: u64) -> Option<FetchEntriesResponse> {
//         let start_time = Instant::now();

//         #[cfg(test)]
//         let (resp, _) = mock_fetch_entries().await;
        
//         #[cfg(not(test))]
//         let resp: Option<FetchEntriesResponse> = None; // 明确指定类型

//         let elapsed = start_time.elapsed();

//         if resp.is_none() {
//             tracing::info!("ss: 未拉取到日志条目，从版本 {} 开始", prev_commit_version);
//             return None;
//         }

//         let log_count = resp.as_ref().unwrap().log_entries.len();
//         if elapsed.as_secs() >= 1 {
//             tracing::warn!(
//                 "ss: 拉取日志耗时过长: {:?}, 拉取到 {} 条日志",
//                 elapsed,
//                 log_count
//             );
//         }

//         resp
//     }

//     /// 将响应中的日志条目追加到内存状态（MemState）中。
//     ///
//     /// 功能说明：
//     /// - 获取内存状态的写锁，用于修改日志缓存
//     /// - 遍历 `resp.log_entries`，检查日志是否连续：
//     ///     - 若发现某个条目的 `prev_commit_version` 与上一个日志的 `commit_version` 不一致，则视为不连续，打印警告日志并返回 `false`
//     /// - 如果日志连续：
//     ///     - 将所有日志条目追加到 `mem_state.log_entries`
//     ///     - 更新内存状态中的当前日志序列号 `commit_version`
//     ///
//     /// 参数：
//     /// - `cur_commit_version`：当前已知的最新日志序列号
//     /// - `resp`：从远端拉取到的日志响应对象，包含多个 `LogEntry`
//     ///
//     /// 返回值：
//     /// - `true`：追加成功，日志连续
//     /// - `false`：日志不连续，未追加
//     async fn append_log_entries(&self, cur_commit_version: u64, resp: FetchEntriesResponse) -> bool {
//         let mut mem_state = self.mem_state.write().await;
        
//         let mut expected_prev_version = cur_commit_version;
        
//         for entry in &resp.log_entries {
//             if entry.prev_commit_version != expected_prev_version {
//                 tracing::warn!(
//                     "ss: 日志不连续，期望 prev_commit_version: {}, 实际: {}",
//                     expected_prev_version,
//                     entry.prev_commit_version
//                 );
//                 return false;
//             }
//             expected_prev_version = entry.commit_version;
//         }
        
//         // 追加日志条目
//         for entry in resp.log_entries {
//             mem_state.log_entries.push_back(entry);
//         }
        
//         // 更新最大提交版本
//         if let Some(last_entry) = mem_state.log_entries.back() {
//             mem_state.max_commit_version = last_entry.commit_version;
//         }
        
//         true
//     }
    

//     /// 从内存状态中获取日志条目，并将其应用（写入）到底层 RocksDB 存储中。
//     ///
//     /// 功能说明：
//     /// - 从 `mem_state` 中提取当前所有日志条目
//     /// - 如果日志为空，输出info并提前返回
//     /// - 获取最新的日志序列号 `new_applied_commit_version`，用于记录写入版本
//     /// - 检查并在必要时创建对应的列族（Column Families）
//     ///     - 若检查失败则输出warn并返回
//     /// - 按照日志中的图 ID 对日志进行整理（分组）
//     /// - 异步执行批量写入任务：
//     ///     - 将每个图的数据写入 RocksDB
//     ///     - 写入完成后输出操作耗时与统计信息
//     ///     - 更新持久化的最新日志序列号（applied_commit_version）
//     /// - 将异步写入任务的句柄保存到 `handles` 中，以便后续统一管理或关闭
//     ///
//     /// 注意事项：
//     /// - 默认假设日志是正确的（即日志已经过排序与验证）
//     /// - 写入操作采用异步任务进行，不会阻塞主流程
//     async fn apply_entries(&self) {
        
//         let (new_applied_commit_version, entries) = self.get_log_entries().await;
        
//         tracing::info!("ss: 准备应用日志，新版本: {}, 待应用条目数: {}", 
//             new_applied_commit_version, entries.len());

//         if entries.is_empty() {
//             tracing::info!("ss: 没有需要应用的日志条目");
//             return;
//         }
//         // 打印第一条和最后一条日志的详细信息
//         if let Some(first_entry) = entries.first() {
//             tracing::info!("ss: 第一条日志 - 版本: {}, graph_id: {}", 
//                         first_entry.commit_version, first_entry.graph_id);
//         }
//         if let Some(last_entry) = entries.last() {
//             tracing::info!("ss: 最后一条日志 - 版本: {}, graph_id: {}", 
//                         last_entry.commit_version, last_entry.graph_id);
//         }

//         tracing::info!("ss: 开始应用 {} 条日志条目，新版本: {}", 
//                     entries.len(), new_applied_commit_version);
        
//         // 检查并创建列族
//         if !self.check_and_create_column_families(&entries) {
//             tracing::warn!("ss: 列族检查失败，跳过本次应用");
//             return;
//         }
        
//         // 组织日志条目
//         let graph_data = self.organize_entries(entries);
        
//         if graph_data.is_empty() {
//             tracing::warn!("ss: 组织后的日志数据为空，跳过写入");
//             return;
//         }
        
//         // 批量写入
//         let storage_clone = self.storage.clone();
//         let start_time = Instant::now();
        
//         Self::batch_write_graphs(storage_clone, graph_data).await;
        
//         let elapsed = start_time.elapsed();
//         tracing::info!("ss: 批量写入完成，耗时: {:?}", elapsed);
        
//         // 更新已应用的提交版本
//         if let Err(e) = self.storage.write_applied_commit_version(new_applied_commit_version) {
//             tracing::warn!("ss: 更新已应用提交版本失败: {:?}", e);
//         } else {
//             // 更新内存状态
//             let mut mem_state = self.mem_state.write().await;
//             mem_state.update_applied_commit_version(new_applied_commit_version);
//             tracing::info!("ss: 已更新应用提交版本为: {}", new_applied_commit_version);
//         }
//     }

//     /* ==================================== start()子函数 END ====================================*/

//     /* ========================== consumer_apply_entries()子函数  START ==========================*/

//     // 从MemState中取出日志（即读取并删除MemState中LogEntry），写锁保护
//     async fn get_log_entries(&self) -> (u64, Vec<LogEntry>) {
//         let mut mem_state = self.mem_state.write().await;
        
//         if mem_state.log_entries.is_empty() {
//             return (mem_state.applied_commit_version, vec![]);
//         }
        
//         let new_applied_commit_version = mem_state.log_entries.back().unwrap().commit_version;
//         let entries: Vec<LogEntry> = mem_state.log_entries.drain(..).collect();
        
//         (new_applied_commit_version, entries)
//     }


//     // 预先创建不存在的列族
//     fn check_and_create_column_families(&self, entries: &[LogEntry]) -> bool {
//         // 使用迭代器一次性完成 graph_id 的提取和字符串转换
//         let graph_ids: HashSet<_> = entries.iter().map(|entry| entry.graph_id.to_string())
//             .collect();
    
//         // 使用提前转换好的字符串进行后续操作
//         for graph_id_str in graph_ids {
//             if !self.storage.is_cf_exist(&graph_id_str) {
//                 if let Err(e) = self.storage.create_cf(&graph_id_str) {
//                     // 改用更合适的 Display trait 输出字符串
//                     tracing::warn!("ss: Failed to precreate graph {}: {:?}", graph_id_str, e);
//                     return false;
//                 }
//             }
//         }
    
//         // 使用 Rust 惯用的表达式返回
//         true
//     }

//     // 组织entries为graph data
//     // 返回值: HashMap<graph_id, HashMap<version, Vec<(key, Option<value>)>>>
//     fn organize_entries(&self, entries: Vec<LogEntry>) -> HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>> {
//         // 初始化 graph_data：先按 graph_id 聚合 -> 再按 version 聚合 -> 对应的 (key, value) 列表
//         let mut graph_data: HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>> = HashMap::new();

//         let mut in_batch_merge_acc: HashMap<(u32, Vec<u8>), i64> = HashMap::new();

//         for entry in entries {
//             let version = entry.commit_version;
//             let graph_id = entry.graph_id;
//             if let Some(write_set) = entry.write_set {
//                 // 处理 upsert
//                 for kv in write_set.upsert_kvs {
//                     graph_data.entry(graph_id).or_default().entry(version).or_default().push((kv.key, Some(kv.value)));
//                 }

//                 // 处理 delete
//                 for key in write_set.deleted_keys.into_iter() {
//                     graph_data.entry(graph_id).or_default().entry(version).or_default().push((key, None));
//                 }
//             }
//         }

//         graph_data
//     }

//     // 并发批量写入，每个graph id创建单独的异步任务
//     async fn batch_write_graphs(
//         storage: Arc<RocksDBStorage>,
//         graph_data: HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>>,
//     ) {
//         let mut write_handles = vec![];

//         for (graph_id, datasets) in graph_data {
//             let storage_ref = storage.clone();
//             let write_handle = tokio::spawn(async move {
//                 tracing::info!("ss: 对graph {} 进行batch write……", graph_id);
//                 for (applied_commit_version, kvs) in datasets {
//                     if let Err(e) = storage_ref.batch_write(graph_id, kvs, applied_commit_version) {
//                         tracing::warn!("ss: Failed to apply log entries for graph {}: {:?}", graph_id, e);
//                     }
//                 }
//                 tracing::info!("ss: graph {} batch write完成", graph_id);
//             });
//             write_handles.push(write_handle);
//         }

//         for future in write_handles {
//             let _ = future.await;
//         }
//     }
    
//     /* ========================== consumer_apply_entries()子函数  END ==========================*/
// }

// #[cfg(test)]
// async fn mock_fetch_entries() -> (
//     Option<FetchEntriesResponse>,
//     HashMap<(u32, Vec<u8>, u64), (Option<Vec<u8>>, String)>
// ) {
//     let resp = Some(FetchEntriesResponse {
//         log_entries: vec![
//             LogEntry {
//                 prev_commit_version: 0,
//                 commit_version: 1,
//                 graph_id: 42,
//                 write_set: Some(WriteSet {
//                     upsert_kvs: vec![
//                         Kv { key: b"key1".to_vec(), value: b"value1".to_vec() },
//                         Kv { key: b"key3".to_vec(), value: b"value3".to_vec() },
//                     ],
//                     deleted_keys: vec![],
//                 }),
//             },
//             LogEntry {
//                 prev_commit_version: 1,
//                 commit_version: 2,
//                 graph_id: 42,
//                 write_set: Some(WriteSet {
//                     upsert_kvs: vec![
//                         Kv { key: b"key2".to_vec(), value: 7i64.to_be_bytes().to_vec() },
//                     ],
//                     deleted_keys: vec![b"key1".to_vec()],
//                 }),
//             },
//             LogEntry {
//                 prev_commit_version: 2,
//                 commit_version: 3,
//                 graph_id: 43,
//                 write_set: Some(WriteSet {
//                     upsert_kvs: vec![
//                         Kv { key: b"keyA".to_vec(), value: b"valueA".to_vec() },
//                     ],
//                     deleted_keys: vec![b"key2".to_vec()],
//                 }),
//             },
//         ],
//     });

//     let mut expected_values = HashMap::new();
//     expected_values.insert((42, b"key1".to_vec(), 3), (None, "key1 不应存在".to_string()));
//     expected_values.insert((42, b"key2".to_vec(), 3), (Some(7i64.to_be_bytes().to_vec()), "key2 应为整数7".to_string()));
//     expected_values.insert((42, b"key3".to_vec(), 3), (Some(b"value3".to_vec()), "key3 应为'value3'".to_string()));
//     expected_values.insert((42, b"key3".to_vec(), 4), (Some(b"value3".to_vec()), "更大version的key3也应为'value3'".to_string()));
//     expected_values.insert((42, b"key3".to_vec(), 2), (Some(b"value3".to_vec()), "版本2的key3应为'value3'".to_string()));

//     (resp, expected_values)
// }


// #[cfg(test)]
// mod tests {

//     use std::sync::Arc;
//     use tempfile::TempDir;
//     use tokio::time::{sleep, Duration};
//     use crate::storage::RocksDBStorage;
//     use crate::log2storage::Log2Storage;
//     // types for mock
//     use rpc::log::{FetchEntriesResponse, LogEntry};
//     use rpc::common::{WriteSet, Kv};
//     use std::collections::HashMap;
//     use super::mock_fetch_entries; // 添加这行导入
//     // 开启info日志
//     fn init_tracing() {
//         let _ = tracing_subscriber::fmt()
//             .with_env_filter("info")
//             .try_init();
//     }    
//     /*
//         Mock测试log2stroage的基本功能，包含创建、删除、拉取、应用
//         其中日志获取mock模拟拉取，实际需要通过RPC拉取
//     */
//     #[tokio::test]
//     async fn mock_test_basic() {
        
//         init_tracing();
//         let temp_dir = TempDir::new().expect("Failed to create temp dir");
//         let storage = Arc::new(RocksDBStorage::new(temp_dir));
//         let log2storage = Arc::new(Log2Storage::new(storage.clone(), "not_used".to_string()).await);
        
//         // 检查MemState是否处于初始状态
//         {
//             let mem_state = log2storage.mem_state.read().await;
//             assert_eq!(mem_state.log_entries.len(), 0, "Log entries should be empty at first");
//             assert_eq!(mem_state.max_commit_version, 0, "LSN should be zero at first");
//         }

//         // 启动异步任务
//         log2storage.start().await;
        
//         // 等待一段时间后，提前停止log2storage
//         sleep(Duration::from_millis(3000)).await;
//         log2storage.stop().await;

//         // 检查 mem_state，确保日志已被应用
//         {
//             let mem_state = log2storage.mem_state.read().await;
//             assert!(mem_state.log_entries.is_empty(), "Log entries should be consumed after apply");
//             assert_eq!(mem_state.applied_commit_version, 3, "applied_commit_version should be updated after applying entries");
//         }
//         // 检查持久化元数据，确保已写入磁盘
//         {
//             let applied_commit_version :u64 = storage.get_applied_commit_version().unwrap().unwrap();
//             assert_eq!(applied_commit_version, 3, "applied_commit_version should be written into disk");
//         }

//         // 从storage中读取并验证内容无误
//         let expected_values = mock_fetch_entries().await.1;
//         for ((graph_id, key, version), (expected_value, message)) in expected_values {
//             let actual_value = storage.get(graph_id, key.clone(), version.into()).unwrap(); 
//             assert_eq!(actual_value, expected_value, "{message}")
//         }
//     }

//     // 以硬编码方式替代实际的RPC获取，并附带预期结果
//     // async fn mock_fetch_entries() -> (
//     //     Option<FetchEntriesResponse> , HashMap<(u32, Vec<u8>, u64), (Option<Vec<u8>>, String)>
//     // ){
//     //     let resp = Some(FetchEntriesResponse {
//     //         log_entries: vec![
//     //             LogEntry {
//     //                 prev_commit_version: 0,
//     //                 commit_version: 1,
//     //                 graph_id: 42,
//     //                 write_set: Some(WriteSet {
//     //                     upsert_kvs: vec![
//     //                         Kv { key: b"key1".to_vec(), value: b"value1".to_vec() },
//     //                         Kv { key: b"key3".to_vec(), value: b"value3".to_vec() },
//     //                     ],
//     //                     deleted_keys: vec![],
//     //                 }),
//     //             },
//     //             LogEntry {
//     //                 prev_commit_version: 1,
//     //                 commit_version: 2,
//     //                 graph_id: 42,
//     //                 write_set: Some(WriteSet {
//     //                     upsert_kvs: vec![
//     //                         Kv { key: vec![], value: vec![] },
//     //                     ],
//     //                     deleted_keys: vec![b"key1".to_vec()],
//     //                 }),
//     //             },
//     //             LogEntry {
//     //                 prev_commit_version: 2,
//     //                 commit_version: 3,
//     //                 graph_id: 43,
//     //                 write_set: Some(WriteSet {
//     //                     upsert_kvs: vec![
//     //                         Kv { key: b"keyA".to_vec(), value: b"valueA".to_vec() },
//     //                     ],
//     //                     deleted_keys: vec![b"key2".to_vec()],
//     //                 }),
//     //             },
//     //         ],
//     //     });

//     // let mut expected_values = HashMap::new();
//     // expected_values.insert((42, b"key1".to_vec(), 3), (None, "key1 不应存在".to_string()));
//     // expected_values.insert((42, b"key2".to_vec(), 3), (Some(7i64.to_be_bytes().to_vec()), "key2 应为整数7".to_string()));
//     // expected_values.insert((42, b"key3".to_vec(), 3), (Some(b"value3".to_vec()), "key3 应为'value3'".to_string()));
//     // expected_values.insert((42, b"key3".to_vec(), 4), (Some(b"value3".to_vec()), "更大version的key3也应为'value3'".to_string()));
//     // expected_values.insert((42, b"key3".to_vec(), 2), (None, "更小version的key3不应存在".to_string()));

//     //     (resp, expected_values)
//     // }
// }
// #[allow(unused_imports)]
// use rpc::common::WriteSet;
// use rpc::log::{LogEntry, FetchEntriesRequest, FetchEntriesResponse};
// #[allow(unused_imports)]
// use rpc::log::log_client::LogClient;
// #[allow(unused_imports)]
// use rpc::storage::storage_server::Storage;
// #[allow(unused_imports)]
// use tonic::transport::Channel;
// #[allow(unused_imports)]
// use tonic::{Code, Request, Status};
// use std::{
//     net::IpAddr,
//     net::Ipv4Addr
// };

// use crate::storage::RocksDBStorage;

// use core::time::Duration;
// use std::collections::{HashMap, HashSet, VecDeque};
// #[allow(unused_imports)]
// use std::{
//     sync::{Arc, Mutex}
// };
// use tokio::{sync::RwLock, task::JoinHandle, time::Instant};
// use tracing::{info, warn};
// #[cfg(test)]
// use rpc::common::Kv;

// #[derive(Clone, Debug, serde::Deserialize)]
// struct LogServerConfig {
//     #[allow(dead_code)]
//     #[serde(default = "default_ip")]
//     pub ip: IpAddr,
//     #[allow(dead_code)]
//     #[serde(default = "default_port")]
//     pub port: u16,
// }

// fn default_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }
// fn default_port() -> u16 { 25002 }

// impl Default for LogServerConfig {
//     fn default() -> Self {
//         Self {
//             ip: default_ip(),
//             port: default_port(),
//         }
//     }
// }

// pub const FETCH_LOG_ENTRY_LIMIT: u32 = 500;
// // 拉取间隔
// const FETCH_INTERVAL: u64 = 1000;

// macro_rules! wait {
//     ($interval:expr) => {{
//         if $interval > 0 {
//             tokio::time::sleep(Duration::from_millis($interval)).await;
//         }
//     }};
// }

// #[derive(Clone)]
// pub struct Log2Storage {
//     /// 当前已拉取的最大版本号
//     last_fetched_version: Arc<RwLock<u64>>,
//     pub storage: Arc<RocksDBStorage>,
//     handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
//     log_client: Arc<tokio::sync::Mutex<LogClient<Channel>>>,
// }

// // 对外接口
// impl Log2Storage {
//     pub async fn new(
//         storage: Arc<RocksDBStorage>,
//         data_path: String,
//     ) -> Log2Storage {
//         let mut last_fetched_version: u64 = 0;
        
//         // 从存储中恢复上次拉取的版本（如果有）
//         if let Ok(Some(persist_version)) = storage.get_applied_commit_version() {
//             last_fetched_version = persist_version;
//         }

//         // 连接LS服务
//         let log_configure = LogServerConfig::default();
//         let log_configure= format!("http://{}:{}", log_configure.ip, log_configure.port);
//         let log_client = LogClient::connect(log_configure).await.expect("ss: failed to connetct logserver");

//         let log2storage = Log2Storage {
//             last_fetched_version: Arc::new(RwLock::new(last_fetched_version)),
//             storage: storage.clone(),
//             handles: Arc::new(Mutex::new(vec![])),
//             log_client: Arc::new(tokio::sync::Mutex::new(log_client)),
//         };
        
//         info!("ss: Log2Storage initialized with last_fetched_version: {}", last_fetched_version);
//         log2storage
//     }

//     pub async fn start(self: &Arc<Self>) {
//         let log2storage_clone = Arc::clone(&self);

//         tracing::info!("ss: log2storage 任务启动 - 模式：仅拉取和打印日志");
        
//         let handle = tokio::spawn(async move {
//             let mut interval = 0;
//             let mut loop_count = 0;

//             loop {
//                 loop_count += 1;
//                 tracing::info!("-------------------------------");
//                 tracing::info!("[ss: Log2Storage::start] 第{}次循环", loop_count);
                
//                 wait!(interval);
//                 interval = FETCH_INTERVAL;
                
//                 // 获取当前最后拉取的版本
//                 let last_version = {
//                     let version_guard = log2storage_clone.last_fetched_version.read().await;
//                     *version_guard
//                 };
                
//                 tracing::info!("[ss: Log2Storage] 从版本 {} 开始拉取日志", last_version);
                
//                 // 从LS拉取日志
//                 if let Some(resp) = log2storage_clone.fetch_log_entries(last_version).await {
//                     let entries_count = resp.log_entries.len();
//                     tracing::info!("[ss: Log2Storage] 成功拉取到 {} 条日志", entries_count);
                    
//                     if entries_count > 0 {
//                         // 打印日志条目
//                         log2storage_clone.print_log_entries(&resp).await;
                        
//                         // 更新最后拉取的版本
//                         if let Some(last_entry) = resp.log_entries.last() {
//                             let mut version_guard = log2storage_clone.last_fetched_version.write().await;
//                             *version_guard = last_entry.commit_version;
//                             tracing::info!("[ss: Log2Storage] 更新最后拉取版本为: {}", last_entry.commit_version);
                            
//                             // 可选：持久化当前版本（如果需要重启后继续）
//                             if let Err(e) = log2storage_clone.storage.write_applied_commit_version(last_entry.commit_version) {
//                                 tracing::warn!("[ss: Log2Storage] 持久化版本失败: {:?}", e);
//                             }
//                         }
//                     } else {
//                         tracing::info!("[ss: Log2Storage] 没有新的日志条目");
//                     }
//                 } else {
//                     tracing::warn!("[ss: Log2Storage] 拉取日志失败");
//                 }
                
//                 tracing::info!("[ss: Log2Storage] 当前循环完成");
//             }
//         });

//         self.handles.lock().unwrap().push(handle);
//     }

//     pub async fn stop(&self) {
//         let handles = self.handles.lock().unwrap();
//         for handle in handles.iter() {
//             handle.abort();
//         }
//     }
// }

// // 内部函数
// impl Log2Storage {
//     /// 通过RPC获取日志信息
//     async fn fetch_log_entries(&self, prev_commit_version: u64) -> Option<FetchEntriesResponse> {
//         // 获取 LogClient 的可变引用
//         let mut client_guard = self.log_client.lock().await;
        
//         match client_guard
//             .fetch_entries(Request::new(FetchEntriesRequest {
//                 prev_commit_version,
//                 max_entries: FETCH_LOG_ENTRY_LIMIT,
//             }))
//             .await
//         {
//             Ok(response) => {
//                 Some(response.into_inner())
//             }
//             Err(e) => {
//                 warn!("ss: Fetch log entries error. Desc: {}.", e);
//                 None
//             }
//         }
//     }

//     /// 打印日志条目
//     async fn print_log_entries(&self, response: &FetchEntriesResponse) {
//         info!("=== 开始打印日志条目 ===");
        
//         for (index, entry) in response.log_entries.iter().enumerate() {
//             info!("条目 {}:", index + 1);
//             info!("  前一个提交版本: {}", entry.prev_commit_version);
//             info!("  提交版本: {}", entry.commit_version);
//             info!("  图ID: {}", entry.graph_id);
            
//             if let Some(write_set) = &entry.write_set {
//                 info!("  写入集:");
                
//                 // 打印插入/更新的键值对
//                 if !write_set.upsert_kvs.is_empty() {
//                     info!("    插入/更新操作 ({} 个):", write_set.upsert_kvs.len());
//                     for kv in &write_set.upsert_kvs {
//                         let key_str = String::from_utf8_lossy(&kv.key);
//                         let value_str = String::from_utf8_lossy(&kv.value);
//                         info!("      Key: {}, Value: {}", key_str, value_str);
//                     }
//                 }
                
//                 // 打印删除的键
//                 if !write_set.deleted_keys.is_empty() {
//                     info!("    删除操作 ({} 个):", write_set.deleted_keys.len());
//                     for key in &write_set.deleted_keys {
//                         let key_str = String::from_utf8_lossy(key);
//                         info!("      Key: {}", key_str);
//                     }
//                 }
                
//                 if write_set.upsert_kvs.is_empty() && write_set.deleted_keys.is_empty() {
//                     info!("    空写入集");
//                 }
//             } else {
//                 info!("  无写入集");
//             }
            
//             info!("  ---");
//         }
        
//         info!("=== 日志条目打印完成，共 {} 条 ===", response.log_entries.len());
//     }
// }
// log2storage.rs
#[allow(unused_imports)]
use rpc::common::WriteSet;
use rpc::log::{LogEntry, FetchEntriesRequest, FetchEntriesResponse};
#[allow(unused_imports)]
use rpc::log::log_client::LogClient;
#[allow(unused_imports)]
use rpc::storage::storage_server::Storage;
#[allow(unused_imports)]
use tonic::transport::Channel;
#[allow(unused_imports)]
use tonic::{Code, Request, Status};
use std::{
    net::IpAddr,
    net::Ipv4Addr
};

use crate::storage::RocksDBStorage;

use core::time::Duration;
use std::collections::{HashMap, HashSet, VecDeque};
#[allow(unused_imports)]
use std::{
    sync::{Arc, Mutex}
};
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};
use tracing::{info, warn};
#[cfg(test)]
use rpc::common::Kv;

#[derive(Clone, Debug, serde::Deserialize)]
struct LogServerConfig {
    #[allow(dead_code)]
    #[serde(default = "default_ip")]
    pub ip: IpAddr,
    #[allow(dead_code)]
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }
fn default_port() -> u16 { 25002 }

impl Default for LogServerConfig {
    fn default() -> Self {
        Self {
            ip: default_ip(),
            port: default_port(),
        }
    }
}

pub const FETCH_LOG_ENTRY_LIMIT: u32 = 500;
// 拉取间隔
const FETCH_INTERVAL: u64 = 1000;

// 内存缓存的最大条目数
#[cfg(debug_assertions)]
const MAX_CACHED_ENTRIES: usize = 500;
#[cfg(not(debug_assertions))]
const MAX_CACHED_ENTRIES: usize = 50;

macro_rules! wait {
    ($interval:expr) => {{
        if $interval > 0 {
            tokio::time::sleep(Duration::from_millis($interval)).await;
        }
    }};
}

#[derive(Clone)]
pub struct Log2Storage {
    mem_state: Arc<RwLock<MemState>>,
    pub storage: Arc<RocksDBStorage>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    log_client: Arc<tokio::sync::Mutex<LogClient<Channel>>>,
}

// 内存状态，用于缓存日志条目
struct MemState {
    /// 当前已应用（已落盘）的最大版本号
    applied_commit_version: u64,
    /// 当前缓存中最后一个日志的版本号
    max_commit_version: u64,
    /// 缓存的日志条目队列
    log_entries: VecDeque<LogEntry>,
}

impl MemState {
    fn new(persist_version: u64) -> Self {
        MemState {
            applied_commit_version: persist_version,
            max_commit_version: persist_version,
            log_entries: VecDeque::with_capacity(MAX_CACHED_ENTRIES),
        }
    }

    /// 检查缓存是否已满
    fn cache_full(&self) -> bool {
        self.log_entries.len() >= MAX_CACHED_ENTRIES
    }

    /// 更新已应用的提交版本，并移除已应用的日志条目
    fn update_applied_commit_version(&mut self, new_applied_commit_version: u64) {
        self.applied_commit_version = new_applied_commit_version;
        
        // 移除所有已应用的日志条目
        while let Some(front) = self.log_entries.front() {
            if front.commit_version <= new_applied_commit_version {
                self.log_entries.pop_front();
            } else {
                break;
            }
        }
    }
}

// 对外接口
impl Log2Storage {
    pub async fn new(
        storage: Arc<RocksDBStorage>,
        data_path: String,
    ) -> Log2Storage {
        let mut applied_commit_version: u64 = 0;
        
        // 从存储中恢复已应用的版本
        if let Ok(Some(persist_version)) = storage.get_applied_commit_version() {
            applied_commit_version = persist_version;
        }

        // 连接LS服务
        let log_configure = LogServerConfig::default();
        let log_configure = format!("http://{}:{}", log_configure.ip, log_configure.port);
        let log_client = LogClient::connect(log_configure).await.expect("ss: failed to connect logserver");

        let log2storage = Log2Storage {
            mem_state: Arc::new(RwLock::new(MemState::new(applied_commit_version))),
            storage: storage.clone(),
            handles: Arc::new(Mutex::new(vec![])),
            log_client: Arc::new(tokio::sync::Mutex::new(log_client)),
        };
        
        info!("ss: Log2Storage initialized with applied_commit_version: {}", applied_commit_version);
        log2storage
    }

    pub async fn start(self: &Arc<Self>) {
        let log2storage_clone = Arc::clone(&self);

        info!("ss: log2storage 任务启动 - 模式：拉取日志并应用到数据库");
        
        let handle = tokio::spawn(async move {
            let mut interval = 0;
            let mut loop_count = 0;

            loop {
                loop_count += 1;
                info!("-------------------------------");
                info!("[ss: Log2Storage::start] 第{}次循环", loop_count);
                
                wait!(interval);
                interval = FETCH_INTERVAL;
                
                // 步骤1: 检查内存状态，获取拉取起始版本
                let fetch_start_version = match log2storage_clone.check_mem_state().await {
                    Some(version) => version,
                    None => {
                        warn!("[ss: Log2Storage] 内存缓存已满，无法拉取新日志，等待应用");
                        // 如果缓存已满，先应用已有的日志
                        log2storage_clone.apply_entries().await;
                        continue;
                    }
                };
                
                info!("[ss: Log2Storage] 从版本 {} 开始拉取日志", fetch_start_version);
                
                // 步骤2: 从LS拉取日志
                if let Some(resp) = log2storage_clone.fetch_log_entries(fetch_start_version+1).await {
                    let entries_count = resp.log_entries.len();
                    info!("[ss: Log2Storage] 成功拉取到 {} 条日志", entries_count);
                    info!("[ss: Log2Storage] fetch_start_version:{}", fetch_start_version);
                    
                    if entries_count > 0 {
                        // 打印日志条目
                        log2storage_clone.print_log_entries(&resp).await;
                        
                        // 步骤3: 追加日志到内存缓存
                        if !log2storage_clone.append_log_entries(fetch_start_version, resp).await {
                            warn!("[ss: Log2Storage] 日志追加失败，可能日志不连续");
                            continue;
                        }
                        
                        info!("[ss: Log2Storage] 日志成功追加到内存缓存");
                    } else {
                        info!("[ss: Log2Storage] 没有新的日志条目");
                    }
                } else {
                    warn!("[ss: Log2Storage] 拉取日志失败");
                }
                
                // 步骤4: 应用日志到存储
                log2storage_clone.apply_entries().await;
                
                info!("[ss: Log2Storage] 当前循环完成");
            }
        });

        self.handles.lock().unwrap().push(handle);
    }

    pub async fn stop(&self) {
        let handles = self.handles.lock().unwrap();
        for handle in handles.iter() {
            handle.abort();
        }
    }
}

// 内部函数
impl Log2Storage {
    /// 检查内存状态，返回可以拉取的起始版本号
    async fn check_mem_state(&self) -> Option<u64> {
        let mem_state = self.mem_state.read().await;
        if mem_state.cache_full() {
            None
        } else {
            Some(mem_state.max_commit_version)
        }
    }

    /// 通过RPC获取日志信息
    async fn fetch_log_entries(&self, prev_commit_version: u64) -> Option<FetchEntriesResponse> {
        // 获取 LogClient 的可变引用
        let mut client_guard = self.log_client.lock().await;
        
        match client_guard
            .fetch_entries(Request::new(FetchEntriesRequest {
                prev_commit_version,
                max_entries: FETCH_LOG_ENTRY_LIMIT,
            }))
            .await
        {
            Ok(response) => {
                Some(response.into_inner())
            }
            Err(e) => {
                warn!("ss: Fetch log entries error. Desc: {}.", e);
                None
            }
        }
    }

    /// 打印日志条目
    async fn print_log_entries(&self, response: &FetchEntriesResponse) {
        info!("=== 开始打印日志条目 ===");
        
        for (index, entry) in response.log_entries.iter().enumerate() {
            info!("条目 {}:", index + 1);
            info!("  前一个提交版本: {}", entry.prev_commit_version);
            info!("  提交版本: {}", entry.commit_version);
            info!("  图ID: {}", entry.graph_id);
            
            if let Some(write_set) = &entry.write_set {
                info!("  写入集:");
                
                // 打印插入/更新的键值对
                if !write_set.upsert_kvs.is_empty() {
                    info!("    插入/更新操作 ({} 个):", write_set.upsert_kvs.len());
                    for kv in &write_set.upsert_kvs {
                        let key_str = String::from_utf8_lossy(&kv.key);
                        let value_str = String::from_utf8_lossy(&kv.value);
                        info!("      Key: {}, Value: {}", key_str, value_str);
                    }
                }
                
                // 打印删除的键
                if !write_set.deleted_keys.is_empty() {
                    info!("    删除操作 ({} 个):", write_set.deleted_keys.len());
                    for key in &write_set.deleted_keys {
                        let key_str = String::from_utf8_lossy(key);
                        info!("      Key: {}", key_str);
                    }
                }
                
                if write_set.upsert_kvs.is_empty() && write_set.deleted_keys.is_empty() {
                    info!("    空写入集");
                }
            } else {
                info!("  无写入集");
            }
            
            info!("  ---");
        }
        
        info!("=== 日志条目打印完成，共 {} 条 ===", response.log_entries.len());
    }

    /// 将拉取的日志条目追加到内存缓存
    async fn append_log_entries(&self, cur_commit_version: u64, resp: FetchEntriesResponse) -> bool {
        let mut mem_state = self.mem_state.write().await;
        
        let mut expected_prev_version = cur_commit_version;
        
        // 检查日志连续性
        for entry in &resp.log_entries {
            if entry.prev_commit_version != expected_prev_version {
                warn!(
                    "ss: 日志不连续，期望 prev_commit_version: {}, 实际: {}",
                    expected_prev_version,
                    entry.prev_commit_version
                );
                return false;
            }
            expected_prev_version = entry.commit_version;
        }
        
        // 追加日志条目到缓存
        for entry in resp.log_entries {
            mem_state.log_entries.push_back(entry);
        }
        
        // 更新最大提交版本
        if let Some(last_entry) = mem_state.log_entries.back() {
            let last_version = last_entry.commit_version; // 先获取值，而不是引用
            mem_state.max_commit_version = last_version;   // 然后赋值
            info!("[ss: Log2Storage] 更新最大提交版本为: {}", last_version);
        }
        
        true
    }

    /// 从内存缓存中获取日志条目并应用到存储
    async fn apply_entries(&self) {
        // 获取待应用的日志条目
        let (new_applied_commit_version, entries) = self.get_log_entries().await;
        
        if entries.is_empty() {
            info!("[ss: Log2Storage] 没有需要应用的日志条目");
            return;
        }
        
        info!("[ss: Log2Storage] 准备应用 {} 条日志，新版本: {}", 
              entries.len(), new_applied_commit_version);
        
        // 检查并创建列族
        if !self.check_and_create_column_families(&entries) {
            warn!("[ss: Log2Storage] 列族检查失败，跳过本次应用");
            return;
        }
        
        // 组织日志条目按图ID分组
        let graph_data = self.organize_entries(entries);
        
        if graph_data.is_empty() {
            warn!("[ss: Log2Storage] 组织后的日志数据为空，跳过写入");
            return;
        }
        
        // 批量写入存储
        let storage_clone = self.storage.clone();
        let start_time = Instant::now();
        
        self.batch_write_graphs(storage_clone, graph_data).await;
        
        let elapsed = start_time.elapsed();
        info!("[ss: Log2Storage] 批量写入完成，耗时: {:?}", elapsed);
        
        // 更新已应用的提交版本
        if let Err(e) = self.storage.write_applied_commit_version(new_applied_commit_version) {
            warn!("[ss: Log2Storage] 更新已应用提交版本失败: {:?}", e);
        } else {
            // 更新内存状态，移除已应用的日志
            let mut mem_state = self.mem_state.write().await;
            mem_state.update_applied_commit_version(new_applied_commit_version);
            info!("[ss: Log2Storage] 已更新应用提交版本为: {}", new_applied_commit_version);
        }
    }

    /// 从内存状态中取出待应用的日志条目
    async fn get_log_entries(&self) -> (u64, Vec<LogEntry>) {
        let mut mem_state = self.mem_state.write().await;
        
        if mem_state.log_entries.is_empty() {
            return (mem_state.applied_commit_version, vec![]);
        }
        
        // 先获取版本值，然后释放对 log_entries 的引用
        let new_applied_commit_version = {
            if let Some(last_entry) = mem_state.log_entries.back() {
                last_entry.commit_version
            } else {
                mem_state.applied_commit_version
            }
        };
        
        // 取出所有待应用的条目
        let entries: Vec<LogEntry> = mem_state.log_entries.drain(..).collect();
        
        (new_applied_commit_version, entries)
    }

    /// 检查并创建不存在的列族
    fn check_and_create_column_families(&self, entries: &[LogEntry]) -> bool {
        // 使用迭代器一次性完成 graph_id 的提取和字符串转换
        let graph_ids: HashSet<_> = entries.iter().map(|entry| entry.graph_id.to_string())
            .collect();
    
        // 使用提前转换好的字符串进行后续操作
        for graph_id_str in graph_ids {
            if !self.storage.is_cf_exist(&graph_id_str) {
                if let Err(e) = self.storage.create_cf(&graph_id_str) {
                    // 改用更合适的 Display trait 输出字符串
                    tracing::warn!("ss: Failed to precreate graph {}: {:?}HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH", graph_id_str, e);
                    return false;
                }
                else{tracing::warn!("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")};
            }
        }
    
        // 使用 Rust 惯用的表达式返回
        true
    }

    /// 组织日志条目为按图ID和版本分组的格式
    fn organize_entries(&self, entries: Vec<LogEntry>) -> HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>> {
        let mut graph_data: HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>> = HashMap::new();

        for entry in entries {
            let version = entry.commit_version;
            let graph_id = entry.graph_id;
            
            if let Some(write_set) = entry.write_set {
                // 处理 upsert 操作
                for kv in write_set.upsert_kvs {
                    graph_data
                        .entry(graph_id)
                        .or_default()
                        .entry(version)
                        .or_default()
                        .push((kv.key, Some(kv.value)));
                }

                // 处理 delete 操作
                for key in write_set.deleted_keys {
                    graph_data
                        .entry(graph_id)
                        .or_default()
                        .entry(version)
                        .or_default()
                        .push((key, None));
                }
            }
        }

        graph_data
    }

    /// 并发批量写入，每个图ID创建单独的异步任务
    async fn batch_write_graphs(
        &self,
        storage: Arc<RocksDBStorage>,
        graph_data: HashMap<u32, HashMap<u64, Vec<(Vec<u8>, Option<Vec<u8>>)>>>,
    ) {
        let mut write_handles = vec![];

        for (graph_id, datasets) in graph_data {
            let storage_ref = storage.clone();
            let write_handle = tokio::spawn(async move {
                info!("[ss: Log2Storage] 对图 {} 进行批量写入...", graph_id);
                
                for (applied_commit_version, kvs) in datasets {
                    if let Err(e) = storage_ref.batch_write(graph_id, kvs, applied_commit_version) {
                        warn!("[ss: Log2Storage] 应用日志条目到图 {} 失败: {:?}", graph_id, e);
                    }
                }
                
                info!("[ss: Log2Storage] 图 {} 批量写入完成", graph_id);
            });
            write_handles.push(write_handle);
        }

        // 等待所有写入任务完成
        for future in write_handles {
            let _ = future.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};
    use rpc::common::{WriteSet, Kv};

    // 开启info日志
    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();
    }

    // Mock测试 - 测试日志拉取、缓存和应用
    #[tokio::test]
    async fn mock_test_log_application() {
        init_tracing();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = Arc::new(RocksDBStorage::new(temp_dir.path()));
        let log2storage = Arc::new(Log2Storage::new(storage.clone(), "not_used".to_string()).await);
        
        // 检查初始状态
        {
            let mem_state = log2storage.mem_state.read().await;
            assert_eq!(mem_state.applied_commit_version, 0, "初始应用版本应该为0");
            assert_eq!(mem_state.log_entries.len(), 0, "初始日志条目应该为空");
        }

        // 启动异步任务
        log2storage.start().await;
        
        // 等待一段时间让任务运行
        sleep(Duration::from_millis(3000)).await;
        log2storage.stop().await;

        info!("Mock测试完成");
    }

    // 测试日志组织功能
    #[tokio::test]
    async fn test_organize_entries() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = Arc::new(RocksDBStorage::new(temp_dir.path()));
        let log2storage = Arc::new(Log2Storage::new(storage.clone(), "not_used".to_string()).await);
        
        // 创建测试日志条目
        let entries = vec![
            LogEntry {
                prev_commit_version: 0,
                commit_version: 1,
                graph_id: 42,
                write_set: Some(WriteSet {
                    upsert_kvs: vec![
                        Kv { key: b"key1".to_vec(), value: b"value1".to_vec() },
                    ],
                    deleted_keys: vec![b"key2".to_vec()],
                }),
            },
            LogEntry {
                prev_commit_version: 1,
                commit_version: 2,
                graph_id: 42,
                write_set: Some(WriteSet {
                    upsert_kvs: vec![
                        Kv { key: b"key3".to_vec(), value: b"value3".to_vec() },
                    ],
                    deleted_keys: vec![],
                }),
            },
            LogEntry {
                prev_commit_version: 2,
                commit_version: 3,
                graph_id: 43,
                write_set: Some(WriteSet {
                    upsert_kvs: vec![
                        Kv { key: b"keyA".to_vec(), value: b"valueA".to_vec() },
                    ],
                    deleted_keys: vec![],
                }),
            },
        ];
        
        // 测试组织功能
        let organized = log2storage.organize_entries(entries);
        
        assert!(organized.contains_key(&42), "应该包含图42");
        assert!(organized.contains_key(&43), "应该包含图43");
        
        let graph42_data = organized.get(&42).unwrap();
        assert!(graph42_data.contains_key(&1), "图42应该包含版本1");
        assert!(graph42_data.contains_key(&2), "图42应该包含版本2");
        
        info!("日志组织功能测试完成");
    }
}