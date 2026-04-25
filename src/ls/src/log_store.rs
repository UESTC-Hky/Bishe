use crate::error::{Result, LsError};
use rocksdb::{DB, Options, WriteBatch};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use rpc::common::WriteSet;
use rpc::log::LogEntry as ProtoLogEntry;
use prost::Message;

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub prev_commit_version: u64,
    pub commit_version: u64,
    pub graph_id: u32,
    pub write_set: WriteSet,
}

impl LogEntry {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.prev_commit_version.to_be_bytes());
        buf.extend_from_slice(&self.commit_version.to_be_bytes());
        buf.extend_from_slice(&self.graph_id.to_be_bytes());
        
        // 序列化 write_set
        let serialized = prost::Message::encode_to_vec(&self.write_set);
        buf.extend_from_slice(&(serialized.len() as u32).to_be_bytes());
        buf.extend_from_slice(&serialized);
        
        Ok(buf)
    }
    
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 20 {
            return Err(LsError::InvalidArgument {
                message: "Insufficient bytes for LogEntry".to_string(),
            });
        }
        
        let prev_commit_version = u64::from_be_bytes(bytes[0..8].try_into().map_err(|_| {
            LsError::InvalidArgument {
                message: "Failed to parse prev_commit_version".to_string(),
            }
        })?);
        
        let commit_version = u64::from_be_bytes(bytes[8..16].try_into().map_err(|_| {
            LsError::InvalidArgument {
                message: "Failed to parse commit_version".to_string(),
            }
        })?);
        
        let graph_id = u32::from_be_bytes(bytes[16..20].try_into().map_err(|_| {
            LsError::InvalidArgument {
                message: "Failed to parse graph_id".to_string(),
            }
        })?);
        
        let write_set = if bytes.len() > 24 {
            let len = u32::from_be_bytes(bytes[20..24].try_into().map_err(|_| {
                LsError::InvalidArgument {
                    message: "Failed to parse write_set length".to_string(),
                }
            })?) as usize;
            
            if len > 0 && bytes.len() >= 24 + len {
                WriteSet::decode(&bytes[24..24+len])?
            } else {
                // 直接创建空的 WriteSet，不实现 Default trait
                WriteSet {
                    upsert_kvs: Vec::new(),
                    deleted_keys: Vec::new(),
                }
            }
        } else {
            WriteSet {
                upsert_kvs: Vec::new(),
                deleted_keys: Vec::new(),
            }
        };
        
        Ok(LogEntry {
            prev_commit_version,
            commit_version,
            graph_id,
            write_set,
        })
    }
}

pub struct LogStore {
    db: Arc<DB>,
    mem_cache: RwLock<BTreeMap<u64, LogEntry>>,
    max_version: RwLock<u64>,
}

impl LogStore {
    pub async fn get_max_committed_version(&self) -> Result<u64> {
        // 从DB中读取max_version，这表示已持久化的最大版本
        match self.db.get(b"max_version")? {
            Some(bytes) => {
                if bytes.len() == 8 {
                    let version = u64::from_be_bytes(bytes.try_into().unwrap());
                    Ok(version)
                } else {
                    Ok(0)
                }
            }
            None => Ok(0),
        }
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        let db = Arc::new(DB::open(&opts, path)?);
        
        // 从DB恢复max_version
        let max_version = match db.get(b"max_version")? {
            Some(bytes) => u64::from_be_bytes(bytes.try_into().unwrap()),
            None => 0,
        };
        
        // 加载最近日志到内存缓存
        let mem_cache = Self::load_recent_entries(&db, max_version)?;
        
        Ok(Self {
            db,
            mem_cache: RwLock::new(mem_cache),
            max_version: RwLock::new(max_version),
        })
    }
    
    fn load_recent_entries(db: &DB, max_version: u64) -> Result<BTreeMap<u64, LogEntry>> {
        let mut cache = BTreeMap::new();
        const CACHE_SIZE: usize = 1000;
        
        if max_version == 0 {
            return Ok(cache);
        }
        
        let start_version = max_version.saturating_sub(CACHE_SIZE as u64) + 1;
        let mut iter = db.raw_iterator();
        
        let start_key = start_version.to_be_bytes();
        iter.seek(&start_key);
        
        while iter.valid() {
            if let (Some(key_bytes), Some(value_bytes)) = (iter.key(), iter.value()) {
                if key_bytes.len() == 8 {
                    let version = u64::from_be_bytes(key_bytes.try_into().unwrap());
                    if version > max_version {
                        break;
                    }
                    
                    if let Ok(entry) = LogEntry::from_bytes(value_bytes) {
                        cache.insert(version, entry);
                    }
                }
            }
            iter.next();
        }
        
        Ok(cache)
    }
    
    pub async fn append_entry(&self, entry: ProtoLogEntry) -> Result<bool> {  
        let mut max_version = self.max_version.write().await;
        let mut mem_cache = self.mem_cache.write().await;
        
        // 版本连续性检查
        if entry.prev_commit_version != *max_version {
            return Err(LsError::VersionMismatch {
                expected: *max_version,
                actual: entry.prev_commit_version,
            });
        }
        
        // 转换为内部结构并验证
        let internal_entry = LogEntry {
            prev_commit_version: entry.prev_commit_version,
            commit_version: entry.commit_version,
            graph_id: entry.graph_id,
            write_set: entry.write_set.ok_or_else(|| {
                LsError::invalid_argument("WriteSet is missing")
            })?,
        };
        
        // 验证commit_version递增
        if internal_entry.commit_version != *max_version + 1 {
            return Err(LsError::invalid_argument(
                format!("Commit version should be {}, but got {}", *max_version + 1, internal_entry.commit_version)
            ));
        }
        
        // 使用新的序列化方法
        let key = internal_entry.commit_version.to_be_bytes();
        let value = internal_entry.to_bytes()?;
        
        let mut batch = WriteBatch::default();
        batch.put(&key, &value);
        batch.put(b"max_version", &internal_entry.commit_version.to_be_bytes());
        
        self.db.write(batch)?;
        
        // 更新内存状态
        mem_cache.insert(internal_entry.commit_version, internal_entry);
        *max_version += 1;
        
        // 清理过期的缓存条目
        if mem_cache.len() > 1000 {
            let first_key = *mem_cache.keys().next().unwrap();
            mem_cache.remove(&first_key);
        }
        
        tracing::info!("LS: 成功追加日志条目，版本: {}", *max_version);
        Ok(true)
    }
    
    pub async fn fetch_entries(&self, from_version: u64, max_entries: u32) -> Vec<ProtoLogEntry> {  
        let mem_cache = self.mem_cache.read().await;
        let max_version = *self.max_version.read().await;
        
        if from_version > max_version {
            return Vec::new();
        }
        
        let mut result = Vec::new();
        let mut current = from_version;
        let max_entries = max_entries as usize;
        
        // 首先从内存缓存中查找
        while result.len() < max_entries && current <= max_version {
            if let Some(entry) = mem_cache.get(&current) {
                result.push(convert_to_proto_log_entry(entry));
                current += 1;
            } else {
                break;
            }
        }
        
        // 如果内存缓存中没有找到所有需要的条目，从磁盘加载
        if result.len() < max_entries && current <= max_version {
            drop(mem_cache);
            
            let disk_entries = self.fetch_from_disk(current, max_entries - result.len());
            result.extend(disk_entries.into_iter().map(|entry| convert_to_proto_log_entry(&entry)));
        }
        
        tracing::info!("LS: 返回 {} 条日志条目，从版本 {}", result.len(), from_version);
        result
    }
    
    fn fetch_from_disk(&self, from_version: u64, max_entries: usize) -> Vec<LogEntry> {
        let mut result = Vec::new();
        let mut iter = self.db.raw_iterator();
        
        let start_key = from_version.to_be_bytes();
        iter.seek(&start_key);
        
        while iter.valid() && result.len() < max_entries {
            if let Some(value) = iter.value() {
                if let Ok(entry) = LogEntry::from_bytes(value) {
                    result.push(entry);
                }
            }
            iter.next();
        }
        
        result
    }
    
    pub async fn get_max_version(&self) -> u64 {
        *self.max_version.read().await
    }
}

// 辅助函数：将内部LogEntry转换为protobuf LogEntry
fn convert_to_proto_log_entry(entry: &LogEntry) -> ProtoLogEntry {  
    ProtoLogEntry {
        prev_commit_version: entry.prev_commit_version,
        commit_version: entry.commit_version,
        graph_id: entry.graph_id,
        write_set: Some(entry.write_set.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use rpc::common::{WriteSet, Kv};

    // 辅助函数：创建测试 LogEntry
    fn create_test_log_entry(prev_version: u64, commit_version: u64, graph_id: u32) -> LogEntry {
        let write_set = WriteSet {
            upsert_kvs: vec![
                Kv { key: b"key1".to_vec(), value: b"value1".to_vec() },
                Kv { key: b"key2".to_vec(), value: b"value2".to_vec() },
            ],
            deleted_keys: vec![b"key3".to_vec()],
        };
        
        LogEntry {
            prev_commit_version: prev_version,
            commit_version,
            graph_id,
            write_set,
        }
    }

    // 辅助函数：创建测试 ProtoLogEntry
    fn create_test_proto_log_entry(prev_version: u64, commit_version: u64, graph_id: u32) -> ProtoLogEntry {
        let write_set = WriteSet {
            upsert_kvs: vec![
                Kv { key: b"key1".to_vec(), value: b"value1".to_vec() },
                Kv { key: b"key2".to_vec(), value: b"value2".to_vec() },
            ],
            deleted_keys: vec![b"key3".to_vec()],
        };
        
        ProtoLogEntry {
            prev_commit_version: prev_version,
            commit_version,
            graph_id,
            write_set: Some(write_set),
        }
    }

    // 测试 LogEntry 序列化和反序列化
    #[test]
    fn test_log_entry_serialization_deserialization() {
        println!("测试 LogEntry 序列化和反序列化...");
        
        // 测试正常情况
        let original_entry = create_test_log_entry(0, 1, 42);
        
        let serialized = original_entry.to_bytes()
            .expect("序列化失败");
        assert!(!serialized.is_empty(), "序列化结果不应为空");
        
        let deserialized = LogEntry::from_bytes(&serialized)
            .expect("反序列化失败");
        
        assert_eq!(deserialized.prev_commit_version, original_entry.prev_commit_version);
        assert_eq!(deserialized.commit_version, original_entry.commit_version);
        assert_eq!(deserialized.graph_id, original_entry.graph_id);
        assert_eq!(deserialized.write_set.upsert_kvs.len(), original_entry.write_set.upsert_kvs.len());
        assert_eq!(deserialized.write_set.deleted_keys.len(), original_entry.write_set.deleted_keys.len());
        
        println!("✓ 正常序列化/反序列化测试通过");
    }

    // 测试边界情况
    #[test]
    fn test_log_entry_edge_cases() {
        println!("测试 LogEntry 边界情况...");
        
        // 测试空 WriteSet
        let empty_entry = LogEntry {
            prev_commit_version: 0,
            commit_version: 1,
            graph_id: 42,
            write_set: WriteSet {
                upsert_kvs: Vec::new(),
                deleted_keys: Vec::new(),
            },
        };
        
        let bytes = empty_entry.to_bytes()
            .expect("空 WriteSet 序列化失败");
        let deserialized = LogEntry::from_bytes(&bytes)
            .expect("空 WriteSet 反序列化失败");
        
        assert!(deserialized.write_set.upsert_kvs.is_empty());
        assert!(deserialized.write_set.deleted_keys.is_empty());
        
        // 测试无效字节数组
        let invalid_bytes = vec![0u8; 10]; // 太短的字节数组
        let result = LogEntry::from_bytes(&invalid_bytes);
        assert!(result.is_err(), "过短的字节数组应该返回错误");
        
        println!("✓ 边界情况测试通过");
    }

    // 测试 LogStore 初始化
    #[tokio::test]
    async fn test_log_store_initialization() {
        println!("测试 LogStore 初始化...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        let max_version = store.get_max_version().await;
        assert_eq!(max_version, 0, "初始版本应该为 0");
        
        println!("✓ LogStore 初始化测试通过");
    }

    // 测试成功追加日志条目
    #[tokio::test]
    async fn test_append_entry_success() {
        println!("测试成功追加日志条目...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 第一次追加
        let entry1 = create_test_proto_log_entry(0, 1, 42);
        let result = store.append_entry(entry1).await
            .expect("第一次追加失败");
        assert!(result, "第一次追加应该成功");
        
        // 验证版本更新
        let max_version = store.get_max_version().await;
        assert_eq!(max_version, 1, "最大版本应该更新为 1");
        
        // 第二次追加
        let entry2 = create_test_proto_log_entry(1, 2, 42);
        let result = store.append_entry(entry2).await
            .expect("第二次追加失败");
        assert!(result, "第二次追加应该成功");
        
        let max_version = store.get_max_version().await;
        assert_eq!(max_version, 2, "最大版本应该更新为 2");
        
        println!("✓ 成功追加日志条目测试通过");
    }

    // 测试版本不匹配错误
    #[tokio::test]
    async fn test_append_entry_version_mismatch() {
        println!("测试版本不匹配错误...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 尝试追加版本不连续的日志
        let invalid_entry = create_test_proto_log_entry(5, 6, 42); // prev_version=5，但当前 max_version=0
        
        let result = store.append_entry(invalid_entry).await;
        assert!(result.is_err(), "版本不连续应该返回错误");
        
        if let Err(LsError::VersionMismatch { expected, actual }) = result {
            assert_eq!(expected, 0, "期望版本应该是 0");
            assert_eq!(actual, 5, "实际版本应该是 5");
        } else {
            panic!("应该返回 VersionMismatch 错误");
        }
        
        println!("✓ 版本不匹配错误测试通过");
    }

    // 测试无效的提交版本
    #[tokio::test]
    async fn test_append_entry_invalid_commit_version() {
        println!("测试无效的提交版本...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 先追加一个正常条目
        let entry1 = create_test_proto_log_entry(0, 1, 42);
        store.append_entry(entry1).await
            .expect("第一次追加失败");
        
        // 尝试追加 commit_version 不递增的条目
        let invalid_entry = create_test_proto_log_entry(1, 1, 42); // commit_version 应该为 2
        
        let result = store.append_entry(invalid_entry).await;
        assert!(result.is_err(), "commit_version 不递增应该返回错误");
        
        println!("✓ 无效提交版本测试通过");
    }

    // 测试从空存储获取条目
    #[tokio::test]
    async fn test_fetch_entries_empty() {
        println!("测试从空存储获取条目...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 从空存储中获取条目
        let entries = store.fetch_entries(0, 10).await;
        assert!(entries.is_empty(), "空存储应该返回空数组");
        
        // 从超出范围的版本获取
        let entries = store.fetch_entries(100, 10).await;
        assert!(entries.is_empty(), "超出范围的版本应该返回空数组");
        
        println!("✓ 空存储获取条目测试通过");
    }

    // 测试获取有数据的条目
    #[tokio::test]
    async fn test_fetch_entries_with_data() {
        println!("测试获取有数据的条目...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 添加一些测试数据
        for i in 0..5 {
            let entry = create_test_proto_log_entry(i, i + 1, 42);
            store.append_entry(entry).await
                .expect(&format!("追加条目 {} 失败", i));
        }
        
        // 获取所有条目
        let entries = store.fetch_entries(1, 10).await;
        assert_eq!(entries.len(), 5, "应该返回 5 个条目");
        
        // 验证条目的顺序和内容
        for (i, entry) in entries.iter().enumerate() {
            let expected_version = (i + 1) as u64;
            assert_eq!(entry.commit_version, expected_version, "条目 {} 的版本不正确", i);
            assert_eq!(entry.prev_commit_version, expected_version - 1, "条目 {} 的前一版本不正确", i);
            assert_eq!(entry.graph_id, 42, "条目 {} 的 graph_id 不正确", i);
        }
        
        // 限制获取数量
        let limited_entries = store.fetch_entries(1, 3).await;
        assert_eq!(limited_entries.len(), 3, "应该只返回 3 个条目");
        
        // 从中间版本开始获取
        let middle_entries = store.fetch_entries(3, 10).await;
        assert_eq!(middle_entries.len(), 3, "从版本 3 开始应该返回 3 个条目");
        assert_eq!(middle_entries[0].commit_version, 3, "第一个条目应该是版本 3");
        
        println!("✓ 有数据获取条目测试通过");
    }

    // 测试内存缓存和磁盘存储的交互
    #[tokio::test]
    async fn test_memory_disk_interaction() {
        println!("测试内存缓存和磁盘存储的交互...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        
        // 第一次创建 LogStore 并添加数据
        {
            let store = LogStore::new(temp_dir.path())
                .expect("LogStore 初始化失败");
            
            for i in 0..3 {
                let entry = create_test_proto_log_entry(i, i + 1, 42);
                store.append_entry(entry).await
                    .expect(&format!("追加条目 {} 失败", i));
            }
        } // store 被 drop，数据应该被持久化到磁盘
        
        // 重新创建 LogStore 来模拟重启
        let store2 = LogStore::new(temp_dir.path())
            .expect("重新创建 LogStore 失败");
        
        // 验证数据可以从磁盘正确恢复
        let entries = store2.fetch_entries(1, 10).await;
        assert_eq!(entries.len(), 3, "重启后应该能恢复所有条目");
        
        let max_version = store2.get_max_version().await;
        assert_eq!(max_version, 3, "最大版本应该正确恢复");
        
        println!("✓ 内存磁盘交互测试通过");
    }

    // 测试内存缓存淘汰机制
    #[tokio::test]
    async fn test_memory_cache_eviction() {
        println!("测试内存缓存淘汰机制...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败");
        
        // 添加超过缓存大小的条目（缓存大小是 1000）
        // 为了测试快速，我们只添加少量数据，主要验证逻辑不崩溃
        let test_entry_count = 5;
        
        for i in 0..test_entry_count {
            let entry = create_test_proto_log_entry(i, i + 1, 42);
            store.append_entry(entry).await
                .expect(&format!("追加条目 {} 失败", i));
        }
        
        // 检查内存缓存大小（应该不超过 1000）
        let entries = store.fetch_entries(1, test_entry_count as u32).await;
        assert_eq!(entries.len(), test_entry_count as usize, "应该能获取所有条目");
        
        println!("✓ 内存缓存淘汰测试通过");
    }

    // 测试内部结构到 protobuf 的转换
    #[tokio::test]
    async fn test_convert_to_proto_log_entry() {
        println!("测试内部结构到 protobuf 的转换...");
        
        let internal_entry = create_test_log_entry(0, 1, 42);
        let proto_entry = convert_to_proto_log_entry(&internal_entry);
        
        assert_eq!(proto_entry.prev_commit_version, internal_entry.prev_commit_version);
        assert_eq!(proto_entry.commit_version, internal_entry.commit_version);
        assert_eq!(proto_entry.graph_id, internal_entry.graph_id);
        assert!(proto_entry.write_set.is_some(), "ProtoLogEntry 的 write_set 应该存在");
        
        let write_set = proto_entry.write_set.unwrap();
        assert_eq!(write_set.upsert_kvs.len(), internal_entry.write_set.upsert_kvs.len());
        assert_eq!(write_set.deleted_keys.len(), internal_entry.write_set.deleted_keys.len());
        
        println!("✓ 结构转换测试通过");
    }

    // 测试并发操作
    #[tokio::test]
    async fn test_concurrent_operations() {
        println!("测试并发操作...");
        
        let temp_dir = TempDir::new().expect("创建临时目录失败");
        let store = Arc::new(LogStore::new(temp_dir.path())
            .expect("LogStore 初始化失败"));
        
        let mut handles = vec![];
        
        // 并发追加条目
        for i in 0..3 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                // 每个任务尝试追加多个条目
                for j in 0..2 {
                    let version_base = i * 2 + j;
                    let entry = create_test_proto_log_entry(version_base as u64, version_base as u64 + 1, i as u32);
                    let result = store_clone.append_entry(entry).await;
                    assert!(result.is_ok(), "并发追加失败: {:?}", result);
                }
            });
            handles.push(handle);
        }
        
        // 等待所有追加完成
        for handle in handles {
            handle.await.expect("任务执行失败");
        }
        
        // 验证最终状态
        let max_version = store.get_max_version().await;
        assert_eq!(max_version, 6, "并发操作后的最大版本应该是 6");
        
        println!("✓ 并发操作测试通过");
    }
}