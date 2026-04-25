use crate::error::ErrorLocation;
use crate::error::{Result, SsError};

use core::result::Result::Err;
use rocksdb::{DBRawIteratorWithThreadMode, DBWithThreadMode, MultiThreaded, WriteBatch};
use std::cmp::Ordering;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

// DELETE_FLAG是已删除 key 的值，即 key 在特定版本中已被删除。
pub const DELETED_FLAG: &[u8] = b"__nil__";
// 元数据列族名
pub const METADATA_CF_NAME: &str = "metadata";

pub struct RocksDBStorage {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    path: String,
}

impl RocksDBStorage {
    fn get_options() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();

        // 内存与 memtable：根据机器内存调整。示例保守值（64MB * 4）
        options.set_write_buffer_size(64 * 1024 * 1024); // 64MB
        options.set_max_write_buffer_number(4);
        options.set_min_write_buffer_number_to_merge(1);

        // 并行度：基于 CPU 核心数
        let cpus = num_cpus::get().saturating_sub(1).max(1) as i32;
        // 如果可用，优先使用 increase_parallelism（提高内部线程/并行度）
        let _ = options.increase_parallelism(cpus);
        options.set_max_background_jobs(cpus);

        // compaction、WAL limit 等
        options.set_max_subcompactions(2);
        options.set_max_total_wal_size(1 << 30); // 1GB

        // 打开文件数：-1 表示 unlimited（根据系统 ulimit 调整）
        options.set_max_open_files(-1);

        // 压缩与 level 策略（减少磁盘使用与 IO）
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        options.set_level_compaction_dynamic_level_bytes(true);

        options.create_if_missing(true);
        options.create_missing_column_families(true);

        options.set_enable_pipelined_write(true);

        options
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let options = Self::get_options();

        let cfs = rocksdb::DB::list_cf(&options, path.as_ref()).unwrap_or(vec![]);

        info!(
            "ss: open rocksdb at: {:?}, CF includes: {:?}",
            path.as_ref(),
            cfs
        );

        // 基于设定的列族打开数据库
        let db = rocksdb::DBWithThreadMode::open_cf(&options, path.as_ref(), cfs).unwrap();

        Self {
            db: Arc::new(db),
            path: path.as_ref().display().to_string(),
        }
    }

    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<()> {
        warn!("ss: destroy rocksdb at {:?}", path.as_ref());
        Ok(fs::remove_dir_all(path)?)
    }

    pub fn create_cf(&self, cf_name: &str) -> Result<()> {
        if self.is_cf_exist(cf_name) {
            info!("ss: Column family {} already exists", cf_name);
            return Ok(());
        }

        self.db
            .create_cf(cf_name, &Self::get_options())
            .map_err(|e| ss_error!(RocksDB { source: e }))?;

        info!("ss: Created column family: {}", cf_name);
        Ok(())
    }

    pub fn drop_cf(&self, cf_name: &str) -> Result<()> {
        if !self.is_cf_exist(cf_name) {
            info!("ss: Column family {} does not exist", cf_name);
            return Ok(());
        }

        self.db
            .drop_cf(cf_name)
            .map_err(|e| ss_error!(RocksDB { source: e }))?;

        info!("ss: Dropped column family: {}", cf_name);
        Ok(())
    }
    pub fn is_cf_exist(&self, cf_name: &str) -> bool {
        self.db.cf_handle(cf_name).is_some()
    }

    // 批量写，所有 kv 对的 version 相同；若 kv 对中的值(value) 为 None, 代表可删除该 kv 对
    pub fn batch_write(
        &self,
        graph_id: u32,
        kvs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        version: u64,
    ) -> Result<()> {
        let kvs_len = kvs.len();
        let cf_name = graph_id.to_string();
        let cf_handle = self
            .db
            .cf_handle(&cf_name)
            .ok_or_else(|| ss_error!(ColumnFamilyNotFound { name: cf_name }))?;

        let mut batch = WriteBatch::default();

        for (key, value) in kvs {
            // 构建版本化的键：key + version (大端字节序)
            let versioned_key = Self::build_versioned_key(&key, version);

            match value {
                Some(value) => {
                    batch.put_cf(&cf_handle, versioned_key, value);
                }
                None => {
                    batch.put_cf(&cf_handle, versioned_key, DELETED_FLAG);
                }
            }
        }

        self.db
            .write(batch)
            .map_err(|e| ss_error!(RocksDB { source: e }))?;

        info!(
            "ss: Batch write completed for graph {}, version: {}, {} entries",
            graph_id, version, kvs_len
        );
        Ok(())
    }

    pub async fn flush(&self, graph_id: u32) -> Result<()> {
        let db = self.db.clone();
        let begin = std::time::Instant::now();
        let handle: tokio::task::JoinHandle<Result<()>> = tokio::task::spawn_blocking(move || {
            db.flush_cf(&db.cf_handle(&graph_id.to_string()).ok_or_else(|| {
                ss_error!(ColumnFamilyNotFound {
                    name: graph_id.to_string()
                })
            })?)?;
            tracing::info!(
                "ss: flush graph (id: {}) succeed, time cost {:?}",
                graph_id,
                begin.elapsed()
            );
            Ok(())
        });
        handle.await??;
        Ok(())
    }

    pub fn get_applied_commit_version(&self) -> Result<Option<u64>> {
        if !self.is_cf_exist(METADATA_CF_NAME) {
            return Ok(None);
        }

        let cf_handle = self.db.cf_handle(METADATA_CF_NAME).ok_or_else(|| {
            ss_error!(ColumnFamilyNotFound {
                name: METADATA_CF_NAME.to_string()
            })
        })?;

        match self.db.get_cf(&cf_handle, "applied_commit_version") {
            Ok(Some(value)) => {
                if value.len() == 8 {
                    let version = u64::from_be_bytes(value.try_into().unwrap());
                    Ok(Some(version))
                } else {
                    Ok(None)
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ss_error!(RocksDB { source: e })),
        }
    }

    // 将applied_commit_version写入metadata列族中,若无则创建
    pub fn write_applied_commit_version(&self, applied_commit_version: u64) -> Result<()> {
        if !self.is_cf_exist(METADATA_CF_NAME) {
            self.db.create_cf(METADATA_CF_NAME, &Self::get_options())?;
        }
        let cf_handle = self.db.cf_handle(METADATA_CF_NAME).ok_or_else(|| {
            ss_error!(ColumnFamilyNotFound {
                name: METADATA_CF_NAME.to_string()
            })
        });
        match cf_handle {
            Ok(inner_handle) => {
                // 写入applied_commit_version
                let mut batch = WriteBatch::default();
                batch.put_cf(
                    &inner_handle,
                    "applied_commit_version",
                    applied_commit_version.to_be_bytes(),
                );
                self.db.write(batch)?;
                tracing::info!(
                    "ss: applied_commit_version = {}, 已持久化",
                    applied_commit_version
                );
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }

    pub fn get(&self, graph_id: u32, key: Vec<u8>, version: u64) -> Result<Option<Vec<u8>>> {
        let cf_name = graph_id.to_string();
        let cf_handle = self
            .db
            .cf_handle(&cf_name)
            .ok_or_else(|| ss_error!(ColumnFamilyNotFound { name: cf_name }))?;

        // 构建搜索键：原始 key + 版本（大端序）
        let search_key = Self::build_versioned_key(&key, version);
        let mut iter = self.db.raw_iterator_cf(&cf_handle);

        // 定位到 <= search_key 的最大键
        iter.seek_for_prev(&search_key);

        if iter.valid() {
            if let (Some(found_key), Some(found_value)) = (iter.key(), iter.value()) {
                let raw_found = Self::extract_raw_key(found_key);

                // 必须确保 raw key 匹配
                if raw_found == key {
                    // 由于 seek_for_prev 保证 found_key <= search_key，
                    // 因此提取出的版本一定 <= version，无需再次比较
                    if found_value == DELETED_FLAG {
                        return Ok(None);
                    } else {
                        return Ok(Some(found_value.to_vec()));
                    }
                }
                // raw key 不匹配，说明目标 key 的所有版本都大于 version（不存在 ≤ version 的版本）
            }
        }

        Ok(None)
    }
    // 获取指定 graph_id 中多个 key 的 value，若某 key 不存在则其对应的 value 为 None
    pub fn multi_get(
        &self,
        graph_id: u32,
        keys: Vec<Vec<u8>>,
        version: u64,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let value = self.get(graph_id, key, version)?;
            results.push(value);
        }

        Ok(results)
    }

    // 辅助函数：构建版本化的键
    fn build_versioned_key(key: &[u8], version: u64) -> Vec<u8> {
        let mut versioned_key = key.to_vec();
        versioned_key.extend_from_slice(&version.to_be_bytes());
        versioned_key
    }

    // 辅助函数：从版本化键中提取原始键
    fn extract_raw_key(versioned_key: &[u8]) -> Vec<u8> {
        if versioned_key.len() >= 8 {
            versioned_key[..versioned_key.len() - 8].to_vec()
        } else {
            versioned_key.to_vec()
        }
    }

    // 辅助函数：从版本化键中提取版本号
    fn extract_version(versioned_key: &[u8]) -> u64 {
        if versioned_key.len() >= 8 {
            let version_bytes = &versioned_key[versioned_key.len() - 8..];
            u64::from_be_bytes(version_bytes.try_into().unwrap())
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // 辅助函数：将字符串转换为字节向量
    trait ToBytes {
        fn to_k(&self) -> Vec<u8>;
        fn to_v(&self) -> Vec<u8>;
    }

    impl ToBytes for &str {
        fn to_k(&self) -> Vec<u8> {
            self.as_bytes().to_vec()
        }

        fn to_v(&self) -> Vec<u8> {
            self.as_bytes().to_vec()
        }
    }

    // 获取测试存储路径
    fn get_ss_path() -> String {
        "test_rocksdb_storage".to_string()
    }

    #[test]
    #[serial]
    fn test_batch_write() {
        // 清理旧数据
        let _ = RocksDBStorage::destroy(get_ss_path());

        // 初始化存储
        let store = RocksDBStorage::new(get_ss_path());
        let graph_id = 1;
        store
            .create_cf(&graph_id.to_string())
            .expect("CF creation failed");

        println!("=== 测试 batch_write 功能 ===");

        // 测试正常写入
        let version = 100;
        let entries = vec![
            ("key1".to_k(), Some("value1".to_v())),
            ("key2".to_k(), Some("value2".to_v())),
            ("key3".to_k(), Some("value3".to_v())),
        ];

        store
            .batch_write(graph_id, entries, version)
            .expect("Batch write failed");

        // 验证写入成功
        assert_eq!(
            store.get(graph_id, "key1".to_k(), version).unwrap(),
            Some("value1".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key2".to_k(), version).unwrap(),
            Some("value2".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key3".to_k(), version).unwrap(),
            Some("value3".to_v())
        );

        // 测试包含删除操作的写入
        let version2 = 200;
        let entries_with_delete = vec![
            ("key1".to_k(), Some("updated_value1".to_v())), // 更新
            ("key2".to_k(), None),                          // 删除
            ("key4".to_k(), Some("value4".to_v())),         // 新增
        ];

        store
            .batch_write(graph_id, entries_with_delete, version2)
            .expect("Batch write with delete failed");

        // 验证更新和删除
        assert_eq!(
            store.get(graph_id, "key1".to_k(), version2).unwrap(),
            Some("updated_value1".to_v())
        );
        assert_eq!(store.get(graph_id, "key2".to_k(), version2).unwrap(), None);
        assert_eq!(
            store.get(graph_id, "key4".to_k(), version2).unwrap(),
            Some("value4".to_v())
        );

        println!("=== batch_write 测试通过 ===");

        // 清理测试数据
        let _ = RocksDBStorage::destroy(get_ss_path());
    }

    #[test]
    #[serial]
    fn test_get() {
        // 清理旧数据
        let _ = RocksDBStorage::destroy(get_ss_path());

        // 初始化存储
        let store = RocksDBStorage::new(get_ss_path());
        let graph_id = 2;
        store
            .create_cf(&graph_id.to_string())
            .expect("CF creation failed");

        println!("=== 测试 get 功能 - 版本隔离 ===");

        // 写入版本 V1
        let v1 = 100;
        let entries_v1 = vec![
            ("key1".to_k(), Some("value1_v100".to_v())),
            ("key2".to_k(), Some("value2_v100".to_v())),
        ];
        store
            .batch_write(graph_id, entries_v1, v1)
            .expect("V1 write failed");

        // 写入版本 V2
        let v2 = 200;
        let entries_v2 = vec![
            ("key1".to_k(), Some("value1_v200".to_v())), // 更新
            ("key3".to_k(), Some("value3_v200".to_v())), // 新增
        ];
        store
            .batch_write(graph_id, entries_v2, v2)
            .expect("V2 write failed");

        // 验证版本隔离
        // 在版本 V1 读取，应返回版本 V1 写入的值
        assert_eq!(
            store.get(graph_id, "key1".to_k(), v1).unwrap(),
            Some("value1_v100".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key2".to_k(), v1).unwrap(),
            Some("value2_v100".to_v())
        );
        assert_eq!(store.get(graph_id, "key3".to_k(), v1).unwrap(), None);

        // 在 V1 与 V2 之间的版本读取，应保持只见 V1 的旧值
        let middle_version = 150;
        assert_eq!(
            store.get(graph_id, "key1".to_k(), middle_version).unwrap(),
            Some("value1_v100".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key3".to_k(), middle_version).unwrap(),
            None
        );

        // 在 V2 及之后的版本读取，应返回版本 V2 的新值
        assert_eq!(
            store.get(graph_id, "key1".to_k(), v2).unwrap(),
            Some("value1_v200".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key2".to_k(), v2).unwrap(),
            Some("value2_v100".to_v())
        );
        assert_eq!(
            store.get(graph_id, "key3".to_k(), v2).unwrap(),
            Some("value3_v200".to_v())
        );

        // 在首次写入前的版本读取，应返回 None
        assert_eq!(store.get(graph_id, "key1".to_k(), 50).unwrap(), None);

        println!("=== get 测试通过 ===");

        // 清理测试数据
        let _ = RocksDBStorage::destroy(get_ss_path());
    }

    #[test]
    #[serial]
    fn test_multi_get() {
        // 清理旧数据
        let _ = RocksDBStorage::destroy(get_ss_path());

        // 初始化存储
        let store = RocksDBStorage::new(get_ss_path());
        let graph_id = 3;
        store
            .create_cf(&graph_id.to_string())
            .expect("CF creation failed");

        println!("=== 测试 multi_get 功能 ===");

        // 准备测试数据
        let version = 100;
        let entries = vec![
            ("key1".to_k(), Some("value1".to_v())),
            ("key2".to_k(), Some("value2".to_v())),
            ("key3".to_k(), Some("value3".to_v())),
        ];
        store
            .batch_write(graph_id, entries, version)
            .expect("Write failed");

        // 测试批量读取存在的键
        let keys = vec!["key1".to_k(), "key2".to_k(), "key3".to_k()];
        let results = store
            .multi_get(graph_id, keys, version)
            .expect("Multi-get failed");

        assert_eq!(
            results,
            vec![
                Some("value1".to_v()),
                Some("value2".to_v()),
                Some("value3".to_v())
            ]
        );

        // 测试混合读取（存在和不存在的键）
        let mixed_keys = vec![
            "key1".to_k(),         // 存在
            "non_existent".to_k(), // 不存在
            "key3".to_k(),         // 存在
        ];
        let mixed_results = store
            .multi_get(graph_id, mixed_keys, version)
            .expect("Mixed multi-get failed");

        assert_eq!(
            mixed_results,
            vec![Some("value1".to_v()), None, Some("value3".to_v())]
        );

        // 测试空键列表
        let empty_keys: Vec<Vec<u8>> = vec![];
        let empty_results = store
            .multi_get(graph_id, empty_keys, version)
            .expect("Empty multi-get failed");
        assert_eq!(empty_results.len(), 0);

        println!("=== multi_get 测试通过 ===");

        // 清理测试数据
        let _ = RocksDBStorage::destroy(get_ss_path());
    }
}
//cargo test test_get -- --nocapture
