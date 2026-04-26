use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{error, info, warn};

use crate::error::{Result, TsError};

/// WAL 日志条目类型
#[derive(Debug, Clone, PartialEq)]
pub enum WalEntryType {
    /// 事务开始
    Begin = 1,
    /// 写操作记录
    Write = 2,
    /// 准备提交（2PC 第一阶段）
    Prepare = 3,
    /// 提交（2PC 第二阶段）
    Commit = 4,
    /// 回滚
    Rollback = 5,
}

impl WalEntryType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Begin),
            2 => Some(Self::Write),
            3 => Some(Self::Prepare),
            4 => Some(Self::Commit),
            5 => Some(Self::Rollback),
            _ => None,
        }
    }
}

/// WAL 条目
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub entry_type: WalEntryType,
    pub tx_id: u64,
    pub graph_id: u32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>, // 用于undo
    pub timestamp: u64,
}

/// Write-Ahead Log 管理器
/// 确保在发生崩溃时可以通过日志恢复
pub struct WalManager {
    /// WAL 文件路径
    wal_dir: PathBuf,
    /// 当前写入文件
    current_file: Arc<Mutex<Option<File>>>,
    /// 当前文件序号
    current_seq_no: Arc<Mutex<u64>>,
    /// 是否启用 fsync
    enable_fsync: bool,
    /// 上一次检查点
    last_checkpoint: Arc<Mutex<u64>>,
}

impl WalManager {
    /// 创建 WAL 管理器
    /// 扫描已存在的 WAL 文件以确定起始序号，避免与新文件冲突
    pub fn new(wal_dir: PathBuf, enable_fsync: bool) -> Result<Self> {
        // 确保 WAL 目录存在
        fs::create_dir_all(&wal_dir).map_err(|e| TsError::Io(e))?;

        // 扫描已存在的 WAL 文件，找到最大的序号
        let max_seq = Self::find_max_seq_no(&wal_dir)?;

        let manager = Self {
            wal_dir,
            current_file: Arc::new(Mutex::new(None)),
            current_seq_no: Arc::new(Mutex::new(max_seq)),
            enable_fsync,
            last_checkpoint: Arc::new(Mutex::new(0)),
        };

        // 创建新的 WAL 文件（序号会在 max_seq 基础上递增）
        manager.rotate_wal_file()?;

        Ok(manager)
    }

    /// 扫描 WAL 目录，找到最大的文件序号
    fn find_max_seq_no(wal_dir: &Path) -> Result<u64> {
        let mut max_seq = 0u64;
        if !wal_dir.exists() {
            return Ok(0);
        }
        let entries = match fs::read_dir(wal_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(0),
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "log").unwrap_or(false) {
                if let Some(name) = path.file_stem() {
                    let name = name.to_string_lossy();
                    // 文件名格式: wal_0000000001 -> 提取序号 1
                    if name.starts_with("wal_") {
                        if let Ok(num) = name[4..].parse::<u64>() {
                            if num > max_seq {
                                max_seq = num;
                            }
                        }
                    }
                }
            }
        }
        Ok(max_seq)
    }

    /// 获取WAL目录路径
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// 轮换 WAL 文件（定期创建新文件）
    fn rotate_wal_file(&self) -> Result<()> {
        let mut seq_no = self.current_seq_no.lock().unwrap();
        *seq_no += 1;

        let file_name = format!("wal_{:010}.log", *seq_no);
        let file_path = self.wal_dir.join(&file_name);

        // 使用 create_new 确保不会覆盖已有文件（已有文件由恢复过程读取）
        // 如果文件居然存在（序号冲突），回退到 truncate 模式
        let file = match OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(_) => {
                // 文件已存在（异常情况），截断并复用
                warn!("WAL 文件 {:?} 已存在，将截断复用", file_path);
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&file_path)
                    .map_err(|e| TsError::Io(e))?
            }
        };

        info!("创建新 WAL 文件: {:?}", file_path);

        let mut current = self.current_file.lock().unwrap();
        if let Some(ref mut old_file) = *current {
            if self.enable_fsync {
                old_file.sync_all().map_err(|e| TsError::Io(e))?;
            }
            old_file.flush().map_err(|e| TsError::Io(e))?;
        }
        *current = Some(file);

        Ok(())
    }

    /// 写入 WAL 条目
    pub fn append_entry(&self, entry: &WalEntry) -> Result<()> {
        let mut file_guard = self.current_file.lock().unwrap();
        let file = file_guard
            .as_mut()
            .ok_or_else(|| TsError::Unknown("WAL 文件未初始化".into()))?;

        // 序列化条目
        let data = self.serialize_entry(entry)?;

        // 写入长度前缀
        file.write_u32::<BigEndian>(data.len() as u32)
            .map_err(|e| TsError::Io(e))?;

        // 写入数据
        file.write_all(&data).map_err(|e| TsError::Io(e))?;

        // 如果启用 fsync，强制刷入磁盘
        if self.enable_fsync {
            file.sync_all().map_err(|e| TsError::Io(e))?;
        }

        // 每 1000 条记录轮换一次文件
        if entry.tx_id % 1000 == 0 {
            drop(file_guard);
            self.rotate_wal_file()?;
        }

        Ok(())
    }

    /// 序列化 WAL 条目
    fn serialize_entry(&self, entry: &WalEntry) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // entry_type (1 byte)
        buf.write_u8(entry.entry_type.clone() as u8)
            .map_err(|e| TsError::Io(e))?;

        // tx_id (8 bytes)
        buf.write_u64::<BigEndian>(entry.tx_id)
            .map_err(|e| TsError::Io(e))?;

        // graph_id (4 bytes)
        buf.write_u32::<BigEndian>(entry.graph_id)
            .map_err(|e| TsError::Io(e))?;

        // timestamp (8 bytes)
        buf.write_u64::<BigEndian>(entry.timestamp)
            .map_err(|e| TsError::Io(e))?;

        // key 存在性 + 长度
        match &entry.key {
            Some(key) => {
                buf.write_u32::<BigEndian>(key.len() as u32)
                    .map_err(|e| TsError::Io(e))?;
                buf.write_all(key).map_err(|e| TsError::Io(e))?;
            }
            None => {
                buf.write_u32::<BigEndian>(0xFFFFFFFF)
                    .map_err(|e| TsError::Io(e))?;
            }
        }

        // value 存在性 + 长度
        match &entry.value {
            Some(value) => {
                buf.write_u32::<BigEndian>(value.len() as u32)
                    .map_err(|e| TsError::Io(e))?;
                buf.write_all(value).map_err(|e| TsError::Io(e))?;
            }
            None => {
                buf.write_u32::<BigEndian>(0xFFFFFFFF)
                    .map_err(|e| TsError::Io(e))?;
            }
        }

        // old_value 存在性 + 长度
        match &entry.old_value {
            Some(old_val) => {
                buf.write_u32::<BigEndian>(old_val.len() as u32)
                    .map_err(|e| TsError::Io(e))?;
                buf.write_all(old_val).map_err(|e| TsError::Io(e))?;
            }
            None => {
                buf.write_u32::<BigEndian>(0xFFFFFFFF)
                    .map_err(|e| TsError::Io(e))?;
            }
        }

        Ok(buf)
    }

    /// 从文件中读取所有 WAL 条目（用于恢复）
    /// 对部分写入的条目会优雅跳过，不影响其他有效条目
    pub fn read_all_entries(&self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let wal_dir_entries = fs::read_dir(&self.wal_dir).map_err(|e| TsError::Io(e))?;

        let mut file_paths: Vec<PathBuf> = wal_dir_entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "log")
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();
        file_paths.sort();

        for file_path in &file_paths {
            // 尝试打开文件；如果文件正在被写入（Windows 文件锁）则跳过
            let mut file = match File::open(file_path) {
                Ok(f) => f,
                Err(e) => {
                    warn!("无法打开 WAL 文件 {:?} 进行恢复: {}，跳过", file_path, e);
                    continue;
                }
            };

            // 检查文件是否为空，提前跳过
            let metadata = match file.metadata() {
                Ok(m) => m,
                Err(e) => {
                    warn!("无法读取 WAL 文件 {:?} 元数据: {}，跳过", file_path, e);
                    continue;
                }
            };
            if metadata.len() == 0 {
                continue;
            }

            loop {
                let mut len_buf = [0u8; 4];
                match file.read_exact(&mut len_buf) {
                    Ok(()) => {
                        let len = u32::from_be_bytes(len_buf) as usize;
                        // 对异常大的长度值做保护（防止损坏的长度前缀分配过大内存）
                        if len > 100 * 1024 * 1024 {
                            // 100MB 上限
                            warn!(
                                "WAL 文件 {:?} 中读取到异常长度 {} 字节，可能文件已损坏，跳过剩余部分",
                                file_path, len
                            );
                            break;
                        }
                        let mut data = vec![0u8; len];
                        match file.read_exact(&mut data) {
                            Ok(()) => {
                                if let Ok(entry) = self.deserialize_entry(&data) {
                                    entries.push(entry);
                                } else {
                                    warn!(
                                        "WAL 文件 {:?} 中反序列化条目失败，跳过该条目",
                                        file_path
                                    );
                                }
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                                // 数据部分不完整（崩溃时部分写入），跳过该条目不继续读
                                warn!(
                                    "WAL 文件 {:?} 中条目数据不完整（部分写入），跳过剩余部分",
                                    file_path
                                );
                                break;
                            }
                            Err(e) => {
                                return Err(TsError::Io(e));
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // 长度前缀不完整，文件正常结束
                        break;
                    }
                    Err(e) => {
                        return Err(TsError::Io(e));
                    }
                }
            }
        }

        Ok(entries)
    }

    /// 反序列化 WAL 条目
    fn deserialize_entry(&self, data: &[u8]) -> Result<WalEntry> {
        let mut cursor = std::io::Cursor::new(data);

        let entry_type_val = cursor.read_u8().map_err(|e| TsError::Io(e))?;
        let entry_type = WalEntryType::from_u8(entry_type_val)
            .ok_or_else(|| TsError::Unknown(format!("无效的 WAL 条目类型: {}", entry_type_val)))?;

        let tx_id = cursor.read_u64::<BigEndian>().map_err(|e| TsError::Io(e))?;
        let graph_id = cursor.read_u32::<BigEndian>().map_err(|e| TsError::Io(e))?;
        let timestamp = cursor.read_u64::<BigEndian>().map_err(|e| TsError::Io(e))?;

        // 读取 key
        let key_len = cursor.read_u32::<BigEndian>().map_err(|e| TsError::Io(e))?;
        let key = if key_len == 0xFFFFFFFF {
            None
        } else {
            let mut k = vec![0u8; key_len as usize];
            cursor.read_exact(&mut k).map_err(|e| TsError::Io(e))?;
            Some(k)
        };

        // 读取 value
        let value_len = cursor.read_u32::<BigEndian>().map_err(|e| TsError::Io(e))?;
        let value = if value_len == 0xFFFFFFFF {
            None
        } else {
            let mut v = vec![0u8; value_len as usize];
            cursor.read_exact(&mut v).map_err(|e| TsError::Io(e))?;
            Some(v)
        };

        // 读取 old_value
        let old_val_len = cursor.read_u32::<BigEndian>().map_err(|e| TsError::Io(e))?;
        let old_value = if old_val_len == 0xFFFFFFFF {
            None
        } else {
            let mut ov = vec![0u8; old_val_len as usize];
            cursor.read_exact(&mut ov).map_err(|e| TsError::Io(e))?;
            Some(ov)
        };

        Ok(WalEntry {
            entry_type,
            tx_id,
            graph_id,
            key,
            value,
            old_value,
            timestamp,
        })
    }

    /// 创建检查点 - 标记已安全持久化的日志位置
    pub fn create_checkpoint(&self) -> Result<()> {
        let mut checkpoint = self.last_checkpoint.lock().unwrap();
        let seq_no = *self.current_seq_no.lock().unwrap();
        *checkpoint = seq_no;

        // 写入检查点标记
        let mut file_guard = self.current_file.lock().unwrap();
        if let Some(ref mut file) = *file_guard {
            // 写入检查点元数据
            file.write_all(b"CHECKPOINT").map_err(|e| TsError::Io(e))?;

            if self.enable_fsync {
                file.sync_all().map_err(|e| TsError::Io(e))?;
            }
        }

        info!("WAL 检查点已创建: seq_no={}", seq_no);
        Ok(())
    }

    /// 获取当前检查点位置
    pub fn get_checkpoint(&self) -> u64 {
        *self.last_checkpoint.lock().unwrap()
    }

    /// 强制刷入所有数据
    pub fn flush(&self) -> Result<()> {
        let mut file_guard = self.current_file.lock().unwrap();
        if let Some(ref mut file) = *file_guard {
            file.flush().map_err(|e| TsError::Io(e))?;
            if self.enable_fsync {
                file.sync_all().map_err(|e| TsError::Io(e))?;
            }
        }
        Ok(())
    }

    /// 获取 WAL 文件大小
    pub fn wal_size(&self) -> Result<u64> {
        let mut total = 0u64;
        let wal_dir_entries = fs::read_dir(&self.wal_dir).map_err(|e| TsError::Io(e))?;

        for entry in wal_dir_entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total += metadata.len();
            }
        }

        Ok(total)
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        if let Ok(mut file_guard) = self.current_file.lock() {
            if let Some(ref mut file) = *file_guard {
                let _ = file.flush();
                if self.enable_fsync {
                    let _ = file.sync_all();
                }
            }
        }
    }
}
