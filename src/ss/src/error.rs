use thiserror::Error;
use std::fmt;

pub type Result<T> = std::result::Result<T, SsError>;

/// 统一错误位置信息结构
#[derive(Debug, Clone, Copy)]
pub struct ErrorLocation {
    file: &'static str,
    line: u32,
    column: u32,
}

impl ErrorLocation {
    #[inline]
    pub fn new(file: &'static str, line: u32, column: u32) -> Self {
        Self { file, line, column }
    }

    /// 获取当前位置的快捷方法
    #[inline]
    pub fn here() -> Self {
        Self::new(file!(), line!(), column!())
    }
}


impl fmt::Display for ErrorLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

#[derive(Debug, Error)]
pub enum SsError {
    #[error("IO操作失败 @ {location}: {source}")]
    Io {
        #[source]
        source: std::io::Error,
        location: ErrorLocation,
    },

    // RocksDB相关错误
    #[error("列族未找到 @ {location}: {name}")]
    ColumnFamilyNotFound {
        name: String,
        location: ErrorLocation,
    },

    #[error("RocksDB操作失败 @ {location}: {source}")]
    RocksDB {
        #[source]
        source: rocksdb::Error,
        location: ErrorLocation,
    },

    // 异步任务错误
    #[error("异步任务执行失败 @ {location}: {source}")]
    JoinError {
        #[source]
        source: tokio::task::JoinError,
        location: ErrorLocation,
    },
}

// 自动转换实现
impl From<std::io::Error> for SsError {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            source,
            location: ErrorLocation::here(),
        }
    }
}

impl From<rocksdb::Error> for SsError {
    fn from(source: rocksdb::Error) -> Self {
        Self::RocksDB {
            source,
            location: ErrorLocation::here(),
        }
    }
}

impl From<tokio::task::JoinError> for SsError {
    fn from(source: tokio::task::JoinError) -> Self {
        Self::JoinError {
            source,
            location: ErrorLocation::here(),
        }
    }
}

macro_rules! ss_error {
    // 处理需要自动添加位置的错误类型
    ($variant:ident { $($field:ident : $value:expr),+ $(,)? }) => {
        SsError::$variant {
            $($field: $value),+,
            location: ErrorLocation::here(),
        }
    };

    // 直接包装错误源
    ($source:expr => $variant:ident) => {
        SsError::$variant {
            source: $source,
            location: ErrorLocation::here(),
        }
    };
}