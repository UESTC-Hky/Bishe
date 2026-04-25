use thiserror::Error;

pub type Result<T> = std::result::Result<T, LsError>;

#[derive(Debug, Error)]
pub enum LsError {
    #[error("IO错误: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
    
    #[error("RocksDB错误: {source}")]
    RocksDB {
        #[from]
        source: rocksdb::Error,
    },
    
    #[error("版本不连续: 期望 {expected}, 实际 {actual}")]
    VersionMismatch {
        expected: u64,
        actual: u64,
    },
    
    #[error("gRPC传输错误: {source}")]
    Transport {
        #[from]
        source: tonic::transport::Error,
    },
    
    #[error("无效参数: {message}")]
    InvalidArgument {
        message: String,
    },

    #[error("地址解析错误: {message}")]
    AddrParseError {
        message: String,
    },

    #[error("Protobuf编码/解码错误: {source}")]
    ProstError {
        #[from]
        source: prost::DecodeError,
    },
}

impl LsError {
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument { message: message.into() }
    }
}

impl From<std::net::AddrParseError> for LsError {
    fn from(err: std::net::AddrParseError) -> Self {
        LsError::AddrParseError {
            message: err.to_string(),
        }
    }
}