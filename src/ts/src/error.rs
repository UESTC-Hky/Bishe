use thiserror::Error;

pub type Result<T> = std::result::Result<T, TsError>;

#[derive(Debug, Error)]
pub enum TsError {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("gRPC错误: {0}")]
    Grpc(String),
    
    #[error("事务未找到: {0}")]
    TransactionNotFound(u64),
    
    #[error("事务已提交: {0}")]
    TransactionAlreadyCommitted(u64),
    
    #[error("事务已回滚: {0}")]
    TransactionAlreadyRolledBack(u64),
    
    #[error("版本冲突: 期望 {expected}, 实际 {actual}")]
    VersionConflict {
        expected: u64,
        actual: u64,
    },
    
    #[error("读取冲突: {0}")]
    ReadConflict(String),
    
    #[error("写冲突: {0}")]
    WriteConflict(String),
    
    #[error("超时: {0}")]
    Timeout(String),
    
    #[error("无效参数: {0}")]
    InvalidArgument(String),
    
    #[error("LS错误: {0}")]
    LsError(String),
    
    #[error("SS错误: {0}")]
    SsError(String),
    
    #[error("序列化错误: {0}")]
    Serialization(String),
    
    #[error("未知错误: {0}")]
    Unknown(String),
}

impl From<tonic::Status> for TsError {
    fn from(status: tonic::Status) -> Self {
        TsError::Grpc(status.message().to_string())
    }
}

impl From<tonic::transport::Error> for TsError {
    fn from(err: tonic::transport::Error) -> Self {
        TsError::Grpc(err.to_string())
    }
}