use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsConfig {
    pub server: ServerConfig,
    pub ls: LsConfig,
    pub ss: SsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_ip")]
    pub ip: IpAddr,
    
    #[serde(default = "default_ts_port")]
    pub port: u16,
    
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,
    
    #[serde(default = "default_max_transactions")]
    pub max_transactions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsConfig {
    #[serde(default = "default_ip")]
    pub ip: IpAddr,
    
    #[serde(default = "default_ls_port")]
    pub port: u16,
    
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsConfig {
    #[serde(default = "default_ip")]
    pub ip: IpAddr,
    
    #[serde(default = "default_ss_port")]
    pub port: u16,
    
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

// 默认值函数
fn default_ip() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }
fn default_ts_port() -> u16 { 25003 }
fn default_ls_port() -> u16 { 25002 }
fn default_ss_port() -> u16 { 25001 }
fn default_worker_threads() -> usize { 4 }
fn default_max_transactions() -> usize { 1000 }
fn default_timeout_ms() -> u64 { 5000 }

impl Default for TsConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                ip: default_ip(),
                port: default_ts_port(),
                worker_threads: default_worker_threads(),
                max_transactions: default_max_transactions(),
            },
            ls: LsConfig {
                ip: default_ip(),
                port: default_ls_port(),
                timeout_ms: default_timeout_ms(),
            },
            ss: SsConfig {
                ip: default_ip(),
                port: default_ss_port(),
                timeout_ms: default_timeout_ms(),
            },
        }
    }
}

impl TsConfig {
    pub fn server_addr(&self) -> SocketAddr {
        SocketAddr::new(self.server.ip, self.server.port)
    }
    
    pub fn ls_addr(&self) -> String {
        format!("http://{}:{}", self.ls.ip, self.ls.port)
    }
    
    pub fn ss_addr(&self) -> String {
        format!("http://{}:{}", self.ss.ip, self.ss.port)
    }
}