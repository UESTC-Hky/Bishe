# Store Sever Exerice简介

Store Server对应于Foundation DB论文（https://www.foundationdb.org/files/fdb-paper.pdf）中的ss部分

它负责提供下面两个服务：
- log2storage: 向LogServer不断发起RPC请求获取日志，并将获得的日志持久化入Rocksdb
- RPC：对上层暴露读接口（Get/MultiGet等）

本部分练习，需要完成部分均已使用todo宏标注：

## Part-1 RocksDBStorage的实现
- 文件：src/ss/src/storage.rs
- 目标：掌握RocksDB的基本使用，我们使用RocksDB作为事务日志及图数据的存储引擎。
- 测试：本部分需要参考test_get，自己完善并撰写相关测试。

## Part 2 实现log2storage的核心逻辑
- 文件：src/ss/src/log2storage.rs
- 目标：理解log2storage流程（拉取日志），掌握异步编程的基本使用
- 测试：须通过测试，必要时补充更多的测试用例。

## Part-3 StorageServerImpl的实现 
- 文件：src/ss/src/service.rs
- 目标：掌握rpc服务的具体实现。

## Part-4 StoreServerManager的实现 
- 文件：src/ss/src/lib.rs
- 目标：掌握服务启动流程, 理解Storage Server整体结构



## 提示

- 测试代码（如果有）通常能帮助理解需求

## 额外挑战

- 目前log2stroage服务通过硬编码模拟日志拉取，可以自己编写一个简单的SimpleLogServer实现真正的RPC拉取