# MVCC 迭代器与存储设计说明

本文档介绍了基于 RocksDB 的 **MVCC（多版本并发控制）迭代器设计**，包括其基本行为、版本编码规则、边界行为说明，以及事务性存储格式的设计与接口。

## 总览

本系统中的 Storage Server 提供一个支持事务的键值存储系统，底层基于 **RocksDB** 实现。它从 Log Server 接收写日志，将数据写入 RocksDB，并向上层 Executor 层提供读取接口（例如 `Get` / `MultiGet` ）。通过对 Key 编码引入版本信息，实现了多版本数据的有效管理。


## 存储格式设计

### 键的编码方式
   Key在存储引擎中的编码格式如下：
   +----------------+-----------------+
   | Data           | Version(encoded)|
   +----------------+-----------------+
   
   Version：取反编码，以满足顺序要求
   - 首先将版本号按位取反（为了让版本号越大，编码后越靠前）；
   - 再使用大端方式将其编码为 8 字节整数。

基于上述编码方式，同一个逻辑 Key 的多个版本会按从新到旧的顺序存储在 RocksDB 中。


### 示例：

```
Key1_Version10 => Value
Key1_Version8 => Value
Key1_Version5 => Value
Key2_Version6 => Value
Key2_Version3 => Value
...
```

### 查询行为示例：

- `get(key1, version=7)` → 命中 `Key1_Version5`
- `get(key2, version=6)` → 命中 `Key2_Version6`

也就是说，总是返回小于等于目标版本号的最新一条记录。
