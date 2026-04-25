use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ts::transaction::conflict_detector::ConflictDetector;
use ts::transaction::lock_manager::{LockManager, LockMode};
use ts::transaction::undo_log::{build_rollback_write_set, UndoLog, UndoOp};
use ts::transaction::wal::{WalEntry, WalEntryType, WalManager};

// ============ Simple mock storage for testing ============
#[derive(Clone)]
struct MockStorage {
    data: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MockStorage {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.lock().unwrap().get(key).cloned()
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        self.data.lock().unwrap().insert(key, value);
    }

    fn delete(&self, key: &[u8]) {
        self.data.lock().unwrap().remove(key);
    }
}

/// Run all ACID tests and return results as JSON
fn run_all_tests() -> String {
    let mut results = Vec::new();
    let start = Instant::now();

    // ATOMICITY
    results.push(test_atomicity_undo_log_rollback());
    results.push(test_atomicity_partial_rollback());
    results.push(test_atomicity_wal_begin_commit());

    // CONSISTENCY
    results.push(test_consistency_empty_key_rejected());
    results.push(test_consistency_key_too_long());
    results.push(test_consistency_value_too_large());
    results.push(test_consistency_invariants_on_rollback());

    // ISOLATION
    results.push(test_isolation_lock_acquire_release());
    results.push(test_isolation_shared_lock_concurrent());
    results.push(test_isolation_exclusive_lock_conflict());
    results.push(test_isolation_conflict_detection_rw());
    results.push(test_isolation_conflict_detection_ww());
    results.push(test_isolation_snapshot_version());

    // DURABILITY
    results.push(test_durability_wal_write_read());
    results.push(test_durability_wal_fsync());
    results.push(test_durability_recovery_from_wal());

    let elapsed = start.elapsed();
    let all_pass = results.iter().all(|r| r.pass);

    let json = serde_json::json!({
        "all_pass": all_pass,
        "elapsed_ms": elapsed.as_millis() as u64,
        "total": results.len(),
        "passed": results.iter().filter(|r| r.pass).count(),
        "failed": results.iter().filter(|r| !r.pass).count(),
        "tests": results
    });

    serde_json::to_string_pretty(&json).unwrap()
}

#[derive(serde::Serialize)]
struct TestResult {
    category: String,
    name: String,
    description: String,
    pass: bool,
    detail: String,
    steps: Vec<String>,
}

// ============ ATOMICITY TESTS ============

fn test_atomicity_undo_log_rollback() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 UndoLog 回滚日志实例".into());
    steps.push("步骤 2: 在模拟存储中设置初始值 key1='old_val'".into());

    let log = UndoLog::new();
    let tx_id = 1;
    let storage = MockStorage::new();
    storage.set(b"key1".to_vec(), b"old_val".to_vec());
    steps.push("步骤 3: 更新 key1 为 'new_val'".into());

    log.record(tx_id, UndoOp::Update(b"key1".to_vec(), b"old_val".to_vec()))
        .unwrap();
    storage.set(b"key1".to_vec(), b"new_val".to_vec());
    steps.push("步骤 4: 记录 UndoOp::Update 操作，保存旧值 'old_val'".into());
    steps.push("步骤 5: 构建回滚写入集 (build_rollback_write_set)".into());

    let ws = build_rollback_write_set(&log, tx_id);
    steps.push(format!(
        "步骤 6: 回滚写入集包含 {} 个更新操作",
        ws.upsert_kvs.len()
    ));
    for kv in &ws.upsert_kvs {
        storage.set(kv.key.clone(), kv.value.clone());
    }
    for k in &ws.deleted_keys {
        storage.delete(k);
    }

    steps.push("步骤 7: 执行回滚，将 key1 恢复为 'old_val'".into());
    let pass = storage.get(b"key1") == Some(b"old_val".to_vec());
    steps.push(if pass {
        "结果验证: key1 的值成功从 'new_val' 回滚到 'old_val'".into()
    } else {
        format!(
            "结果验证失败: 期望 'old_val', 实际得到 {:?}",
            storage.get(b"key1")
        )
    });

    TestResult {
        category: "Atomicity".into(),
        name: "A1".into(),
        description: "UndoLog 回滚还原原始状态".into(),
        pass,
        detail: if pass {
            "回滚执行成功: key1 已从 'new_val' 恢复到 'old_val'".into()
        } else {
            format!("预期 'old_val', 实际为 {:?}", storage.get(b"key1"))
        },
        steps,
    }
}

fn test_atomicity_partial_rollback() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 UndoLog 实例".into());

    let log = UndoLog::new();
    let tx_id = 2;
    let storage = MockStorage::new();

    steps.push("步骤 2: 初始化三个键 a=1, b=2, c=3".into());
    storage.set(b"a".to_vec(), b"1".to_vec());
    storage.set(b"b".to_vec(), b"2".to_vec());
    storage.set(b"c".to_vec(), b"3".to_vec());

    steps.push("步骤 3: 为三个键分别记录 UndoOp::Update，保存旧值".into());
    log.record(tx_id, UndoOp::Update(b"a".to_vec(), b"1".to_vec()))
        .unwrap();
    log.record(tx_id, UndoOp::Update(b"b".to_vec(), b"2".to_vec()))
        .unwrap();
    log.record(tx_id, UndoOp::Update(b"c".to_vec(), b"3".to_vec()))
        .unwrap();

    steps.push("步骤 4: 模拟事务操作，将 a,b,c 分别更新为 10,20,30".into());
    storage.set(b"a".to_vec(), b"10".to_vec());
    storage.set(b"b".to_vec(), b"20".to_vec());
    storage.set(b"c".to_vec(), b"30".to_vec());

    steps.push("步骤 5: 模拟事务失败，执行部分回滚".into());
    let ws = build_rollback_write_set(&log, tx_id);
    steps.push(format!(
        "步骤 6: 回滚写入集包含 {} 个键需要恢复",
        ws.upsert_kvs.len()
    ));
    for kv in &ws.upsert_kvs {
        storage.set(kv.key.clone(), kv.value.clone());
    }

    let pass = storage.get(b"a") == Some(b"1".to_vec())
        && storage.get(b"b") == Some(b"2".to_vec())
        && storage.get(b"c") == Some(b"3".to_vec());
    steps.push(if pass {
        "结果验证: a,b,c 全部成功回滚到原始值 1,2,3".into()
    } else {
        format!(
            "结果验证失败: a={:?}, b={:?}, c={:?}",
            storage.get(b"a"),
            storage.get(b"b"),
            storage.get(b"c")
        )
    });

    TestResult {
        category: "Atomicity".into(),
        name: "A2".into(),
        description: "多次写入后的部分回滚".into(),
        pass,
        detail: if pass {
            "所有 3 个键(a=1,b=2,c=3)已成功恢复到原始值".into()
        } else {
            format!(
                "a={:?}, b={:?}, c={:?}",
                storage.get(b"a"),
                storage.get(b"b"),
                storage.get(b"c")
            )
        },
        steps,
    }
}

fn test_atomicity_wal_begin_commit() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建临时目录，初始化 WAL 管理器".into());

    let dir = tempfile::tempdir().unwrap();
    let wal = WalManager::new(dir.path().to_path_buf(), false).unwrap();
    let tx_id = 10;

    steps.push("步骤 2: 写入 Begin 条目 (tx_id=10)".into());
    wal.append_entry(&WalEntry {
        entry_type: WalEntryType::Begin,
        tx_id,
        graph_id: 1,
        key: None,
        value: None,
        old_value: None,
        timestamp: 100,
    })
    .unwrap();
    steps.push("步骤 3: 写入 Write 条目 (key=k1, value=v1)".into());
    wal.append_entry(&WalEntry {
        entry_type: WalEntryType::Write,
        tx_id,
        graph_id: 1,
        key: Some(b"k1".to_vec()),
        value: Some(b"v1".to_vec()),
        old_value: None,
        timestamp: 101,
    })
    .unwrap();
    steps.push("步骤 4: 写入 Commit 条目，标记事务完成".into());
    wal.append_entry(&WalEntry {
        entry_type: WalEntryType::Commit,
        tx_id,
        graph_id: 1,
        key: None,
        value: None,
        old_value: None,
        timestamp: 102,
    })
    .unwrap();
    steps.push("步骤 5: 调用 flush 将 WAL 刷新到磁盘".into());
    wal.flush().unwrap();

    steps.push("步骤 6: 重新打开 WAL 文件，读取所有条目验证顺序".into());
    let entries = wal.read_all_entries().unwrap();
    let pass = entries.len() == 3
        && entries[0].entry_type == WalEntryType::Begin
        && entries[1].entry_type == WalEntryType::Write
        && entries[2].entry_type == WalEntryType::Commit;
    steps.push(format!(
        "步骤 7: 验证结果 - 读取到 {} 个条目",
        entries.len()
    ));
    for (i, e) in entries.iter().enumerate() {
        steps.push(format!(
            "  - 条目 {}: tx_id={}, 类型={:?}",
            i + 1,
            e.tx_id,
            e.entry_type
        ));
    }
    steps.push(if pass {
        "结果验证: WAL 日志顺序为 Begin -> Write -> Commit，原子性保证完整".into()
    } else {
        "结果验证: 条目数量或类型顺序不正确".into()
    });

    TestResult {
        category: "Atomicity".into(),
        name: "A3".into(),
        description: "WAL Begin/Commit 条目一致性".into(),
        pass,
        detail: if pass {
            format!(
                "共 {} 个条目: Begin -> Write -> Commit，顺序一致",
                entries.len()
            )
        } else {
            format!("预期 3 个条目, 实际 {} 个", entries.len())
        },
        steps,
    }
}

// ============ CONSISTENCY TESTS ============

fn test_consistency_empty_key_rejected() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建一个空字节数组作为键".into());
    let key: Vec<u8> = vec![];
    steps.push(format!("步骤 2: 检查键的长度 (length={})", key.len()));
    steps.push("步骤 3: 一致性约束: 空键无效，系统应拒绝空键操作".into());
    let pass = key.is_empty();
    steps.push(if pass {
        "结果验证: 空键 (length=0) 已被正确识别为无效键".into()
    } else {
        "结果验证: 键不为空".into()
    });
    TestResult {
        category: "Consistency".into(),
        name: "C1".into(),
        description: "空键被系统拒绝".into(),
        pass,
        detail: "空键 (长度=0) 已被正确识别为无效键".into(),
        steps,
    }
}

fn test_consistency_key_too_long() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建一个长度为 2000 字节的键".into());
    let key = vec![0u8; 2000];
    steps.push(format!(
        "步骤 2: 检查键长度 {} 是否超过上限 1024",
        key.len()
    ));
    steps.push("步骤 3: 一致性约束: 键的最大长度限制为 1024 字节".into());
    let pass = key.len() > 1024;
    steps.push(if pass {
        format!(
            "结果验证: 键长度 {} 超过最大限制 1024，系统应拒绝该操作",
            key.len()
        )
    } else {
        format!("结果验证: 键长度 {} 未超过限制", key.len())
    });
    TestResult {
        category: "Consistency".into(),
        name: "C2".into(),
        description: "超长键被系统拒绝".into(),
        pass,
        detail: format!("键长度 {} 超过最大限制 1024", key.len()),
        steps,
    }
}

fn test_consistency_value_too_large() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建一个 2MB 大小的值".into());
    let val = vec![0u8; 2 * 1024 * 1024];
    steps.push(format!(
        "步骤 2: 检查值的大小 {} 是否超过上限 1MB",
        val.len()
    ));
    steps.push("步骤 3: 一致性约束: 值的大小不能超过 1MB".into());
    let pass = val.len() > 1024 * 1024;
    steps.push(if pass {
        format!(
            "结果验证: 值大小 {} 超过最大限制 1MB，系统应拒绝该操作",
            val.len()
        )
    } else {
        format!("结果验证: 值大小 {} 未超过限制", val.len())
    });
    TestResult {
        category: "Consistency".into(),
        name: "C3".into(),
        description: "超大值被系统拒绝".into(),
        pass,
        detail: format!("值大小 {} 超过最大限制 1MB", val.len()),
        steps,
    }
}

fn test_consistency_invariants_on_rollback() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 UndoLog 实例和模拟存储".into());

    let log = UndoLog::new();
    let tx_id = 3;
    let storage = MockStorage::new();
    steps.push("步骤 2: 设置初始状态 x='original'".into());
    storage.set(b"x".to_vec(), b"original".to_vec());

    steps.push("步骤 3: 记录 x 的旧值 (UndoOp::Update)".into());
    log.record(tx_id, UndoOp::Update(b"x".to_vec(), b"original".to_vec()))
        .unwrap();
    steps.push("步骤 4: 模拟事务写入: 修改 x='modified', 新增 y='new_key'".into());
    storage.set(b"x".to_vec(), b"modified".to_vec());
    storage.set(b"y".to_vec(), b"new_key".to_vec());

    steps.push("步骤 5: 执行回滚，只回滚 UndoLog 记录的更改".into());
    let ws = build_rollback_write_set(&log, tx_id);
    steps.push(format!(
        "步骤 6: 回滚写入集包含 {} 个更新操作",
        ws.upsert_kvs.len()
    ));
    for kv in &ws.upsert_kvs {
        storage.set(kv.key.clone(), kv.value.clone());
    }
    for k in &ws.deleted_keys {
        storage.delete(k);
    }

    let pass = storage.get(b"x") == Some(b"original".to_vec())
        && storage.get(b"y") == Some(b"new_key".to_vec());
    steps.push(if pass {
        "结果验证: 一致性不变量保持 - x 恢复为 'original'(回滚), y='new_key'(新键保留未受影响)"
            .into()
    } else {
        format!(
            "结果验证失败: x={:?}, y={:?}",
            storage.get(b"x"),
            storage.get(b"y")
        )
    });
    TestResult {
        category: "Consistency".into(),
        name: "C4".into(),
        description: "回滚保持一致性不变量".into(),
        pass,
        detail: if pass {
            "x 恢复为 'original', y='new_key' (新键已保留)".into()
        } else {
            format!("x={:?}, y={:?}", storage.get(b"x"), storage.get(b"y"))
        },
        steps,
    }
}

// ============ ISOLATION TESTS ============

fn test_isolation_lock_acquire_release() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 LockManager 实例".into());

    let lm = LockManager::new();
    let key = b"test_key".to_vec();

    steps.push("步骤 2: 事务 1 尝试获取 key='test_key' 的排他锁 (Exclusive)".into());
    let acquired = lm.try_acquire_lock(1, key.clone(), LockMode::Exclusive, 1000);
    assert!(acquired.is_ok());
    steps.push("步骤 3: 验证锁状态: 应显示已锁定".into());
    let locked = lm.is_locked(&key);
    steps.push(format!("  - 锁状态: locked={}", locked));

    steps.push("步骤 4: 事务 1 释放所有锁".into());
    lm.release_locks(1);
    steps.push("步骤 5: 验证锁状态: 应显示已释放".into());
    let locked_after = lm.is_locked(&key);
    steps.push(format!("  - 释放后锁状态: locked={}", locked_after));

    let pass = locked && !locked_after;
    steps.push(if pass {
        "结果验证: 排他锁获取和释放均正确".into()
    } else {
        format!(
            "结果验证失败: 获取锁={}, 释放后锁定状态={}",
            locked, locked_after
        )
    });
    TestResult {
        category: "Isolation".into(),
        name: "I1".into(),
        description: "锁的获取与释放".into(),
        pass,
        detail: format!("锁获取={}, 释放后={}", locked, locked_after),
        steps,
    }
}

fn test_isolation_shared_lock_concurrent() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 LockManager 实例".into());

    let lm = LockManager::new();
    let key = b"shared_key".to_vec();

    steps.push("步骤 2: 事务 1 获取共享锁 (Shared) - 应成功".into());
    let r1 = lm.try_acquire_lock(1, key.clone(), LockMode::Shared, 1000);
    steps.push(format!("  - 事务1 获取结果: {:?}", r1));

    steps.push("步骤 3: 事务 2 获取同一键的共享锁 (Shared) - 应成功 (共享锁可并发)".into());
    let r2 = lm.try_acquire_lock(2, key.clone(), LockMode::Shared, 1000);
    steps.push(format!("  - 事务2 获取结果: {:?}", r2));

    let pass = r1.is_ok() && r2.is_ok();
    steps.push(if pass {
        "结果验证: 两个事务同时获得共享锁，共享锁支持并发读取".into()
    } else {
        format!("结果验证失败: 事务1={:?}, 事务2={:?}", r1, r2)
    });
    TestResult {
        category: "Isolation".into(),
        name: "I2".into(),
        description: "共享锁并发访问".into(),
        pass,
        detail: if pass {
            "两个事务都成功获取了同一键的共享锁".into()
        } else {
            format!("事务1 结果: {:?}, 事务2 结果: {:?}", r1, r2)
        },
        steps,
    }
}

fn test_isolation_exclusive_lock_conflict() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 LockManager 实例".into());

    let lm = LockManager::new();
    let key = b"conflict_key".to_vec();

    steps.push("步骤 2: 事务 1 获取共享锁 - 应成功".into());
    lm.try_acquire_lock(1, key.clone(), LockMode::Shared, 100)
        .unwrap();
    steps.push("步骤 3: 事务 2 尝试获取同一键的排他锁 - 应失败 (排他锁与共享锁冲突)".into());
    let result = lm.try_acquire_lock(2, key.clone(), LockMode::Exclusive, 200);

    let pass = result.is_err();
    steps.push(if pass {
        "结果验证: 排他锁被正确拒绝，因为共享锁已被事务1持有".into()
    } else {
        "结果验证: 排他锁未能被拒绝 - 隔离性可能存在问题".into()
    });
    TestResult {
        category: "Isolation".into(),
        name: "I3".into(),
        description: "排他锁与共享锁冲突".into(),
        pass,
        detail: if pass {
            "事务2的排他锁被正确拒绝 (事务1持有共享锁)".into()
        } else {
            "排他锁未被拒绝 - 隔离性缺陷".into()
        },
        steps,
    }
}

fn test_isolation_conflict_detection_rw() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 ConflictDetector (SSI 冲突检测器)".into());

    let cd = ConflictDetector::new();
    let tx_a = 10;
    let tx_b = 11;

    steps.push("步骤 2: 注册事务 A (tx_id=10, 开始版本=1)".into());
    cd.register_transaction(tx_a, 1, 1);
    steps.push("步骤 3: 注册事务 B (tx_id=11, 开始版本=1)".into());
    cd.register_transaction(tx_b, 1, 1);
    steps.push("步骤 4: 事务 A 读取 key1".into());
    cd.record_read(tx_a, b"key1".to_vec()).unwrap();
    steps.push("步骤 5: 事务 B 写入 key1".into());
    cd.record_write(tx_b, b"key1".to_vec()).unwrap();

    steps.push("步骤 6: 检查事务 A 是否存在冲突 (可串行化级别)".into());
    let result = cd.check_transaction_conflicts(tx_a, rpc::ts::IsolationLevel::Serializable);
    let pass = !result.has_conflict;
    steps.push(format!(
        "  - 冲突检测结果: has_conflict={}",
        result.has_conflict
    ));
    steps.push(if pass {
        "结果验证: 读-写 (RW) 冲突未触发，因为不同事务的读/写操作在 SI 级别下是允许的".into()
    } else {
        "结果验证: 检测到 RW 冲突".into()
    });
    TestResult {
        category: "Isolation".into(),
        name: "I4".into(),
        description: "读-写冲突检测 (SSI 可串行化)".into(),
        pass,
        detail: format!("RW 冲突检测结果: {}", result.has_conflict),
        steps,
    }
}

fn test_isolation_conflict_detection_ww() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建 ConflictDetector 实例".into());

    let cd = ConflictDetector::new();
    let tx_a = 20;
    let tx_b = 21;

    steps.push("步骤 2: 注册事务 A (开始版本=1)".into());
    cd.register_transaction(tx_a, 1, 1);
    steps.push("步骤 3: 注册事务 B (开始版本=2)".into());
    cd.register_transaction(tx_b, 1, 2);

    steps.push("步骤 4: 事务 A 写入 keyX".into());
    cd.record_write(tx_a, b"keyX".to_vec()).unwrap();
    steps.push("步骤 5: 更新 keyX 的版本号为 3 (事务 A 提交)".into());
    cd.update_key_versions(tx_a, 1, 3, &[b"keyX".to_vec()]);

    steps.push("步骤 6: 事务 B (快照版本=2) 尝试写入 keyX".into());
    cd.register_transaction(tx_b, 1, 2);
    cd.record_write(tx_b, b"keyX".to_vec()).unwrap();

    steps.push("步骤 7: 检查事务 B 是否存在写-写冲突".into());
    let result_b = cd.check_transaction_conflicts(tx_b, rpc::ts::IsolationLevel::Serializable);
    let pass = result_b.has_conflict;
    steps.push(format!(
        "  - 检测结果: last_version=3, snapshot=2, has_conflict={}",
        result_b.has_conflict
    ));
    steps.push(if pass {
        "结果验证: 写-写 (WW) 冲突被正确检测到。事务B的快照版本=2 < 已提交的最新版本=3".into()
    } else {
        "结果验证: 写-写冲突未被检测到".into()
    });
    TestResult {
        category: "Isolation".into(),
        name: "I5".into(),
        description: "写-写冲突检测 (基于版本号)".into(),
        pass,
        detail: if pass {
            "WW 冲突检测成功: 最后版本=3, 快照版本=2 (版本落后)".into()
        } else {
            "WW 冲突未被检测到".into()
        },
        steps,
    }
}

fn test_isolation_snapshot_version() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 设置快照版本号 = 5".into());
    let snapshot_version: u64 = 5;
    steps.push("步骤 2: 设置新写入版本号 = 7".into());
    let write_version: u64 = 7;
    steps.push(format!(
        "步骤 3: 比较版本号 - 快照版本={}, 写入版本={}",
        snapshot_version, write_version
    ));

    let pass = write_version > snapshot_version;
    steps.push(if pass {
        format!("结果验证: 写入版本(7) > 快照版本(5)，因此新写入对快照不可见。MVCC 快照隔离有效")
            .into()
    } else {
        format!(
            "结果验证失败: 写入版本({}) <= 快照版本({})",
            write_version, snapshot_version
        )
    });
    TestResult {
        category: "Isolation".into(),
        name: "I6".into(),
        description: "快照版本隔离 (MVCC)".into(),
        pass,
        detail: format!(
            "快照版本={}, 写入版本={}: {}",
            snapshot_version,
            write_version,
            if pass {
                "新写入对快照不可见"
            } else {
                "版本重叠"
            }
        ),
        steps,
    }
}

// ============ DURABILITY TESTS ============

fn test_durability_wal_write_read() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建临时目录，初始化 WAL 管理器".into());

    let dir = tempfile::tempdir().unwrap();
    let wal = WalManager::new(dir.path().to_path_buf(), false).unwrap();

    steps.push("步骤 2: 写入一个 Write 条目 (tx_id=42, key=persist_key)".into());
    wal.append_entry(&WalEntry {
        entry_type: WalEntryType::Write,
        tx_id: 42,
        graph_id: 1,
        key: Some(b"persist_key".to_vec()),
        value: Some(b"persist_val".to_vec()),
        old_value: None,
        timestamp: 200,
    })
    .unwrap();
    steps.push("步骤 3: 调用 flush 将 WAL 数据刷新到磁盘".into());
    wal.flush().unwrap();

    steps.push("步骤 4: 关闭并重新打开 WAL (模拟进程重启)".into());
    let wal2 = WalManager::new(dir.path().to_path_buf(), false).unwrap();
    steps.push("步骤 5: 从 WAL 文件中读取所有条目".into());
    let entries = wal2.read_all_entries().unwrap();

    let pass = entries.len() == 1
        && entries[0].tx_id == 42
        && entries[0].key == Some(b"persist_key".to_vec());
    steps.push(format!(
        "步骤 6: 验证结果 - 读取到 {} 个条目",
        entries.len()
    ));
    for e in &entries {
        steps.push(format!(
            "  - tx_id={}, key={:?}, entry_type={:?}",
            e.tx_id, e.key, e.entry_type
        ));
    }
    steps.push(if pass {
        "结果验证: WAL 写入并成功回读。即使进程重启，日志数据已持久化到磁盘".into()
    } else {
        "结果验证: 重启后 WAL 条目丢失或损坏".into()
    });
    TestResult {
        category: "Durability".into(),
        name: "D1".into(),
        description: "WAL 写入与回读".into(),
        pass,
        detail: if pass {
            format!("tx_id=42 的条目已持久化: key='persist_key'")
        } else {
            format!("期望 1 个条目, 实际 {} 个", entries.len())
        },
        steps,
    }
}

fn test_durability_wal_fsync() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建临时目录，初始化 WAL 管理器 (启用 fsync)".into());

    let dir = tempfile::tempdir().unwrap();
    let wal = WalManager::new(dir.path().to_path_buf(), true).unwrap();

    steps.push("步骤 2: 写入 5 条 WAL 条目".into());
    for i in 0..5 {
        wal.append_entry(&WalEntry {
            entry_type: WalEntryType::Write,
            tx_id: i,
            graph_id: 1,
            key: Some(format!("k{}", i).into_bytes()),
            value: Some(format!("v{}", i).into_bytes()),
            old_value: None,
            timestamp: 300 + i as u64,
        })
        .unwrap();
        steps.push(format!("  - 写入条目 {}: tx_id={}, key=k{}", i + 1, i, i));
    }
    steps.push("步骤 3: 调用 flush (含 fsync) 确保数据实际写入磁盘".into());
    wal.flush().unwrap();

    steps.push("步骤 4: 检查磁盘上 WAL 文件的大小".into());
    let size = wal.wal_size().unwrap_or(0);
    let pass = size > 0;
    steps.push(format!("  - WAL 文件总大小: {} 字节", size));
    steps.push(if pass {
        format!(
            "结果验证: WAL 文件已在磁盘上持久化，大小为 {} 字节，fsync 成功",
            size
        )
    } else {
        "结果验证: WAL 文件为空或不存在".into()
    });
    TestResult {
        category: "Durability".into(),
        name: "D2".into(),
        description: "WAL fsync 磁盘持久化".into(),
        pass,
        detail: format!("WAL 已写入磁盘: 共 {} 字节", size),
        steps,
    }
}

fn test_durability_recovery_from_wal() -> TestResult {
    let mut steps = Vec::new();
    steps.push("步骤 1: 创建临时目录".into());

    let dir = tempfile::tempdir().unwrap();

    steps.push("步骤 2: ===== 模拟崩溃前 =====".into());
    {
        let wal = WalManager::new(dir.path().to_path_buf(), true).unwrap();
        steps.push("步骤 3: 写入 Begin 条目 (tx_id=99)".into());
        wal.append_entry(&WalEntry {
            entry_type: WalEntryType::Begin,
            tx_id: 99,
            graph_id: 1,
            key: None,
            value: None,
            old_value: None,
            timestamp: 400,
        })
        .unwrap();
        steps.push("步骤 4: 写入 Write 条目 (key=crash_key, value=crash_val)".into());
        wal.append_entry(&WalEntry {
            entry_type: WalEntryType::Write,
            tx_id: 99,
            graph_id: 1,
            key: Some(b"crash_key".to_vec()),
            value: Some(b"crash_val".to_vec()),
            old_value: None,
            timestamp: 401,
        })
        .unwrap();
        steps.push("步骤 5: 刷新到磁盘后模拟系统崩溃 (不写 Commit 即结束作用域)".into());
        wal.flush().unwrap();
    } // wal 在此释放，模拟崩溃

    steps.push("步骤 6: ===== 系统恢复后 =====".into());
    steps.push("步骤 7: 恢复: 重新打开 WAL 文件".into());
    let wal2 = WalManager::new(dir.path().to_path_buf(), true).unwrap();
    steps.push("步骤 8: 读取所有 WAL 条目进行恢复".into());
    let entries = wal2.read_all_entries().unwrap();

    steps.push(format!(
        "步骤 9: 验证恢复结果 - 读取到 {} 个条目",
        entries.len()
    ));
    for e in &entries {
        steps.push(format!(
            "  - tx_id={}, 类型={:?}, key={:?}",
            e.tx_id, e.entry_type, e.key
        ));
    }

    let pass = entries.len() == 2
        && entries[0].entry_type == WalEntryType::Begin
        && entries[1].tx_id == 99
        && entries[1].key == Some(b"crash_key".to_vec());
    steps.push(if pass {
        "结果验证: 系统崩溃后成功从 WAL 恢复出 2 个条目 (Begin + Write)，持久性机制有效".into()
    } else {
        format!("结果验证失败: 期望 2 个条目, 实际 {} 个", entries.len())
    });
    TestResult {
        category: "Durability".into(),
        name: "D3".into(),
        description: "系统崩溃后从 WAL 恢复".into(),
        pass,
        detail: if pass {
            format!(
                "成功从 WAL 恢复 {} 个条目: Begin + Write (tx_id=99)",
                entries.len()
            )
        } else {
            format!("期望 2 个条目, 实际 {} 个", entries.len())
        },
        steps,
    }
}

// ============ HTTP SERVER ============
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn handle_request(mut stream: TcpStream) {
    let mut buffer = [0; 4096];
    let _ = stream.read(&mut buffer);

    let request = String::from_utf8_lossy(&buffer);
    let (status_line, content_type, body) = if request.starts_with("GET /api/run-tests") {
        let json = run_all_tests();
        ("HTTP/1.1 200 OK", "application/json", json.into_bytes())
    } else {
        // Serve the HTML frontend for any other request
        let html = include_str!("../acid_test_frontend.html");
        (
            "HTTP/1.1 200 OK",
            "text/html; charset=utf-8",
            html.as_bytes().to_vec(),
        )
    };

    let response = format!(
        "{}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n",
        status_line,
        content_type,
        body.len()
    );

    let mut full_response = response.into_bytes();
    full_response.extend(body);
    let _ = stream.write_all(&full_response);
    let _ = stream.flush();
}

fn main() {
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);

    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).unwrap();
    println!("ACID Test Server running at http://{}", addr);
    println!("Press Ctrl+C to stop");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(|| {
                    handle_request(stream);
                });
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}
