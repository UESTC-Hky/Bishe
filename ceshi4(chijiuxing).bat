@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID 测试4：持久性 (Durability)
echo      事务提交后数据永久保存，即使故障也不丢失
echo ============================================================
echo.

echo [测试 4.1] 提交后数据稳定可读
echo   操作：写入 3 个键各自提交，连续 3 轮读取验证一致性
cargo run --bin ce add 1 dr_k1 val_one
cargo run --bin ce add 1 dr_k2 val_two
cargo run --bin ce add 1 dr_k3 val_three
echo ---------- 第 1 轮读取 ----------
cargo run --bin ce read 1 dr_k1
cargo run --bin ce read 1 dr_k2
cargo run --bin ce read 1 dr_k3
echo ---------- 第 2 轮读取 ----------
cargo run --bin ce read 1 dr_k1
cargo run --bin ce read 1 dr_k2
cargo run --bin ce read 1 dr_k3
echo ---------- 第 3 轮读取 ----------
cargo run --bin ce read 1 dr_k1
cargo run --bin ce read 1 dr_k2
cargo run --bin ce read 1 dr_k3
echo [预期] 三轮输出完全一致：val_one, val_two, val_three
echo.

echo [测试 4.2] 删除的持久性
echo   操作：写 dr_del=todelete -提交 -删 dr_del -提交 -连续 3 轮读
cargo run --bin ce add 1 dr_del todelete
cargo run --bin ce delete 1 dr_del
echo ---------- 第 1 轮读取 ----------
cargo run --bin ce read 1 dr_del
echo ---------- 第 2 轮读取 ----------
cargo run --bin ce read 1 dr_del
echo ---------- 第 3 轮读取 ----------
cargo run --bin ce read 1 dr_del
echo [预期] 三轮均返回"不存在"（删除结果持久）
echo.

echo [测试 4.3] 多键事务持久
echo   操作：一个事务写 3 个键，连续 3 轮读取
cargo run --bin ce add 1 dr_multi_a apple + add dr_multi_b banana + add dr_multi_c cherry
echo ---------- 第 1 轮读取 ----------
cargo run --bin ce read 1 dr_multi_a
cargo run --bin ce read 1 dr_multi_b
cargo run --bin ce read 1 dr_multi_c
echo ---------- 第 2 轮读取 ----------
cargo run --bin ce read 1 dr_multi_a
cargo run --bin ce read 1 dr_multi_b
cargo run --bin ce read 1 dr_multi_c
echo ---------- 第 3 轮读取 ----------
cargo run --bin ce read 1 dr_multi_a
cargo run --bin ce read 1 dr_multi_b
cargo run --bin ce read 1 dr_multi_c
echo [预期] 三轮输出完全一致：apple, banana, cherry
echo.

echo [测试 4.4] WAL 崩溃恢复测试
echo   操作：写入 dr_recover=survive 并提交；
echo         然后需要手动重启 TS 服务以模拟故障恢复
cargo run --bin ce add 1 dr_recover survive
echo.
echo -----------------------------------------------------------
echo [验证] 提交成功，读 dr_recover:
cargo run --bin ce read 1 dr_recover
echo [预期] 读到 "survive"
echo.
echo +----------------------------------------------------------+
echo ^|  !! 请在另一个终端中执行以下操作：                       ^|
echo ^|                                                          ^|
echo ^|  1. 按 Ctrl+C 停止当前 TS 进程                           ^|
echo ^|  2. 重新运行: cargo run --bin ts                         ^|
echo ^|                                                          ^|
echo ^|  模拟系统故障后重启，验证 WAL 恢复机制                    ^|
echo +----------------------------------------------------------+
echo.
pause
echo.
echo -----------------------------------------------------------
echo [验证] 重启后，读 dr_recover:
cargo run --bin ce read 1 dr_recover
echo [预期] 读到 "survive"（WAL 恢复成功，数据不丢失）
echo.
echo [验证] 重启后，读已存在的其他键:
cargo run --bin ce read 1 dr_k1
echo [预期] 读到 "val_one"（之前提交的所有数据均恢复）
cargo run --bin ce read 1 dr_multi_a
echo [预期] 读到 "apple"（多键事务数据也恢复）
echo.

echo ============================================================
echo      持久性测试完成
echo      请检查上述每个 [预期] 是否与实际输出一致
echo ============================================================
echo.
pause