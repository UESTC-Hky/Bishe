@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID 测试3：隔离性 (Isolation)
echo      并发事务互不干扰 — 快照隔离
echo ============================================================
echo.

echo [测试 3.1] 快照隔离：T1 读不到 T2 未提交的写
echo   操作流程：
echo     T1 开始(-unpost)，读 iso_key1，暂不提交
echo     T2 开始，写 iso_key1=T2_value，提交
echo     T3 读 iso_key1，应读到 T2_value
echo.
echo ---------- 准备数据 ----------
cargo run --bin ce add 1 iso_key1 original
echo.
echo ---------- T1 开始，读 iso_key1，不提交 ----------
cargo run --bin ce read 1 iso_key1 -unpost > "%TEMP%\iso_tx.txt" 2>&1
type "%TEMP%\iso_tx.txt"
echo.
echo ---------- 自动提取事务ID ----------
for /f "tokens=3 delims=: " %%a in ('findstr /c:"事务开始，ID:" "%TEMP%\iso_tx.txt"') do set "ISO_T1=%%a"
echo ---------- T2 写入 iso_key1=T2_value，提交 ----------
cargo run --bin ce add 1 iso_key1 T2_value
echo.
echo ---------- 自动提交 T1 (ID: %ISO_T1%) ----------
cargo run --bin ce commit %ISO_T1%
echo.
echo ---------- T3 读 iso_key1，应读到 T2_value ----------
cargo run --bin ce read 1 iso_key1
echo [预期] 读到 "T2_value"（T2 提交后数据可见）
del "%TEMP%\iso_tx.txt" 2>nul
echo.

echo ============================================================
echo      隔离性测试完成
echo      请检查上述每个 [预期] 是否与实际输出一致
echo ============================================================
echo.
pause