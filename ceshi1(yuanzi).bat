@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID 测试1：原子性 (Atomicity)
echo      事务中所有操作要么全部完成，要么全部不完成
echo ============================================================
echo.

:: ============================================================
:: 1.1 多键原子提交
:: ============================================================
echo [测试 1.1] 多键原子提交
echo   操作：一个事务写入 K1=v1, K2=v2, K3=v3，提交后分别读取
echo ---------- 执行写入事务 ----------
cargo run --bin ce add 1 at_k1 v1 + add at_k2 v2 + add at_k3 v3
echo.
echo ---------- 独立事务分别读取验证 ----------
echo 读 at_k1:
cargo run --bin ce read 1 at_k1
echo 读 at_k2:
cargo run --bin ce read 1 at_k2
echo 读 at_k3:
cargo run --bin ce read 1 at_k3
echo [预期] 三个键全部存在，值分别为 v1, v2, v3
echo.

:: ============================================================
:: 1.2+1.3 未提交事务不可见 + 提交后可见
:: ============================================================
echo [测试 1.2] 未提交事务完全不可见
echo   操作：T1 写入 at_uncommit=secret 但不提交；T2、T3 分别读取
echo ---------- T1 开启，写入但不提交 (-unpost) ----------
cargo run --bin ce add 1 at_uncommit secret -unpost > "%TEMP%\ac_tx.txt" 2>&1
type "%TEMP%\ac_tx.txt"
echo.
echo ---------- 自动提取事务ID并暂存 ----------
for /f "tokens=3 delims=: " %%a in ('findstr /c:"事务开始，ID:" "%TEMP%\ac_tx.txt"') do set "AC_TXID=%%a"
echo.
echo ---------- T2 独立事务读 at_uncommit ----------
cargo run --bin ce read 1 at_uncommit
echo [预期] 返回"不存在"（未提交数据对其他事务不可见）
echo ---------- T3 独立事务读 at_uncommit ----------
cargo run --bin ce read 1 at_uncommit
echo [预期] 返回"不存在"（再次确认）
echo.

echo [测试 1.3] 提交后数据才可见
echo   操作：自动提交 T1，T4 读取验证
echo ---------- 提交 T1 (ID: %AC_TXID%) ----------
cargo run --bin ce commit %AC_TXID%
echo.
echo ---------- T4 独立事务读 at_uncommit ----------
cargo run --bin ce read 1 at_uncommit
echo [预期] 读到 "secret"（一旦提交，全部操作生效）
del "%TEMP%\ac_tx.txt" 2>nul
echo.

:: ============================================================
:: 1.4 事务内中间态不对外泄露
:: ============================================================
echo [测试 1.4] 事务内中间态不对外泄露
echo   操作：一个事务内 写 at_mid=v1 -删 at_mid -写 at_mid=v2，提交后读
cargo run --bin ce add 1 at_mid v1 + delete at_mid + add at_mid v2
echo.
echo ---------- 独立事务读 at_mid ----------
cargo run --bin ce read 1 at_mid
echo [预期] 读到 "v2"（中间态 v1 和被删除态均不可见）
echo.

:: ============================================================
:: 1.5 写-写冲突回滚
:: ============================================================
echo [测试 1.5] 写-写冲突回滚
echo   操作：T1 写 at_conflict=old(-unpost)；T2 写 at_conflict=new 并提交；
echo         T1 commit 应失败；T3 读 at_conflict 应是 new
echo ---------- T1 写入 at_conflict=old，不提交 ----------
cargo run --bin ce add 1 at_conflict old -unpost > "%TEMP%\ac_tx2.txt" 2>&1
type "%TEMP%\ac_tx2.txt"
echo.
for /f "tokens=3 delims=: " %%a in ('findstr /c:"事务开始，ID:" "%TEMP%\ac_tx2.txt"') do set "AC_TXID2=%%a"
echo ---------- T2 写入 at_conflict=new，提交（先提交者胜）----------
cargo run --bin ce add 1 at_conflict new
echo.
echo ---------- 尝试提交 T1 (ID: %AC_TXID2%) ----------
cargo run --bin ce commit %AC_TXID2%
echo [预期] 提交失败（写-写冲突）
echo.
echo ---------- T3 读 at_conflict ----------
cargo run --bin ce read 1 at_conflict
echo [预期] 读到 "new"（T1 的 old 仿佛从未存在过）
del "%TEMP%\ac_tx2.txt" 2>nul
echo.

echo ============================================================
echo      原子性测试完成
echo      请检查上述每个 [预期] 是否与实际输出一致
echo ============================================================
echo.
pause