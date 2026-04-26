@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID 测试2：一致性 (Consistency)
echo      事务前后数据库完整性不被破坏
echo ============================================================
echo.

:: ============================================================
:: 2.1 基本读写一致
:: ============================================================
echo [测试 2.1] 基本读写一致
echo   操作：写入 K=hello，提交后独立事务读取
cargo run --bin ce add 1 cn_k1 hello
echo.
echo --- 独立事务读 cn_k1 ---
cargo run --bin ce read 1 cn_k1
echo [预期] 读到 "hello"（写入的值与读取的值一致）
echo.

:: ============================================================
:: 2.2 更新覆盖正确
:: ============================================================
echo [测试 2.2] 更新覆盖正确
echo   操作：依次写 cn_k2=old -写 cn_k2=new，读验证
cargo run --bin ce add 1 cn_k2 old
cargo run --bin ce add 1 cn_k2 new
echo.
echo --- 读 cn_k2 ---
cargo run --bin ce read 1 cn_k2
echo [预期] 读到 "new"（旧值被正确覆盖，无残留）
echo.

:: ============================================================
:: 2.3 删除语义正确
:: ============================================================
echo [测试 2.3] 删除语义正确
echo   操作：写 cn_k3=val -删除 cn_k3，读验证
cargo run --bin ce add 1 cn_k3 val
cargo run --bin ce delete 1 cn_k3
echo.
echo --- 读 cn_k3 ---
cargo run --bin ce read 1 cn_k3
echo [预期] 返回"不存在"（删除后数据库状态合法）
echo.

:: ============================================================
:: 2.4 不存在键的读操作合法
:: ============================================================
echo [测试 2.4] 从未存在键的读取
echo   操作：读取一个从未写入过的键 cn_never_exist
cargo run --bin ce read 1 cn_never_exist
echo [预期] 返回"不存在"（不会崩溃，不会返回随机数据）
echo.

:: ============================================================
:: 2.5 多键一致性
:: ============================================================
echo [测试 2.5] 多键数据精度与串联性
echo   操作：一个事务写 3 个键，分别独立事务读验证值不串位
cargo run --bin ce add 1 cn_multi_a valueA + add cn_multi_b valueB + add cn_multi_c valueC
echo.
echo --- 读 cn_multi_a ---
cargo run --bin ce read 1 cn_multi_a
echo [预期] 读到 "valueA"
echo --- 读 cn_multi_b ---
cargo run --bin ce read 1 cn_multi_b
echo [预期] 读到 "valueB"
echo --- 读 cn_multi_c ---
cargo run --bin ce read 1 cn_multi_c
echo [预期] 读到 "valueC"（精度正确，值不串位）
echo.

echo ============================================================
echo      一致性测试完成
echo      请检查上述每个 [预期] 是否与实际输出一致
echo ============================================================
echo.
pause