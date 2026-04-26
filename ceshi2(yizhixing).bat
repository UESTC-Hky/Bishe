@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID Test 2: Consistency
echo      Database integrity is preserved across transactions
echo ============================================================
echo.

:: ============================================================
:: 0. Clean and start servers
:: ============================================================
echo [Setup] Stopping old servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
timeout /t 1 /nobreak > nul
echo [Setup] Cleaning old data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Setup] Starting LS...
start "" target\debug\ls.exe --data-path ./ls_data --listen-addr 127.0.0.1:25002
timeout /t 3 /nobreak > nul
echo [Setup] Starting SS...
start "" target\debug\ss.exe --data-path ./ss_data
timeout /t 5 /nobreak > nul
echo [Setup] Starting TS...
start "" target\debug\ts.exe --ls-addr 127.0.0.1:25002 --ss-addr 127.0.0.1:25001
timeout /t 3 /nobreak > nul
echo.

set CE=target\debug\ce.exe

:: ============================================================
:: 2.1 Basic read-write consistency
:: ============================================================
echo [Test 2.1] Basic read-write consistency
echo   Write K=hello, then read back
%CE% add 1 cn_k1 hello
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read cn_k1 ----------
%CE% read 1 cn_k1
echo [Expected] Read "hello" (value matches what was written)
echo.

:: ============================================================
:: 2.2 Overwrite correctness
:: ============================================================
echo [Test 2.2] Overwrite correctness
echo   Write old, then overwrite with new
%CE% add 1 cn_k2 old
timeout /t 3 /nobreak > nul
%CE% add 1 cn_k2 new
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read cn_k2 ----------
%CE% read 1 cn_k2
echo [Expected] Read "new" (old value correctly overwritten)
echo.

:: ============================================================
:: 2.3 Delete semantics
:: ============================================================
echo [Test 2.3] Delete semantics
echo   Write then delete, verify key is gone
%CE% add 1 cn_k3 val
timeout /t 3 /nobreak > nul
%CE% delete 1 cn_k3
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read cn_k3 ----------
%CE% read 1 cn_k3
echo [Expected] Key not found (deleted correctly)
echo.

:: ============================================================
:: 2.4 Read never-existing key
:: ============================================================
echo [Test 2.4] Read never-existing key
echo   Read a key that was never written
%CE% read 1 cn_never_exist
echo [Expected] Key not found (no crash, no garbage data)
echo.

:: ============================================================
:: 2.5 Multi-key precision
:: ============================================================
echo [Test 2.5] Multi-key precision and isolation
echo   Write 3 keys in one transaction, then read each independently
%CE% add 1 cn_multi_a valueA + add cn_multi_b valueB + add cn_multi_c valueC
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read cn_multi_a ----------
%CE% read 1 cn_multi_a
echo [Expected] Read "valueA"
echo ---------- Read cn_multi_b ----------
%CE% read 1 cn_multi_b
echo [Expected] Read "valueB"
echo ---------- Read cn_multi_c ----------
%CE% read 1 cn_multi_c
echo [Expected] Read "valueC" (values not swapped)
echo.

:: ============================================================
:: Cleanup
:: ============================================================
echo ============================================================
echo      Consistency test complete
echo ============================================================
echo.
echo [Cleanup] Stopping servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
echo [Cleanup] Removing test data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Cleanup] Done.
pause