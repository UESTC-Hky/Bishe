@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID Test 4: Durability
echo      Committed data survives crashes and restarts
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
:: 4.1 Committed data is stably readable
:: ============================================================
echo [Test 4.1] Committed data is stably readable
echo   Write 3 keys, then read 3 rounds to verify consistency
%CE% add 1 dr_k1 val_one
timeout /t 3 /nobreak > nul
%CE% add 1 dr_k2 val_two
timeout /t 3 /nobreak > nul
%CE% add 1 dr_k3 val_three
timeout /t 3 /nobreak > nul
echo ---------- Round 1 ----------
%CE% read 1 dr_k1
%CE% read 1 dr_k2
%CE% read 1 dr_k3
echo ---------- Round 2 ----------
%CE% read 1 dr_k1
%CE% read 1 dr_k2
%CE% read 1 dr_k3
echo ---------- Round 3 ----------
%CE% read 1 dr_k1
%CE% read 1 dr_k2
%CE% read 1 dr_k3
echo [Expected] All 3 rounds identical: val_one, val_two, val_three
echo.

:: ============================================================
:: 4.2 Delete durability
:: ============================================================
echo [Test 4.2] Delete durability
echo   Write then delete, verify 3 rounds of reads return "not found"
%CE% add 1 dr_del todelete
timeout /t 3 /nobreak > nul
%CE% delete 1 dr_del
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Round 1 ----------
%CE% read 1 dr_del
echo ---------- Round 2 ----------
%CE% read 1 dr_del
echo ---------- Round 3 ----------
%CE% read 1 dr_del
echo [Expected] All 3 rounds: key not found (delete is durable)
echo.

:: ============================================================
:: 4.3 Multi-key transaction durability
:: ============================================================
echo [Test 4.3] Multi-key transaction durability
echo   Write 3 keys in one transaction, read 3 rounds
%CE% add 1 dr_multi_a apple + add dr_multi_b banana + add dr_multi_c cherry
timeout /t 3 /nobreak > nul
echo ---------- Round 1 ----------
%CE% read 1 dr_multi_a
%CE% read 1 dr_multi_b
%CE% read 1 dr_multi_c
echo ---------- Round 2 ----------
%CE% read 1 dr_multi_a
%CE% read 1 dr_multi_b
%CE% read 1 dr_multi_c
echo ---------- Round 3 ----------
%CE% read 1 dr_multi_a
%CE% read 1 dr_multi_b
%CE% read 1 dr_multi_c
echo [Expected] All 3 rounds identical: apple, banana, cherry
echo.

:: ============================================================
:: 4.4 WAL crash recovery
:: ============================================================
echo [Test 4.4] WAL crash recovery
echo   Write dr_recover=survive and commit
echo   Then restart TS to simulate crash recovery
%CE% add 1 dr_recover survive
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read dr_recover before restart ----------
%CE% read 1 dr_recover
echo [Expected] Read "survive"
echo.
echo +----------------------------------------------------------+
echo ^|  Restarting TS to simulate crash recovery...              ^|
echo +----------------------------------------------------------+
echo.
echo [Recovery] Killing TS process...
taskkill /f /im ts.exe > nul 2>&1
timeout /t 2 /nobreak > nul
echo [Recovery] Restarting TS (WAL recovery should replay now)...
start "" target\debug\ts.exe --ls-addr 127.0.0.1:25002 --ss-addr 127.0.0.1:25001
timeout /t 3 /nobreak > nul
echo.
echo ---------- After restart: read dr_recover ----------
%CE% read 1 dr_recover
echo [Expected] Read "survive" (data survived crash!)
echo.
echo ---------- After restart: read other existing keys ----------
%CE% read 1 dr_k1
echo [Expected] Read "val_one" (all previously committed data recovered)
%CE% read 1 dr_multi_a
echo [Expected] Read "apple" (multi-key transaction data also recovered)
echo.

:: ============================================================
:: Cleanup
:: ============================================================
echo ============================================================
echo      Durability test complete
echo ============================================================
echo.
echo [Cleanup] Stopping servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
echo [Cleanup] Removing test data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Cleanup] Done.
pause