@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID Test 1: Atomicity
echo      All-or-nothing transaction commit
echo ============================================================
echo.

:: ============================================================
:: 0. Clean old data and start fresh servers
:: ============================================================
echo [Setup] Stopping old servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
timeout /t 1 /nobreak > nul
echo [Setup] Cleaning old data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Setup] Starting LS on port 25002...
start "" target\debug\ls.exe --data-path ./ls_data --listen-addr 127.0.0.1:25002
timeout /t 3 /nobreak > nul
echo [Setup] Starting SS on port 25001...
start "" target\debug\ss.exe --data-path ./ss_data
timeout /t 5 /nobreak > nul
echo [Setup] Starting TS on port 25003...
start "" target\debug\ts.exe --ls-addr 127.0.0.1:25002 --ss-addr 127.0.0.1:25001
timeout /t 3 /nobreak > nul
echo [Setup] All servers started.
echo.

set CE=target\debug\ce.exe

:: ============================================================
:: 1.1 Multi-key atomic commit
:: ============================================================
echo [Test 1.1] Multi-key atomic commit
echo   Write K1=v1, K2=v2, K3=v3 in one transaction, then read each
echo ---------- Writing ----------
%CE% add 1 at_k1 v1 + add at_k2 v2 + add at_k3 v3
echo.
echo ---------- Waiting for Log2Storage sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Read back ----------
echo Read at_k1:
%CE% read 1 at_k1
echo Read at_k2:
%CE% read 1 at_k2
echo Read at_k3:
%CE% read 1 at_k3
echo [Expected] at_k1=v1, at_k2=v2, at_k3=v3
echo.

:: ============================================================
:: 1.2 + 1.3 Uncommitted data not visible / visible after commit
:: ============================================================
echo [Test 1.2] Uncommitted data NOT visible to other transactions
echo   T1 writes at_uncommit=secret but does NOT commit
echo   T2 and T3 read independently
echo ---------- T1: write without commit (-unpost) ----------
%CE% add 1 at_uncommit secret -unpost > "%TEMP%\ac_tx.txt" 2>&1
type "%TEMP%\ac_tx.txt"
echo.
echo ---------- Extract transaction ID ----------
for /f "tokens=2 delims=: " %%a in ('findstr /c:"ID" "%TEMP%\ac_tx.txt"') do set "AC_TXID=%%a"
echo Extracted ID: %AC_TXID%
echo.
echo ---------- T2: independent read ----------
%CE% read 1 at_uncommit
echo [Expected] Key not found (uncommitted data invisible)
echo ---------- T3: independent read ----------
%CE% read 1 at_uncommit
echo [Expected] Key not found (confirmed)
echo.

echo [Test 1.3] Data visible after commit
echo   Commit T1, then T4 reads
echo ---------- Commit T1 (ID: %AC_TXID%) ----------
%CE% commit %AC_TXID%
echo.
echo ---------- Wait for Log2Storage sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T4: independent read ----------
%CE% read 1 at_uncommit
echo [Expected] Read "secret" (once committed, all writes take effect)
del "%TEMP%\ac_tx.txt" 2>nul
echo.

:: ============================================================
:: 1.4 Intermediate state isolation
:: ============================================================
echo [Test 1.4] Intermediate state not leaked
echo   Single transaction: write v1 - delete - write v2, then commit and read
%CE% add 1 at_mid v1 + delete at_mid + add at_mid v2
echo.
echo ---------- Wait for Log2Storage sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Independent read ----------
%CE% read 1 at_mid
echo [Expected] Read "v2" (intermediate v1 and deleted state invisible)
echo.

:: ============================================================
:: 1.5 Write-write conflict rollback
:: ============================================================
echo [Test 1.5] Write-write conflict rollback
echo   T1 writes at_conflict=old (no commit)
echo   T2 writes at_conflict=new and commits (first-committer wins)
echo   T1 commit should FAIL. T3 should read "new"
echo ---------- T1: write old, no commit ----------
%CE% add 1 at_conflict old -unpost > "%TEMP%\ac_tx2.txt" 2>&1
type "%TEMP%\ac_tx2.txt"
echo.
for /f "tokens=2 delims=: " %%a in ('findstr /c:"ID" "%TEMP%\ac_tx2.txt"') do set "AC_TXID2=%%a"
echo Extracted ID: %AC_TXID2%
echo.
echo ---------- T2: write new and commit ----------
%CE% add 1 at_conflict new
echo.
echo ---------- T1: attempt commit (should FAIL) ----------
%CE% commit %AC_TXID2%
echo [Expected] Commit failed (write-write conflict)
echo.
echo ---------- Wait for Log2Storage sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T3: read ----------
%CE% read 1 at_conflict
echo [Expected] Read "new" (T1's old value never existed)
del "%TEMP%\ac_tx2.txt" 2>nul
echo.

:: ============================================================
:: Cleanup
:: ============================================================
echo ============================================================
echo      Atomicity test complete
echo      Check each [Expected] against actual output
echo ============================================================
echo.
echo [Cleanup] Stopping servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
echo [Cleanup] Removing test data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Cleanup] Done.
pause