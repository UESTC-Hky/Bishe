@echo off
chcp 65001 > nul

echo.
echo ============================================================
echo      ACID Test 3: Isolation
echo      Snapshot isolation - concurrent txns do not interfere
echo ============================================================
echo.

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

echo [Test 3.1] Snapshot read isolation
echo   T1 reads iso_key1 (no commit yet)
echo   T2 writes iso_key1=T2_value and commits
echo   T3 reads iso_key1, should see T2_value
echo ---------- Prepare data ----------
%CE% add 1 iso_key1 original
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T1: read iso_key1, do NOT commit ----------
%CE% read 1 iso_key1 -unpost > "%TEMP%\iso_t1.txt" 2>&1
findstr /c:"ID:" "%TEMP%\iso_t1.txt"
echo ---------- Extract TX ID ----------
for /f "tokens=2 delims=: " %%a in ('findstr /c:"ID:" "%TEMP%\iso_t1.txt"') do set "ISO_T1=%%a"
echo T1 ID: %ISO_T1%
echo.
echo ---------- T2: write iso_key1=T2_value and commit ----------
%CE% add 1 iso_key1 T2_value
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- Commit T1 (read-only, should succeed) ----------
%CE% commit %ISO_T1%
echo [Expected] T1 commit succeeds (read-only, no conflict)
echo.
echo ---------- T3: read iso_key1 ----------
%CE% read 1 iso_key1
echo [Expected] Read "T2_value" (T2 committed value visible)
del "%TEMP%\iso_t1.txt" 2>nul
echo.

echo [Test 3.2] Read-your-writes within transaction
echo   T1 writes iso_rw=first_val, reads it back in same txn
echo   T2 overwrites with iso_rw=second_val
echo   T3 reads back
echo ---------- T1: write and read back ----------
%CE% add 1 iso_rw first_val + read iso_rw
echo [Expected] Read "first_val" (own write visible within txn)
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T2: overwrite and commit ----------
%CE% add 1 iso_rw second_val
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T3: independent read ----------
%CE% read 1 iso_rw
echo [Expected] Read "second_val" (overwrite took effect)
echo.

echo [Test 3.3] Read isolation - uncommitted write invisible
echo   T1 writes iso_rr=v1 and commits
echo   T2 writes iso_rr=v2 but does NOT commit
echo   T3 reads iso_rr, should still see v1
echo   T2 commits, then T4 reads v2
echo ---------- T1: write v1 and commit ----------
%CE% add 1 iso_rr v1
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T2: write v2, no commit ----------
%CE% add 1 iso_rr v2 -unpost > "%TEMP%\iso_t3b.txt" 2>&1
findstr /c:"ID:" "%TEMP%\iso_t3b.txt"
for /f "tokens=2 delims=: " %%a in ('findstr /c:"ID:" "%TEMP%\iso_t3b.txt"') do set "ISO_T3B=%%a"
echo T2 ID: %ISO_T3B%
echo.
echo ---------- T3: read iso_rr ----------
%CE% read 1 iso_rr
echo [Expected] Read "v1" (T2 uncommitted v2 invisible)
echo.
echo ---------- T2: commit ----------
%CE% commit %ISO_T3B%
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T4: read iso_rr ----------
%CE% read 1 iso_rr
echo [Expected] Read "v2" (T2 committed value now visible)
del "%TEMP%\iso_t3b.txt" 2>nul
echo.

echo [Test 3.4] Non-conflicting concurrent writes
echo   T1 writes iso_a=valA (no commit)
echo   T2 writes iso_b=valB and commits
echo   T1 commits, should succeed (different keys)
echo ---------- T1: write iso_a=valA, no commit ----------
%CE% add 1 iso_a valA -unpost > "%TEMP%\iso_t4.txt" 2>&1
findstr /c:"ID:" "%TEMP%\iso_t4.txt"
for /f "tokens=2 delims=: " %%a in ('findstr /c:"ID:" "%TEMP%\iso_t4.txt"') do set "ISO_T4=%%a"
echo T1 ID: %ISO_T4%
echo.
echo ---------- T2: write iso_b=valB and commit ----------
%CE% add 1 iso_b valB
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T1: commit ----------
%CE% commit %ISO_T4%
echo [Expected] T1 commit succeeds (no conflict, different keys)
echo.
echo ---------- Wait for sync (3s) ----------
timeout /t 3 /nobreak > nul
echo ---------- T3: read both keys ----------
%CE% read 1 iso_a
echo [Expected] Read "valA"
%CE% read 1 iso_b
echo [Expected] Read "valB" (both writes succeeded independently)
del "%TEMP%\iso_t4.txt" 2>nul
echo.

echo ============================================================
echo      Isolation test complete
echo ============================================================
echo.
echo [Cleanup] Stopping servers...
taskkill /f /im ls.exe /im ss.exe /im ts.exe > nul 2>&1
echo [Cleanup] Removing test data...
rmdir /s /q ls_data ss_data wal_logs 2>nul
echo [Cleanup] Done.
pause