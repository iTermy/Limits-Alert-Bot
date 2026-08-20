@echo off
REM Force the VPS to match GitHub, then start the bot.
REM Stop the running bot (Ctrl+C) before running this.
REM
REM Local changes to tracked files are DISCARDED - GitHub is the source of truth.
REM Not affected: .env, the venv and logs (gitignored), and the live config files
REM pinned with skip-worktree (see DEPLOY.md), which reset --hard leaves alone.

REM Re-exec from a temp copy first: this script runs "git reset --hard", which can
REM rewrite update.bat itself mid-run, and cmd.exe reads batch files by byte offset -
REM a file that changes underneath it corrupts execution from that point on.
if /i not "%~1"=="__from_temp" (
    copy /y "%~f0" "%TEMP%\tmbot_update.bat" >nul
    call "%TEMP%\tmbot_update.bat" __from_temp "%~dp0"
    exit /b %errorlevel%
)

setlocal
cd /d "%~2"

set "BRANCH=stage20_professionalize"

REM Use the venv interpreter if present, else fall back to PATH python.
set "PYTHON=python"
if exist "venv\Scripts\python.exe"  set "PYTHON=venv\Scripts\python.exe"
if exist ".venv\Scripts\python.exe" set "PYTHON=.venv\Scripts\python.exe"

for /f "delims=" %%H in ('git rev-parse HEAD 2^>nul') do set "BEFORE=%%H"
if not defined BEFORE goto notarepo

echo Fetching %BRANCH% from GitHub...
git fetch origin %BRANCH%
if errorlevel 1 goto fetchfailed

git reset --hard origin/%BRANCH%
if errorlevel 1 goto resetfailed

for /f "delims=" %%H in ('git rev-parse HEAD') do set "AFTER=%%H"
if "%BEFORE%"=="%AFTER%" (
    echo Already up to date.
    goto run
)

echo.
echo Updated:
git --no-pager log --oneline %BEFORE%..%AFTER%
echo.

REM Substring match, not findstr /x: git pipes LF-only line endings and findstr's
REM whole-line mode treats an LF-only stream as a single line, so /x never matches.
git diff --name-only %BEFORE% %AFTER% | findstr /c:"requirements.txt" >nul
if errorlevel 1 goto run
echo requirements.txt changed - installing dependencies...
"%PYTHON%" -m pip install -r requirements.txt

:run
echo.
echo Starting bot...
"%PYTHON%" main.py
goto :eof

:notarepo
echo This folder is not a git clone - see DEPLOY.md for the one-time setup.
pause
exit /b 1

:fetchfailed
echo.
echo Fetch failed - check the network and GitHub credentials. Bot NOT started.
pause
exit /b 1

:resetfailed
echo.
echo Reset failed. Bot NOT started.
pause
exit /b 1
