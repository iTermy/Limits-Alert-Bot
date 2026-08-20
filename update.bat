@echo off
REM Pull the latest code from GitHub, install any new dependencies, start the bot.
REM Stop the running bot (Ctrl+C) before running this.
REM
REM Live config (config/*.json, data/news_events.json, data/info_embeds.json) is
REM marked skip-worktree on the VPS, so runtime edits made there by !tp / !alertdist
REM / !nmconfig / !news survive every pull. See DEPLOY.md for the one-time setup.

setlocal
cd /d "%~dp0"

REM Use the venv interpreter if present, else fall back to PATH python.
set "PYTHON=python"
if exist "%~dp0venv\Scripts\python.exe"  set "PYTHON=%~dp0venv\Scripts\python.exe"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON=%~dp0.venv\Scripts\python.exe"

for /f "delims=" %%H in ('git rev-parse HEAD') do set "BEFORE=%%H"

echo Pulling latest from GitHub...
git pull --ff-only
if errorlevel 1 goto pullfailed

for /f "delims=" %%H in ('git rev-parse HEAD') do set "AFTER=%%H"
if "%BEFORE%"=="%AFTER%" echo Already up to date.& goto run

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

:pullfailed
echo.
echo Pull failed - the VPS has local commits or modified tracked files.
echo Run "git status" here and resolve before deploying. Bot NOT started.
pause
exit /b 1
