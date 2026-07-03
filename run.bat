@echo off
REM External restart supervisor for TM Bot.
REM
REM main.py has its own in-process restart loop for recoverable crashes, but the
REM loop-freeze watchdog in feed_health_monitor.py calls os._exit(1) when the event
REM loop is wedged (e.g. an unresponsive MT5 terminal during spread-hour rollover).
REM os._exit kills the whole interpreter, including main.py's supervisor, so a fresh
REM instance can only come back if something OUTSIDE the process relaunches it.
REM That is this script's job. Start the bot with run.bat, not `python main.py`.

setlocal
cd /d "%~dp0"

REM Use the venv interpreter if present, else fall back to PATH python.
set "PYTHON=python"
if exist "%~dp0venv\Scripts\python.exe" set "PYTHON=%~dp0venv\Scripts\python.exe"

:loop
echo [%date% %time%] Starting TM Bot...
"%PYTHON%" main.py
echo [%date% %time%] Bot process exited (code %errorlevel%). Restarting in 5s...
timeout /t 5 /nobreak >nul
goto loop
