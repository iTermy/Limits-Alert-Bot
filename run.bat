@echo off
REM Convenience launcher for TM Bot. Equivalent to running `python main.py`.
REM
REM No restart loop here: main.py has an in-process supervisor for recoverable
REM crashes, and the loop-freeze watchdog in feed_health_monitor.py now spawns
REM its own replacement before hard-exiting. Adding an external restart loop on
REM top of that would start a second bot every time the watchdog relaunches.

setlocal
cd /d "%~dp0"

REM Use the venv interpreter if present, else fall back to PATH python.
set "PYTHON=python"
if exist "%~dp0venv\Scripts\python.exe" set "PYTHON=%~dp0venv\Scripts\python.exe"

"%PYTHON%" main.py
