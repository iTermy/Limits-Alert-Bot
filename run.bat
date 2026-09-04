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

REM Disable QuickEdit for this console session. Entering Select mode otherwise
REM pauses synchronous console writes indefinitely. ENABLE_EXTENDED_FLAGS is
REM required when clearing ENABLE_QUICK_EDIT_MODE.
"%PYTHON%" -c "import ctypes as c; k=c.WinDLL('kernel32',use_last_error=True); k.GetStdHandle.argtypes=[c.c_int]; k.GetStdHandle.restype=c.c_void_p; k.GetConsoleMode.argtypes=[c.c_void_p,c.POINTER(c.c_ulong)]; k.GetConsoleMode.restype=c.c_int; k.SetConsoleMode.argtypes=[c.c_void_p,c.c_ulong]; k.SetConsoleMode.restype=c.c_int; h=k.GetStdHandle(-10); m=c.c_ulong(); ok=k.GetConsoleMode(h,c.byref(m)); ok and k.SetConsoleMode(h,(m.value|0x80)&0xFFFFFFBF)"

"%PYTHON%" main.py
