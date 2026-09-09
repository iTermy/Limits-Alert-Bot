@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_monitoring.ps1" %*
exit /b %errorlevel%
