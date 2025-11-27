@echo off
REM 简易入口：调用 PowerShell 脚本以避免批处理 BOM/编码问题
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0\run_local.ps1"
exit /b
