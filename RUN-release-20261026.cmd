@echo off
rem Release 2026-10-26 (backup, branch, build, gate, migrate, deploy knowledge services, verify).
rem Rollback: RUN-release-20261026.cmd -Rollback
cd /d "%~dp0"
where pwsh >nul 2>&1
if %errorlevel%==0 (
  pwsh -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\release-20261026.ps1" %*
) else (
  powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\release-20261026.ps1" %*
)
echo.
echo Finished. You can close this window.
pause
