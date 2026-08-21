@echo off
echo Booting Portfolio Agent...
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
chcp 65001 >nul
py -m uvicorn main:app --reload --port 8000
pause
