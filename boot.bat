@echo off
echo Booting Portfolio Agent...
cd portfolio_agent
py -m uvicorn main:app --reload --port 8000
pause
