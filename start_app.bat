@echo off
cd /d "%~dp0"
echo Starting LightingControl System...
".\venv38\Scripts\python.exe" main.py
echo.
echo Application exited with code %ERRORLEVEL%
pause
