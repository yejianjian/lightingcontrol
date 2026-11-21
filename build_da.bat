@echo off
rem ============================================================
rem  OPC DA edition single-file build (Windows Server 2012 target)
rem  Output: dist\LightingControlDA_v1.2.1.exe
rem
rem  NOTE: keep this file ASCII-only. A UTF-8 .bat is misread by
rem  cmd.exe before "chcp 65001" takes effect, which garbles the
rem  Chinese labels (this happened to a previous revision).
rem ============================================================
setlocal
cd /d "%~dp0"

set APP_NAME=LightingControlDA_v1.2.1
set SPEC=LightingControlDA.spec

echo ============================================================
echo  Building %APP_NAME%.exe  (single file, OPC DA edition)
echo  Python 3.8 environment: venv38
echo ============================================================
echo.

if exist ".\venv38\Scripts\pyinstaller.exe" (
    echo [1/2] Running PyInstaller with venv38 Python 3.8 ...
    ".\venv38\Scripts\pyinstaller.exe" "%SPEC%" --clean --noconfirm
) else (
    echo [1/2] venv38 not found, falling back to system pyinstaller ...
    pyinstaller "%SPEC%" --clean --noconfirm
)

if not exist "dist\%APP_NAME%.exe" (
    echo.
    echo ============================================================
    echo  BUILD FAILED: dist\%APP_NAME%.exe was not produced
    echo ============================================================
    exit /b 1
)

echo.
echo ============================================================
echo  BUILD OK
for %%F in ("dist\%APP_NAME%.exe") do echo  File: %%~fF  (%%~zF bytes)
echo ============================================================
endlocal
