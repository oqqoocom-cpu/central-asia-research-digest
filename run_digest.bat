@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo Python was not found. Please install Python or add it to PATH.
    echo.
    pause
    exit /b 1
)

echo Generating Central Asia Research Daily Digest...
python "%~dp0digest_generator.py"
set "EXITCODE=%ERRORLEVEL%"

echo.
if not "%EXITCODE%"=="0" (
    echo Generation failed. Please check the error messages above.
    echo.
    pause
    exit /b %EXITCODE%
)

echo Done. Open the newest CentralAsia_Research_YYYY-MM-DD.md file in this folder.
echo.
pause
endlocal
