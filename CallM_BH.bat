@echo off
REM CallM_BH Windows Launcher

cd /d "%~dp0"

REM Check for virtual environment
if not exist "venv\" (
    if not exist ".venv\" (
        echo ============================================================
        echo ERROR: Virtual environment not found!
        echo.
        echo Please run SETUP.bat first to set up CallM_BH
        echo ============================================================
        pause
        exit /b 1
    )
)

REM Check for .env file
if not exist ".env" (
    echo ============================================================
    echo ERROR: .env file not found!
    echo.
    echo Please run SETUP.bat to configure your Google API Key
    echo ============================================================
    pause
    exit /b 1
)

REM Use venv or .venv
if exist "venv\" (
    set VENV_DIR=venv
) else (
    set VENV_DIR=.venv
)

REM Activate virtual environment and run launcher
echo ============================================================
echo Starting CallM_BH...
echo ============================================================
echo.

call %VENV_DIR%\Scripts\activate.bat
python launcher.py

REM Keep window open if there's an error
if errorlevel 1 (
    echo.
    echo ============================================================
    echo ERROR: CallM_BH failed to start
    echo ============================================================
    pause
)
