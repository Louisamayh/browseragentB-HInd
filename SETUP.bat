@echo off
setlocal enabledelayedexpansion
REM CallM_BH Setup Script for Windows
REM Run this once to install CallM_BH

echo ==========================================
echo CallM_BH Setup
echo ==========================================
echo.

REM Check Python
echo Checking Python...
py --version >nul 2>&1
if errorlevel 1 (
    python --version >nul 2>&1
    if errorlevel 1 (
        echo ERROR: Python is not installed!
        echo.
        echo Please install Python 3.11 or higher from python.org
        echo Make sure to check "Add Python to PATH" during installation
        echo.
        pause
        exit /b 1
    )
    set PYTHON_CMD=python
) else (
    set PYTHON_CMD=py
)

%PYTHON_CMD% --version
echo Python found
echo.

REM Create virtual environment
echo Creating virtual environment...
if exist "venv\" (
    echo Virtual environment already exists. Recreating...
    rmdir /s /q venv
)

%PYTHON_CMD% -m venv venv
echo Virtual environment created
echo.

REM Activate and install dependencies
echo Installing dependencies...
call venv\Scripts\activate.bat
%PYTHON_CMD% -m pip install --upgrade pip
pip install -r requirements.txt
echo Dependencies installed
echo.

REM Check for .env file
if not exist ".env" (
    echo .env file not found
    set /p CREATE_ENV="Do you want to enter your Google API Key now? (y/n): "
    if /i "%CREATE_ENV%"=="y" (
        set /p API_KEY="Enter your Google API Key: "
        echo GOOGLE_API_KEY=!API_KEY! > .env
        echo .env file created
    ) else (
        echo You can create .env later with your GOOGLE_API_KEY
    )
) else (
    echo .env file found
)
echo.

REM Create desktop shortcut
echo Creating desktop shortcut...
set SCRIPT_DIR=%~dp0
set DESKTOP=%USERPROFILE%\Desktop
set SHORTCUT=%DESKTOP%\CallM_BH.lnk

REM Use PowerShell to create shortcut
powershell -Command "$WS = New-Object -ComObject WScript.Shell; $SC = $WS.CreateShortcut('%SHORTCUT%'); $SC.TargetPath = '%SCRIPT_DIR%CallM_BH.bat'; $SC.WorkingDirectory = '%SCRIPT_DIR%'; $SC.IconLocation = '%SCRIPT_DIR%icon.ico'; $SC.Save()"

if exist "%SHORTCUT%" (
    echo Desktop shortcut created
) else (
    echo Could not create desktop shortcut
    echo You can manually create a shortcut to CallM_BH.bat
)
echo.

echo ==========================================
echo Setup Complete!
echo ==========================================
echo.
echo To start CallM_BH:
echo    1. Double-click the CallM_BH icon on your desktop
echo    2. Or double-click: CallM_BH.bat (in this folder)
echo ==========================================
echo.
pause
