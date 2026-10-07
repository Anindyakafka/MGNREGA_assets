@echo off
setlocal
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
    set "APP_PYTHON=.venv\Scripts\python.exe"
) else (
    set "APP_PYTHON=python"
)
"%APP_PYTHON%" -c "import requests, bs4, tkinter" >nul 2>&1
if errorlevel 1 (
    echo Python, Tkinter, or dependencies are missing.
    echo Follow the setup instructions in README.md, then run this launcher again.
    pause
    exit /b 1
)
"%APP_PYTHON%" app.py gui
if errorlevel 1 (
    echo The GUI stopped with an error. See the message above.
    pause
    exit /b 1
)
