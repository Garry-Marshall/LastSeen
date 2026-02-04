@echo off
call venv\Scripts\activate.bat

:loop
python main.py
set EXIT_CODE=%ERRORLEVEL%

REM Exit code 2 = config error, do not restart
if %EXIT_CODE% equ 2 (
    echo Config error — fix .env and restart manually.
    pause
    exit /b 2
)

REM Exit code 0 = clean shutdown (e.g. Ctrl+C), do not restart
if %EXIT_CODE% equ 0 (
    echo Bot stopped.
    pause
    exit /b 0
)

REM Any other exit code = unexpected crash, restart after delay
echo Bot crashed (exit code %EXIT_CODE%). Restarting in 5 seconds...
echo Crash details have been written to logs\crash.log
timeout /t 5 /nobreak >nul
goto loop
