@echo off
call venv\Scripts\activate.bat
echo Pulling latest changes from git...
git pull
echo.
echo Installing/updating dependencies...
pip install -r requirements.txt
echo.
echo Update complete!
pause