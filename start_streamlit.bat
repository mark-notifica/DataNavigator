@echo off
REM Kill any process using port 8501
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8501 ^| findstr LISTENING') do (
    taskkill /PID %%a /F >nul 2>&1
)

REM Wait a moment for the port to be released
timeout /t 2 /nobreak >nul

REM Start Streamlit
cd /d c:\Projects\DataNavigator
c:\Projects\DataNavigator\venv\Scripts\python.exe -m streamlit run app.py
