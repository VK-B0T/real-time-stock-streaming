@echo off
cd /d D:\VARUN\BCA\Streamlit

echo Starting Docker Compose...
start /min cmd /c "docker compose up"

timeout /t 15 >nul

echo Checking Kafka container status...
docker ps -a | findstr kafka >nul

IF ERRORLEVEL 1 (
    echo Kafka container not found or not running. Restarting it...
    docker start sha256:a2716a120846c7f3f4b7c136692e23498721fca1d0f1fa79e4a444f6cf26a2b9
) ELSE (
    echo Kafka container is already running.
)

timeout /t 10 >nul

echo Starting Producer...
start cmd /k "python producer.py"

timeout /t 5 >nul

echo Starting Consumer...
start cmd /k "python consumer.py"
