@echo off
REM Starts the Bootstrap Node, N workers and M clients, each in its own window.
REM Build first:  mvn clean package
set /p WORKERS=How many workers (3-10)? 
set /p CLIENTS=How many clients? 

start "Bootstrap" cmd /k java -jar bootstrap-node\target\bootstrap-node.jar --port=50050
timeout /t 2 >nul

for /L %%i in (1,1,%WORKERS%) do (
    start "Worker %%i" cmd /k java -jar worker-node\target\worker-node.jar --bootstrapHost=localhost --bootstrapPort=50050
    timeout /t 1 >nul
)

timeout /t 3 >nul
for /L %%i in (1,1,%CLIENTS%) do (
    start "Client %%i" cmd /k java -jar client\target\client.jar --bootstrapHost=localhost --bootstrapPort=50050
)
