@echo off
REM Starts the Bootstrap Node, N workers and M clients, each in its own window.
REM Build first in NetBeans: right-click the project, Clean and Build.
cd /d "%~dp0"
set /p WORKERS=How many workers (3-10)? 
set /p CLIENTS=How many clients? 

start "Bootstrap" cmd /k java -cp target\distrilab.jar bootstrap.BootstrapNode
timeout /t 2 >nul

for /L %%i in (1,1,%WORKERS%) do (
    start "Worker %%i" cmd /k java -cp target\distrilab.jar worker.WorkerNode
    timeout /t 1 >nul
)

timeout /t 3 >nul
for /L %%i in (1,1,%CLIENTS%) do (
    start "Client %%i" cmd /k java -cp target\distrilab.jar client.JobClientGUI
)
