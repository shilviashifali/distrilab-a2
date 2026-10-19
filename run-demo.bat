@echo off
set /p N=How many workers (3-10)? 
set CP=dist\DistriLab.jar

start "Bootstrap" cmd /k java -cp %CP% bootstrap.BootstrapNode
timeout /t 2 >nul

for /L %%i in (1,1,%N%) do (
    start "Worker %%i" cmd /k java -cp %CP% worker.WorkerNode
    timeout /t 1 >nul
)

timeout /t 3 >nul
start "Client 1" cmd /k java -cp %CP% client.JobClientGUI
start "Client 2" cmd /k java -cp %CP% client.JobClientGUI
