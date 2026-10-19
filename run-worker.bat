@echo off
title Worker Node
cd /d "%~dp0"
java -cp target\distrilab.jar worker.WorkerNode %*
pause
