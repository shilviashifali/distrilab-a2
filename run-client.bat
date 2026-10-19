@echo off
title Client
cd /d "%~dp0"
java -cp target\distrilab.jar client.JobClientGUI %*
pause
