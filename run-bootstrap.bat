@echo off
title Bootstrap Node
cd /d "%~dp0"
java -cp target\distrilab.jar bootstrap.BootstrapNode %*
pause
