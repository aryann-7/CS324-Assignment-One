@echo off
setlocal enabledelayedexpansion

REM =========================================================================
REM CS324 Add Dynamic Client Script (Windows)
REM =========================================================================
REM Usage:
REM   add_client.bat [clientId]
REM Examples:
REM   add_client.bat        (defaults to clientId 3)
REM   add_client.bat 3
REM   add_client.bat 4
REM =========================================================================

set CLIENT_ID=%1
if "%CLIENT_ID%"=="" set CLIENT_ID=3

echo =========================================================================
echo Launching dynamic Client GUI #%CLIENT_ID%...
echo =========================================================================

start "Client GUI #%CLIENT_ID%" java -cp bin client.ClientMain %CLIENT_ID%

echo Client GUI #%CLIENT_ID% started in a new window.
