@echo off
REM =========================================================================
REM CS324 Distributed Computing Cluster Build and Run Script (Windows)
REM =========================================================================

echo [1/6] Cleaning previous build artifacts...
if exist bin rd /s /q bin
mkdir bin

echo [2/6] Compiling Java source files...
javac -d bin src\common\models\*.java src\common\interfaces\*.java src\bootstrap\*.java src\worker\election\*.java src\worker\computation\*.java src\worker\coordinator\*.java src\worker\*.java src\client\parser\*.java src\client\gui\*.java src\client\*.java
if %ERRORLEVEL% NEQ 0 (
    echo Compilation failed!
    exit /b %ERRORLEVEL%
)
echo Compilation successful.

echo [3/6] Starting RMI Registry on port 1099...
start "RMI Registry" rmiregistry -J-cp -Jbin 1099

timeout /t 2 /nobreak >nul

echo [4/6] Launching Bootstrap Server...
start "Bootstrap Server" java -cp bin bootstrap.BootstrapServer

timeout /t 2 /nobreak >nul

echo [5/6] Launching Worker Nodes (3 workers with unique integer IDs)...
start "Worker-101" java -cp bin worker.WorkerMain 101 localhost 1101 localhost 1099
start "Worker-102" java -cp bin worker.WorkerMain 102 localhost 1102 localhost 1099
start "Worker-103" java -cp bin worker.WorkerMain 103 localhost 1103 localhost 1099

timeout /t 2 /nobreak >nul

echo [6/6] Launching Client GUI...
start "Client GUI" java -cp bin client.ClientMain

echo Cluster launched successfully.
