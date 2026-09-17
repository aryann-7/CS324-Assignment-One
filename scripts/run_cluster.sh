#!/usr/bin/env bash
# =========================================================================
# CS324 Distributed Computing Cluster Build and Run Script (Linux/macOS)
# =========================================================================

set -e

echo "[1/6] Cleaning previous build artifacts..."
rm -rf bin
mkdir -p bin

echo "[2/6] Compiling Java source files..."
javac -d bin $(find src -name "*.java")
echo "Compilation successful."

echo "[3/6] Starting RMI Registry on port 1099..."
rmiregistry -J-cp -Jbin 1099 &
RMI_PID=$!
sleep 2

echo "[4/6] Launching Bootstrap Server..."
java -cp bin bootstrap.BootstrapServer &
BOOTSTRAP_PID=$!
sleep 2

echo "[5/6] Launching Worker Nodes (unique integer IDs)..."
java -cp bin worker.WorkerMain 101 localhost 1101 localhost 1099 &
W1_PID=$!
java -cp bin worker.WorkerMain 102 localhost 1102 localhost 1099 &
W2_PID=$!
java -cp bin worker.WorkerMain 103 localhost 1103 localhost 1099 &
W3_PID=$!
sleep 2

echo "[6/6] Launching Client GUI..."
java -cp bin client.ClientMain &
CLIENT_PID=$!

echo "Cluster launched. Press Ctrl+C to terminate all processes."

trap "kill $RMI_PID $BOOTSTRAP_PID $W1_PID $W2_PID $W3_PID $CLIENT_PID 2>/dev/null || true" EXIT
wait
