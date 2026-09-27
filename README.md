# CS324 Distributed Computing Cluster System 🖥️

**Assignment One Project for CS324 Distributed Systems Course @USP**

A fully-functional distributed computing cluster implementation using Java RMI that allows computational jobs to run in parallel across multiple worker nodes under the coordination of an automatically-elected leader.

[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](https://www.oracle.com/java/)
[![RMI](https://img.shields.io/badge/RMI-Enabled-blue.svg)](https://docs.oracle.com/javase/tutorial/rmi/)
[![Status](https://img.shields.io/badge/Status-Complete-success.svg)](https://github.com)

## 🎯 Quick Start

**Follow these 4 simple steps:**

1. **Start Bootstrap Server:**
   ```bash
   java -cp out bootstrap.BootstrapServer
   ```

2. **Start Worker Nodes** (open 3 new terminals):
   ```bash
   java -cp out worker.WorkerMain 1 localhost 1101 localhost 1099
   java -cp out worker.WorkerMain 2 localhost 1102 localhost 1099
   java -cp out worker.WorkerMain 3 localhost 1103 localhost 1099
   ```

3. **Wait for Election** (look for "Worker X has become the COORDINATOR" message)

4. **Start Client:**
   ```bash
   java -cp out client.ClientMain
   ```

Now you can submit jobs through the GUI! 🎉

---

## 📚 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [How It Works](#how-it-works)
- [Installation & Setup](#installation--setup)
- [Running the System](#running-the-system)
- [Using the Client GUI](#using-the-client-gui)
- [Testing Scenarios](#testing-scenarios)
- [Troubleshooting](#troubleshooting)
- [Code Structure](#code-structure)
- [Advanced Topics](#advanced-topics)
- [License](#license)

---

## 🎓 Overview

A distributed computing cluster implementation using Java RMI that allows jobs to run across multiple worker nodes under the coordination of an elected leader.

## System Components

### 1. Bootstrap Node
- Maintains registry of all active worker nodes
- Does not participate in elections or job processing
- Runs as a separate Java process

### 2. Worker Nodes
- Form an unstructured network with peer connections
- Participate in leader elections based on Job Allocation Counter (JAC)
- Execute distributed computational jobs
- Support concurrent job execution using multithreading

### 3. Coordinator (Elected Leader)
- Elected worker that accepts jobs from clients
- Distributes workload evenly among available workers
- Serves for one term (up to 5 jobs)
- New election triggered after term ends

### 4. Client (GUI Application)
- Java Swing GUI for submitting computational jobs
- Supports manual data entry and CSV file loading
- Handles concurrent job submissions within single client
- Multiple clients can run simultaneously

## Supported Job Types

1. **MAX(numbers)** - Returns the largest value from an unsorted list of numbers
2. **PRIMESUM(start, end)** - Calculates the sum of all prime numbers within the specified range
3. **PRIMECOUNT(numbers)** - Counts the occurrence of prime numbers in an unsorted list

## Prerequisites

- Java JDK 8 or higher
- Java RMI configured and enabled
- Network connectivity between all nodes

## How to Compile

From the project root directory:

```bash
# Compile all source files
javac -d bin -sourcepath src src/**/*.java

# Or if using a build tool like Maven/Gradle, follow those instructions
```

## How to Run the System

### Step 1: Start the Bootstrap Node

The Bootstrap Node must be started first as it maintains the registry of active workers.

```bash
# Start Bootstrap Node on default port 1099
java -cp bin bootstrap.BootstrapServer

# Or specify a custom port
java -cp bin bootstrap.BootstrapServer 2000
```

Expected output:
```
Bootstrap Node started on port 1099
Waiting for worker registrations...
```

### Step 2: Start Worker Nodes

Start multiple worker nodes. Each worker needs a unique ID and will connect to the bootstrap node.

```bash
# Start Worker 1
java -cp bin worker.WorkerMain 1 localhost 1099

# Start Worker 2 (in a new terminal)
java -cp bin worker.WorkerMain 2 localhost 1099

# Start Worker 3 (in a new terminal)
java -cp bin worker.WorkerMain 3 localhost 1099

# Start Worker 4 (in a new terminal)
java -cp bin worker.WorkerMain 4 localhost 1099
```

Command format: `java -cp bin worker.WorkerMain <workerID> <bootstrapHost> <bootstrapPort>`

Expected output from each worker:
```
Worker 1 started
Connected to Bootstrap Node at localhost:1099
Waiting for coordinator election...
```

### Step 3: Leader Election

Once workers are running, an election will be triggered automatically. The worker with the lowest Job Allocation Counter (JAC) will be elected. If multiple workers have the same JAC, the one with the highest ID wins.

You should see output like:
```
ELECTION initiated by Worker 3
Worker 1 elected as COORDINATOR for this term
```

### Step 4: Start Client GUI(s)

Once a coordinator is elected, clients can connect and submit jobs.

```bash
# Start Client 1
java -cp bin client.ClientMain 1

# Start Client 2 (in a new terminal, for testing multiple concurrent clients)
java -cp bin client.ClientMain 2

# Start Client 3 (in a new terminal)
java -cp bin client.ClientMain 3
```

Command format: `java -cp bin client.ClientMain <clientID>`

## Using the Client GUI

### Connection Setup
1. Enter the coordinator host (default: `localhost`)
2. Enter the coordinator port (default: `1099`)
3. Click "Test Discovery" to verify connection to the coordinator

### Submitting Jobs

#### For MAX or PRIMECOUNT:
1. Select job type from dropdown (MAX or PRIMECOUNT)
2. **Option A - Manual Entry:**
   - Enter comma-separated numbers in the text area
   - Example: `10, 25, 3, 99, 17, 4, 101, 88, 2, 7`
3. **Option B - Load from CSV:**
   - Click "Browse CSV File..."
   - Select a CSV file containing numbers (see `test-data-*.csv` files)
4. Click "Submit Job (Async)"

#### For PRIMESUM:
1. Select "PRIMESUM" from dropdown
2. Enter Start value (e.g., `1`)
3. Enter End value (e.g., `1000`)
4. Click "Submit Job (Async)"

### Concurrent Job Submission
- You can submit multiple jobs rapidly without waiting for previous jobs to complete
- The GUI remains responsive during job execution
- Results appear in the output log with timestamps

### Multiple Concurrent Clients
- Run multiple client processes simultaneously
- Each client can submit jobs independently
- All jobs are handled by the same coordinator and distributed across workers

## Test Data Files

Three CSV test files are provided:

1. **test-data-small.csv** - Small dataset (10 numbers) for quick testing
2. **test-data-large.csv** - Larger dataset (66 numbers) with mix of primes and composites
3. **test-data-edge-cases.csv** - Edge cases including duplicates, negatives, and large numbers

## Example Workflow

```bash
# Terminal 1: Bootstrap
java -cp bin bootstrap.BootstrapServer

# Terminal 2-5: Workers
java -cp bin worker.WorkerMain 1 localhost 1099
java -cp bin worker.WorkerMain 2 localhost 1099
java -cp bin worker.WorkerMain 3 localhost 1099
java -cp bin worker.WorkerMain 4 localhost 1099

# Wait for election to complete and coordinator to be elected

# Terminal 6-7: Clients
java -cp bin client.ClientMain 1
java -cp bin client.ClientMain 2

# Use the GUI to submit jobs and see distributed computation in action
```

## Architecture Notes

### Leader Election Algorithm
- Custom election algorithm propagates ELECTION messages through the unstructured network
- Each worker processes election messages only once (prevents loops)
- All reachable active workers participate in the election
- Election criteria: Lowest JAC, then highest ID as tiebreaker
- COORDINATOR message propagated after election to inform all workers
- System eventually reaches consensus on a single coordinator per term

### Job Distribution
The coordinator divides work evenly among available workers:
- **PRIMESUM(1, 1000)** with 4 workers:
  - Worker 1: PRIMESUM(1, 250)
  - Worker 2: PRIMESUM(251, 500)
  - Worker 3: PRIMESUM(501, 750)
  - Worker 4: PRIMESUM(751, 1000)

### Concurrent Execution
- Each worker uses Java threads for concurrent job execution
- Multiple jobs can run simultaneously on the same worker
- Client uses `ExecutorService` thread pool for non-blocking job submission

## Troubleshooting

### Client cannot connect to coordinator
- Ensure Bootstrap Node is running first
- Ensure at least one worker is running and elected as coordinator
- Check that host/port settings in client GUI match the bootstrap configuration
- Verify no firewall blocking RMI communication

### Workers cannot find bootstrap node
- Ensure Bootstrap Node is started before workers
- Check that bootstrap host and port are correct in worker startup command
- Verify RMI registry is accessible

### Jobs not executing
- Ensure coordinator has been elected (check worker terminal output)
- Verify workers are actively running and connected
- Check for errors in worker or coordinator terminal output

## Development & Version Control

This project uses Git for version control with separate feature branches:
- `feature/section1-bootstrap` - Bootstrap Node implementation
- `feature/section2-workers` - Worker Node implementation  
- `feature/section3-leader-election` - Leader Election implementation
- `feature/section4-client-gui` - Client GUI implementation (this section)



## License

Academic project for CS324 course at University of the South Pacific
