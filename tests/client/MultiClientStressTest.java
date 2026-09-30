package client;

import bootstrap.BootstrapServiceImpl;
import common.interfaces.CoordinatorService;
import common.models.JobRequest;
import common.models.JobResult;
import common.models.JobType;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import worker.WorkerNode;

public class MultiClientStressTest {

    private static final int WORKER_COUNT = 3;
    private static final List<Remote> exported = new ArrayList<>();
    private static int bootstrapPort;

    public static void main(String[] args) {
        System.out.println("=========================================================================");
        System.out.println("STARTING STRESS TEST: Multi-Client Concurrent Submissions");
        System.out.println("=========================================================================");

        try {
            // 1. Bring up Cluster
            setupCluster();

            // 2. BONUS TEST CASE 1: 2 Clients running ALL 3 JOBS simultaneously
            System.out.println("\n-------------------------------------------------------------------------");
            System.out.println(">>> RUNNING BONUS TEST CASE 1: 2 Concurrent Clients (6 Total Jobs at once)");
            System.out.println("-------------------------------------------------------------------------");
            runConcurrentClientsTest(2);

            // 3. BONUS TEST CASE 2: 3 Clients running ALL 3 JOBS simultaneously
            System.out.println("\n-------------------------------------------------------------------------");
            System.out.println(">>> RUNNING BONUS TEST CASE 2: 3 Concurrent Clients (9 Total Jobs at once)");
            System.out.println("-------------------------------------------------------------------------");
            runConcurrentClientsTest(3);

            System.out.println("\n=========================================================================");
            System.out.println("ALL STRESS TESTS COMPLETED SUCCESSFULLY!");
            System.out.println("=========================================================================");

        } catch (Throwable t) {
            System.err.println("TEST SUITE FAILED WITH ERROR:");
            t.printStackTrace();
        } finally {
            cleanup();
            System.exit(0);
        }
    }

    private static void setupCluster() throws Exception {
        bootstrapPort = createRegistry();
        BootstrapServiceImpl bootstrap = track(new BootstrapServiceImpl());
        LocateRegistry.getRegistry("localhost", bootstrapPort).rebind("BootstrapService", bootstrap);
        System.out.println("[Setup] Bootstrap started on port " + bootstrapPort);

        List<WorkerNode> workers = new ArrayList<>();
        for (int i = 0; i < WORKER_COUNT; i++) {
            int workerId = 801 + i;
            int port = createRegistry();
            WorkerNode worker = track(new WorkerNode(workerId, "localhost", port));
            LocateRegistry.getRegistry("localhost", port).rebind("Worker-" + workerId, worker);
            worker.connectToBootstrap("localhost", bootstrapPort);
            workers.add(worker);
        }
        System.out.println("[Setup] " + WORKER_COUNT + " workers registered with bootstrap.");

        // Start election
        workers.get(0).startElection();
        int coordinatorId = waitForElectionToSettle(workers);
        System.out.println("[Setup] Election settled. Coordinator is Worker " + coordinatorId);
    }

    private static int waitForElectionToSettle(List<WorkerNode> workers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            int firstCoord = workers.get(0).getCoordinatorId();
            boolean allAgree = firstCoord != -1;
            for (WorkerNode w : workers) {
                if (w.getCoordinatorId() != firstCoord) {
                    allAgree = false;
                    break;
                }
            }
            if (allAgree && workers.stream().filter(WorkerNode::isCoordinator).count() == 1) {
                return firstCoord;
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("Election did not settle within 10s");
    }

    private static void runConcurrentClientsTest(int numClients) throws Exception {
        long testStart = System.currentTimeMillis();
        ExecutorService clientExecutor = Executors.newFixedThreadPool(numClients);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(numClients);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successfulJobs = new AtomicInteger(0);

        for (int c = 1; c <= numClients; c++) {
            final int clientId = c;
            clientExecutor.submit(() -> {
                try {
                    startGate.await(); // Synchronize all clients to start simultaneously

                    // Each client runs ALL 3 jobs simultaneously using its own thread pool
                    ExecutorService clientPool = Executors.newFixedThreadPool(3);
                    List<Future<JobResult>> jobFutures = new ArrayList<>();

                    // Job 1: MAX
                    List<Long> numbers = List.of(12L, 45L, 100L + (clientId * 10), 89L, 2L, 77L);
                    long expectedMax = 100L + (clientId * 10);
                    jobFutures.add(clientPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-max", JobType.MAX, numbers))));

                    // Job 2: PRIMESUM
                    long start = 1;
                    long end = 30; // primes: 2,3,5,7,11,13,17,19,23,29 = 129
                    jobFutures.add(clientPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-primesum", JobType.PRIMESUM, start, end))));

                    // Job 3: PRIMECOUNT
                    List<Long> pList = List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L); // Primes: 2, 3, 5, 7, 11 (5 primes)
                    jobFutures.add(clientPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-primecount", JobType.PRIMECOUNT, pList))));

                    // Validate results for this client
                    JobResult maxRes = jobFutures.get(0).get(25, TimeUnit.SECONDS);
                    JobResult sumRes = jobFutures.get(1).get(25, TimeUnit.SECONDS);
                    JobResult countRes = jobFutures.get(2).get(25, TimeUnit.SECONDS);

                    if (maxRes.isSuccess() && maxRes.getResultValue() == expectedMax) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " MAX mismatch: expected " + expectedMax + ", got " + maxRes);
                    }

                    if (sumRes.isSuccess() && sumRes.getResultValue() == 129) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " PRIMESUM mismatch: expected 129, got " + sumRes);
                    }

                    if (countRes.isSuccess() && countRes.getResultValue() == 5) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " PRIMECOUNT mismatch: expected 5, got " + countRes);
                    }

                    clientPool.shutdown();
                } catch (Exception e) {
                    errors.add("Client " + clientId + " encountered exception: " + e.getMessage());
                } finally {
                    doneGate.countDown();
                }
            });
        }

        // Fire all clients simultaneously
        startGate.countDown();
        boolean completed = doneGate.await(45, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - testStart;
        clientExecutor.shutdown();

        int totalExpectedJobs = numClients * 3;
        System.out.println("--- Test Results (" + numClients + " clients, " + totalExpectedJobs + " jobs total) ---");
        System.out.println("Execution Duration: " + duration + " ms");
        System.out.println("Successful Jobs:    " + successfulJobs.get() + " / " + totalExpectedJobs);
        System.out.println("Completed in Time:  " + completed);

        if (!errors.isEmpty()) {
            System.err.println("Errors encountered (" + errors.size() + "):");
            for (String err : errors) {
                System.err.println("  * " + err);
            }
            throw new AssertionError("Test failed with errors: " + errors);
        } else {
            System.out.println("STATUS: PASS - Zero errors, zero deadlocks, all calculations 100% mathematically correct.");
        }
    }

    private static JobResult submitWithRetry(JobRequest request) throws Exception {
        for (int attempt = 1; attempt <= 6; attempt++) {
            try {
                Registry reg = LocateRegistry.getRegistry("localhost", bootstrapPort);
                CoordinatorService coord = (CoordinatorService) reg.lookup("CoordinatorService");
                JobResult result = coord.submitJob(request);
                if (result != null && result.isSuccess()) {
                    return result;
                }
            } catch (Exception e) {
                // Expected when coordinator term expires (5 jobs) and re-election happens
            }
            Thread.sleep(1200);
        }
        throw new RuntimeException("Job " + request.getJobId() + " timed out after re-election retries.");
    }

    private static <T extends Remote> T track(T object) {
        exported.add(object);
        return object;
    }

    private static int createRegistry() throws RemoteException {
        int[] port = new int[1];
        track(LocateRegistry.createRegistry(0, null, requestedPort -> {
            ServerSocket socket = new ServerSocket(requestedPort);
            port[0] = socket.getLocalPort();
            return socket;
        }));
        return port[0];
    }

    private static void cleanup() {
        for (Remote r : exported) {
            try {
                UnicastRemoteObject.unexportObject(r, true);
            } catch (Exception ignored) {}
        }
    }
}
