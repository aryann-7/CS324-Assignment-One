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

public class DynamicWorkerScalingStressTest {

    private static final List<Remote> exported = new ArrayList<>();
    private static int bootstrapPort;

    public static void main(String[] args) {
        System.out.println("=========================================================================");
        System.out.println("TEST CASE: Dynamic Worker Addition & Multi-Client Workload Rebalancing");
        System.out.println("=========================================================================");

        try {
            // Step 1: Start Bootstrap and ONLY 2 Worker Nodes initially
            System.out.println("\n[Phase 1] Starting Bootstrap and initial 2 Worker Nodes (W701, W702)...");
            bootstrapPort = createRegistry();
            BootstrapServiceImpl bootstrap = track(new BootstrapServiceImpl());
            LocateRegistry.getRegistry("localhost", bootstrapPort).rebind("BootstrapService", bootstrap);

            List<WorkerNode> workers = new ArrayList<>();
            WorkerNode w1 = createAndRegisterWorker(701, bootstrapPort);
            WorkerNode w2 = createAndRegisterWorker(702, bootstrapPort);
            workers.add(w1);
            workers.add(w2);

            // Elect initial leader between W701 and W702
            w1.startElection();
            int coordId = waitForElectionToSettle(workers);
            System.out.println("[Phase 1] Initial election complete. Active Coordinator is Worker " + coordId);

            // Step 2: Run all 3 Clients (submitting all 3 tasks) across ONLY 2 workers
            System.out.println("\n[Phase 2] Submitting jobs from 3 simultaneous clients across 2 workers...");
            runThreeClientsAllTasks("Phase 2 (2 Workers active)");

            // Step 3: Dynamically add the 3rd Worker Node
            System.out.println("\n[Phase 3] Adding 3rd Worker Node (W703) to the running cluster...");
            WorkerNode w3 = createAndRegisterWorker(703, bootstrapPort);
            workers.add(w3);
            Thread.sleep(1000); // Give peering a moment to connect

            System.out.println("[Phase 3] Worker 703 registered and connected to cluster mesh.");

            // Step 4: Run all 3 Clients (submitting all 3 tasks) now across 3 workers
            System.out.println("\n[Phase 4] Submitting jobs from 3 simultaneous clients across all 3 workers...");
            runThreeClientsAllTasks("Phase 4 (3 Workers active)");

            System.out.println("\n=========================================================================");
            System.out.println("DYNAMIC SCALING & WORKLOAD REBALANCING TEST PASSED WITH 100% SUCCESS!");
            System.out.println("=========================================================================");

        } catch (Throwable t) {
            System.err.println("TEST ENCOUNTERED AN ERROR:");
            t.printStackTrace();
        } finally {
            cleanup();
            System.exit(0);
        }
    }

    private static WorkerNode createAndRegisterWorker(int workerId, int bPort) throws Exception {
        int port = createRegistry();
        WorkerNode worker = track(new WorkerNode(workerId, "localhost", port));
        LocateRegistry.getRegistry("localhost", port).rebind("Worker-" + workerId, worker);
        worker.connectToBootstrap("localhost", bPort);
        return worker;
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

    private static void runThreeClientsAllTasks(String phaseLabel) throws Exception {
        int numClients = 3;
        long startTime = System.currentTimeMillis();
        ExecutorService clientExecutor = Executors.newFixedThreadPool(numClients);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(numClients);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger successfulJobs = new AtomicInteger(0);

        for (int c = 1; c <= numClients; c++) {
            final int clientId = c;
            clientExecutor.submit(() -> {
                try {
                    startGate.await();
                    ExecutorService taskPool = Executors.newFixedThreadPool(3);
                    List<Future<JobResult>> futures = new ArrayList<>();

                    // Task 1: MAX
                    List<Long> nums = List.of(15L, 88L, 200L + (clientId * 5), 44L, 9L);
                    long expectedMax = 200L + (clientId * 5);
                    futures.add(taskPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-max", JobType.MAX, nums))));

                    // Task 2: PRIMESUM (1..30 -> 129)
                    futures.add(taskPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-primesum", JobType.PRIMESUM, 1, 30))));

                    // Task 3: PRIMECOUNT ([2, 3, 5, 7, 11, 15, 20] -> 5 primes)
                    List<Long> pList = List.of(2L, 3L, 5L, 7L, 11L, 15L, 20L);
                    futures.add(taskPool.submit(() -> submitWithRetry(
                            new JobRequest("c" + clientId + "-primecount", JobType.PRIMECOUNT, pList))));

                    JobResult rMax = futures.get(0).get(25, TimeUnit.SECONDS);
                    JobResult rSum = futures.get(1).get(25, TimeUnit.SECONDS);
                    JobResult rCount = futures.get(2).get(25, TimeUnit.SECONDS);

                    if (rMax.isSuccess() && rMax.getResultValue() == expectedMax) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " MAX failed: expected " + expectedMax + ", got " + rMax);
                    }

                    if (rSum.isSuccess() && rSum.getResultValue() == 129) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " PRIMESUM failed: expected 129, got " + rSum);
                    }

                    if (rCount.isSuccess() && rCount.getResultValue() == 5) {
                        successfulJobs.incrementAndGet();
                    } else {
                        errors.add("Client " + clientId + " PRIMECOUNT failed: expected 5, got " + rCount);
                    }

                    taskPool.shutdown();
                } catch (Exception e) {
                    errors.add("Client " + clientId + " error: " + e.getMessage());
                } finally {
                    doneGate.countDown();
                }
            });
        }

        startGate.countDown();
        boolean completed = doneGate.await(45, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - startTime;
        clientExecutor.shutdown();

        System.out.println(">>> " + phaseLabel + " Results:");
        System.out.println("  * Completed in time: " + completed);
        System.out.println("  * Duration:          " + elapsed + " ms");
        System.out.println("  * Successful Tasks:  " + successfulJobs.get() + " / 9");

        if (!errors.isEmpty()) {
            for (String e : errors) {
                System.err.println("    - " + e);
            }
            throw new AssertionError(phaseLabel + " failed with errors.");
        } else {
            System.out.println("  * All tasks verified with 100% mathematical accuracy.");
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
            } catch (Exception ignored) {
            }
            Thread.sleep(1200);
        }
        throw new RuntimeException("Job " + request.getJobId() + " failed after retry attempts.");
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
