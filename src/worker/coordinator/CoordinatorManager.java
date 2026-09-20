package worker.coordinator;

import common.interfaces.CoordinatorService;
import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles coordinator duties when a worker transitions into coordinator mode.
 *
 * Term limit & JAC rules:
 * - Coordinator serves for exactly one term (up to 5 job assignments).
 * - Job Allocation Counter (JAC): Each worker maintains a JAC. The counter is incremented
 *   each time that worker, while acting as the coordinator, assigns a job to another worker.
 * - Coordinator divides and distributes computational workload as evenly as possible among available workers.
 * - After 5 job assignments have been assigned, the term ends, and a new leader election must take place.
 */
public class CoordinatorManager extends UnicastRemoteObject implements CoordinatorService {

    private static final long serialVersionUID = 1L;

    public static final int MAX_JOBS_PER_TERM = 5;

    /**
     * Term job counter (0 to 5) tracking jobs completed in this coordinator term.
     */
    private final AtomicInteger termJobCount = new AtomicInteger(0);

    /**
     * Persistent Job Allocation Counter (JAC) for the hosting worker node.
     */
    private final AtomicInteger persistentJac;

    /**
     * Current leadership term sequence number.
     */
    private final AtomicInteger currentTerm = new AtomicInteger(1);

    public CoordinatorManager(int initialJac) throws RemoteException {
        super();
        this.persistentJac = new AtomicInteger(initialJac);
    }

    /**
     * Set of active workers (or endpoints / remote stubs) available in the cluster.
     */
    private final java.util.List<common.interfaces.WorkerService> activeWorkers = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Dedicated thread pool for dispatching partitioned sub-tasks concurrently to workers.
     */
    private final java.util.concurrent.ExecutorService dispatchPool = java.util.concurrent.Executors.newCachedThreadPool();

    /**
     * Adds an active worker service reference to the coordinator's worker pool.
     */
    public void addWorker(common.interfaces.WorkerService worker) {
        if (worker != null && !activeWorkers.contains(worker)) {
            activeWorkers.add(worker);
        }
    }

    /**
     * Removes an active worker reference (e.g., if unresponsive).
     */
    public void removeWorker(common.interfaces.WorkerService worker) {
        activeWorkers.remove(worker);
    }

    /**
     * Returns the list of currently active workers.
     */
    public java.util.List<common.interfaces.WorkerService> getActiveWorkers() {
        return activeWorkers;
    }

    /**
     * Submits a batch computation job from client(s).
     * Splits the workload evenly across available active workers,
     * aggregates partial results, and increments JAC on this coordinator.
     */
    @Override
    public synchronized JobResult submitJob(JobRequest request) throws RemoteException {
        long startTime = System.currentTimeMillis();
        JobResult finalResult = new JobResult();

        if (request == null) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("JobRequest cannot be null");
            finalResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);
            return finalResult;
        }

        finalResult.setJobId(request.getJobId());

        // 1. Verify term limit
        if (isTermExpired()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("Coordinator term expired (5 jobs limit reached). Election required.");
            stepDown();
            return finalResult;
        }

        // 2. Verify active worker availability
        if (activeWorkers.isEmpty()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("No active workers available in cluster to process job");
            return finalResult;
        }

        int numWorkers = activeWorkers.size();
        java.util.List<common.models.JobTask> tasks = new java.util.ArrayList<>();

        // 3. Partition workload evenly across active workers
        switch (request.getJobType()) {
            case MAX:
            case PRIMECOUNT:
                java.util.List<java.util.List<Long>> chunks = partitionList(request.getNumbers(), numWorkers);
                for (int i = 0; i < chunks.size(); i++) {
                    String taskId = request.getJobId() + "-task-" + (i + 1);
                    tasks.add(new common.models.JobTask(request.getJobId(), taskId, request.getJobType(), chunks.get(i)));
                }
                break;
            case PRIMESUM:
                java.util.List<long[]> ranges = partitionRange(request.getRangeStart(), request.getRangeEnd(), numWorkers);
                for (int i = 0; i < ranges.size(); i++) {
                    String taskId = request.getJobId() + "-task-" + (i + 1);
                    tasks.add(new common.models.JobTask(request.getJobId(), taskId, request.getJobType(), ranges.get(i)[0], ranges.get(i)[1]));
                }
                break;
            default:
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Unsupported job type: " + request.getJobType());
                return finalResult;
        }

        // 4. Dispatch tasks concurrently to workers using Java threads
        java.util.List<java.util.concurrent.Future<JobResult>> futures = new java.util.ArrayList<>();
        int workersAssignedCount = 0;

        for (int i = 0; i < tasks.size(); i++) {
            common.models.JobTask task = tasks.get(i);
            common.interfaces.WorkerService worker = activeWorkers.get(i % numWorkers);
            workersAssignedCount++;

            futures.add(dispatchPool.submit(() -> {
                try {
                    return worker.executeJob(task);
                } catch (Exception e) {
                    JobResult err = new JobResult();
                    err.setJobId(task.getJobId());
                    err.setTaskId(task.getTaskId());
                    err.setSuccess(false);
                    err.setErrorMessage("Worker RPC failed: " + e.getMessage());
                    return err;
                }
            }));
        }

        // 5. Aggregate worker results into final JobResult
        java.util.List<JobResult> subResults = new java.util.ArrayList<>();
        for (java.util.concurrent.Future<JobResult> future : futures) {
            try {
                JobResult res = future.get();
                if (res != null && !res.isSuccess()) {
                    finalResult.setSuccess(false);
                    finalResult.setErrorMessage("Subtask failure: " + res.getErrorMessage());
                    return finalResult;
                }
                subResults.add(res);
            } catch (Exception e) {
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Execution interrupted or failed: " + e.getMessage());
                return finalResult;
            }
        }

        aggregateResults(request.getJobType(), subResults, finalResult);

        // 6. Increment JAC (each time coordinator assigns a job to another worker) and increment term counter
        if (workersAssignedCount > 0) {
            persistentJac.incrementAndGet();
        }
        termJobCount.incrementAndGet();

        finalResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);

        // 7. Check if term limit has been reached
        if (isTermExpired()) {
            stepDown();
        }

        return finalResult;
    }

    /**
     * Splits a list of numbers as evenly as possible into N chunks.
     */
    private java.util.List<java.util.List<Long>> partitionList(java.util.List<Long> numbers, int numChunks) {
        java.util.List<java.util.List<Long>> partitions = new java.util.ArrayList<>();
        if (numbers == null || numbers.isEmpty() || numChunks <= 0) {
            partitions.add(numbers != null ? numbers : new java.util.ArrayList<>());
            return partitions;
        }

        int totalSize = numbers.size();
        int actualChunks = Math.min(numChunks, totalSize);
        int baseChunkSize = totalSize / actualChunks;
        int remainder = totalSize % actualChunks;

        int currentIndex = 0;
        for (int i = 0; i < actualChunks; i++) {
            int currentSize = baseChunkSize + (i < remainder ? 1 : 0);
            partitions.add(new java.util.ArrayList<>(numbers.subList(currentIndex, currentIndex + currentSize)));
            currentIndex += currentSize;
        }
        return partitions;
    }

    /**
     * Splits a numerical range [start, end] as evenly as possible across N workers.
     */
    private java.util.List<long[]> partitionRange(long start, long end, int numWorkers) {
        java.util.List<long[]> ranges = new java.util.ArrayList<>();
        if (start > end) {
            long temp = start;
            start = end;
            end = temp;
        }

        long totalElements = (end - start) + 1;
        int actualChunks = (int) Math.min(numWorkers, totalElements);
        long baseSize = totalElements / actualChunks;
        long remainder = totalElements % actualChunks;

        long currentStart = start;
        for (int i = 0; i < actualChunks; i++) {
            long currentChunkSize = baseSize + (i < remainder ? 1 : 0);
            long currentEnd = currentStart + currentChunkSize - 1;
            ranges.add(new long[]{currentStart, currentEnd});
            currentStart = currentEnd + 1;
        }
        return ranges;
    }

    /**
     * Aggregates partial sub-task results into a single consolidated result.
     */
    private void aggregateResults(common.models.JobType jobType, java.util.List<JobResult> subResults, JobResult finalResult) {
        if (subResults.isEmpty()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("No subtask results to aggregate");
            return;
        }

        switch (jobType) {
            case MAX:
                long globalMax = Long.MIN_VALUE;
                for (JobResult r : subResults) {
                    if (r != null && r.getResultValue() > globalMax) {
                        globalMax = r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(globalMax);
                break;
            case PRIMESUM:
                long totalPrimeSum = 0;
                for (JobResult r : subResults) {
                    if (r != null) {
                        totalPrimeSum += r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(totalPrimeSum);
                break;
            case PRIMECOUNT:
                long totalPrimeCount = 0;
                for (JobResult r : subResults) {
                    if (r != null) {
                        totalPrimeCount += r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(totalPrimeCount);
                break;
            default:
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Unknown job type: " + jobType);
                break;
        }
    }


    /**
     * Receives heartbeat or coordinator management messages from cluster nodes.
     */
    @Override
    public void handleCoordinatorMessage(CoordinatorMessage message) throws RemoteException {
        // Method stub: handle coordination message
    }

    /**
     * Queries current coordinator status including JAC, term, and jobs completed in this term.
     */
    @Override
    public String getCoordinatorStatus() throws RemoteException {
        // Method stub: return status string (term, termJobCount, persistentJac)
        return "";
    }

    /**
     * Checks if the coordinator has reached its maximum jobs for the current term (5 jobs).
     *
     * @return true if termJobCount >= MAX_JOBS_PER_TERM.
     */
    public boolean isTermExpired() {
        return termJobCount.get() >= MAX_JOBS_PER_TERM;
    }

    /**
     * Steps down from coordinator role and triggers a new leader election across reachable workers.
     */
    public void stepDown() {
        // Method stub: broadcast step down and initiate new ELECTION round
    }

    public int getPersistentJac() {
        return persistentJac.get();
    }
}

