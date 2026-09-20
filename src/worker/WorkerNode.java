package worker;

import common.interfaces.BootstrapService;
import common.interfaces.CoordinatorService;
import common.interfaces.WorkerService;
import common.models.ElectionMessage;
import common.models.JobResult;
import common.models.JobTask;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import worker.computation.JobRunner;
import worker.coordinator.CoordinatorManager;
import worker.election.ElectionManager;

/**
 * Main Worker Node process.
 * Acts as an independent worker process with a unique integer ID.
 * Capable of transitioning into coordinator mode.
 */
public class WorkerNode extends UnicastRemoteObject implements WorkerService {
    private static final long serialVersionUID = 1L;

    /**
     * Required assignment worker-level variable.
     */
    private final String leaderman = "cs324";

    /**
     * Unique integer ID for this worker.
     */
    private final int workerId;
    private final String host;
    private final int port;
    private boolean isCoordinator = false;

    /**
     * Job Allocation Counter (JAC) tracking.
     * Incremented each time that worker, while acting as the coordinator, assigns a job to another worker.
     */
    private final AtomicInteger jac = new AtomicInteger(0);


    private final ElectionManager electionManager = new ElectionManager();
    private CoordinatorManager coordinatorManager;
    private final List<String> neighborEndpoints = new ArrayList<>();
    private final ExecutorService computationThreadPool = Executors.newFixedThreadPool(4);

    public WorkerNode(int workerId, String host, int port) throws RemoteException {
        super();
        this.workerId = workerId;
        this.host = host;
        this.port = port;
    }

    /**
     * Executes an assigned job task concurrently using Java threads.
     */
    @Override
    public JobResult executeJob(JobTask task) throws RemoteException {
        if (task == null) {
            JobResult err = new JobResult();
            err.setSuccess(false);
            err.setErrorMessage("Received null JobTask");
            return err;
        }

        try {
            java.util.concurrent.Future<JobResult> future = computationThreadPool.submit(new JobRunner(task));
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            JobResult err = new JobResult();
            err.setJobId(task.getJobId());
            err.setTaskId(task.getTaskId());
            err.setSuccess(false);
            err.setErrorMessage("Task interrupted: " + e.getMessage());
            return err;
        } catch (java.util.concurrent.ExecutionException e) {
            JobResult err = new JobResult();
            err.setJobId(task.getJobId());
            err.setTaskId(task.getTaskId());
            err.setSuccess(false);
            err.setErrorMessage("Execution error: " + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()));
            return err;
        }
    }


    /**
     * Receives and handles election flooding messages with duplicate suppression.
     * Evaluates candidate with lowest JAC and tie-breaker highest integer worker ID.
     */
    @Override
    public void receiveElectionMessage(ElectionMessage message) throws RemoteException {
        // Method stub: check duplication via electionManager and process/flood to neighbors
    }

    /**
     * Liveness check.
     */
    @Override
    public boolean ping() throws RemoteException {
        // Method stub: return true
        return true;
    }

    /**
     * Contacts the Bootstrap Node to register this worker and acquire initial random neighbor.
     */
    public void connectToBootstrap(String bootstrapHost, int bootstrapPort) {
        // Method stub: lookup BootstrapService in RMI registry and call registerWorker + getInitialNeighbors
    }

    /**
     * Transitions this worker into the coordinator role.
     * Serves for exactly one term (up to 5 job assignments).
     */
    public synchronized void transitionToCoordinator() {
        try {
            isCoordinator = true;
            coordinatorManager = new CoordinatorManager(jac.get());
            // Add self as an available compute worker in the coordinator pool
            coordinatorManager.addWorker(this);
        } catch (RemoteException e) {
            System.err.println("Failed to transition to coordinator: " + e.getMessage());
        }
    }

    /**
     * Steps down from coordinator role back to regular worker mode when term expires (after 5 jobs).
     */
    public synchronized void stepDownToWorker() {
        if (coordinatorManager != null) {
            jac.set(coordinatorManager.getPersistentJac());
            coordinatorManager = null;
        }
        isCoordinator = false;
    }


    public String getLeaderman() {
        return leaderman;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac.get();
    }

    public void incrementJac() {
        this.jac.incrementAndGet();
    }

    public boolean isCoordinator() {
        return isCoordinator;
    }
}

