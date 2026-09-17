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
     * Submits a batch computation job from client(s).
     * Splits the workload evenly across available active workers,
     * aggregates partial results, and increments JAC on this coordinator.
     */
    @Override
    public JobResult submitJob(JobRequest request) throws RemoteException {
        // Method stub:
        // 1. Verify term limit (termJobCount < MAX_JOBS_PER_TERM)
        // 2. Split request evenly across active workers (data chunks for MAX/PRIMECOUNT, sub-ranges for PRIMESUM)
        // 3. Dispatch tasks concurrently to workers
        // 4. Aggregate worker results into single JobResult
        // 5. Increment persistentJac and termJobCount
        // 6. If termJobCount == MAX_JOBS_PER_TERM, trigger stepDown and new election
        return null;
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

