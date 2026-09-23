package worker;

import common.interfaces.BootstrapService;
import common.interfaces.WorkerService;
import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import worker.computation.JobRunner;
import worker.coordinator.CoordinatorManager;
import worker.election.ElectionManager;

/**
 * Main Worker Node.
 *
 * Each worker:
 * - Has a unique integer ID.
 * - Maintains a JAC.
 * - Communicates using Java RMI.
 * - Participates in leader elections.
 * - Propagates COORDINATOR messages.
 * - Can become the coordinator.
 */
public class WorkerNode extends UnicastRemoteObject
        implements WorkerService {

    private static final long serialVersionUID = 1L;

    /**
     * Required assignment variable.
     */
    private final String leaderman = "cs324";

    /**
     * Unique worker ID.
     */
    private final int workerId;

    private final String host;
    private final int port;

    /**
     * Current coordinator ID.
     */
    private volatile int coordinatorId = -1;

    /**
     * Current election term.
     */
    private final AtomicInteger currentTerm =
            new AtomicInteger(1);

    /**
     * JAC is persistent between terms.
     */
    private final AtomicInteger jac =
            new AtomicInteger(0);

    /**
     * Whether this worker is coordinator.
     */
    private volatile boolean isCoordinator = false;

    /**
     * Handles election logic.
     */
    private final ElectionManager electionManager =
            new ElectionManager();

    /**
     * Coordinator manager created only when this worker
     * becomes coordinator.
     */
    private CoordinatorManager coordinatorManager;

    /**
     * Remote neighbour workers.
     *
     * Map:
     * worker ID -> RMI remote object
     */
    private final Map<Integer, WorkerService> neighbours =
            new ConcurrentHashMap<>();

    /**
     * Used for concurrent computation.
     */
    private final ExecutorService computationThreadPool =
            Executors.newFixedThreadPool(4);

    /**
     * Stores the parent worker for each election.
     */
    private final Map<String, Integer> electionParents =
            new ConcurrentHashMap<>();

    /**
     * Stores workers that are expected to return an election result.
     */
    private final Map<String, Set<Integer>> pendingReplies =
            new ConcurrentHashMap<>();

    /**
     * Stores the current best candidate for each election.
     */
    private final Map<String, ElectionCandidate> bestCandidates =
            new ConcurrentHashMap<>();

    public WorkerNode(
            int workerId,
            String host,
            int port) throws RemoteException {

        super();

        this.workerId = workerId;
        this.host = host;
        this.port = port;
    }

    /**
     * Executes a job using a worker thread.
     */
    @Override
    public JobResult executeJob(JobTask task)
            throws RemoteException {

        if (task == null) {

            JobResult error = new JobResult();

            error.setSuccess(false);
            error.setErrorMessage(
                    "Received null JobTask");

            return error;
        }

        try {

            var future =
                    computationThreadPool.submit(
                            new JobRunner(task));

            return future.get();

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            JobResult error = new JobResult();

            error.setJobId(task.getJobId());
            error.setTaskId(task.getTaskId());
            error.setSuccess(false);
            error.setErrorMessage(
                    "Task interrupted: " + e.getMessage());

            return error;

        } catch (Exception e) {

            JobResult error = new JobResult();

            error.setJobId(task.getJobId());
            error.setTaskId(task.getTaskId());
            error.setSuccess(false);
            error.setErrorMessage(
                    "Execution error: " + e.getMessage());

            return error;
        }
    }

    /**
     * Starts a new leader election.
     */
    public void startElection() {

        int newTerm =
                currentTerm.incrementAndGet();

        System.out.println();
        System.out.println(
                "====================================");

        System.out.println(
                "Worker " + workerId +
                " starting ELECTION.");

        System.out.println(
                "Term: " + newTerm);

        System.out.println(
                "Worker JAC: " + jac.get());

        System.out.println(
                "====================================");

        electionManager.resetElectionState();

        ElectionMessage message =
                electionManager.initiateElection(
                        workerId,
                        jac.get(),
                        newTerm);

        // Mark our own election message as processed.
        electionManager.isDuplicateAndMark(
                message.getMessageId());

        electionParents.put(
                message.getMessageId(),
                -1);

        ElectionCandidate localCandidate =
                new ElectionCandidate(
                        workerId,
                        jac.get());

        bestCandidates.put(
                message.getMessageId(),
                localCandidate);

        Set<Integer> children =
                ConcurrentHashMap.newKeySet();

        pendingReplies.put(
                message.getMessageId(),
                children);

        // Send the election to every neighbour.
        for (Map.Entry<Integer, WorkerService> entry
                : neighbours.entrySet()) {

            children.add(entry.getKey());

            sendElection(
                    entry.getValue(),
                    message);
        }

        // If this is the only active worker,
        // it becomes coordinator.
        checkElectionFinished(message);
    }

    /**
     * Receives ELECTION or COORDINATOR messages.
     */
    @Override
    public void receiveElectionMessage(
            ElectionMessage message)
            throws RemoteException {

        if (message == null) {
            return;
        }

        /*
         * Election messages must have a valid unique ID.
         */
        if (message.getMessageId() == null
                || message.getMessageId().isBlank()) {

            System.out.println(
                    "Worker " + workerId +
                    " rejected election message with no ID.");

            return;
        }

        /*
         * Prevent duplicate processing.
         */
        if (electionManager.isDuplicateAndMark(
                message.getMessageId())) {

            System.out.println(
                    "Worker " + workerId +
                    " ignored duplicate " +
                    message.getType() +
                    " message " +
                    message.getMessageId());

            return;
        }

        /*
         * COORDINATOR message.
         */
        if (message.getType()
                == ElectionMessage.MessageType.COORDINATOR) {

            receiveCoordinatorMessage(message);
            return;
        }

        /*
         * Normal ELECTION message.
         */
        System.out.println(
                "Worker " + workerId +
                " received ELECTION " +
                message.getMessageId());

        /*
         * Remember the worker that sent this message.
         */
        int parentId = message.getSenderId();

        electionParents.put(
                message.getMessageId(),
                parentId);

        /*
         * Compare the election candidate with this
         * worker's own JAC and ID.
         */
        ElectionMessage updated =
                electionManager.processElectionMessage(
                        message,
                        workerId,
                        jac.get());

        bestCandidates.put(
                message.getMessageId(),
                new ElectionCandidate(
                        updated.getCandidateId(),
                        updated.getCandidateJac()));

        /*
         * Find all neighbours except our parent.
         */
        Set<Integer> children =
                ConcurrentHashMap.newKeySet();

        for (Integer neighbourId :
                neighbours.keySet()) {

            if (neighbourId != parentId) {

                children.add(neighbourId);
            }
        }

        /*
         * Show that the election is being propagated.
         */
        System.out.println(
                "Worker " + workerId +
                " forwarding ELECTION " +
                message.getMessageId() +
                " to " +
                children.size() +
                " neighbour(s).");

        pendingReplies.put(
                message.getMessageId(),
                children);

        /*
         * Forward the election.
         */
        for (Integer neighbourId : children) {

            WorkerService neighbour =
                    neighbours.get(neighbourId);

            if (neighbour != null) {

                sendElection(
                        neighbour,
                        updated);
            }
        }

        /*
         * If there are no children, immediately
         * return our candidate to the parent.
         */
        checkElectionFinished(updated);
    }

    /**
     * Sends an election message to another worker.
     */
    private void sendElection(
            WorkerService worker,
            ElectionMessage message) {

        /*
         * Create a copy so the sender ID represents
         * the current worker.
         */
        ElectionMessage forwarded =
                new ElectionMessage(
                        message.getMessageId(),
                        message.getType(),
                        message.getCandidateId(),
                        message.getCandidateJac(),
                        message.getTerm(),
                        workerId);

        try {

            worker.receiveElectionMessage(
                    forwarded);

        } catch (Exception e) {

            System.out.println(
                    "Worker " + workerId +
                    " could not forward ELECTION: " +
                    e.getMessage());
        }
    }

    /**
     * Receives an election result from a child worker.
     */
    @Override
    public void receiveElectionReply(
            ElectionReply reply)
            throws RemoteException {

        if (reply == null) {
            return;
        }

        String electionId =
                reply.getMessageId();

        ElectionCandidate current =
                bestCandidates.get(electionId);

        ElectionCandidate received =
                new ElectionCandidate(
                        reply.getCandidateId(),
                        reply.getCandidateJac());

        if (current == null ||
                isBetterCandidate(
                        received,
                        current)) {

            bestCandidates.put(
                    electionId,
                    received);
        }

        Set<Integer> pending =
                pendingReplies.get(electionId);

        if (pending != null) {

            pending.remove(
                    reply.getSenderId());
        }

        ElectionMessage message =
                new ElectionMessage(
                        electionId,
                        ElectionMessage.MessageType.ELECTION,
                        bestCandidates.get(electionId)
                                .workerId,
                        bestCandidates.get(electionId)
                                .jac,
                        reply.getTerm(),
                        workerId);

        checkElectionFinished(message);
    }

    /**
     * Checks whether all child workers have replied.
     */
    private void checkElectionFinished(
            ElectionMessage message) {

        Set<Integer> pending =
                pendingReplies.get(
                        message.getMessageId());

        if (pending != null &&
                !pending.isEmpty()) {

            return;
        }

        ElectionCandidate best =
                bestCandidates.get(
                        message.getMessageId());

        if (best == null) {
            return;
        }

        Integer parent =
                electionParents.get(
                        message.getMessageId());

        /*
         * Root worker has no parent.
         */
        if (parent == null || parent == -1) {

            System.out.println();
            System.out.println(
                    "Election completed.");

            System.out.println(
                    "Lowest JAC: " +
                    best.jac);

            System.out.println(
                    "Selected coordinator: Worker " +
                    best.workerId);

            announceCoordinator(
                    best.workerId,
                    best.jac,
                    message.getTerm());

        } else {

            WorkerService parentWorker =
                    neighbours.get(parent);

            if (parentWorker != null) {

                try {

                    ElectionReply reply =
                            new ElectionReply(
                                    message.getMessageId(),
                                    message.getTerm(),
                                    workerId,
                                    best.workerId,
                                    best.jac);

                    parentWorker.receiveElectionReply(
                            reply);

                } catch (Exception e) {

                    System.out.println(
                            "Could not return election result: "
                                    + e.getMessage());
                }
            }
        }
    }

    /**
     * Compares two election candidates.
     *
     * Lower JAC wins.
     * Highest ID wins when JAC is equal.
     */
    private boolean isBetterCandidate(
            ElectionCandidate first,
            ElectionCandidate second) {

        if (first.jac < second.jac) {
            return true;
        }

        if (first.jac == second.jac
                && first.workerId > second.workerId) {

            return true;
        }

        return false;
    }

    /**
     * Announces the selected coordinator.
     */
    private void announceCoordinator(
            int coordinator,
            int coordinatorJac,
            int term) {

        ElectionMessage message =
                electionManager.createCoordinatorAnnouncement(
                        coordinator,
                        coordinatorJac,
                        term);

        // Mark our own coordinator message.
        electionManager.isDuplicateAndMark(
                message.getMessageId());

        receiveCoordinatorMessage(message);

        /*
         * Propagate the COORDINATOR message.
         */
        for (Map.Entry<Integer, WorkerService> entry
                : neighbours.entrySet()) {

            sendCoordinator(
                    entry.getValue(),
                    message);
        }
    }

    /**
     * Processes a COORDINATOR message.
     */
    private void receiveCoordinatorMessage(
            ElectionMessage message) {

        coordinatorId =
                message.getCandidateId();

        isCoordinator =
                (workerId == coordinatorId);

        System.out.println(
                "Worker " + workerId +
                " now recognizes Worker " +
                coordinatorId +
                " as coordinator.");

        if (isCoordinator) {

            System.out.println(
                    "Worker " + workerId +
                    " has become the COORDINATOR.");

            transitionToCoordinator();

        } else {

            /*
             * If this worker was previously coordinator,
             * step down.
             */
            if (coordinatorManager != null) {

                stepDownToWorker();
            }
        }

        /*
         * Forward the coordinator announcement.
         */
        for (Map.Entry<Integer, WorkerService> entry
                : neighbours.entrySet()) {

            if (entry.getKey()
                    == message.getSenderId()) {

                continue;
            }

            sendCoordinator(
                    entry.getValue(),
                    message);
        }
    }

    /**
     * Sends a coordinator message to a neighbour.
     */
    private void sendCoordinator(
            WorkerService worker,
            ElectionMessage message) {

        ElectionMessage forwarded =
                new ElectionMessage(
                        message.getMessageId(),
                        ElectionMessage.MessageType.COORDINATOR,
                        message.getCandidateId(),
                        message.getCandidateJac(),
                        message.getTerm(),
                        workerId);

        try {

            worker.receiveElectionMessage(
                    forwarded);

        } catch (Exception e) {

            System.out.println(
                    "Could not propagate COORDINATOR: "
                            + e.getMessage());
        }
    }

    /**
     * Connects this worker to the Bootstrap Node.
     */
    public void connectToBootstrap(
            String bootstrapHost,
            int bootstrapPort) {

        try {

            BootstrapService bootstrap =
                    (BootstrapService) Naming.lookup(
                            "rmi://" +
                            bootstrapHost +
                            ":" +
                            bootstrapPort +
                            "/BootstrapService");

            /*
             * Register this worker first.
             */
            bootstrap.registerWorker(
                    workerId,
                    host,
                    port);

            /*
             * Ask Bootstrap for an initial neighbour.
             */
            List<String> endpoints =
                    bootstrap.getInitialNeighbors(
                            workerId);

            for (String endpoint : endpoints) {

                addNeighbourFromEndpoint(endpoint);
            }

            System.out.println(
                    "Worker " + workerId +
                    " connected to Bootstrap.");

            System.out.println(
                    "Neighbours: " +
                    neighbours.keySet());

        } catch (Exception e) {

            System.err.println(
                    "Bootstrap connection failed: " +
                    e.getMessage());
        }
    }

    /**
     * Converts a Bootstrap endpoint into an RMI worker stub.
     *
     * Endpoint format:
     * workerId|host|port
     */
    private void addNeighbourFromEndpoint(
            String endpoint) {

        try {

            String[] parts =
                    endpoint.split("\\|");

            int id =
                    Integer.parseInt(parts[0]);

            String workerHost =
                    parts[1];

            int workerPort =
                    Integer.parseInt(parts[2]);

            Registry registry =
                    LocateRegistry.getRegistry(
                            workerHost,
                            workerPort);

            WorkerService worker =
                    (WorkerService) registry.lookup(
                            "Worker-" + id);

            neighbours.put(
                    id,
                    worker);

            System.out.println(
                    "Worker " + workerId +
                    " connected to Worker " +
                    id);

        } catch (Exception e) {

            System.err.println(
                    "Could not connect to neighbour " +
                    endpoint + ": " +
                    e.getMessage());
        }
    }

    /**
     * Makes this worker the coordinator.
     */
    public synchronized void transitionToCoordinator() {

        if (coordinatorManager != null) {
            return;
        }

        try {

            isCoordinator = true;

            coordinatorManager =
                    new CoordinatorManager(
                            jac.get());

            /*
             * Add other workers to the coordinator's
             * computation pool.
             */
            for (WorkerService worker :
                    neighbours.values()) {

                coordinatorManager.addWorker(
                        worker);
            }

            System.out.println(
                    "Worker " + workerId +
                    " is now coordinator.");

        } catch (RemoteException e) {

            System.err.println(
                    "Failed to become coordinator: " +
                    e.getMessage());
        }
    }

    /**
     * Steps down and starts a new election.
     */
    public synchronized void stepDownToWorker() {

        if (coordinatorManager != null) {

            jac.set(
                    coordinatorManager
                            .getPersistentJac());

            coordinatorManager = null;
        }

        isCoordinator = false;

        System.out.println(
                "Worker " + workerId +
                " stepped down from coordinator.");

        /*
         * Start a new term.
         */
        startElection();
    }

    /**
     * Ping used for liveness checking.
     */
    @Override
    public boolean ping() {
        return true;
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

    public boolean isCoordinator() {
        return isCoordinator;
    }

    /**
     * Returns the current coordinator ID.
     */
    public int getCoordinatorId() {
        return coordinatorId;
    }

    /**
     * Returns the current election term.
     */
    public int getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Small candidate class.
     */
    private static class ElectionCandidate {

        private final int workerId;
        private final int jac;

        ElectionCandidate(
                int workerId,
                int jac) {

            this.workerId = workerId;
            this.jac = jac;
        }
    }
}