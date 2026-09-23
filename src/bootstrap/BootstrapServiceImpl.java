package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bootstrap Node.
 *
 * The Bootstrap Node only helps workers join the network.
 * It does not participate in elections or computation.
 */
public class BootstrapServiceImpl
        extends UnicastRemoteObject
        implements BootstrapService {

    private static final long serialVersionUID = 1L;

    /**
     * Worker ID -> endpoint.
     */
    private final Map<Integer, String> registeredWorkers =
            new ConcurrentHashMap<>();

    private final Random random = new Random();

    public BootstrapServiceImpl()
            throws RemoteException {

        super();
    }

    /**
     * Registers an active worker.
     */
    @Override
    public void registerWorker(int workerId, String host, int port) throws RemoteException {
        if (workerId <= 0) {
            throw new RemoteException("Worker ID must be greater than 0: " + workerId);
        }

        if (host == null || host.trim().isEmpty()) {
            throw new RemoteException("Worker host cannot be empty");
        }

        if (port <= 0 || port > 65535) {
            throw new RemoteException("Invalid worker port: " + port);
        }

        String workerEndpoint = host + ":" + port;

        String existingWorker = registeredWorkers.putIfAbsent(workerId, workerEndpoint);

        if (existingWorker != null) {
            throw new RemoteException("Worker ID " + workerId + " is already registered");
        }

        System.out.println("Worker " + workerId + " registered at " + workerEndpoint);
    }
    public void registerWorker(
            int workerId,
            String host,
            int port)
            throws RemoteException {

        String endpoint =
                workerId +
                "|" +
                host +
                "|" +
                port;

    @Override
    public List<String> getInitialNeighbors(int workerId) throws RemoteException {
        if (!registeredWorkers.containsKey(workerId)) {
            throw new RemoteException("Worker ID " + workerId + " is not registered");
        }

        List<String> candidates = new ArrayList<>();
        for (Map.Entry<Integer, String> worker : registeredWorkers.entrySet()) {
            if (worker.getKey() != workerId) {
                candidates.add(worker.getValue());
            }
        }

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        int index = ThreadLocalRandom.current().nextInt(candidates.size());
        return Collections.singletonList(candidates.get(index));
        registeredWorkers.put(
                workerId,
                endpoint);

        System.out.println(
                "Registered Worker " +
                workerId +
                " at " +
                host +
                ":" +
                port);
    }

    /**
     * Gives a joining worker one random active neighbour.
     */
    @Override
    public List<String> getInitialNeighbors(
            int workerId)
            throws RemoteException {

        List<String> activeWorkers =
                new ArrayList<>(
                        registeredWorkers.values());

        /*
         * Do not connect a worker to itself.
         */
        activeWorkers.removeIf(
                endpoint ->
                        endpoint.startsWith(
                                workerId + "|"));

        List<String> result =
                new ArrayList<>();

        if (!activeWorkers.isEmpty()) {

            String selected =
                    activeWorkers.get(
                            random.nextInt(
                                    activeWorkers.size()));

            result.add(selected);

            System.out.println(
                    "Worker " + workerId +
                    " connected randomly to " +
                    selected);
        }

        return result;
    }

    /**
     * Removes a worker from the active registry.
     */
    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        String workerEndpoint = registeredWorkers.remove(workerId);
        if (workerEndpoint != null) {
            System.out.println("Worker " + workerId + " deregistered from " + workerEndpoint);
        } else {
            System.out.println("Worker " + workerId + " is not registered");
        }
    public void deregisterWorker(
            int workerId)
            throws RemoteException {

        registeredWorkers.remove(
                workerId);

        System.out.println(
                "Worker " + workerId +
                " deregistered.");
    }
}