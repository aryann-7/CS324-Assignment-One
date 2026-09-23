package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
     * Worker ID -> endpoint ("workerId|host|port").
     */
    private final Map<Integer, String> registeredWorkers =
            new ConcurrentHashMap<>();

    public BootstrapServiceImpl()
            throws RemoteException {

        super();
    }

    /**
     * Registers an active worker.
     */
    @Override
    public void registerWorker(
            int workerId,
            String host,
            int port)
            throws RemoteException {

        if (workerId <= 0) {
            throw new RemoteException("Worker ID must be greater than 0: " + workerId);
        }

        if (host == null || host.trim().isEmpty()) {
            throw new RemoteException("Worker host cannot be empty");
        }

        if (port <= 0 || port > 65535) {
            throw new RemoteException("Invalid worker port: " + port);
        }

        String endpoint = workerId + "|" + host + "|" + port;

        String existing = registeredWorkers.putIfAbsent(workerId, endpoint);
        if (existing != null) {
            throw new RemoteException("Worker ID " + workerId + " is already registered");
        }

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

        if (!registeredWorkers.containsKey(workerId)) {
            throw new RemoteException("Worker ID " + workerId + " is not registered");
        }

        List<String> candidates = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : registeredWorkers.entrySet()) {
            if (entry.getKey() != workerId) {
                candidates.add(entry.getValue());
            }
        }

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        int index = ThreadLocalRandom.current().nextInt(candidates.size());
        String selected = candidates.get(index);

        System.out.println(
                "Worker " + workerId +
                " connected randomly to " +
                selected);

        return Collections.singletonList(selected);
    }

    /**
     * Removes a worker from the active registry.
     */
    @Override
    public void deregisterWorker(
            int workerId)
            throws RemoteException {

        String removed = registeredWorkers.remove(workerId);
        if (removed != null) {
            System.out.println(
                    "Worker " + workerId +
                    " deregistered from " +
                    removed);
        } else {
            System.out.println(
                    "Worker " + workerId +
                    " is not registered");
        }
    }
}