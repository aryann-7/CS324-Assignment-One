package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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