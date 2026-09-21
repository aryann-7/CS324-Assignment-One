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

//Implementation of the BootstrapService remote interface.
//Tracks active worker registries and connects joining workers randomly to an active worker.
//Must NOT participate in election or compute jobs.

public class BootstrapServiceImpl extends UnicastRemoteObject implements BootstrapService {
    private static final long serialVersionUID = 1L;

    // Map tracking active workers: workerId (int) -> network endpoint
    // (e.g.host:port).

    private final Map<Integer, String> registeredWorkers = new ConcurrentHashMap<>();

    public BootstrapServiceImpl() throws RemoteException {
        super();
    }

    // Registers a new worker node with the bootstrap service.

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

    // Connects joining worker randomly to an active worker.

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
    }

    // Deregisters or removes a worker node upon shutdown or failure.

    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        // Method stub: remove worker from registry map
    }
}
