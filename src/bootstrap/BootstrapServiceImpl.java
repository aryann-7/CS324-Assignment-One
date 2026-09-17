package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
        // Method stub: record worker information in active registry
    }

    // Connects joining worker randomly to an active worker.

    @Override
    public List<String> getInitialNeighbors(int workerId) throws RemoteException {
        // Method stub: select an active worker randomly and return as neighbor
        return Collections.emptyList();
    }

    // Deregisters or removes a worker node upon shutdown or failure.

    @Override
    public void deregisterWorker(int workerId) throws RemoteException {
        // Method stub: remove worker from registry map
    }
}
