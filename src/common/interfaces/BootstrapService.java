package common.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

//Remote interface for the Bootstrap Node.
//Tracks active worker registries and connects joining workers randomly to an active worker.
//Must NOT participate in election or compute jobs.

public interface BootstrapService extends Remote {

    // Registers a new worker node with the bootstrap service.
    void registerWorker(int workerId, String host, int port) throws RemoteException;

    /*
     * Retrieves assigned active worker-neighbor connection for topology formation.
     * Connects joining worker randomly to an active worker.
     */
    List<String> getInitialNeighbors(int workerId) throws RemoteException;

    // Deregisters or removes a worker node upon shutdown or failure.
    void deregisterWorker(int workerId) throws RemoteException;
}