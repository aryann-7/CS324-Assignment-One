package common.interfaces;

import common.models.ElectionMessage;
import common.models.JobResult;
import common.models.JobTask;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for a Worker node in the cluster.
 * Handles task execution, peer-to-peer flooding message relaying, and health checks.
 */
public interface WorkerService extends Remote {

    /**
     * Executes an assigned job task and returns the computed result.
     *
     * @param task Job task specifying computation type and dataset.
     * @return JobResult containing computation outcomes.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    JobResult executeJob(JobTask task) throws RemoteException;

    /**
     * Receives and propagates election messages across the network flooding topology.
     * Used for custom leader election and duplicate message suppression.
     *
     * @param message Election message payload.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    void receiveElectionMessage(ElectionMessage message) throws RemoteException;

    /**
     * Ping / Heartbeat method to verify worker liveness.
     *
     * @return true if the node is alive and responsive.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    boolean ping() throws RemoteException;
}
