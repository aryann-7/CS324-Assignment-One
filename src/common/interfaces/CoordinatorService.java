package common.interfaces;

import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for the active Coordinator node in the cluster.
 * Handles client job submissions, job delegation to workers, and coordinator
 * heartbeat/announcements.
 */
public interface CoordinatorService extends Remote {

    // Submits a batch computation job from the client.
    JobResult submitJob(JobRequest request) throws RemoteException;

    // Receives heartbeat or coordinator management messages from cluster nodes.
    void handleCoordinatorMessage(CoordinatorMessage message) throws RemoteException;

    // Queries the current status of the coordinator including the Job Allocation
    // Counter (JAC) and term.
    String getCoordinatorStatus() throws RemoteException;
}