package common.interfaces;

import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for Worker nodes.
 */
public interface WorkerService extends Remote {

    /**
     * Executes a distributed computation task.
     */
    JobResult executeJob(JobTask task) throws RemoteException;

    /**
     * Receives and propagates ELECTION and COORDINATOR messages.
     */
    void receiveElectionMessage(ElectionMessage message)
            throws RemoteException;

    /**
     * Sends the best election candidate back toward the
     * worker that started the election.
     */
    void receiveElectionReply(ElectionReply reply)
            throws RemoteException;

    /**
     * Checks whether this worker is alive.
     */
    boolean ping() throws RemoteException;
}