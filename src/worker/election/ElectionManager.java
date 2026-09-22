package worker.election;

import common.models.ElectionMessage;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles leader-election message creation and duplicate prevention.
 *
 * Election rules:
 * 1. Lowest JAC wins.
 * 2. If JAC is equal, highest worker ID wins.
 */
public class ElectionManager {

    /*
     * Stores election message IDs that have already been processed.
     *
     * ConcurrentHashMap is used because RMI calls can arrive
     * from multiple workers at the same time.
     */
    private final Set<String> seenMessageIds =
            ConcurrentHashMap.newKeySet();

    /**
     * Checks whether a message was already processed.
     *
     * @return true if the message is a duplicate.
     */
    public boolean isDuplicateAndMark(String messageId) {

        // add() returns false if the ID already exists
        return !seenMessageIds.add(messageId);
    }

    /**
     * Creates a new ELECTION message.
     */
    public ElectionMessage initiateElection(
            int candidateId,
            int jac,
            int term) {

        // Generate a unique ID for this election
        String messageId = UUID.randomUUID().toString();

        return new ElectionMessage(
                messageId,
                ElectionMessage.MessageType.ELECTION,
                candidateId,
                jac,
                term,
                candidateId
        );
    }

    /**
     * Compares the candidate in the message with the local worker.
     *
     * Lower JAC wins.
     * If JAC is equal, higher worker ID wins.
     */
    public ElectionMessage processElectionMessage(
            ElectionMessage message,
            int localId,
            int localJac) {

        boolean localWorkerWins = false;

        // Lower JAC is preferred
        if (localJac < message.getCandidateJac()) {
            localWorkerWins = true;
        }

        // If JAC is equal, higher worker ID wins
        else if (localJac == message.getCandidateJac()
                && localId > message.getCandidateId()) {

            localWorkerWins = true;
        }

        if (localWorkerWins) {

            message.setCandidateId(localId);
            message.setCandidateJac(localJac);
        }

        return message;
    }

    /**
     * Creates a COORDINATOR announcement.
     */
    public ElectionMessage createCoordinatorAnnouncement(
            int electedCoordinatorId,
            int coordinatorJac,
            int term) {

        return new ElectionMessage(
                UUID.randomUUID().toString(),
                ElectionMessage.MessageType.COORDINATOR,
                electedCoordinatorId,
                coordinatorJac,
                term,
                electedCoordinatorId
        );
    }

    /**
     * Clears old election information.
     */
    public void resetElectionState() {

        // Old message IDs are cleared when starting a new term.
        seenMessageIds.clear();
    }
}