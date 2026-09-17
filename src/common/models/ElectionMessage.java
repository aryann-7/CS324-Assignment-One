package common.models;

import java.io.Serializable;

/**
 * Data Transfer Object representing an election flooding message.
 * Propagates across the unstructured graph.
 * Election criteria: Elect active worker with lowest Job Allocation Counter (JAC).
 * Tie-breaker: Highest worker ID (integer).
 */
public class ElectionMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum MessageType {
        ELECTION,
        COORDINATOR
    }

    private String messageId;
    private MessageType type;
    private int candidateId;
    private int candidateJac;
    private int term;
    private int senderId;

    public ElectionMessage() {
    }

    public ElectionMessage(String messageId, MessageType type, int candidateId, int candidateJac, int term, int senderId) {
        this.messageId = messageId;
        this.type = type;
        this.candidateId = candidateId;
        this.candidateJac = candidateJac;
        this.term = term;
        this.senderId = senderId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(int candidateId) {
        this.candidateId = candidateId;
    }

    public int getCandidateJac() {
        return candidateJac;
    }

    public void setCandidateJac(int candidateJac) {
        this.candidateJac = candidateJac;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getSenderId() {
        return senderId;
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }
}

