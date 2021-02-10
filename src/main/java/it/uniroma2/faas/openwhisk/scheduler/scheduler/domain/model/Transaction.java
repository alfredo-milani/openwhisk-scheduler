package it.uniroma2.faas.openwhisk.scheduler.scheduler.domain.model;

public class Transaction {

    private final String id;
    private final Long timestamp;

    public Transaction(String id, Long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

}