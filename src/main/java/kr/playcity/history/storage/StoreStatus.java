package kr.playcity.history.storage;

public record StoreStatus(
    String backend,
    boolean ready,
    boolean accepting,
    boolean healthy,
    int queued,
    long accepted,
    long persisted,
    long compacted,
    long rejected,
    long purged,
    int interruptedOperations,
    String lastError
) {
}
