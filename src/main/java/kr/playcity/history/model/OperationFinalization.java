package kr.playcity.history.model;

import java.util.Objects;
import java.util.UUID;

/** Final operation state after all applied chunks have durable checkpoints. */
public record OperationFinalization(
    UUID operationId,
    long completedAt,
    OperationStatus status,
    int skipped,
    String failure
) {
    public OperationFinalization {
        operationId = Objects.requireNonNull(operationId, "operationId");
        status = Objects.requireNonNull(status, "status");
        failure = failure == null ? "" : failure;
        if (status == OperationStatus.PREPARED) {
            throw new IllegalArgumentException("A finalized operation cannot remain prepared");
        }
        if (skipped < 0) {
            throw new IllegalArgumentException("skipped must not be negative");
        }
    }
}
