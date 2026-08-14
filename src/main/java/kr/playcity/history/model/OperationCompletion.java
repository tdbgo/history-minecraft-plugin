package kr.playcity.history.model;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record OperationCompletion(
    UUID operationId,
    long completedAt,
    OperationStatus status,
    List<AppliedOperationItem> applied,
    int skipped,
    String failure
) {
    public OperationCompletion {
        operationId = Objects.requireNonNull(operationId, "operationId");
        status = Objects.requireNonNull(status, "status");
        applied = List.copyOf(applied);
        failure = failure == null ? "" : failure;
        if (skipped < 0) {
            throw new IllegalArgumentException("skipped must not be negative");
        }
    }
}
