package kr.playcity.history.model;

import java.util.Objects;

public record StoredOperation(
    OperationDraft draft,
    OperationStatus status,
    int appliedCount,
    int skippedCount,
    String failure
) {
    public StoredOperation {
        draft = Objects.requireNonNull(draft, "draft");
        status = Objects.requireNonNull(status, "status");
        failure = failure == null ? "" : failure;
    }
}
