package kr.playcity.history.rollback;

import kr.playcity.history.model.OperationStatus;

import java.util.UUID;

public record OperationRunResult(
    UUID operationId,
    OperationStatus status,
    int applied,
    int skipped,
    String failure
) {
}
