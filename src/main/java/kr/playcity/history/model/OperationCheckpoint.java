package kr.playcity.history.model;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** One exact chunk's applied results, persisted before the next chunk starts. */
public record OperationCheckpoint(
    UUID operationId,
    long checkpointAt,
    List<AppliedOperationItem> applied
) {
    public OperationCheckpoint {
        operationId = Objects.requireNonNull(operationId, "operationId");
        applied = List.copyOf(applied);
        if (applied.isEmpty()) {
            throw new IllegalArgumentException("Operation checkpoint must contain an applied item");
        }
    }
}
