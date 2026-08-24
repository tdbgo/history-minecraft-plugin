package kr.playcity.history.model;

import java.util.Objects;

/** Operation metadata and counters without materializing its block items. */
public record OperationSummary(
    OperationHeader header,
    OperationStatus status,
    int appliedCount,
    int skippedCount,
    String failure
) {
    public OperationSummary {
        header = Objects.requireNonNull(header, "header");
        status = Objects.requireNonNull(status, "status");
        failure = failure == null ? "" : failure;
        if (appliedCount < 0 || skippedCount < 0) {
            throw new IllegalArgumentException("Operation counters must not be negative");
        }
    }
}
