package kr.playcity.history.rollback;

import kr.playcity.history.model.OperationKind;

import java.util.Objects;
import java.util.UUID;

public record RollbackPreview(
    String token,
    UUID ownerId,
    long expiresAt,
    OperationKind kind,
    String summary,
    UUID inverseOf,
    int itemCount,
    int chunkCount,
    long sourceChanges,
    int conflicts,
    int alreadyTarget
) {
    public RollbackPreview {
        token = Objects.requireNonNull(token, "token");
        ownerId = Objects.requireNonNull(ownerId, "ownerId");
        kind = Objects.requireNonNull(kind, "kind");
        summary = Objects.requireNonNull(summary, "summary");
        if (itemCount < 0 || chunkCount < 0 || sourceChanges < 0 || conflicts < 0 || alreadyTarget < 0) {
            throw new IllegalArgumentException("Rollback preview counters must not be negative");
        }
    }
}
