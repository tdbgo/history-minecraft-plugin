package kr.playcity.history.rollback;

import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record RollbackPreview(
    String token,
    UUID ownerId,
    long expiresAt,
    OperationKind kind,
    String summary,
    UUID inverseOf,
    List<OperationItem> items,
    int sourceChanges,
    int conflicts,
    int alreadyTarget,
    boolean sourceLimitReached
) {
    public RollbackPreview {
        token = Objects.requireNonNull(token, "token");
        ownerId = Objects.requireNonNull(ownerId, "ownerId");
        kind = Objects.requireNonNull(kind, "kind");
        summary = Objects.requireNonNull(summary, "summary");
        items = List.copyOf(items);
    }
}
