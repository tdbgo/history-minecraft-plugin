package kr.playcity.history.model;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record OperationDraft(
    UUID id,
    long createdAt,
    ActorRef actor,
    OperationKind kind,
    String summary,
    UUID inverseOf,
    List<OperationItem> items
) {
    public OperationDraft {
        id = Objects.requireNonNull(id, "id");
        actor = Objects.requireNonNull(actor, "actor");
        kind = Objects.requireNonNull(kind, "kind");
        summary = Objects.requireNonNull(summary, "summary");
        items = List.copyOf(items);
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Operation must contain at least one item");
        }
    }
}
