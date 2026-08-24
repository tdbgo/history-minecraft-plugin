package kr.playcity.history.model;

import java.util.Objects;
import java.util.UUID;

/** Durable operation metadata written before any world mutation begins. */
public record OperationHeader(
    UUID id,
    long createdAt,
    ActorRef actor,
    OperationKind kind,
    String summary,
    UUID inverseOf,
    int itemCount
) {
    public OperationHeader {
        id = Objects.requireNonNull(id, "id");
        actor = Objects.requireNonNull(actor, "actor");
        kind = Objects.requireNonNull(kind, "kind");
        summary = Objects.requireNonNull(summary, "summary");
        if (itemCount <= 0) {
            throw new IllegalArgumentException("Operation must contain at least one item");
        }
    }

    public static OperationHeader from(OperationDraft draft) {
        Objects.requireNonNull(draft, "draft");
        return new OperationHeader(
            draft.id(),
            draft.createdAt(),
            draft.actor(),
            draft.kind(),
            draft.summary(),
            draft.inverseOf(),
            draft.items().size()
        );
    }
}
