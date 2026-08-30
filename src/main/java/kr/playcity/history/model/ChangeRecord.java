package kr.playcity.history.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ChangeRecord(
    long id,
    long occurredAt,
    BlockPosition position,
    ActorRef actor,
    ChangeCause cause,
    BlockSnapshot before,
    BlockSnapshot after,
    UUID operationId,
    UUID batchId,
    String metadata,
    UUID captureId
) {
    public ChangeRecord {
        position = Objects.requireNonNull(position, "position");
        actor = Objects.requireNonNull(actor, "actor");
        cause = Objects.requireNonNull(cause, "cause");
        before = Objects.requireNonNull(before, "before");
        after = Objects.requireNonNull(after, "after");
        metadata = metadata == null ? "" : metadata;
        if (occurredAt < 0L) {
            throw new IllegalArgumentException("occurredAt must not be negative");
        }
    }

    public ChangeRecord(
        long id,
        long occurredAt,
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID operationId,
        String metadata
    ) {
        this(
            id, occurredAt, position, actor, cause, before, after, operationId, null, metadata,
            id == 0L ? UUID.randomUUID() : null
        );
    }

    /** Compatibility constructor for callers that do not provide a durable capture identity. */
    public ChangeRecord(
        long id,
        long occurredAt,
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID operationId,
        UUID batchId,
        String metadata
    ) {
        this(
            id, occurredAt, position, actor, cause, before, after, operationId, batchId, metadata,
            id == 0L ? UUID.randomUUID() : null
        );
    }

    public static ChangeRecord captured(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        String metadata
    ) {
        return new ChangeRecord(
            0L,
            Instant.now().toEpochMilli(),
            position,
            actor,
            cause,
            before,
            after,
            null,
            null,
            metadata,
            UUID.randomUUID()
        );
    }

    public static ChangeRecord capturedInBatch(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID batchId,
        String metadata
    ) {
        return new ChangeRecord(
            0L,
            Instant.now().toEpochMilli(),
            position,
            actor,
            cause,
            before,
            after,
            null,
            batchId,
            metadata,
            UUID.randomUUID()
        );
    }

    public ChangeRecord withId(long persistedId) {
        return new ChangeRecord(
            persistedId,
            occurredAt,
            position,
            actor,
            cause,
            before,
            after,
            operationId,
            batchId,
            metadata,
            captureId
        );
    }
}
