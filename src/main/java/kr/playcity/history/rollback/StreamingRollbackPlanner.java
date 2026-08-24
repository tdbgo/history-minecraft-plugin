package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.storage.ChangeRecordSink;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Consolidates a position-ordered database stream while retaining only one
 * block's history in memory. Completed candidates are written to a disk spool.
 */
final class StreamingRollbackPlanner implements ChangeRecordSink {
    private final RequestedRollbackBoundary boundary;
    private final int minimumHeight;
    private final int maximumHeight;
    private final boolean restoreBlockEntityData;
    private final OperationPlanSpool.Writer writer;
    private PositionAccumulator current;
    private BlockPosition lastPosition;
    private ExactChunkCoordinate lastCandidateChunk;
    private long sourceChanges;
    private int candidateCount;
    private int chunkCount;
    private int conflicts;
    private int alreadyTarget;
    private boolean finished;

    StreamingRollbackPlanner(
        RequestedRollbackBoundary boundary,
        int minimumHeight,
        int maximumHeight,
        boolean restoreBlockEntityData,
        OperationPlanSpool.Writer writer
    ) {
        this.boundary = Objects.requireNonNull(boundary, "boundary");
        if (minimumHeight >= maximumHeight) {
            throw new IllegalArgumentException("World height bounds are invalid");
        }
        this.minimumHeight = minimumHeight;
        this.maximumHeight = maximumHeight;
        this.restoreBlockEntityData = restoreBlockEntityData;
        this.writer = Objects.requireNonNull(writer, "writer");
    }

    @Override
    public void accept(ChangeRecord change) {
        Objects.requireNonNull(change, "change");
        if (finished) {
            throw new IllegalStateException("Rollback planner is already finished");
        }
        if (!change.cause().rollbackEligible()) {
            throw new IllegalStateException("Rollback scan returned an audit-only change");
        }
        if (!boundary.contains(change.position())) {
            throw new IllegalStateException("Storage returned a change outside the requested rollback boundary");
        }
        if (change.id() <= 0L) {
            throw new IllegalStateException("Rollback scan returned a change without a durable ID");
        }
        sourceChanges++;
        if (!change.position().equals(lastPosition)) {
            emitCurrent();
            current = new PositionAccumulator(change);
            lastPosition = change.position();
        } else {
            current.accept(change);
        }
    }

    Result finish() {
        if (finished) {
            throw new IllegalStateException("Rollback planner is already finished");
        }
        finished = true;
        emitCurrent();
        writer.close();
        if (writer.count() != candidateCount) {
            throw new IllegalStateException("Rollback plan candidate count does not match its spool");
        }
        return new Result(
            writer.path(),
            sourceChanges,
            candidateCount,
            chunkCount,
            conflicts,
            alreadyTarget
        );
    }

    private void emitCurrent() {
        if (current == null) {
            return;
        }
        PositionResult result = current.finish();
        current = null;
        if (result.unsafe()) {
            conflicts++;
            return;
        }
        BlockPosition position = result.position();
        if (position.y() < minimumHeight || position.y() >= maximumHeight) {
            conflicts++;
            return;
        }
        if (!restoreBlockEntityData
            && (result.expected().hasPayload() || result.target().hasPayload())) {
            conflicts++;
            return;
        }
        BlockSnapshot expected = payloadMode(result.expected());
        BlockSnapshot target = payloadMode(result.target());
        if (expected.sameState(target, restoreBlockEntityData)) {
            alreadyTarget++;
            return;
        }
        OperationItem item = new OperationItem(
            candidateCount,
            position,
            expected,
            target,
            result.sourceIds()
        );
        writer.write(item);
        candidateCount = Math.incrementExact(candidateCount);
        ExactChunkCoordinate chunk = ExactChunkCoordinate.from(position);
        if (!chunk.equals(lastCandidateChunk)) {
            chunkCount = Math.incrementExact(chunkCount);
            lastCandidateChunk = chunk;
        }
    }

    private BlockSnapshot payloadMode(BlockSnapshot snapshot) {
        return restoreBlockEntityData ? snapshot : snapshot.withoutPayload();
    }

    record Result(
        Path planFile,
        long sourceChanges,
        int candidateCount,
        int chunkCount,
        int conflicts,
        int alreadyTarget
    ) {
        Result {
            planFile = Objects.requireNonNull(planFile, "planFile");
        }
    }

    private static final class PositionAccumulator {
        private final BlockPosition position;
        private final BlockSnapshot expected;
        private final long newestSourceId;
        private BlockSnapshot target;
        private UUID lastBatchId;
        private BlockSnapshot lastBefore;
        private BlockSnapshot lastAfter;
        private long lastOccurredAt;
        private long lastId;
        private boolean unsafe;

        private PositionAccumulator(ChangeRecord first) {
            position = first.position();
            expected = first.after();
            target = first.before();
            lastBatchId = first.batchId();
            lastBefore = first.before();
            lastAfter = first.after();
            lastOccurredAt = first.occurredAt();
            lastId = first.id();
            newestSourceId = first.id();
        }

        private void accept(ChangeRecord change) {
            if (change.occurredAt() > lastOccurredAt
                || (change.occurredAt() == lastOccurredAt && change.id() >= lastId)) {
                throw new IllegalStateException("Rollback scan is not newest-first inside a block position");
            }
            lastOccurredAt = change.occurredAt();
            lastId = change.id();
            if (isDuplicateBatchCapture(change)) {
                return;
            }
            if (!target.sameState(change.after(), true)) {
                unsafe = true;
                return;
            }
            target = change.before();
            lastBatchId = change.batchId();
            lastBefore = change.before();
            lastAfter = change.after();
        }

        private boolean isDuplicateBatchCapture(ChangeRecord change) {
            return change.batchId() != null
                && change.batchId().equals(lastBatchId)
                && change.before().sameState(lastBefore, true)
                && change.after().sameState(lastAfter, true);
        }

        private PositionResult finish() {
            // The newest durable ID is sufficient for stale-history validation.
            // Total provenance is counted by sourceChanges without retaining an
            // unbounded list for a single frequently edited coordinate.
            return new PositionResult(position, expected, target, List.of(newestSourceId), unsafe);
        }
    }

    private record PositionResult(
        BlockPosition position,
        BlockSnapshot expected,
        BlockSnapshot target,
        List<Long> sourceIds,
        boolean unsafe
    ) {
        private PositionResult {
            sourceIds = List.copyOf(sourceIds);
        }
    }
}
