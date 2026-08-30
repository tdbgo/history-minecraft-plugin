package kr.playcity.history.storage;

import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationCheckpoint;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.OperationFinalization;
import kr.playcity.history.model.OperationHeader;
import kr.playcity.history.model.OperationSummary;
import kr.playcity.history.model.StoredOperation;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface HistoryStore {
    boolean append(ChangeRecord change);

    /**
     * Appends a FAWE change with storage backpressure. Implementations may wait
     * on a non-server worker instead of rejecting a recoverable burst.
     */
    default boolean appendWorldEdit(ChangeRecord change) {
        return append(change);
    }

    /**
     * Appends an applied FAWE batch with bounded worker-side backpressure.
     * Implementations must not call this waiting path from the server thread.
     */
    default boolean appendWorldEditBatch(List<ChangeRecord> changes) {
        for (ChangeRecord change : changes) {
            if (!appendWorldEdit(change)) {
                return false;
            }
        }
        return true;
    }

    /** Attempts post-application capture without waiting for queue space. */
    default boolean tryAppendWorldEdit(ChangeRecord change) {
        return append(change);
    }

    /**
     * Attempts to enqueue one already-applied WorldEdit/FAWE chunk atomically.
     * A false result means that none of the supplied records were admitted.
     * Implementations must not make the caller wait for storage headroom.
     */
    default boolean tryAppendWorldEditBatch(List<ChangeRecord> changes) {
        for (ChangeRecord change : changes) {
            if (!tryAppendWorldEdit(change)) {
                return false;
            }
        }
        return true;
    }

    /** Records a known or estimated capture gap without stopping world edits. */
    default void reportCaptureGap(long estimatedChanges, String source, String reason) {
    }

    /** Tracks a FAWE callback that has not reached post-processing yet. */
    default void beginExternalCapture(UUID observationId, long estimatedChanges, String source) {
    }

    /** Completes a previously tracked callback. Duplicate completion is harmless. */
    default void completeExternalCapture(UUID observationId) {
    }

    /**
     * Ends a callback that cannot be confirmed and records its potential gap.
     * Duplicate abandonment is harmless.
     */
    default void abandonExternalCapture(UUID observationId, String reason) {
    }

    CompletableFuture<List<ChangeRecord>> query(HistoryQuery query);

    default CompletableFuture<Void> scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Streaming rollback scan is unavailable"));
    }

    default CompletableFuture<java.util.Map<BlockPosition, ChangeRecord>> latestChanges(
        List<BlockPosition> positions
    ) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Latest-state lookup is unavailable"));
    }

    CompletableFuture<Void> prepareOperation(OperationDraft operation);

    default CompletableFuture<Void> prepareOperation(
        OperationHeader operation,
        OperationItemSource items,
        int batchSize
    ) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Streaming operation preparation is unavailable"));
    }

    default CompletableFuture<Void> checkpointOperation(OperationCheckpoint checkpoint) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Operation checkpoints are unavailable"));
    }

    default CompletableFuture<Void> finalizeOperation(OperationFinalization finalization) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Operation finalization is unavailable"));
    }

    CompletableFuture<Void> completeOperation(OperationCompletion completion);

    CompletableFuture<Optional<StoredOperation>> loadOperation(UUID operationId);

    CompletableFuture<Optional<StoredOperation>> findLastOperation(UUID actorId);

    default CompletableFuture<Optional<OperationSummary>> loadOperationSummary(UUID operationId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Operation summaries are unavailable"));
    }

    default CompletableFuture<Optional<OperationSummary>> findLastOperationSummary(UUID actorId) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Operation summaries are unavailable"));
    }

    default CompletableFuture<Void> scanAppliedOperationItems(UUID operationId, OperationItemSink sink) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Operation item streaming is unavailable"));
    }

    default CompletableFuture<Void> scanPendingOperationItems(UUID operationId, OperationItemSink sink) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Pending operation streaming is unavailable"));
    }

    default CompletableFuture<List<UUID>> interruptedOperationIds(int limit) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Interrupted operation listing is unavailable"));
    }

    default CompletableFuture<StorageProfile> storageProfile() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Storage profiling is unavailable"));
    }

    StoreStatus status();

    default CompletableFuture<CaptureRecoveryResult> resumeCapture() {
        return CompletableFuture.completedFuture(new CaptureRecoveryResult(
            false,
            "This History storage does not support capture recovery"
        ));
    }

    CompletableFuture<Void> closeAsync();
}
