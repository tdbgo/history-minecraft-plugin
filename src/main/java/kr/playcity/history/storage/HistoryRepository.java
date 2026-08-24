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

interface HistoryRepository {
    void open();

    void insertBatch(List<ChangeRecord> changes);

    List<ChangeRecord> query(HistoryQuery query);

    default void scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        throw new UnsupportedOperationException("Streaming rollback scan is unavailable");
    }

    java.util.Map<BlockPosition, ChangeRecord> latestChanges(List<BlockPosition> positions);

    void prepareOperation(OperationDraft operation);

    default void prepareOperation(OperationHeader operation, OperationItemSource items, int batchSize) {
        throw new UnsupportedOperationException("Streaming operation preparation is unavailable");
    }

    default void checkpointOperation(OperationCheckpoint checkpoint) {
        throw new UnsupportedOperationException("Operation checkpoints are unavailable");
    }

    default void finalizeOperation(OperationFinalization finalization) {
        throw new UnsupportedOperationException("Operation finalization is unavailable");
    }

    void completeOperation(OperationCompletion completion);

    Optional<StoredOperation> loadOperation(UUID operationId);

    Optional<StoredOperation> findLastOperation(UUID actorId);

    default Optional<OperationSummary> loadOperationSummary(UUID operationId) {
        throw new UnsupportedOperationException("Operation summaries are unavailable");
    }

    default Optional<OperationSummary> findLastOperationSummary(UUID actorId) {
        throw new UnsupportedOperationException("Operation summaries are unavailable");
    }

    default void scanAppliedOperationItems(UUID operationId, OperationItemSink sink) {
        throw new UnsupportedOperationException("Operation item streaming is unavailable");
    }

    default void scanPendingOperationItems(UUID operationId, OperationItemSink sink) {
        throw new UnsupportedOperationException("Pending operation streaming is unavailable");
    }

    default List<UUID> interruptedOperationIds(int limit) {
        throw new UnsupportedOperationException("Interrupted operation listing is unavailable");
    }

    int interruptedOperationCount();

    int purgeChangesBefore(long cutoffMillis, int limit);

    StorageProfile storageProfile();

    String backendName();

    void close();
}
