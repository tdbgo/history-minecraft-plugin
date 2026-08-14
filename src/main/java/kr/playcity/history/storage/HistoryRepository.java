package kr.playcity.history.storage;

import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.StoredOperation;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

interface HistoryRepository {
    void open();

    void insertBatch(List<ChangeRecord> changes);

    List<ChangeRecord> query(HistoryQuery query);

    java.util.Map<BlockPosition, ChangeRecord> latestChanges(List<BlockPosition> positions);

    void prepareOperation(OperationDraft operation);

    void completeOperation(OperationCompletion completion);

    Optional<StoredOperation> loadOperation(UUID operationId);

    Optional<StoredOperation> findLastOperation(UUID actorId);

    int interruptedOperationCount();

    int purgeChangesBefore(long cutoffMillis, int limit);

    StorageProfile storageProfile();

    String backendName();

    void close();
}
