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
import java.util.concurrent.CompletableFuture;

public interface HistoryStore {
    boolean append(ChangeRecord change);

    CompletableFuture<List<ChangeRecord>> query(HistoryQuery query);

    default CompletableFuture<java.util.Map<BlockPosition, ChangeRecord>> latestChanges(
        List<BlockPosition> positions
    ) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Latest-state lookup is unavailable"));
    }

    CompletableFuture<Void> prepareOperation(OperationDraft operation);

    CompletableFuture<Void> completeOperation(OperationCompletion completion);

    CompletableFuture<Optional<StoredOperation>> loadOperation(UUID operationId);

    CompletableFuture<Optional<StoredOperation>> findLastOperation(UUID actorId);

    default CompletableFuture<StorageProfile> storageProfile() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Storage profiling is unavailable"));
    }

    StoreStatus status();

    CompletableFuture<Void> closeAsync();
}
