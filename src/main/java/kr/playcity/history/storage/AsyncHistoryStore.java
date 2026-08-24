package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AsyncHistoryStore implements HistoryStore {
    private static final int DURABLE_OPERATION_ATTEMPTS = 10;
    private static final long MAXIMUM_OPERATION_RETRY_DELAY_MILLIS = 5_000L;
    private final HistoryConfig.Storage config;
    private final Logger logger;
    private final ArrayBlockingQueue<ChangeRecord> directQueue;
    private final ArrayBlockingQueue<ChangeRecord> worldEditQueue;
    private final Semaphore worldEditCapacity;
    private final ExecutorService databaseExecutor;
    private final ScheduledExecutorService flushTimer;
    private final HistoryRepository repository;
    private final List<ChangeRecord> retryBatch = new ArrayList<>();
    private final AtomicInteger retrySize = new AtomicInteger();
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean storageHealthy = new AtomicBoolean(true);
    private final AtomicBoolean degraded = new AtomicBoolean();
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private final AtomicBoolean maintenanceScheduled = new AtomicBoolean();
    private final AtomicBoolean storageErrorLogged = new AtomicBoolean();
    private final AtomicBoolean captureErrorLogged = new AtomicBoolean();
    private final AtomicBoolean worldEditErrorLogged = new AtomicBoolean();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong compacted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong worldEditCaptureGapEvents = new AtomicLong();
    private final AtomicLong captureGapEvents = new AtomicLong();
    private final AtomicLong captureGapChanges = new AtomicLong();
    private final AtomicLong unknownCaptureGapEvents = new AtomicLong();
    private final AtomicLong worldEditCaptureGapChanges = new AtomicLong();
    private final AtomicLong purged = new AtomicLong();
    private final AtomicInteger interruptedOperations = new AtomicInteger();
    private final AtomicReference<String> storageError = new AtomicReference<>("");
    private final AtomicReference<String> captureError = new AtomicReference<>("");
    private final AtomicReference<String> worldEditError = new AtomicReference<>("");
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private final ConcurrentMap<UUID, ExternalCapture> externalCaptures = new ConcurrentHashMap<>();
    private final CompletableFuture<Void> readyFuture;
    private boolean preferWorldEdit = true;

    public AsyncHistoryStore(HistoryConfig.Storage config, Logger logger) {
        this(config, logger, HistoryRepositoryFactory.create(config));
    }

    AsyncHistoryStore(HistoryConfig.Storage config, Logger logger, HistoryRepository repository) {
        this.config = config;
        this.logger = logger;
        // Each lane receives the configured headroom. FAWE can no longer consume
        // the capacity required by direct player and server changes.
        this.directQueue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.worldEditQueue = new ArrayBlockingQueue<>(config.worldEditQueueCapacity());
        this.worldEditCapacity = new Semaphore(config.worldEditQueueCapacity());
        this.databaseExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("History-Database").factory()
        );
        this.flushTimer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("History-FlushTimer").factory()
        );
        this.repository = repository;
        this.readyFuture = CompletableFuture.runAsync(repository::open, databaseExecutor)
            .thenRun(() -> {
                interruptedOperations.set(repository.interruptedOperationCount());
                ready.set(true);
            })
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    markStorageFailure(failure);
                    accepting.set(false);
                }
            });

        flushTimer.scheduleAtFixedRate(
            this::requestFlush,
            config.flushIntervalMillis(),
            config.flushIntervalMillis(),
            TimeUnit.MILLISECONDS
        );
        if (config.retentionDays() > 0) {
            flushTimer.scheduleAtFixedRate(
                this::requestMaintenance,
                config.maintenanceIntervalMinutes(),
                config.maintenanceIntervalMinutes(),
                TimeUnit.MINUTES
            );
        }
    }

    @Override
    public boolean append(ChangeRecord change) {
        if (!accepting.get()) {
            rejected.incrementAndGet();
            return false;
        }
        boolean offered = directQueue.offer(change);
        if (!offered) {
            requestFlush();
            offered = directQueue.offer(change);
        }
        if (!offered) {
            rejected.incrementAndGet();
            reportCaptureGap(
                1L,
                "direct",
                "직접 변경 기록 대기열이 포화되어 변경 1건을 기록하지 못했습니다. "
                    + "후속 변경 기록은 계속 시도하며 /history status에서 공백을 확인할 수 있습니다."
            );
            return false;
        }
        accepted.incrementAndGet();
        if (directQueue.size() >= config.batchSize()) {
            requestFlush();
        }
        return true;
    }

    @Override
    public boolean appendWorldEdit(ChangeRecord change) {
        return tryAppendWorldEditBatch(List.of(change));
    }

    @Override
    public boolean tryAppendWorldEdit(ChangeRecord change) {
        return tryAppendWorldEditBatch(List.of(change));
    }

    @Override
    public boolean tryAppendWorldEditBatch(List<ChangeRecord> changes) {
        List<ChangeRecord> immutableChanges = List.copyOf(changes);
        if (immutableChanges.isEmpty()) {
            return true;
        }
        int changeCount = immutableChanges.size();
        if (!accepting.get() || !storageHealthy.get()) {
            requestFlush();
            reportCaptureGap(
                changeCount,
                "worldedit",
                "저장소가 변경 기록을 수락할 수 없어 이미 적용된 WorldEdit/FAWE 변경의 기록 공백이 발생했습니다."
            );
            return false;
        }
        if (changeCount > config.worldEditQueueCapacity()) {
            reportCaptureGap(
                changeCount,
                "worldedit",
                "단일 WorldEdit/FAWE 캡처 묶음이 내부 대기열보다 커서 기록하지 못했습니다. "
                    + "편집 결과 자체는 유지됩니다."
            );
            return false;
        }
        boolean reserved = worldEditCapacity.tryAcquire(changeCount);
        if (!reserved) {
            requestFlush();
            reserved = worldEditCapacity.tryAcquire(changeCount);
        }
        if (!reserved) {
            reportCaptureGap(
                changeCount,
                "worldedit",
                "WorldEdit/FAWE 기록 대기열의 즉시 사용 가능한 공간이 부족했습니다. "
                    + "편집은 중단하지 않았으며 해당 변경은 캡처 공백으로 표시됩니다."
            );
            return false;
        }
        try {
            enqueueWorldEditBatch(immutableChanges);
            return true;
        } catch (RuntimeException failure) {
            worldEditCapacity.release(changeCount);
            reportCaptureGap(changeCount, "worldedit", "WorldEdit/FAWE 기록 대기열 내부 오류가 발생했습니다.");
            markStorageFailure(failure);
            return false;
        }
    }

    @Override
    public void reportCaptureGap(long estimatedChanges, String source, String reason) {
        degraded.set(true);
        captureGapEvents.incrementAndGet();
        if (estimatedChanges > 0L) {
            captureGapChanges.addAndGet(estimatedChanges);
        } else {
            unknownCaptureGapEvents.incrementAndGet();
        }
        if ("worldedit".equalsIgnoreCase(source) || "fawe".equalsIgnoreCase(source)) {
            worldEditCaptureGapEvents.incrementAndGet();
            if (estimatedChanges > 0L) {
                worldEditCaptureGapChanges.addAndGet(estimatedChanges);
            }
            worldEditError.set(reason);
            logErrorOnce(worldEditErrorLogged, reason, null);
        } else {
            captureError.set(reason);
            logErrorOnce(captureErrorLogged, reason, null);
        }
    }

    @Override
    public void beginExternalCapture(UUID observationId, long estimatedChanges, String source) {
        if (observationId == null) {
            return;
        }
        externalCaptures.putIfAbsent(
            observationId,
            new ExternalCapture(System.nanoTime(), Math.max(0L, estimatedChanges), source == null ? "" : source)
        );
    }

    @Override
    public void completeExternalCapture(UUID observationId) {
        if (observationId != null) {
            externalCaptures.remove(observationId);
        }
    }

    @Override
    public void abandonExternalCapture(UUID observationId, String reason) {
        if (observationId == null) {
            return;
        }
        ExternalCapture capture = externalCaptures.remove(observationId);
        if (capture != null) {
            reportCaptureGap(capture.estimatedChanges(), capture.source(), reason);
        }
    }

    @Override
    public CompletableFuture<List<ChangeRecord>> query(HistoryQuery query) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.query(query);
        });
    }

    @Override
    public CompletableFuture<Void> scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            repository.scanRollbackChanges(query, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<java.util.Map<BlockPosition, ChangeRecord>> latestChanges(
        List<BlockPosition> positions
    ) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.latestChanges(positions);
        });
    }

    @Override
    public CompletableFuture<Void> prepareOperation(OperationDraft operation) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            repository.prepareOperation(operation);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> prepareOperation(
        OperationHeader operation,
        OperationItemSource items,
        int batchSize
    ) {
        if (batchSize <= 0) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Operation preparation batch size must be positive")
            );
        }
        return this.<Void>onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            repository.prepareOperation(operation, items, batchSize);
            interruptedOperations.set(repository.interruptedOperationCount());
            return null;
        }).whenComplete((unused, failure) -> items.close());
    }

    @Override
    public CompletableFuture<Void> checkpointOperation(OperationCheckpoint checkpoint) {
        return retryingDatabaseOperation(() -> {
            repository.checkpointOperation(checkpoint);
            return null;
        }, "checkpoint History operation");
    }

    @Override
    public CompletableFuture<Void> finalizeOperation(OperationFinalization finalization) {
        return retryingDatabaseOperation(() -> {
            repository.finalizeOperation(finalization);
            interruptedOperations.set(repository.interruptedOperationCount());
            return null;
        }, "finalize History operation");
    }

    @Override
    public CompletableFuture<Void> completeOperation(OperationCompletion completion) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            repository.completeOperation(completion);
            interruptedOperations.set(repository.interruptedOperationCount());
            return null;
        });
    }

    @Override
    public CompletableFuture<Optional<StoredOperation>> loadOperation(UUID operationId) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.loadOperation(operationId);
        });
    }

    @Override
    public CompletableFuture<Optional<StoredOperation>> findLastOperation(UUID actorId) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.findLastOperation(actorId);
        });
    }

    @Override
    public CompletableFuture<Optional<OperationSummary>> loadOperationSummary(UUID operationId) {
        return onDatabaseThread(() -> repository.loadOperationSummary(operationId));
    }

    @Override
    public CompletableFuture<Optional<OperationSummary>> findLastOperationSummary(UUID actorId) {
        return onDatabaseThread(() -> repository.findLastOperationSummary(actorId));
    }

    @Override
    public CompletableFuture<Void> scanAppliedOperationItems(UUID operationId, OperationItemSink sink) {
        return onDatabaseThread(() -> {
            repository.scanAppliedOperationItems(operationId, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> scanPendingOperationItems(UUID operationId, OperationItemSink sink) {
        return onDatabaseThread(() -> {
            repository.scanPendingOperationItems(operationId, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<UUID>> interruptedOperationIds(int limit) {
        if (limit <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Interrupted operation limit must be positive"));
        }
        return onDatabaseThread(() -> repository.interruptedOperationIds(limit));
    }

    @Override
    public CompletableFuture<StorageProfile> storageProfile() {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.storageProfile();
        });
    }

    @Override
    public StoreStatus status() {
        ReservationDiagnostics reservations = reservationDiagnostics();
        return new StoreStatus(
            repository.backendName(),
            ready.get(),
            accepting.get(),
            storageHealthy.get(),
            degraded.get(),
            directQueue.size() + worldEditQueue.size() + retrySize.get(),
            reservations.count(),
            reservations.changes(),
            reservations.oldestAgeMillis(),
            reservations.oldestId(),
            accepted.get(),
            persisted.get(),
            compacted.get(),
            rejected.get(),
            captureGapEvents.get(),
            captureGapChanges.get(),
            unknownCaptureGapEvents.get(),
            worldEditCaptureGapEvents.get(),
            worldEditCaptureGapChanges.get(),
            purged.get(),
            interruptedOperations.get(),
            latestIssue()
        );
    }

    @Override
    public CompletableFuture<CaptureRecoveryResult> resumeCapture() {
        if (closeFuture.get() != null) {
            return CompletableFuture.completedFuture(new CaptureRecoveryResult(
                false,
                "History is closing; capture cannot be resumed"
            ));
        }
        return readyFuture.handleAsync((unused, startupFailure) -> {
            if (startupFailure != null || !ready.get()) {
                return new CaptureRecoveryResult(
                    false,
                    "History storage is not ready; resolve the startup failure first"
                );
            }
            if (!degraded.get()) {
                return new CaptureRecoveryResult(
                    false,
                    "History capture is not in an active degraded state"
                );
            }
            try {
                flushAllOnDatabaseThread();
                interruptedOperations.set(repository.interruptedOperationCount());
            } catch (RuntimeException failure) {
                markStorageFailure(failure);
                return new CaptureRecoveryResult(
                    false,
                    "History storage verification failed; capture remains halted"
                );
            }
            if (hasPendingWrites() || !externalCaptures.isEmpty()) {
                return new CaptureRecoveryResult(
                    false,
                    "History still has database writes or unconfirmed external-edit callbacks; wait for them to drain"
                );
            }
            storageHealthy.set(true);
            storageError.set("");
            accepting.set(true);
            degraded.set(false);
            captureError.set(
                "Capture returned to normal after verification; " + captureGapEvents.get()
                    + " earlier capture gap events remain visible for this server run"
            );
            logger.warning(
                "History capture returned to normal after verification; earlier gaps remain visible in /history status"
            );
            return new CaptureRecoveryResult(
                true,
                "History capture resumed. The earlier rejected-change gap remains recorded in status."
            );
        }, databaseExecutor);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> existing = closeFuture.get();
        if (existing != null) {
            return existing;
        }
        accepting.set(false);
        abandonAllExternalCaptures("서버 종료 전에 FAWE 후처리 콜백 완료를 확인하지 못했습니다.");
        flushTimer.shutdown();
        CompletableFuture<Void> created = readyFuture.handle((unused, failure) -> null)
            .thenRunAsync(() -> {
                if (ready.get()) {
                    flushAllOnDatabaseThread();
                    repository.close();
                }
            }, databaseExecutor)
            .whenComplete((unused, failure) -> {
                databaseExecutor.shutdown();
                if (failure != null) {
                    markStorageFailure(failure);
                }
            });
        if (closeFuture.compareAndSet(null, created)) {
            return created;
        }
        return closeFuture.get();
    }

    private void requestFlush() {
        if (hasPendingWrites() && flushScheduled.compareAndSet(false, true)) {
            readyFuture.thenRunAsync(this::flushOneBatchOnDatabaseThread, databaseExecutor)
                .whenComplete((unused, failure) -> {
                    flushScheduled.set(false);
                    if (failure != null) {
                        markStorageFailure(failure);
                    } else if (!worldEditQueue.isEmpty() || directQueue.size() >= config.batchSize()) {
                        requestFlush();
                    }
                });
        }
    }

    private void requestMaintenance() {
        if (config.retentionDays() <= 0 || !ready.get()
            || !maintenanceScheduled.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            long retentionMillis = TimeUnit.DAYS.toMillis(config.retentionDays());
            long cutoff = System.currentTimeMillis() - retentionMillis;
            int deleted = repository.purgeChangesBefore(cutoff, config.purgeBatchSize());
            purged.addAndGet(deleted);
        }, databaseExecutor).whenComplete((unused, failure) -> {
            maintenanceScheduled.set(false);
            if (failure != null) {
                markStorageFailure(failure);
            }
        });
    }

    private void flushAllOnDatabaseThread() {
        while (hasPendingWrites()) {
            flushOneBatchOnDatabaseThread();
        }
    }

    private void flushOneBatchOnDatabaseThread() {
        List<ChangeRecord> batch = new ArrayList<>();
        if (!retryBatch.isEmpty()) {
            batch.addAll(retryBatch);
            retryBatch.clear();
            retrySize.set(0);
        } else if ((preferWorldEdit && !worldEditQueue.isEmpty()) || directQueue.isEmpty()) {
            int drained = worldEditQueue.drainTo(
                batch,
                adaptiveBatchSize(worldEditQueue.size(), config.worldEditQueueCapacity())
            );
            worldEditCapacity.release(drained);
            preferWorldEdit = false;
        } else {
            directQueue.drainTo(batch, adaptiveBatchSize(directQueue.size(), config.queueCapacity()));
            preferWorldEdit = true;
        }
        if (batch.isEmpty()) {
            return;
        }
        try {
            List<ChangeRecord> compactedBatch = WorldEditBatchCompactor.compact(batch);
            repository.insertBatch(compactedBatch);
            persisted.addAndGet(compactedBatch.size());
            compacted.addAndGet(batch.size() - compactedBatch.size());
            storageHealthy.set(true);
            storageError.set("");
            storageErrorLogged.set(false);
        } catch (RuntimeException failure) {
            retryBatch.addAll(batch);
            retrySize.set(retryBatch.size());
            throw failure;
        }
    }

    private boolean hasPendingWrites() {
        return !directQueue.isEmpty() || !worldEditQueue.isEmpty() || !retryBatch.isEmpty();
    }

    private <T> CompletableFuture<T> onDatabaseThread(DatabaseOperation<T> operation) {
        return readyFuture.thenApplyAsync(unused -> operation.run(), databaseExecutor)
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    markStorageFailure(failure);
                }
            });
    }

    private CompletableFuture<Void> retryingDatabaseOperation(
        DatabaseOperation<Void> operation,
        String description
    ) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        readyFuture.whenComplete((unused, startupFailure) -> {
            if (startupFailure != null) {
                result.completeExceptionally(startupFailure);
            } else {
                attemptDatabaseOperation(operation, description, 1, result);
            }
        });
        return result;
    }

    private void attemptDatabaseOperation(
        DatabaseOperation<Void> operation,
        String description,
        int attempt,
        CompletableFuture<Void> result
    ) {
        if (result.isDone()) {
            return;
        }
        try {
            databaseExecutor.execute(() -> {
                try {
                    operation.run();
                    storageHealthy.set(true);
                    storageError.set("");
                    storageErrorLogged.set(false);
                    result.complete(null);
                } catch (RuntimeException failure) {
                    markStorageFailure(failure);
                    if (attempt >= DURABLE_OPERATION_ATTEMPTS || closeFuture.get() != null) {
                        result.completeExceptionally(new StorageException(
                            "Unable to " + description + " after " + attempt + " attempts",
                            failure
                        ));
                        return;
                    }
                    long delay = Math.min(
                        MAXIMUM_OPERATION_RETRY_DELAY_MILLIS,
                        100L << Math.min(attempt - 1, 6)
                    );
                    try {
                        flushTimer.schedule(
                            () -> attemptDatabaseOperation(
                                operation,
                                description,
                                attempt + 1,
                                result
                            ),
                            delay,
                            TimeUnit.MILLISECONDS
                        );
                    } catch (RuntimeException schedulingFailure) {
                        failure.addSuppressed(schedulingFailure);
                        result.completeExceptionally(failure);
                    }
                }
            });
        } catch (RuntimeException rejected) {
            result.completeExceptionally(rejected);
        }
    }

    private void markStorageFailure(Throwable failure) {
        storageHealthy.set(false);
        Throwable root = unwrap(failure);
        String message = root.getClass().getSimpleName()
            + (root.getMessage() == null ? "" : ": " + root.getMessage());
        storageError.set(message);
        logErrorOnce(storageErrorLogged, "History storage entered a degraded state", root);
    }

    private void enqueueWorldEditBatch(List<ChangeRecord> changes) {
        if (worldEditQueue.remainingCapacity() < changes.size()) {
            failWorldEditBatchInvariant();
        }
        for (ChangeRecord change : changes) {
            if (!worldEditQueue.offer(change)) {
                failWorldEditBatchInvariant();
            }
        }
        accepted.addAndGet(changes.size());
        worldEditError.set("");
        worldEditErrorLogged.set(false);
        if (worldEditQueue.size() >= config.batchSize()) {
            requestFlush();
        }
    }

    private void failWorldEditBatchInvariant() {
        rejected.incrementAndGet();
        reportCaptureGap(
            0L,
            "worldedit",
            "WorldEdit 기록 대기열의 내부 일관성 오류로 적용된 변경 일부의 기록 여부를 확인할 수 없습니다."
        );
        throw new IllegalStateException("Acquired History WorldEdit batch capacity was unavailable");
    }

    private ReservationDiagnostics reservationDiagnostics() {
        long now = System.nanoTime();
        long changes = 0L;
        long oldestStarted = Long.MAX_VALUE;
        String oldestId = "";
        for (var entry : externalCaptures.entrySet()) {
            ExternalCapture capture = entry.getValue();
            changes += capture.estimatedChanges();
            if (capture.startedNanos() < oldestStarted) {
                oldestStarted = capture.startedNanos();
                oldestId = entry.getKey().toString();
            }
        }
        long ageMillis = oldestStarted == Long.MAX_VALUE
            ? 0L
            : TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - oldestStarted));
        return new ReservationDiagnostics(externalCaptures.size(), changes, ageMillis, oldestId);
    }

    private void abandonAllExternalCaptures(String reason) {
        for (UUID observationId : List.copyOf(externalCaptures.keySet())) {
            abandonExternalCapture(observationId, reason);
        }
    }

    private int adaptiveBatchSize(int queued, int capacity) {
        int multiplier;
        if (queued >= capacity * 3 / 4) {
            multiplier = 16;
        } else if (queued >= capacity / 2) {
            multiplier = 8;
        } else if (queued >= capacity / 4) {
            multiplier = 4;
        } else {
            multiplier = 1;
        }
        long scaled = (long) config.batchSize() * multiplier;
        return (int) Math.min(capacity, Math.min(8_192L, scaled));
    }

    private String latestIssue() {
        String capture = captureError.get();
        String storage = storageError.get();
        if (!capture.isBlank() && !storage.isBlank()) {
            return capture + " Storage error: " + storage;
        }
        if (!capture.isBlank()) {
            return capture;
        }
        if (!storage.isBlank()) {
            return storage;
        }
        return worldEditError.get();
    }

    private void logErrorOnce(AtomicBoolean gate, String message, Throwable failure) {
        if (!gate.compareAndSet(false, true)) {
            return;
        }
        if (failure == null) {
            logger.severe(message);
        } else {
            logger.log(Level.SEVERE, message, failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null
            && (current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)) {
            current = current.getCause();
        }
        return current;
    }

    @FunctionalInterface
    private interface DatabaseOperation<T> {
        T run();
    }

    private record ExternalCapture(long startedNanos, long estimatedChanges, String source) {
    }

    private record ReservationDiagnostics(int count, long changes, long oldestAgeMillis, String oldestId) {
    }
}
