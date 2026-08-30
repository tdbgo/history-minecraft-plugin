package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.ChangeCause;
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
import java.util.concurrent.CompletionException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
    private final ExecutorService readExecutor;
    private final ScheduledExecutorService flushTimer;
    private final HistoryRepository writeRepository;
    private final HistoryRepository readRepository;
    private final boolean separateReadRepository;
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
    private final ReentrantReadWriteLock admissionLifecycle = new ReentrantReadWriteLock();
    private final Lock admissionRead = admissionLifecycle.readLock();
    private final Lock admissionWrite = admissionLifecycle.writeLock();
    private final CompletableFuture<Void> writeReadyFuture;
    private final CompletableFuture<Void> readyFuture;
    private boolean preferWorldEdit = true;

    public AsyncHistoryStore(HistoryConfig.Storage config, Logger logger) {
        this(
            config,
            logger,
            HistoryRepositoryFactory.create(config),
            HistoryRepositoryFactory.create(config),
            true
        );
    }

    AsyncHistoryStore(HistoryConfig.Storage config, Logger logger, HistoryRepository repository) {
        this(config, logger, repository, repository, false);
    }

    AsyncHistoryStore(
        HistoryConfig.Storage config,
        Logger logger,
        HistoryRepository writeRepository,
        HistoryRepository readRepository
    ) {
        this(config, logger, writeRepository, readRepository, true);
    }

    private AsyncHistoryStore(
        HistoryConfig.Storage config,
        Logger logger,
        HistoryRepository writeRepository,
        HistoryRepository readRepository,
        boolean separateReadRepository
    ) {
        this.config = config;
        this.logger = logger;
        // Each lane receives the configured headroom. FAWE can no longer consume
        // the capacity required by direct player and server changes.
        this.directQueue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.worldEditQueue = new ArrayBlockingQueue<>(config.worldEditQueueCapacity());
        this.worldEditCapacity = new Semaphore(config.worldEditQueueCapacity(), true);
        this.databaseExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("History-Database").factory()
        );
        this.readExecutor = separateReadRepository
            ? Executors.newSingleThreadExecutor(Thread.ofPlatform().name("History-Database-Read").factory())
            : databaseExecutor;
        this.flushTimer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("History-FlushTimer").factory()
        );
        this.writeRepository = writeRepository;
        this.readRepository = readRepository;
        this.separateReadRepository = separateReadRepository;
        this.writeReadyFuture = CompletableFuture.runAsync(() -> {
            writeRepository.open();
            interruptedOperations.set(writeRepository.interruptedOperationCount());
        }, databaseExecutor);
        this.readyFuture = writeReadyFuture
            .thenCompose(unused -> separateReadRepository
                ? CompletableFuture.runAsync(readRepository::open, readExecutor)
                : CompletableFuture.completedFuture(null))
            .thenRun(() -> {
                ready.set(true);
            })
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    failStartup(failure);
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
        admissionRead.lock();
        try {
            return appendDirect(change);
        } finally {
            admissionRead.unlock();
        }
    }

    private boolean appendDirect(ChangeRecord change) {
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
        return appendWorldEditBatch(List.of(change));
    }

    @Override
    public boolean appendWorldEditBatch(List<ChangeRecord> changes) {
        List<ChangeRecord> immutableChanges = List.copyOf(changes);
        if (immutableChanges.isEmpty()) {
            return true;
        }
        int capacity = config.worldEditQueueCapacity();
        for (int start = 0; start < immutableChanges.size(); start += capacity) {
            int end = Math.min(immutableChanges.size(), start + capacity);
            if (!appendWorldEditBatchWithBackpressure(immutableChanges.subList(start, end))) {
                int unattempted = immutableChanges.size() - end;
                if (unattempted > 0) {
                    reportCaptureGap(
                        unattempted,
                        "worldedit",
                        "앞선 WorldEdit/FAWE 기록 구간을 수락하지 못해 남은 변경을 기록하지 않았습니다. "
                            + "편집 결과 자체는 유지됩니다."
                    );
                }
                return false;
            }
        }
        return true;
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
        admissionRead.lock();
        try {
            return tryAppendWorldEditBatchAfterStartupCheck(immutableChanges);
        } finally {
            admissionRead.unlock();
        }
    }

    private boolean tryAppendWorldEditBatchAfterStartupCheck(List<ChangeRecord> immutableChanges) {
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

    private boolean appendWorldEditBatchWithBackpressure(List<ChangeRecord> changes) {
        int changeCount = changes.size();
        admissionRead.lock();
        try {
            if (!accepting.get() || !storageHealthy.get()) {
                reportCaptureGap(
                    changeCount,
                    "worldedit",
                    "저장소가 변경 기록을 수락할 수 없어 이미 적용된 WorldEdit/FAWE 변경의 기록 공백이 발생했습니다."
                );
                return false;
            }
            requestFlush();
            boolean reserved;
            try {
                reserved = worldEditCapacity.tryAcquire(
                    changeCount,
                    config.worldEditAdmissionTimeoutMillis(),
                    TimeUnit.MILLISECONDS
                );
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                reportCaptureGap(
                    changeCount,
                    "worldedit",
                    "WorldEdit/FAWE 기록 공간을 기다리던 작업이 중단되었습니다. 편집 결과는 유지됩니다."
                );
                return false;
            }
            if (!reserved) {
                reportCaptureGap(
                    changeCount,
                    "worldedit",
                    "WorldEdit/FAWE 기록 대기열이 " + config.worldEditAdmissionTimeoutMillis()
                        + "ms 안에 비워지지 않았습니다. 편집 결과는 유지됩니다."
                );
                return false;
            }
            if (!accepting.get() || !storageHealthy.get()) {
                worldEditCapacity.release(changeCount);
                reportCaptureGap(
                    changeCount,
                    "worldedit",
                    "대기 중 저장소가 비정상 상태로 전환되어 WorldEdit/FAWE 기록을 수락하지 못했습니다."
                );
                return false;
            }
            try {
                enqueueWorldEditBatch(changes);
                return true;
            } catch (RuntimeException failure) {
                worldEditCapacity.release(changeCount);
                reportCaptureGap(changeCount, "worldedit", "WorldEdit/FAWE 기록 대기열 내부 오류가 발생했습니다.");
                markStorageFailure(failure);
                return false;
            }
        } finally {
            admissionRead.unlock();
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
        return onReadThread(() -> readRepository.query(query));
    }

    @Override
    public CompletableFuture<Void> scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        return onReadThread(() -> {
            readRepository.scanRollbackChanges(query, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<java.util.Map<BlockPosition, ChangeRecord>> latestChanges(
        List<BlockPosition> positions
    ) {
        return onReadThread(() -> readRepository.latestChanges(positions));
    }

    @Override
    public CompletableFuture<Void> prepareOperation(OperationDraft operation) {
        long acceptedBarrier = accepted.get();
        return onDatabaseThread(() -> {
            flushAcceptedThroughOnDatabaseThread(acceptedBarrier);
            writeRepository.prepareOperation(operation);
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
        long acceptedBarrier = accepted.get();
        return this.<Void>onDatabaseThread(() -> {
            flushAcceptedThroughOnDatabaseThread(acceptedBarrier);
            writeRepository.prepareOperation(operation, items, batchSize);
            interruptedOperations.set(writeRepository.interruptedOperationCount());
            return null;
        }).whenComplete((unused, failure) -> items.close());
    }

    @Override
    public CompletableFuture<Void> checkpointOperation(OperationCheckpoint checkpoint) {
        return retryingDatabaseOperation(() -> {
            writeRepository.checkpointOperation(checkpoint);
            return null;
        }, "checkpoint History operation");
    }

    @Override
    public CompletableFuture<Void> finalizeOperation(OperationFinalization finalization) {
        return retryingDatabaseOperation(() -> {
            writeRepository.finalizeOperation(finalization);
            interruptedOperations.set(writeRepository.interruptedOperationCount());
            return null;
        }, "finalize History operation");
    }

    @Override
    public CompletableFuture<Void> completeOperation(OperationCompletion completion) {
        long acceptedBarrier = accepted.get();
        return onDatabaseThread(() -> {
            flushAcceptedThroughOnDatabaseThread(acceptedBarrier);
            writeRepository.completeOperation(completion);
            interruptedOperations.set(writeRepository.interruptedOperationCount());
            return null;
        });
    }

    @Override
    public CompletableFuture<Optional<StoredOperation>> loadOperation(UUID operationId) {
        return onReadThread(() -> readRepository.loadOperation(operationId));
    }

    @Override
    public CompletableFuture<Optional<StoredOperation>> findLastOperation(UUID actorId) {
        return onReadThread(() -> readRepository.findLastOperation(actorId));
    }

    @Override
    public CompletableFuture<Optional<OperationSummary>> loadOperationSummary(UUID operationId) {
        return onReadThread(() -> readRepository.loadOperationSummary(operationId));
    }

    @Override
    public CompletableFuture<Optional<OperationSummary>> findLastOperationSummary(UUID actorId) {
        return onReadThread(() -> readRepository.findLastOperationSummary(actorId));
    }

    @Override
    public CompletableFuture<Void> scanAppliedOperationItems(UUID operationId, OperationItemSink sink) {
        return onReadThread(() -> {
            readRepository.scanAppliedOperationItems(operationId, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> scanPendingOperationItems(UUID operationId, OperationItemSink sink) {
        return onReadThread(() -> {
            readRepository.scanPendingOperationItems(operationId, sink);
            return null;
        });
    }

    @Override
    public CompletableFuture<List<UUID>> interruptedOperationIds(int limit) {
        if (limit <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Interrupted operation limit must be positive"));
        }
        return onReadThread(() -> readRepository.interruptedOperationIds(limit));
    }

    @Override
    public CompletableFuture<StorageProfile> storageProfile() {
        return onReadThread(readRepository::storageProfile);
    }

    @Override
    public StoreStatus status() {
        ReservationDiagnostics reservations = reservationDiagnostics();
        return new StoreStatus(
            writeRepository.backendName(),
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
                interruptedOperations.set(writeRepository.interruptedOperationCount());
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
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!closeFuture.compareAndSet(null, result)) {
            return closeFuture.get();
        }
        admissionWrite.lock();
        try {
            accepting.set(false);
        } finally {
            admissionWrite.unlock();
        }
        abandonAllExternalCaptures("서버 종료 전에 FAWE 후처리 콜백 완료를 확인하지 못했습니다.");
        flushTimer.shutdown();
        CompletableFuture<Void> writeClose = readyFuture.handle((unused, failure) -> null)
            .thenRunAsync(() -> {
                if (ready.get()) {
                    RuntimeException flushFailure = null;
                    try {
                        flushAllOnDatabaseThread();
                    } catch (RuntimeException failure) {
                        flushFailure = failure;
                        discardPendingWritesAfterFinalFlushFailure();
                    }
                    try {
                        writeRepository.close();
                    } catch (RuntimeException closeFailure) {
                        if (flushFailure != null) {
                            flushFailure.addSuppressed(closeFailure);
                        } else {
                            flushFailure = closeFailure;
                        }
                    }
                    if (flushFailure != null) {
                        throw flushFailure;
                    }
                } else {
                    writeRepository.close();
                }
            }, databaseExecutor);
        writeClose.handleAsync((unused, writeFailure) -> {
            Throwable failure = writeFailure == null ? null : unwrap(writeFailure);
            if (separateReadRepository) {
                try {
                    readRepository.close();
                } catch (RuntimeException readFailure) {
                    if (failure == null) {
                        failure = readFailure;
                    } else {
                        failure.addSuppressed(readFailure);
                    }
                }
            }
            if (failure != null) {
                throw new CompletionException(failure);
            }
            return null;
        }, readExecutor)
            .whenComplete((unused, failure) -> {
                databaseExecutor.shutdown();
                if (separateReadRepository) {
                    readExecutor.shutdown();
                }
                if (failure != null) {
                    markStorageFailure(failure);
                    result.completeExceptionally(unwrap(failure));
                } else {
                    result.complete(null);
                }
            });
        return result;
    }

    private void requestFlush() {
        if (accepting.get() && closeFuture.get() == null
            && hasPendingWrites() && flushScheduled.compareAndSet(false, true)) {
            writeReadyFuture.thenRunAsync(this::flushOneBatchOnDatabaseThread, databaseExecutor)
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
        if (config.retentionDays() <= 0 || !ready.get() || !accepting.get() || closeFuture.get() != null
            || !maintenanceScheduled.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture.runAsync(() -> {
            long retentionMillis = TimeUnit.DAYS.toMillis(config.retentionDays());
            long cutoff = System.currentTimeMillis() - retentionMillis;
            int deleted = writeRepository.purgeChangesBefore(cutoff, config.purgeBatchSize());
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

    /**
     * Persists every capture accepted before a caller took its barrier without
     * waiting forever for newer live traffic. This gives reads and operation
     * preparation a stable invocation-time view on continuously busy servers.
     */
    private void flushAcceptedThroughOnDatabaseThread(long acceptedBarrier) {
        while (processedAcceptedCount() < acceptedBarrier) {
            if (!hasPendingWrites()) {
                throw new StorageException(
                    "History accepted-write accounting fell behind its persistence barrier"
                );
            }
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
            writeRepository.insertBatch(compactedBatch);
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

    private long processedAcceptedCount() {
        return persisted.get() + compacted.get();
    }

    private <T> CompletableFuture<T> onDatabaseThread(DatabaseOperation<T> operation) {
        return writeReadyFuture.thenApplyAsync(unused -> operation.run(), databaseExecutor)
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    markStorageFailure(failure);
                }
            });
    }

    private <T> CompletableFuture<T> onReadThread(DatabaseOperation<T> operation) {
        long acceptedBarrier = accepted.get();
        return readyFuture
            .thenCompose(unused -> CompletableFuture.runAsync(
                () -> flushAcceptedThroughOnDatabaseThread(acceptedBarrier),
                databaseExecutor
            ))
            .thenApplyAsync(unused -> operation.run(), readExecutor)
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
        writeReadyFuture.whenComplete((unused, startupFailure) -> {
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

    private void failStartup(Throwable failure) {
        admissionWrite.lock();
        try {
            accepting.set(false);
        } finally {
            admissionWrite.unlock();
        }
        markStorageFailure(failure);
        CompletableFuture.runAsync(() -> discardPendingWrites(
            "저장소 시작에 실패하여 시작 중 수락했던 직접 변경을 저장하지 못했습니다. "
                + "원인을 해결하고 서버를 정상 재시작하십시오.",
            "저장소 시작에 실패하여 시작 중 수락했던 WorldEdit/FAWE 변경을 저장하지 못했습니다. "
                + "편집 결과는 유지되지만 해당 기록은 복구할 수 없습니다."
        ), databaseExecutor).whenComplete((unused, cleanupFailure) -> {
            if (cleanupFailure != null) {
                markStorageFailure(cleanupFailure);
            }
        });
    }

    private static long drainLostStartupChanges(ArrayBlockingQueue<ChangeRecord> queue) {
        long drained = 0L;
        while (queue.poll() != null) {
            drained++;
        }
        return drained;
    }

    private void discardPendingWritesAfterFinalFlushFailure() {
        discardPendingWrites(
            "서버 종료 시 저장소의 최종 기록에 실패하여 직접 변경을 저장하지 못했습니다. "
                + "직전 저장소 오류를 확인하십시오.",
            "서버 종료 시 저장소의 최종 기록에 실패하여 WorldEdit/FAWE 변경을 저장하지 못했습니다. "
                + "편집 결과는 유지되지만 해당 기록은 복구할 수 없습니다."
        );
    }

    private void discardPendingWrites(String directReason, String worldEditReason) {
        long directLost = drainLostStartupChanges(directQueue);
        long worldEditLost = drainLostStartupChanges(worldEditQueue);
        worldEditCapacity.release((int) worldEditLost);
        for (ChangeRecord change : retryBatch) {
            if (change.cause() == ChangeCause.WORLD_EDIT) {
                worldEditLost++;
            } else {
                directLost++;
            }
        }
        retryBatch.clear();
        retrySize.set(0);
        if (directLost > 0L) {
            reportCaptureGap(
                directLost,
                "direct",
                directReason + " 손실 " + directLost + "건."
            );
        }
        if (worldEditLost > 0L) {
            reportCaptureGap(
                worldEditLost,
                "worldedit",
                worldEditReason + " 손실 " + worldEditLost + "건."
            );
        }
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
