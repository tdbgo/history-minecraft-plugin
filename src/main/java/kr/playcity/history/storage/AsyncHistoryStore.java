package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.StoredOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class AsyncHistoryStore implements HistoryStore {
    private final HistoryConfig.Storage config;
    private final Logger logger;
    private final ArrayBlockingQueue<ChangeRecord> directQueue;
    private final ArrayBlockingQueue<ChangeRecord> worldEditQueue;
    private final ExecutorService databaseExecutor;
    private final ScheduledExecutorService flushTimer;
    private final HistoryRepository repository;
    private final List<ChangeRecord> retryBatch = new ArrayList<>();
    private final AtomicInteger retrySize = new AtomicInteger();
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private final AtomicBoolean maintenanceScheduled = new AtomicBoolean();
    private final AtomicBoolean errorLogged = new AtomicBoolean();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong compacted = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong purged = new AtomicLong();
    private final AtomicInteger interruptedOperations = new AtomicInteger();
    private final AtomicReference<String> lastError = new AtomicReference<>("");
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
    private final CompletableFuture<Void> readyFuture;
    private boolean preferWorldEdit = true;

    public AsyncHistoryStore(HistoryConfig.Storage config, Logger logger) {
        this.config = config;
        this.logger = logger;
        // Each lane receives the configured headroom. FAWE can no longer consume
        // the capacity required by direct player and server changes.
        this.directQueue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.worldEditQueue = new ArrayBlockingQueue<>(config.queueCapacity());
        this.databaseExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("History-Database").factory()
        );
        this.flushTimer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("History-FlushTimer").factory()
        );
        this.repository = HistoryRepositoryFactory.create(config);
        this.readyFuture = CompletableFuture.runAsync(repository::open, databaseExecutor)
            .thenRun(() -> {
                interruptedOperations.set(repository.interruptedOperationCount());
                ready.set(true);
            })
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    markFailure(failure);
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
            rejected.incrementAndGet();
            healthy.set(false);
            lastError.compareAndSet("", "History write queue is full; logging was halted to expose data loss");
            accepting.set(false);
            logErrorOnce(lastError.get(), null);
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
        if (!accepting.get() || !healthy.get()) {
            rejected.incrementAndGet();
            return false;
        }
        try {
            while (accepting.get() && healthy.get()) {
                if (worldEditQueue.offer(change, 250, TimeUnit.MILLISECONDS)) {
                    accepted.incrementAndGet();
                    if (worldEditQueue.size() >= config.batchSize()) {
                        requestFlush();
                    }
                    return true;
                }
                requestFlush();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        rejected.incrementAndGet();
        return false;
    }

    @Override
    public CompletableFuture<List<ChangeRecord>> query(HistoryQuery query) {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.query(query);
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
    public CompletableFuture<StorageProfile> storageProfile() {
        return onDatabaseThread(() -> {
            flushAllOnDatabaseThread();
            return repository.storageProfile();
        });
    }

    @Override
    public StoreStatus status() {
        return new StoreStatus(
            repository.backendName(),
            ready.get(),
            accepting.get(),
            healthy.get(),
            directQueue.size() + worldEditQueue.size() + retrySize.get(),
            accepted.get(),
            persisted.get(),
            compacted.get(),
            rejected.get(),
            purged.get(),
            interruptedOperations.get(),
            lastError.get()
        );
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> existing = closeFuture.get();
        if (existing != null) {
            return existing;
        }
        accepting.set(false);
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
                    markFailure(failure);
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
                        markFailure(failure);
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
                markFailure(failure);
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
            worldEditQueue.drainTo(batch, config.batchSize());
            preferWorldEdit = false;
        } else {
            directQueue.drainTo(batch, config.batchSize());
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
            healthy.set(true);
            lastError.set("");
            errorLogged.set(false);
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
                    markFailure(failure);
                }
            });
    }

    private void markFailure(Throwable failure) {
        healthy.set(false);
        Throwable root = unwrap(failure);
        String message = root.getClass().getSimpleName()
            + (root.getMessage() == null ? "" : ": " + root.getMessage());
        lastError.set(message);
        logErrorOnce("History storage entered a degraded state", root);
    }

    private void logErrorOnce(String message, Throwable failure) {
        if (!errorLogged.compareAndSet(false, true)) {
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
}
