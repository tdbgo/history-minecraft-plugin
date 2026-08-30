package kr.playcity.history.rollback;

import kr.playcity.history.capture.SnapshotCodec;
import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.AppliedOperationItem;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.OperationCheckpoint;
import kr.playcity.history.model.OperationFinalization;
import kr.playcity.history.model.OperationHeader;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import kr.playcity.history.model.OperationStatus;
import kr.playcity.history.model.OperationSummary;
import kr.playcity.history.storage.HistoryStore;
import kr.playcity.history.storage.StoreStatus;
import kr.playcity.history.util.DurationParser;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Streaming, exact-chunk rollback engine. */
public final class RollbackService {
    private static final int WORLDWIDE_RADIUS = 42_500_000;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30L;

    private final JavaPlugin plugin;
    private final HistoryConfig.Rollback config;
    private final HistoryStore store;
    private final SnapshotCodec snapshots;
    private final LatestHistoryValidator latestHistoryValidator = new LatestHistoryValidator();
    private final RecoveryReconciler recoveryReconciler = new RecoveryReconciler();
    private final PreviewRegistry previews;
    private final ActivePositionGuard positionGuard;
    private final ScopedChunkLeaseManager chunkLeases;
    private final ExecutorService planExecutor;
    private final Path planDirectory;
    private final Set<Execution> activeExecutions = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<OperationRunResult>> operationResults = ConcurrentHashMap.newKeySet();
    private final Object executionLifecycle = new Object();
    private final Set<PendingStart> pendingStarts = new HashSet<>();
    private volatile boolean shutdownRequested;
    private CompletableFuture<Void> shutdownFuture;

    public RollbackService(
        JavaPlugin plugin,
        HistoryConfig.Rollback config,
        HistoryStore store,
        SnapshotCodec snapshots,
        RollbackPlanner planner,
        PreviewRegistry previews,
        ActivePositionGuard positionGuard
    ) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        Objects.requireNonNull(planner, "planner");
        this.previews = Objects.requireNonNull(previews, "previews");
        this.positionGuard = Objects.requireNonNull(positionGuard, "positionGuard");
        this.chunkLeases = new ScopedChunkLeaseManager(
            plugin,
            config.maxConcurrentChunkLeases(),
            config.chunkLoadTimeoutSeconds(),
            config.generateMissingChunks()
        );
        this.planExecutor = Executors.newSingleThreadExecutor(
            Thread.ofPlatform().name("History-RollbackPlan").factory()
        );
        this.planDirectory = OperationPlanSpool.prepareDirectory(plugin.getDataFolder().toPath());
    }

    public CompletableFuture<RollbackPreview> createRollbackPreview(
        Player player,
        String actor,
        Duration duration,
        int radius
    ) {
        Location center = player.getLocation();
        return createRollbackPreview(
            player,
            actor,
            duration,
            center.getWorld(),
            center.getBlockX(),
            center.getBlockZ(),
            radius,
            "반경 " + radius
        );
    }

    public CompletableFuture<RollbackPreview> createGlobalRollbackPreview(
        Player player,
        String actor,
        Duration duration
    ) {
        return createRollbackPreview(
            player,
            actor,
            duration,
            player.getWorld(),
            0,
            0,
            WORLDWIDE_RADIUS,
            "현재 월드 전체"
        );
    }

    private CompletableFuture<RollbackPreview> createRollbackPreview(
        Player player,
        String actor,
        Duration duration,
        World world,
        int centerX,
        int centerZ,
        int radius,
        String scopeLabel
    ) {
        requireServerThread();
        requireRollbackAvailable();
        UUID ownerId = player.getUniqueId();
        long since = Instant.now().minus(duration).toEpochMilli();
        HistoryQuery query = HistoryQuery.nearby(
            world.getUID(),
            centerX,
            centerZ,
            radius,
            since,
            actor,
            config.planningFetchSize()
        ).forRollback();
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(
            world.getUID(), centerX, centerZ, radius
        );
        String actorLabel = actor == null || actor.equals("*") ? "모든 원인" : actor;
        String summary = actorLabel + " · 최근 " + DurationParser.compact(duration) + " · " + scopeLabel;
        OperationPlanSpool.Writer writer = OperationPlanSpool.create(planDirectory);
        StreamingRollbackPlanner planner = new StreamingRollbackPlanner(
            boundary,
            world.getMinHeight(),
            world.getMaxHeight(),
            config.restoreBlockEntityData(),
            writer
        );
        return store.scanRollbackChanges(query, planner)
            .thenApply(unused -> planner.finish())
            .thenApply(result -> registerPreview(
                ownerId, OperationKind.ROLLBACK, summary, null, result
            ))
            .whenComplete((preview, failure) -> {
                if (failure != null) {
                    closeWriterAfterFailure(writer, failure);
                    deletePlanAfterFailure(writer.path(), failure);
                }
            });
    }

    public CompletableFuture<RollbackPreview> createUndoPreview(Player player, UUID operationId) {
        requireServerThread();
        requireRollbackAvailable();
        UUID ownerId = player.getUniqueId();
        CompletableFuture<Optional<OperationSummary>> operationFuture = operationId == null
            ? store.findLastOperationSummary(ownerId)
            : store.loadOperationSummary(operationId);
        return operationFuture.thenCompose(optional -> {
            OperationSummary stored = optional.orElseThrow(
                () -> new IllegalArgumentException("되돌릴 History 작업을 찾지 못했습니다.")
            );
            if (stored.status() != OperationStatus.APPLIED && stored.status() != OperationStatus.PARTIAL) {
                throw new IllegalArgumentException("완료된 작업만 취소할 수 있습니다.");
            }
            OperationPlanSpool.Writer writer = OperationPlanSpool.create(planDirectory);
            UndoSpoolBuilder builder = new UndoSpoolBuilder(writer);
            return store.scanAppliedOperationItems(stored.header().id(), builder::accept)
                .thenApply(unused -> builder.finish())
                .thenApply(result -> {
                    if (result.candidateCount() != stored.appliedCount()) {
                        throw new IllegalStateException("Undo plan did not contain every applied operation item");
                    }
                    return registerPreview(
                        ownerId,
                        OperationKind.UNDO,
                        "작업 " + shortId(stored.header().id()) + " 취소",
                        stored.header().id(),
                        result
                    );
                })
                .whenComplete((preview, failure) -> {
                    if (failure != null) {
                        closeWriterAfterFailure(writer, failure);
                        deletePlanAfterFailure(writer.path(), failure);
                    }
                });
        });
    }

    private RollbackPreview registerPreview(
        UUID ownerId,
        OperationKind kind,
        String summary,
        UUID inverseOf,
        StreamingRollbackPlanner.Result result
    ) {
        RollbackPreview preview = new RollbackPreview(
            "",
            ownerId,
            System.currentTimeMillis() + config.previewTtlSeconds() * 1_000L,
            kind,
            summary,
            inverseOf,
            result.candidateCount(),
            result.chunkCount(),
            result.sourceChanges(),
            result.conflicts(),
            result.alreadyTarget()
        );
        if (preview.itemCount() == 0) {
            OperationPlanSpool.delete(result.planFile());
            return preview;
        }
        return previews.register(preview, result.planFile());
    }

    public CompletableFuture<OperationRunResult> apply(Player player, String token) {
        requireRollbackAvailable();
        PreparedRollbackPreview prepared = previews.consume(token, player.getUniqueId())
            .orElseThrow(() -> new IllegalArgumentException("미리보기가 없거나 만료되었습니다."));
        RollbackPreview preview = prepared.preview();
        String requiredPermission = preview.kind() == OperationKind.ROLLBACK
            ? "history.rollback"
            : "history.undo";
        if (!player.hasPermission(requiredPermission)) {
            OperationPlanSpool.delete(prepared.planFile());
            throw new IllegalArgumentException("이 작업을 적용할 권한이 없습니다.");
        }
        if (preview.itemCount() == 0) {
            OperationPlanSpool.delete(prepared.planFile());
            throw new IllegalArgumentException("적용할 안전한 변경이 없습니다.");
        }

        OperationHeader header = new OperationHeader(
            UUID.randomUUID(),
            System.currentTimeMillis(),
            ActorRef.player(player.getUniqueId(), player.getName()),
            preview.kind(),
            preview.summary(),
            preview.inverseOf(),
            preview.itemCount()
        );
        OperationPlanSpool.Reader preparationSource = OperationPlanSpool.open(prepared.planFile());
        CompletableFuture<OperationRunResult> result = new CompletableFuture<>();
        trackOperation(result);
        store.prepareOperation(header, preparationSource, config.operationWriteBatchSize())
            .whenComplete((unused, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(deletePlan(prepared.planFile(), failure));
                    return;
                }
                scheduleExecution(new PendingStart(
                    header, prepared.planFile(), result, false, 0
                ));
            });
        return result;
    }

    /** Resumes a durable PREPARED operation from its database item stream. */
    public CompletableFuture<OperationRunResult> recover(Player player, UUID operationId) {
        requireServerThread();
        Objects.requireNonNull(player, "player");
        Objects.requireNonNull(operationId, "operationId");
        CompletableFuture<OperationRunResult> recovery = store.loadOperationSummary(operationId).thenCompose(optional -> {
            OperationSummary summary = optional.orElseThrow(
                () -> new IllegalArgumentException("복구할 History 작업을 찾지 못했습니다.")
            );
            if (summary.status() != OperationStatus.PREPARED) {
                throw new IllegalArgumentException("PREPARED 상태의 중단 작업만 복구할 수 있습니다.");
            }
            OperationPlanSpool.Writer writer = OperationPlanSpool.create(planDirectory);
            RecoverySpoolBuilder builder = new RecoverySpoolBuilder(writer);
            return store.scanPendingOperationItems(operationId, builder::accept)
                .thenCompose(unused -> {
                    StreamingRollbackPlanner.Result pending = builder.finish();
                    if (pending.candidateCount() == 0) {
                        OperationStatus completed = summary.appliedCount() == summary.header().itemCount()
                            ? OperationStatus.APPLIED
                            : OperationStatus.PARTIAL;
                        int skipped = summary.header().itemCount() - summary.appliedCount();
                        return store.finalizeOperation(new OperationFinalization(
                            operationId,
                            System.currentTimeMillis(),
                            completed,
                            skipped,
                            summary.failure()
                        )).thenApply(ignored -> new OperationRunResult(
                            operationId, completed, summary.appliedCount(), skipped, summary.failure()
                        )).whenComplete((result, failure) -> OperationPlanSpool.delete(pending.planFile()));
                    }
                    CompletableFuture<OperationRunResult> result = new CompletableFuture<>();
                    scheduleExecution(new PendingStart(
                        summary.header(), pending.planFile(), result, true, summary.appliedCount()
                    ));
                    return result;
                }).whenComplete((result, failure) -> {
                    if (failure != null) {
                        closeWriterAfterFailure(writer, failure);
                        deletePlanAfterFailure(writer.path(), failure);
                    }
                });
        });
        return trackOperation(recovery);
    }

    public boolean cancelPreview(Player player, String token) {
        return previews.cancel(token, player.getUniqueId());
    }

    public CompletableFuture<Void> shutdown() {
        requireServerThread();
        if (shutdownFuture != null) {
            return shutdownFuture;
        }
        List<PendingStart> starts;
        synchronized (executionLifecycle) {
            shutdownRequested = true;
            starts = List.copyOf(pendingStarts);
            pendingStarts.clear();
        }
        Throwable shutdownFailure = null;
        for (PendingStart pending : starts) {
            try {
                pending.cancelTask();
                cancelBeforeExecution(pending);
            } catch (RuntimeException failure) {
                shutdownFailure = combineFailures(shutdownFailure, failure);
                pending.result.completeExceptionally(failure);
            }
        }
        try {
            previews.close();
        } catch (RuntimeException failure) {
            shutdownFailure = combineFailures(shutdownFailure, failure);
        }
        for (Execution execution : List.copyOf(activeExecutions)) {
            try {
                execution.abort("plugin-disabled");
            } catch (RuntimeException failure) {
                shutdownFailure = combineFailures(shutdownFailure, failure);
                execution.result.completeExceptionally(failure);
            }
        }
        try {
            chunkLeases.close();
        } catch (RuntimeException failure) {
            shutdownFailure = combineFailures(shutdownFailure, failure);
        }
        CompletableFuture<Void> completion = CompletableFuture.allOf(
            List.copyOf(operationResults).toArray(CompletableFuture[]::new)
        );
        if (shutdownFailure != null) {
            Throwable terminalFailure = shutdownFailure;
            completion = completion.handle((unused, operationFailure) -> {
                if (operationFailure != null) {
                    terminalFailure.addSuppressed(operationFailure);
                }
                throw new java.util.concurrent.CompletionException(terminalFailure);
            });
        }
        shutdownFuture = completion
            .orTimeout(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .whenComplete((unused, failure) -> {
                if (failure == null) {
                    planExecutor.shutdown();
                } else {
                    planExecutor.shutdownNow();
                }
            });
        return shutdownFuture;
    }

    private CompletableFuture<OperationRunResult> trackOperation(
        CompletableFuture<OperationRunResult> result
    ) {
        operationResults.add(result);
        result.whenComplete((unused, failure) -> operationResults.remove(result));
        return result;
    }

    private void scheduleExecution(PendingStart pending) {
        boolean cancel;
        RuntimeException schedulingFailure = null;
        synchronized (executionLifecycle) {
            cancel = shutdownRequested || !plugin.isEnabled();
            if (!cancel && !Bukkit.isPrimaryThread()) {
                pendingStarts.add(pending);
                try {
                    pending.task = Bukkit.getScheduler().runTask(plugin, () -> startPendingExecution(pending));
                    return;
                } catch (RuntimeException failure) {
                    pendingStarts.remove(pending);
                    cancel = true;
                    schedulingFailure = failure;
                }
            }
        }
        if (cancel) {
            cancelBeforeExecution(pending, schedulingFailure);
        } else {
            startExecution(pending);
        }
    }

    private void startPendingExecution(PendingStart pending) {
        requireServerThread();
        boolean cancel;
        synchronized (executionLifecycle) {
            if (!pendingStarts.remove(pending)) {
                return;
            }
            pending.task = null;
            cancel = shutdownRequested || !plugin.isEnabled();
        }
        if (cancel) {
            cancelBeforeExecution(pending, null);
        } else {
            startExecution(pending);
        }
    }

    private void startExecution(PendingStart pending) {
        requireServerThread();
        Execution execution = new Execution(
            pending.header,
            pending.planFile,
            pending.result,
            pending.recovering,
            pending.alreadyApplied
        );
        activeExecutions.add(execution);
        execution.start();
    }

    private void cancelBeforeExecution(PendingStart pending) {
        cancelBeforeExecution(pending, null);
    }

    private void cancelBeforeExecution(PendingStart pending, Throwable cause) {
        if (pending.recovering) {
            Throwable terminalCause = deletePlan(pending.planFile, cause);
            pending.result.completeExceptionally(recoveryFailure(
                pending.header.id(),
                "플러그인 종료로 중단 작업 복구를 시작하지 못했습니다.",
                terminalCause
            ));
        } else {
            finalizeWithoutExecution(pending.header, pending.planFile, pending.result);
        }
    }

    private void finalizeWithoutExecution(
        OperationHeader header,
        Path planFile,
        CompletableFuture<OperationRunResult> result
    ) {
        OperationFinalization finalization = new OperationFinalization(
            header.id(),
            System.currentTimeMillis(),
            OperationStatus.FAILED,
            header.itemCount(),
            "plugin-disabled-before-execution"
        );
        store.finalizeOperation(finalization).whenComplete((unused, failure) -> {
            Throwable terminalFailure = deletePlan(planFile, failure);
            if (terminalFailure != null) {
                result.completeExceptionally(terminalFailure);
            } else {
                result.complete(new OperationRunResult(
                    header.id(), OperationStatus.FAILED, 0, header.itemCount(), finalization.failure()
                ));
            }
        });
    }

    private CompletableFuture<java.util.Map<BlockPosition, LatestHistoryValidator.LatestState>>
        validateLatestHistory(List<OperationItem> items) {
        List<BlockPosition> positions = items.stream().map(OperationItem::position).toList();
        return store.latestChanges(positions).thenApply(changes -> {
            java.util.Map<BlockPosition, LatestHistoryValidator.LatestState> latest = new java.util.HashMap<>();
            for (ChangeRecord change : changes.values()) {
                latest.put(
                    change.position(),
                    new LatestHistoryValidator.LatestState(
                        change.id(), change.after(), change.operationId()
                    )
                );
            }
            return java.util.Map.copyOf(latest);
        });
    }

    private BlockSnapshot payloadMode(BlockSnapshot snapshot) {
        return config.restoreBlockEntityData() ? snapshot : snapshot.withoutPayload();
    }

    private final class Execution {
        private final OperationHeader header;
        private final Path planFile;
        private final CompletableFuture<OperationRunResult> result;
        private final List<AppliedOperationItem> appliedInChunk = new ArrayList<>();
        private OperationPlanSpool.Reader reader;
        private CompletableFuture<OperationPlanSpool.PlanChunk> pendingPlanRead;
        private CompletableFuture<ScopedChunkLeaseManager.Lease> pendingLease;
        private OperationPlanSpool.PlanChunk currentChunk;
        private ExactMutationScope currentScope;
        private ActivePositionGuard.Watch currentWatch;
        private ScopedChunkLeaseManager.Lease activeLease;
        private BukkitTask tickTask;
        private int itemCursor;
        private int appliedTotal;
        private String firstFailure = "";
        private boolean terminalRequested;
        private boolean checkpointInFlight;
        private boolean finalizationStarted;
        private final boolean recovering;

        private Execution(
            OperationHeader header,
            Path planFile,
            CompletableFuture<OperationRunResult> result,
            boolean recovering,
            int alreadyApplied
        ) {
            this.header = header;
            this.planFile = planFile;
            this.result = result;
            this.recovering = recovering;
            this.appliedTotal = alreadyApplied;
        }

        private void start() {
            requireServerThread();
            readNextChunk();
        }

        private void readNextChunk() {
            if (terminalRequested || finalizationStarted) {
                checkpointOrFinalize(false);
                return;
            }
            pendingPlanRead = CompletableFuture.supplyAsync(() -> {
                if (reader == null) {
                    reader = OperationPlanSpool.open(planFile);
                }
                return reader.readChunk();
            }, planExecutor);
            pendingPlanRead.whenComplete((chunk, failure) -> runOnServerForExecution(() -> {
                pendingPlanRead = null;
                if (failure != null) {
                    fail("rollback-plan-read-failed", failure);
                } else if (terminalRequested) {
                    checkpointOrFinalize(false);
                } else if (chunk == null) {
                    requestFinish("");
                } else {
                    beginChunk(chunk);
                }
            }));
        }

        private void beginChunk(OperationPlanSpool.PlanChunk chunk) {
            requireServerThread();
            try {
                currentChunk = chunk;
                currentScope = ExactMutationScope.create(chunk.items());
                currentScope.requireChunkAllowed(chunk.coordinate());
                currentWatch = positionGuard.watch(
                    chunk.items().stream().map(OperationItem::position).toList()
                );
                itemCursor = 0;
                appliedInChunk.clear();
                pendingLease = chunkLeases.acquire(currentScope, chunk.coordinate());
                pendingLease.whenComplete((lease, failure) -> runOnServerForExecution(
                    () -> handleLease(chunk.coordinate(), lease, failure)
                ));
            } catch (RuntimeException failure) {
                fail("chunk-scope-violation", failure);
            }
        }

        private void handleLease(
            ExactChunkCoordinate target,
            ScopedChunkLeaseManager.Lease lease,
            Throwable failure
        ) {
            pendingLease = null;
            if (terminalRequested) {
                if (lease != null) {
                    lease.close();
                }
                checkpointOrFinalize(false);
                return;
            }
            if (failure != null) {
                fail("chunk-load-failed", failure);
                return;
            }
            try {
                if (lease == null || !lease.coordinate().equals(target)) {
                    throw new IllegalStateException("Loaded chunk does not match its exact work group");
                }
                currentScope.requireChunkAllowed(lease.coordinate());
                lease.chunk();
                activeLease = lease;
                if (recovering) {
                    startMutationTicks();
                } else {
                    validateCurrentChunkHistory();
                }
            } catch (RuntimeException violation) {
                if (lease != null) {
                    lease.close();
                }
                fail("chunk-scope-violation", violation);
            }
        }

        private void validateCurrentChunkHistory() {
            List<OperationItem> validatingItems = currentChunk.items();
            validateLatestHistory(validatingItems).whenComplete((latestByPosition, failure) ->
                runOnServerForExecution(() -> {
                    if (terminalRequested) {
                        checkpointOrFinalize(false);
                    } else if (failure != null) {
                        fail("chunk-history-validation-failed", failure);
                    } else {
                        try {
                            latestHistoryValidator.requireCurrent(
                                validatingItems,
                                latestByPosition,
                                header.kind(),
                                header.inverseOf(),
                                config.restoreBlockEntityData()
                            );
                            validatingItems.forEach(item -> currentWatch.requireUnchanged(item.position()));
                            startMutationTicks();
                        } catch (RuntimeException stale) {
                            fail("chunk-history-became-stale", stale);
                        }
                    }
                })
            );
        }

        private void startMutationTicks() {
            currentChunk.items().forEach(item -> currentWatch.requireUnchanged(item.position()));
            tickTask = Bukkit.getScheduler().runTaskTimer(plugin, this::runTick, 0L, 1L);
        }

        private void runTick() {
            requireServerThread();
            int processed = 0;
            while (itemCursor < currentChunk.items().size() && processed < config.blocksPerTick()) {
                OperationItem item = currentChunk.items().get(itemCursor++);
                processed++;
                try {
                    currentScope.requireAllowed(item);
                    currentWatch.requireUnchanged(item.position());
                    if (!ExactChunkCoordinate.from(item.position()).equals(activeLease.coordinate())) {
                        throw new IllegalStateException("Rollback item escaped its leased exact chunk");
                    }
                } catch (RuntimeException violation) {
                    fail("mutation-scope-violation", violation);
                    return;
                }
                if (!applyOne(item)) {
                    requestFinish("block-application-failed");
                    return;
                }
            }
            if (itemCursor >= currentChunk.items().size()) {
                stopTick();
                checkpointOrFinalize(true);
            }
        }

        private boolean applyOne(OperationItem item) {
            try {
                Chunk chunk = activeLease.chunk();
                World world = chunk.getWorld();
                if (item.position().y() < world.getMinHeight() || item.position().y() >= world.getMaxHeight()) {
                    firstFailure = "World height changed after the rollback preview";
                    return false;
                }
                Block block = chunk.getBlock(
                    item.position().x() & 15, item.position().y(), item.position().z() & 15
                );
                BlockSnapshot current = payloadMode(snapshots.capture(block));
                if (recovering) {
                    RecoveryReconciler.Decision decision = recoveryReconciler.decide(
                        current, item.before(), item.after(), config.restoreBlockEntityData()
                    );
                    if (decision == RecoveryReconciler.Decision.ALREADY_APPLIED) {
                        appliedInChunk.add(new AppliedOperationItem(item, item.before(), current));
                        return true;
                    }
                    if (decision == RecoveryReconciler.Decision.CONFLICT) {
                        firstFailure = "중단 작업 복구 중 현재 블록이 계획의 이전/목표 상태와 모두 다릅니다: "
                            + item.position().x() + "," + item.position().y() + "," + item.position().z();
                        return false;
                    }
                }
                if (!current.sameState(item.before(), config.restoreBlockEntityData())) {
                    firstFailure = "Live block state changed after chunk history validation at "
                        + item.position().x() + "," + item.position().y() + "," + item.position().z();
                    return false;
                }
                currentWatch.requireUnchanged(item.position());
                snapshots.apply(block, item.after(), config.restoreBlockEntityData());
                BlockSnapshot actualAfter;
                try {
                    actualAfter = payloadMode(snapshots.capture(block));
                } catch (RuntimeException captureFailure) {
                    appliedInChunk.add(new AppliedOperationItem(item, current, item.after()));
                    throw captureFailure;
                }
                appliedInChunk.add(new AppliedOperationItem(item, current, actualAfter));
                currentWatch.requireUnchanged(item.position());
                if (!actualAfter.sameState(item.after(), config.restoreBlockEntityData())) {
                    firstFailure = "Applied block did not reach its planned target state at "
                        + item.position().x() + "," + item.position().y() + "," + item.position().z();
                    return false;
                }
                return true;
            } catch (RuntimeException exception) {
                if (firstFailure.isEmpty()) {
                    firstFailure = describeFailure(exception);
                }
                return false;
            }
        }

        private void checkpointOrFinalize(boolean continueAfter) {
            if (checkpointInFlight || finalizationStarted) {
                return;
            }
            stopTick();
            if (appliedInChunk.isEmpty()) {
                closeCurrentChunk();
                if (continueAfter && !terminalRequested) {
                    readNextChunk();
                } else {
                    finalizeOperation();
                }
                return;
            }
            checkpointInFlight = true;
            List<AppliedOperationItem> checkpointItems = List.copyOf(appliedInChunk);
            store.checkpointOperation(new OperationCheckpoint(
                header.id(), System.currentTimeMillis(), checkpointItems
            )).whenComplete((unused, failure) -> runOnServerForExecution(() -> {
                checkpointInFlight = false;
                if (failure != null) {
                    firstFailure = describeFailure(failure);
                    abandonInterruptedOperation(failure);
                    return;
                }
                appliedTotal = Math.addExact(appliedTotal, checkpointItems.size());
                appliedInChunk.clear();
                closeCurrentChunk();
                if (continueAfter && !terminalRequested) {
                    readNextChunk();
                } else {
                    finalizeOperation();
                }
            }));
        }

        private void requestFinish(String reason) {
            if (!reason.isEmpty() && firstFailure.isEmpty()) {
                firstFailure = reason;
            }
            terminalRequested = true;
            stopTick();
            if (pendingPlanRead != null) {
                return;
            }
            if (pendingLease != null) {
                pendingLease.cancel(false);
                pendingLease = null;
            }
            checkpointOrFinalize(false);
        }

        private void fail(String reason, Throwable failure) {
            if (firstFailure.isEmpty()) {
                firstFailure = describeFailure(failure);
            }
            requestFinish(reason);
        }

        private void abort(String reason) {
            requestFinish(reason);
        }

        private void finalizeOperation() {
            if (finalizationStarted) {
                return;
            }
            finalizationStarted = true;
            closeCurrentChunk();
            closeReader();
            activeExecutions.remove(this);
            int skipped = header.itemCount() - appliedTotal;
            OperationStatus status;
            if (appliedTotal == 0) {
                status = OperationStatus.FAILED;
            } else if (skipped == 0 && firstFailure.isEmpty()) {
                status = OperationStatus.APPLIED;
            } else {
                status = OperationStatus.PARTIAL;
            }
            OperationFinalization finalization = new OperationFinalization(
                header.id(), System.currentTimeMillis(), status, skipped, firstFailure
            );
            store.finalizeOperation(finalization).whenComplete((unused, failure) -> {
                Throwable terminalFailure = deletePlan(planFile, failure);
                if (terminalFailure != null) {
                    result.completeExceptionally(recoveryFailure(
                        header.id(), "작업 완료 상태 저장 또는 계획 파일 정리에 실패했습니다.", terminalFailure
                    ));
                } else {
                    result.complete(new OperationRunResult(
                        header.id(), status, appliedTotal, skipped, firstFailure
                    ));
                }
            });
        }

        private void abandonInterruptedOperation(Throwable failure) {
            finalizationStarted = true;
            closeCurrentChunk();
            closeReader();
            activeExecutions.remove(this);
            Throwable terminalFailure = deletePlan(planFile, failure);
            result.completeExceptionally(recoveryFailure(
                header.id(),
                "월드 변경 뒤 체크포인트를 확정하지 못했습니다.",
                terminalFailure
            ));
        }

        private void closeCurrentChunk() {
            stopTick();
            if (activeLease != null) {
                activeLease.close();
                activeLease = null;
            }
            if (currentWatch != null) {
                currentWatch.close();
                currentWatch = null;
            }
            currentChunk = null;
            currentScope = null;
            itemCursor = 0;
        }

        private void stopTick() {
            if (tickTask != null) {
                tickTask.cancel();
                tickTask = null;
            }
        }

        private void closeReader() {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

        private void runOnServerForExecution(Runnable action) {
            if (shutdownRequested || Bukkit.isPrimaryThread()) {
                action.run();
            } else if (plugin.isEnabled()) {
                Bukkit.getScheduler().runTask(plugin, action);
            } else {
                action.run();
            }
        }
    }

    private static final class PendingStart {
        private final OperationHeader header;
        private final Path planFile;
        private final CompletableFuture<OperationRunResult> result;
        private final boolean recovering;
        private final int alreadyApplied;
        private BukkitTask task;

        private PendingStart(
            OperationHeader header,
            Path planFile,
            CompletableFuture<OperationRunResult> result,
            boolean recovering,
            int alreadyApplied
        ) {
            this.header = Objects.requireNonNull(header, "header");
            this.planFile = Objects.requireNonNull(planFile, "planFile");
            this.result = Objects.requireNonNull(result, "result");
            this.recovering = recovering;
            this.alreadyApplied = alreadyApplied;
        }

        private void cancelTask() {
            if (task != null) {
                task.cancel();
                task = null;
            }
        }
    }

    private static final class UndoSpoolBuilder {
        private final OperationPlanSpool.Writer writer;
        private ExactChunkCoordinate lastChunk;
        private int candidates;
        private int chunks;

        private UndoSpoolBuilder(OperationPlanSpool.Writer writer) {
            this.writer = writer;
        }

        private void accept(OperationItem item) {
            writer.write(new OperationItem(
                item.sequence(), item.position(), item.after(), item.before(), item.sourceIds()
            ));
            candidates = Math.incrementExact(candidates);
            ExactChunkCoordinate chunk = ExactChunkCoordinate.from(item.position());
            if (!chunk.equals(lastChunk)) {
                chunks = Math.incrementExact(chunks);
                lastChunk = chunk;
            }
        }

        private StreamingRollbackPlanner.Result finish() {
            writer.close();
            return new StreamingRollbackPlanner.Result(
                writer.path(), candidates, candidates, chunks, 0, 0
            );
        }
    }

    private static final class RecoverySpoolBuilder {
        private final OperationPlanSpool.Writer writer;
        private ExactChunkCoordinate lastChunk;
        private int candidates;
        private int chunks;

        private RecoverySpoolBuilder(OperationPlanSpool.Writer writer) {
            this.writer = writer;
        }

        private void accept(OperationItem item) {
            writer.write(item);
            candidates = Math.incrementExact(candidates);
            ExactChunkCoordinate chunk = ExactChunkCoordinate.from(item.position());
            if (!chunk.equals(lastChunk)) {
                chunks = Math.incrementExact(chunks);
                lastChunk = chunk;
            }
        }

        private StreamingRollbackPlanner.Result finish() {
            writer.close();
            return new StreamingRollbackPlanner.Result(
                writer.path(), candidates, candidates, chunks, 0, 0
            );
        }
    }

    private static void closeWriterAfterFailure(OperationPlanSpool.Writer writer, Throwable failure) {
        try {
            writer.close();
        } catch (RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static void deletePlanAfterFailure(Path planFile, Throwable failure) {
        try {
            OperationPlanSpool.delete(planFile);
        } catch (RuntimeException deleteFailure) {
            failure.addSuppressed(deleteFailure);
        }
    }

    private static Throwable deletePlan(Path planFile, Throwable failure) {
        try {
            OperationPlanSpool.delete(planFile);
            return failure;
        } catch (RuntimeException deleteFailure) {
            if (failure != null) {
                failure.addSuppressed(deleteFailure);
                return failure;
            }
            return deleteFailure;
        }
    }

    private static Throwable combineFailures(Throwable current, Throwable added) {
        if (current == null) {
            return added;
        }
        current.addSuppressed(added);
        return current;
    }

    private static String shortId(UUID id) {
        return id.toString().substring(0, 8);
    }

    private void requireRollbackAvailable() {
        StoreStatus status = store.status();
        if (!status.ready() || !status.healthy() || !status.accepting() || status.degraded()) {
            throw new IllegalArgumentException(
                "기록 저장소가 정상 상태가 아니므로 복구 작업을 시작할 수 없습니다. /history status를 확인하십시오."
            );
        }
        if (status.interruptedOperations() > 0) {
            throw new IllegalArgumentException(
                "미복구 중단 작업이 있습니다. 해당 작업 ID로 /history recover <operation-id>를 먼저 실행하십시오."
            );
        }
    }

    private static void requireServerThread() {
        if (!Bukkit.isPrimaryThread()) {
            throw new IllegalStateException("Rollback execution must run on the server thread");
        }
    }

    private static String describeFailure(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return current.getClass().getSimpleName()
            + (message == null || message.isBlank() ? "" : ": " + message);
    }

    private static IllegalStateException recoveryFailure(UUID operationId, String message, Throwable cause) {
        String instruction = message + " 작업 ID: " + operationId
            + " · 저장소가 정상화된 뒤 /history recover " + operationId + " 를 실행하십시오.";
        return cause == null
            ? new IllegalStateException(instruction)
            : new IllegalStateException(instruction, cause);
    }
}
