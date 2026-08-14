package kr.playcity.history.rollback;

import kr.playcity.history.capture.SnapshotCodec;
import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.AppliedOperationItem;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import kr.playcity.history.model.OperationStatus;
import kr.playcity.history.model.PlannedBlockChange;
import kr.playcity.history.model.RollbackPlan;
import kr.playcity.history.model.StoredOperation;
import kr.playcity.history.storage.HistoryStore;
import kr.playcity.history.util.DurationParser;
import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public final class RollbackService {
    private final JavaPlugin plugin;
    private final HistoryConfig.Rollback config;
    private final HistoryStore store;
    private final SnapshotCodec snapshots;
    private final RollbackPlanner planner;
    private final RollbackCandidateEvaluator candidateEvaluator = new RollbackCandidateEvaluator();
    private final LatestHistoryValidator latestHistoryValidator = new LatestHistoryValidator();
    private final PreviewRegistry previews;
    private final ActivePositionGuard positionGuard;
    private final ScopedChunkLeaseManager chunkLeases;
    private final Set<Execution> activeExecutions = ConcurrentHashMap.newKeySet();

    public RollbackService(
        JavaPlugin plugin,
        HistoryConfig.Rollback config,
        HistoryStore store,
        SnapshotCodec snapshots,
        RollbackPlanner planner,
        PreviewRegistry previews,
        ActivePositionGuard positionGuard
    ) {
        this.plugin = plugin;
        this.config = config;
        this.store = store;
        this.snapshots = snapshots;
        this.planner = planner;
        this.previews = previews;
        this.positionGuard = positionGuard;
        this.chunkLeases = new ScopedChunkLeaseManager(
            plugin,
            config.maxConcurrentChunkLeases(),
            config.chunkLoadTimeoutSeconds(),
            config.generateMissingChunks()
        );
    }

    public CompletableFuture<RollbackPreview> createRollbackPreview(
        Player player,
        String actor,
        Duration duration,
        int radius
    ) {
        Location center = player.getLocation();
        long since = Instant.now().minus(duration).toEpochMilli();
        HistoryQuery query = HistoryQuery.nearby(
            center.getWorld().getUID(),
            center.getBlockX(),
            center.getBlockZ(),
            radius,
            since,
            actor,
            config.maxSourceChanges() + 1
        ).forRollback();
        UUID worldId = center.getWorld().getUID();
        int centerX = center.getBlockX();
        int centerZ = center.getBlockZ();
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(
            worldId,
            centerX,
            centerZ,
            radius
        );
        String actorLabel = actor == null || actor.equals("*") ? "모든 원인" : actor;
        String summary = actorLabel + " · 최근 " + DurationParser.compact(duration) + " · 반경 " + radius;
        return store.query(query).thenCompose(changes -> onServerThread(() -> {
            boolean sourceLimitReached = changes.size() > config.maxSourceChanges();
            List<ChangeRecord> boundedChanges = sourceLimitReached
                ? changes.subList(0, config.maxSourceChanges())
                : changes;
            boundary.requireContainsAll(boundedChanges);
            // Keep payloads in the plan even when restoration is disabled so block entities
            // can be rejected safely instead of being silently emptied or duplicated.
            RollbackPlan plan = planner.consolidate(boundedChanges, true);
            List<PlannedBlockChange> executable = sourceLimitReached ? List.of() : plan.changes();
            return evaluate(
                player.getUniqueId(),
                OperationKind.ROLLBACK,
                summary,
                null,
                executable,
                plan.sourceChangeCount(),
                plan.unsafePositionCount() + (sourceLimitReached ? plan.changes().size() : 0),
                sourceLimitReached
            );
        }));
    }

    public CompletableFuture<RollbackPreview> createUndoPreview(Player player, UUID operationId) {
        CompletableFuture<Optional<StoredOperation>> operationFuture = operationId == null
            ? store.findLastOperation(player.getUniqueId())
            : store.loadOperation(operationId);
        return operationFuture.thenCompose(optional -> {
            StoredOperation stored = optional.orElseThrow(
                () -> new IllegalArgumentException("되돌릴 History 작업을 찾지 못했습니다.")
            );
            if (stored.status() != OperationStatus.APPLIED && stored.status() != OperationStatus.PARTIAL) {
                throw new IllegalArgumentException("완료된 작업만 취소할 수 있습니다.");
            }
            List<PlannedBlockChange> reversed = new ArrayList<>();
            for (OperationItem item : stored.draft().items()) {
                reversed.add(new PlannedBlockChange(
                    item.position(),
                    item.after(),
                    item.before(),
                    item.sourceIds().isEmpty() ? List.of((long) item.sequence()) : item.sourceIds()
                ));
            }
            return onServerThread(() -> evaluate(
                player.getUniqueId(),
                OperationKind.UNDO,
                "작업 " + shortId(stored.draft().id()) + " 취소",
                stored.draft().id(),
                reversed,
                reversed.size(),
                0,
                false
            ));
        });
    }

    public CompletableFuture<OperationRunResult> apply(Player player, String token) {
        RollbackPreview preview = previews.consume(token, player.getUniqueId())
            .orElseThrow(() -> new IllegalArgumentException("미리보기가 없거나 만료되었습니다."));
        String requiredPermission = preview.kind() == OperationKind.ROLLBACK
            ? "history.rollback"
            : "history.undo";
        if (!player.hasPermission(requiredPermission)) {
            throw new IllegalArgumentException("이 작업을 적용할 권한이 없습니다.");
        }
        if (preview.items().isEmpty()) {
            throw new IllegalArgumentException("적용할 안전한 변경이 없습니다.");
        }

        ExactMutationScope scope = ExactMutationScope.create(
            preview.items(),
            config.maxSourceChanges(),
            config.maxChunksPerOperation()
        );
        ActivePositionGuard.Watch positionWatch = positionGuard.watch(
            preview.items().stream().map(OperationItem::position).toList()
        );
        CompletableFuture<OperationRunResult> operation = validateLatestHistory(preview.items()).thenCompose(latestByPosition ->
            onServerThread(() -> {
                latestHistoryValidator.requireCurrent(
                    preview.items(),
                    latestByPosition,
                    preview.kind(),
                    preview.inverseOf(),
                    config.restoreBlockEntityData()
                );
                return startPreparedOperation(player, preview, scope, positionWatch);
            })
        ).thenCompose(future -> future);
        return operation.whenComplete((unused, failure) -> positionWatch.close());
    }

    private CompletableFuture<java.util.Map<BlockPosition, LatestHistoryValidator.LatestState>> validateLatestHistory(
        List<OperationItem> items
    ) {
        List<BlockPosition> positions = items.stream().map(OperationItem::position).toList();
        return store.latestChanges(positions).thenApply(changes -> {
            java.util.Map<BlockPosition, LatestHistoryValidator.LatestState> latest = new java.util.HashMap<>();
            for (ChangeRecord change : changes.values()) {
                latest.put(
                    change.position(),
                    new LatestHistoryValidator.LatestState(
                        change.id(),
                        change.after(),
                        change.operationId()
                    )
                );
            }
            return java.util.Map.copyOf(latest);
        });
    }

    private CompletableFuture<OperationRunResult> startPreparedOperation(
        Player player,
        RollbackPreview preview,
        ExactMutationScope scope,
        ActivePositionGuard.Watch positionWatch
    ) {

        UUID operationId = UUID.randomUUID();
        OperationDraft draft = new OperationDraft(
            operationId,
            System.currentTimeMillis(),
            ActorRef.player(player.getUniqueId(), player.getName()),
            preview.kind(),
            preview.summary(),
            preview.inverseOf(),
            preview.items()
        );
        CompletableFuture<OperationRunResult> result = new CompletableFuture<>();
        store.prepareOperation(draft).whenComplete((unused, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
                return;
            }
            if (!plugin.isEnabled()) {
                completeWithoutExecution(draft, "plugin-disabled-before-execution", result);
                return;
            }
            Bukkit.getScheduler().runTask(plugin, () -> {
                Execution execution = new Execution(draft, scope, positionWatch, result);
                activeExecutions.add(execution);
                execution.start();
            });
        });
        return result;
    }

    public boolean cancelPreview(Player player, String token) {
        return previews.cancel(token, player.getUniqueId());
    }

    public CompletableFuture<Void> shutdown() {
        List<CompletableFuture<OperationRunResult>> results = new ArrayList<>();
        for (Execution execution : List.copyOf(activeExecutions)) {
            execution.abort("plugin-disabled");
            results.add(execution.result);
        }
        chunkLeases.close();
        return CompletableFuture.allOf(results.toArray(CompletableFuture[]::new));
    }

    private RollbackPreview evaluate(
        UUID ownerId,
        OperationKind kind,
        String summary,
        UUID inverseOf,
        List<PlannedBlockChange> planned,
        int sourceChanges,
        int planningConflicts,
        boolean sourceLimitReached
    ) {
        World world = null;
        if (!planned.isEmpty()) {
            UUID worldId = planned.getFirst().position().worldId();
            boolean crossWorld = planned.stream().anyMatch(change -> !change.position().worldId().equals(worldId));
            if (crossWorld) {
                throw new IllegalArgumentException("한 번의 복구는 하나의 월드 안에서만 실행할 수 있습니다.");
            }
            world = Bukkit.getWorld(worldId);
            if (world == null) {
                throw new IllegalArgumentException("복구 대상 월드가 현재 로드되어 있지 않습니다: " + worldId);
            }
        }
        RollbackCandidateEvaluator.Result evaluation = candidateEvaluator.evaluate(
            planned,
            world == null ? Integer.MIN_VALUE : world.getMinHeight(),
            world == null ? Integer.MAX_VALUE : world.getMaxHeight(),
            config.restoreBlockEntityData()
        );
        List<OperationItem> candidates = evaluation.candidates();
        int conflicts = planningConflicts + evaluation.conflicts();
        int alreadyTarget = evaluation.alreadyTarget();
        if (!candidates.isEmpty()) {
            // Validate both exact block and exact chunk limits before exposing a
            // confirmation button. The same immutable scope is rebuilt after
            // the single-use preview is consumed.
            ExactMutationScope.create(
                candidates,
                config.maxSourceChanges(),
                config.maxChunksPerOperation()
            );
        }
        RollbackPreview preview = new RollbackPreview(
            "",
            ownerId,
            System.currentTimeMillis() + config.previewTtlSeconds() * 1_000L,
            kind,
            summary,
            inverseOf,
            candidates,
            sourceChanges,
            conflicts,
            alreadyTarget,
            sourceLimitReached
        );
        return candidates.isEmpty() ? preview : previews.register(preview);
    }

    private BlockSnapshot payloadMode(BlockSnapshot snapshot) {
        return config.restoreBlockEntityData() ? snapshot : snapshot.withoutPayload();
    }

    private <T> CompletableFuture<T> onServerThread(Supplier<T> action) {
        CompletableFuture<T> result = new CompletableFuture<>();
        if (!plugin.isEnabled()) {
            result.completeExceptionally(new IllegalStateException("History is disabled"));
            return result;
        }
        Bukkit.getScheduler().runTask(plugin, () -> {
            try {
                result.complete(action.get());
            } catch (RuntimeException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    private void completeWithoutExecution(
        OperationDraft draft,
        String reason,
        CompletableFuture<OperationRunResult> result
    ) {
        OperationCompletion completion = new OperationCompletion(
            draft.id(),
            System.currentTimeMillis(),
            OperationStatus.FAILED,
            List.of(),
            draft.items().size(),
            reason
        );
        store.completeOperation(completion).whenComplete((unused, failure) -> {
            if (failure != null) {
                result.completeExceptionally(failure);
            } else {
                result.complete(new OperationRunResult(
                    draft.id(),
                    OperationStatus.FAILED,
                    0,
                    draft.items().size(),
                    reason
                ));
            }
        });
    }

    private static String shortId(UUID id) {
        return id.toString().substring(0, 8);
    }

    private final class Execution {
        private final OperationDraft draft;
        private final ExactMutationScope scope;
        private final ActivePositionGuard.Watch positionWatch;
        private final CompletableFuture<OperationRunResult> result;
        private final List<AppliedOperationItem> applied = new ArrayList<>();
        private final List<ExactChunkCoordinate> chunks;
        private int chunkCursor;
        private List<OperationItem> currentItems = List.of();
        private int currentItemCursor;
        private int processedItems;
        private int skipped;
        private String firstFailure = "";
        private CompletableFuture<ScopedChunkLeaseManager.Lease> pendingLease;
        private ScopedChunkLeaseManager.Lease activeLease;
        private BukkitTask tickTask;
        private boolean finished;

        private Execution(
            OperationDraft draft,
            ExactMutationScope scope,
            ActivePositionGuard.Watch positionWatch,
            CompletableFuture<OperationRunResult> result
        ) {
            this.draft = draft;
            this.scope = scope;
            this.positionWatch = positionWatch;
            this.result = result;
            this.chunks = scope.chunks();
        }

        private void start() {
            requireServerThread();
            loadNextChunk();
        }

        private void loadNextChunk() {
            requireServerThread();
            if (finished) {
                return;
            }
            if (chunkCursor >= chunks.size()) {
                finish("");
                return;
            }
            ExactChunkCoordinate target = chunks.get(chunkCursor++);
            try {
                scope.requireChunkAllowed(target);
            } catch (RuntimeException violation) {
                firstFailure = describeFailure(violation);
                finish("chunk-scope-violation");
                return;
            }
            pendingLease = chunkLeases.acquire(scope, target);
            pendingLease.whenComplete((lease, failure) -> handleLease(target, lease, failure));
        }

        private void handleLease(
            ExactChunkCoordinate target,
            ScopedChunkLeaseManager.Lease lease,
            Throwable failure
        ) {
            requireServerThread();
            pendingLease = null;
            if (finished) {
                if (lease != null) {
                    lease.close();
                }
                return;
            }
            if (failure != null) {
                firstFailure = describeFailure(failure);
                finish("chunk-load-failed");
                return;
            }
            try {
                if (!lease.coordinate().equals(target)) {
                    throw new IllegalStateException("Loaded chunk does not match its exact work group");
                }
                scope.requireChunkAllowed(lease.coordinate());
                lease.chunk();
                activeLease = lease;
                currentItems = scope.itemsIn(lease.coordinate());
                currentItemCursor = 0;
                validateCurrentChunkHistory();
            } catch (RuntimeException violation) {
                lease.close();
                firstFailure = describeFailure(violation);
                finish("chunk-scope-violation");
            }
        }

        private void validateCurrentChunkHistory() {
            List<OperationItem> validatingItems = currentItems;
            validateLatestHistory(validatingItems).whenComplete((latestByPosition, failure) -> {
                if (!plugin.isEnabled()) {
                    return;
                }
                try {
                    Bukkit.getScheduler().runTask(plugin, () -> {
                        if (finished || currentItems != validatingItems) {
                            return;
                        }
                        if (failure != null) {
                            firstFailure = describeFailure(failure);
                            finish("chunk-history-validation-failed");
                            return;
                        }
                        try {
                            latestHistoryValidator.requireCurrent(
                                validatingItems,
                                latestByPosition,
                                draft.kind(),
                                draft.inverseOf(),
                                config.restoreBlockEntityData()
                            );
                            validatingItems.forEach(item -> positionWatch.requireUnchanged(item.position()));
                            tickTask = Bukkit.getScheduler().runTaskTimer(plugin, this::runTick, 0L, 1L);
                        } catch (RuntimeException stale) {
                            firstFailure = describeFailure(stale);
                            finish("chunk-history-became-stale");
                        }
                    });
                } catch (RuntimeException ignored) {
                    // Plugin shutdown owns execution finalization and ticket release.
                }
            });
        }

        private void runTick() {
            requireServerThread();
            int processed = 0;
            while (currentItemCursor < currentItems.size() && processed < config.blocksPerTick()) {
                OperationItem item = currentItems.get(currentItemCursor++);
                processedItems++;
                processed++;
                try {
                    scope.requireAllowed(item);
                    positionWatch.requireUnchanged(item.position());
                    if (!ExactChunkCoordinate.from(item.position()).equals(activeLease.coordinate())) {
                        throw new IllegalStateException("Rollback item escaped its leased exact chunk");
                    }
                } catch (RuntimeException violation) {
                    skipped++;
                    firstFailure = describeFailure(violation);
                    finish("mutation-scope-violation");
                    return;
                }
                if (!applyOne(item)) {
                    finish("block-application-failed");
                    return;
                }
            }
            if (currentItemCursor >= currentItems.size()) {
                finishCurrentChunk();
                loadNextChunk();
            }
        }

        private boolean applyOne(OperationItem item) {
            int appliedBefore = applied.size();
            try {
                Chunk chunk = activeLease.chunk();
                World world = chunk.getWorld();
                if (item.position().y() < world.getMinHeight() || item.position().y() >= world.getMaxHeight()) {
                    skipped++;
                    firstFailure = "World height changed after the rollback preview";
                    return false;
                }
                Block block = chunk.getBlock(
                    item.position().x() & 15,
                    item.position().y(),
                    item.position().z() & 15
                );
                BlockSnapshot current = payloadMode(snapshots.capture(block));
                if (!current.sameState(item.before(), config.restoreBlockEntityData())) {
                    skipped++;
                    firstFailure = "Live block state changed after chunk history validation at "
                        + item.position().x() + "," + item.position().y() + "," + item.position().z();
                    return false;
                }
                positionWatch.requireUnchanged(item.position());
                snapshots.apply(block, item.after(), config.restoreBlockEntityData());
                BlockSnapshot actualAfter;
                try {
                    actualAfter = payloadMode(snapshots.capture(block));
                } catch (RuntimeException captureFailure) {
                    applied.add(new AppliedOperationItem(item, current, item.after()));
                    throw captureFailure;
                }
                applied.add(new AppliedOperationItem(item, current, actualAfter));
                positionWatch.requireUnchanged(item.position());
                if (!actualAfter.sameState(item.after(), config.restoreBlockEntityData())) {
                    firstFailure = "Applied block did not reach its planned target state at "
                        + item.position().x() + "," + item.position().y() + "," + item.position().z();
                    return false;
                }
                return true;
            } catch (RuntimeException exception) {
                if (applied.size() == appliedBefore) {
                    skipped++;
                }
                if (firstFailure.isEmpty()) {
                    firstFailure = describeFailure(exception);
                }
                return false;
            }
        }

        private void abort(String reason) {
            finish(reason);
        }

        private void finishCurrentChunk() {
            requireServerThread();
            if (tickTask != null) {
                tickTask.cancel();
                tickTask = null;
            }
            if (activeLease != null) {
                activeLease.close();
                activeLease = null;
            }
            currentItems = List.of();
            currentItemCursor = 0;
        }

        private void finish(String reason) {
            if (finished) {
                return;
            }
            finished = true;
            if (pendingLease != null) {
                pendingLease.cancel(false);
                pendingLease = null;
            }
            finishCurrentChunk();
            activeExecutions.remove(this);
            int notVisited = draft.items().size() - processedItems;
            skipped += notVisited;
            String failure = firstFailure.isEmpty() ? reason : firstFailure;
            OperationStatus status;
            if (applied.isEmpty()) {
                status = OperationStatus.FAILED;
            } else if (skipped == 0 && failure.isEmpty()) {
                status = OperationStatus.APPLIED;
            } else {
                status = OperationStatus.PARTIAL;
            }
            OperationCompletion completion = new OperationCompletion(
                draft.id(),
                System.currentTimeMillis(),
                status,
                applied,
                skipped,
                failure
            );
            store.completeOperation(completion).whenComplete((unused, storageFailure) -> {
                if (storageFailure != null) {
                    result.completeExceptionally(storageFailure);
                } else {
                    result.complete(new OperationRunResult(
                        draft.id(),
                        status,
                        applied.size(),
                        skipped,
                        failure
                    ));
                }
            });
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
        return current.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
    }
}
