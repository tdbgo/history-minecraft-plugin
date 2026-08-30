package kr.playcity.history.capture;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.rollback.ActivePositionGuard;
import kr.playcity.history.storage.HistoryStore;
import org.bukkit.World;
import org.bukkit.Location;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.List;
import java.util.UUID;

public final class ChangeRecorder {
    private static final long WARNING_INTERVAL_NANOS = 30_000_000_000L;
    private static final int MAXIMUM_METADATA_LENGTH = 2_048;
    private final HistoryStore store;
    private final Logger logger;
    private final ActivePositionGuard positionGuard;
    private final AtomicLong lastWarning = new AtomicLong();

    public ChangeRecorder(HistoryStore store, Logger logger) {
        this(store, logger, new ActivePositionGuard());
    }

    public ChangeRecorder(HistoryStore store, Logger logger, ActivePositionGuard positionGuard) {
        this.store = store;
        this.logger = logger;
        this.positionGuard = positionGuard;
    }

    public void record(
        Block block,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        String metadata
    ) {
        record(position(block.getWorld(), block.getX(), block.getY(), block.getZ()), actor, cause, before, after, metadata);
    }

    public void record(
        BlockState state,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        String metadata
    ) {
        record(position(state.getWorld(), state.getX(), state.getY(), state.getZ()), actor, cause, before, after, metadata);
    }

    public void record(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        String metadata
    ) {
        if (before.sameState(after, true)) {
            return;
        }
        boolean accepted = positionGuard.recordMutation(
            position,
            () -> store.append(ChangeRecord.captured(position, actor, cause, before, after, metadata))
        );
        if (!accepted) {
            warnRejectedWrite();
        }
    }

    /** Records a fallback WorldEdit mutation after the delegate has applied it. */
    public boolean recordAppliedBatchChange(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID batchId,
        String metadata
    ) {
        if (before.sameState(after, true)) {
            return true;
        }
        ChangeRecord change = ChangeRecord.capturedInBatch(
            position, actor, cause, before, after, batchId, metadata
        );
        boolean recorded;
        try {
            recorded = store.tryAppendWorldEdit(change);
            positionGuard.markMutation(position);
        } catch (RuntimeException | LinkageError failure) {
            safeReportCaptureGap(1L, "worldedit", "WorldEdit 적용 후 기록 중 내부 오류가 발생했습니다.");
            recorded = false;
        }
        if (!recorded) {
            warnWorldEditCaptureGap();
        }
        return recorded;
    }

    /** Records one already-applied FAWE chunk as an all-or-none queue admission. */
    public boolean recordAppliedFaweBatch(List<ChangeRecord> changes) {
        return recordAppliedFaweBatch(changes, true);
    }

    /** Records a FAWE chunk, optionally waiting only when the caller is a non-server worker. */
    public boolean recordAppliedFaweBatch(List<ChangeRecord> changes, boolean waitForCapacity) {
        if (changes.isEmpty()) {
            return true;
        }
        boolean recorded;
        try {
            recorded = waitForCapacity
                ? store.appendWorldEditBatch(changes)
                : store.tryAppendWorldEditBatch(changes);
            for (ChangeRecord change : changes) {
                positionGuard.markMutation(change.position());
            }
        } catch (RuntimeException | LinkageError failure) {
            safeReportCaptureGap(changes.size(), "fawe", "FAWE 청크 적용 후 기록 중 내부 오류가 발생했습니다.");
            recorded = false;
        }
        if (!recorded) {
            warnWorldEditCaptureGap();
        }
        return recorded;
    }

    public void beginFaweCapture(UUID observationId, long estimatedChanges) {
        store.beginExternalCapture(observationId, estimatedChanges, "fawe");
    }

    public void completeFaweCapture(UUID observationId) {
        store.completeExternalCapture(observationId);
    }

    public void abandonFaweCapture(UUID observationId, String reason) {
        store.abandonExternalCapture(observationId, reason);
        warnWorldEditCaptureGap();
    }

    public void reportFaweCaptureGap(long estimatedChanges, String reason) {
        safeReportCaptureGap(estimatedChanges, "fawe", reason);
        warnWorldEditCaptureGap();
    }

    private void safeReportCaptureGap(long estimatedChanges, String source, String reason) {
        try {
            store.reportCaptureGap(estimatedChanges, source, reason);
        } catch (RuntimeException | LinkageError ignored) {
            // Capture diagnostics must never escape into an applied edit path.
        }
    }

    public void recordAudit(Location location, ActorRef actor, ChangeCause cause, String metadata) {
        recordAudit(location, actor, cause, null, metadata);
    }

    public void recordAudit(
        Location location,
        ActorRef actor,
        ChangeCause cause,
        String subjectKey,
        String metadata
    ) {
        if (location.getWorld() == null) {
            return;
        }
        recordAudit(
            position(location.getWorld(), location.getBlockX(), location.getBlockY(), location.getBlockZ()),
            actor,
            cause,
            subjectKey,
            metadata
        );
    }

    public void recordAudit(BlockPosition position, ActorRef actor, ChangeCause cause, String metadata) {
        recordAudit(position, actor, cause, null, metadata);
    }

    public void recordAudit(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        String subjectKey,
        String metadata
    ) {
        if (cause.rollbackEligible()) {
            throw new IllegalArgumentException("Audit records require an audit-only change cause");
        }
        BlockSnapshot subject = subjectKey == null ? BlockSnapshot.air() : BlockSnapshot.block(subjectKey);
        boolean accepted = store.append(ChangeRecord.captured(
            position,
            actor,
            cause,
            BlockSnapshot.air(),
            subject,
            limitMetadata(metadata)
        ));
        if (!accepted) {
            warnRejectedWrite();
        }
    }

    private void warnRejectedWrite() {
        long now = System.nanoTime();
        long previous = lastWarning.get();
        if (now - previous >= WARNING_INTERVAL_NANOS && lastWarning.compareAndSet(previous, now)) {
            logger.severe("History rejected a world change; check /history status immediately.");
        }
    }

    private void warnWorldEditCaptureGap() {
        long now = System.nanoTime();
        long previous = lastWarning.get();
        if (now - previous >= WARNING_INTERVAL_NANOS && lastWarning.compareAndSet(previous, now)) {
            logger.warning(
                "History가 WorldEdit/FAWE 편집은 중단하지 않았지만 일부 기록을 남기지 못했습니다. "
                    + "/history status에서 캡처 공백을 확인하십시오."
            );
        }
    }

    private static String limitMetadata(String metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return "";
        }
        String compact = metadata.replace('\r', ' ').replace('\n', ' ');
        return compact.length() <= MAXIMUM_METADATA_LENGTH
            ? compact
            : compact.substring(0, MAXIMUM_METADATA_LENGTH);
    }

    private static BlockPosition position(World world, int x, int y, int z) {
        return new BlockPosition(world.getUID(), x, y, z);
    }
}
