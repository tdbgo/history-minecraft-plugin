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

    public void recordBatchChange(
        BlockPosition position,
        ActorRef actor,
        ChangeCause cause,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID batchId,
        String metadata
    ) {
        if (before.sameState(after, true)) {
            return;
        }
        boolean accepted = positionGuard.recordMutation(position, () -> store.append(ChangeRecord.capturedInBatch(
            position,
            actor,
            cause,
            before,
            after,
            batchId,
            metadata
        )));
        if (!accepted) {
            warnRejectedWrite();
        }
    }

    public boolean recordFaweBatchChange(
        BlockPosition position,
        ActorRef actor,
        BlockSnapshot before,
        BlockSnapshot after,
        UUID batchId
    ) {
        if (before.sameState(after, true)) {
            return true;
        }
        boolean accepted = positionGuard.recordMutation(position, () -> store.appendWorldEdit(
            ChangeRecord.capturedInBatch(
                position,
                actor,
                ChangeCause.WORLD_EDIT,
                before,
                after,
                batchId,
                ""
            )
        ));
        if (!accepted) {
            warnRejectedWrite();
        }
        return accepted;
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
