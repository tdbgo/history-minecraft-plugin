package kr.playcity.history.integration.worldedit;

import com.fastasyncworldedit.core.queue.IBatchProcessor;
import com.fastasyncworldedit.core.queue.IChunk;
import com.fastasyncworldedit.core.queue.IChunkGet;
import com.fastasyncworldedit.core.queue.IChunkSet;
import com.fastasyncworldedit.core.extent.processor.ProcessorScope;
import com.sk89q.worldedit.extent.Extent;
import com.sk89q.worldedit.world.block.BlockState;
import com.sk89q.worldedit.world.block.BlockTypesCache;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Captures FAWE's chunk batches without forcing them through per-block Bukkit or
 * generic Extent writes. The set is observed but never modified.
 */
final class FaweBatchProcessor implements IBatchProcessor {
    private final UUID worldId;
    private final ActorRef actor;
    private final UUID batchId;
    private final ChangeRecorder recorder;
    private final Logger logger;
    private final Map<BlockState, String> stateStrings = new ConcurrentHashMap<>();
    private final FaweChunkAdmission admission = new FaweChunkAdmission();
    private final AtomicBoolean failureLogged = new AtomicBoolean();

    FaweBatchProcessor(
        UUID worldId,
        ActorRef actor,
        UUID batchId,
        ChangeRecorder recorder,
        Logger logger
    ) {
        this.worldId = Objects.requireNonNull(worldId, "worldId");
        this.actor = Objects.requireNonNull(actor, "actor");
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.recorder = Objects.requireNonNull(recorder, "recorder");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Override
    public IChunkSet processSet(IChunk chunk, IChunkGet get, IChunkSet set) {
        // This processor is installed only as a post-processor. Keep the
        // method harmless if FAWE invokes it through an implementation detail.
        return set;
    }

    @Override
    public Future<?> postProcessSet(IChunk chunk, IChunkGet get, IChunkSet set) {
        long chunkKey = chunkKey(chunk);
        if (!admission.beginPost(chunkKey)) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            List<ChangeRecord> changes = capture(chunk, get, set);
            recorder.recordAppliedFaweBatch(changes);
        } catch (RuntimeException | LinkageError failure) {
            recorder.reportFaweCaptureGap(
                0L,
                "FAWE가 적용한 청크의 변경 기록을 검사하지 못했습니다: " + chunk.getX() + "," + chunk.getZ()
            );
            if (failureLogged.compareAndSet(false, true)) {
                logger.log(Level.WARNING, "History FAWE 후처리 캡처에 실패했지만 편집 결과는 유지됩니다.", failure);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() {
        admission.releaseAll();
    }

    private List<ChangeRecord> capture(IChunk chunk, IChunkGet get, IChunkSet set) {
        List<ChangeRecord> changes = new ArrayList<>();
        int baseX = chunk.getX() << 4;
        int baseZ = chunk.getZ() << 4;
        for (int section = get.getMinSectionPosition(); section <= get.getMaxSectionPosition(); section++) {
            if (!set.hasSection(section)) {
                continue;
            }
            char[] ordinals = set.loadIfPresent(section);
            if (ordinals == null) {
                continue;
            }
            int baseY = section << 4;
            int index = 0;
            for (int localY = 0; localY < 16; localY++) {
                int y = baseY + localY;
                for (int localZ = 0; localZ < 16; localZ++) {
                    for (int localX = 0; localX < 16; localX++) {
                        int ordinal = ordinals[index++];
                        if (ordinal == BlockTypesCache.ReservedIDs.__RESERVED__) {
                            continue;
                        }
                        BlockState before = get.getBlock(localX, y, localZ);
                        BlockState after = set.getBlock(localX, y, localZ);
                        if (before == after || before.equals(after)) {
                            continue;
                        }
                        changes.add(ChangeRecord.capturedInBatch(
                            new BlockPosition(worldId, baseX + localX, y, baseZ + localZ),
                            actor,
                            ChangeCause.WORLD_EDIT,
                            BlockSnapshot.block(stateString(before)),
                            BlockSnapshot.block(stateString(after)),
                            batchId,
                            ""
                        ));
                    }
                }
            }
        }
        return changes;
    }

    @Override
    public Extent construct(Extent child) {
        // processSet observes the complete FAWE chunk. Wrapping the extent as
        // well records the same mutation twice and doubles ingress pressure.
        return child;
    }

    @Override
    public ProcessorScope getScope() {
        // FAWE supplies the pre-change chunk copy to post processors at this scope.
        return ProcessorScope.READING_BLOCKS;
    }

    private String stateString(BlockState state) {
        return stateStrings.computeIfAbsent(state, BlockState::getAsString);
    }

    private static long chunkKey(IChunk chunk) {
        return ((long) chunk.getX() << 32) ^ (chunk.getZ() & 0xffff_ffffL);
    }
}
