package kr.playcity.history.integration.worldedit;

import com.sk89q.worldedit.WorldEditException;
import com.sk89q.worldedit.extent.AbstractDelegateExtent;
import com.sk89q.worldedit.extent.Extent;
import com.sk89q.worldedit.math.BlockVector3;
import com.sk89q.worldedit.world.block.BlockStateHolder;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;

import java.util.Objects;
import java.util.UUID;

final class WorldEditChangeExtent extends AbstractDelegateExtent {
    private final UUID worldId;
    private final ActorRef actor;
    private final UUID batchId;
    private final ChangeRecorder recorder;

    WorldEditChangeExtent(
        Extent extent,
        UUID worldId,
        ActorRef actor,
        UUID batchId,
        ChangeRecorder recorder
    ) {
        super(extent);
        this.worldId = Objects.requireNonNull(worldId, "worldId");
        this.actor = Objects.requireNonNull(actor, "actor");
        this.batchId = Objects.requireNonNull(batchId, "batchId");
        this.recorder = Objects.requireNonNull(recorder, "recorder");
    }

    @Override
    public <T extends BlockStateHolder<T>> boolean setBlock(BlockVector3 position, T target)
        throws WorldEditException {
        String before = getExtent().getBlock(position).getAsString();
        boolean changed = super.setBlock(position, target);
        if (changed) {
            record(position.x(), position.y(), position.z(), before, target.getAsString());
        }
        return changed;
    }

    /**
     * FAWE adds this allocation-free overload to WorldEdit's Extent contract.
     * It intentionally has no {@code @Override} so the same binary also compiles
     * against upstream WorldEdit, where the overload does not exist.
     */
    @SuppressWarnings("deprecation")
    public <T extends BlockStateHolder<T>> boolean setBlock(int x, int y, int z, T target)
        throws WorldEditException {
        BlockVector3 position = BlockVector3.at(x, y, z);
        String before = getExtent().getBlock(position).getAsString();
        boolean changed = getExtent().setBlock(position, target);
        if (changed) {
            record(x, y, z, before, target.getAsString());
        }
        return changed;
    }

    private void record(int x, int y, int z, String before, String after) {
        recorder.recordBatchChange(
            new BlockPosition(worldId, x, y, z),
            actor,
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            batchId,
            ""
        );
    }
}
