package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;

import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

/**
 * One exact chunk coordinate authorized by a rollback plan.
 */
public record ExactChunkCoordinate(UUID worldId, int x, int z) {
    public static final Comparator<ExactChunkCoordinate> STABLE_ORDER = Comparator
        .comparing(ExactChunkCoordinate::worldId)
        .thenComparingInt(ExactChunkCoordinate::x)
        .thenComparingInt(ExactChunkCoordinate::z);

    public ExactChunkCoordinate {
        worldId = Objects.requireNonNull(worldId, "worldId");
    }

    public static ExactChunkCoordinate from(BlockPosition position) {
        Objects.requireNonNull(position, "position");
        return new ExactChunkCoordinate(position.worldId(), position.chunkX(), position.chunkZ());
    }
}
