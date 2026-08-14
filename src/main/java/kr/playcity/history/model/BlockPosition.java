package kr.playcity.history.model;

import java.util.Objects;
import java.util.UUID;

public record BlockPosition(UUID worldId, int x, int y, int z) {
    public BlockPosition {
        worldId = Objects.requireNonNull(worldId, "worldId");
    }

    public int chunkX() {
        return x >> 4;
    }

    public int chunkZ() {
        return z >> 4;
    }
}
