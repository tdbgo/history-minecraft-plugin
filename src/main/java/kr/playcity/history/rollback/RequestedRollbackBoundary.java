package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.ChangeRecord;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Exact query boundary revalidation independent of the database implementation. */
public record RequestedRollbackBoundary(UUID worldId, int centerX, int centerZ, int radius) {
    public RequestedRollbackBoundary {
        worldId = Objects.requireNonNull(worldId, "worldId");
        if (radius < 0) {
            throw new IllegalArgumentException("radius must not be negative");
        }
    }

    public boolean contains(BlockPosition position) {
        if (!worldId.equals(position.worldId())) {
            return false;
        }
        long deltaX = (long) position.x() - centerX;
        long deltaZ = (long) position.z() - centerZ;
        if (Math.abs(deltaX) > radius || Math.abs(deltaZ) > radius) {
            return false;
        }
        long squaredRadius = (long) radius * radius;
        return deltaX * deltaX + deltaZ * deltaZ <= squaredRadius;
    }

    public void requireContainsAll(List<ChangeRecord> changes) {
        Objects.requireNonNull(changes, "changes");
        for (ChangeRecord change : changes) {
            if (!contains(change.position())) {
                throw new IllegalStateException(
                    "Storage returned a change outside the requested rollback boundary"
                );
            }
        }
    }
}
