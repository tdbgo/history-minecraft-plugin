package kr.playcity.history.model;

import java.util.List;

public record RollbackPlan(
    List<PlannedBlockChange> changes,
    int sourceChangeCount,
    int unsafePositionCount
) {
    public RollbackPlan {
        changes = List.copyOf(changes);
        if (sourceChangeCount < 0) {
            throw new IllegalArgumentException("sourceChangeCount must not be negative");
        }
        if (unsafePositionCount < 0) {
            throw new IllegalArgumentException("unsafePositionCount must not be negative");
        }
    }
}
