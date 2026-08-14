package kr.playcity.history.model;

import java.util.List;
import java.util.Objects;

public record PlannedBlockChange(
    BlockPosition position,
    BlockSnapshot expected,
    BlockSnapshot target,
    List<Long> sourceIds
) {
    public PlannedBlockChange {
        position = Objects.requireNonNull(position, "position");
        expected = Objects.requireNonNull(expected, "expected");
        target = Objects.requireNonNull(target, "target");
        sourceIds = List.copyOf(sourceIds);
        if (sourceIds.isEmpty()) {
            throw new IllegalArgumentException("A planned change needs at least one source ID");
        }
    }
}
