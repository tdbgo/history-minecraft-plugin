package kr.playcity.history.model;

import java.util.List;
import java.util.Objects;

public record OperationItem(
    int sequence,
    BlockPosition position,
    BlockSnapshot before,
    BlockSnapshot after,
    List<Long> sourceIds
) {
    public OperationItem {
        position = Objects.requireNonNull(position, "position");
        before = Objects.requireNonNull(before, "before");
        after = Objects.requireNonNull(after, "after");
        sourceIds = List.copyOf(sourceIds);
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must not be negative");
        }
    }
}
